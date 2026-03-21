use std::collections::HashMap;

use async_trait::async_trait;
use chrono::NaiveDate;
use itertools::Itertools;
use tokio::{sync::mpsc::Sender, time::Instant};

use crate::{
    CANDIDATE_TICKER_RATIO, PROGRESS_INTERVAL_SECS, REQUIRED_DATA_COMPLETENESS,
    TRADE_DAYS_FRACTION,
    error::VfResult,
    filter::{filter_invalid::has_invalid_price, filter_st::is_st},
    financial::{
        helper::{calc_stock_cash_ratio, calc_stock_current_ratio, calc_stock_roe_of_years},
        stock::{StockIndicatorField, fetch_stock_indicators},
    },
    rule::{
        BacktestEvent, FundBacktestContext, RuleDefinition, RuleExecutor, calc_weights,
        rule_notify_calc_progress, rule_notify_indicators, rule_send_info, rule_send_warning,
    },
    spec::RuleOptions,
    ticker::Ticker,
    utils::stats,
};

pub struct Executor {
    options: RuleOptions,

    clusters_holding: HashMap<String, Vec<(Ticker, f64)>>,
}

impl Executor {
    pub fn new(definition: &RuleDefinition) -> Self {
        Self {
            options: definition.options.clone(),

            clusters_holding: HashMap::new(),
        }
    }
}

#[async_trait]
impl RuleExecutor for Executor {
    async fn exec(
        &mut self,
        context: &mut FundBacktestContext,
        date: &NaiveDate,
        event_sender: &Sender<BacktestEvent>,
    ) -> VfResult<()> {
        let rule_name = mod_name!();

        let cash_ratio_quantile_lower =
            self.options
                .read_f64_in_range("cash_ratio_quantile_lower", 0.0, 0.0..=1.0);
        let clusters_lookback_trade_days = self.options.read_object("clusters_lookback_trade_days");
        let current_ratio_quantile_lower =
            self.options
                .read_f64_in_range("current_ratio_quantile_lower", 0.0, 0.0..=1.0);
        let default_lookback_trade_days = self
            .options
            .read_u64_no_zero("default_lookback_trade_days", 1000);
        let limit = self.options.read_u64_no_zero("limit", 5);
        let pb_mean_count = self.options.read_u64_no_zero("pb_mean_count", 21);
        let pb_quantile_lower = self
            .options
            .read_f64_in_range("pb_quantile_lower", 0.0, 0.0..=1.0);
        let pb_quantile_upper = self
            .options
            .read_f64_in_range("pb_quantile_upper", 1.0, 0.0..=1.0);
        let roe_quantile_lower =
            self.options
                .read_f64_in_range("roe_quantile_lower", 0.0, 0.0..=1.0);
        let roe_years = self.options.read_u64_no_zero("roe_years", 3);
        let weight_method = self.options.read_str("weight_method", "equal");

        let mut clusters_lookback_trade_days_map: HashMap<String, u64> = HashMap::new();
        if let Some(clusters_lookback_trade_days) = clusters_lookback_trade_days {
            for (k, v) in clusters_lookback_trade_days {
                if let Some(v_u64) = v.as_u64() {
                    clusters_lookback_trade_days_map.insert(k.to_string(), v_u64);
                }
            }
        }

        let tickers_map = context.fund_definition.all_tickers_map(date).await?;
        if !tickers_map.is_empty() {
            let mut clusters_tickers: HashMap<String, Vec<Ticker>> = HashMap::new();
            for (ticker, (_, optional_ticker_source)) in &tickers_map {
                if let Some(ticker_source) = optional_ticker_source {
                    let cluster_name = ticker_source.source.to_string();
                    clusters_tickers
                        .entry(cluster_name)
                        .or_default()
                        .push(ticker.clone());
                }
            }

            let mut last_time = Instant::now();
            let mut calc_count: usize = 0;

            let mut clusters_targets: HashMap<String, Vec<(Ticker, f64)>> = HashMap::new();
            let mut clusters_candidates: HashMap<String, Vec<(Ticker, f64)>> = HashMap::new();

            for (cluster_name, cluster_tickers) in clusters_tickers.iter() {
                let mut cluster_factors_map: HashMap<NaiveDate, Vec<(f64, f64)>> = HashMap::new();
                let mut tickers_pb_map: HashMap<Ticker, f64> = HashMap::new();
                let mut tickers_factors_map: HashMap<Ticker, Factors> = HashMap::new();

                let cluster_lookback_trade_days = clusters_lookback_trade_days_map
                    .get(cluster_name)
                    .copied()
                    .unwrap_or(default_lookback_trade_days);

                for ticker in cluster_tickers {
                    calc_count += 1;

                    if let Ok(st) = is_st(
                        ticker,
                        date,
                        (cluster_lookback_trade_days as f64 / TRADE_DAYS_FRACTION) as u64,
                    )
                    .await
                    {
                        if st {
                            continue;
                        }
                    } else {
                        continue;
                    }

                    if let Ok(invalid_price) = has_invalid_price(ticker, date).await {
                        if invalid_price {
                            rule_send_warning(
                                rule_name,
                                &format!("[Invalid Price] {ticker}"),
                                date,
                                event_sender,
                            )
                            .await;
                            continue;
                        }
                    } else {
                        continue;
                    }

                    let stock_indicators = fetch_stock_indicators(ticker).await?;
                    let pbs_with_date_lt = stock_indicators.get_latest_values::<f64>(
                        date,
                        false,
                        &StockIndicatorField::Pb.to_string(),
                        cluster_lookback_trade_days as u32,
                    );
                    if pbs_with_date_lt.len()
                        < (cluster_lookback_trade_days as f64 * REQUIRED_DATA_COMPLETENESS).round()
                            as usize
                    {
                        rule_send_warning(
                            rule_name,
                            &format!(
                                "[No Enough Data] {ticker} {cluster_lookback_trade_days}({})",
                                pbs_with_date_lt.len()
                            ),
                            date,
                            event_sender,
                        )
                        .await;
                        continue;
                    }

                    let market_caps_with_date = stock_indicators.get_latest_values::<f64>(
                        date,
                        false,
                        &StockIndicatorField::MarketValueTotal.to_string(),
                        cluster_lookback_trade_days as u32,
                    );
                    let market_cap_map: HashMap<NaiveDate, f64> =
                        market_caps_with_date.into_iter().collect();

                    for (date, pb) in &pbs_with_date_lt {
                        if let Some(market_cap) = market_cap_map.get(date) {
                            cluster_factors_map
                                .entry(*date)
                                .or_default()
                                .push((*pb, *market_cap));
                        }
                    }

                    let pbs_lt: Vec<f64> = pbs_with_date_lt.iter().map(|(_, v)| *v).collect();
                    let recent_pbs = pbs_lt
                        .iter()
                        .tail(pb_mean_count as usize)
                        .copied()
                        .collect::<Vec<_>>();
                    if let Some(pb) = stats::mean(&recent_pbs) {
                        if pb.is_finite() {
                            tickers_pb_map.insert(ticker.clone(), pb);
                        }
                    }

                    if let Ok(cash_ratio) = calc_stock_cash_ratio(ticker, date).await
                        && let Ok(current_ratio) = calc_stock_current_ratio(ticker, date).await
                        && let Ok(Some(roe)) =
                            calc_stock_roe_of_years(ticker, date, roe_years as u32).await
                    {
                        tickers_factors_map.insert(
                            ticker.clone(),
                            Factors {
                                cash_ratio,
                                current_ratio,
                                roe,
                            },
                        );
                    }

                    if last_time.elapsed().as_secs() > PROGRESS_INTERVAL_SECS {
                        rule_notify_calc_progress(
                            rule_name,
                            calc_count as f64 / tickers_map.len() as f64 * 100.0,
                            date,
                            event_sender,
                        )
                        .await;

                        last_time = Instant::now();
                    }
                }

                let tickers_cash_ratio_lower = stats::quantile_value(
                    &tickers_factors_map
                        .values()
                        .map(|f| f.cash_ratio)
                        .collect::<Vec<f64>>(),
                    cash_ratio_quantile_lower,
                )
                .unwrap_or(0.0);

                let tickers_current_ratio_lower = stats::quantile_value(
                    &tickers_factors_map
                        .values()
                        .map(|f| f.current_ratio)
                        .collect::<Vec<f64>>(),
                    current_ratio_quantile_lower,
                )
                .unwrap_or(0.0);

                let tickers_roe_lower = stats::quantile_value(
                    &tickers_factors_map
                        .values()
                        .map(|f| f.roe)
                        .collect::<Vec<f64>>(),
                    roe_quantile_lower,
                )
                .unwrap_or(0.0);

                let mut cluster_pbs_with_date: Vec<(NaiveDate, f64)> = cluster_factors_map
                    .into_iter()
                    .map(|(date, pbs)| {
                        let weighted_pb = pbs
                            .iter()
                            .map(|(pb, market_cap)| pb * market_cap.sqrt())
                            .sum::<f64>()
                            / pbs
                                .iter()
                                .map(|(_, market_cap)| market_cap.sqrt())
                                .sum::<f64>();
                        (date, weighted_pb)
                    })
                    .filter(|(_, pb)| pb.is_finite())
                    .collect();
                cluster_pbs_with_date.sort_by_key(|(date, _)| *date);

                let cluster_pbs: Vec<f64> =
                    cluster_pbs_with_date.iter().map(|(_, pb)| *pb).collect();
                let recent_cluster_pbs: Vec<f64> = cluster_pbs
                    .iter()
                    .tail(pb_mean_count as usize)
                    .copied()
                    .collect();
                if let Some(cluster_pb) = stats::mean(&recent_cluster_pbs) {
                    if let Some(holding) = self.clusters_holding.get(cluster_name) {
                        // Check overvalued
                        clusters_targets.insert(cluster_name.to_string(), holding.clone());

                        if let Some(cluster_pb_sell) =
                            stats::quantile_value(&cluster_pbs, pb_quantile_upper)
                            && let Some(cluster_pb_overvalued) =
                                stats::quantile_value(&cluster_pbs, pb_quantile_upper - 0.1)
                        {
                            if cluster_pb > cluster_pb_sell {
                                rule_send_info(
                                    rule_name,
                                    &format!("[Cluster Sell Signal] {cluster_name} PB:{cluster_pb:.2}>{cluster_pb_sell:.2}"),
                                    date,
                                    event_sender,
                                )
                                .await;

                                clusters_targets.remove(cluster_name);
                            } else if cluster_pb > cluster_pb_overvalued {
                                rule_send_info(
                                    rule_name,
                                    &format!("[Cluster Overvalued] {cluster_name} PB:{cluster_pb_overvalued:.2}<{cluster_pb:.2}<{cluster_pb_sell:.2}"),
                                    date,
                                    event_sender,
                                )
                                .await;
                            }
                        }
                    } else {
                        // Check undervalued
                        if let Some(cluster_pb_buy) =
                            stats::quantile_value(&cluster_pbs, pb_quantile_lower)
                            && let Some(cluster_pb_undervalued) =
                                stats::quantile_value(&cluster_pbs, pb_quantile_lower + 0.1)
                            && let Some(cluster_pb_std) = stats::std(&cluster_pbs)
                        {
                            if cluster_pb < cluster_pb_buy {
                                rule_send_info(
                                    rule_name,
                                    &format!("[Cluster Buy Signal] {cluster_name} PB:{cluster_pb:.2}<{cluster_pb_buy:.2}"),
                                    date,
                                    event_sender,
                                )
                                .await;

                                let mut tickers_indicator: Vec<(Ticker, f64)> = tickers_pb_map
                                    .iter()
                                    .filter(|(ticker, _)| {
                                        if let Some(f) = tickers_factors_map.get(ticker) {
                                            f.cash_ratio > tickers_cash_ratio_lower
                                                && f.current_ratio > tickers_current_ratio_lower
                                                && f.roe > tickers_roe_lower
                                        } else {
                                            false
                                        }
                                    })
                                    .map(|(ticker, pb)| {
                                        (ticker.clone(), (cluster_pb - *pb) / cluster_pb_std)
                                    })
                                    .collect();
                                tickers_indicator.sort_by(|(_, a), (_, b)| b.total_cmp(a));

                                let top: Vec<(Ticker, f64)> = tickers_indicator
                                    .iter()
                                    .take((CANDIDATE_TICKER_RATIO + 1) * limit as usize)
                                    .cloned()
                                    .collect();
                                let targets: Vec<(Ticker, f64)> =
                                    top.iter().take(limit as usize).cloned().collect();
                                let candidates: Vec<(Ticker, f64)> =
                                    top.iter().skip(limit as usize).cloned().collect();

                                self.clusters_holding
                                    .insert(cluster_name.to_string(), targets.clone());

                                clusters_targets.insert(cluster_name.to_string(), targets);
                                clusters_candidates.insert(cluster_name.to_string(), candidates);
                            } else if cluster_pb < cluster_pb_undervalued {
                                rule_send_info(
                                    rule_name,
                                    &format!("[Cluster Undervalued] {cluster_name} PB:{cluster_pb_buy:.2}<{cluster_pb:.2}<{cluster_pb_undervalued:.2}"),
                                    date,
                                    event_sender,
                                )
                                .await;
                                continue;
                            }
                        }
                    }
                }
            }

            rule_notify_calc_progress(rule_name, 100.0, date, event_sender).await;

            let mut targets_indicator: Vec<(Ticker, f64)> = vec![];
            for targets in clusters_targets.values_mut() {
                targets_indicator.append(targets);
            }

            let mut candidates_indicator: Vec<(Ticker, f64)> = vec![];
            for candidates in clusters_candidates.values_mut() {
                candidates_indicator.append(candidates);
            }

            rule_notify_indicators(
                rule_name,
                &targets_indicator
                    .iter()
                    .map(|&(ref t, v)| (t.clone(), format!("{v:.4}")))
                    .collect::<Vec<_>>(),
                &candidates_indicator
                    .iter()
                    .map(|&(ref t, v)| (t.clone(), format!("{v:.4}")))
                    .collect::<Vec<_>>(),
                date,
                event_sender,
            )
            .await;

            let weights = calc_weights(&targets_indicator, weight_method)?;
            context.rebalance(&weights, date, event_sender).await?;
        }

        Ok(())
    }
}

#[derive(Debug)]
struct Factors {
    cash_ratio: f64,
    current_ratio: f64,
    roe: f64,
}
