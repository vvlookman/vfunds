use std::collections::HashMap;

use async_trait::async_trait;
use chrono::NaiveDate;
use itertools::Itertools;
use tokio::{sync::mpsc::Sender, time::Instant};

use crate::{
    CANDIDATE_TICKER_RATIO, PROGRESS_INTERVAL_SECS, REQUIRED_DATA_COMPLETENESS, STALE_DAYS_LONG,
    TRADE_DAYS_FRACTION,
    error::VfResult,
    filter::{filter_invalid::has_invalid_price, filter_st::is_st},
    financial::stock::{
        StockIndicatorField, StockReportPershareField, fetch_stock_indicators,
        fetch_stock_report_pershare,
    },
    rule::{
        BacktestEvent, FundBacktestContext, RuleDefinition, RuleExecutor, calc_weights,
        rule_notify_calc_progress, rule_notify_indicators, rule_send_warning,
    },
    ticker::Ticker,
    utils::stats,
};

pub struct Executor {
    #[allow(dead_code)]
    options: HashMap<String, serde_json::Value>,

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

        let clusters_lookback_trade_days = self
            .options
            .get("clusters_lookback_trade_days")
            .and_then(|v| v.as_object());
        let default_lookback_trade_days = self
            .options
            .get("default_lookback_trade_days")
            .and_then(|v| v.as_u64())
            .unwrap_or(1000);
        let limit = self
            .options
            .get("limit")
            .and_then(|v| v.as_u64())
            .unwrap_or(10);
        let pb_mean_count = self
            .options
            .get("pb_mean_count")
            .and_then(|v| v.as_u64())
            .unwrap_or(21);
        let pb_quantile_lower = self
            .options
            .get("pb_quantile_lower")
            .and_then(|v| v.as_f64())
            .unwrap_or(0.3);
        let pb_quantile_upper = self
            .options
            .get("pb_quantile_upper")
            .and_then(|v| v.as_f64())
            .unwrap_or(0.7);
        let roe_quantile_lower = self
            .options
            .get("roe_quantile_lower")
            .and_then(|v| v.as_f64())
            .unwrap_or(0.0);
        let weight_method = self
            .options
            .get("weight_method")
            .and_then(|v| v.as_str())
            .unwrap_or("equal");
        {
            if default_lookback_trade_days == 0 {
                panic!("default_lookback_trade_days must > 0");
            }

            if limit == 0 {
                panic!("limit must > 0");
            }

            if pb_mean_count == 0 {
                panic!("pb_mean_count must > 0");
            }
        }

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
                let mut tickers_roe_map: HashMap<Ticker, f64> = HashMap::new();

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
                            &format!("[No Enough Data] {ticker}"),
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

                    let report_pershare = fetch_stock_report_pershare(ticker).await?;
                    let roe_with_date = report_pershare.get_latest_value::<f64>(
                        date,
                        STALE_DAYS_LONG,
                        false,
                        &StockReportPershareField::Roe.to_string(),
                    );

                    if let Some((_, roe)) = roe_with_date {
                        tickers_roe_map.insert(ticker.clone(), roe);
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

                let tickers_roe_lower = stats::quantile_value(
                    &tickers_roe_map.values().copied().collect::<Vec<f64>>(),
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

                        if let Some(cluster_pb_upper) =
                            stats::quantile_value(&cluster_pbs, pb_quantile_upper)
                        {
                            if cluster_pb > cluster_pb_upper {
                                clusters_targets.remove(cluster_name);
                            }
                        }
                    } else {
                        // Check undervalued
                        if let Some(cluster_pb_lower) =
                            stats::quantile_value(&cluster_pbs, pb_quantile_lower)
                            && let Some(cluster_pb_std) = stats::std(&cluster_pbs)
                        {
                            if cluster_pb < cluster_pb_lower {
                                let mut tickers_indicator: Vec<(Ticker, f64)> = tickers_pb_map
                                    .iter()
                                    .filter(|(ticker, _)| {
                                        if let Some(roe) = tickers_roe_map.get(ticker) {
                                            *roe > tickers_roe_lower
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
