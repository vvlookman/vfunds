use std::collections::HashMap;

use async_trait::async_trait;
use chrono::NaiveDate;
use tokio::{sync::mpsc::Sender, time::Instant};

use crate::{
    PROGRESS_INTERVAL_SECS, REQUIRED_DATA_COMPLETENESS, STALE_DAYS_SHORT, TRADE_DAYS_FRACTION,
    error::VfResult,
    filter::{filter_invalid::has_invalid_price, filter_st::is_st},
    financial::{
        KlineField, get_ticker_kline,
        stock::{StockIndicatorField, fetch_stock_indicators},
    },
    rule::{
        BacktestEvent, FundBacktestContext, RuleDefinition, RuleExecutor, calc_weights,
        rule_notify_calc_progress, rule_notify_indicators, rule_send_info, rule_send_warning,
        select_by_indicators,
    },
    ticker::Ticker,
    utils::stats::{pct_change, quantile_value},
};

pub struct Executor {
    #[allow(dead_code)]
    options: HashMap<String, serde_json::Value>,
}

impl Executor {
    pub fn new(definition: &RuleDefinition) -> Self {
        Self {
            options: definition.options.clone(),
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

        let deviation_quantile_upper = self
            .options
            .get("deviation_quantile_upper")
            .and_then(|v| v.as_f64())
            .unwrap_or(1.0);
        let limit = self
            .options
            .get("limit")
            .and_then(|v| v.as_u64())
            .unwrap_or(10);
        let lookback_trade_days = self
            .options
            .get("lookback_trade_days")
            .and_then(|v| v.as_u64())
            .unwrap_or(20);
        let weight_method = self
            .options
            .get("weight_method")
            .and_then(|v| v.as_str())
            .unwrap_or("equal");
        {
            if limit == 0 {
                panic!("limit must > 0");
            }

            if lookback_trade_days == 0 {
                panic!("lookback_trade_days must > 0");
            }
        }

        let tickers_map = context.fund_definition.all_tickers_map(date).await?;
        if !tickers_map.is_empty() {
            let mut indicators: Vec<(Ticker, f64)> = vec![];

            let tickers_map_by_source: HashMap<String, Vec<Ticker>> = tickers_map.iter().fold(
                HashMap::new(),
                |mut acc, (ticker, (_, optional_ticker_source))| {
                    if let Some(ticker_source) = optional_ticker_source {
                        acc.entry(ticker_source.source.to_string())
                            .or_insert_with(Vec::new)
                            .push(ticker.clone());
                    }

                    acc
                },
            );

            let mut last_time = Instant::now();
            let mut calc_count: usize = 0;

            let lookback_days = lookback_trade_days as f64 / TRADE_DAYS_FRACTION;

            for (ticker_source, tickers_in_source) in tickers_map_by_source.iter() {
                let mut tickers_factors: Vec<(Ticker, Factors)> = vec![];
                {
                    for ticker in tickers_in_source {
                        calc_count += 1;

                        if let Ok(st) = is_st(ticker, date, lookback_days as u64).await {
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

                        let kline = get_ticker_kline(ticker, false).await?;
                        let prices_with_date: Vec<(NaiveDate, f64)> = kline
                            .get_latest_values::<f64>(
                                date,
                                false,
                                &KlineField::Close.to_string(),
                                lookback_trade_days as u32,
                            );
                        if prices_with_date.len()
                            < (lookback_trade_days as f64 * REQUIRED_DATA_COMPLETENESS).round()
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

                        if let Some((start_date, _)) = prices_with_date.first() {
                            let daily_values: Vec<f64> =
                                prices_with_date.iter().map(|&(_, v)| v).collect();
                            let daily_returns = pct_change(&daily_values);

                            let return_mean =
                                daily_returns.iter().sum::<f64>() / daily_returns.len() as f64;

                            let stock_indicators = fetch_stock_indicators(ticker).await?;
                            let market_cap_with_date = stock_indicators.get_latest_value::<f64>(
                                date,
                                STALE_DAYS_SHORT,
                                false,
                                &StockIndicatorField::MarketValueCirculating.to_string(),
                            );

                            tickers_factors.push((
                                ticker.clone(),
                                Factors {
                                    lookback_start_date: *start_date,
                                    lookback_return_mean: return_mean,
                                    market_cap: market_cap_with_date.map(|(_, v)| v),
                                },
                            ));

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
                    }
                }

                if let Some(latest_start_date) = tickers_factors
                    .iter()
                    .map(|(_, f)| f.lookback_start_date)
                    .max()
                {
                    let aligned_ticker_factors = tickers_factors
                        .iter()
                        .filter(|(_, f)| f.lookback_start_date == latest_start_date)
                        .collect::<Vec<_>>();

                    let market_cap_min: f64 = aligned_ticker_factors
                        .iter()
                        .filter_map(|(_, f)| f.market_cap)
                        .min_by(|a, b| a.total_cmp(b))
                        .unwrap_or(0.0);

                    let weighted_return_mean = aligned_ticker_factors
                        .iter()
                        .map(|(_, f)| {
                            f.lookback_return_mean * f.market_cap.unwrap_or(market_cap_min).sqrt()
                        })
                        .sum::<f64>()
                        / aligned_ticker_factors
                            .iter()
                            .map(|(_, f)| f.market_cap.unwrap_or(market_cap_min).sqrt())
                            .sum::<f64>();
                    let weighted_return_std = (aligned_ticker_factors
                        .iter()
                        .map(|(_, f)| {
                            (f.lookback_return_mean - weighted_return_mean).powi(2)
                                * f.market_cap.unwrap_or(market_cap_min).sqrt()
                        })
                        .sum::<f64>()
                        / aligned_ticker_factors
                            .iter()
                            .map(|(_, f)| f.market_cap.unwrap_or(market_cap_min).sqrt())
                            .sum::<f64>())
                    .sqrt();

                    let values_deviation: Vec<f64> = aligned_ticker_factors
                        .iter()
                        .map(|(_, f)| {
                            (weighted_return_mean - f.lookback_return_mean) / weighted_return_std
                        })
                        .collect();

                    let mut keep_count: usize = 0;
                    if let Some(deviation_upper) =
                        quantile_value(&values_deviation, deviation_quantile_upper)
                    {
                        for (ticker, f) in aligned_ticker_factors.iter() {
                            let deviation = (weighted_return_mean - f.lookback_return_mean)
                                / weighted_return_std;
                            if deviation > 0.0 && deviation <= deviation_upper {
                                keep_count += 1;
                                indicators.push((ticker.clone(), deviation));
                            }
                        }
                    }

                    rule_send_info(
                        rule_name,
                        &format!(
                            "[Universe] [{ticker_source}] {}({})",
                            tickers_in_source.len(),
                            keep_count
                        ),
                        date,
                        event_sender,
                    )
                    .await;
                }
            }

            rule_notify_calc_progress(rule_name, 100.0, date, event_sender).await;

            indicators.sort_by(|a, b| b.1.total_cmp(&a.1));

            let (targets_indicators, candidates_indicators) =
                select_by_indicators(&indicators, limit as usize, false).await?;

            rule_notify_indicators(
                rule_name,
                &targets_indicators
                    .iter()
                    .map(|&(ref t, v)| (t.clone(), format!("{v:.4}")))
                    .collect::<Vec<_>>(),
                &candidates_indicators
                    .iter()
                    .map(|&(ref t, v)| (t.clone(), format!("{v:.4}")))
                    .collect::<Vec<_>>(),
                date,
                event_sender,
            )
            .await;

            let weights = calc_weights(&targets_indicators, weight_method)?;
            context.rebalance(&weights, date, event_sender).await?;
        }

        Ok(())
    }
}

#[derive(Debug)]
struct Factors {
    lookback_start_date: NaiveDate,
    lookback_return_mean: f64,
    market_cap: Option<f64>,
}
