use std::{cmp::Ordering, collections::HashMap};

use async_trait::async_trait;
use chrono::NaiveDate;
use tokio::{sync::mpsc::Sender, time::Instant};

use crate::{
    PROGRESS_INTERVAL_SECS, REQUIRED_DATA_COMPLETENESS,
    error::VfResult,
    financial::{
        KlineField,
        stock::{
            StockDetail, StockDividendAdjust, StockReportPershareField, fetch_stock_detail,
            fetch_stock_kline, fetch_stock_report_pershare,
        },
        tool::{calc_stock_market_cap, calc_stock_pb, calc_stock_pe_ttm},
    },
    rule::{
        BacktestEvent, FundBacktestContext, RuleDefinition, RuleExecutor, notify_calc_progress,
        notify_tickers_indicator,
    },
    ticker::Ticker,
    utils::{
        datetime::date_to_str,
        financial::{calc_annualized_volatility, calc_regression_momentum},
        math::signed_powf,
        stats::quantile,
    },
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
        event_sender: Sender<BacktestEvent>,
    ) -> VfResult<()> {
        let rule_name = mod_name!();

        let limit = self
            .options
            .get("limit")
            .and_then(|v| v.as_u64())
            .unwrap_or(10);
        let lookback_trade_days = self
            .options
            .get("lookback_trade_days")
            .and_then(|v| v.as_u64())
            .unwrap_or(126);
        let market_cap_quantile_lower = self
            .options
            .get("market_cap_quantile_lower")
            .and_then(|v| v.as_f64())
            .unwrap_or(0.0);
        let market_cap_quantile_upper = self
            .options
            .get("market_cap_quantile_upper")
            .and_then(|v| v.as_f64())
            .unwrap_or(1.0);
        let momentum_quantile_lower = self
            .options
            .get("momentum_quantile_lower")
            .and_then(|v| v.as_f64())
            .unwrap_or(0.0);
        let momentum_quantile_upper = self
            .options
            .get("momentum_quantile_upper")
            .and_then(|v| v.as_f64())
            .unwrap_or(1.0);
        let pb_quantile_lower = self
            .options
            .get("pb_quantile_lower")
            .and_then(|v| v.as_f64())
            .unwrap_or(0.0);
        let pb_quantile_upper = self
            .options
            .get("pb_quantile_upper")
            .and_then(|v| v.as_f64())
            .unwrap_or(1.0);
        let pe_quantile_lower = self
            .options
            .get("pe_quantile_lower")
            .and_then(|v| v.as_f64())
            .unwrap_or(0.0);
        let pe_quantile_upper = self
            .options
            .get("pe_quantile_upper")
            .and_then(|v| v.as_f64())
            .unwrap_or(1.0);
        let profit_rate_lower = self
            .options
            .get("profit_rate_lower")
            .and_then(|v| v.as_f64())
            .unwrap_or(0.0);
        let revenue_rate_lower = self
            .options
            .get("revenue_rate_lower")
            .and_then(|v| v.as_f64())
            .unwrap_or(0.0);
        let skip_same_sector = self
            .options
            .get("skip_same_sector")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let volatility_quantile_lower = self
            .options
            .get("volatility_quantile_lower")
            .and_then(|v| v.as_f64())
            .unwrap_or(0.0);
        let volatility_quantile_upper = self
            .options
            .get("volatility_quantile_upper")
            .and_then(|v| v.as_f64())
            .unwrap_or(1.0);
        let weight_exp = self
            .options
            .get("weight_exp")
            .and_then(|v| v.as_f64())
            .unwrap_or(0.0);
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
            let date_str = date_to_str(date);

            let mut tickers_factors: Vec<(Ticker, Factors)> = vec![];
            {
                let mut last_time = Instant::now();
                let mut calc_count: usize = 0;

                for ticker in tickers_map.keys() {
                    calc_count += 1;

                    if context.portfolio.reserved_cash.contains_key(ticker) {
                        continue;
                    }

                    let kline = fetch_stock_kline(ticker, StockDividendAdjust::ForwardProp).await?;
                    let prices: Vec<f64> = kline
                        .get_latest_values::<f64>(
                            date,
                            false,
                            &KlineField::Close.to_string(),
                            lookback_trade_days as u32,
                        )
                        .iter()
                        .map(|&(_, v)| v)
                        .collect();
                    if prices.len()
                        < (lookback_trade_days as f64 * REQUIRED_DATA_COMPLETENESS).round() as usize
                    {
                        let _ = event_sender
                            .send(BacktestEvent::Info(format!(
                                "[{date_str}] [{rule_name}] [No Enough Data] {ticker}"
                            )))
                            .await;
                        continue;
                    }

                    // let report_pershare = fetch_stock_report_pershare(ticker).await?;
                    if let (
                        Some(market_cap),
                        // Some(momentum),
                        // Some(pb),
                        // Some(pe),
                        // Some((_, profit_rate)),
                        // Some((_, revenue_rate)),
                        Some(volatility),
                    ) = (
                        calc_stock_market_cap(ticker, date).await?,
                        // calc_regression_momentum(&prices),
                        // calc_stock_pb(ticker, date).await?,
                        // calc_stock_pe_ttm(ticker, date).await?,
                        // report_pershare.get_latest_value::<f64>(
                        //     date,
                        //     false,
                        //     &StockReportPershareField::AdjustedNetProfitRate.to_string(),
                        // ),
                        // report_pershare.get_latest_value::<f64>(
                        //     date,
                        //     false,
                        //     &StockReportPershareField::IncRevenueRate.to_string(),
                        // ),
                        calc_annualized_volatility(&prices),
                    ) {
                        tickers_factors.push((
                            ticker.clone(),
                            Factors {
                                market_cap,
                                // momentum,
                                // pb,
                                // pe,
                                // profit_rate,
                                // revenue_rate,
                                volatility,
                            },
                        ));
                    }

                    if last_time.elapsed().as_secs() > PROGRESS_INTERVAL_SECS {
                        notify_calc_progress(
                            event_sender.clone(),
                            date,
                            rule_name,
                            calc_count as f64 / tickers_map.len() as f64 * 100.0,
                        )
                        .await;

                        last_time = Instant::now();
                    }
                }

                notify_calc_progress(event_sender.clone(), date, rule_name, 100.0).await;
            }

            let factors_market_cap = tickers_factors
                .iter()
                .map(|(_, f)| f.market_cap)
                .collect::<Vec<f64>>();
            let market_cap_lower = quantile(&factors_market_cap, market_cap_quantile_lower);
            let market_cap_upper = quantile(&factors_market_cap, market_cap_quantile_upper);

            // let factors_momentum = tickers_factors
            //     .iter()
            //     .map(|(_, f)| f.momentum)
            //     .collect::<Vec<f64>>();
            // let momentum_lower = quantile(&factors_momentum, momentum_quantile_lower);
            // let momentum_upper = quantile(&factors_momentum, momentum_quantile_upper);

            // let factors_pb = tickers_factors
            //     .iter()
            //     .map(|(_, f)| f.pb)
            //     .collect::<Vec<f64>>();
            // let pb_lower = quantile(&factors_pb, pb_quantile_lower);
            // let pb_upper = quantile(&factors_pb, pb_quantile_upper);

            // let factors_pe = tickers_factors
            //     .iter()
            //     .map(|(_, f)| f.pe)
            //     .collect::<Vec<f64>>();
            // let pe_lower = quantile(&factors_pe, pe_quantile_lower);
            // let pe_upper = quantile(&factors_pe, pe_quantile_upper);

            let factors_volatility = tickers_factors
                .iter()
                .map(|(_, f)| f.volatility)
                .collect::<Vec<f64>>();
            let volatility_lower = quantile(&factors_volatility, volatility_quantile_lower);
            let volatility_upper = quantile(&factors_volatility, volatility_quantile_upper);

            let mut indicators: Vec<(Ticker, f64)> = vec![];
            for (ticker, factors) in tickers_factors {
                if let Some(market_cap_lower) = market_cap_lower {
                    if factors.market_cap < market_cap_lower {
                        continue;
                    }
                }

                if let Some(market_cap_upper) = market_cap_upper {
                    if factors.market_cap > market_cap_upper {
                        continue;
                    }
                }

                // if let Some(momentum_lower) = momentum_lower {
                //     if factors.momentum < momentum_lower {
                //         continue;
                //     }
                // }

                // if let Some(momentum_upper) = momentum_upper {
                //     if factors.momentum > momentum_upper {
                //         continue;
                //     }
                // }

                // if let Some(pb_lower) = pb_lower {
                //     if factors.pb < pb_lower {
                //         continue;
                //     }
                // }

                // if let Some(pb_upper) = pb_upper {
                //     if factors.pb > pb_upper {
                //         continue;
                //     }
                // }

                // if let Some(pe_lower) = pe_lower {
                //     if factors.pe < pe_lower {
                //         continue;
                //     }
                // }

                // if let Some(pe_upper) = pe_upper {
                //     if factors.pe > pe_upper {
                //         continue;
                //     }
                // }

                if let Some(volatility_lower) = volatility_lower {
                    if factors.volatility < volatility_lower {
                        continue;
                    }
                }

                if let Some(volatility_upper) = volatility_upper {
                    if factors.volatility > volatility_upper {
                        continue;
                    }
                }

                // if factors.profit_rate >= profit_rate_lower
                //     && factors.revenue_rate >= revenue_rate_lower
                if factors.market_cap > 0.0 {
                    indicators.push((ticker, 1e8 / factors.market_cap));
                }
            }
            indicators.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(Ordering::Equal));

            let top_indicators = indicators
                .iter()
                .take(3 * limit as usize)
                .collect::<Vec<_>>();

            let mut tickers_detail: HashMap<Ticker, StockDetail> = HashMap::new();
            if skip_same_sector {
                for (ticker, _) in &top_indicators {
                    let detail = fetch_stock_detail(ticker).await?;
                    tickers_detail.insert(ticker.clone(), detail);
                }
            }

            let mut targets_indicator: Vec<(Ticker, f64)> = vec![];
            let mut candidates_indicator: Vec<(Ticker, f64)> = vec![];
            for (ticker, indicator) in &top_indicators {
                if targets_indicator.len() < limit as usize {
                    if skip_same_sector
                        && targets_indicator.iter().any(|(a, _)| {
                            if let (Some(Some(sector_a)), Some(Some(sector_b))) = (
                                tickers_detail.get(a).map(|v| &v.sector),
                                tickers_detail.get(ticker).map(|v| &v.sector),
                            ) {
                                sector_a == sector_b
                            } else {
                                false
                            }
                        })
                    {
                        candidates_indicator.push((ticker.clone(), *indicator));
                    } else {
                        targets_indicator.push((ticker.clone(), *indicator));
                    }
                } else {
                    candidates_indicator.push((ticker.clone(), *indicator));
                }
            }

            notify_tickers_indicator(
                event_sender.clone(),
                date,
                rule_name,
                &targets_indicator
                    .iter()
                    .map(|&(ref t, v)| (t.clone(), format!("{v:.4}")))
                    .collect::<Vec<_>>(),
                &candidates_indicator
                    .iter()
                    .map(|&(ref t, v)| (t.clone(), format!("{v:.4}")))
                    .collect::<Vec<_>>(),
            )
            .await;

            let mut targets_weight: Vec<(Ticker, f64)> = vec![];
            for (ticker, indicator) in &targets_indicator {
                if let Some((weight, _)) = tickers_map.get(ticker) {
                    targets_weight.push((
                        ticker.clone(),
                        (*weight) * signed_powf(*indicator, weight_exp),
                    ));
                }
            }

            context
                .rebalance(&targets_weight, date, event_sender)
                .await?;
        }

        Ok(())
    }
}

#[derive(Debug)]
struct Factors {
    market_cap: f64,
    // momentum: f64,
    // pb: f64,
    // pe: f64,
    // profit_rate: f64,
    // revenue_rate: f64,
    volatility: f64,
}
