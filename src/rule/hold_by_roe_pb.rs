use std::{cmp::Ordering, collections::HashMap};

use async_trait::async_trait;
use chrono::NaiveDate;
use tokio::{sync::mpsc::Sender, time::Instant};

use crate::{
    CANDIDATE_TICKER_RATIO, PROGRESS_INTERVAL_SECS,
    error::VfResult,
    financial::{
        stock::{
            StockDetail, StockReportPershareField, fetch_stock_detail, fetch_stock_report_pershare,
        },
        tool::calc_stock_pb,
    },
    rule::{
        BacktestEvent, FundBacktestContext, RuleDefinition, RuleExecutor, calc_weights,
        rule_notify_calc_progress, rule_notify_indicators, rule_send_warning,
    },
    ticker::Ticker,
    utils::stats::quantile,
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

        let limit = self
            .options
            .get("limit")
            .and_then(|v| v.as_u64())
            .unwrap_or(10);
        let pb_quantile_upper = self
            .options
            .get("pb_quantile_upper")
            .and_then(|v| v.as_f64())
            .unwrap_or(1.0);
        let roe_quantile_lower = self
            .options
            .get("roe_quantile_lower")
            .and_then(|v| v.as_f64())
            .unwrap_or(0.0);
        let skip_same_sector = self
            .options
            .get("skip_same_sector")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let weight_method = self
            .options
            .get("weight_method")
            .and_then(|v| v.as_str())
            .unwrap_or("equal");
        {
            if limit == 0 {
                panic!("limit must > 0");
            }
        }

        let tickers_map = context.fund_definition.all_tickers_map(date).await?;
        if !tickers_map.is_empty() {
            let mut tickers_factors: Vec<(Ticker, Factors)> = vec![];
            {
                let mut last_time = Instant::now();
                let mut calc_count: usize = 0;

                for ticker in tickers_map.keys() {
                    calc_count += 1;

                    if context.portfolio.reserved_cash.contains_key(ticker) {
                        continue;
                    }

                    let report_pershare = fetch_stock_report_pershare(ticker).await?;

                    let roe = report_pershare.get_latest_value::<f64>(
                        date,
                        false,
                        &StockReportPershareField::Roe.to_string(),
                    );
                    let pb = calc_stock_pb(ticker, date).await?;

                    if let Some(fail_factor_name) = match (roe, pb) {
                        (None, _) => Some("roe"),
                        (_, None) => Some("pb"),
                        (Some((_, roe)), Some(pb)) => {
                            if roe > 0.0 && pb > 0.0 {
                                tickers_factors.push((ticker.clone(), Factors { roe, pb }));
                            }

                            None
                        }
                    } {
                        rule_send_warning(
                            rule_name,
                            &format!("[Σ '{fail_factor_name}' Failed] {ticker}"),
                            date,
                            event_sender,
                        )
                        .await;
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

                rule_notify_calc_progress(rule_name, 100.0, date, event_sender).await;
            }

            let factors_roe = tickers_factors
                .iter()
                .map(|(_, f)| f.roe)
                .collect::<Vec<f64>>();
            let roe_lower = quantile(&factors_roe, roe_quantile_lower);

            let factors_pb = tickers_factors
                .iter()
                .map(|(_, f)| f.pb)
                .collect::<Vec<f64>>();
            let pb_upper = quantile(&factors_pb, pb_quantile_upper);

            let mut indicators: Vec<(Ticker, f64)> = vec![];
            for (ticker, factors) in tickers_factors {
                if let Some(roe_lower) = roe_lower {
                    if factors.roe < roe_lower {
                        continue;
                    }
                }

                if let Some(pb_upper) = pb_upper {
                    if factors.pb > pb_upper {
                        continue;
                    }
                }

                indicators.push((ticker, factors.roe / factors.pb));
            }
            indicators.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(Ordering::Equal));

            let top_indicators = indicators
                .iter()
                .take((CANDIDATE_TICKER_RATIO + 1) * limit as usize)
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
    roe: f64,
    pb: f64,
}
