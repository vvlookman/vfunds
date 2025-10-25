use std::{cmp::Ordering, collections::HashMap, str::FromStr};

use async_trait::async_trait;
use chrono::{Days, NaiveDate};
use log::debug;
use tokio::{sync::mpsc::Sender, time::Instant};

use crate::{
    PROGRESS_INTERVAL_SECS,
    error::VfResult,
    financial::{
        bond::{ConvBondAnalysisField, fetch_conv_bond_analysis, fetch_conv_bonds},
        get_ticker_title,
    },
    rule::{BacktestEvent, FundBacktestContext, RuleDefinition, RuleExecutor},
    ticker::Ticker,
    utils::datetime::date_to_str,
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

        let filter_issue_size_floor = self
            .options
            .get("filter_issue_size_floor")
            .and_then(|v| v.as_f64())
            .unwrap_or(10000.0);
        let filter_min_remaining_days = self
            .options
            .get("filter_min_remaining_days")
            .and_then(|v| v.as_u64())
            .unwrap_or(30);
        let limit = self
            .options
            .get("limit")
            .and_then(|v| v.as_u64())
            .unwrap_or(10);
        {
            if limit == 0 {
                panic!("limit must > 0");
            }
        }

        let conv_bonds = fetch_conv_bonds(date, 60).await?;
        if !conv_bonds.is_empty() {
            let date_str = date_to_str(date);

            let mut indicators: Vec<(Ticker, f64)> = vec![];
            {
                let mut last_time = Instant::now();
                let mut calc_count: usize = 0;
                for conv_bond in &conv_bonds {
                    if conv_bond.title.ends_with("退") {
                        continue;
                    }

                    if let Some(issue_size) = conv_bond.issue_size {
                        if issue_size < filter_issue_size_floor {
                            continue;
                        }
                    }

                    if let Some(expire_date) = conv_bond.expire_date {
                        if expire_date < *date + Days::new(filter_min_remaining_days) {
                            continue;
                        }
                    }

                    if let Ok(ticker) = Ticker::from_str(&conv_bond.code) {
                        let analysis = fetch_conv_bond_analysis(&ticker).await?;
                        if let (Some((_, conversion_premium)), Some((_, price))) = (
                            analysis.get_latest_value::<f64>(
                                date,
                                &ConvBondAnalysisField::ConversionPremium.to_string(),
                            ),
                            analysis.get_latest_value::<f64>(
                                date,
                                &ConvBondAnalysisField::Price.to_string(),
                            ),
                        ) {
                            if conversion_premium > 0.0 && price > 0.0 {
                                let indicator = 100.0 / (100.0 + conversion_premium);
                                debug!(
                                    "[{date_str}] [{rule_name}] {ticker}={indicator:.4}(${price:.2})"
                                );

                                indicators.push((ticker, indicator));
                            }
                        }
                    }

                    calc_count += 1;

                    if last_time.elapsed().as_secs() > PROGRESS_INTERVAL_SECS {
                        let calc_progress_pct = calc_count as f64 / conv_bonds.len() as f64 * 100.0;
                        let _ = event_sender
                            .send(BacktestEvent::Toast(format!(
                                "[{date_str}] [{rule_name}] Σ {calc_progress_pct:.2}% ..."
                            )))
                            .await;

                        last_time = Instant::now();
                    }
                }

                let _ = event_sender
                    .send(BacktestEvent::Toast(format!(
                        "[{date_str}] [{rule_name}] Σ 100%"
                    )))
                    .await;
            }
            indicators.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(Ordering::Equal));

            let filetered_indicators = indicators.iter().take(limit as usize).collect::<Vec<_>>();
            if !filetered_indicators.is_empty() {
                let mut top_tickers_strs: Vec<String> = vec![];
                for (ticker, indicator) in &filetered_indicators {
                    let ticker_title = get_ticker_title(ticker).await.unwrap_or_default();
                    top_tickers_strs.push(format!("{ticker}({ticker_title})={indicator:.4}"));
                }

                let top_tickers_str = top_tickers_strs.join(" ");
                let _ = event_sender
                    .send(BacktestEvent::Info(format!(
                        "[{date_str}] [{rule_name}] {top_tickers_str}"
                    )))
                    .await;
            }

            let mut targets_weight: Vec<(Ticker, f64)> = vec![];
            for (ticker, _) in &filetered_indicators {
                targets_weight.push((ticker.clone(), 1.0));
            }

            context
                .rebalance(&targets_weight, date, event_sender)
                .await?;
        }

        Ok(())
    }
}
