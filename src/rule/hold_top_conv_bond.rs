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
        get_ticker_price,
    },
    rule::{
        BacktestEvent, FundBacktestContext, RuleDefinition, RuleExecutor, notify_tickers_indicator,
    },
    ticker::Ticker,
    utils::{datetime::date_to_str, stats::quantile},
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
            .unwrap_or(5);
        let max_conversion_premium = self
            .options
            .get("max_conversion_premium")
            .and_then(|v| v.as_f64())
            .unwrap_or(0.0);
        let max_tenor_months = self
            .options
            .get("max_tenor_months")
            .and_then(|v| v.as_u64())
            .unwrap_or(72);
        let min_issue_size_quantile = self
            .options
            .get("min_issue_size_quantile")
            .and_then(|v| v.as_f64())
            .unwrap_or(0.05);
        let min_remaining_days = self
            .options
            .get("min_remaining_days")
            .and_then(|v| v.as_u64())
            .unwrap_or(30);
        {
            if limit == 0 {
                panic!("limit must > 0");
            }

            if max_tenor_months == 0 {
                panic!("max_tenor_months must > 0");
            }
        }

        let conv_bonds = fetch_conv_bonds(date, max_tenor_months as u32).await?;
        if !conv_bonds.is_empty() {
            let date_str = date_to_str(date);

            let filter_analysis_date = *date - Days::new(7);
            let filter_expire_date = *date + Days::new(min_remaining_days);

            let conv_bonds_issue_size = conv_bonds
                .iter()
                .filter_map(|b| b.issue_size)
                .collect::<Vec<_>>();
            let min_issue_size = quantile(&conv_bonds_issue_size, min_issue_size_quantile);

            let mut indicators: Vec<(Ticker, f64)> = vec![];
            {
                let mut last_time = Instant::now();
                let mut calc_count: usize = 0;
                for conv_bond in &conv_bonds {
                    if conv_bond.title.ends_with("退") {
                        continue;
                    }

                    if let Some(issue_size) = conv_bond.issue_size {
                        if let Some(min_issue_size) = min_issue_size {
                            if issue_size < min_issue_size {
                                continue;
                            }
                        }
                    }

                    if let Some(expire_date) = conv_bond.expire_date {
                        if expire_date < filter_expire_date {
                            continue;
                        }
                    }

                    if let Ok(ticker) = Ticker::from_str(&conv_bond.code) {
                        // Some conv bond price data is missing
                        if let Ok(Some(price)) = get_ticker_price(&ticker, date, false, 0).await {
                            let analysis = fetch_conv_bond_analysis(&ticker).await?;
                            if let Some((latest_date, conversion_premium)) = analysis
                                .get_latest_value::<f64>(
                                    date,
                                    false,
                                    &ConvBondAnalysisField::ConversionPremium.to_string(),
                                )
                            {
                                if price > 0.0
                                    && conversion_premium <= max_conversion_premium
                                    && latest_date >= filter_analysis_date
                                {
                                    let indicator = 100.0 / (100.0 + conversion_premium);
                                    debug!(
                                        "[{date_str}] [{rule_name}] {ticker}={indicator:.4}(${price:.2})"
                                    );

                                    indicators.push((ticker, indicator));
                                }
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

            let targets_indicator = indicators.iter().take(limit as usize).collect::<Vec<_>>();

            notify_tickers_indicator(
                event_sender.clone(),
                date,
                rule_name,
                &targets_indicator
                    .iter()
                    .map(|&(t, v)| (t.clone(), format!("{v:.4}")))
                    .collect::<Vec<_>>(),
                &indicators
                    .iter()
                    .skip(limit as usize)
                    .take(limit as usize)
                    .map(|&(ref t, v)| (t.clone(), format!("{v:.4}")))
                    .collect::<Vec<_>>(),
            )
            .await?;

            let targets_weight = targets_indicator
                .iter()
                .map(|(t, _)| (t.clone(), 1.0))
                .collect();

            context
                .rebalance(&targets_weight, date, event_sender)
                .await?;
        }

        Ok(())
    }
}
