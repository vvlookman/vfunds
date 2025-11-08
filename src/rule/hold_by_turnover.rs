use std::{cmp::Ordering, collections::HashMap};

use async_trait::async_trait;
use chrono::NaiveDate;
use log::debug;
use tokio::{sync::mpsc::Sender, time::Instant};

use crate::{
    PROGRESS_INTERVAL_SECS,
    error::VfResult,
    financial::{
        KlineField,
        stock::{
            StockDividendAdjust, StockReportCapitalField, fetch_stock_kline,
            fetch_stock_report_capital,
        },
    },
    rule::{
        BacktestEvent, FundBacktestContext, RuleDefinition, RuleExecutor, notify_calc_progress,
        notify_tickers_indicator,
    },
    ticker::Ticker,
    utils::{datetime::date_to_str, financial::calc_annualized_volatility},
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

        let enable_indicator_weighting = self
            .options
            .get("enable_indicator_weighting")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let limit = self
            .options
            .get("limit")
            .and_then(|v| v.as_u64())
            .unwrap_or(10);
        let lookback_trade_days = self
            .options
            .get("lookback_trade_days")
            .and_then(|v| v.as_u64())
            .unwrap_or(21);
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

            let mut indicators: Vec<(Ticker, f64)> = vec![];
            {
                let mut last_time = Instant::now();
                let mut calc_count: usize = 0;
                for ticker in tickers_map.keys() {
                    let kline = fetch_stock_kline(ticker, StockDividendAdjust::No).await?;
                    let volumes: Vec<f64> = kline
                        .get_latest_values::<f64>(
                            date,
                            false,
                            &KlineField::Volume.to_string(),
                            lookback_trade_days as u32,
                        )
                        .iter()
                        .map(|&(_, v)| v)
                        .collect();
                    if volumes.len() < (lookback_trade_days as f64 * 0.95).round() as usize {
                        let _ = event_sender
                            .send(BacktestEvent::Info(format!(
                                "[{date_str}] [{rule_name}] [No Enough Data] {ticker}"
                            )))
                            .await;
                        continue;
                    }

                    if let Some(volumes_vol) = calc_annualized_volatility(&volumes) {
                        let report_capital = fetch_stock_report_capital(ticker).await?;
                        if let Some((_, circulating_capital)) = report_capital
                            .get_latest_value::<f64>(
                                date,
                                false,
                                &StockReportCapitalField::Circulating.to_string(),
                            )
                        {
                            let volumes_avg = volumes.iter().sum::<f64>() / volumes.len() as f64;
                            let turnover_ratio = 100.0 * volumes_avg / circulating_capital;

                            let indicator = volumes_vol / turnover_ratio / 100.0;
                            debug!(
                                "[{date_str}] [{rule_name}] {ticker}={indicator:.4}({volumes_vol:.4}/{turnover_ratio:.4})"
                            );

                            if indicator.is_finite() {
                                indicators.push((ticker.clone(), indicator));
                            }
                        }
                    }

                    calc_count += 1;

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
                &[],
            )
            .await;

            let mut targets_weight: Vec<(Ticker, f64)> = vec![];
            for (ticker, indicator) in &targets_indicator {
                if let Some((weight, _)) = tickers_map.get(ticker) {
                    targets_weight.push((
                        ticker.clone(),
                        (*weight)
                            * if enable_indicator_weighting {
                                *indicator
                            } else {
                                1.0
                            },
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
