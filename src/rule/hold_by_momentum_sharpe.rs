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
        stock::{StockDividendAdjust, fetch_stock_kline},
    },
    rule::{
        BacktestEvent, FundBacktestContext, RuleDefinition, RuleExecutor, notify_calc_progress,
        notify_tickers_indicator,
    },
    ticker::Ticker,
    utils::{
        datetime::date_to_str,
        financial::{calc_regression_momentum, calc_sharpe_ratio},
        math::{normalize_zscore, signed_powf},
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

        let factor_momentum_weight = self
            .options
            .get("factor_momentum_weight")
            .and_then(|v| v.as_f64())
            .unwrap_or(1.0);
        let factor_sharpe_weight = self
            .options
            .get("factor_sharpe_weight")
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
            .unwrap_or(21);
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

            let mut factors: Vec<(Ticker, f64, f64)> = vec![];
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
                    if prices.len() < (lookback_trade_days as f64 * 0.95).round() as usize {
                        let _ = event_sender
                            .send(BacktestEvent::Info(format!(
                                "[{date_str}] [{rule_name}] [No Enough Data] {ticker}"
                            )))
                            .await;
                        continue;
                    }

                    if let (Some(momentum), Some(sharpe)) = (
                        calc_regression_momentum(&prices),
                        calc_sharpe_ratio(&prices, 0.0),
                    ) {
                        factors.push((ticker.clone(), momentum, sharpe));
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

            let normalized_momentum_values =
                normalize_zscore(&factors.iter().map(|x| x.1).collect::<Vec<f64>>());
            let normalized_sharpe_values =
                normalize_zscore(&factors.iter().map(|x| x.2).collect::<Vec<f64>>());

            let mut indicators: Vec<(Ticker, f64)> = factors
                .iter()
                .enumerate()
                .filter_map(|(i, x)| {
                    let ticker = &x.0;

                    let momentum = normalized_momentum_values[i];
                    let sharpe = normalized_sharpe_values[i];

                    let indicator = factor_momentum_weight * (1.0 + momentum.tanh()) + factor_sharpe_weight * (1.0 + sharpe.tanh());
                    debug!("[{date_str}] {ticker}={indicator:.4} (Momentum={momentum:.4} Sharpe={sharpe:.4}");

                    if indicator.is_finite() {
                        Some((ticker.clone(), indicator))
                    } else {
                        None
                    }
                })
                .collect();
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
