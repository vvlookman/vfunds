use std::{cmp::Ordering, collections::HashMap};

use async_trait::async_trait;
use chrono::NaiveDate;
use tokio::{sync::mpsc::Sender, time::Instant};

use crate::{
    CANDIDATE_TICKER_RATIO, PROGRESS_INTERVAL_SECS, REQUIRED_DATA_COMPLETENESS,
    error::VfResult,
    financial::{
        KlineField,
        stock::{StockDividendAdjust, fetch_stock_kline},
    },
    rule::{
        BacktestEvent, FundBacktestContext, RuleDefinition, RuleExecutor, calc_weights,
        rule_notify_calc_progress, rule_notify_indicators, rule_send_info, rule_send_warning,
    },
    ticker::Ticker,
    utils::{
        financial::{calc_annualized_momentum, calc_efficiency_factor, calc_ema_bias_momentum},
        math::normalize_zscore,
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
        event_sender: &Sender<BacktestEvent>,
    ) -> VfResult<()> {
        let rule_name = mod_name!();

        let bias_weight = self
            .options
            .get("bias_weight")
            .and_then(|v| v.as_f64())
            .unwrap_or(0.0);
        let efficiency_weight = self
            .options
            .get("efficiency_weight")
            .and_then(|v| v.as_f64())
            .unwrap_or(0.0);
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
        let ma_period = self
            .options
            .get("ma_period")
            .and_then(|v| v.as_u64())
            .unwrap_or(20);
        let regression_r2_adjust = self
            .options
            .get("regression_r2_adjust")
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

            if lookback_trade_days == 0 {
                panic!("lookback_trade_days must > 0");
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

                    let kline = fetch_stock_kline(ticker, StockDividendAdjust::Forward).await?;
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
                        rule_send_warning(
                            rule_name,
                            &format!("[No Enough Data] {ticker}"),
                            date,
                            event_sender,
                        )
                        .await;
                        continue;
                    }

                    let momentum = calc_annualized_momentum(&prices, regression_r2_adjust);
                    let bias_momentum =
                        calc_ema_bias_momentum(&prices, ma_period as usize, regression_r2_adjust);
                    let efficiency_factor = calc_efficiency_factor(&prices);

                    if let Some(fail_factor_name) =
                        match (momentum, bias_momentum, efficiency_factor) {
                            (None, _, _) => Some("momentum"),
                            (_, None, _) => Some("bias_momentum"),
                            (_, _, None) => Some("efficiency_factor"),
                            (Some(momentum), Some(bias_momentum), Some(efficiency_factor)) => {
                                tickers_factors.push((
                                    ticker.clone(),
                                    Factors {
                                        momentum,
                                        bias_momentum,
                                        efficiency_momentum: efficiency_factor * momentum,
                                    },
                                ));

                                None
                            }
                        }
                    {
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

            rule_send_info(
                rule_name,
                &format!(
                    "[Universe] {}({})",
                    tickers_map.len(),
                    tickers_factors.len()
                ),
                date,
                event_sender,
            )
            .await;

            let normalized_factors_momentum = normalize_zscore(
                &tickers_factors
                    .iter()
                    .map(|(_, f)| f.momentum)
                    .collect::<Vec<f64>>(),
            );
            let normalized_factors_bias_momentum = normalize_zscore(
                &tickers_factors
                    .iter()
                    .map(|(_, f)| f.bias_momentum)
                    .collect::<Vec<f64>>(),
            );
            let normalized_factors_efficiency_momentum = normalize_zscore(
                &tickers_factors
                    .iter()
                    .map(|(_, f)| f.efficiency_momentum)
                    .collect::<Vec<f64>>(),
            );

            let mut indicators: Vec<(Ticker, f64)> = vec![];
            for (i, (ticker, _)) in tickers_factors.iter().enumerate() {
                let momentum = normalized_factors_momentum[i];
                let bias_momentum = normalized_factors_bias_momentum[i];
                let efficiency_momentum = normalized_factors_efficiency_momentum[i];

                let indicator = momentum
                    + bias_weight * bias_momentum
                    + efficiency_weight * efficiency_momentum;

                indicators.push((ticker.clone(), indicator));
            }
            indicators.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(Ordering::Equal));

            let targets_indicator = indicators
                .iter()
                .take(limit as usize)
                .map(|(t, v)| (t.clone(), *v))
                .collect::<Vec<_>>();

            rule_notify_indicators(
                rule_name,
                &targets_indicator
                    .iter()
                    .map(|&(ref t, v)| (t.clone(), format!("{v:.4}")))
                    .collect::<Vec<_>>(),
                &indicators
                    .iter()
                    .skip(limit as usize)
                    .take(CANDIDATE_TICKER_RATIO * limit as usize)
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
    momentum: f64,
    bias_momentum: f64,
    efficiency_momentum: f64,
}
