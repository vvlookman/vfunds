use std::{collections::HashMap, f64};

use async_trait::async_trait;
use chrono::NaiveDate;
use tokio::sync::mpsc::Sender;

use crate::{
    TRADE_DAYS_FRACTION,
    error::VfResult,
    financial::{
        KlineField, PriceType,
        stock::{StockDividendAdjust, fetch_stock_kline},
    },
    rule::{BacktestEvent, FundBacktestContext, RuleDefinition, RuleExecutor, rule_send_info},
    spec::Frequency,
    ticker::Ticker,
    utils::financial::calc_annualized_return_rate,
};

pub struct Executor {
    frequency: Frequency,
    #[allow(dead_code)]
    options: HashMap<String, serde_json::Value>,
}

impl Executor {
    pub fn new(definition: &RuleDefinition) -> Self {
        Self {
            frequency: definition.frequency.clone(),
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

        let deviation_threshold = self
            .options
            .get("deviation_threshold")
            .and_then(|v| v.as_f64())
            .unwrap_or(0.1);
        let lookback_periods = self
            .options
            .get("lookback_periods")
            .and_then(|v| v.as_u64())
            .unwrap_or(1);
        let rotation_deviation_ratio = self
            .options
            .get("rotation_deviation_ratio")
            .and_then(|v| v.as_f64())
            .unwrap_or(0.1);
        let rotation_ratio = self
            .options
            .get("rotation_ratio")
            .and_then(|v| v.as_f64())
            .unwrap_or(0.1);

        let lookback_trade_days =
            ((lookback_periods * self.frequency.to_days()) as f64 * TRADE_DAYS_FRACTION) as u32;

        let tickers_map = context.fund_definition.all_tickers_map(date).await?;
        if !tickers_map.is_empty() {
            let mut tickers_arr: HashMap<Ticker, f64> = HashMap::new();
            {
                for ticker in tickers_map.keys() {
                    let kline = fetch_stock_kline(&ticker, StockDividendAdjust::Backward).await?;
                    let prices: Vec<f64> = kline
                        .get_latest_values::<f64>(
                            date,
                            false,
                            &KlineField::Close.to_string(),
                            lookback_trade_days + 1,
                        )
                        .iter()
                        .map(|&(_, v)| v)
                        .collect();
                    if let Some(arr) = calc_annualized_return_rate(&prices) {
                        tickers_arr.insert(ticker.clone(), arr);
                    }
                }
            }

            let max_arr = tickers_arr
                .values()
                .max_by(|a, b| a.total_cmp(b))
                .copied()
                .unwrap_or(f64::NAN);
            let min_arr = tickers_arr
                .values()
                .min_by(|a, b| a.total_cmp(b))
                .copied()
                .unwrap_or(f64::NAN);

            let deviation = max_arr - min_arr;
            if deviation > deviation_threshold {
                rule_send_info(
                    rule_name,
                    &format!("[Deviation Exceeded] {:.2}%", deviation * 100.0),
                    date,
                    event_sender,
                )
                .await;
            }

            let targets_weight: Vec<(Ticker, f64)> = if deviation > deviation_threshold {
                let rotation_tickers: Vec<Ticker> = tickers_arr
                    .iter()
                    .filter(|&(_, arr)| *arr < min_arr + deviation * rotation_deviation_ratio)
                    .map(|(ticker, _)| ticker.clone())
                    .collect();

                let total_value = context.calc_total_value(date, &PriceType::Open).await?;
                let rotation_value = total_value * rotation_ratio;
                let ticker_base_value = (total_value - rotation_value) / tickers_map.len() as f64;
                let ticker_rotation_value = rotation_value / rotation_tickers.len() as f64;

                let mut targets_weight: Vec<(Ticker, f64)> = vec![];
                for ticker in tickers_map.keys() {
                    if rotation_tickers.contains(ticker) {
                        targets_weight
                            .push((ticker.clone(), ticker_base_value + ticker_rotation_value));
                    } else {
                        targets_weight.push((ticker.clone(), ticker_base_value));
                    }
                }

                targets_weight
            } else {
                tickers_map
                    .iter()
                    .map(|(ticker, (weight, _))| (ticker.clone(), *weight))
                    .collect()
            };

            context
                .rebalance(&targets_weight, date, event_sender)
                .await?;
        }

        Ok(())
    }
}
