use std::{collections::HashMap, f64};

use async_trait::async_trait;
use chrono::NaiveDate;
use tokio::sync::mpsc::Sender;

use crate::{
    error::VfResult,
    financial::{
        KlineField,
        stock::{StockDividendAdjust, fetch_stock_kline},
    },
    rule::{BacktestEvent, FundBacktestContext, RuleDefinition, RuleExecutor, rule_send_info},
    ticker::Ticker,
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

        let deviation_threshold = self
            .options
            .get("deviation_threshold")
            .and_then(|v| v.as_f64())
            .unwrap_or(0.01);
        let lookback_trade_days = self
            .options
            .get("lookback_trade_days")
            .and_then(|v| v.as_u64())
            .unwrap_or(5);
        let rotation_ratio = self
            .options
            .get("rotation_ratio")
            .and_then(|v| v.as_f64())
            .unwrap_or(0.1);

        if !context.portfolio.positions.is_empty() {
            let mut tickers_change: HashMap<Ticker, f64> = HashMap::new();
            let mut total_value: f64 = 0.0;
            {
                for (ticker, units) in context.portfolio.positions.clone() {
                    let kline = fetch_stock_kline(&ticker, StockDividendAdjust::Backward).await?;
                    let prices: Vec<f64> = kline
                        .get_latest_values::<f64>(
                            date,
                            false,
                            &KlineField::Close.to_string(),
                            lookback_trade_days as u32 + 1,
                        )
                        .iter()
                        .map(|&(_, v)| v)
                        .collect();
                    if let Some(&first_price) = prices.first()
                        && let Some(&last_price) = prices.last()
                    {
                        tickers_change
                            .insert(ticker.clone(), (last_price - first_price) / first_price);

                        total_value += last_price * units as f64;
                    }
                }
            }

            let max_change = tickers_change
                .values()
                .max_by(|a, b| a.total_cmp(b))
                .unwrap_or(&f64::NAN);
            let min_change = tickers_change
                .values()
                .min_by(|a, b| a.total_cmp(b))
                .unwrap_or(&f64::NAN);
            let deviation = max_change - min_change;
            if deviation > deviation_threshold {
                let rotation_tickers: Vec<Ticker> = tickers_change
                    .iter()
                    .filter(|(_, change)| (*change - min_change) < deviation / 10.0)
                    .map(|(ticker, _)| ticker.clone())
                    .collect();
                if !rotation_tickers.is_empty() {
                    let rotation_value = total_value * rotation_ratio;
                    let ticker_base_value =
                        (total_value - rotation_value) / context.portfolio.positions.len() as f64;
                    let ticker_rotation_value = rotation_value / rotation_tickers.len() as f64;

                    let mut targets_weight: Vec<(Ticker, f64)> = vec![];
                    for (ticker, _) in context.portfolio.positions.iter() {
                        if rotation_tickers.contains(ticker) {
                            targets_weight
                                .push((ticker.clone(), ticker_base_value + ticker_rotation_value));
                        } else {
                            targets_weight.push((ticker.clone(), ticker_base_value));
                        }
                    }

                    rule_send_info(
                        rule_name,
                        &format!("[Deviation Exceeded] {:.2}%", deviation * 100.0),
                        date,
                        event_sender,
                    )
                    .await;

                    context
                        .rebalance(&targets_weight, date, event_sender)
                        .await?;
                }
            }
        }

        Ok(())
    }
}
