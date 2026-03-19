use std::collections::HashMap;

use async_trait::async_trait;
use chrono::NaiveDate;
use tokio::sync::mpsc::Sender;

use crate::{
    error::VfResult,
    financial::{PriceType, get_ticker_price},
    rule::{BacktestEvent, FundBacktestContext, RuleDefinition, RuleExecutor, rule_send_info},
    ticker::Ticker,
    utils::stats,
};

pub struct Executor {
    #[allow(dead_code)]
    options: HashMap<String, serde_json::Value>,

    targets_weight: Option<Vec<(Ticker, f64)>>,
}

impl Executor {
    pub fn new(definition: &RuleDefinition) -> Self {
        Self {
            options: definition.options.clone(),

            targets_weight: None,
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

        let drift_threshold = self
            .options
            .get("drift_threshold")
            .and_then(|v| v.as_f64())
            .unwrap_or(0.01);

        if let Some(targets_weight) = self.targets_weight.clone() {
            let mut current_values: HashMap<Ticker, f64> = HashMap::new();
            {
                for (ticker, units) in context.portfolio.positions.clone() {
                    if let Some(price) =
                        get_ticker_price(&ticker, date, true, &PriceType::Close).await?
                    {
                        current_values.insert(ticker, price * units as f64);
                    }
                }

                for (ticker, (cash, _)) in context.portfolio.reserved_cash.clone() {
                    current_values.insert(ticker, cash);
                }
            }

            let total_value: f64 = current_values.values().sum();
            let current_weights: HashMap<Ticker, f64> = current_values
                .iter()
                .map(|(ticker, value)| (ticker.clone(), *value / total_value))
                .collect();

            let mut drifts: Vec<f64> = vec![];
            for (ticker, target_weight) in &targets_weight {
                if let Some(current_weight) = current_weights.get(ticker) {
                    let drift = ((current_weight - target_weight) / target_weight).abs();
                    drifts.push(drift);
                }
            }

            if let Some(drift_mean) = stats::mean(&drifts) {
                if drift_mean > drift_threshold {
                    rule_send_info(
                        rule_name,
                        &format!("[Drift Exceeded] {:.2}%", drift_mean * 100.0),
                        date,
                        event_sender,
                    )
                    .await;

                    context
                        .rebalance(&targets_weight, date, event_sender)
                        .await?;
                }
            }
        } else {
            let tickers_map = context.fund_definition.all_tickers_map(date).await?;
            if !tickers_map.is_empty() {
                let targets_weight: Vec<(Ticker, f64)> = tickers_map
                    .iter()
                    .map(|(ticker, (weight, _))| (ticker.clone(), *weight))
                    .collect();
                let targets_weight_sum: f64 = targets_weight.iter().map(|(_, weight)| weight).sum();

                self.targets_weight = Some(
                    targets_weight
                        .iter()
                        .map(|(ticker, weight)| (ticker.clone(), *weight / targets_weight_sum))
                        .collect(),
                );
            }
        }

        Ok(())
    }
}
