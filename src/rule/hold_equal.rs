use std::collections::HashMap;

use async_trait::async_trait;
use chrono::NaiveDate;
use tokio::sync::mpsc::Sender;

use crate::{
    error::VfResult,
    rule::{BacktestContext, BacktestEvent, RuleDefinition, RuleExecutor},
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
        context: &mut BacktestContext,
        date: &NaiveDate,
        event_sender: Sender<BacktestEvent>,
    ) -> VfResult<()> {
        let rule_name = mod_name!();

        let tickers = context.fund_definition.all_tickers(date).await?;
        if !tickers.is_empty() {
            let date_str = date_to_str(date);

            let total_value = context.calc_total_value(date).await?;
            let ticker_value = total_value / tickers.len() as f64;
            let target: Vec<(Ticker, f64)> = tickers
                .into_iter()
                .map(|ticker| (ticker, ticker_value))
                .collect();

            let target_str = target
                .iter()
                .map(|(ticker, ticker_value)| format!("{ticker}->{ticker_value:.2}"))
                .collect::<Vec<_>>()
                .join(" ");
            let _ = event_sender
                .send(BacktestEvent::Info(format!(
                    "[{date_str}] [{rule_name}] Rebalance ({target_str})"
                )))
                .await;

            context.rebalance(&target, date, event_sender).await?;
        }

        Ok(())
    }
}
