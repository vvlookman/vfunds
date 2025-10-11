use std::collections::HashMap;

use async_trait::async_trait;
use chrono::NaiveDate;
use tokio::sync::mpsc::Sender;

use crate::{
    error::VfResult,
    rule::{BacktestContext, BacktestEvent, RuleDefinition, RuleExecutor},
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
        context: &mut BacktestContext,
        date: &NaiveDate,
        event_sender: Sender<BacktestEvent>,
    ) -> VfResult<()> {
        let tickers_map = context.fund_definition.all_tickers_map(date).await?;
        if !tickers_map.is_empty() {
            let targets_weight: Vec<(Ticker, f64)> = tickers_map
                .iter()
                .map(|(ticker, (weight, _))| (ticker.clone(), *weight))
                .collect();

            context
                .rebalance(&targets_weight, date, event_sender)
                .await?;
        }

        Ok(())
    }
}
