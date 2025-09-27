use async_trait::async_trait;
use chrono::NaiveDate;
use tokio::sync::mpsc::Sender;

use crate::{
    backtest::{BacktestContext, BacktestEvent},
    error::VfResult,
    spec::RuleDefinition,
};

pub mod hold_dividend_topn_equal;
pub mod hold_equal;
pub mod hold_risk_parity;
pub mod sizing_by_macd_crossover;

pub struct Rule {
    executor: Box<dyn RuleExecutor>,
    definition: RuleDefinition,
}

#[async_trait]
pub trait RuleExecutor: Send {
    async fn exec(
        &mut self,
        context: &mut BacktestContext,
        date: &NaiveDate,
        event_sender: Sender<BacktestEvent>,
    ) -> VfResult<()>;
}

impl Rule {
    pub fn definition(&self) -> &RuleDefinition {
        &self.definition
    }

    pub fn from_definition(definition: &RuleDefinition) -> Self {
        let executor: Box<dyn RuleExecutor> = match definition.name.as_str() {
            "hold_equal" => Box::new(hold_equal::Executor::new(definition)),
            "hold_dividend_topn_equal" => {
                Box::new(hold_dividend_topn_equal::Executor::new(definition))
            }
            "hold_risk_parity" => Box::new(hold_risk_parity::Executor::new(definition)),
            "sizing_by_macd_crossover" => {
                Box::new(sizing_by_macd_crossover::Executor::new(definition))
            }
            _ => panic!("Unsupported rule: {}", definition.name),
        };

        Self {
            executor,
            definition: definition.clone(),
        }
    }

    pub async fn exec(
        &mut self,
        context: &mut BacktestContext<'_>,
        date: &NaiveDate,
        event_sender: Sender<BacktestEvent>,
    ) -> VfResult<()> {
        self.executor.exec(context, date, event_sender).await
    }
}
