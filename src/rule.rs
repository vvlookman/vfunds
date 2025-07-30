use async_trait::async_trait;
use chrono::NaiveDate;
use tokio::sync::mpsc::Sender;

use crate::{
    backtest::{BacktestContext, BacktestEvent},
    error::VfResult,
    spec::RuleDefinition,
};

pub mod hold_all_equal;
pub mod hold_topn_equal;

pub struct Rule {
    executor: Box<dyn RuleExecutor>,
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
    pub fn from_definition(definition: &RuleDefinition) -> Self {
        let executor: Box<dyn RuleExecutor> = match definition.name.as_str() {
            "hold_all_equal" => Box::new(hold_all_equal::Executor::new(definition)),
            "hold_topn_equal" => Box::new(hold_topn_equal::Executor::new(definition)),
            _ => panic!("Unsupported rule: {}", definition.name),
        };

        Self { executor }
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
