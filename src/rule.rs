use async_trait::async_trait;
use chrono::NaiveDate;

use crate::{backtest::BacktestContext, error::VfResult, spec::RuleDefinition};

pub mod hold_all_equal;
pub mod hold_topn_equal;

pub struct Rule {
    executor: Box<dyn RuleExecutor>,
}

#[async_trait]
pub trait RuleExecutor {
    async fn exec(&mut self, context: &mut BacktestContext, date: &NaiveDate) -> VfResult<()>;
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
    ) -> VfResult<()> {
        self.executor.exec(context, date).await
    }
}
