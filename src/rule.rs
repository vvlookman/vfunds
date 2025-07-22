use async_trait::async_trait;
use chrono::NaiveDate;

use crate::{backtest::BacktestContext, error::VfResult, spec::RuleDefinition};

pub mod buy_equaly;

pub struct Rule {
    executor: Box<dyn RuleExecutor>,
    definition: RuleDefinition,
}

#[async_trait]
pub trait RuleExecutor {
    async fn exec(&mut self, context: &mut BacktestContext, date: &NaiveDate) -> VfResult<()>;
}

impl Rule {
    pub fn from_definition(definition: &RuleDefinition) -> Self {
        let executor: Box<dyn RuleExecutor> = match definition.name.as_str() {
            "buy_equaly" => Box::new(buy_equaly::Executor::new()),
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
    ) -> VfResult<()> {
        self.executor.exec(context, date).await
    }
}
