use chrono::NaiveDate;

use crate::error::VfResult;

pub trait RuleExecutor: Send {
    fn exec(&mut self, date: &NaiveDate) -> VfResult<()>;
}
