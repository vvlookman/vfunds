use chrono::NaiveDate;

use crate::{FundDefinition, error::*};

pub struct BacktestOptions {
    pub init_cash: f64,
    pub start_date: NaiveDate,
    pub end_date: NaiveDate,
}

pub struct BacktestResult {
    pub days: i64,
    pub total_return: f64,
    pub annual_return: f64,
}

pub async fn run_fund(
    fund_definition: &FundDefinition,
    options: &BacktestOptions,
) -> VfResult<BacktestResult> {
    let days = (options.end_date - options.start_date).num_days() + 1;

    Ok(BacktestResult {
        days,
        total_return: 0.0,
        annual_return: 0.0,
    })
}
