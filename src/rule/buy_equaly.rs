use std::str::FromStr;

use async_trait::async_trait;
use chrono::NaiveDate;
use log::debug;

use crate::{
    backtest::calc_buy_fee,
    error::VfResult,
    financial::{get_stock_daily_backward_adjust, stock::StockValuationField},
    rule::{BacktestContext, RuleExecutor},
    ticker::Ticker,
    utils,
};

pub struct Executor {
    executed_date: Option<NaiveDate>,
}

impl Executor {
    pub fn new() -> Self {
        Self {
            executed_date: None,
        }
    }
}

#[async_trait]
impl RuleExecutor for Executor {
    async fn exec(&mut self, context: &mut BacktestContext, date: &NaiveDate) -> VfResult<()> {
        let date_str = utils::datetime::date_to_str(date);
        let ticker_strs: Vec<_> = context
            .fund_definition
            .tickers
            .iter()
            .filter(|t| !context.portfolio.positions.contains_key(*t))
            .collect();
        if !ticker_strs.is_empty() {
            let buy_limit = context.portfolio.cash * (1.0 - context.options.broker_commission_rate)
                / ticker_strs.len() as f64;

            for ticker_str in ticker_strs {
                let ticker = Ticker::from_str(ticker_str)?;

                let stock_daily = get_stock_daily_backward_adjust(&ticker).await?;
                if let Some(price) = stock_daily
                    .get_latest_value::<f64>(date, &StockValuationField::Price.to_string())
                {
                    let units = (buy_limit / price).floor();
                    if units > 0.0 {
                        let value = units * price;
                        let fee = calc_buy_fee(value, context.options);
                        let cost = value + fee;

                        context.portfolio.cash -= cost;
                        context
                            .portfolio
                            .positions
                            .insert(ticker_str.to_string(), units as u64);

                        debug!("[+][{date_str}] {ticker} {price:.2}x{units} -> {cost:.2}");
                    }
                }
            }
        }

        Ok(())
    }
}
