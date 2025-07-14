use std::{collections::HashMap, str::FromStr};

use chrono::NaiveDate;
use log::debug;

use crate::{
    FundDefinition,
    error::*,
    financial::{get_stock_daily_valuations, stock::StockValuationFieldName},
    ticker::Ticker,
    utils::datetime,
};

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
    let mut cash = options.init_cash;
    let mut positions: HashMap<String, u64> = HashMap::new();

    let days = (options.end_date - options.start_date).num_days() + 1;

    for date in options.start_date.iter_days().take(days as usize) {
        let date_str = datetime::date_to_str(&date);

        for once_signal in fund_definition
            .signals
            .iter()
            .filter(|s| s.frequency.to_lowercase() == "once")
        {
            match once_signal.name.to_lowercase().as_str() {
                "buy-equaly" => {
                    let buy_tickers: Vec<_> = fund_definition
                        .tickers
                        .iter()
                        .filter(|t| !positions.contains_key(*t))
                        .collect();
                    if !buy_tickers.is_empty() {
                        let buy_limit = cash / buy_tickers.len() as f64;

                        for buy_ticker in buy_tickers {
                            let ticker = Ticker::from_str(buy_ticker)?;

                            let daily_valuations = get_stock_daily_valuations(&ticker).await?;
                            if let Some(price) = daily_valuations.get_latest_value::<f64>(
                                &date,
                                &StockValuationFieldName::Price.to_string(),
                            ) {
                                let amount = (buy_limit / price).floor();
                                let cost = amount * price;

                                cash -= cost;
                                positions.insert(ticker.to_string(), amount as u64);

                                debug!(
                                    "[+][{}] {} {:.2}x{}={:.2}",
                                    date_str,
                                    ticker.to_string(),
                                    price,
                                    amount,
                                    cost
                                );
                            }
                        }
                    }
                }
                _ => todo!(),
            }
        }
    }

    Ok(BacktestResult {
        days,
        total_return: 0.0,
        annual_return: 0.0,
    })
}
