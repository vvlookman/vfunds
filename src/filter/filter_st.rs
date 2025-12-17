use chrono::NaiveDate;

use crate::{error::VfResult, financial::stock::fetch_st_stocks, ticker::Ticker};

pub async fn is_st(ticker: &Ticker, date: &NaiveDate, lookback_days: u64) -> VfResult<bool> {
    let st_tickers = fetch_st_stocks(date, lookback_days).await?;
    Ok(st_tickers.contains(ticker))
}
