use chrono::NaiveDate;

use crate::{error::VfResult, financial::helper::calc_stock_market_cap, ticker::Ticker};

pub async fn is_circulating_ratio_low(
    ticker: &Ticker,
    date: &NaiveDate,
    threshold: f64,
) -> VfResult<bool> {
    let circulating = calc_stock_market_cap(ticker, date, true).await?;
    let total = calc_stock_market_cap(ticker, date, false).await?;

    Ok(circulating < total * threshold)
}
