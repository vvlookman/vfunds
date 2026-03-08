use chrono::NaiveDate;

use crate::{error::VfResult, financial::stock::fetch_delisted_stocks, ticker::Ticker};

pub async fn is_delisted(ticker: &Ticker, date: &NaiveDate) -> VfResult<bool> {
    let delisted_tickers = fetch_delisted_stocks().await?;

    if let Some(delist_date) = delisted_tickers.get(ticker) {
        Ok(date >= delist_date)
    } else {
        Ok(false)
    }
}
