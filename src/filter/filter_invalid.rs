use chrono::NaiveDate;

use crate::{
    STALE_DAYS_SHORT,
    error::VfResult,
    financial::{KlineField, get_ticker_kline},
    ticker::Ticker,
};

pub async fn has_invalid_price(ticker: &Ticker, date: &NaiveDate) -> VfResult<bool> {
    let kline = get_ticker_kline(ticker, false).await?;

    if let Some((_, val)) =
        kline.get_latest_value::<f64>(date, STALE_DAYS_SHORT, true, &KlineField::Open.to_string())
    {
        if val <= 0.0 {
            return Ok(true);
        }
    }

    if let Some((_, val)) =
        kline.get_latest_value::<f64>(date, STALE_DAYS_SHORT, true, &KlineField::Close.to_string())
    {
        if val <= 0.0 {
            return Ok(true);
        }
    }

    if let Some((_, val)) =
        kline.get_latest_value::<f64>(date, STALE_DAYS_SHORT, true, &KlineField::High.to_string())
    {
        if val <= 0.0 {
            return Ok(true);
        }
    }

    if let Some((_, val)) =
        kline.get_latest_value::<f64>(date, STALE_DAYS_SHORT, true, &KlineField::Low.to_string())
    {
        if val <= 0.0 {
            return Ok(true);
        }
    }

    Ok(false)
}
