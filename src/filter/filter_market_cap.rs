use chrono::NaiveDate;

use crate::{
    STALE_DAYS_LONG,
    error::VfResult,
    financial::stock::{StockIndicatorField, fetch_stock_indicators},
    ticker::Ticker,
};

pub async fn is_circulating_ratio_low(
    ticker: &Ticker,
    date: &NaiveDate,
    threshold: f64,
) -> VfResult<bool> {
    let stock_indicators = fetch_stock_indicators(ticker).await?;

    if let (Some((_, circulating)), Some((_, total))) = (
        stock_indicators.get_latest_value::<f64>(
            date,
            STALE_DAYS_LONG,
            false,
            &StockIndicatorField::MarketValueCirculating.to_string(),
        ),
        stock_indicators.get_latest_value::<f64>(
            date,
            STALE_DAYS_LONG,
            false,
            &StockIndicatorField::MarketValueTotal.to_string(),
        ),
    ) {
        return Ok(circulating < total * threshold);
    }

    Ok(false)
}
