use chrono::NaiveDate;

use crate::{
    error::{VfError, VfResult},
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
            false,
            &StockIndicatorField::MarketValueCirculating.to_string(),
        ),
        stock_indicators.get_latest_value::<f64>(
            date,
            false,
            &StockIndicatorField::MarketValueTotal.to_string(),
        ),
    ) {
        Ok(circulating < total * threshold)
    } else {
        Err(VfError::NoData {
            code: "NO_STOCK_INDICATORS_DATA",
            message: format!("Indicators of '{ticker}' not exists"),
        })
    }
}
