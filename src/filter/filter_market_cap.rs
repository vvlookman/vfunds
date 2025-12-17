use chrono::NaiveDate;

use crate::{
    error::{VfError, VfResult},
    financial::stock::{StockReportCapitalField, fetch_stock_report_capital},
    ticker::Ticker,
};

pub async fn is_circulating_ratio_low(
    ticker: &Ticker,
    date: &NaiveDate,
    threshold: f64,
) -> VfResult<bool> {
    let report_capital = fetch_stock_report_capital(ticker).await?;

    if let (Some((_, circulating)), Some((_, total))) = (
        report_capital.get_latest_value::<f64>(
            date,
            false,
            &StockReportCapitalField::Circulating.to_string(),
        ),
        report_capital.get_latest_value::<f64>(
            date,
            false,
            &StockReportCapitalField::Total.to_string(),
        ),
    ) {
        Ok(circulating < total * threshold)
    } else {
        Err(VfError::NoData {
            code: "NO_CAPITAL_DATA",
            message: format!("Captial data of '{ticker}' not exists"),
        })
    }
}
