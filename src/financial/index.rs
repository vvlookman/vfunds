use std::str::FromStr;

use chrono::NaiveDate;
use serde_json::json;

use crate::{ds::aktools, error::VfResult, ticker::Ticker};

pub async fn fetch_cnindex_tickers(symbol: &str, date: &NaiveDate) -> VfResult<Vec<Ticker>> {
    let json = aktools::call_api(
        "/index_detail_cni",
        &json!({
            "symbol": symbol,
            "date": date.format("%Y%m").to_string(),
        }),
        true,
        None,
    )
    .await?;

    let mut tickers: Vec<Ticker> = vec![];

    if let Some(array) = json.as_array() {
        for item in array {
            if let Some(obj) = item.as_object() {
                if let Some(ticker_str) = obj["样本代码"].as_str() {
                    if let Ok(ticker) = Ticker::from_str(ticker_str) {
                        tickers.push(ticker);
                    }
                }
            }
        }
    }

    Ok(tickers)
}
