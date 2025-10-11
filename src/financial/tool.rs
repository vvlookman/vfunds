use std::{collections::HashSet, str::FromStr};

use chrono::NaiveDate;
use serde_json::json;

use crate::{
    ds::aktools,
    error::VfResult,
    financial::stock::{StockDividendAdjust, fetch_stock_kline},
    ticker::Ticker,
    utils::datetime,
};

pub async fn fetch_trade_dates() -> VfResult<HashSet<NaiveDate>> {
    let mut dates: HashSet<NaiveDate> = HashSet::new();

    if let Ok(json) = aktools::call_api("/tool_trade_date_hist_sina", &json!({}), None, None).await
    {
        if let Some(array) = json.as_array() {
            for item in array {
                if let Some(obj) = item.as_object() {
                    if let Some(trade_date_str) = obj["trade_date"].as_str() {
                        if let Ok(date) = datetime::date_from_str(trade_date_str) {
                            dates.insert(date);
                        }
                    }
                }
            }
        }
    } else {
        let bench_ticker = Ticker::from_str("000001.SH")?;
        let bench_kline = fetch_stock_kline(&bench_ticker, StockDividendAdjust::No).await?;
        let bench_dates = bench_kline.get_dates();
        dates.extend(bench_dates);
    }

    Ok(dates)
}
