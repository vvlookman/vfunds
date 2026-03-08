use std::{collections::HashSet, str::FromStr};

use chrono::NaiveDate;
use serde_json::json;

use crate::{
    ds::tushare,
    error::VfResult,
    financial::stock::{StockDividendAdjust, fetch_stock_kline},
    ticker::Ticker,
    utils::datetime::date_from_str,
};

pub async fn fetch_trade_dates() -> VfResult<HashSet<NaiveDate>> {
    let mut dates: HashSet<NaiveDate> = HashSet::new();

    if let Ok(json) = tushare::call_api("trade_cal", &json!({"is_open": "1"}), None, 0, false).await
    {
        if let (Some(fields), Some(items)) = (
            json["data"]["fields"].as_array(),
            json["data"]["items"].as_array(),
        ) {
            if let Some(idx_cal_date) = fields.iter().position(|f| f == "cal_date") {
                for item in items {
                    if let Some(values) = item.as_array() {
                        if let Some(trade_date_str) = values[idx_cal_date].as_str() {
                            if let Ok(date) = date_from_str(trade_date_str) {
                                dates.insert(date);
                            }
                        }
                    }
                }
            }
        }
    } else {
        let bench_ticker = Ticker::from_str("000001.SH")?;
        let bench_kline = fetch_stock_kline(&bench_ticker, StockDividendAdjust::No).await?;
        let bench_dates = bench_kline.all_dates();
        dates.extend(bench_dates);
    }

    Ok(dates)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_fetch_trade_dates() {
        let trade_dates = fetch_trade_dates().await.unwrap();

        assert!(trade_dates.len() > 0);
    }
}
