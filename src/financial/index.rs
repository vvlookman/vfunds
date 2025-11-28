use std::{collections::HashMap, sync::LazyLock};

use chrono::{Duration, NaiveDate};
use dashmap::DashMap;
use serde_json::json;

use crate::{
    ds::tushare,
    error::VfResult,
    ticker::{Ticker, TickersIndex},
    utils::datetime::{date_from_str, date_to_str},
};

pub async fn fetch_index_tickers(index: &TickersIndex, date: &NaiveDate) -> VfResult<Vec<Ticker>> {
    let prev_date = *date - Duration::days(1);

    let cache_key = format!("{index}/{}", date_to_str(&prev_date));
    if let Some(result) = INDEX_TICKERS_CACHE.get(&cache_key) {
        return Ok(result.clone());
    }

    let json = tushare::call_api(
        "index_weight",
        &json!({
            "index_code": index.to_tushare_code(),
            "end_date": prev_date.format("%Y%m%d").to_string(),
        }),
        None,
        30,
    )
    .await?;

    let mut hist_tickers: HashMap<NaiveDate, Vec<Ticker>> = HashMap::new();

    if let (Some(fields), Some(items)) = (
        json["data"]["fields"].as_array(),
        json["data"]["items"].as_array(),
    ) {
        if let (Some(idx_con_code), Some(idx_trade_date)) = (
            fields.iter().position(|f| f == "con_code"),
            fields.iter().position(|f| f == "trade_date"),
        ) {
            for item in items {
                if let Some(values) = item.as_array() {
                    if let (Some(con_code_str), Some(trade_date_str)) = (
                        values[idx_con_code].as_str(),
                        values[idx_trade_date].as_str(),
                    ) {
                        if let (Ok(date), Some(ticker)) = (
                            date_from_str(trade_date_str),
                            Ticker::from_tushare_str(con_code_str),
                        ) {
                            hist_tickers.entry(date).or_default().push(ticker);
                        }
                    }
                }
            }
        }
    }

    let tickers = if let Some(latest_date) = hist_tickers.keys().max() {
        hist_tickers.get(latest_date).unwrap_or(&vec![]).clone()
    } else {
        vec![]
    };

    INDEX_TICKERS_CACHE.insert(cache_key, tickers.clone());

    Ok(tickers)
}

static INDEX_TICKERS_CACHE: LazyLock<DashMap<String, Vec<Ticker>>> = LazyLock::new(DashMap::new);
