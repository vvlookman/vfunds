use std::{collections::HashMap, sync::LazyLock};

use chrono::{Duration, NaiveDate};
use dashmap::DashMap;
use serde_json::{Value, json};

use crate::{
    data::series::DailySeries,
    ds::tushare,
    error::VfResult,
    ticker::{Ticker, TickersIndex},
    utils::datetime::{date_from_str, date_to_str},
};

#[derive(strum::Display, strum::EnumString)]
#[strum(ascii_case_insensitive)]
pub enum IndexIndicatorField {
    Pb,
    Pe,
    PeTtm,
    TurnoverRate,
}

/// Currently only supports:
/// - 000001.SH 上证综指
/// - 399001.SZ 深证成指
/// - 000016.SH 上证50
/// - 000905.SH 中证500
/// - 399005.SZ 中小板指
/// - 399006.SZ 创业板指
pub async fn fetch_index_indicators(index: &TickersIndex) -> VfResult<DailySeries> {
    let cache_key = format!("{index}");
    if let Some(result) = INDEX_INDICATORS_CACHE.get(&cache_key) {
        return Ok(result.clone());
    }

    static PAGE_SIZE: usize = 2000;

    let mut fields: Vec<Value> = vec![];
    let mut items: Vec<Value> = vec![];

    let mut offset: usize = 0;
    while items.len() == offset {
        let json = tushare::call_api(
            "index_dailybasic",
            &json!({
                "ts_code": index.to_tushare_code(),
                "limit": PAGE_SIZE,
                "offset": offset,
            }),
            None,
            0,
        )
        .await?;

        if let Some(page_fields) = json["data"]["fields"].as_array() {
            fields = page_fields.clone();
        }

        if let Some(page_items) = json["data"]["items"].as_array() {
            items.extend_from_slice(page_items);
        }

        offset += PAGE_SIZE;
    }

    let json = json!({
        "data": {
            "fields": fields,
            "items": items,
        }
    });

    let mut fields: HashMap<String, String> = HashMap::new();
    fields.insert(IndexIndicatorField::Pb.to_string(), "pb".to_string());
    fields.insert(IndexIndicatorField::Pe.to_string(), "pe".to_string());
    fields.insert(IndexIndicatorField::PeTtm.to_string(), "pe_ttm".to_string());
    fields.insert(
        IndexIndicatorField::TurnoverRate.to_string(),
        "turnover_rate".to_string(),
    );

    let result = DailySeries::from_tushare_json(&json, "trade_date", &fields)?;
    INDEX_INDICATORS_CACHE.insert(cache_key, result.clone());

    Ok(result)
}

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

static INDEX_INDICATORS_CACHE: LazyLock<DashMap<String, DailySeries>> = LazyLock::new(DashMap::new);
static INDEX_TICKERS_CACHE: LazyLock<DashMap<String, Vec<Ticker>>> = LazyLock::new(DashMap::new);

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;

    #[tokio::test]
    async fn test_fetch_index_indicators() {
        let index = TickersIndex::from_str("000001.SH").unwrap();
        let series = fetch_index_indicators(&index).await.unwrap();

        assert!(series.get_dates().len() > 3000);
    }
}
