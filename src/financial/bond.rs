use std::{collections::HashMap, sync::LazyLock};

use chrono::{Months, NaiveDate};
use dashmap::DashMap;
use serde_json::{Value, json};

use crate::{
    data::series::*,
    ds::tushare,
    error::*,
    ticker::Ticker,
    utils::datetime::{date_from_str, date_to_str},
};

#[derive(strum::Display, strum::EnumString)]
#[strum(ascii_case_insensitive)]
pub enum ConvBondDailyField {
    Open,
    Close,
    High,
    Low,
    Volume,
    StraightValue,
    ConversionValue,
    StraightPremium,
    ConversionPremium,
}

#[derive(Clone, Debug)]
#[allow(dead_code)]
pub struct ConvBondDetail {
    pub ticker: Ticker,
    pub title: String,
    pub issue_size: Option<f64>,
    pub par_value: Option<f64>,
    pub expire_date: Option<NaiveDate>,
}

#[derive(Clone, Debug)]
#[allow(dead_code)]
pub struct ConvBondIssue {
    pub ticker: Ticker,
    pub title: String,
    pub issue_size: Option<f64>,
}

pub async fn fetch_conv_bond_daily(ticker: &Ticker) -> VfResult<DailySeries> {
    let cache_key = format!("{ticker}");
    if let Some(result) = CONV_BOND_DAILY_CACHE.get(&cache_key) {
        return Ok(result.clone());
    }

    let json = tushare::call_api(
        "cb_daily",
        &json!({
            "ts_code": ticker.to_tushare_code(),
        }),
        Some("trade_date,open,close,high,low,vol,bond_value,cb_value,bond_over_rate,cb_over_rate"),
        0,
    )
    .await?;

    let mut fields: HashMap<String, String> = HashMap::new();
    fields.insert(ConvBondDailyField::Open.to_string(), "open".to_string());
    fields.insert(ConvBondDailyField::Close.to_string(), "close".to_string());
    fields.insert(ConvBondDailyField::High.to_string(), "high".to_string());
    fields.insert(ConvBondDailyField::Low.to_string(), "low".to_string());
    fields.insert(ConvBondDailyField::Volume.to_string(), "vol".to_string());
    fields.insert(
        ConvBondDailyField::StraightValue.to_string(),
        "bond_value".to_string(),
    );
    fields.insert(
        ConvBondDailyField::ConversionValue.to_string(),
        "cb_value".to_string(),
    );
    fields.insert(
        ConvBondDailyField::StraightPremium.to_string(),
        "bond_over_rate".to_string(),
    );
    fields.insert(
        ConvBondDailyField::ConversionPremium.to_string(),
        "cb_over_rate".to_string(),
    );

    let result = DailySeries::from_tushare_json(&json, "trade_date", &fields)?;
    CONV_BOND_DAILY_CACHE.insert(cache_key, result.clone());

    Ok(result)
}

pub async fn fetch_conv_bond_detail(ticker: &Ticker) -> VfResult<ConvBondDetail> {
    let cache_key = format!("{ticker}");
    if let Some(result) = CONV_BOND_DETAIL_CACHE.get(&cache_key) {
        return Ok(result.clone());
    }

    let json = tushare::call_api(
        "cb_basic",
        &json!({
            "ts_code": ticker.to_tushare_code(),
        }),
        None,
        30,
    )
    .await?;

    if let (Some(fields), Some(items)) = (
        json["data"]["fields"].as_array(),
        json["data"]["items"].as_array(),
    ) {
        if let Some(item) = items.first() {
            if let Some(values) = item.as_array() {
                let mut json_item: HashMap<String, Value> = HashMap::new();

                for (i, field) in fields.iter().enumerate() {
                    if let Some(field_name) = field.as_str() {
                        if let Some(value) = values.get(i) {
                            json_item.insert(field_name.to_string(), value.clone());
                        }
                    }
                }

                if let Some(ticker) =
                    Ticker::from_tushare_str(json_item["ts_code"].as_str().unwrap_or_default())
                {
                    let result = ConvBondDetail {
                        ticker,
                        title: json_item["bond_short_name"]
                            .as_str()
                            .unwrap_or_default()
                            .to_string(),
                        issue_size: json_item["issue_size"].as_f64(),
                        par_value: json_item["par"].as_f64(),
                        expire_date: json_item["delist_date"]
                            .as_str()
                            .and_then(|s| date_from_str(s).ok()),
                    };

                    CONV_BOND_DETAIL_CACHE.insert(cache_key, result.clone());

                    return Ok(result);
                }
            }
        }
    }

    Err(VfError::Invalid {
        code: "INVALID_JSON",
        message: "Invalid Tushare JSON".to_string(),
    })
}

pub async fn fetch_conv_bonds(
    date: &NaiveDate,
    lookback_months: u32,
) -> VfResult<Vec<ConvBondIssue>> {
    let cache_key = format!("{}/{lookback_months}", date_to_str(date));
    if let Some(result) = CONV_BONDS_CACHE.get(&cache_key) {
        return Ok(result.clone());
    }

    let json = tushare::call_api(
        "cb_issue",
        &json!({
            "start_date": (*date - Months::new(lookback_months)).format("%Y%m%d").to_string(),
            "end_date": date.format("%Y%m%d").to_string(),
        }),
        None,
        30,
    )
    .await?;

    let mut result = vec![];

    if let (Some(fields), Some(items)) = (
        json["data"]["fields"].as_array(),
        json["data"]["items"].as_array(),
    ) {
        for item in items {
            if let Some(values) = item.as_array() {
                let mut json_item: HashMap<String, Value> = HashMap::new();

                for (i, field) in fields.iter().enumerate() {
                    if let Some(field_name) = field.as_str() {
                        if let Some(value) = values.get(i) {
                            json_item.insert(field_name.to_string(), value.clone());
                        }
                    }
                }

                if let Some(ticker) =
                    Ticker::from_tushare_str(json_item["ts_code"].as_str().unwrap_or_default())
                {
                    let cb = ConvBondIssue {
                        ticker,
                        title: json_item["onl_name"]
                            .as_str()
                            .unwrap_or_default()
                            .to_string(),
                        issue_size: json_item["issue_size"].as_f64(),
                    };

                    result.push(cb);
                }
            }
        }
    }

    CONV_BONDS_CACHE.insert(cache_key, result.clone());

    Ok(result)
}

static CONV_BOND_DAILY_CACHE: LazyLock<DashMap<String, DailySeries>> = LazyLock::new(DashMap::new);
static CONV_BOND_DETAIL_CACHE: LazyLock<DashMap<String, ConvBondDetail>> =
    LazyLock::new(DashMap::new);
static CONV_BONDS_CACHE: LazyLock<DashMap<String, Vec<ConvBondIssue>>> =
    LazyLock::new(DashMap::new);

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;
    use crate::utils::datetime::date_from_str;

    #[tokio::test]
    async fn test_fetch_conv_bond_daily() {
        let ticker = Ticker::from_str("110098").unwrap();
        let series = fetch_conv_bond_daily(&ticker).await.unwrap();

        let (_, data) = series
            .get_latest_value::<f64>(
                &date_from_str("2025-08-08").unwrap(),
                false,
                &ConvBondDailyField::ConversionPremium.to_string(),
            )
            .unwrap();

        assert!(data > 0.0);
    }

    #[tokio::test]
    async fn test_fetch_conv_bond_detail() {
        let ticker = Ticker::from_str("110098").unwrap();
        let detail = fetch_conv_bond_detail(&ticker).await.unwrap();

        assert_eq!(detail.ticker.symbol, "110098");
        assert_eq!(detail.title, "南药转债");
    }

    #[tokio::test]
    async fn test_fetch_conv_bonds() {
        let date = date_from_str("2025-08-08").unwrap();
        let bonds = fetch_conv_bonds(&date, 12).await.unwrap();

        assert!(bonds.len() > 0);
    }
}
