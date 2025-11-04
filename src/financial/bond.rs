use std::{collections::HashMap, sync::LazyLock};

use chrono::{Months, NaiveDate};
use dashmap::DashMap;
use serde_json::json;

use crate::{
    data::daily::DailyDataset,
    ds::aktools,
    error::*,
    financial::KlineField,
    ticker::Ticker,
    utils::datetime::{date_from_str, date_to_str},
};

#[derive(Clone)]
#[allow(dead_code)]
pub struct ConvBondDetail {
    pub code: String,
    pub title: String,
    pub issue_size: Option<f64>, // 单位为亿
    pub face_value: Option<f64>,
    pub expire_date: Option<NaiveDate>,
}

#[derive(strum::Display, strum::EnumString)]
#[strum(ascii_case_insensitive)]
pub enum ConvBondAnalysisField {
    Price,
    StraightValue,
    ConversionValue,
    StraightPremium,
    ConversionPremium,
}

pub async fn fetch_conv_bond_analysis(ticker: &Ticker) -> VfResult<DailyDataset> {
    let cache_key = format!("{ticker}");
    if let Some(result) = CONV_BOND_ANALYSIS_CACHE.get(&cache_key) {
        return Ok(result.clone());
    }

    let json = aktools::call_api(
        "/bond_zh_cov_value_analysis",
        &json!({
            "symbol": ticker.symbol,
        }),
        0,
        None,
    )
    .await?;

    let mut fields: HashMap<String, String> = HashMap::new();
    fields.insert(
        ConvBondAnalysisField::Price.to_string(),
        "收盘价".to_string(),
    );
    fields.insert(
        ConvBondAnalysisField::StraightValue.to_string(),
        "纯债价值".to_string(),
    );
    fields.insert(
        ConvBondAnalysisField::ConversionValue.to_string(),
        "转股价值".to_string(),
    );
    fields.insert(
        ConvBondAnalysisField::StraightPremium.to_string(),
        "纯债溢价率".to_string(),
    );
    fields.insert(
        ConvBondAnalysisField::ConversionPremium.to_string(),
        "转股溢价率".to_string(),
    );

    let result = DailyDataset::from_json(&json, "日期", &fields)?;
    CONV_BOND_ANALYSIS_CACHE.insert(cache_key, result.clone());

    Ok(result)
}

pub async fn fetch_conv_bond_detail(ticker: &Ticker) -> VfResult<ConvBondDetail> {
    let cache_key = format!("{ticker}");
    if let Some(result) = CONV_BOND_DETAIL_CACHE.get(&cache_key) {
        return Ok(result.clone());
    }

    let json = aktools::call_api(
        "/bond_zh_cov_info",
        &json!({
            "symbol": ticker.symbol,
        }),
        30,
        None,
    )
    .await?;

    let obj = &json[0];

    let result = ConvBondDetail {
        code: obj["SECURITY_CODE"]
            .as_str()
            .unwrap_or_default()
            .to_string(),
        title: obj["SECURITY_NAME_ABBR"]
            .as_str()
            .unwrap_or_default()
            .to_string(),
        issue_size: obj["ACTUAL_ISSUE_SCALE"].as_f64(),
        face_value: obj["PAR_VALUE"].as_f64(),
        expire_date: obj["EXPIRE_DATE"]
            .as_str()
            .and_then(|s| date_from_str(s).ok()),
    };

    CONV_BOND_DETAIL_CACHE.insert(cache_key, result.clone());

    Ok(result)
}

pub async fn fetch_conv_bond_kline(ticker: &Ticker) -> VfResult<DailyDataset> {
    let cache_key = format!("{ticker}");
    if let Some(result) = CONV_BOND_KLINE_CACHE.get(&cache_key) {
        return Ok(result.clone());
    }

    let json = aktools::call_api(
        "/bond_zh_hs_cov_daily",
        &json!({
            "symbol": ticker.to_sina_code(),
        }),
        0,
        None,
    )
    .await?;

    let mut fields: HashMap<String, String> = HashMap::new();
    fields.insert(KlineField::Open.to_string(), "open".to_string());
    fields.insert(KlineField::Close.to_string(), "close".to_string());
    fields.insert(KlineField::High.to_string(), "high".to_string());
    fields.insert(KlineField::Low.to_string(), "low".to_string());
    fields.insert(KlineField::Volume.to_string(), "volume".to_string());

    let result = DailyDataset::from_json(&json, "date", &fields)?;
    CONV_BOND_KLINE_CACHE.insert(cache_key, result.clone());

    Ok(result)
}

pub async fn fetch_conv_bonds(
    date: &NaiveDate,
    lookback_months: u32,
) -> VfResult<Vec<ConvBondDetail>> {
    let cache_key = format!("{}/{lookback_months}", date_to_str(date));
    if let Some(result) = CONV_BONDS_CACHE.get(&cache_key) {
        return Ok(result.clone());
    }

    let json = aktools::call_api(
        "/bond_cov_issue_cninfo",
        &json!({
            "start_date": (*date - Months::new(lookback_months)).format("%Y%m%d").to_string(),
            "end_date": date.format("%Y%m%d").to_string(),
        }),
        30,
        None,
    )
    .await?;

    let mut result = vec![];

    if let Some(array) = json.as_array() {
        for item in array {
            if let Some(obj) = item.as_object() {
                result.push(ConvBondDetail {
                    code: obj["债券代码"]
                        .as_str()
                        .map(|s| s.to_string())
                        .unwrap_or_default(),
                    title: obj["债券简称"]
                        .as_str()
                        .map(|s| s.to_string())
                        .unwrap_or_default(),
                    issue_size: obj["实际发行总量"].as_f64().map(|v| v / 10000.0),
                    face_value: obj["发行面值"].as_f64(),
                    expire_date: obj["债权登记日"]
                        .as_str()
                        .and_then(|s| date_from_str(s).ok()),
                });
            }
        }
    }

    CONV_BONDS_CACHE.insert(cache_key, result.clone());

    Ok(result)
}

static CONV_BOND_ANALYSIS_CACHE: LazyLock<DashMap<String, DailyDataset>> =
    LazyLock::new(DashMap::new);
static CONV_BOND_DETAIL_CACHE: LazyLock<DashMap<String, ConvBondDetail>> =
    LazyLock::new(DashMap::new);
static CONV_BOND_KLINE_CACHE: LazyLock<DashMap<String, DailyDataset>> = LazyLock::new(DashMap::new);
static CONV_BONDS_CACHE: LazyLock<DashMap<String, Vec<ConvBondDetail>>> =
    LazyLock::new(DashMap::new);

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use chrono::Local;

    use super::*;
    use crate::utils::datetime::date_from_str;

    #[tokio::test]
    async fn test_fetch_conv_bond_analysis() {
        let ticker = Ticker::from_str("110098").unwrap();
        let dataset = fetch_conv_bond_analysis(&ticker).await.unwrap();

        let (_, data) = dataset
            .get_latest_value::<f64>(
                &date_from_str("2025-08-08").unwrap(),
                false,
                &ConvBondAnalysisField::ConversionPremium.to_string(),
            )
            .unwrap();

        assert!(data > 0.0);
    }

    #[tokio::test]
    async fn test_fetch_conv_bond_detail() {
        let ticker = Ticker::from_str("110098").unwrap();
        let detail = fetch_conv_bond_detail(&ticker).await.unwrap();

        assert_eq!(detail.code, "110098");
        assert_eq!(detail.title, "南药转债");
    }

    #[tokio::test]
    async fn test_fetch_conv_bond_kline() {
        let ticker = Ticker::from_str("110098").unwrap();
        let dataset = fetch_conv_bond_kline(&ticker).await.unwrap();

        let (_, data) = dataset
            .get_latest_value::<f64>(
                &Local::now().date_naive(),
                false,
                &KlineField::Close.to_string(),
            )
            .unwrap();

        assert!(data > 0.0);
    }

    #[tokio::test]
    async fn test_fetch_conv_bonds() {
        let date = date_from_str("2025-08-08").unwrap();
        let bonds = fetch_conv_bonds(&date, 12).await.unwrap();

        assert!(bonds.len() > 0);
    }
}
