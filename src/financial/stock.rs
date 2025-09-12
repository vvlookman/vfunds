use std::{collections::HashMap, sync::LazyLock};

use dashmap::DashMap;
use serde_json::json;

use crate::{data::daily::*, ds::qmt, error::*, ticker::Ticker};

#[derive(Clone)]
pub struct StockDetail {
    pub title: String,
}

#[derive(strum::Display, strum::EnumString)]
#[allow(dead_code)]
pub enum StockDividendAdjust {
    Backward,
    BackwardProp,
    Forward,
    ForwardProp,
    No,
}

#[derive(strum::Display, strum::EnumString)]
#[strum(ascii_case_insensitive)]
pub enum StockDividendField {
    Interest,
    PriceAdjustmentFactor,
}

#[derive(strum::Display, strum::EnumString)]
#[strum(ascii_case_insensitive)]
pub enum StockKlineField {
    Open,
    Close,
    High,
    Low,
}

pub async fn fetch_stock_detail(ticker: &Ticker) -> VfResult<StockDetail> {
    let cache_key = format!("{ticker}");
    if let Some(result) = STOCK_DETAIL_CACHE.get(&cache_key) {
        return Ok(result.clone());
    }

    let json = qmt::call_api(
        &format!("/detail/{}", ticker.to_qmt_code()),
        &json!({}),
        true,
    )
    .await?;

    let title = json["InstrumentName"].as_str().unwrap_or_default();

    let result = StockDetail {
        title: title.to_string(),
    };
    STOCK_DETAIL_CACHE.insert(cache_key, result.clone());

    Ok(result)
}

pub async fn fetch_stock_dividends(ticker: &Ticker) -> VfResult<DailyDataset> {
    let cache_key = format!("{ticker}");
    if let Some(result) = STOCK_DIVIDENDS_CACHE.get(&cache_key) {
        return Ok(result.clone());
    }

    let json = qmt::call_api(
        &format!("/dividend/{}", ticker.to_qmt_code()),
        &json!({}),
        true,
    )
    .await?;

    let mut fields: HashMap<String, String> = HashMap::new();
    fields.insert(
        StockDividendField::Interest.to_string(),
        "interest".to_string(),
    );
    fields.insert(
        StockDividendField::PriceAdjustmentFactor.to_string(),
        "dr".to_string(),
    );

    let result = DailyDataset::from_json(&json, "date", &fields)?;
    STOCK_DIVIDENDS_CACHE.insert(cache_key, result.clone());

    Ok(result)
}

pub async fn fetch_stock_kline(
    ticker: &Ticker,
    adjust: StockDividendAdjust,
) -> VfResult<DailyDataset> {
    let cache_key = format!("{ticker}/{adjust}");
    if let Some(result) = STOCK_KLINE_CACHE.get(&cache_key) {
        return Ok(result.clone());
    }

    let param_dividend_type = match adjust {
        StockDividendAdjust::Backward => "back",
        StockDividendAdjust::BackwardProp => "back_ratio",
        StockDividendAdjust::Forward => "front",
        StockDividendAdjust::ForwardProp => "front_ratio",
        StockDividendAdjust::No => "none",
    };

    let json = qmt::call_api(
        &format!("/kline/{}", ticker.to_qmt_code()),
        &json!({
            "dividend_type": param_dividend_type,
        }),
        true,
    )
    .await?;

    let mut fields: HashMap<String, String> = HashMap::new();
    fields.insert(StockKlineField::Open.to_string(), "open".to_string());
    fields.insert(StockKlineField::Close.to_string(), "close".to_string());
    fields.insert(StockKlineField::High.to_string(), "high".to_string());
    fields.insert(StockKlineField::Low.to_string(), "low".to_string());

    let result = DailyDataset::from_json(&json, "date", &fields)?;
    STOCK_KLINE_CACHE.insert(cache_key, result.clone());

    Ok(result)
}

static STOCK_DETAIL_CACHE: LazyLock<DashMap<String, StockDetail>> = LazyLock::new(DashMap::new);
static STOCK_DIVIDENDS_CACHE: LazyLock<DashMap<String, DailyDataset>> = LazyLock::new(DashMap::new);
static STOCK_KLINE_CACHE: LazyLock<DashMap<String, DailyDataset>> = LazyLock::new(DashMap::new);

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use chrono::Local;

    use super::*;

    #[tokio::test]
    async fn test_fetch_stock_detail() {
        let ticker = Ticker::from_str("000001").unwrap();
        let detail = fetch_stock_detail(&ticker).await.unwrap();

        assert_eq!(detail.title, "平安银行");
    }

    #[tokio::test]
    async fn test_fetch_stock_dividends() {
        let ticker = Ticker::from_str("000001").unwrap();
        let dataset = fetch_stock_dividends(&ticker).await.unwrap();

        let data = dataset
            .get_latest_value::<f64>(
                &Local::now().date_naive(),
                &StockDividendField::PriceAdjustmentFactor.to_string(),
            )
            .unwrap();

        assert!(data > 1.0);
    }

    #[tokio::test]
    async fn test_fetch_stock_kline() {
        let ticker = Ticker::from_str("000001").unwrap();
        let dataset = fetch_stock_kline(&ticker, StockDividendAdjust::No)
            .await
            .unwrap();

        let data = dataset
            .get_latest_value::<f64>(
                &Local::now().date_naive(),
                &StockKlineField::Close.to_string(),
            )
            .unwrap();

        assert!(data > 0.0);
    }
}
