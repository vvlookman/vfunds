use std::collections::HashMap;

use serde_json::json;

use crate::{data::daily::*, ds::aktools, error::*, ticker::Ticker};

#[derive(strum::Display, strum::EnumString)]
#[strum(ascii_case_insensitive)]
pub enum StockField {
    Price,

    Dv,

    #[strum(serialize = "dv_ttm")]
    DvTtm,
}

pub async fn fetch_stock_daily_backward_adjusted_price(ticker: &Ticker) -> VfResult<DailyDataset> {
    match ticker.exchange.as_str() {
        "SSE" | "SZSE" => {
            let json = aktools::call_public_api(
                "/stock_zh_a_hist",
                &json!({
                    "symbol": ticker.symbol,
                    "adjust": "hfq",
                }),
            )
            .await?;

            let mut value_field_names: HashMap<String, String> = HashMap::new();
            value_field_names.insert(StockField::Price.to_string(), "收盘".to_string());

            DailyDataset::from_json(&json, "日期", &value_field_names)
        }
        "HKEX" => {
            let json = aktools::call_public_api(
                "/stock_hk_hist",
                &json!({
                    "symbol": ticker.symbol,
                    "adjust": "hfq",
                }),
            )
            .await?;

            let mut value_field_names: HashMap<String, String> = HashMap::new();
            value_field_names.insert(StockField::Price.to_string(), "收盘".to_string());

            DailyDataset::from_json(&json, "日期", &value_field_names)
        }
        _ => Err(VfError::Invalid(
            "EXCHANGE_NOT_SUPPORTED",
            format!("Not yet supported exchange '{}'", ticker.exchange),
        )),
    }
}

pub async fn fetch_stock_daily_indicators(ticker: &Ticker) -> VfResult<DailyDataset> {
    match ticker.exchange.as_str() {
        "SSE" | "SZSE" => {
            let json = aktools::call_public_api(
                "/stock_a_indicator_lg",
                &json!({
                    "symbol": ticker.symbol,
                }),
            )
            .await?;

            let mut value_field_names: HashMap<String, String> = HashMap::new();
            value_field_names.insert(StockField::Dv.to_string(), "dv_ratio".to_string());
            value_field_names.insert(StockField::DvTtm.to_string(), "dv_ttm".to_string());

            DailyDataset::from_json(&json, "trade_date", &value_field_names)
        }
        _ => Err(VfError::Invalid(
            "EXCHANGE_NOT_SUPPORTED",
            format!("Not yet supported exchange '{}'", ticker.exchange),
        )),
    }
}
