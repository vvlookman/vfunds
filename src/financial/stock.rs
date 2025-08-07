use std::collections::HashMap;

use serde_json::json;

use crate::{data::daily::*, ds::aktools, error::*, ticker::Ticker};

pub struct StockDetail {
    pub title: String,
}

#[derive(strum::Display, strum::EnumString)]
#[strum(ascii_case_insensitive)]
pub enum StockField {
    Price,
    Dividend,
    DividendTtm,
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
                false,
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
                false,
            )
            .await?;

            let mut value_field_names: HashMap<String, String> = HashMap::new();
            value_field_names.insert(StockField::Price.to_string(), "收盘".to_string());

            DailyDataset::from_json(&json, "日期", &value_field_names)
        }
        _ => Err(VfError::Invalid(
            "UNSUPPORTED_EXCHANGE",
            format!("Unsupported exchange '{}'", ticker.exchange),
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
                false,
            )
            .await?;

            let mut value_field_names: HashMap<String, String> = HashMap::new();
            value_field_names.insert(StockField::Dividend.to_string(), "dv_ratio".to_string());
            value_field_names.insert(StockField::DividendTtm.to_string(), "dv_ttm".to_string());

            DailyDataset::from_json(&json, "trade_date", &value_field_names)
        }
        _ => Err(VfError::Invalid(
            "UNSUPPORTED_EXCHANGE",
            format!("Unsupported exchange '{}'", ticker.exchange),
        )),
    }
}

pub async fn fetch_stock_detail(ticker: &Ticker) -> VfResult<StockDetail> {
    match ticker.exchange.as_str() {
        "SSE" | "SZSE" => {
            let json = aktools::call_public_api(
                "/stock_individual_info_em",
                &json!({
                    "symbol": ticker.symbol,
                }),
                true,
            )
            .await?;

            if let Some(array) = json.as_array() {
                for item in array {
                    if item["item"].as_str() == Some("股票简称") {
                        if let Some(value) = item["value"].as_str() {
                            return Ok(StockDetail {
                                title: value.to_string(),
                            });
                        }

                        break;
                    }
                }
            }

            Err(VfError::NoData(
                "NO_STOCK_DETAIL",
                format!("Detail of stock '{ticker}' not exists"),
            ))
        }
        _ => Err(VfError::Invalid(
            "UNSUPPORTED_EXCHANGE",
            format!("Unsupported exchange '{}'", ticker.exchange),
        )),
    }
}
