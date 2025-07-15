use std::collections::HashMap;

use serde_json::json;

use crate::{data::daily::*, ds::aktools, error::*, ticker::Ticker};

#[derive(strum::Display)]
pub enum StockValuationFieldName {
    Price,
}

pub async fn fetch_stock_daily_backward_adjust(ticker: &Ticker) -> VfResult<DailyDataset> {
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
            value_field_names.insert(
                StockValuationFieldName::Price.to_string(),
                "收盘".to_string(),
            );

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
            value_field_names.insert(
                StockValuationFieldName::Price.to_string(),
                "收盘".to_string(),
            );

            DailyDataset::from_json(&json, "日期", &value_field_names)
        }
        _ => Err(VfError::Invalid(
            "EXCHANGE_NOT_SUPPORTED",
            format!("Not yet supported exchange '{}'", ticker.exchange),
        )),
    }
}
