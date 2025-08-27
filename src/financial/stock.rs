use std::collections::HashMap;

use serde_json::json;

use crate::{data::daily::*, ds::aktools, error::*, ticker::Ticker};

#[allow(dead_code)]
pub enum StockAdjust {
    No,
    Backward,
    Forward,
}

pub struct StockDetail {
    pub title: String,
}

#[derive(strum::Display, strum::EnumString)]
#[strum(ascii_case_insensitive)]
pub enum StockField {
    DividendRatio,
    PriceOpen,
    PriceClose,
    PriceHigh,
    PriceLow,
}

pub async fn fetch_stock_daily_price(
    ticker: &Ticker,
    adjust: StockAdjust,
) -> VfResult<DailyDataset> {
    match ticker.exchange.as_str() {
        "SSE" | "SZSE" => {
            let adjust_param = match adjust {
                StockAdjust::No => "",
                StockAdjust::Backward => "hfq",
                StockAdjust::Forward => "qfq",
            };

            let json = aktools::call_public_api(
                "/stock_zh_a_hist",
                &json!({
                    "symbol": ticker.symbol,
                    "adjust": adjust_param,
                }),
                false,
            )
            .await?;

            let mut value_field_names: HashMap<String, String> = HashMap::new();
            value_field_names.insert(StockField::PriceOpen.to_string(), "开盘".to_string());
            value_field_names.insert(StockField::PriceClose.to_string(), "收盘".to_string());
            value_field_names.insert(StockField::PriceHigh.to_string(), "最高".to_string());
            value_field_names.insert(StockField::PriceLow.to_string(), "最低".to_string());

            DailyDataset::from_json(&json, "日期", &value_field_names)
        }
        "HKEX" => {
            let adjust_param = match adjust {
                StockAdjust::No => "",
                StockAdjust::Backward => "hfq",
                StockAdjust::Forward => "qfq",
            };

            let json = aktools::call_public_api(
                "/stock_hk_hist",
                &json!({
                    "symbol": ticker.symbol,
                    "adjust": adjust_param,
                }),
                false,
            )
            .await?;

            let mut value_field_names: HashMap<String, String> = HashMap::new();
            value_field_names.insert(StockField::PriceClose.to_string(), "收盘".to_string());

            DailyDataset::from_json(&json, "日期", &value_field_names)
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

pub async fn fetch_stock_dividends(ticker: &Ticker) -> VfResult<DailyDataset> {
    match ticker.exchange.as_str() {
        "SSE" | "SZSE" => {
            let json = aktools::call_public_api(
                "/stock_fhps_detail_em",
                &json!({
                    "symbol": ticker.symbol,
                }),
                false,
            )
            .await?;

            let mut value_field_names: HashMap<String, String> = HashMap::new();
            value_field_names.insert(
                StockField::DividendRatio.to_string(),
                "现金分红-股息率".to_string(),
            );

            DailyDataset::from_json(&json, "预案公告日", &value_field_names)
        }
        _ => Err(VfError::Invalid(
            "UNSUPPORTED_EXCHANGE",
            format!("Unsupported exchange '{}'", ticker.exchange),
        )),
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use chrono::Local;

    use super::*;

    #[tokio::test]
    async fn test_fetch_stock_daily_price() {
        let ticker = Ticker::from_str("000001").unwrap();
        let dataset = fetch_stock_daily_price(&ticker, StockAdjust::No)
            .await
            .unwrap();

        let data = dataset
            .get_latest_value::<f64>(
                &Local::now().date_naive(),
                &StockField::PriceClose.to_string(),
            )
            .unwrap();

        assert!(data > 0.0);
    }

    #[tokio::test]
    async fn test_fetch_stock_detail() {
        let ticker = Ticker::from_str("000001").unwrap();
        let detail = fetch_stock_detail(&ticker).await.unwrap();

        assert_eq!(detail.title, "中国平安");
    }

    #[tokio::test]
    async fn test_fetch_stock_dividends() {
        let ticker = Ticker::from_str("000001").unwrap();
        let dataset = fetch_stock_dividends(&ticker).await.unwrap();

        let data = dataset
            .get_latest_value::<f64>(
                &Local::now().date_naive(),
                &StockField::DividendRatio.to_string(),
            )
            .unwrap();

        assert!(data > 0.0);
    }
}
