use std::collections::HashMap;

use chrono::{Datelike, NaiveDate};
use regex::Regex;
use serde_json::json;

use crate::{
    data::{daily::*, stock::*},
    ds::aktools,
    error::*,
    ticker::Ticker,
    utils::datetime::*,
};

#[derive(strum::Display)]
pub enum StockValuationFieldName {
    Price,
    MarketCap,
    Pe,
    PeTtm,
    Peg,
    Pb,
    Pcf,
    Ps,
}

pub async fn fetch_stock_daily_valuations(ticker: &Ticker) -> VfResult<DailyDataset> {
    match ticker.exchange.as_str() {
        "SSE" | "SZSE" => {
            let json = aktools::call_public_api(
                "/stock_value_em",
                &json!({
                    "symbol": ticker.symbol,
                }),
            )
            .await?;

            let mut value_field_names: HashMap<String, String> = HashMap::new();
            value_field_names.insert(
                StockValuationFieldName::Price.to_string(),
                "当日收盘价".to_string(),
            );
            value_field_names.insert(
                StockValuationFieldName::MarketCap.to_string(),
                "总市值".to_string(),
            );
            value_field_names.insert(
                StockValuationFieldName::Pe.to_string(),
                "PE(静)".to_string(),
            );
            value_field_names.insert(
                StockValuationFieldName::PeTtm.to_string(),
                "PE(TTM)".to_string(),
            );
            value_field_names.insert(
                StockValuationFieldName::Peg.to_string(),
                "PEG值".to_string(),
            );
            value_field_names.insert(
                StockValuationFieldName::Pb.to_string(),
                "市净率".to_string(),
            );
            value_field_names.insert(
                StockValuationFieldName::Pcf.to_string(),
                "市现率".to_string(),
            );
            value_field_names.insert(
                StockValuationFieldName::Ps.to_string(),
                "市销率".to_string(),
            );

            DailyDataset::from_json(&json, "数据日期", &value_field_names)
        }
        "HKEX" => {
            let mut daily_values_map: HashMap<NaiveDate, HashMap<String, serde_json::Value>> =
                HashMap::new();

            {
                let json = aktools::call_public_api(
                    "/stock_hk_daily",
                    &json!({
                        "symbol": ticker.symbol,
                    }),
                )
                .await?;

                if let Some(array) = json.as_array() {
                    for item in array {
                        if let (Some(date_str), Some(close)) =
                            (item["date"].as_str(), item.get("close"))
                        {
                            if let Ok(date) = date_from_str(date_str) {
                                daily_values_map
                                    .entry(date)
                                    .or_default()
                                    .insert("当日收盘价".to_string(), close.clone());
                            }
                        }
                    }
                }
            }

            for indicator in ["总市值", "市盈率(TTM)", "市盈率(静)", "市净率", "市现率"]
            {
                let json = aktools::call_public_api(
                    "/stock_hk_valuation_baidu",
                    &json!({
                        "symbol": ticker.symbol,
                        "indicator": indicator,
                        "period": "全部",
                    }),
                )
                .await?;

                if let Some(array) = json.as_array() {
                    for item in array {
                        if let (Some(date_str), Some(value)) =
                            (item["date"].as_str(), item.get("value"))
                        {
                            if let Ok(date) = date_from_str(date_str) {
                                daily_values_map
                                    .entry(date)
                                    .or_default()
                                    .insert(indicator.to_string(), value.clone());
                            }
                        }
                    }
                }
            }

            let mut daily_values: Vec<serde_json::Map<String, serde_json::Value>> = vec![];
            for (date, values) in daily_values_map {
                let mut values_map = serde_json::Map::new();
                values_map.insert("date".to_string(), json!(date));
                for (indicator, value) in values {
                    values_map.insert(indicator, value);
                }

                daily_values.push(values_map);
            }

            let json = json!(daily_values);

            let mut value_field_names: HashMap<String, String> = HashMap::new();
            value_field_names.insert(
                StockValuationFieldName::Price.to_string(),
                "当日收盘价".to_string(),
            );
            value_field_names.insert(
                StockValuationFieldName::MarketCap.to_string(),
                "总市值".to_string(),
            );
            value_field_names.insert(
                StockValuationFieldName::Pe.to_string(),
                "市盈率(静)".to_string(),
            );
            value_field_names.insert(
                StockValuationFieldName::PeTtm.to_string(),
                "市盈率(TTM)".to_string(),
            );
            value_field_names.insert(
                StockValuationFieldName::Pb.to_string(),
                "市净率".to_string(),
            );
            value_field_names.insert(
                StockValuationFieldName::Pcf.to_string(),
                "市现率".to_string(),
            );

            DailyDataset::from_json(&json, "date", &value_field_names)
        }
        _ => Err(VfError::Invalid(
            "EXCHANGE_NOT_SUPPORTED",
            format!("Not yet supported exchange '{}'", ticker.exchange),
        )),
    }
}

pub async fn fetch_stock_dividends(
    ticker: &Ticker,
    date_start: &NaiveDate,
    date_end: &NaiveDate,
) -> VfResult<Vec<StockDividend>> {
    match ticker.exchange.as_str() {
        "SSE" | "SZSE" => {
            let mut result = vec![];

            {
                let json = aktools::call_public_api(
                    "/stock_fhps_detail_em",
                    &json!({
                        "symbol": ticker.symbol,
                    }),
                )
                .await?;

                if let Some(array) = json.as_array() {
                    for item in array {
                        let date_announce =
                            date_from_str(item["预案公告日"].as_str().unwrap_or_default());
                        let date_record =
                            date_from_str(item["股权登记日"].as_str().unwrap_or_default());
                        let dividend_per_share = item["每股收益"].as_f64();

                        if let (Ok(date_announce), Ok(date_record), Some(dividend_per_share)) =
                            (date_announce, date_record, dividend_per_share)
                        {
                            if date_announce >= *date_start && date_announce <= *date_end {
                                result.push(StockDividend {
                                    date_announce,
                                    date_record,
                                    dividend_per_share,
                                });
                            }
                        }
                    }
                }
            }

            Ok(result)
        }
        "HKEX" => {
            let mut result = vec![];

            {
                let symbol = ticker.symbol.clone();
                let json = aktools::call_public_api(
                    "/stock_hk_fhpx_detail_ths",
                    &json!({
                        "symbol": if let Some(stripped) = symbol.strip_prefix('0') { stripped } else { &symbol },
                    }),
                )
                .await?;

                if let Some(array) = json.as_array() {
                    if let Ok(re) = Regex::new(r"每股(\d+\.?\d*)港元") {
                        for item in array {
                            let date_announce =
                                date_from_str(item["公告日期"].as_str().unwrap_or_default());
                            let date_record =
                                date_from_str(item["除净日"].as_str().unwrap_or_default());
                            let plan = item["方案"].as_str().unwrap_or_default();

                            if let Some(caps) = re.captures(plan) {
                                if let Some(matched) = caps.get(1) {
                                    let dividend_per_share = matched.as_str().parse::<f64>().ok();
                                    if let (
                                        Ok(date_announce),
                                        Ok(date_record),
                                        Some(dividend_per_share),
                                    ) = (date_announce, date_record, dividend_per_share)
                                    {
                                        if date_announce >= *date_start
                                            && date_announce <= *date_end
                                        {
                                            result.push(StockDividend {
                                                date_announce,
                                                date_record,
                                                dividend_per_share,
                                            });
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }

            Ok(result)
        }
        _ => Err(VfError::Invalid(
            "EXCHANGE_NOT_SUPPORTED",
            format!("Not yet supported exchange '{}'", ticker.exchange),
        )),
    }
}

pub async fn fetch_stock_financial_summary(
    ticker: &Ticker,
    fiscal_quater: &FiscalQuarter,
) -> VfResult<StockFinancialSummary> {
    match ticker.exchange.as_str() {
        "SSE" | "SZSE" => {
            let mut result = StockFinancialSummary::default();

            {
                let json = aktools::call_public_api(
                    "/stock_financial_abstract",
                    &json!({
                        "symbol": ticker.symbol,
                    }),
                )
                .await?;

                let quarter_key = format!(
                    "{}{}",
                    fiscal_quater.year,
                    match fiscal_quater.quarter {
                        Quarter::Q1 => "0331",
                        Quarter::Q2 => "0630",
                        Quarter::Q3 => "0930",
                        Quarter::Q4 => "1231",
                    }
                );

                if let Some(array) = json.as_array() {
                    for item in array {
                        match item["指标"].as_str().unwrap_or_default() {
                            "总资产周转率" => {
                                result.asset_turnover = item[&quarter_key].as_f64();
                            }
                            "每股净资产" => {
                                result.book_value_per_share = item[&quarter_key].as_f64();
                            }
                            "现金比率" => {
                                result.cash_ratio = item[&quarter_key].as_f64();
                            }
                            "成本费用利润率" => {
                                result.cost_of_profit =
                                    item[&quarter_key].as_f64().map(|v| v / 100.0);
                            }
                            "成本费用率" => {
                                result.cost_of_revenue =
                                    item[&quarter_key].as_f64().map(|v| v / 100.0);
                            }
                            "销售成本率" => {
                                result.cost_of_sales =
                                    item[&quarter_key].as_f64().map(|v| v / 100.0);
                            }
                            "流动比率" => {
                                result.current_ratio = item[&quarter_key].as_f64();
                            }
                            "总资产周转天数" => {
                                result.days_asset_outstanding = item[&quarter_key].as_f64();
                            }
                            "存货周转天数" => {
                                result.days_inventory_outstanding = item[&quarter_key].as_f64();
                            }
                            "应收账款周转天数" => {
                                result.days_sales_outstanding = item[&quarter_key].as_f64();
                            }
                            "资产负债率" => {
                                result.debt_to_assets =
                                    item[&quarter_key].as_f64().map(|v| v / 100.0);
                            }
                            "产权比率" => {
                                result.debt_to_equity =
                                    item[&quarter_key].as_f64().map(|v| v / 100.0);
                            }
                            "基本每股收益" => {
                                result.earnings_per_share = item[&quarter_key].as_f64();
                            }
                            "每股现金流" => {
                                result.free_cash_flow_per_share = item[&quarter_key].as_f64();
                            }
                            "商誉" => {
                                result.goodwill = item[&quarter_key].as_f64();
                            }
                            "毛利率" => {
                                result.gross_margin =
                                    item[&quarter_key].as_f64().map(|v| v / 100.0);
                            }
                            "存货周转率" => {
                                result.inventory_turnover = item[&quarter_key].as_f64();
                            }
                            "股东权益合计(净资产)" => {
                                result.net_assets = item[&quarter_key].as_f64();
                            }
                            "销售净利率" => {
                                result.net_margin = item[&quarter_key].as_f64().map(|v| v / 100.0);
                            }
                            "净利润" => {
                                result.net_profit = item[&quarter_key].as_f64();
                            }
                            "经营现金流量净额" => {
                                result.operating_cash_flow = item[&quarter_key].as_f64();
                            }
                            "营业成本" => {
                                result.operating_costs = item[&quarter_key].as_f64();
                            }
                            "营业利润率" => {
                                result.operating_margin =
                                    item[&quarter_key].as_f64().map(|v| v / 100.0);
                            }
                            "营业总收入" => {
                                result.operating_revenue = item[&quarter_key].as_f64();
                            }
                            "速动比率" => {
                                result.quick_ratio = item[&quarter_key].as_f64();
                            }
                            "应收账款周转率" => {
                                result.receivables_turnover = item[&quarter_key].as_f64();
                            }
                            "总资产报酬率(ROA)" => {
                                result.return_on_assets =
                                    item[&quarter_key].as_f64().map(|v| v / 100.0);
                            }
                            "净资产收益率(ROE)" => {
                                result.return_on_equity =
                                    item[&quarter_key].as_f64().map(|v| v / 100.0);
                            }
                            "投入资本回报率" => {
                                result.return_on_invested_capital =
                                    item[&quarter_key].as_f64().map(|v| v / 100.0);
                            }
                            "营业总收入增长率" => {
                                result.revenue_growth =
                                    item[&quarter_key].as_f64().map(|v| v / 100.0);
                            }
                            _ => {}
                        }
                    }
                }
            }

            Ok(result)
        }
        "HKEX" => {
            let mut result = StockFinancialSummary::default();

            {
                let json = aktools::call_public_api(
                    "/stock_financial_hk_analysis_indicator_em",
                    &json!({
                        "symbol": ticker.symbol,
                        "indicator": "报告期",
                    }),
                )
                .await?;

                if let Some(array) = json.as_array() {
                    for item in array {
                        if let Some(report_date_str) = item["REPORT_DATE"].as_str() {
                            if let Ok(report_date) = date_from_str(report_date_str) {
                                let quarter = match report_date.month() {
                                    1..=3 => Quarter::Q1,
                                    4..=6 => Quarter::Q2,
                                    7..=9 => Quarter::Q3,
                                    10..=12 => Quarter::Q4,
                                    _ => unreachable!(),
                                };

                                if report_date.year() == fiscal_quater.year
                                    && quarter == fiscal_quater.quarter
                                {
                                    result.book_value_per_share = item["BPS"].as_f64();
                                    result.current_ratio =
                                        item["CURRENT_RATIO"].as_f64().map(|v| v / 100.0);
                                    result.debt_to_assets =
                                        item["DEBT_ASSET_RATIO"].as_f64().map(|v| v / 100.0);
                                    result.earnings_per_share = item["BASIC_EPS"].as_f64();
                                    result.free_cash_flow_per_share =
                                        item["PER_NETCASH_OPERATE"].as_f64();
                                    result.gross_margin =
                                        item["GROSS_PROFIT_RATIO"].as_f64().map(|v| v / 100.0);
                                    result.operating_revenue = item["OPERATE_INCOME"].as_f64();
                                    result.return_on_assets =
                                        item["ROA"].as_f64().map(|v| v / 100.0);
                                    result.return_on_equity =
                                        item["ROE_AVG"].as_f64().map(|v| v / 100.0);
                                    result.revenue_growth =
                                        item["OPERATE_INCOME_YOY"].as_f64().map(|v| v / 100.0);
                                }
                            }
                        }
                    }
                }
            }

            Ok(result)
        }
        _ => Err(VfError::Invalid(
            "EXCHANGE_NOT_SUPPORTED",
            format!("Not yet supported exchange '{}'", ticker.exchange),
        )),
    }
}

pub async fn fetch_stock_info(ticker: &Ticker) -> VfResult<StockInfo> {
    match ticker.exchange.as_str() {
        "SSE" | "SZSE" => {
            let mut result = StockInfo::default();

            {
                let json = aktools::call_public_api(
                    "/stock_individual_info_em",
                    &json!({
                        "symbol": ticker.symbol,
                    }),
                )
                .await?;

                if let Some(array) = json.as_array() {
                    for item in array {
                        match item["item"].as_str().unwrap_or_default() {
                            "股票简称" => {
                                result.name = item["value"].as_str().map(|v| v.to_string());
                            }
                            "行业" => {
                                result.industry = item["value"].as_str().map(|v| v.to_string());
                            }
                            _ => {}
                        }
                    }
                }
            }

            Ok(result)
        }
        "HKEX" => {
            let mut result = StockInfo::default();

            {
                let json = aktools::call_public_api(
                    "/stock_hk_company_profile_em",
                    &json!({
                        "symbol": ticker.symbol,
                    }),
                )
                .await?;

                if let Some(array) = json.as_array() {
                    if let Some(item) = array.first() {
                        result.name = item["股票简称"].as_str().map(|v| v.to_string());
                        result.industry = item["所属行业"].as_str().map(|v| v.to_string());
                    }
                }
            }

            Ok(result)
        }
        _ => Err(VfError::Invalid(
            "EXCHANGE_NOT_SUPPORTED",
            format!("Not yet supported exchange '{}'", ticker.exchange),
        )),
    }
}
