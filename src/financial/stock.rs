use std::{collections::HashMap, sync::LazyLock};

use chrono::NaiveDate;
use dashmap::DashMap;
use serde_json::json;

use crate::{
    data::daily::*,
    ds::qmt,
    error::*,
    financial::{KlineField, sector::fetch_sector_tickers},
    ticker::Ticker,
    utils::datetime::date_from_str,
};

#[derive(Clone)]
#[allow(dead_code)]
pub struct StockDetail {
    pub title: String,
    pub sector: Option<String>,
    pub trading_date: Option<NaiveDate>,
    pub expire_date: Option<NaiveDate>,
    pub pre_close_price: Option<f64>,
    pub float_volume: Option<u64>,
    pub total_volume: Option<u64>,
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
pub enum StockReportCapitalField {
    ReportDate,
    Total,
    Circulating,
    FreeFloat,
}

#[derive(strum::Display, strum::EnumString)]
#[strum(ascii_case_insensitive)]
pub enum StockReportIncomeField {
    ReportDate,
    Revenue,
    OperatingProfit,
    TotalProfit,
}

#[derive(strum::Display, strum::EnumString)]
#[strum(ascii_case_insensitive)]
pub enum StockReportPershareField {
    ReportDate,
    Bps,
    Cfps,
    Eps,
    GrossProfitRate,
    NetProfitRate,
    RoeRate,
}

pub async fn fetch_stock_detail(ticker: &Ticker) -> VfResult<StockDetail> {
    let cache_key = format!("{ticker}");
    if let Some(result) = STOCK_DETAIL_CACHE.get(&cache_key) {
        return Ok(result.clone());
    }

    let json = qmt::call_api(
        &format!("/stock_detail/{}", ticker.to_qmt_code()),
        &json!({}),
        Some(30),
    )
    .await?;

    let tickers_sector_map = fetch_sector_tickers("SW1").await?;

    let result = StockDetail {
        title: json["InstrumentName"]
            .as_str()
            .unwrap_or_default()
            .to_string(),
        sector: tickers_sector_map.get(ticker).map(|s| s.to_string()),
        trading_date: json["TradingDay"]
            .as_str()
            .and_then(|s| date_from_str(s).ok()),
        expire_date: json["ExpireDate"]
            .as_str()
            .and_then(|s| date_from_str(s).ok()),
        pre_close_price: json["PreClose"].as_f64(),
        float_volume: json["FloatVolume"].as_u64(),
        total_volume: json["TotalVolume"].as_u64(),
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
        &format!("/stock_dividend/{}", ticker.to_qmt_code()),
        &json!({}),
        None,
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
        &format!("/stock_kline/{}", ticker.to_qmt_code()),
        &json!({
            "dividend_type": param_dividend_type,
        }),
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
    STOCK_KLINE_CACHE.insert(cache_key, result.clone());

    Ok(result)
}

pub async fn fetch_stock_report_capital(ticker: &Ticker) -> VfResult<DailyDataset> {
    let cache_key = format!("{ticker}");
    if let Some(result) = STOCK_REPORT_CAPITAL_CACHE.get(&cache_key) {
        return Ok(result.clone());
    }

    let json = qmt::call_api(
        &format!("/stock_report/{}", ticker.to_qmt_code()),
        &json!({
            "table": "Capital",
        }),
        None,
    )
    .await?;

    let mut fields: HashMap<String, String> = HashMap::new();
    fields.insert(
        StockReportCapitalField::ReportDate.to_string(),
        "m_timetag".to_string(),
    );
    fields.insert(
        StockReportCapitalField::Total.to_string(),
        "total_capital".to_string(),
    );
    fields.insert(
        StockReportCapitalField::Circulating.to_string(),
        "circulating_capital".to_string(),
    );
    fields.insert(
        StockReportCapitalField::FreeFloat.to_string(),
        "freeFloatCapital".to_string(),
    );

    let result = DailyDataset::from_json(&json, "date", &fields)?;
    STOCK_REPORT_CAPITAL_CACHE.insert(cache_key, result.clone());

    Ok(result)
}

pub async fn fetch_stock_report_income(ticker: &Ticker) -> VfResult<DailyDataset> {
    let cache_key = format!("{ticker}");
    if let Some(result) = STOCK_REPORT_INCOME_CACHE.get(&cache_key) {
        return Ok(result.clone());
    }

    let json = qmt::call_api(
        &format!("/stock_report/{}", ticker.to_qmt_code()),
        &json!({
            "table": "Income",
        }),
        None,
    )
    .await?;

    let mut fields: HashMap<String, String> = HashMap::new();
    fields.insert(
        StockReportIncomeField::ReportDate.to_string(),
        "m_timetag".to_string(),
    );
    fields.insert(
        StockReportIncomeField::Revenue.to_string(),
        "revenue".to_string(),
    );
    fields.insert(
        StockReportIncomeField::OperatingProfit.to_string(),
        "oper_profit".to_string(),
    );
    fields.insert(
        StockReportIncomeField::TotalProfit.to_string(),
        "tot_profit".to_string(),
    );

    let result = DailyDataset::from_json(&json, "date", &fields)?;
    STOCK_REPORT_INCOME_CACHE.insert(cache_key, result.clone());

    Ok(result)
}

pub async fn fetch_stock_report_pershare(ticker: &Ticker) -> VfResult<DailyDataset> {
    let cache_key = format!("{ticker}");
    if let Some(result) = STOCK_REPORT_PERSHARE_CACHE.get(&cache_key) {
        return Ok(result.clone());
    }

    let json = qmt::call_api(
        &format!("/stock_report/{}", ticker.to_qmt_code()),
        &json!({
            "table": "PershareIndex",
        }),
        None,
    )
    .await?;

    let mut fields: HashMap<String, String> = HashMap::new();
    fields.insert(
        StockReportPershareField::ReportDate.to_string(),
        "m_timetag".to_string(),
    );
    fields.insert(
        StockReportPershareField::Bps.to_string(),
        "s_fa_bps".to_string(),
    );
    fields.insert(
        StockReportPershareField::Cfps.to_string(),
        "s_fa_ocfps".to_string(),
    );
    fields.insert(
        StockReportPershareField::Eps.to_string(),
        "s_fa_eps_basic".to_string(),
    );
    fields.insert(
        StockReportPershareField::GrossProfitRate.to_string(),
        "gross_profit".to_string(),
    );
    fields.insert(
        StockReportPershareField::NetProfitRate.to_string(),
        "net_profit".to_string(),
    );
    fields.insert(
        StockReportPershareField::RoeRate.to_string(),
        "equity_roe".to_string(),
    );

    let result = DailyDataset::from_json(&json, "date", &fields)?;
    STOCK_REPORT_PERSHARE_CACHE.insert(cache_key, result.clone());

    Ok(result)
}

static STOCK_DETAIL_CACHE: LazyLock<DashMap<String, StockDetail>> = LazyLock::new(DashMap::new);
static STOCK_DIVIDENDS_CACHE: LazyLock<DashMap<String, DailyDataset>> = LazyLock::new(DashMap::new);
static STOCK_KLINE_CACHE: LazyLock<DashMap<String, DailyDataset>> = LazyLock::new(DashMap::new);
static STOCK_REPORT_CAPITAL_CACHE: LazyLock<DashMap<String, DailyDataset>> =
    LazyLock::new(DashMap::new);
static STOCK_REPORT_INCOME_CACHE: LazyLock<DashMap<String, DailyDataset>> =
    LazyLock::new(DashMap::new);
static STOCK_REPORT_PERSHARE_CACHE: LazyLock<DashMap<String, DailyDataset>> =
    LazyLock::new(DashMap::new);

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
        assert_eq!(&detail.sector.unwrap(), "银行");
    }

    #[tokio::test]
    async fn test_fetch_stock_dividends() {
        let ticker = Ticker::from_str("000001").unwrap();
        let dataset = fetch_stock_dividends(&ticker).await.unwrap();

        let (_, data) = dataset
            .get_latest_value::<f64>(
                &Local::now().date_naive(),
                false,
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

        let (_, data) = dataset
            .get_latest_value::<f64>(
                &Local::now().date_naive(),
                false,
                &KlineField::Close.to_string(),
            )
            .unwrap();

        assert!(data > 0.0);
    }
}
