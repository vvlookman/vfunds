use std::{collections::HashMap, sync::LazyLock};

use chrono::{Days, NaiveDate};
use dashmap::DashMap;
use serde_json::{Value, json};

use crate::{
    data::series::*,
    ds::{qmt, tushare},
    error::*,
    financial::{KlineField, sector::fetch_sector_tickers},
    ticker::Ticker,
    utils::datetime::{date_from_str, date_to_str},
};

#[derive(Clone, Debug)]
#[allow(dead_code)]
pub struct StockBasic {
    pub ticker: Ticker,
    pub name: String,
    pub industry: String,
    pub list_date: Option<NaiveDate>,
    pub delist_date: Option<NaiveDate>,
}

#[derive(Clone, Debug)]
#[allow(dead_code)]
pub struct StockDetail {
    pub name: String,
    pub sector: Option<String>,
    pub trading_date: Option<NaiveDate>,
    pub expire_date: Option<NaiveDate>,
    pub pre_close_price: Option<f64>,
    pub float_volume: Option<u64>,
    pub total_volume: Option<u64>,
}

#[derive(Clone, Debug, strum::Display, strum::EnumString)]
#[allow(dead_code)]
pub enum StockDividendAdjust {
    Backward,
    Forward,
    No,
}

#[derive(Clone, Debug, strum::Display, strum::EnumString)]
#[strum(ascii_case_insensitive)]
pub enum StockDividendField {
    Interest,
    StockBonus,
    StockGift,
    AllotNum,
    AllotPrice,
    PriceAdjustmentFactor,
}

#[derive(Clone, Debug, strum::Display, strum::EnumString)]
#[strum(ascii_case_insensitive)]
pub enum StockIndicatorField {
    DividendRatio,
    DividendRatioTtm,
    MarketValueCirculating,
    MarketValueTotal,
    Pb,
    Pe,
    PeTtm,
    Ps,
    PsTtm,
    SharesCirculating,
    SharesTotal,
    TurnoverRate,
    VolumeRatio,
}

#[derive(Clone, Debug, strum::Display, strum::EnumString)]
#[strum(ascii_case_insensitive)]
pub enum StockReportBalanceField {
    ReportDate,
    CashEquivalents,
    TotalAssets,
    TotalCurrentAssets,
    TotalLiability,
    TotalCurrentLiability,
}

#[derive(Clone, Debug, strum::Display, strum::EnumString)]
#[strum(ascii_case_insensitive)]
pub enum StockReportCapitalField {
    ReportDate,
    Total,
    Circulating,
    FreeFloat,
}

#[derive(Clone, Debug, strum::Display, strum::EnumString)]
#[strum(ascii_case_insensitive)]
pub enum StockReportCashFlowField {
    ReportDate,
    NetCashByOperatingActivities,
    CapitalExpenditures,
}

#[derive(Clone, Debug, strum::Display, strum::EnumString)]
#[strum(ascii_case_insensitive)]
pub enum StockReportIncomeField {
    ReportDate,
    Revenue,
    OperatingProfit,
    TotalProfit,
}

#[derive(Clone, Debug, strum::Display, strum::EnumString)]
#[strum(ascii_case_insensitive)]
pub enum StockReportPershareField {
    ReportDate,
    Bps,
    Eps,
    EquityRoe,
    GrossProfit,
    NetProfit,
    Ocfps,
}

pub async fn fetch_delisted_stocks() -> VfResult<HashMap<Ticker, NaiveDate>> {
    let cache_key = "tushare:delisted_stocks".to_string();
    if let Some(result) = DELISTED_STOCKS_CACHE.get(&cache_key) {
        return Ok(result.clone());
    }

    let json = tushare::call_api(
        "stock_basic",
        &json!({
            "list_status": "D".to_string(),
        }),
        Some("ts_code,delist_date"),
        0,
        false,
    )
    .await?;

    let mut result = HashMap::new();

    if let (Some(fields), Some(items)) = (
        json["data"]["fields"].as_array(),
        json["data"]["items"].as_array(),
    ) {
        if let Some(ts_code_idx) = fields.iter().position(|f| f == "ts_code")
            && let Some(delist_date_idx) = fields.iter().position(|f| f == "delist_date")
        {
            for item in items {
                if let Some(values) = item.as_array() {
                    if let Some(ticker) = values[ts_code_idx]
                        .as_str()
                        .and_then(Ticker::from_tushare_str)
                        && let Some(delist_date) = values[delist_date_idx]
                            .as_str()
                            .and_then(|s| date_from_str(s).ok())
                    {
                        result.insert(ticker, delist_date);
                    }
                }
            }
        }
    }

    DELISTED_STOCKS_CACHE.insert(cache_key, result.clone());

    Ok(result)
}

pub async fn fetch_st_stocks(date: &NaiveDate, lookback_days: u64) -> VfResult<Vec<Ticker>> {
    let cache_key = format!("tushare:{}/{lookback_days}", date_to_str(date));
    if let Some(result) = ST_STOCKS_CACHE.get(&cache_key) {
        return Ok(result.clone());
    }

    let json = tushare::call_api(
        "stock_st",
        &json!({
            "start_date": (*date - Days::new(lookback_days)).format("%Y%m%d").to_string(),
            "end_date": date.format("%Y%m%d").to_string(),
        }),
        None,
        30,
        false,
    )
    .await?;

    let mut result = vec![];

    if let (Some(fields), Some(items)) = (
        json["data"]["fields"].as_array(),
        json["data"]["items"].as_array(),
    ) {
        if let Some(ts_code_idx) = fields.iter().position(|f| f == "ts_code") {
            for item in items {
                if let Some(values) = item.as_array() {
                    if let Some(ticker) = values[ts_code_idx]
                        .as_str()
                        .and_then(Ticker::from_tushare_str)
                    {
                        result.push(ticker);
                    }
                }
            }
        }
    }

    ST_STOCKS_CACHE.insert(cache_key, result.clone());

    Ok(result)
}

pub async fn fetch_stock_basic(ticker: &Ticker) -> VfResult<StockBasic> {
    let cache_key = format!("tushare:{ticker}");
    if let Some(result) = STOCK_BASIC_CACHE.get(&cache_key) {
        return Ok(result.clone());
    }

    let json = tushare::call_api(
        "stock_basic",
        &json!({
            "ts_code": ticker.to_tushare_code(),
        }),
        Some("ts_code,name,industry,list_date,delist_date"),
        30,
        false,
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

                if let Some(ticker) = json_item["ts_code"]
                    .as_str()
                    .and_then(Ticker::from_tushare_str)
                {
                    let result = StockBasic {
                        ticker,
                        name: json_item["name"].as_str().unwrap_or_default().to_string(),
                        industry: json_item["industry"]
                            .as_str()
                            .unwrap_or_default()
                            .to_string(),
                        list_date: json_item["list_date"]
                            .as_str()
                            .and_then(|s| date_from_str(s).ok()),
                        delist_date: json_item["delist_date"]
                            .as_str()
                            .and_then(|s| date_from_str(s).ok()),
                    };

                    STOCK_BASIC_CACHE.insert(cache_key, result.clone());

                    return Ok(result);
                }
            }
        }
    }

    Err(VfError::NoData {
        code: "NO_DATA",
        message: format!("No data from Tushare stock_basic of {ticker}"),
    })
}

pub async fn fetch_stock_detail(ticker: &Ticker) -> VfResult<StockDetail> {
    let cache_key = format!("qmt:{ticker}");
    if let Some(result) = STOCK_DETAIL_CACHE.get(&cache_key) {
        return Ok(result.clone());
    }

    let json = qmt::call_api(
        &format!("/stock_detail/{}", ticker.to_qmt_code()),
        &json!({}),
        30,
        false,
    )
    .await?;

    let tickers_sector_map = fetch_sector_tickers("SW1").await?;

    let result = StockDetail {
        name: json["InstrumentName"]
            .as_str()
            .unwrap_or_default()
            .to_string(),
        sector: tickers_sector_map.get(ticker).map(|s| s.replace("\"", "")),
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

pub async fn fetch_stock_dividends(ticker: &Ticker) -> VfResult<DailySeries> {
    let cache_key = format!("qmt:{ticker}");
    if let Some(result) = STOCK_DIVIDENDS_CACHE.get(&cache_key) {
        return Ok(result.clone());
    }

    let json = qmt::call_api(
        &format!("/stock_dividend/{}", ticker.to_qmt_code()),
        &json!({}),
        0,
        false,
    )
    .await?;

    let mut fields: HashMap<String, String> = HashMap::new();
    fields.insert(
        StockDividendField::Interest.to_string(),
        "interest".to_string(),
    );
    fields.insert(
        StockDividendField::StockBonus.to_string(),
        "stockBonus".to_string(),
    );
    fields.insert(
        StockDividendField::StockGift.to_string(),
        "stockGift".to_string(),
    );
    fields.insert(
        StockDividendField::AllotNum.to_string(),
        "allotNum".to_string(),
    );
    fields.insert(
        StockDividendField::AllotPrice.to_string(),
        "allotPrice".to_string(),
    );
    fields.insert(
        StockDividendField::PriceAdjustmentFactor.to_string(),
        "dr".to_string(),
    );

    let result = DailySeries::from_qmt_json(&json, "date", &fields)?;
    STOCK_DIVIDENDS_CACHE.insert(cache_key, result.clone());

    Ok(result)
}

pub async fn fetch_stock_indicators(ticker: &Ticker) -> VfResult<DailySeries> {
    let cache_key = format!("tushare:{ticker}");
    if let Some(result) = STOCK_INDICATORS_CACHE.get(&cache_key) {
        return Ok(result.clone());
    }

    static PAGE_SIZE: usize = 5000;

    let mut fields: Vec<Value> = vec![];
    let mut items: Vec<Value> = vec![];

    let mut offset: usize = 0;
    while items.len() == offset {
        let json = tushare::call_api(
            "daily_basic",
            &json!({
                "ts_code": ticker.to_tushare_code(),
                "limit": PAGE_SIZE,
                "offset": offset,
            }),
            None,
            0,
            false,
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
    fields.insert(
        StockIndicatorField::DividendRatio.to_string(),
        "dv_ratio".to_string(),
    );
    fields.insert(
        StockIndicatorField::DividendRatioTtm.to_string(),
        "dv_ttm".to_string(),
    );
    fields.insert(
        StockIndicatorField::MarketValueCirculating.to_string(),
        "circ_mv".to_string(),
    );
    fields.insert(
        StockIndicatorField::MarketValueTotal.to_string(),
        "total_mv".to_string(),
    );
    fields.insert(StockIndicatorField::Pb.to_string(), "pb".to_string());
    fields.insert(StockIndicatorField::Pe.to_string(), "pe".to_string());
    fields.insert(StockIndicatorField::PeTtm.to_string(), "pe_ttm".to_string());
    fields.insert(StockIndicatorField::Ps.to_string(), "ps".to_string());
    fields.insert(StockIndicatorField::PsTtm.to_string(), "ps_ttm".to_string());
    fields.insert(
        StockIndicatorField::SharesCirculating.to_string(),
        "float_share".to_string(),
    );
    fields.insert(
        StockIndicatorField::SharesTotal.to_string(),
        "total_share".to_string(),
    );
    fields.insert(
        StockIndicatorField::TurnoverRate.to_string(),
        "turnover_rate".to_string(),
    );
    fields.insert(
        StockIndicatorField::VolumeRatio.to_string(),
        "volume_ratio".to_string(),
    );

    let result = DailySeries::from_tushare_json(&json, "trade_date", &fields)?;
    STOCK_INDICATORS_CACHE.insert(cache_key, result.clone());

    Ok(result)
}

pub async fn fetch_stock_kline(
    ticker: &Ticker,
    adjust: StockDividendAdjust,
) -> VfResult<DailySeries> {
    if let Ok(result) = fetch_stock_kline_tushare(ticker, adjust.clone(), false).await {
        if result.len() > 0 {
            return Ok(result);
        }
    }

    if let Ok(result) = fetch_stock_kline_qmt(ticker, adjust.clone(), false).await {
        if result.len() > 0 {
            return Ok(result);
        }
    }

    Err(VfError::NoData {
        code: "NO_DATA",
        message: format!("No stock kline data of {ticker}"),
    })
}

pub async fn fetch_stock_kline_with_ds(
    ticker: &Ticker,
    adjust: StockDividendAdjust,
    ds_name: &str,
) -> VfResult<DailySeries> {
    match ds_name.to_lowercase().as_str() {
        "qmt" => fetch_stock_kline_qmt(ticker, adjust.clone(), false).await,
        "tushare" => fetch_stock_kline_tushare(ticker, adjust.clone(), false).await,
        _ => Err(VfError::Invalid {
            code: "INVALID_DATA_SOURCE",
            message: format!("No data source '{ds_name}'"),
        }),
    }
}

pub async fn fetch_stock_kline_ignore_cache(
    ticker: &Ticker,
    adjust: StockDividendAdjust,
) -> VfResult<DailySeries> {
    if let Ok(result) = fetch_stock_kline_tushare(ticker, adjust.clone(), true).await {
        if result.len() > 0 {
            return Ok(result);
        }
    }

    if let Ok(result) = fetch_stock_kline_qmt(ticker, adjust.clone(), true).await {
        if result.len() > 0 {
            return Ok(result);
        }
    }

    Err(VfError::NoData {
        code: "NO_DATA",
        message: format!("No stock kline data of {ticker}"),
    })
}

pub async fn fetch_stock_kline_ignore_cache_with_ds(
    ticker: &Ticker,
    adjust: StockDividendAdjust,
    ds_name: &str,
) -> VfResult<DailySeries> {
    match ds_name.to_lowercase().as_str() {
        "qmt" => fetch_stock_kline_qmt(ticker, adjust.clone(), true).await,
        "tushare" => fetch_stock_kline_tushare(ticker, adjust.clone(), true).await,
        _ => Err(VfError::Invalid {
            code: "INVALID_DATA_SOURCE",
            message: format!("No data source '{ds_name}'"),
        }),
    }
}

pub async fn fetch_stock_report_capital(ticker: &Ticker) -> VfResult<DailySeries> {
    let cache_key = format!("qmt:{ticker}");
    if let Some(result) = STOCK_REPORT_CAPITAL_CACHE.get(&cache_key) {
        return Ok(result.clone());
    }

    let json = qmt::call_api(
        &format!("/stock_report/{}", ticker.to_qmt_code()),
        &json!({
            "table": "Capital",
        }),
        0,
        false,
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

    let result = DailySeries::from_qmt_json(&json, "date", &fields)?;
    STOCK_REPORT_CAPITAL_CACHE.insert(cache_key, result.clone());

    Ok(result)
}

pub async fn fetch_stock_report_balance(ticker: &Ticker) -> VfResult<DailySeries> {
    if let Ok(result) = fetch_stock_report_balance_tushare(ticker).await {
        if result.len() > 0 {
            return Ok(result);
        }
    }

    if let Ok(result) = fetch_stock_report_balance_qmt(ticker).await {
        if result.len() > 0 {
            return Ok(result);
        }
    }

    Err(VfError::NoData {
        code: "NO_DATA",
        message: format!("No stock report balance data of {ticker}"),
    })
}

pub async fn fetch_stock_report_cash_flow(ticker: &Ticker) -> VfResult<DailySeries> {
    if let Ok(result) = fetch_stock_report_cash_flow_tushare(ticker).await {
        if result.len() > 0 {
            return Ok(result);
        }
    }

    if let Ok(result) = fetch_stock_report_cash_flow_qmt(ticker).await {
        if result.len() > 0 {
            return Ok(result);
        }
    }

    Err(VfError::NoData {
        code: "NO_DATA",
        message: format!("No stock report cash flow data of {ticker}"),
    })
}

pub async fn fetch_stock_report_income(ticker: &Ticker) -> VfResult<DailySeries> {
    if let Ok(result) = fetch_stock_report_income_tushare(ticker).await {
        if result.len() > 0 {
            return Ok(result);
        }
    }

    if let Ok(result) = fetch_stock_report_income_qmt(ticker).await {
        if result.len() > 0 {
            return Ok(result);
        }
    }

    Err(VfError::NoData {
        code: "NO_DATA",
        message: format!("No stock report income data of {ticker}"),
    })
}

pub async fn fetch_stock_report_pershare(ticker: &Ticker) -> VfResult<DailySeries> {
    if let Ok(result) = fetch_stock_report_pershare_tushare(ticker).await {
        if result.len() > 0 {
            return Ok(result);
        }
    }

    if let Ok(result) = fetch_stock_report_pershare_qmt(ticker).await {
        if result.len() > 0 {
            return Ok(result);
        }
    }

    Err(VfError::NoData {
        code: "NO_DATA",
        message: format!("No stock report pershare data of {ticker}"),
    })
}

static DELISTED_STOCKS_CACHE: LazyLock<DashMap<String, HashMap<Ticker, NaiveDate>>> =
    LazyLock::new(DashMap::new);
static ST_STOCKS_CACHE: LazyLock<DashMap<String, Vec<Ticker>>> = LazyLock::new(DashMap::new);
static STOCK_BASIC_CACHE: LazyLock<DashMap<String, StockBasic>> = LazyLock::new(DashMap::new);
static STOCK_DETAIL_CACHE: LazyLock<DashMap<String, StockDetail>> = LazyLock::new(DashMap::new);
static STOCK_DIVIDENDS_CACHE: LazyLock<DashMap<String, DailySeries>> = LazyLock::new(DashMap::new);
static STOCK_INDICATORS_CACHE: LazyLock<DashMap<String, DailySeries>> = LazyLock::new(DashMap::new);
static STOCK_KLINE_CACHE: LazyLock<DashMap<String, DailySeries>> = LazyLock::new(DashMap::new);
static STOCK_REPORT_BALANCE_CACHE: LazyLock<DashMap<String, DailySeries>> =
    LazyLock::new(DashMap::new);
static STOCK_REPORT_CAPITAL_CACHE: LazyLock<DashMap<String, DailySeries>> =
    LazyLock::new(DashMap::new);
static STOCK_REPORT_CASH_FLOW_CACHE: LazyLock<DashMap<String, DailySeries>> =
    LazyLock::new(DashMap::new);
static STOCK_REPORT_INCOME_CACHE: LazyLock<DashMap<String, DailySeries>> =
    LazyLock::new(DashMap::new);
static STOCK_REPORT_PERSHARE_CACHE: LazyLock<DashMap<String, DailySeries>> =
    LazyLock::new(DashMap::new);

/// QMT does not support delisted stocks, consider use tushare first
async fn fetch_stock_kline_qmt(
    ticker: &Ticker,
    adjust: StockDividendAdjust,
    ignore_cache: bool,
) -> VfResult<DailySeries> {
    let cache_key = format!("qmt:{ticker}/{adjust}");
    if !ignore_cache && let Some(result) = STOCK_KLINE_CACHE.get(&cache_key) {
        return Ok(result.clone());
    }

    let param_dividend_type = match adjust {
        StockDividendAdjust::Backward => "back",
        StockDividendAdjust::Forward => "front",
        StockDividendAdjust::No => "none",
    };

    let json = qmt::call_api(
        &format!("/stock_kline/{}", ticker.to_qmt_code()),
        &json!({
            "dividend_type": param_dividend_type,
        }),
        0,
        ignore_cache,
    )
    .await?;

    let mut fields: HashMap<String, String> = HashMap::new();
    fields.insert(KlineField::Open.to_string(), "open".to_string());
    fields.insert(KlineField::Close.to_string(), "close".to_string());
    fields.insert(KlineField::High.to_string(), "high".to_string());
    fields.insert(KlineField::Low.to_string(), "low".to_string());
    fields.insert(KlineField::Volume.to_string(), "volume".to_string());

    let result = DailySeries::from_qmt_json(&json, "date", &fields)?;
    STOCK_KLINE_CACHE.insert(cache_key, result.clone());

    Ok(result)
}

async fn fetch_stock_kline_tushare(
    ticker: &Ticker,
    adjust: StockDividendAdjust,
    ignore_cache: bool,
) -> VfResult<DailySeries> {
    let cache_key = format!("tushare:{ticker}/{adjust}");
    if !ignore_cache && let Some(result) = STOCK_KLINE_CACHE.get(&cache_key) {
        return Ok(result.clone());
    }

    static PAGE_SIZE: usize = 5000;

    let mut fields: Vec<Value> = vec![];
    let mut items: Vec<Value> = vec![];

    let mut trade_date_idx: Option<usize> = None;
    let mut open_idx: Option<usize> = None;
    let mut close_idx: Option<usize> = None;
    let mut high_idx: Option<usize> = None;
    let mut low_idx: Option<usize> = None;

    let mut offset: usize = 0;
    while items.len() == offset {
        let json = tushare::call_api(
            "daily",
            &json!({
                "ts_code": ticker.to_tushare_code(),
                "limit": PAGE_SIZE,
                "offset": offset,
            }),
            None,
            0,
            ignore_cache,
        )
        .await?;

        if let Some(page_fields) = json["data"]["fields"].as_array() {
            trade_date_idx = page_fields.iter().position(|f| f == "trade_date");
            open_idx = page_fields.iter().position(|f| f == "open");
            close_idx = page_fields.iter().position(|f| f == "close");
            high_idx = page_fields.iter().position(|f| f == "high");
            low_idx = page_fields.iter().position(|f| f == "low");

            fields = page_fields.clone();
        }

        if let Some(page_items) = json["data"]["items"].as_array() {
            items.extend_from_slice(page_items);
        }

        offset += PAGE_SIZE;
    }

    // Adjustment
    if let Some(trade_date_idx) = trade_date_idx
        && let Some(open_idx) = open_idx
        && let Some(close_idx) = close_idx
        && let Some(high_idx) = high_idx
        && let Some(low_idx) = low_idx
    {
        let adjust_json = tushare::call_api(
            "adj_factor",
            &json!({
                "ts_code": ticker.to_tushare_code(),
            }),
            None,
            0,
            ignore_cache,
        )
        .await?;

        if let Some(adjust_fields) = adjust_json["data"]["fields"].as_array()
            && let Some(adjust_items) = adjust_json["data"]["items"].as_array()
        {
            if let Some(adjust_trade_date_idx) =
                adjust_fields.iter().position(|f| f == "trade_date")
                && let Some(adjust_adj_factor_idx) =
                    adjust_fields.iter().position(|f| f == "adj_factor")
            {
                let mut adjust_factors: HashMap<NaiveDate, f64> = HashMap::new();
                for adjust_item in adjust_items {
                    if let Some(date) = adjust_item[adjust_trade_date_idx]
                        .as_str()
                        .and_then(|s| date_from_str(s).ok())
                        && let Some(factor) = adjust_item[adjust_adj_factor_idx].as_f64()
                    {
                        adjust_factors.insert(date, factor);
                    }
                }

                match adjust {
                    StockDividendAdjust::Backward => {
                        for item_idx in (0..items.len()).rev() {
                            if let Some(trade_date) = items[item_idx][trade_date_idx]
                                .as_str()
                                .and_then(|s| date_from_str(s).ok())
                                && let Some(open) = items[item_idx][open_idx].as_f64()
                                && let Some(close) = items[item_idx][close_idx].as_f64()
                                && let Some(high) = items[item_idx][high_idx].as_f64()
                                && let Some(low) = items[item_idx][low_idx].as_f64()
                            {
                                if let Some(adjust_factor) = adjust_factors.get(&trade_date) {
                                    items[item_idx][open_idx] = json!(open * adjust_factor);
                                    items[item_idx][close_idx] = json!(close * adjust_factor);
                                    items[item_idx][high_idx] = json!(high * adjust_factor);
                                    items[item_idx][low_idx] = json!(low * adjust_factor);
                                } else {
                                    items.remove(item_idx);
                                }
                            }
                        }
                    }
                    StockDividendAdjust::Forward => {
                        if let Some(latest_adjust_factor) = adjust_factors
                            .iter()
                            .max_by_key(|(date, _)| *date)
                            .map(|(_, factor)| *factor)
                        {
                            for item_idx in (0..items.len()).rev() {
                                if let Some(trade_date) = items[item_idx][trade_date_idx]
                                    .as_str()
                                    .and_then(|s| date_from_str(s).ok())
                                    && let Some(open) = items[item_idx][open_idx].as_f64()
                                    && let Some(close) = items[item_idx][close_idx].as_f64()
                                    && let Some(high) = items[item_idx][high_idx].as_f64()
                                    && let Some(low) = items[item_idx][low_idx].as_f64()
                                {
                                    if let Some(adjust_factor) = adjust_factors.get(&trade_date) {
                                        let factor = latest_adjust_factor / adjust_factor;
                                        items[item_idx][open_idx] = json!(open * factor);
                                        items[item_idx][close_idx] = json!(close * factor);
                                        items[item_idx][high_idx] = json!(high * factor);
                                        items[item_idx][low_idx] = json!(low * factor);
                                    } else {
                                        items.remove(item_idx);
                                    }
                                }
                            }
                        }
                    }
                    _ => {}
                }
            }
        }
    }

    let json = json!({
        "data": {
            "fields": fields,
            "items": items,
        }
    });

    let mut fields: HashMap<String, String> = HashMap::new();
    fields.insert(KlineField::Open.to_string(), "open".to_string());
    fields.insert(KlineField::Close.to_string(), "close".to_string());
    fields.insert(KlineField::High.to_string(), "high".to_string());
    fields.insert(KlineField::Low.to_string(), "low".to_string());
    fields.insert(KlineField::Volume.to_string(), "vol".to_string());

    let result = DailySeries::from_tushare_json(&json, "trade_date", &fields)?;
    STOCK_KLINE_CACHE.insert(cache_key, result.clone());

    Ok(result)
}

async fn fetch_stock_report_balance_qmt(ticker: &Ticker) -> VfResult<DailySeries> {
    let cache_key = format!("qmt:{ticker}");
    if let Some(result) = STOCK_REPORT_BALANCE_CACHE.get(&cache_key) {
        return Ok(result.clone());
    }

    let json = qmt::call_api(
        &format!("/stock_report/{}", ticker.to_qmt_code()),
        &json!({
            "table": "Balance",
        }),
        0,
        false,
    )
    .await?;

    let mut fields: HashMap<String, String> = HashMap::new();
    fields.insert(
        StockReportBalanceField::ReportDate.to_string(),
        "m_timetag".to_string(),
    );
    fields.insert(
        StockReportBalanceField::CashEquivalents.to_string(),
        "cash_equivalents".to_string(),
    );
    fields.insert(
        StockReportBalanceField::TotalAssets.to_string(),
        "tot_assets".to_string(),
    );
    fields.insert(
        StockReportBalanceField::TotalCurrentAssets.to_string(),
        "total_current_assets".to_string(),
    );
    fields.insert(
        StockReportBalanceField::TotalLiability.to_string(),
        "tot_liab".to_string(),
    );
    fields.insert(
        StockReportBalanceField::TotalCurrentLiability.to_string(),
        "total_current_liability".to_string(),
    );

    let result = DailySeries::from_qmt_json(&json, "date", &fields)?;
    STOCK_REPORT_BALANCE_CACHE.insert(cache_key, result.clone());

    Ok(result)
}

async fn fetch_stock_report_balance_tushare(ticker: &Ticker) -> VfResult<DailySeries> {
    let cache_key = format!("tushare:{ticker}");
    if let Some(result) = STOCK_REPORT_BALANCE_CACHE.get(&cache_key) {
        return Ok(result.clone());
    }

    let json = tushare::call_api(
        "balancesheet",
        &json!({
            "ts_code": ticker.to_tushare_code(),
        }),
        None,
        0,
        false,
    )
    .await?;

    let mut fields: HashMap<String, String> = HashMap::new();
    fields.insert(
        StockReportBalanceField::ReportDate.to_string(),
        "end_date".to_string(),
    );
    fields.insert(
        StockReportBalanceField::CashEquivalents.to_string(),
        "money_cap".to_string(),
    );
    fields.insert(
        StockReportBalanceField::TotalAssets.to_string(),
        "total_assets".to_string(),
    );
    fields.insert(
        StockReportBalanceField::TotalCurrentAssets.to_string(),
        "total_cur_assets".to_string(),
    );
    fields.insert(
        StockReportBalanceField::TotalLiability.to_string(),
        "total_liab".to_string(),
    );
    fields.insert(
        StockReportBalanceField::TotalCurrentLiability.to_string(),
        "total_cur_liab".to_string(),
    );

    let result = DailySeries::from_tushare_json(&json, "f_ann_date", &fields)?;
    STOCK_REPORT_BALANCE_CACHE.insert(cache_key, result.clone());

    Ok(result)
}

async fn fetch_stock_report_cash_flow_qmt(ticker: &Ticker) -> VfResult<DailySeries> {
    let cache_key = format!("qmt:{ticker}");
    if let Some(result) = STOCK_REPORT_CASH_FLOW_CACHE.get(&cache_key) {
        return Ok(result.clone());
    }

    let json = qmt::call_api(
        &format!("/stock_report/{}", ticker.to_qmt_code()),
        &json!({
            "table": "CashFlow",
        }),
        0,
        false,
    )
    .await?;

    let mut fields: HashMap<String, String> = HashMap::new();
    fields.insert(
        StockReportCashFlowField::ReportDate.to_string(),
        "m_timetag".to_string(),
    );
    fields.insert(
        StockReportCashFlowField::NetCashByOperatingActivities.to_string(),
        "net_cash_flows_oper_act".to_string(),
    );
    fields.insert(
        StockReportCashFlowField::CapitalExpenditures.to_string(),
        "cash_pay_acq_const_fiolta".to_string(),
    );

    let result = DailySeries::from_qmt_json(&json, "date", &fields)?;
    STOCK_REPORT_CASH_FLOW_CACHE.insert(cache_key, result.clone());

    Ok(result)
}

async fn fetch_stock_report_cash_flow_tushare(ticker: &Ticker) -> VfResult<DailySeries> {
    let cache_key = format!("tushare:{ticker}");
    if let Some(result) = STOCK_REPORT_CASH_FLOW_CACHE.get(&cache_key) {
        return Ok(result.clone());
    }

    let json = tushare::call_api(
        "cashflow",
        &json!({
            "ts_code": ticker.to_tushare_code(),
        }),
        None,
        0,
        false,
    )
    .await?;

    let mut fields: HashMap<String, String> = HashMap::new();
    fields.insert(
        StockReportCashFlowField::ReportDate.to_string(),
        "end_date".to_string(),
    );
    fields.insert(
        StockReportCashFlowField::NetCashByOperatingActivities.to_string(),
        "n_cashflow_act".to_string(),
    );
    fields.insert(
        StockReportCashFlowField::CapitalExpenditures.to_string(),
        "c_pay_acq_const_fiolta".to_string(),
    );

    let result = DailySeries::from_tushare_json(&json, "f_ann_date", &fields)?;
    STOCK_REPORT_CASH_FLOW_CACHE.insert(cache_key, result.clone());

    Ok(result)
}

async fn fetch_stock_report_income_qmt(ticker: &Ticker) -> VfResult<DailySeries> {
    let cache_key = format!("qmt:{ticker}");
    if let Some(result) = STOCK_REPORT_INCOME_CACHE.get(&cache_key) {
        return Ok(result.clone());
    }

    let json = qmt::call_api(
        &format!("/stock_report/{}", ticker.to_qmt_code()),
        &json!({
            "table": "Income",
        }),
        0,
        false,
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

    let result = DailySeries::from_qmt_json(&json, "date", &fields)?;
    STOCK_REPORT_INCOME_CACHE.insert(cache_key, result.clone());

    Ok(result)
}

async fn fetch_stock_report_income_tushare(ticker: &Ticker) -> VfResult<DailySeries> {
    let cache_key = format!("tushare:{ticker}");
    if let Some(result) = STOCK_REPORT_INCOME_CACHE.get(&cache_key) {
        return Ok(result.clone());
    }

    let json = tushare::call_api(
        "income",
        &json!({
            "ts_code": ticker.to_tushare_code(),
        }),
        None,
        0,
        false,
    )
    .await?;

    let mut fields: HashMap<String, String> = HashMap::new();
    fields.insert(
        StockReportCapitalField::ReportDate.to_string(),
        "end_date".to_string(),
    );
    fields.insert(
        StockReportIncomeField::Revenue.to_string(),
        "total_revenue".to_string(),
    );
    fields.insert(
        StockReportIncomeField::OperatingProfit.to_string(),
        "operate_profit".to_string(),
    );
    fields.insert(
        StockReportIncomeField::TotalProfit.to_string(),
        "total_profit".to_string(),
    );

    let result = DailySeries::from_tushare_json(&json, "f_ann_date", &fields)?;
    STOCK_REPORT_INCOME_CACHE.insert(cache_key, result.clone());

    Ok(result)
}

async fn fetch_stock_report_pershare_qmt(ticker: &Ticker) -> VfResult<DailySeries> {
    let cache_key = format!("qmt:{ticker}");
    if let Some(result) = STOCK_REPORT_PERSHARE_CACHE.get(&cache_key) {
        return Ok(result.clone());
    }

    let json = qmt::call_api(
        &format!("/stock_report/{}", ticker.to_qmt_code()),
        &json!({
            "table": "PershareIndex",
        }),
        0,
        false,
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
        StockReportPershareField::Eps.to_string(),
        "s_fa_eps_basic".to_string(),
    );
    fields.insert(
        StockReportPershareField::EquityRoe.to_string(),
        "equity_roe".to_string(),
    );
    fields.insert(
        StockReportPershareField::GrossProfit.to_string(),
        "gross_profit".to_string(),
    );
    fields.insert(
        StockReportPershareField::NetProfit.to_string(),
        "net_profit".to_string(),
    );
    fields.insert(
        StockReportPershareField::Ocfps.to_string(),
        "s_fa_ocfps".to_string(),
    );

    let result = DailySeries::from_qmt_json(&json, "date", &fields)?;
    STOCK_REPORT_PERSHARE_CACHE.insert(cache_key, result.clone());

    Ok(result)
}

async fn fetch_stock_report_pershare_tushare(ticker: &Ticker) -> VfResult<DailySeries> {
    let cache_key = format!("tushare:{ticker}");
    if let Some(result) = STOCK_REPORT_PERSHARE_CACHE.get(&cache_key) {
        return Ok(result.clone());
    }

    static PAGE_SIZE: usize = 100;

    let mut fields: Vec<Value> = vec![];
    let mut items: Vec<Value> = vec![];

    let mut offset: usize = 0;
    while items.len() == offset {
        let json = tushare::call_api(
            "fina_indicator",
            &json!({
                "ts_code": ticker.to_tushare_code(),
                "limit": PAGE_SIZE,
                "offset": offset,
            }),
            None,
            0,
            false,
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
    fields.insert(
        StockReportPershareField::ReportDate.to_string(),
        "end_date".to_string(),
    );
    fields.insert(StockReportPershareField::Bps.to_string(), "bps".to_string());
    fields.insert(StockReportPershareField::Eps.to_string(), "eps".to_string());
    fields.insert(
        StockReportPershareField::EquityRoe.to_string(),
        "roe_waa".to_string(),
    );
    fields.insert(
        StockReportPershareField::GrossProfit.to_string(),
        "gross_margin".to_string(),
    );
    fields.insert(
        StockReportPershareField::NetProfit.to_string(),
        "netprofit_margin".to_string(),
    );
    fields.insert(
        StockReportPershareField::Ocfps.to_string(),
        "ocfps".to_string(),
    );

    let result = DailySeries::from_tushare_json(&json, "ann_date", &fields)?;
    STOCK_REPORT_PERSHARE_CACHE.insert(cache_key, result.clone());

    Ok(result)
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;
    use crate::{STALE_DAYS_LONG, STALE_DAYS_SHORT};

    #[tokio::test]
    async fn test_fetch_st_stocks() {
        let date = date_from_str("2018-01-01").unwrap();
        let st_stocks = fetch_st_stocks(&date, 7).await.unwrap();

        assert!(!st_stocks.is_empty());
    }

    #[tokio::test]
    async fn test_fetch_stock_basic() {
        let ticker = Ticker::from_str("000046").unwrap();
        let basic = fetch_stock_basic(&ticker).await.unwrap();

        assert_eq!(basic.name, "平安银行");
        assert_eq!(basic.industry, "银行");
    }

    #[tokio::test]
    async fn test_fetch_stock_detail() {
        let ticker = Ticker::from_str("000001").unwrap();
        let detail = fetch_stock_detail(&ticker).await.unwrap();

        assert_eq!(detail.name, "平安银行");
        assert_eq!(&detail.sector.unwrap(), "银行");
    }

    #[tokio::test]
    async fn test_fetch_stock_dividends() {
        let ticker = Ticker::from_str("000001").unwrap();
        let dataset = fetch_stock_dividends(&ticker).await.unwrap();

        let (_, data) = dataset
            .get_latest_value::<f64>(
                &date_from_str("2018-01-01").unwrap(),
                STALE_DAYS_LONG,
                false,
                &StockDividendField::PriceAdjustmentFactor.to_string(),
            )
            .unwrap();

        assert!(data > 1.0);
    }

    #[tokio::test]
    async fn test_fetch_stock_indicators() {
        let ticker = Ticker::from_str("000001").unwrap();
        let dataset = fetch_stock_indicators(&ticker).await.unwrap();

        let (_, data) = dataset
            .get_latest_value::<f64>(
                &date_from_str("2018-01-01").unwrap(),
                STALE_DAYS_SHORT,
                false,
                &StockIndicatorField::VolumeRatio.to_string(),
            )
            .unwrap();

        assert!(data > 0.0);
    }

    #[tokio::test]
    async fn test_fetch_stock_kline() {
        let ticker = Ticker::from_str("002155").unwrap();
        let dataset = fetch_stock_kline(&ticker, StockDividendAdjust::Backward)
            .await
            .unwrap();

        let (_, data) = dataset
            .get_latest_value::<f64>(
                &date_from_str("2026-01-12").unwrap(),
                STALE_DAYS_SHORT,
                true,
                &KlineField::Close.to_string(),
            )
            .unwrap();

        assert!(data > 0.0);
    }

    #[tokio::test]
    async fn test_fetch_stock_kline_from_qmt_and_tushare() {
        let ticker = Ticker::from_str("600595").unwrap();
        let date = date_from_str("2019-08-15").unwrap();

        {
            let dataset_qmt = fetch_stock_kline_qmt(&ticker, StockDividendAdjust::No, false)
                .await
                .unwrap();
            let dataset_tushare =
                fetch_stock_kline_tushare(&ticker, StockDividendAdjust::No, false)
                    .await
                    .unwrap();

            let (_, data_qmt) = dataset_qmt
                .get_value::<f64>(&date, &KlineField::Close.to_string())
                .unwrap();
            let (_, data_tushare) = dataset_tushare
                .get_value::<f64>(&date, &KlineField::Close.to_string())
                .unwrap();

            assert!(((data_qmt - data_tushare) / data_tushare).abs() < 0.001);
        }

        // Can not pass for now
        {
            let dataset_qmt = fetch_stock_kline_qmt(&ticker, StockDividendAdjust::Backward, false)
                .await
                .unwrap();
            let dataset_tushare =
                fetch_stock_kline_tushare(&ticker, StockDividendAdjust::Backward, false)
                    .await
                    .unwrap();

            let (_, data_qmt) = dataset_qmt
                .get_value::<f64>(&date, &KlineField::Close.to_string())
                .unwrap();
            let (_, data_tushare) = dataset_tushare
                .get_value::<f64>(&date, &KlineField::Close.to_string())
                .unwrap();

            assert!(((data_qmt - data_tushare) / data_tushare).abs() < 0.001);
        }

        {
            let dataset_qmt = fetch_stock_kline_qmt(&ticker, StockDividendAdjust::Forward, false)
                .await
                .unwrap();
            let dataset_tushare =
                fetch_stock_kline_tushare(&ticker, StockDividendAdjust::Forward, false)
                    .await
                    .unwrap();

            let (_, data_qmt) = dataset_qmt
                .get_value::<f64>(&date, &KlineField::Close.to_string())
                .unwrap();
            let (_, data_tushare) = dataset_tushare
                .get_value::<f64>(&date, &KlineField::Close.to_string())
                .unwrap();

            assert!(((data_qmt - data_tushare) / data_tushare).abs() < 0.001);
        }
    }

    #[tokio::test]
    async fn test_fetch_stock_report_pershare_from_qmt_and_tushare() {
        let ticker = Ticker::from_str("600595").unwrap();
        let date = date_from_str("2019-08-15").unwrap();

        let dataset_qmt = fetch_stock_report_pershare_qmt(&ticker).await.unwrap();
        let dataset_tushare = fetch_stock_report_pershare_tushare(&ticker).await.unwrap();

        {
            let (_, data_qmt) = dataset_qmt
                .get_latest_value::<f64>(
                    &date,
                    STALE_DAYS_SHORT,
                    false,
                    &StockReportPershareField::EquityRoe.to_string(),
                )
                .unwrap();

            let (_, data_tushare) = dataset_tushare
                .get_latest_value::<f64>(
                    &date,
                    STALE_DAYS_SHORT,
                    false,
                    &StockReportPershareField::EquityRoe.to_string(),
                )
                .unwrap();

            assert!(((data_qmt - data_tushare) / data_tushare).abs() < 0.001);
        }
    }
}
