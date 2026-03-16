use std::collections::HashMap;

use async_trait::async_trait;
use chrono::{Datelike, Duration, NaiveDate};
use tokio::{sync::mpsc::Sender, time::Instant};

use crate::{
    PROGRESS_INTERVAL_SECS, REQUIRED_DATA_COMPLETENESS, STALE_DAYS_LONG,
    error::VfResult,
    filter::{filter_invalid::has_invalid_price, filter_st::is_st},
    financial::{
        KlineField,
        helper::{
            calc_stock_cash_ratio, calc_stock_current_ratio, calc_stock_dividend_ratio_lt,
            calc_stock_dividend_ratio_ttm, calc_stock_free_cash_ratio_lt,
            calc_stock_free_cash_ratio_ttm, calc_stock_roe_lt,
        },
        stock::{
            StockDividendAdjust, StockDividendField, fetch_stock_detail, fetch_stock_dividends,
            fetch_stock_kline,
        },
    },
    rule::{
        BacktestEvent, FundBacktestContext, RuleDefinition, RuleExecutor, calc_weights,
        rule_notify_calc_progress, rule_notify_indicators, rule_send_info, rule_send_warning,
        select_by_indicators,
    },
    ticker::Ticker,
    utils::{
        financial::{calc_annualized_momentum, calc_annualized_volatility_mad},
        stats::quantile_rank,
    },
};

pub struct Executor {
    #[allow(dead_code)]
    options: HashMap<String, serde_json::Value>,
}

impl Executor {
    pub fn new(definition: &RuleDefinition) -> Self {
        Self {
            options: definition.options.clone(),
        }
    }
}

#[async_trait]
impl RuleExecutor for Executor {
    async fn exec(
        &mut self,
        context: &mut FundBacktestContext,
        date: &NaiveDate,
        event_sender: &Sender<BacktestEvent>,
    ) -> VfResult<()> {
        let rule_name = mod_name!();

        let adjust_momentum_weight = self
            .options
            .get("adjust_momentum_weight")
            .and_then(|v| v.as_f64())
            .unwrap_or(1.0);
        let adjust_roe_weight = self
            .options
            .get("adjust_roe_weight")
            .and_then(|v| v.as_f64())
            .unwrap_or(1.0);
        let adjust_volatility_weight = self
            .options
            .get("adjust_volatility_weight")
            .and_then(|v| v.as_f64())
            .unwrap_or(1.0);
        let cash_ratio_quantile_lower = self
            .options
            .get("cash_ratio_quantile_lower")
            .and_then(|v| v.as_f64())
            .unwrap_or(0.0);
        let current_ratio_quantile_lower = self
            .options
            .get("current_ratio_quantile_lower")
            .and_then(|v| v.as_f64())
            .unwrap_or(0.0);
        let dividend_ratio_lt_lower = self
            .options
            .get("dividend_ratio_lt_lower")
            .and_then(|v| v.as_f64())
            .unwrap_or(0.0);
        let dividend_ratio_quantile_lower = self
            .options
            .get("dividend_ratio_quantile_lower")
            .and_then(|v| v.as_f64())
            .unwrap_or(0.0);
        let free_cash_ratio_lt_lower = self
            .options
            .get("free_cash_ratio_lt_lower")
            .and_then(|v| v.as_f64())
            .unwrap_or(0.0);
        let free_cash_ratio_quantile_lower = self
            .options
            .get("free_cash_ratio_quantile_lower")
            .and_then(|v| v.as_f64())
            .unwrap_or(0.0);
        let exclude_sectors: Vec<String> = self
            .options
            .get("exclude_sectors")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str())
                    .map(|s| s.to_string())
                    .collect::<Vec<String>>()
            })
            .unwrap_or_default();
        let free_cash_weight = self
            .options
            .get("free_cash_weight")
            .and_then(|v| v.as_f64())
            .unwrap_or(0.5);
        let limit = self
            .options
            .get("limit")
            .and_then(|v| v.as_u64())
            .unwrap_or(10);
        let lookback_lt_years = self
            .options
            .get("lookback_lt_years")
            .and_then(|v| v.as_u64())
            .unwrap_or(3);
        let lookback_trade_days = self
            .options
            .get("lookback_trade_days")
            .and_then(|v| v.as_u64())
            .unwrap_or(250);
        let skip_same_sector = self
            .options
            .get("skip_same_sector")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let weight_method = self
            .options
            .get("weight_method")
            .and_then(|v| v.as_str())
            .unwrap_or("equal");
        {
            if limit == 0 {
                panic!("limit must > 0");
            }

            if lookback_lt_years == 0 {
                panic!("lookback_lt_years must > 0");
            }

            if lookback_trade_days == 0 {
                panic!("lookback_trade_days must > 0");
            }
        }

        let tickers_map = context.fund_definition.all_tickers_map(date).await?;
        if !tickers_map.is_empty() {
            let mut tickers_factors: Vec<(Ticker, Factors)> = vec![];
            {
                let mut last_time = Instant::now();
                let mut calc_count: usize = 0;

                for ticker in tickers_map.keys() {
                    calc_count += 1;

                    if let Ok(st) = is_st(ticker, date, STALE_DAYS_LONG as u64).await {
                        if st {
                            continue;
                        }
                    } else {
                        continue;
                    }

                    if let Ok(invalid_price) = has_invalid_price(ticker, date).await {
                        if invalid_price {
                            rule_send_warning(
                                rule_name,
                                &format!("[Invalid Price] {ticker}"),
                                date,
                                event_sender,
                            )
                            .await;
                            continue;
                        }
                    } else {
                        continue;
                    }

                    if !exclude_sectors.is_empty() {
                        if let Ok(stock_detail) = fetch_stock_detail(ticker).await {
                            if let Some(sector) = stock_detail.sector {
                                if exclude_sectors.contains(&sector) {
                                    continue;
                                }
                            }
                        }
                    }

                    if let Ok(dividend_ratio_lt) =
                        calc_stock_dividend_ratio_lt(ticker, date, lookback_lt_years as u32).await
                    {
                        if dividend_ratio_lt < dividend_ratio_lt_lower {
                            continue;
                        }
                    } else {
                        continue;
                    }

                    if let Ok(free_cash_ratio_lt) =
                        calc_stock_free_cash_ratio_lt(ticker, date, lookback_lt_years as u32).await
                    {
                        if free_cash_ratio_lt < free_cash_ratio_lt_lower {
                            continue;
                        }
                    } else {
                        continue;
                    }

                    {
                        let lt_date_from = date
                            .with_year(date.year() - lookback_lt_years as i32)
                            .unwrap();
                        let lt_date_to = *date - Duration::days(1);

                        match fetch_stock_dividends(ticker).await {
                            Ok(stock_dividends) => {
                                if let Ok(year_dividends) =
                                    stock_dividends.slice_by_date_range(&lt_date_from, &lt_date_to)
                                {
                                    let mut interest_count: usize = 0;

                                    for div_date in year_dividends.all_dates() {
                                        if let Some((_, interest)) = year_dividends
                                            .get_value::<f64>(
                                                &div_date,
                                                &StockDividendField::Interest.to_string(),
                                            )
                                        {
                                            if interest > 0.0 {
                                                interest_count += 1;
                                            }
                                        }
                                    }

                                    if interest_count < lookback_lt_years as usize {
                                        continue;
                                    }
                                } else {
                                    continue;
                                }
                            }
                            Err(_) => {
                                continue;
                            }
                        }
                    }

                    if let Ok(cash_ratio) = calc_stock_cash_ratio(ticker, date).await
                        && let Ok(current_ratio) = calc_stock_current_ratio(ticker, date).await
                        && let Ok(dividend_ratio) =
                            calc_stock_dividend_ratio_ttm(ticker, date).await
                        && let Ok(free_cash_ratio) =
                            calc_stock_free_cash_ratio_ttm(ticker, date).await
                    {
                        if dividend_ratio > 0.0 && free_cash_ratio > 0.0 {
                            let kline =
                                fetch_stock_kline(ticker, StockDividendAdjust::Backward).await?;
                            let prices: Vec<f64> = kline
                                .get_latest_values::<f64>(
                                    date,
                                    false,
                                    &KlineField::Close.to_string(),
                                    lookback_trade_days as u32,
                                )
                                .iter()
                                .map(|&(_, v)| v)
                                .collect();
                            if prices.len()
                                < (lookback_trade_days as f64 * REQUIRED_DATA_COMPLETENESS).round()
                                    as usize
                            {
                                rule_send_warning(
                                    rule_name,
                                    &format!(
                                        "[No Enough Data] {ticker} {lookback_trade_days}({})",
                                        prices.len()
                                    ),
                                    date,
                                    event_sender,
                                )
                                .await;
                                continue;
                            }

                            tickers_factors.push((
                                ticker.clone(),
                                Factors {
                                    cash_ratio,
                                    current_ratio,
                                    dividend_ratio,
                                    free_cash_ratio,
                                    momentum: calc_annualized_momentum(&prices, false),
                                    roe: calc_stock_roe_lt(ticker, date, lookback_lt_years as u32)
                                        .await
                                        .ok()
                                        .flatten(),
                                    volatility: calc_annualized_volatility_mad(&prices),
                                },
                            ));
                        }
                    }

                    if last_time.elapsed().as_secs() > PROGRESS_INTERVAL_SECS {
                        rule_notify_calc_progress(
                            rule_name,
                            calc_count as f64 / tickers_map.len() as f64 * 100.0,
                            date,
                            event_sender,
                        )
                        .await;

                        last_time = Instant::now();
                    }
                }

                rule_notify_calc_progress(rule_name, 100.0, date, event_sender).await;
            }

            let factors_cash_ratio = tickers_factors
                .iter()
                .map(|(_, f)| f.cash_ratio)
                .collect::<Vec<f64>>();

            let factors_current_ratio = tickers_factors
                .iter()
                .map(|(_, f)| f.current_ratio)
                .collect::<Vec<f64>>();

            let factors_dividend_ratio = tickers_factors
                .iter()
                .map(|(_, f)| f.dividend_ratio)
                .collect::<Vec<f64>>();

            let factors_free_cash_ratio = tickers_factors
                .iter()
                .map(|(_, f)| f.free_cash_ratio)
                .collect::<Vec<f64>>();

            let factors_momentum = tickers_factors
                .iter()
                .filter_map(|(_, f)| f.momentum)
                .collect::<Vec<f64>>();

            let factors_roe = tickers_factors
                .iter()
                .filter_map(|(_, f)| f.roe)
                .collect::<Vec<f64>>();

            let factors_volatility = tickers_factors
                .iter()
                .filter_map(|(_, f)| f.volatility)
                .collect::<Vec<f64>>();

            let mut indicators: Vec<(Ticker, f64)> = vec![];
            for (ticker, factors) in tickers_factors {
                if let Some(momentum) = factors.momentum
                    && let Some(roe) = factors.roe
                    && let Some(volatility) = factors.volatility
                {
                    if let Some(cash_ratio_rank) =
                        quantile_rank(&factors_cash_ratio, factors.cash_ratio)
                        && let Some(current_ratio_rank) =
                            quantile_rank(&factors_current_ratio, factors.current_ratio)
                        && let Some(dividend_ratio_rank) =
                            quantile_rank(&factors_dividend_ratio, factors.dividend_ratio)
                        && let Some(free_cash_ratio_rank) =
                            quantile_rank(&factors_free_cash_ratio, factors.free_cash_ratio)
                        && let Some(momentum_rank) = quantile_rank(&factors_momentum, momentum)
                        && let Some(roe_rank) = quantile_rank(&factors_roe, roe)
                        && let Some(volatility_rank) =
                            quantile_rank(&factors_volatility, volatility)
                    {
                        if cash_ratio_rank > cash_ratio_quantile_lower
                            && current_ratio_rank > current_ratio_quantile_lower
                            && dividend_ratio_rank > dividend_ratio_quantile_lower
                            && free_cash_ratio_rank > free_cash_ratio_quantile_lower
                        {
                            let adjust: f64 = adjust_momentum_weight * (momentum_rank - 0.5)
                                + adjust_roe_weight * (roe_rank - 0.5)
                                + adjust_volatility_weight * (0.5 - volatility_rank);

                            let indicator = ((1.0 - free_cash_weight) * dividend_ratio_rank
                                + free_cash_weight * free_cash_ratio_rank)
                                * (1.0 + adjust);

                            indicators.push((ticker, indicator));
                        }
                    }
                }
            }
            indicators.sort_by(|a, b| b.1.total_cmp(&a.1));

            rule_send_info(
                rule_name,
                &format!("[Universe] {}({})", tickers_map.len(), indicators.len()),
                date,
                event_sender,
            )
            .await;

            let (targets_indicators, candidates_indicators) =
                select_by_indicators(&indicators, limit as usize, skip_same_sector).await?;

            rule_notify_indicators(
                rule_name,
                &targets_indicators
                    .iter()
                    .map(|&(ref t, v)| (t.clone(), format!("{v:.4}")))
                    .collect::<Vec<_>>(),
                &candidates_indicators
                    .iter()
                    .map(|&(ref t, v)| (t.clone(), format!("{v:.4}")))
                    .collect::<Vec<_>>(),
                date,
                event_sender,
            )
            .await;

            let weights = calc_weights(&targets_indicators, weight_method)?;
            context.rebalance(&weights, date, event_sender).await?;
        }

        Ok(())
    }
}

#[derive(Debug)]
struct Factors {
    cash_ratio: f64,
    current_ratio: f64,
    dividend_ratio: f64,
    free_cash_ratio: f64,
    momentum: Option<f64>,
    roe: Option<f64>,
    volatility: Option<f64>,
}
