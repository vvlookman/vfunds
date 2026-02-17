use std::collections::HashMap;

use async_trait::async_trait;
use chrono::{Datelike, Duration, NaiveDate};
use tokio::{sync::mpsc::Sender, time::Instant};

use crate::{
    PROGRESS_INTERVAL_SECS, REQUIRED_DATA_COMPLETENESS, STALE_DAYS_LONG, STALE_DAYS_SHORT,
    error::VfResult,
    filter::{filter_invalid::has_invalid_price, filter_st::is_st},
    financial::{
        KlineField,
        stock::{
            StockDividendAdjust, StockDividendField, StockReportPershareField,
            fetch_stock_dividends, fetch_stock_kline, fetch_stock_report_pershare,
        },
    },
    rule::{
        BacktestEvent, FundBacktestContext, RuleDefinition, RuleExecutor, calc_weights,
        rule_notify_calc_progress, rule_notify_indicators, rule_send_info, rule_send_warning,
        select_by_indicators,
    },
    ticker::Ticker,
    utils::{
        financial::{calc_annualized_momentum, calc_annualized_volatility},
        stats::{self, quantile},
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

        let div_allot_weight = self
            .options
            .get("div_allot_weight")
            .and_then(|v| v.as_f64())
            .unwrap_or(0.0);
        let div_bonus_gift_weight = self
            .options
            .get("div_bonus_gift_weight")
            .and_then(|v| v.as_f64())
            .unwrap_or(0.0);
        let div_weight_yearly_decay = self
            .options
            .get("div_weight_yearly_decay")
            .and_then(|v| v.as_f64())
            .unwrap_or(0.0);
        let limit = self
            .options
            .get("limit")
            .and_then(|v| v.as_u64())
            .unwrap_or(10);
        let lookback_div_years = self
            .options
            .get("lookback_div_years")
            .and_then(|v| v.as_u64())
            .unwrap_or(3);
        let lookback_trade_days = self
            .options
            .get("lookback_trade_days")
            .and_then(|v| v.as_u64())
            .unwrap_or(252);
        let min_div_count_per_year = self
            .options
            .get("min_div_count_per_year")
            .and_then(|v| v.as_f64())
            .unwrap_or(1.0);
        let momentum_quantile_lower = self
            .options
            .get("momentum_quantile_lower")
            .and_then(|v| v.as_f64())
            .unwrap_or(0.0);
        let roe_quantile_lower = self
            .options
            .get("roe_quantile_lower")
            .and_then(|v| v.as_f64())
            .unwrap_or(0.0);
        let recent_avg_count = self
            .options
            .get("recent_avg_count")
            .and_then(|v| v.as_u64())
            .unwrap_or(5);
        let skip_same_sector = self
            .options
            .get("skip_same_sector")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let volatility_quantile_upper = self
            .options
            .get("volatility_quantile_upper")
            .and_then(|v| v.as_f64())
            .unwrap_or(1.0);
        let weight_method = self
            .options
            .get("weight_method")
            .and_then(|v| v.as_str())
            .unwrap_or("equal");
        {
            if limit == 0 {
                panic!("limit must > 0");
            }

            if lookback_div_years == 0 {
                panic!("lookback_div_years must > 0");
            }

            if lookback_trade_days == 0 {
                panic!("lookback_trade_days must > 0");
            }

            if recent_avg_count == 0 {
                panic!("recent_avg_count must > 0");
            }

            if min_div_count_per_year <= 0.0 {
                panic!("min_div_count_per_year must > 0");
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

                    if context.portfolio.reserved_cash.contains_key(ticker) {
                        continue;
                    }

                    if is_st(ticker, date, 30).await? {
                        continue;
                    }

                    if has_invalid_price(ticker, date).await? {
                        rule_send_warning(
                            rule_name,
                            &format!("[Invalid Price] {ticker}"),
                            date,
                            event_sender,
                        )
                        .await;
                        continue;
                    }

                    let kline_no_adjust =
                        fetch_stock_kline(ticker, StockDividendAdjust::No).await?;
                    let prices_no_adjust: Vec<f64> = kline_no_adjust
                        .get_latest_values::<f64>(
                            date,
                            false,
                            &KlineField::Close.to_string(),
                            recent_avg_count as u32,
                        )
                        .iter()
                        .map(|&(_, v)| v)
                        .collect();
                    let price_no_adjust = stats::mean(&prices_no_adjust).unwrap_or(0.0);
                    if price_no_adjust > 0.0 {
                        let mut dividends: Vec<f64> = vec![];

                        let stock_dividends = fetch_stock_dividends(ticker).await?;
                        for i in 0..lookback_div_years {
                            let year_date_from =
                                date.with_year(date.year() - 1 - i as i32).unwrap();
                            let year_date_to =
                                date.with_year(date.year() - i as i32).unwrap() - Duration::days(1);
                            if let Ok(year_dividends) =
                                stock_dividends.slice_by_date_range(&year_date_from, &year_date_to)
                            {
                                let div_dates = year_dividends.all_dates();

                                for div_date in div_dates {
                                    if let (
                                        Some((_, interest)),
                                        Some((_, allot_num)),
                                        Some((_, allot_price)),
                                        Some((_, stock_bonus)),
                                        Some((_, stock_gift)),
                                    ) = (
                                        year_dividends.get_value::<f64>(
                                            &div_date,
                                            &StockDividendField::Interest.to_string(),
                                        ),
                                        year_dividends.get_value::<f64>(
                                            &div_date,
                                            &StockDividendField::AllotNum.to_string(),
                                        ),
                                        year_dividends.get_value::<f64>(
                                            &div_date,
                                            &StockDividendField::AllotPrice.to_string(),
                                        ),
                                        year_dividends.get_value::<f64>(
                                            &div_date,
                                            &StockDividendField::StockBonus.to_string(),
                                        ),
                                        year_dividends.get_value::<f64>(
                                            &div_date,
                                            &StockDividendField::StockGift.to_string(),
                                        ),
                                    ) {
                                        let mut dividend = interest;

                                        if div_allot_weight != 0.0 && allot_num > 0.0 {
                                            if let Some((_, price_div)) = kline_no_adjust
                                                .get_latest_value::<f64>(
                                                    &div_date,
                                                    STALE_DAYS_SHORT,
                                                    true,
                                                    &KlineField::Close.to_string(),
                                                )
                                            {
                                                dividend += allot_num
                                                    * (price_div - allot_price)
                                                    * div_allot_weight
                                                    * price_no_adjust
                                                    / price_div;
                                            }
                                        }

                                        if div_bonus_gift_weight != 0.0
                                            && (stock_bonus > 0.0 || stock_gift > 0.0)
                                        {
                                            dividend += (stock_bonus + stock_gift)
                                                * price_no_adjust
                                                * div_bonus_gift_weight;
                                        }

                                        let weighted_dividend = dividend
                                            * (1.0 - div_weight_yearly_decay).powi(i as i32);
                                        dividends.push(weighted_dividend);
                                    }
                                }
                            }
                        }

                        if (dividends.len() as f64 / lookback_div_years as f64)
                            < min_div_count_per_year
                        {
                            continue;
                        }

                        let dv_ratio = dividends.iter().sum::<f64>()
                            / lookback_div_years as f64
                            / price_no_adjust;
                        if dv_ratio > 0.0 {
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
                                    &format!("[No Enough Data] {ticker}"),
                                    date,
                                    event_sender,
                                )
                                .await;
                                continue;
                            }

                            let momentum = calc_annualized_momentum(&prices, false);
                            let volatility = calc_annualized_volatility(&prices);

                            let report_pershare = fetch_stock_report_pershare(ticker).await?;
                            let roe_with_date = report_pershare.get_latest_value::<f64>(
                                date,
                                STALE_DAYS_LONG,
                                false,
                                &StockReportPershareField::Roe.to_string(),
                            );

                            if let Some(fail_factor_name) =
                                match (momentum, roe_with_date, volatility) {
                                    (None, _, _) => Some("momentum"),
                                    (_, None, _) => Some("roe"),
                                    (_, _, None) => Some("volatility"),
                                    (Some(momentum), Some((_, roe)), Some(volatility)) => {
                                        tickers_factors.push((
                                            ticker.clone(),
                                            Factors {
                                                dv_ratio,
                                                momentum,
                                                roe,
                                                volatility,
                                            },
                                        ));

                                        None
                                    }
                                }
                            {
                                rule_send_warning(
                                    rule_name,
                                    &format!("[Σ '{fail_factor_name}' Failed] {ticker}"),
                                    date,
                                    event_sender,
                                )
                                .await;
                            }
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

            let factors_momentum = tickers_factors
                .iter()
                .map(|(_, f)| f.momentum)
                .collect::<Vec<f64>>();
            let momentum_lower = quantile(&factors_momentum, momentum_quantile_lower);

            let factors_roe = tickers_factors
                .iter()
                .map(|(_, f)| f.roe)
                .collect::<Vec<f64>>();
            let roe_lower = quantile(&factors_roe, roe_quantile_lower);

            let factors_volatility = tickers_factors
                .iter()
                .map(|(_, f)| f.volatility)
                .collect::<Vec<f64>>();
            let volatility_upper = quantile(&factors_volatility, volatility_quantile_upper);

            let mut indicators: Vec<(Ticker, f64)> = vec![];
            for (ticker, factors) in tickers_factors {
                if let Some(momentum_lower) = momentum_lower {
                    if factors.momentum < momentum_lower {
                        continue;
                    }
                }

                if let Some(roe_lower) = roe_lower {
                    if factors.roe < roe_lower {
                        continue;
                    }
                }

                if let Some(volatility_upper) = volatility_upper {
                    if factors.volatility > volatility_upper {
                        continue;
                    }
                }

                indicators.push((ticker, factors.dv_ratio));
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
    dv_ratio: f64,
    momentum: f64,
    roe: f64,
    volatility: f64,
}
