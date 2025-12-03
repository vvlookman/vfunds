use std::{cmp::Ordering, collections::HashMap};

use async_trait::async_trait;
use chrono::{Datelike, Duration, NaiveDate};
use tokio::{sync::mpsc::Sender, time::Instant};

use crate::{
    PROGRESS_INTERVAL_SECS, REQUIRED_DATA_COMPLETENESS,
    error::VfResult,
    financial::{
        KlineField,
        stock::{
            StockDetail, StockDividendAdjust, StockDividendField, fetch_stock_detail,
            fetch_stock_dividends, fetch_stock_kline,
        },
    },
    rule::{
        BacktestEvent, FundBacktestContext, RuleDefinition, RuleExecutor,
        rule_notify_calc_progress, rule_notify_indicators, rule_send_warning,
    },
    ticker::Ticker,
    utils::{
        financial::{calc_annualized_return_rate, calc_annualized_volatility},
        math::signed_powf,
        stats::quantile,
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

        let arr_quantile_lower = self
            .options
            .get("arr_quantile_lower")
            .and_then(|v| v.as_f64())
            .unwrap_or(0.0);
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
        let price_avg_count = self
            .options
            .get("price_avg_count")
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
        let weight_exp = self
            .options
            .get("weight_exp")
            .and_then(|v| v.as_f64())
            .unwrap_or(0.0);
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

            if price_avg_count == 0 {
                panic!("price_avg_count must > 0");
            }

            if min_div_count_per_year <= 0.0 {
                panic!("min_div_count_per_year must > 0");
            }
        }

        let tickers_map = context.fund_definition.all_tickers_map(date).await?;
        if !tickers_map.is_empty() {
            let date_from = date
                .with_year(date.year() - lookback_div_years as i32)
                .unwrap()
                + Duration::days(1);

            let mut tickers_factors: Vec<(Ticker, Factors)> = vec![];
            {
                let mut last_time = Instant::now();
                let mut calc_count: usize = 0;

                for ticker in tickers_map.keys() {
                    calc_count += 1;

                    if context.portfolio.reserved_cash.contains_key(ticker) {
                        continue;
                    }

                    let kline = fetch_stock_kline(ticker, StockDividendAdjust::ForwardProp).await?;
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
                        < (lookback_trade_days as f64 * REQUIRED_DATA_COMPLETENESS).round() as usize
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

                    let kline_no_adjust =
                        fetch_stock_kline(ticker, StockDividendAdjust::No).await?;

                    let price_no_adjust = {
                        let prices: Vec<f64> = kline_no_adjust
                            .get_latest_values::<f64>(
                                date,
                                false,
                                &KlineField::Close.to_string(),
                                price_avg_count as u32,
                            )
                            .iter()
                            .map(|&(_, v)| v)
                            .collect();
                        prices.iter().sum::<f64>() / prices.len() as f64
                    };
                    if price_no_adjust > 0.0 {
                        let dividends = fetch_stock_dividends(ticker).await?;

                        let interest_values = dividends.get_values::<f64>(
                            &date_from,
                            date,
                            &StockDividendField::Interest.to_string(),
                        );

                        let mut total_income = interest_values.iter().map(|(_, v)| v).sum::<f64>();
                        let mut total_count =
                            interest_values.iter().filter(|(_, v)| *v > 0.0).count();

                        if div_bonus_gift_weight != 0.0 {
                            let mut free_shares_map: HashMap<NaiveDate, f64> = HashMap::new();

                            for (div_date, stock_bonus) in dividends.get_values::<f64>(
                                &date_from,
                                date,
                                &StockDividendField::StockBonus.to_string(),
                            ) {
                                free_shares_map
                                    .entry(div_date)
                                    .and_modify(|x| *x += stock_bonus)
                                    .or_insert(stock_bonus);
                            }

                            for (div_date, stock_gift) in dividends.get_values::<f64>(
                                &date_from,
                                date,
                                &StockDividendField::StockGift.to_string(),
                            ) {
                                free_shares_map
                                    .entry(div_date)
                                    .and_modify(|x| *x += stock_gift)
                                    .or_insert(stock_gift);
                            }

                            for (div_date, shares) in free_shares_map {
                                if shares > 0.0 {
                                    if let Some((_, price_no_adjust)) = kline_no_adjust
                                        .get_latest_value::<f64>(
                                            &div_date,
                                            true,
                                            &KlineField::Close.to_string(),
                                        )
                                    {
                                        total_income +=
                                            shares * price_no_adjust * div_bonus_gift_weight;
                                    }

                                    if div_bonus_gift_weight > 0.0 {
                                        total_count += 1;
                                    }
                                }
                            }
                        }

                        if div_allot_weight != 0.0 {
                            let allot_num_map = dividends
                                .get_values::<f64>(
                                    &date_from,
                                    date,
                                    &StockDividendField::AllotNum.to_string(),
                                )
                                .iter()
                                .copied()
                                .collect::<HashMap<NaiveDate, f64>>();

                            let allot_price_map = dividends
                                .get_values::<f64>(
                                    &date_from,
                                    date,
                                    &StockDividendField::AllotPrice.to_string(),
                                )
                                .iter()
                                .copied()
                                .collect::<HashMap<NaiveDate, f64>>();

                            for (div_date, allot_num) in allot_num_map {
                                if allot_num > 0.0 {
                                    if let Some(allot_price) = allot_price_map.get(&div_date) {
                                        if let Some((_, price_no_adjust)) = kline_no_adjust
                                            .get_latest_value::<f64>(
                                                &div_date,
                                                true,
                                                &KlineField::Close.to_string(),
                                            )
                                        {
                                            total_income += allot_num
                                                * (price_no_adjust - allot_price)
                                                * div_allot_weight;
                                        }
                                    }

                                    if div_allot_weight > 0.0 {
                                        total_count += 1;
                                    }
                                }
                            }
                        }

                        if total_income > 0.0
                            && (total_count as f64 / lookback_div_years as f64)
                                >= min_div_count_per_year
                        {
                            let dv_ratio =
                                total_income / lookback_div_years as f64 / price_no_adjust;

                            if dv_ratio.is_finite() {
                                let arr = calc_annualized_return_rate(&prices);
                                let volatility = calc_annualized_volatility(&prices);

                                if let Some(fail_factor_name) = match (arr, volatility) {
                                    (None, _) => Some("arr"),
                                    (_, None) => Some("volatility"),
                                    (Some(arr), Some(volatility)) => {
                                        tickers_factors.push((
                                            ticker.clone(),
                                            Factors {
                                                dv_ratio,
                                                arr,
                                                volatility,
                                            },
                                        ));

                                        None
                                    }
                                } {
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

            let factors_arr = tickers_factors
                .iter()
                .map(|(_, f)| f.arr)
                .collect::<Vec<f64>>();
            let arr_lower = quantile(&factors_arr, arr_quantile_lower);

            let factors_volatility = tickers_factors
                .iter()
                .map(|(_, f)| f.volatility)
                .collect::<Vec<f64>>();
            let volatility_upper = quantile(&factors_volatility, volatility_quantile_upper);

            let mut indicators: Vec<(Ticker, f64)> = vec![];
            for (ticker, factors) in tickers_factors {
                if let Some(arr_lower) = arr_lower {
                    if factors.arr < arr_lower {
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
            indicators.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(Ordering::Equal));

            let top_indicators = indicators
                .iter()
                .take(3 * limit as usize)
                .collect::<Vec<_>>();

            let mut tickers_detail: HashMap<Ticker, StockDetail> = HashMap::new();
            if skip_same_sector {
                for (ticker, _) in &top_indicators {
                    let detail = fetch_stock_detail(ticker).await?;
                    tickers_detail.insert(ticker.clone(), detail);
                }
            }

            let mut targets_indicator: Vec<(Ticker, f64)> = vec![];
            let mut candidates_indicator: Vec<(Ticker, f64)> = vec![];
            for (ticker, indicator) in &top_indicators {
                if targets_indicator.len() < limit as usize {
                    if skip_same_sector
                        && targets_indicator.iter().any(|(a, _)| {
                            if let (Some(Some(sector_a)), Some(Some(sector_b))) = (
                                tickers_detail.get(a).map(|v| &v.sector),
                                tickers_detail.get(ticker).map(|v| &v.sector),
                            ) {
                                sector_a == sector_b
                            } else {
                                false
                            }
                        })
                    {
                        candidates_indicator.push((ticker.clone(), *indicator));
                    } else {
                        targets_indicator.push((ticker.clone(), *indicator));
                    }
                } else {
                    candidates_indicator.push((ticker.clone(), *indicator));
                }
            }

            rule_notify_indicators(
                rule_name,
                &targets_indicator
                    .iter()
                    .map(|&(ref t, v)| (t.clone(), format!("{v:.4}")))
                    .collect::<Vec<_>>(),
                &candidates_indicator
                    .iter()
                    .map(|&(ref t, v)| (t.clone(), format!("{v:.4}")))
                    .collect::<Vec<_>>(),
                date,
                event_sender,
            )
            .await;

            let mut targets_weight: Vec<(Ticker, f64)> = vec![];
            for (ticker, indicator) in &targets_indicator {
                if let Some((weight, _)) = tickers_map.get(ticker) {
                    targets_weight.push((
                        ticker.clone(),
                        (*weight) * signed_powf(*indicator, weight_exp),
                    ));
                }
            }

            context
                .rebalance(&targets_weight, date, event_sender)
                .await?;
        }

        Ok(())
    }
}

#[derive(Debug)]
struct Factors {
    dv_ratio: f64,
    arr: f64,
    volatility: f64,
}
