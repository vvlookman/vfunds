use std::{cmp::Ordering, collections::HashMap};

use async_trait::async_trait;
use chrono::{Datelike, Duration, NaiveDate};
use log::debug;
use tokio::{sync::mpsc::Sender, time::Instant};

use crate::{
    PROGRESS_INTERVAL_SECS,
    error::VfResult,
    financial::{
        KlineField,
        stock::{
            StockDetail, StockDividendAdjust, StockDividendField, StockReportPershareField,
            fetch_stock_detail, fetch_stock_dividends, fetch_stock_kline,
            fetch_stock_report_pershare,
        },
    },
    rule::{
        BacktestEvent, FundBacktestContext, RuleDefinition, RuleExecutor, notify_calc_progress,
        notify_tickers_indicator,
    },
    ticker::Ticker,
    utils::datetime::date_to_str,
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
        event_sender: Sender<BacktestEvent>,
    ) -> VfResult<()> {
        let rule_name = mod_name!();

        let enable_indicator_weighting = self
            .options
            .get("enable_indicator_weighting")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let limit = self
            .options
            .get("limit")
            .and_then(|v| v.as_u64())
            .unwrap_or(10);
        let lookback_years = self
            .options
            .get("lookback_years")
            .and_then(|v| v.as_u64())
            .unwrap_or(3);
        let min_div_count_per_year = self
            .options
            .get("min_div_count_per_year")
            .and_then(|v| v.as_f64())
            .unwrap_or(1.0);
        let min_roe = self
            .options
            .get("min_roe")
            .and_then(|v| v.as_f64())
            .unwrap_or(0.0);
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
        let weight_allot = self
            .options
            .get("weight_allot")
            .and_then(|v| v.as_f64())
            .unwrap_or(0.0);
        let weight_bonus_gift = self
            .options
            .get("weight_bonus_gift")
            .and_then(|v| v.as_f64())
            .unwrap_or(0.0);
        {
            if limit == 0 {
                panic!("limit must > 0");
            }

            if lookback_years == 0 {
                panic!("lookback_years must > 0");
            }

            if price_avg_count == 0 {
                panic!("price_avg_count must > 0");
            }

            if min_div_count_per_year <= 0.0 {
                panic!("min_div_count_per_year must > 0");
            }

            if min_roe < 0.0 {
                panic!("min_roe must >= 0");
            }
        }

        let tickers_map = context.fund_definition.all_tickers_map(date).await?;
        if !tickers_map.is_empty() {
            let date_str = date_to_str(date);
            let date_from =
                date.with_year(date.year() - lookback_years as i32).unwrap() + Duration::days(1);

            let mut indicators: Vec<(Ticker, f64)> = vec![];
            {
                let mut last_time = Instant::now();
                let mut calc_count: usize = 0;
                for ticker in tickers_map.keys() {
                    let kline = fetch_stock_kline(ticker, StockDividendAdjust::No).await?;
                    let prices: Vec<f64> = kline
                        .get_latest_values::<f64>(
                            date,
                            false,
                            &KlineField::Close.to_string(),
                            price_avg_count as u32,
                        )
                        .iter()
                        .map(|&(_, v)| v)
                        .collect();

                    let price = prices.iter().sum::<f64>() / prices.len() as f64;
                    if price > 0.0 {
                        let filter_roe = if min_roe > 0.0 {
                            let report_pershare = fetch_stock_report_pershare(ticker).await?;
                            let roe = report_pershare
                                .get_latest_value::<f64>(
                                    date,
                                    false,
                                    &StockReportPershareField::RoeRate.to_string(),
                                )
                                .map(|(_, v)| v)
                                .unwrap_or(0.0)
                                / 100.0;

                            roe > min_roe
                        } else {
                            true
                        };

                        if filter_roe {
                            let dividends = fetch_stock_dividends(ticker).await?;

                            let interest_values = dividends.get_values::<f64>(
                                &date_from,
                                date,
                                &StockDividendField::Interest.to_string(),
                            );

                            let mut total_income =
                                interest_values.iter().map(|(_, v)| v).sum::<f64>();
                            let mut total_count =
                                interest_values.iter().filter(|(_, v)| *v > 0.0).count();

                            if weight_bonus_gift != 0.0 {
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
                                        if let Some((_, price)) = kline.get_latest_value::<f64>(
                                            &div_date,
                                            true,
                                            &KlineField::Close.to_string(),
                                        ) {
                                            total_income += shares * price * weight_bonus_gift;
                                        }

                                        if weight_bonus_gift > 0.0 {
                                            total_count += 1;
                                        }
                                    }
                                }
                            }

                            if weight_allot != 0.0 {
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
                                            if let Some((_, price)) = kline.get_latest_value::<f64>(
                                                &div_date,
                                                true,
                                                &KlineField::Close.to_string(),
                                            ) {
                                                total_income += allot_num
                                                    * (price - allot_price)
                                                    * weight_allot;
                                            }
                                        }

                                        if weight_allot > 0.0 {
                                            total_count += 1;
                                        }
                                    }
                                }
                            }

                            if total_income > 0.0
                                && (total_count as f64 / lookback_years as f64)
                                    >= min_div_count_per_year
                            {
                                let indicator = total_income / price;
                                debug!(
                                    "[{date_str}] [{rule_name}] {ticker}={indicator:.4}({total_count})"
                                );

                                indicators.push((ticker.clone(), indicator));
                            }
                        }
                    }

                    calc_count += 1;

                    if last_time.elapsed().as_secs() > PROGRESS_INTERVAL_SECS {
                        notify_calc_progress(
                            event_sender.clone(),
                            date,
                            rule_name,
                            calc_count as f64 / tickers_map.len() as f64 * 100.0,
                        )
                        .await;

                        last_time = Instant::now();
                    }
                }

                notify_calc_progress(event_sender.clone(), date, rule_name, 100.0).await;
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

            notify_tickers_indicator(
                event_sender.clone(),
                date,
                rule_name,
                &targets_indicator
                    .iter()
                    .map(|&(ref t, v)| (t.clone(), format!("{v:.4}")))
                    .collect::<Vec<_>>(),
                &candidates_indicator
                    .iter()
                    .map(|&(ref t, v)| (t.clone(), format!("{v:.4}")))
                    .collect::<Vec<_>>(),
            )
            .await;

            let mut targets_weight: Vec<(Ticker, f64)> = vec![];
            for (ticker, indicator) in &targets_indicator {
                if let Some((weight, _)) = tickers_map.get(ticker) {
                    targets_weight.push((
                        ticker.clone(),
                        (*weight)
                            * if enable_indicator_weighting {
                                *indicator
                            } else {
                                1.0
                            },
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
