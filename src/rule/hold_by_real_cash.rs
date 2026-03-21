use async_trait::async_trait;
use chrono::NaiveDate;
use tokio::{sync::mpsc::Sender, time::Instant};

use crate::{
    PROGRESS_INTERVAL_SECS, REQUIRED_DATA_COMPLETENESS, STALE_DAYS_LONG,
    error::VfResult,
    filter::{filter_invalid::has_invalid_price, filter_st::is_st},
    financial::{
        KlineField,
        helper::{
            calc_stock_cash_ratio, calc_stock_current_ratio, calc_stock_dividend_ratio_of_years,
            calc_stock_free_cash_ratio_of_years, calc_stock_roe_of_years,
        },
        stock::{StockDividendAdjust, fetch_stock_detail, fetch_stock_kline},
    },
    rule::{
        BacktestEvent, FundBacktestContext, RuleDefinition, RuleExecutor, calc_weights,
        rule_notify_calc_progress, rule_notify_indicators, rule_send_info, rule_send_warning,
        select_by_indicators,
    },
    spec::RuleOptions,
    ticker::Ticker,
    utils::{
        financial::{calc_annualized_momentum, calc_annualized_volatility_mad},
        math::normalize_zscore,
        stats::quantile_value,
    },
};

pub struct Executor {
    #[allow(dead_code)]
    options: RuleOptions,
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

        let adjust_dividend_ratio_weight =
            self.options
                .read_f64_gte("adjust_dividend_ratio_weight", 0.0, 0.0);
        let adjust_momentum_weight = self
            .options
            .read_f64_gte("adjust_momentum_weight", 0.0, 0.0);
        let adjust_volatility_weight =
            self.options
                .read_f64_gte("adjust_volatility_weight", 0.0, 0.0);
        let cash_ratio_quantile_lower =
            self.options
                .read_f64_in_range("cash_ratio_quantile_lower", 0.0, 0.0..=1.0);
        let current_ratio_quantile_lower =
            self.options
                .read_f64_in_range("current_ratio_quantile_lower", 0.0, 0.0..=1.0);
        let dividend_ratio_quantile_lower =
            self.options
                .read_f64_in_range("dividend_ratio_quantile_lower", 0.0, 0.0..=1.0);
        let exclude_sectors: Vec<String> = self
            .options
            .read_array("exclude_sectors")
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str())
                    .map(|s| s.to_string())
                    .collect::<Vec<String>>()
            })
            .unwrap_or_default();
        let limit = self.options.read_u64_no_zero("limit", 5);
        let lookback_trade_days = self.options.read_u64_no_zero("lookback_trade_days", 250);
        let roe_quantile_lower =
            self.options
                .read_f64_in_range("roe_quantile_lower", 0.0, 0.0..=1.0);
        let roe_years = self.options.read_u64_no_zero("roe_years", 3);
        let skip_same_sector = self.options.read_bool("skip_same_sector", false);
        let weight_method = self.options.read_str("weight_method", "equal");

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

                    if let Ok(cash_ratio) = calc_stock_cash_ratio(ticker, date).await
                        && let Ok(current_ratio) = calc_stock_current_ratio(ticker, date).await
                        && let Ok(dividend_ratio) =
                            calc_stock_dividend_ratio_of_years(ticker, date, 1).await
                        && let Ok(free_cash_ratio) =
                            calc_stock_free_cash_ratio_of_years(ticker, date, 1).await
                        && let Some(roe) = calc_stock_roe_of_years(ticker, date, roe_years as u32)
                            .await
                            .ok()
                            .flatten()
                    {
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

                        if let Some(momentum) = calc_annualized_momentum(&prices, false)
                            && let Some(volatility) = calc_annualized_volatility_mad(&prices)
                        {
                            tickers_factors.push((
                                ticker.clone(),
                                Factors {
                                    cash_ratio,
                                    current_ratio,
                                    dividend_ratio,
                                    free_cash_ratio,
                                    momentum,
                                    roe,
                                    volatility,
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
            let cash_ratio_lower = quantile_value(&factors_cash_ratio, cash_ratio_quantile_lower);

            let factors_current_ratio = tickers_factors
                .iter()
                .map(|(_, f)| f.current_ratio)
                .collect::<Vec<f64>>();
            let current_ratio_lower =
                quantile_value(&factors_current_ratio, current_ratio_quantile_lower);

            let factors_dividend_ratio = tickers_factors
                .iter()
                .map(|(_, f)| f.dividend_ratio)
                .collect::<Vec<f64>>();
            let dividend_ratio_lower =
                quantile_value(&factors_dividend_ratio, dividend_ratio_quantile_lower);
            let normalized_factors_dividend_ratio = normalize_zscore(&factors_dividend_ratio);

            let factors_free_cash_ratio = tickers_factors
                .iter()
                .map(|(_, f)| f.free_cash_ratio)
                .collect::<Vec<f64>>();
            let normalized_factors_free_cash_ratio = normalize_zscore(&factors_free_cash_ratio);

            let factors_momentum = tickers_factors
                .iter()
                .map(|(_, f)| f.momentum)
                .collect::<Vec<f64>>();
            let normalized_factors_momentum = normalize_zscore(&factors_momentum);

            let factors_roe = tickers_factors
                .iter()
                .map(|(_, f)| f.roe)
                .collect::<Vec<f64>>();
            let roe_lower = quantile_value(&factors_roe, roe_quantile_lower);

            let factors_volatility = tickers_factors
                .iter()
                .map(|(_, f)| f.volatility)
                .collect::<Vec<f64>>();
            let normalized_factors_volatility = normalize_zscore(&factors_volatility);

            let mut indicators: Vec<(Ticker, f64)> = vec![];
            for (i, (ticker, factors)) in tickers_factors.iter().enumerate() {
                if let Some(cash_ratio_lower) = cash_ratio_lower
                    && let Some(current_ratio_lower) = current_ratio_lower
                    && let Some(dividend_ratio_lower) = dividend_ratio_lower
                    && let Some(roe_lower) = roe_lower
                {
                    if factors.cash_ratio > cash_ratio_lower
                        && factors.current_ratio > current_ratio_lower
                        && factors.dividend_ratio > dividend_ratio_lower
                        && factors.roe > roe_lower
                    {
                        let normalized_dividend_ratio = normalized_factors_dividend_ratio[i];
                        let normalized_free_cash_ratio = normalized_factors_free_cash_ratio[i];
                        let normalized_momentum = normalized_factors_momentum[i];
                        let normalized_volatility = normalized_factors_volatility[i];

                        let indicator = normalized_free_cash_ratio
                            * (1.0
                                + adjust_dividend_ratio_weight * normalized_dividend_ratio
                                + adjust_momentum_weight * normalized_momentum
                                - adjust_volatility_weight * normalized_volatility);

                        indicators.push((ticker.clone(), indicator));
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
    momentum: f64,
    roe: f64,
    volatility: f64,
}
