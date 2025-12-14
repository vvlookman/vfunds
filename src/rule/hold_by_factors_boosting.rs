use std::{cmp::Ordering, collections::HashMap};

use async_trait::async_trait;
use chrono::NaiveDate;
use log::debug;
use smartcore::{
    linalg::basic::{arrays::Array, matrix::DenseMatrix},
    metrics::{mean_absolute_error, r2},
    xgboost::{XGRegressor, XGRegressorParameters},
};
use tokio::{sync::mpsc::Sender, time::Instant};

use crate::{
    CANDIDATE_TICKER_RATIO, PROGRESS_INTERVAL_SECS,
    error::VfResult,
    financial::{
        KlineField,
        stock::{
            StockDetail, StockDividendAdjust, StockReportCapitalField, fetch_stock_detail,
            fetch_stock_kline, fetch_stock_report_capital,
        },
    },
    rule::{
        BacktestEvent, FundBacktestContext, RuleDefinition, RuleExecutor, calc_weights,
        rule_notify_calc_progress, rule_notify_indicators,
    },
    ticker::Ticker,
    utils::{datetime::date_to_str, financial::*, math::normalize_zscore},
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

        let bbands_multiplier = self
            .options
            .get("bbands_multiplier")
            .and_then(|v| v.as_f64())
            .unwrap_or(2.0);
        let bbands_period = self
            .options
            .get("bbands_period")
            .and_then(|v| v.as_u64())
            .unwrap_or(20);
        let limit = self
            .options
            .get("limit")
            .and_then(|v| v.as_u64())
            .unwrap_or(10);
        let metric_r2_threshold = self
            .options
            .get("metric_r2_threshold")
            .and_then(|v| v.as_f64())
            .unwrap_or(0.8);
        let rsi_period = self
            .options
            .get("rsi_period")
            .and_then(|v| v.as_u64())
            .unwrap_or(14);
        let score_arr_weight = self
            .options
            .get("score_arr_weight")
            .and_then(|v| v.as_f64())
            .unwrap_or(0.6);
        let skip_same_sector = self
            .options
            .get("skip_same_sector")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let step_trade_days = self
            .options
            .get("step_trade_days")
            .and_then(|v| v.as_u64())
            .unwrap_or(20);
        let steps = self
            .options
            .get("steps")
            .and_then(|v| v.as_u64())
            .unwrap_or(10);
        let weight_method = self
            .options
            .get("weight_method")
            .and_then(|v| v.as_str())
            .unwrap_or("equal");
        let xgboost_gamma = self
            .options
            .get("xgboost_gamma")
            .and_then(|v| v.as_f64())
            .unwrap_or(0.0);
        let xgboost_lambda = self
            .options
            .get("xgboost_lambda")
            .and_then(|v| v.as_f64())
            .unwrap_or(1.0);
        let xgboost_learning_rate = self
            .options
            .get("xgboost_learning_rate")
            .and_then(|v| v.as_f64())
            .unwrap_or(0.1);
        let xgboost_max_depth = self
            .options
            .get("xgboost_max_depth")
            .and_then(|v| v.as_u64())
            .unwrap_or(3);
        let xgboost_min_child_weight = self
            .options
            .get("xgboost_min_child_weight")
            .and_then(|v| v.as_u64())
            .unwrap_or(3);
        let xgboost_n_estimators = self
            .options
            .get("xgboost_n_estimators")
            .and_then(|v| v.as_u64())
            .unwrap_or(50);
        {
            if limit == 0 {
                panic!("limit must > 0");
            }

            if !(0.0..=1.0).contains(&score_arr_weight) {
                panic!("score_arr_weight must >= 0 and <= 1");
            }

            if step_trade_days == 0 {
                panic!("step_trade_days must > 0");
            }

            if steps == 0 {
                panic!("steps must > 0");
            }
        }

        let tickers_map = context.fund_definition.all_tickers_map(date).await?;
        if !tickers_map.is_empty() {
            let date_str = date_to_str(date);

            let mut tickers_train_factors_metrics: Vec<(Ticker, Vec<f64>, f64, f64)> = vec![];
            let mut tickers_test_factors: Vec<(Ticker, Vec<f64>)> = vec![];
            {
                let mut last_time = Instant::now();
                let mut calc_count: usize = 0;

                for ticker in tickers_map.keys() {
                    calc_count += 1;

                    if context.portfolio.reserved_cash.contains_key(ticker) {
                        continue;
                    }

                    let kline = fetch_stock_kline(ticker, StockDividendAdjust::ForwardProp).await?;
                    let score_prices_with_date = kline.get_latest_values::<f64>(
                        date,
                        false,
                        &KlineField::Close.to_string(),
                        step_trade_days as u32,
                    );
                    if let Some((score_start_date, _)) = score_prices_with_date.first() {
                        let train_factors: Vec<f64> = calc_factors(
                            ticker,
                            score_start_date,
                            step_trade_days as usize,
                            steps as usize,
                            bbands_multiplier,
                            bbands_period as usize,
                            rsi_period as usize,
                        )
                        .await?;

                        let test_factors: Vec<f64> = calc_factors(
                            ticker,
                            date,
                            step_trade_days as usize,
                            steps as usize,
                            bbands_multiplier,
                            bbands_period as usize,
                            rsi_period as usize,
                        )
                        .await?;

                        let score_prices: Vec<f64> =
                            score_prices_with_date.iter().map(|&(_, v)| v).collect();
                        let score_arr = calc_annualized_return_rate(&score_prices)
                            .map(|v| if v.is_finite() { v } else { 0.0 })
                            .unwrap_or(0.0);
                        let score_sharpe = calc_sharpe_ratio(&score_prices, 0.0)
                            .map(|v| if v.is_finite() { v } else { 0.0 })
                            .unwrap_or(0.0);

                        tickers_train_factors_metrics.push((
                            ticker.clone(),
                            train_factors,
                            score_arr,
                            score_sharpe,
                        ));
                        tickers_test_factors.push((ticker.clone(), test_factors));
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

            let valid_tickers_train_factors_metrics: Vec<_> = tickers_train_factors_metrics
                .iter()
                .filter(|(_, factors, _, _)| {
                    !factors.is_empty() && !factors.iter().any(|v| v.is_nan() || v.is_infinite())
                })
                .collect();
            let valid_tickers_test_factors: Vec<_> = tickers_test_factors
                .iter()
                .filter(|(_, factors)| {
                    !factors.is_empty() && !factors.iter().any(|v| v.is_nan() || v.is_infinite())
                })
                .collect();

            if let (Ok(x_train), Ok(x_test)) = (
                DenseMatrix::from_2d_array(
                    &valid_tickers_train_factors_metrics
                        .iter()
                        .map(|v| v.1.as_slice())
                        .collect::<Vec<&[f64]>>(),
                ),
                DenseMatrix::from_2d_array(
                    &valid_tickers_test_factors
                        .iter()
                        .map(|v| v.1.as_slice())
                        .collect::<Vec<&[f64]>>(),
                ),
            ) {
                let score_arr_values = normalize_zscore(
                    &valid_tickers_train_factors_metrics
                        .iter()
                        .map(|x| x.2)
                        .collect::<Vec<f64>>(),
                );
                let score_sharpe_values = normalize_zscore(
                    &valid_tickers_train_factors_metrics
                        .iter()
                        .map(|x| x.3)
                        .collect::<Vec<f64>>(),
                );
                let y_train = score_arr_values
                    .iter()
                    .zip(score_sharpe_values.iter())
                    .map(|(score_arr, score_sharpe)| {
                        score_arr * score_arr_weight + score_sharpe * (1.0 - score_arr_weight)
                    })
                    .collect::<Vec<f64>>();

                let parameters = XGRegressorParameters::default()
                    .with_gamma(xgboost_gamma)
                    .with_lambda(xgboost_lambda)
                    .with_learning_rate(xgboost_learning_rate)
                    .with_max_depth(xgboost_max_depth as u16)
                    .with_min_child_weight(xgboost_min_child_weight as usize)
                    .with_n_estimators(xgboost_n_estimators as usize)
                    .with_seed(0)
                    .with_subsample(1.0);

                if let Ok(model) = XGRegressor::fit(&x_train, &y_train, parameters) {
                    if let (Ok(y_train_pred), Ok(y_test_pred)) =
                        (model.predict(&x_train), model.predict(&x_test))
                    {
                        let r2_score = r2(&y_train, &y_train_pred);
                        debug!(
                            "[{date_str}] R2={r2_score:.4} MAE={:.4} SHAPE={:?}",
                            mean_absolute_error(&y_train, &y_train_pred),
                            x_train.shape(),
                        );

                        let mut indicators: Vec<(Ticker, f64)> =
                            if r2_score > metric_r2_threshold && r2_score < 1.0 - 1e-8 {
                                valid_tickers_test_factors
                                    .iter()
                                    .enumerate()
                                    .filter_map(|(i, x)| {
                                        let ticker = &x.0;

                                        let indicator = y_test_pred[i];
                                        debug!("[{date_str}] {ticker}={indicator:.4}");

                                        if indicator > 0.0 {
                                            Some((ticker.clone(), indicator))
                                        } else {
                                            None
                                        }
                                    })
                                    .collect()
                            } else {
                                vec![]
                            };

                        indicators.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(Ordering::Equal));

                        let top_indicators = indicators
                            .iter()
                            .take((CANDIDATE_TICKER_RATIO + 1) * limit as usize)
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

                        let weights = calc_weights(&targets_indicator, weight_method)?;
                        context.rebalance(&weights, date, event_sender).await?;
                    }
                }
            }
        }

        Ok(())
    }
}

async fn calc_factors(
    ticker: &Ticker,
    end_date: &NaiveDate,
    step_trade_days: usize,
    steps: usize,
    bbands_multiplier: f64,
    bbands_period: usize,
    rsi_period: usize,
) -> VfResult<Vec<f64>> {
    let mut factors: Vec<f64> = vec![];

    let kline = fetch_stock_kline(ticker, StockDividendAdjust::ForwardProp).await?;
    let report_capital = fetch_stock_report_capital(ticker).await?;

    for i in 1..=steps {
        let lookback_trade_days = step_trade_days * i;

        let prices: Vec<f64> = kline
            .get_latest_values::<f64>(
                end_date,
                false,
                &KlineField::Close.to_string(),
                lookback_trade_days as u32,
            )
            .iter()
            .map(|&(_, v)| v)
            .collect();

        let volumes: Vec<f64> = kline
            .get_latest_values::<f64>(
                end_date,
                false,
                &KlineField::Volume.to_string(),
                lookback_trade_days as u32,
            )
            .iter()
            .map(|&(_, v)| v)
            .collect();
        let volumes_avg = volumes.iter().sum::<f64>() / volumes.len() as f64;

        factors.push(prices.last().map(|x| x.ln()).unwrap_or(f64::NAN));
        factors.push(volumes.last().map(|x| x / volumes_avg).unwrap_or(f64::NAN));
        factors.push(calc_annualized_return_rate(&prices).unwrap_or(f64::NAN));
        factors.push(calc_annualized_volatility(&prices).unwrap_or(f64::NAN));
        factors.push(
            calc_bollinger_band_position(&prices, bbands_period, bbands_multiplier)
                .unwrap_or(f64::NAN),
        );
        factors.push(calc_annualized_momentum(&prices).unwrap_or(f64::NAN));
        factors.push(
            calc_rsi(&prices, rsi_period)
                .last()
                .copied()
                .unwrap_or(f64::NAN),
        );
        factors.push(calc_sharpe_ratio(&prices, 0.0).unwrap_or(f64::NAN));

        if let Some((_, circulating_capital)) = report_capital.get_latest_value::<f64>(
            end_date,
            false,
            &StockReportCapitalField::Circulating.to_string(),
        ) {
            let turnover_ratio = volumes_avg / circulating_capital;
            factors.push(turnover_ratio);
        } else {
            factors.push(f64::NAN);
        }
    }

    Ok(factors)
}
