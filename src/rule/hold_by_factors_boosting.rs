use std::collections::HashMap;

use async_trait::async_trait;
use chrono::{Days, NaiveDate};
use smartcore::{
    linalg::basic::matrix::DenseMatrix,
    metrics::r2,
    xgboost::{XGRegressor, XGRegressorParameters},
};
use tokio::{sync::mpsc::Sender, time::Instant};

use crate::{
    PROGRESS_INTERVAL_SECS, STALE_DAYS_LONG, STALE_DAYS_SHORT, TRADE_DAYS_FRACTION,
    error::VfResult,
    financial::{
        KlineField,
        stock::{
            StockDividendAdjust, StockIndicatorField, StockReportPershareField,
            fetch_stock_indicators, fetch_stock_kline, fetch_stock_report_pershare,
        },
    },
    rule::{
        BacktestEvent, FundBacktestContext, RuleDefinition, RuleExecutor, calc_weights,
        rule_notify_calc_progress, rule_notify_indicators, rule_send_info, rule_send_warning,
        select_by_indicators,
    },
    ticker::Ticker,
    utils::{
        financial::*,
        smartcore::{validate_array, validate_matrix},
    },
};

pub struct Executor {
    #[allow(dead_code)]
    options: HashMap<String, serde_json::Value>,

    frequency_days: u64,
}

impl Executor {
    pub fn new(definition: &RuleDefinition) -> Self {
        Self {
            options: definition.options.clone(),

            frequency_days: definition.frequency.to_days(),
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
        let train_trade_days = self
            .options
            .get("train_trade_days")
            .and_then(|v| v.as_u64())
            .unwrap_or(60);
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
        }

        let predict_trade_days = (self.frequency_days as f64 * TRADE_DAYS_FRACTION).round() as u32;

        let tickers_map = context.fund_definition.all_tickers_map(date).await?;
        if !tickers_map.is_empty() {
            let mut indicators: Vec<(Ticker, f64)> = vec![];
            {
                let mut last_time = Instant::now();
                let mut calc_count: usize = 0;

                for ticker in tickers_map.keys() {
                    calc_count += 1;

                    if context.portfolio.reserved_cash.contains_key(ticker) {
                        continue;
                    }

                    let kline = fetch_stock_kline(ticker, StockDividendAdjust::Backward).await?;

                    let mut factors_and_score: Vec<(Vec<f64>, f64)> = vec![];
                    let mut ticker_factor_invalid: bool = false;
                    'calc_factors_loop: for i in 0..train_trade_days {
                        let post_train_prices_with_date = kline.get_latest_values::<f64>(
                            date,
                            false,
                            &KlineField::Close.to_string(),
                            predict_trade_days + i as u32,
                        );
                        let post_predict_prices_with_date = kline.get_latest_values::<f64>(
                            date,
                            false,
                            &KlineField::Close.to_string(),
                            i as u32,
                        );
                        if let Some((post_train_start_date, _)) =
                            post_train_prices_with_date.first()
                            && let Some((post_predict_start_date, _)) =
                                post_predict_prices_with_date.first()
                        {
                            let factors = calc_factors(ticker, post_train_start_date).await?;
                            for (factor_name, v) in &factors {
                                if !v.is_finite() {
                                    ticker_factor_invalid = true;

                                    rule_send_warning(
                                        rule_name,
                                        &format!("[Factor {factor_name} Invalid] {ticker}"),
                                        date,
                                        event_sender,
                                    )
                                    .await;

                                    break 'calc_factors_loop;
                                }
                            }

                            let score_prices_with_date = kline
                                .slice_by_date_range(
                                    post_train_start_date,
                                    &(*post_predict_start_date - Days::new(1)),
                                )?
                                .get_values::<f64>(&KlineField::Close.to_string());
                            let score_prices: Vec<f64> =
                                score_prices_with_date.iter().map(|&(_, v)| v).collect();
                            if let Some(score) = calc_annualized_momentum(&score_prices, false) {
                                factors_and_score
                                    .push((factors.iter().map(|(_, v)| *v).collect(), score));
                            } else {
                                rule_send_warning(
                                    rule_name,
                                    &format!("[Calc Score Failed] {ticker}"),
                                    date,
                                    event_sender,
                                )
                                .await;
                            }
                        }
                    }

                    if !ticker_factor_invalid {
                        let train_len = (factors_and_score.len() as f64 * 0.8).floor() as usize;
                        let train_factors_and_score = factors_and_score[0..train_len].to_vec();
                        let test_factors_and_score = factors_and_score[train_len..].to_vec();

                        if !train_factors_and_score.is_empty() && !test_factors_and_score.is_empty()
                        {
                            if let Ok(x_train) = DenseMatrix::from_2d_array(
                                &train_factors_and_score
                                    .iter()
                                    .map(|(factors, _)| factors.as_slice())
                                    .collect::<Vec<&[f64]>>(),
                            ) && let Ok(x_test) = DenseMatrix::from_2d_array(
                                &test_factors_and_score
                                    .iter()
                                    .map(|(factors, _)| factors.as_slice())
                                    .collect::<Vec<&[f64]>>(),
                            ) {
                                let y_train = train_factors_and_score
                                    .iter()
                                    .map(|(_, score)| *score)
                                    .collect::<Vec<f64>>();
                                let y_test = test_factors_and_score
                                    .iter()
                                    .map(|(_, score)| *score)
                                    .collect::<Vec<f64>>();

                                if validate_matrix(&x_train).is_ok()
                                    && validate_matrix(&x_test).is_ok()
                                    && validate_array(&y_train).is_ok()
                                    && validate_array(&y_test).is_ok()
                                {
                                    let parameters = XGRegressorParameters::default()
                                        .with_gamma(xgboost_gamma)
                                        .with_lambda(xgboost_lambda)
                                        .with_learning_rate(xgboost_learning_rate)
                                        .with_max_depth(xgboost_max_depth as u16)
                                        .with_min_child_weight(xgboost_min_child_weight as usize)
                                        .with_n_estimators(xgboost_n_estimators as usize)
                                        .with_seed(0)
                                        .with_subsample(1.0);

                                    if let Ok(model) =
                                        XGRegressor::fit(&x_train, &y_train, parameters)
                                    {
                                        if let Ok(y_test_pred) = model.predict(&x_test) {
                                            let r2_score = r2(&y_test, &y_test_pred);
                                            if r2_score > metric_r2_threshold
                                                && r2_score < 1.0 - 1e-8
                                            {
                                                let predict_factors =
                                                    calc_factors(ticker, date).await?;
                                                if !predict_factors.is_empty()
                                                    && !predict_factors
                                                        .iter()
                                                        .any(|(_, v)| !v.is_finite())
                                                {
                                                    if let Ok(x_predict) =
                                                        DenseMatrix::from_2d_vec(&vec![
                                                            predict_factors
                                                                .iter()
                                                                .map(|(_, factor)| *factor)
                                                                .collect(),
                                                        ])
                                                    {
                                                        if let Ok(y_predict) =
                                                            model.predict(&x_predict)
                                                        {
                                                            if let Some(score) = y_predict.first() {
                                                                if *score > 0.0 {
                                                                    let indicator =
                                                                        score * r2_score;
                                                                    indicators.push((
                                                                        ticker.clone(),
                                                                        indicator,
                                                                    ));
                                                                }
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
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
            indicators.sort_by(|a, b| b.1.total_cmp(&a.1));

            rule_send_info(
                rule_name,
                &format!("[Universe] {}({})", tickers_map.len(), indicators.len()),
                date,
                event_sender,
            )
            .await;

            let (targets_indicators, candidates_indicators) =
                select_by_indicators(&indicators, limit as usize, false).await?;

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

async fn calc_factors(ticker: &Ticker, end_date: &NaiveDate) -> VfResult<Vec<(String, f64)>> {
    let mut factors: Vec<(String, f64)> = vec![];

    let kline = fetch_stock_kline(ticker, StockDividendAdjust::Backward).await?;
    let indicators = fetch_stock_indicators(ticker).await?;
    let report_pershare = fetch_stock_report_pershare(ticker).await?;

    // Momentum
    {
        for days in &[20, 40, 60, 120, 240] {
            let prices_days: Vec<f64> = kline
                .get_latest_values::<f64>(end_date, false, &KlineField::Close.to_string(), *days)
                .iter()
                .map(|&(_, v)| v)
                .collect();
            factors.push((
                format!("Momentum{days}T"),
                calc_annualized_momentum(&prices_days, false).unwrap_or(f64::NAN),
            ));
        }
    }

    // Valuation
    {
        for field in &[
            StockIndicatorField::Pb,
            StockIndicatorField::Pe,
            StockIndicatorField::PeTtm,
            StockIndicatorField::Ps,
            StockIndicatorField::PsTtm,
        ] {
            factors.push((
                field.to_string(),
                indicators
                    .get_latest_value::<f64>(end_date, STALE_DAYS_SHORT, false, &field.to_string())
                    .map(|(_, v)| v)
                    .unwrap_or(f64::NAN),
            ));
        }
    }

    // Quality
    {
        for field in &[
            StockReportPershareField::Bps,
            StockReportPershareField::Cfps,
            StockReportPershareField::Eps,
            StockReportPershareField::Roe,
        ] {
            factors.push((
                field.to_string(),
                report_pershare
                    .get_latest_value::<f64>(end_date, STALE_DAYS_LONG, false, &field.to_string())
                    .map(|(_, v)| v)
                    .unwrap_or(f64::NAN),
            ));
        }
    }

    // Liquidity
    {
        for field in &[
            StockIndicatorField::TurnoverRate,
            StockIndicatorField::VolumeRatio,
        ] {
            factors.push((
                field.to_string(),
                indicators
                    .get_latest_value::<f64>(end_date, STALE_DAYS_SHORT, false, &field.to_string())
                    .map(|(_, v)| v)
                    .unwrap_or(f64::NAN),
            ));
        }
    }

    // Volatility
    {
        for days in &[20, 40, 60, 120, 240] {
            let prices_days: Vec<f64> = kline
                .get_latest_values::<f64>(end_date, false, &KlineField::Close.to_string(), *days)
                .iter()
                .map(|&(_, v)| v)
                .collect();
            factors.push((
                format!("Volatility{days}T"),
                calc_annualized_volatility(&prices_days).unwrap_or(f64::NAN),
            ));
            factors.push((
                format!("MaxDrawdown{days}T"),
                calc_max_drawdown(&prices_days).unwrap_or(f64::NAN),
            ));
        }
    }

    // Deviation
    {
        for days in &[20, 40, 60, 120, 240] {
            let prices_days: Vec<f64> = kline
                .get_latest_values::<f64>(end_date, false, &KlineField::Close.to_string(), *days)
                .iter()
                .map(|&(_, v)| v)
                .collect();
            factors.push((
                format!("EmaDeviation{days}T"),
                calc_ema_deviation(&prices_days).unwrap_or(f64::NAN),
            ));
        }
    }

    Ok(factors)
}
