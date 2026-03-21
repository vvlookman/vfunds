use async_trait::async_trait;
use chrono::{Datelike, NaiveDate};
use log::debug;
use smartcore::{
    linalg::basic::{arrays::Array, matrix::DenseMatrix},
    linear::ridge_regression::{RidgeRegression, RidgeRegressionParameters},
    metrics::{mean_absolute_error, r2},
};
use tokio::{sync::mpsc::Sender, time::Instant};

use crate::{
    PROGRESS_INTERVAL_SECS, REQUIRED_DATA_COMPLETENESS,
    error::VfResult,
    financial::{
        KlineField,
        stock::{StockDividendAdjust, fetch_stock_kline},
    },
    rule::{
        BacktestEvent, FundBacktestContext, RuleDefinition, RuleExecutor, calc_weights,
        rule_notify_calc_progress, rule_notify_indicators, rule_send_warning, select_by_indicators,
    },
    spec::RuleOptions,
    ticker::Ticker,
    utils::{
        datetime::date_to_str,
        financial::{calc_annualized_return_rate, calc_ema},
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

        let limit = self.options.read_u64_no_zero("limit", 5);
        let lookback_trade_days = self.options.read_u64_no_zero("lookback_trade_days", 21);
        let ma_exp = self.options.read_u64("ma_exp", 10);
        let ma_period_fast = self.options.read_u64_no_zero("ma_period_fast", 5);
        let ma_period_slow = self.options.read_u64_no_zero("ma_period_slow", 20);
        let metric_r2_threshold =
            self.options
                .read_f64_in_range("metric_r2_threshold", 0.8, 0.0..=1.0);
        let regression_alpha = self.options.read_f64_gte("regression_alpha", 1.0, 0.0);
        let weight_method = self.options.read_str("weight_method", "equal");

        let tickers_map = context.fund_definition.all_tickers_map(date).await?;
        if !tickers_map.is_empty() {
            let date_str = date_to_str(date);

            let mut indicators: Vec<(Ticker, f64)> = vec![];
            {
                let mut last_time = Instant::now();
                let mut calc_count: usize = 0;

                for ticker in tickers_map.keys() {
                    calc_count += 1;

                    let kline = fetch_stock_kline(ticker, StockDividendAdjust::Backward).await?;
                    let prices_with_date = kline.get_latest_values::<f64>(
                        date,
                        false,
                        &KlineField::Close.to_string(),
                        lookback_trade_days as u32,
                    );
                    if prices_with_date.len()
                        < (lookback_trade_days as f64 * REQUIRED_DATA_COMPLETENESS).round() as usize
                    {
                        rule_send_warning(
                            rule_name,
                            &format!(
                                "[No Enough Data] {ticker} {lookback_trade_days}({})",
                                prices_with_date.len()
                            ),
                            date,
                            event_sender,
                        )
                        .await;
                        continue;
                    }

                    let prices: Vec<f64> = prices_with_date.iter().map(|&(_, v)| v).collect();
                    if let Some(arr) = calc_annualized_return_rate(&prices) {
                        let features: Vec<Vec<f64>> = prices_with_date
                            .iter()
                            .enumerate()
                            .map(|(i, &(date, _))| {
                                let x_i = i as f64;
                                let x_weekday = date.weekday().number_from_monday() as f64;
                                let x_monthday = date.day() as f64;

                                vec![x_i, x_weekday, x_monthday]
                            })
                            .collect();

                        if let Ok(x_train) = DenseMatrix::from_2d_array(
                            &features
                                .iter()
                                .map(|v| v.as_slice())
                                .collect::<Vec<&[f64]>>(),
                        ) {
                            let y_train: Vec<f64> = prices.iter().map(|&v| v.ln()).collect();

                            let parameters =
                                RidgeRegressionParameters::default().with_alpha(regression_alpha);
                            if let Ok(model) = RidgeRegression::fit(&x_train, &y_train, parameters)
                            {
                                if let Ok(y_train_pred) = model.predict(&x_train) {
                                    let r2_score = r2(&y_train, &y_train_pred);
                                    debug!(
                                        "[{date_str}] R2={r2_score:.4} MAE={:.4} SHAPE={:?}",
                                        mean_absolute_error(&y_train, &y_train_pred),
                                        x_train.shape(),
                                    );

                                    if r2_score > metric_r2_threshold && r2_score < 1.0 - 1e-8 {
                                        let emas_fast = calc_ema(&prices, ma_period_fast as usize);
                                        let emas_slow = calc_ema(&prices, ma_period_slow as usize);
                                        let ema_ratio = if let (Some(ema_fast), Some(ema_slow)) =
                                            (emas_fast.last(), emas_slow.last())
                                        {
                                            (ema_slow / ema_fast).powi(ma_exp as i32)
                                        } else {
                                            0.0
                                        };

                                        let indicator = arr * r2_score * ema_ratio;
                                        debug!(
                                            "[{date_str}] [{rule_name}] {ticker} = {indicator:.4} (ARR={arr:.4} R2={r2_score:.4} EMA_RATIO={ema_ratio:.4})"
                                        );

                                        if indicator > 0.0 {
                                            indicators.push((ticker.clone(), indicator));
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
