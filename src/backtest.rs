use std::{
    cmp::Ordering,
    collections::{BTreeMap, HashMap},
};

use chrono::{Datelike, Duration, NaiveDate};
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use tokio::sync::{
    mpsc,
    mpsc::{Receiver, Sender},
};

use crate::{
    CHANNEL_BUFFER_DEFAULT, WORKSPACE,
    error::*,
    financial::{
        Portfolio,
        stock::{StockDividendAdjust, StockKlineField, fetch_stock_detail, fetch_stock_kline},
        tool::fetch_trade_dates,
    },
    rule::Rule,
    spec::{FofDefinition, FundDefinition, TickerSourceDefinition},
    ticker::Ticker,
    utils::{
        datetime::date_to_str,
        financial::{
            calc_annualized_return_rate, calc_annualized_volatility, calc_max_drawdown,
            calc_profit_factor, calc_sharpe_ratio, calc_sortino_ratio, calc_win_rate,
        },
        math::normalize_min_max,
        stats::mean,
    },
};

pub enum BacktestEvent {
    Buy(String),
    Sell(String),
    Info(String),
    Toast(String),
    Result(Box<BacktestResult>),
    Error(VfError),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BacktestOptions {
    pub init_cash: f64,
    #[serde(serialize_with = "serialize_date")]
    pub start_date: NaiveDate,
    #[serde(serialize_with = "serialize_date")]
    pub end_date: NaiveDate,
    pub buffer_ratio: f64,
    pub risk_free_rate: f64,
    pub stamp_duty_rate: f64,
    pub stamp_duty_min_fee: f64,
    pub broker_commission_rate: f64,
    pub broker_commission_min_fee: f64,

    #[serde(skip)]
    pub benchmark: Option<String>,
    #[serde(skip)]
    pub cv_search: bool,
    #[serde(skip)]
    pub cv_window: bool,
    #[serde(skip)]
    pub cv_min_window_days: u64,
    #[serde(skip)]
    pub cv_score_sharpe_weight: f64,
}

pub struct BacktestStream {
    receiver: Receiver<BacktestEvent>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct BacktestMetrics {
    #[serde(serialize_with = "serialize_option_date")]
    pub last_trade_date: Option<NaiveDate>,
    pub trade_days: usize,
    pub total_return: f64,
    #[serde(default)]
    pub calendar_year_returns: HashMap<i32, f64>,
    pub annualized_return_rate: Option<f64>,
    pub annualized_volatility: Option<f64>,
    pub max_drawdown: Option<f64>,
    pub win_rate: Option<f64>,
    pub profit_factor: Option<f64>,
    pub sharpe_ratio: Option<f64>,
    pub calmar_ratio: Option<f64>,
    pub sortino_ratio: Option<f64>,
}

#[derive(Clone, Debug)]
pub struct BacktestResult {
    pub options: BacktestOptions,
    pub final_cash: f64,
    pub final_positions_value: HashMap<Ticker, f64>,
    pub metrics: BacktestMetrics,
    pub trade_dates_value: Vec<(NaiveDate, f64)>,
}

pub struct FundBacktestContext<'a> {
    pub options: &'a BacktestOptions,
    pub fund_definition: &'a FundDefinition,
    pub portfolio: &'a mut Portfolio,
}

pub async fn backtest_fof(
    fof_definition: &FofDefinition,
    options: &BacktestOptions,
) -> VfResult<BacktestStream> {
    let (sender, receiver) = mpsc::channel(CHANNEL_BUFFER_DEFAULT);

    let fof_definition = fof_definition.clone();
    let options = options.clone();

    tokio::spawn(async move {
        let single_run = async |fof_definition: &FofDefinition,
                                options: &BacktestOptions|
               -> VfResult<BacktestResult> {
            let weights_sum: f64 = fof_definition.funds.values().sum();
            if weights_sum > 0.0 {
                let workspace = { WORKSPACE.read().await.clone() };

                let mut funds_result: Vec<(usize, BacktestResult)> = vec![];
                for (fund_index, (fund_name, weight)) in fof_definition.funds.iter().enumerate() {
                    if *weight <= 0.0 {
                        continue;
                    }

                    let fund_path = workspace.join(format!("{fund_name}.fund.toml"));
                    let fund_definition = FundDefinition::from_file(&fund_path)?;

                    let mut fund_options = options.clone();
                    fund_options.init_cash = options.init_cash * weight / weights_sum;
                    fund_options.cv_search = false;
                    fund_options.cv_window = false;

                    let mut stream = backtest_fund(&fund_definition, &fund_options).await?;

                    while let Some(event) = stream.next().await {
                        match event {
                            BacktestEvent::Buy(s) => {
                                let _ = sender
                                    .send(BacktestEvent::Buy(format!("[{fund_name}] {s}")))
                                    .await;
                            }
                            BacktestEvent::Sell(s) => {
                                let _ = sender
                                    .send(BacktestEvent::Sell(format!("[{fund_name}] {s}")))
                                    .await;
                            }
                            BacktestEvent::Info(s) => {
                                let _ = sender
                                    .send(BacktestEvent::Info(format!("[{fund_name}] {s}")))
                                    .await;
                            }
                            BacktestEvent::Toast(s) => {
                                let _ = sender
                                    .send(BacktestEvent::Toast(format!("[{fund_name}] {s}")))
                                    .await;
                            }
                            BacktestEvent::Result(fund_result) => {
                                funds_result.push((fund_index, *fund_result));
                            }
                            BacktestEvent::Error(_) => {
                                let _ = sender.send(event).await;
                            }
                        }
                    }
                }

                let final_cash = funds_result
                    .iter()
                    .map(|(_, fund_result)| fund_result.final_cash)
                    .sum();

                let mut final_positions_value: HashMap<Ticker, f64> = HashMap::new();
                for (_, fund_result) in &funds_result {
                    for (ticker, value) in &fund_result.final_positions_value {
                        final_positions_value
                            .entry(ticker.clone())
                            .and_modify(|v| *v += value)
                            .or_insert(*value);
                    }
                }

                let _ = notify_portfolio(
                    sender.clone(),
                    &options.end_date,
                    final_cash,
                    &final_positions_value,
                )
                .await;

                let trade_dates_value: Vec<(NaiveDate, f64)> = {
                    let mut trade_dates_value_map: BTreeMap<NaiveDate, f64> = BTreeMap::new();

                    for (_, fund_result) in funds_result {
                        for (date, value) in fund_result.trade_dates_value {
                            trade_dates_value_map
                                .entry(date)
                                .and_modify(|v| *v += value)
                                .or_insert(value);
                        }
                    }

                    trade_dates_value_map.into_iter().collect::<_>()
                };

                Ok(BacktestResult {
                    options: options.clone(),
                    final_cash,
                    final_positions_value,
                    metrics: BacktestMetrics::from_daily_data(&trade_dates_value, options),
                    trade_dates_value,
                })
            } else {
                Ok(BacktestResult {
                    options: options.clone(),
                    final_cash: options.init_cash,
                    final_positions_value: HashMap::new(),
                    metrics: BacktestMetrics::default(),
                    trade_dates_value: vec![],
                })
            }
        };

        let process = async || -> VfResult<BacktestResult> {
            if options.cv_search {
                let all_search: Vec<(String, Vec<f64>)> = fof_definition
                    .search
                    .clone()
                    .into_iter()
                    .collect::<Vec<_>>();

                let mut cv_results: Vec<(Vec<(String, f64)>, BacktestResult)> = vec![];
                let cv_count = all_search.iter().map(|(_, v)| v.len()).product::<usize>();
                for (i, funds_weight) in all_search
                    .iter()
                    .map(|(fund_name, weights)| {
                        weights
                            .iter()
                            .map(|weight| (fund_name.to_string(), *weight))
                            .collect::<Vec<_>>()
                    })
                    .multi_cartesian_product()
                    .enumerate()
                {
                    let mut fof_definition = fof_definition.clone();
                    for (fund_name, weight) in &funds_weight {
                        fof_definition.funds.insert(fund_name.clone(), *weight);
                    }

                    let result = single_run(&fof_definition, &options).await?;

                    let _ = sender
                        .send(BacktestEvent::Info(format!(
                            "[CV {}/{cv_count}] ARR={} Sharpe={} ({})",
                            i + 1,
                            result
                                .metrics
                                .annualized_return_rate
                                .map(|v| format!("{:.2}%", v * 100.0))
                                .unwrap_or("-".to_string()),
                            result
                                .metrics
                                .sharpe_ratio
                                .map(|v| format!("{v:.3}"))
                                .unwrap_or("-".to_string()),
                            funds_weight
                                .iter()
                                .map(|(fund_name, weight)| { format!("{fund_name}={weight}") })
                                .collect::<Vec<_>>()
                                .join(" ")
                        )))
                        .await;

                    cv_results.push((funds_weight.clone(), result));
                }

                if !cv_results.is_empty() {
                    let cv_metrics: Vec<BacktestMetrics> = cv_results
                        .iter()
                        .map(|(_, result)| result.metrics.clone())
                        .collect();
                    let cv_sort = sort_cv_metrics(&cv_metrics, &options);

                    for (i, index) in cv_sort.into_iter().rev().enumerate() {
                        let (funds_weight, result) = &cv_results[index];

                        let top = cv_results.len() - i - 1;

                        let top_str = if top == 0 {
                            "Best"
                        } else {
                            &format!("Top {top}")
                        };

                        let _ = sender
                            .send(BacktestEvent::Info(format!(
                                "[CV] [{top_str}] {}",
                                funds_weight
                                    .iter()
                                    .map(|(fund_name, weight)| { format!("{fund_name}={weight}") })
                                    .collect::<Vec<_>>()
                                    .join(" ")
                            )))
                            .await;

                        if top == 0 {
                            return Ok(result.clone());
                        }
                    }
                }
            } else if options.cv_window {
                type DateRange = (NaiveDate, NaiveDate);

                let mut windows: Vec<DateRange> = vec![(options.start_date, options.end_date)];

                let total_days = (options.end_date - options.start_date).num_days();
                let i_max = (total_days / options.cv_min_window_days as i64).ilog2() + 1;
                if i_max >= 1 {
                    for i in 1..=i_max {
                        let n = 2_i64.pow(i);
                        let half_window_days = total_days / (n + 1);
                        let window_days = half_window_days * 2;

                        for j in 0..n {
                            let window_end =
                                options.end_date - Duration::days(j * half_window_days);
                            let window_start = window_end - Duration::days(window_days);
                            windows.push((window_start, window_end));
                        }
                    }
                }

                let mut cv_results: Vec<(DateRange, BacktestResult)> = vec![];
                let cv_count = windows.len();
                for (i, (window_start, window_end)) in windows.iter().enumerate() {
                    let mut options = options.clone();
                    options.start_date = *window_start;
                    options.end_date = *window_end;

                    let result = single_run(&fof_definition, &options).await?;

                    let _ = sender
                        .send(BacktestEvent::Info(format!(
                            "[CV {}/{cv_count}] ARR={} Sharpe={} ({}-{})",
                            i + 1,
                            result
                                .metrics
                                .annualized_return_rate
                                .map(|v| format!("{:.2}%", v * 100.0))
                                .unwrap_or("-".to_string()),
                            result
                                .metrics
                                .sharpe_ratio
                                .map(|v| format!("{v:.3}"))
                                .unwrap_or("-".to_string()),
                            date_to_str(window_start),
                            date_to_str(window_end),
                        )))
                        .await;

                    cv_results.push(((*window_start, *window_end), result));
                }

                if !cv_results.is_empty() {
                    for ((window_start, window_end), result) in cv_results.iter() {
                        let _ = sender
                            .send(BacktestEvent::Info(format!(
                                "[CV] [{}~{}] ARR={} Sharpe={} MDD={} ({}d)",
                                date_to_str(window_start),
                                date_to_str(window_end),
                                result
                                    .metrics
                                    .annualized_return_rate
                                    .map(|v| format!("{:.2}%", v * 100.0))
                                    .unwrap_or("-".to_string()),
                                result
                                    .metrics
                                    .sharpe_ratio
                                    .map(|v| format!("{v:.3}"))
                                    .unwrap_or("-".to_string()),
                                result
                                    .metrics
                                    .max_drawdown
                                    .map(|v| format!("{:.2}%", v * 100.0))
                                    .unwrap_or("-".to_string()),
                                (*window_end - *window_start).num_days() + 1
                            )))
                            .await;
                    }

                    if let Some((_, result)) = cv_results.first() {
                        {
                            let arrs: Vec<f64> = cv_results
                                .iter()
                                .filter_map(|(_, result)| result.metrics.annualized_return_rate)
                                .collect();
                            if let (Some(arr_mean), Some(arr_min)) = (
                                mean(&arrs),
                                arrs.iter()
                                    .min_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal))
                                    .copied(),
                            ) {
                                let _ = sender
                                    .send(BacktestEvent::Info(format!(
                                        "[CV] [ARR Mean={:.2}% Min={:.2}%]",
                                        arr_mean * 100.0,
                                        arr_min * 100.0
                                    )))
                                    .await;
                            }
                        }

                        {
                            let sharpes: Vec<f64> = cv_results
                                .iter()
                                .filter_map(|(_, result)| result.metrics.sharpe_ratio)
                                .collect();
                            if let (Some(sharpe_mean), Some(sharpe_min)) = (
                                mean(&sharpes),
                                sharpes
                                    .iter()
                                    .min_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal))
                                    .copied(),
                            ) {
                                let _ = sender
                                    .send(BacktestEvent::Info(format!(
                                        "[CV] [Sharpe Mean={sharpe_mean:.3} Min={sharpe_min:.3}]"
                                    )))
                                    .await;
                            }
                        }

                        return Ok(result.clone());
                    }
                }
            }

            single_run(&fof_definition, &options).await
        };

        match process().await {
            Ok(result) => {
                let _ = sender.send(BacktestEvent::Result(Box::new(result))).await;
            }
            Err(err) => {
                let _ = sender.send(BacktestEvent::Error(err)).await;
            }
        }
    });

    Ok(BacktestStream { receiver })
}

pub async fn backtest_fund(
    fund_definition: &FundDefinition,
    options: &BacktestOptions,
) -> VfResult<BacktestStream> {
    options.check();

    let (sender, receiver) = mpsc::channel(CHANNEL_BUFFER_DEFAULT);

    let fund_definition = fund_definition.clone();
    let options = options.clone();

    tokio::spawn(async move {
        let single_run = async |fund_definition: &FundDefinition,
                                options: &BacktestOptions|
               -> VfResult<BacktestResult> {
            let mut context = FundBacktestContext {
                fund_definition,
                options,
                portfolio: &mut Portfolio::new(options.init_cash),
            };

            let mut rules = fund_definition
                .rules
                .iter()
                .map(Rule::from_definition)
                .collect::<Vec<_>>();

            let days = (options.end_date - options.start_date).num_days() as u64 + 1;

            let mut trade_dates_value: Vec<(NaiveDate, f64)> = vec![];

            let mut rule_executed_date: HashMap<usize, NaiveDate> = HashMap::new();
            let trade_dates = fetch_trade_dates().await?;
            for date in options.start_date.iter_days().take(days as usize) {
                if trade_dates.contains(&date) {
                    for (rule_index, rule) in rules.iter_mut().enumerate() {
                        if let Some(executed_date) = rule_executed_date.get(&rule_index) {
                            let executed_days = (date - *executed_date).num_days();
                            let frequency_days = rule.definition().frequency.days;
                            if frequency_days > 0 {
                                if executed_days < frequency_days {
                                    continue;
                                }
                            } else {
                                continue;
                            }
                        }

                        rule.exec(&mut context, &date, sender.clone()).await?;
                        rule_executed_date.insert(rule_index, date);
                    }

                    let total_value = context.calc_total_value(&date).await?;
                    trade_dates_value.push((date, total_value));
                }
            }

            let mut final_cash = context.portfolio.free_cash;
            for cash in context.portfolio.reserved_cash.values() {
                final_cash += *cash;
            }

            let mut final_positions_value: HashMap<Ticker, f64> = HashMap::new();
            for (ticker, units) in &context.portfolio.positions {
                let kline = fetch_stock_kline(ticker, StockDividendAdjust::ForwardProp).await?;
                if let Some((_, price)) = kline
                    .get_latest_value::<f64>(&options.end_date, &StockKlineField::Close.to_string())
                {
                    let value = *units as f64 * price;
                    final_positions_value.insert(ticker.clone(), value);
                }
            }

            let _ = notify_portfolio(
                sender.clone(),
                &options.end_date,
                final_cash,
                &final_positions_value,
            )
            .await;

            Ok(BacktestResult {
                options: options.clone(),
                final_cash,
                final_positions_value,
                metrics: BacktestMetrics::from_daily_data(&trade_dates_value, options),
                trade_dates_value,
            })
        };

        let process = async || -> VfResult<BacktestResult> {
            if options.cv_search {
                type RuleOptionValue = (String, String, serde_json::Value);

                let mut all_search: Vec<RuleOptionValue> = vec![];
                for rule_definition in &fund_definition.rules {
                    for (k, v) in &rule_definition.search {
                        all_search.push((
                            rule_definition.name.to_string(),
                            k.to_string(),
                            v.clone(),
                        ));
                    }
                }

                let mut cv_results: Vec<(Vec<RuleOptionValue>, BacktestResult)> = vec![];
                let cv_count = all_search
                    .iter()
                    .map(|(_, _, v)| v.as_array().map(|array| array.len()).unwrap_or(0))
                    .product::<usize>();
                for (i, rule_option_values) in all_search
                    .iter()
                    .filter_map(|(rule_name, option_name, value)| {
                        value.as_array().map(|array| {
                            array
                                .iter()
                                .map(|option_value| {
                                    (
                                        rule_name.to_string(),
                                        option_name.to_string(),
                                        option_value.clone(),
                                    )
                                })
                                .collect::<Vec<_>>()
                        })
                    })
                    .multi_cartesian_product()
                    .enumerate()
                {
                    let mut fund_definition = fund_definition.clone();

                    for (rule_name, option_name, option_value) in &rule_option_values {
                        if let Some(rule_definition) = fund_definition
                            .rules
                            .iter_mut()
                            .find(|r| r.name == *rule_name)
                        {
                            rule_definition
                                .options
                                .insert(option_name.to_string(), option_value.clone());
                        }
                    }

                    let result = single_run(&fund_definition, &options).await?;

                    let _ = sender
                        .send(BacktestEvent::Info(format!(
                            "[CV {}/{cv_count}] ARR={} Sharpe={} ({})",
                            i + 1,
                            result
                                .metrics
                                .annualized_return_rate
                                .map(|v| format!("{:.2}%", v * 100.0))
                                .unwrap_or("-".to_string()),
                            result
                                .metrics
                                .sharpe_ratio
                                .map(|v| format!("{v:.3}"))
                                .unwrap_or("-".to_string()),
                            rule_option_values
                                .iter()
                                .map(|(_, option_name, option_value)| {
                                    format!("{option_name}={option_value}")
                                })
                                .collect::<Vec<_>>()
                                .join(" ")
                        )))
                        .await;

                    cv_results.push((rule_option_values.clone(), result));
                }

                if !cv_results.is_empty() {
                    let cv_metrics: Vec<BacktestMetrics> = cv_results
                        .iter()
                        .map(|(_, result)| result.metrics.clone())
                        .collect();
                    let cv_sort = sort_cv_metrics(&cv_metrics, &options);

                    for (i, index) in cv_sort.into_iter().rev().enumerate() {
                        let (rule_option_values, result) = &cv_results[index];

                        let top = cv_results.len() - i - 1;

                        let top_str = if top == 0 {
                            "Best"
                        } else {
                            &format!("Top {top}")
                        };

                        let _ = sender
                            .send(BacktestEvent::Info(format!(
                                "[CV] [{top_str}] {}",
                                rule_option_values
                                    .iter()
                                    .map(|(_, option_name, option_value)| {
                                        format!("{option_name}={option_value}")
                                    })
                                    .collect::<Vec<_>>()
                                    .join(" ")
                            )))
                            .await;

                        if top == 0 {
                            return Ok(result.clone());
                        }
                    }
                }
            } else if options.cv_window {
                type DateRange = (NaiveDate, NaiveDate);

                let mut windows: Vec<DateRange> = vec![(options.start_date, options.end_date)];

                let total_days = (options.end_date - options.start_date).num_days();
                let i_max = (total_days / options.cv_min_window_days as i64).ilog2() + 1;
                if i_max >= 1 {
                    for i in 1..=i_max {
                        let n = 2_i64.pow(i);
                        let half_window_days = total_days / (n + 1);
                        let window_days = half_window_days * 2;

                        for j in 0..n {
                            let window_end =
                                options.end_date - Duration::days(j * half_window_days);
                            let window_start = window_end - Duration::days(window_days);
                            windows.push((window_start, window_end));
                        }
                    }
                }

                let mut cv_results: Vec<(DateRange, BacktestResult)> = vec![];
                let cv_count = windows.len();
                for (i, (window_start, window_end)) in windows.iter().enumerate() {
                    let mut options = options.clone();
                    options.start_date = *window_start;
                    options.end_date = *window_end;

                    let result = single_run(&fund_definition, &options).await?;

                    let _ = sender
                        .send(BacktestEvent::Info(format!(
                            "[CV {}/{cv_count}] ARR={} Sharpe={} ({}-{})",
                            i + 1,
                            result
                                .metrics
                                .annualized_return_rate
                                .map(|v| format!("{:.2}%", v * 100.0))
                                .unwrap_or("-".to_string()),
                            result
                                .metrics
                                .sharpe_ratio
                                .map(|v| format!("{v:.3}"))
                                .unwrap_or("-".to_string()),
                            date_to_str(window_start),
                            date_to_str(window_end),
                        )))
                        .await;

                    cv_results.push(((*window_start, *window_end), result));
                }

                if !cv_results.is_empty() {
                    for ((window_start, window_end), result) in cv_results.iter() {
                        let _ = sender
                            .send(BacktestEvent::Info(format!(
                                "[CV] [{}~{}] ARR={} Sharpe={} MDD={} ({}d)",
                                date_to_str(window_start),
                                date_to_str(window_end),
                                result
                                    .metrics
                                    .annualized_return_rate
                                    .map(|v| format!("{:.2}%", v * 100.0))
                                    .unwrap_or("-".to_string()),
                                result
                                    .metrics
                                    .sharpe_ratio
                                    .map(|v| format!("{v:.3}"))
                                    .unwrap_or("-".to_string()),
                                result
                                    .metrics
                                    .max_drawdown
                                    .map(|v| format!("{:.2}%", v * 100.0))
                                    .unwrap_or("-".to_string()),
                                (*window_end - *window_start).num_days() + 1
                            )))
                            .await;
                    }

                    if let Some((_, result)) = cv_results.first() {
                        {
                            let arrs: Vec<f64> = cv_results
                                .iter()
                                .filter_map(|(_, result)| result.metrics.annualized_return_rate)
                                .collect();
                            if let (Some(arr_mean), Some(arr_min)) = (
                                mean(&arrs),
                                arrs.iter()
                                    .min_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal))
                                    .copied(),
                            ) {
                                let _ = sender
                                    .send(BacktestEvent::Info(format!(
                                        "[CV] [ARR Mean={:.2}% Min={:.2}%]",
                                        arr_mean * 100.0,
                                        arr_min * 100.0
                                    )))
                                    .await;
                            }
                        }

                        {
                            let sharpes: Vec<f64> = cv_results
                                .iter()
                                .filter_map(|(_, result)| result.metrics.sharpe_ratio)
                                .collect();
                            if let (Some(sharpe_mean), Some(sharpe_min)) = (
                                mean(&sharpes),
                                sharpes
                                    .iter()
                                    .min_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal))
                                    .copied(),
                            ) {
                                let _ = sender
                                    .send(BacktestEvent::Info(format!(
                                        "[CV] [Sharpe Mean={sharpe_mean:.3} Min={sharpe_min:.3}]"
                                    )))
                                    .await;
                            }
                        }

                        return Ok(result.clone());
                    }
                }
            }

            single_run(&fund_definition, &options).await
        };

        match process().await {
            Ok(result) => {
                let _ = sender.send(BacktestEvent::Result(Box::new(result))).await;
            }
            Err(err) => {
                let _ = sender.send(BacktestEvent::Error(err)).await;
            }
        }
    });

    Ok(BacktestStream { receiver })
}

impl FundBacktestContext<'_> {
    pub async fn cash_deploy_free(
        &mut self,
        date: &NaiveDate,
        event_sender: Sender<BacktestEvent>,
    ) -> VfResult<()> {
        if !self.portfolio.positions.is_empty() {
            let position_tickers_map = self.position_tickers_map(date).await?;
            let position_weight_sum = position_tickers_map
                .iter()
                .map(|(_, (weight, _))| *weight)
                .sum::<f64>();
            if position_weight_sum > 0.0 {
                let total_value = self.calc_total_value(date).await?;
                let buffer_cash = total_value * self.options.buffer_ratio;

                let total_deploy_cash = self.portfolio.free_cash - buffer_cash;
                if total_deploy_cash > 0.0 {
                    for (ticker, units) in &self.portfolio.positions.clone() {
                        if let Some((weight, _)) = position_tickers_map.get(ticker) {
                            let deploy_cash = total_deploy_cash * weight / position_weight_sum;

                            let fee = calc_buy_fee(deploy_cash, self.options);
                            let delta_value = deploy_cash - fee;
                            if delta_value > 0.0 {
                                let kline =
                                    fetch_stock_kline(ticker, StockDividendAdjust::ForwardProp)
                                        .await?;
                                if let Some((_, price)) = kline.get_latest_value::<f64>(
                                    date,
                                    &StockKlineField::Close.to_string(),
                                ) {
                                    let ticker_value = *units as f64 * price + delta_value;

                                    self.scale_position(
                                        ticker,
                                        ticker_value,
                                        date,
                                        event_sender.clone(),
                                    )
                                    .await?;
                                }
                            }
                        }
                    }
                }
            }
        }

        Ok(())
    }

    #[allow(dead_code)]
    pub async fn cash_raise(
        &mut self,
        cash: f64,
        date: &NaiveDate,
        event_sender: Sender<BacktestEvent>,
    ) -> VfResult<()> {
        if !self.portfolio.positions.is_empty() {
            let position_tickers_map = self.position_tickers_map(date).await?;
            let position_weight_sum = position_tickers_map
                .iter()
                .map(|(_, (weight, _))| *weight)
                .sum::<f64>();
            if position_weight_sum > 0.0 {
                for (ticker, units) in &self.portfolio.positions.clone() {
                    if let Some((weight, _)) = position_tickers_map.get(ticker) {
                        let raise_cash = cash * weight / position_weight_sum;
                        let fee = calc_sell_fee(raise_cash, self.options);
                        let delta_value = raise_cash + fee;

                        let kline =
                            fetch_stock_kline(ticker, StockDividendAdjust::ForwardProp).await?;
                        if let Some((_, price)) =
                            kline.get_latest_value::<f64>(date, &StockKlineField::Close.to_string())
                        {
                            let sell_units = (delta_value / price).ceil().min(*units as f64);
                            let ticker_value = (*units as f64 - sell_units) * price;
                            if ticker_value > 0.0 {
                                self.scale_position(
                                    ticker,
                                    ticker_value,
                                    date,
                                    event_sender.clone(),
                                )
                                .await?;
                            } else {
                                self.position_exit(ticker, false, date, event_sender.clone())
                                    .await?;
                            }
                        }
                    }
                }
            }
        }

        Ok(())
    }

    pub async fn rebalance(
        &mut self,
        targets_weight: &Vec<(Ticker, f64)>,
        date: &NaiveDate,
        event_sender: Sender<BacktestEvent>,
    ) -> VfResult<()> {
        // Exit unneeded positions and reserved cash
        {
            let position_tickers: Vec<_> = self.portfolio.positions.keys().cloned().collect();
            for ticker in &position_tickers {
                if !targets_weight.iter().any(|(t, _)| t == ticker) {
                    self.position_exit(ticker, false, date, event_sender.clone())
                        .await?;
                }
            }

            let reserved_tickers: Vec<_> = self.portfolio.reserved_cash.keys().cloned().collect();
            for ticker in &reserved_tickers {
                if !targets_weight.iter().any(|(t, _)| t == ticker) {
                    if let Some(cash) = self.portfolio.reserved_cash.get(ticker) {
                        self.portfolio.free_cash += cash;
                    }

                    self.portfolio.reserved_cash.remove(ticker);
                }
            }
        }

        // Scale positions and reserved cash
        {
            let targets_weight_sum = targets_weight
                .iter()
                .map(|(_, weight)| *weight)
                .sum::<f64>();
            if targets_weight_sum > 0.0 {
                let total_value = self.calc_total_value(date).await?;

                for &(ref ticker, weight) in targets_weight {
                    let ticker_value = total_value * (1.0 - self.options.buffer_ratio) * weight
                        / targets_weight_sum;
                    if let Some(current_reserved_cash) = self.portfolio.reserved_cash.get(ticker) {
                        let delta_cash = ticker_value - current_reserved_cash;

                        self.portfolio.free_cash -= delta_cash;
                        self.portfolio
                            .reserved_cash
                            .entry(ticker.clone())
                            .and_modify(|v| *v += delta_cash);
                    } else {
                        self.scale_position(ticker, ticker_value, date, event_sender.clone())
                            .await?;
                    }
                }
            }
        }

        let total_value = self.calc_total_value(date).await?;

        let mut position_strs: Vec<String> = vec![];
        for (ticker, position_units) in self.portfolio.positions.iter() {
            let ticker_title = fetch_stock_detail(ticker).await?.title;
            let kline = fetch_stock_kline(ticker, StockDividendAdjust::ForwardProp).await?;
            if let Some((_, price)) =
                kline.get_latest_value::<f64>(date, &StockKlineField::Close.to_string())
            {
                let value = *position_units as f64 * price;
                let value_pct = value / total_value * 100.0;

                position_strs.push(format!(
                    "{ticker}({ticker_title})=${value:.2}({value_pct:.2}%)"
                ));
            } else {
                position_strs.push(format!("{ticker}({ticker_title})=$?"));
            }
        }

        let cash = self.portfolio.free_cash + self.portfolio.reserved_cash.values().sum::<f64>();

        let mut positions_value: HashMap<Ticker, f64> = HashMap::new();
        for (ticker, position_units) in self.portfolio.positions.iter() {
            let kline = fetch_stock_kline(ticker, StockDividendAdjust::ForwardProp).await?;
            if let Some((_, price)) =
                kline.get_latest_value::<f64>(date, &StockKlineField::Close.to_string())
            {
                let value = *position_units as f64 * price;
                positions_value.insert(ticker.clone(), value);
            }
        }

        let _ = notify_portfolio(event_sender.clone(), date, cash, &positions_value).await;

        Ok(())
    }

    pub async fn position_init_reserved(
        &mut self,
        ticker: &Ticker,
        date: &NaiveDate,
        event_sender: Sender<BacktestEvent>,
    ) -> VfResult<()> {
        if let Some(reserved_cash) = self.portfolio.reserved_cash.get(ticker) {
            let date_str = date_to_str(date);
            let ticker_title = fetch_stock_detail(ticker).await?.title;

            let kline = fetch_stock_kline(ticker, StockDividendAdjust::ForwardProp).await?;
            if let Some((_, price)) =
                kline.get_latest_value::<f64>(date, &StockKlineField::Close.to_string())
            {
                let delta_value = reserved_cash - calc_buy_fee(*reserved_cash, self.options);

                let buy_units = (delta_value / price).floor();
                if buy_units > 0.0 {
                    let value = buy_units * price;
                    let fee = calc_buy_fee(value, self.options);
                    let cost = value + fee;

                    self.portfolio.free_cash += *reserved_cash - cost;
                    self.portfolio.reserved_cash.remove(ticker);

                    self.portfolio
                        .positions
                        .entry(ticker.clone())
                        .and_modify(|v| *v += buy_units as u64)
                        .or_insert(buy_units as u64);

                    let _ = event_sender
                                .send(BacktestEvent::Buy(format!(
                                    "[{date_str}] {ticker}({ticker_title}) -${cost:.2} (${price:.2}x{buy_units})"
                                )))
                                .await;
                }
            } else {
                let _ = event_sender
                    .send(BacktestEvent::Info(format!(
                        "[{date_str}] [!] Price of '{ticker}' not exists"
                    )))
                    .await;
            }
        }

        Ok(())
    }

    pub async fn position_exit(
        &mut self,
        ticker: &Ticker,
        make_reserved: bool,
        date: &NaiveDate,
        event_sender: Sender<BacktestEvent>,
    ) -> VfResult<()> {
        let position_units = *self.portfolio.positions.get(ticker).unwrap_or(&0);
        if position_units > 0 {
            let date_str = date_to_str(date);
            let ticker_title = fetch_stock_detail(ticker).await?.title;

            let kline = fetch_stock_kline(ticker, StockDividendAdjust::ForwardProp).await?;
            if let Some((_, price)) =
                kline.get_latest_value::<f64>(date, &StockKlineField::Close.to_string())
            {
                let sell_units = position_units as f64;
                let value = sell_units * price;
                let fee = calc_sell_fee(value, self.options);
                let cash = value - fee;

                if make_reserved {
                    self.portfolio
                        .reserved_cash
                        .entry(ticker.clone())
                        .and_modify(|v| *v += cash)
                        .or_insert(cash);
                } else {
                    self.portfolio.free_cash += cash;
                }
                self.portfolio.positions.remove(ticker);

                let _ = event_sender
                                .send(BacktestEvent::Sell(format!(
                                    "[{date_str}] {ticker}({ticker_title}) +${cash:.2} (${price:.2}x{sell_units})"
                                )))
                                .await;
            } else {
                let _ = event_sender
                    .send(BacktestEvent::Info(format!(
                        "[{date_str}] [!] Price of '{ticker}' not exists"
                    )))
                    .await;
            }
        }

        Ok(())
    }

    pub fn watching_tickers(&self) -> Vec<Ticker> {
        let hold_tickers: Vec<Ticker> = self.portfolio.positions.keys().cloned().collect();
        let reserved_tickers: Vec<Ticker> = self.portfolio.reserved_cash.keys().cloned().collect();

        hold_tickers.into_iter().chain(reserved_tickers).collect()
    }

    async fn calc_total_cash(&self) -> VfResult<f64> {
        let mut total_cash: f64 = self.portfolio.free_cash;

        for cash in self.portfolio.reserved_cash.values() {
            total_cash += *cash;
        }

        Ok(total_cash)
    }

    async fn calc_total_value(&self, date: &NaiveDate) -> VfResult<f64> {
        let mut total_value: f64 = self.calc_total_cash().await?;

        for (ticker, units) in &self.portfolio.positions {
            let kline = fetch_stock_kline(ticker, StockDividendAdjust::ForwardProp).await?;
            if let Some((_, price)) =
                kline.get_latest_value::<f64>(date, &StockKlineField::Close.to_string())
            {
                total_value += *units as f64 * price;
            }
        }

        Ok(total_value)
    }

    async fn position_tickers_map(
        &self,
        date: &NaiveDate,
    ) -> VfResult<HashMap<Ticker, (f64, Option<TickerSourceDefinition>)>> {
        let all_tickers_map = self.fund_definition.all_tickers_map(date).await?;
        Ok(all_tickers_map
            .into_iter()
            .filter(|(ticker, _)| self.portfolio.positions.contains_key(ticker))
            .collect::<HashMap<_, _>>())
    }

    async fn scale_position(
        &mut self,
        ticker: &Ticker,
        ticker_value: f64,
        date: &NaiveDate,
        event_sender: Sender<BacktestEvent>,
    ) -> VfResult<()> {
        let date_str = date_to_str(date);
        let ticker_title = fetch_stock_detail(ticker).await?.title;

        let kline = fetch_stock_kline(ticker, StockDividendAdjust::ForwardProp).await?;
        if let Some((_, price)) =
            kline.get_latest_value::<f64>(date, &StockKlineField::Close.to_string())
        {
            let position_units = *self.portfolio.positions.get(ticker).unwrap_or(&0);
            let position_value = position_units as f64 * price;
            let delta_value = ticker_value - position_value;

            if delta_value > 0.0 {
                let buy_units = (delta_value / price).floor();
                if buy_units > 0.0 {
                    let value = buy_units * price;
                    let fee = calc_buy_fee(value, self.options);
                    let cost = value + fee;

                    self.portfolio.free_cash -= cost;

                    self.portfolio
                        .positions
                        .entry(ticker.clone())
                        .and_modify(|v| *v += buy_units as u64)
                        .or_insert(buy_units as u64);

                    let _ = event_sender
                                .send(BacktestEvent::Buy(format!(
                                    "[{date_str}] {ticker}({ticker_title}) -${cost:.2} (${price:.2}x{buy_units})"
                                )))
                                .await;
                } else {
                    let _ = event_sender
                        .send(BacktestEvent::Buy(format!(
                            "[{date_str}] {ticker}({ticker_title}) $0 (≈{position_value})"
                        )))
                        .await;
                }
            } else {
                let sell_value = delta_value.abs();

                let sell_units = (sell_value / price).floor().min(position_units as f64);
                if sell_units > 0.0 {
                    let value = sell_units * price;
                    let fee = calc_sell_fee(value, self.options);
                    let cash = value - fee;

                    self.portfolio.free_cash += cash;

                    if sell_units as u64 == position_units {
                        self.portfolio.positions.remove(ticker);
                    } else {
                        self.portfolio
                            .positions
                            .entry(ticker.clone())
                            .and_modify(|v| *v -= sell_units as u64);
                    }

                    let _ = event_sender
                                .send(BacktestEvent::Sell(format!(
                                    "[{date_str}] {ticker}({ticker_title}) +${cash:.2} (${price:.2}x{sell_units})"
                                )))
                                .await;
                } else {
                    let _ = event_sender
                        .send(BacktestEvent::Sell(format!(
                            "[{date_str}] {ticker}({ticker_title}) $0 (≈{position_value})"
                        )))
                        .await;
                }
            }
        } else {
            let _ = event_sender
                .send(BacktestEvent::Info(format!(
                    "[{date_str}] [!] Price of '{ticker}' not exists"
                )))
                .await;
        }

        Ok(())
    }
}

impl BacktestMetrics {
    pub fn from_daily_data(
        trade_dates_value: &Vec<(NaiveDate, f64)>,
        options: &BacktestOptions,
    ) -> Self {
        let mut calendar_year_returns: HashMap<i32, f64> = HashMap::new();
        {
            let mut prev_value = options.init_cash;
            for (date, value) in trade_dates_value {
                let daily_return = value - prev_value;
                prev_value = *value;

                calendar_year_returns
                    .entry(date.year())
                    .and_modify(|v| *v += daily_return)
                    .or_insert(daily_return);
            }
        }

        let final_value = trade_dates_value
            .last()
            .map(|(_, v)| *v)
            .unwrap_or(options.init_cash);
        let total_return = final_value - options.init_cash;
        let annualized_return_rate = calc_annualized_return_rate(
            options.init_cash,
            final_value,
            (options.end_date - options.start_date).num_days() as u64 + 1,
        );
        let daily_values: Vec<f64> = trade_dates_value.iter().map(|(_, v)| *v).collect();
        let max_drawdown = calc_max_drawdown(&daily_values);
        let annualized_volatility = calc_annualized_volatility(&daily_values);
        let win_rate = calc_win_rate(&daily_values);
        let profit_factor = calc_profit_factor(&daily_values);
        let sharpe_ratio = calc_sharpe_ratio(&daily_values, options.risk_free_rate);
        let calmar_ratio = if let (Some(arr), Some(mdd)) = (annualized_return_rate, max_drawdown) {
            if mdd > 0.0 { Some(arr / mdd) } else { None }
        } else {
            None
        };
        let sortino_ratio = calc_sortino_ratio(&daily_values, options.risk_free_rate);

        Self {
            last_trade_date: trade_dates_value.last().map(|(d, _)| *d),
            trade_days: trade_dates_value.len(),
            total_return,
            calendar_year_returns,
            annualized_return_rate,
            annualized_volatility,
            max_drawdown,
            win_rate,
            profit_factor,
            sharpe_ratio,
            calmar_ratio,
            sortino_ratio,
        }
    }
}

impl BacktestOptions {
    pub fn check(&self) {
        if self.init_cash <= 0.0 {
            panic!("init_cash must > 0");
        }

        if self.end_date < self.start_date {
            panic!(
                "The end date {} cannot be earlier than the start date {}",
                date_to_str(&self.end_date),
                date_to_str(&self.start_date)
            );
        }

        if self.buffer_ratio < 0.0 || self.buffer_ratio >= 1.0 {
            panic!("buffer_ratio must >= 0 and < 1");
        }

        if self.risk_free_rate < 0.0 {
            panic!("risk_free_rate must >= 0");
        }

        if self.stamp_duty_rate < 0.0 || self.stamp_duty_rate >= 1.0 {
            panic!("stamp_duty_rate must >= 0 and < 1");
        }

        if self.stamp_duty_min_fee < 0.0 {
            panic!("stamp_duty_min_fee must >= 0");
        }

        if self.broker_commission_rate < 0.0 || self.broker_commission_rate >= 1.0 {
            panic!("broker_commission_rate must >= 0 and < 1");
        }

        if self.broker_commission_min_fee < 0.0 {
            panic!("broker_commission_min_fee must >= 0");
        }
    }
}

impl BacktestStream {
    pub fn new(receiver: Receiver<BacktestEvent>) -> Self {
        Self { receiver }
    }

    pub fn close(&mut self) {
        self.receiver.close()
    }

    pub async fn next(&mut self) -> Option<BacktestEvent> {
        self.receiver.recv().await
    }
}

fn calc_buy_fee(value: f64, options: &BacktestOptions) -> f64 {
    let broker_commission = value * options.broker_commission_rate;
    if broker_commission > options.broker_commission_min_fee {
        broker_commission
    } else {
        options.broker_commission_min_fee
    }
}

fn calc_sell_fee(value: f64, options: &BacktestOptions) -> f64 {
    let stamp_duty = value * options.stamp_duty_rate;
    let stamp_duty_fee = if stamp_duty > options.stamp_duty_min_fee {
        stamp_duty
    } else {
        options.stamp_duty_min_fee
    };

    let broker_commission = value * options.broker_commission_rate;
    let broker_commission_fee = if broker_commission > options.broker_commission_min_fee {
        broker_commission
    } else {
        options.broker_commission_min_fee
    };

    stamp_duty_fee + broker_commission_fee
}

async fn notify_portfolio(
    event_sender: Sender<BacktestEvent>,
    date: &NaiveDate,
    cash: f64,
    positions_value: &HashMap<Ticker, f64>,
) -> VfResult<()> {
    let date_str = date_to_str(date);

    let total_value = cash + positions_value.values().sum::<f64>();
    if total_value > 0.0 {
        let mut portfolio_str = String::new();

        {
            let cash_pct = cash / total_value * 100.0;
            portfolio_str.push_str(&format!("${cash:.2}({cash_pct:.2}%)"));
        }

        for (ticker, value) in positions_value {
            if *value > 0.0 {
                let ticker_title = fetch_stock_detail(ticker).await?.title;
                let value_pct = value / total_value * 100.0;

                portfolio_str.push_str(
                    format!(" {ticker}({ticker_title})=${value:.2}({value_pct:.2}%)").as_str(),
                );
            }
        }

        let _ = event_sender
            .send(BacktestEvent::Info(format!(
                "[{date_str}] [${total_value:.2}] {portfolio_str}"
            )))
            .await;
    } else {
        let _ = event_sender
            .send(BacktestEvent::Info(format!("[{date_str}] [$0]")))
            .await;
    }

    Ok(())
}

fn serialize_date<S>(date: &NaiveDate, ser: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    ser.serialize_str(&date_to_str(date))
}

fn serialize_option_date<S>(date: &Option<NaiveDate>, ser: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    if let Some(date) = date {
        ser.serialize_str(&date_to_str(date))
    } else {
        ser.serialize_none()
    }
}

fn sort_cv_metrics(cv_metrics: &[BacktestMetrics], options: &BacktestOptions) -> Vec<usize> {
    let normalized_sharpe_values = {
        let sharpe_values: Vec<f64> = cv_metrics
            .iter()
            .map(|m| m.sharpe_ratio.unwrap_or(f64::NEG_INFINITY))
            .collect();
        normalize_min_max(&sharpe_values)
    };

    let normalized_arr_values = {
        let arr_values: Vec<f64> = cv_metrics
            .iter()
            .map(|m| m.annualized_return_rate.unwrap_or(f64::NEG_INFINITY))
            .collect();
        normalize_min_max(&arr_values)
    };

    let scores_tuple: Vec<(f64, f64)> = normalized_sharpe_values
        .into_iter()
        .zip(normalized_arr_values)
        .collect();

    let mut cv_scores: Vec<(usize, f64)> = scores_tuple
        .into_iter()
        .enumerate()
        .map(|(index, (sharpe, arr))| {
            (
                index,
                sharpe * options.cv_score_sharpe_weight
                    + arr * (1.0 - options.cv_score_sharpe_weight),
            )
        })
        .collect();
    cv_scores.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(Ordering::Equal));

    cv_scores.into_iter().map(|(i, _)| i).collect()
}
