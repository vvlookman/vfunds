use std::{
    cmp::Ordering,
    collections::{HashMap, HashSet},
    str::FromStr,
    time::Instant,
};

use chrono::{Duration, NaiveDate};
use itertools::Itertools;
use tokio::sync::mpsc;

use crate::{
    CHANNEL_BUFFER_DEFAULT, WORKSPACE,
    backtest::{fund::backtest_funds, *},
    spec::*,
    utils::{
        datetime::{date_to_str, secs_to_human_str},
        stats::mean,
    },
};

pub async fn backtest_fof(
    fof_definition: &FofDefinition,
    options: &BacktestOptions,
) -> VfResult<BacktestStream> {
    options.check();

    let (sender, receiver) = mpsc::channel(CHANNEL_BUFFER_DEFAULT);

    let fof_definition = fof_definition.clone();
    let options = options.clone();

    tokio::spawn(async move {
        let single_run = async |fof_definition: &FofDefinition,
                                options: &BacktestOptions|
               -> VfResult<BacktestResult> {
            let valid_funds: Vec<_> = fof_definition
                .funds
                .iter()
                .filter(|(_, w)| **w > 0.0)
                .collect();
            if !valid_funds.is_empty() {
                let workspace = { WORKSPACE.read().await.clone() };

                let mut funds: Vec<(String, FundDefinition)> = vec![];
                let mut funds_weight: Vec<(String, f64)> = vec![];
                for (fund_name, fund_weight) in &valid_funds {
                    let fund_path = workspace.join(format!("{fund_name}.fund.toml"));
                    let fund_definition = FundDefinition::from_file(&fund_path)?;
                    funds.push((fund_name.to_string(), fund_definition));
                    funds_weight.push((fund_name.to_string(), **fund_weight));
                }
                let funds_result = backtest_funds(&funds, options, &sender).await?;

                let order_dates = calc_order_dates_value_from_funds_result(&funds_result);

                let trade_dates_value = calc_trade_dates_value_from_funds_result(
                    &funds_result,
                    &funds_weight,
                    fof_definition.frequency.to_days(),
                    options,
                    &sender,
                )
                .await;

                Ok(BacktestResult {
                    title: Some(fof_definition.title.clone()),
                    options: options.clone(),
                    final_cash: 0.0,
                    final_positions_value: HashMap::new(),
                    metrics: BacktestMetrics::from_daily_value(&trade_dates_value, options),
                    order_dates,
                    trade_dates_value,
                })
            } else {
                Ok(BacktestResult {
                    title: Some(fof_definition.title.clone()),
                    options: options.clone(),
                    final_cash: options.init_cash,
                    final_positions_value: HashMap::new(),
                    metrics: BacktestMetrics::default(),
                    order_dates: vec![],
                    trade_dates_value: vec![],
                })
            }
        };

        match single_run(&fof_definition, &options).await {
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

pub async fn backtest_fof_cv(
    fof_definition: &FofDefinition,
    cv_options: &BacktestCvOptions,
) -> VfResult<BacktestStream> {
    cv_options.base_options.check();

    let (sender, receiver) = mpsc::channel(CHANNEL_BUFFER_DEFAULT);

    let fof_definition = fof_definition.clone();
    let cv_options = cv_options.clone();

    tokio::spawn(async move {
        let process = async || -> VfResult<()> {
            if cv_options.cv_search {
                let valid_funds: Vec<_> = fof_definition
                    .funds
                    .iter()
                    .filter(|(_, w)| **w > 0.0)
                    .collect();
                if !valid_funds.is_empty() {
                    let workspace = { WORKSPACE.read().await.clone() };

                    let mut funds_result_map: HashMap<NaiveDate, Vec<(String, BacktestResult)>> =
                        HashMap::new();

                    for cv_start_date in &cv_options.cv_start_dates {
                        let mut options = cv_options.base_options.clone();
                        options.start_date = *cv_start_date;

                        let mut funds: Vec<(String, FundDefinition)> = vec![];
                        for (fund_name, _) in &valid_funds {
                            let fund_path = workspace.join(format!("{fund_name}.fund.toml"));
                            let fund_definition = FundDefinition::from_file(&fund_path)?;
                            funds.push((fund_name.to_string(), fund_definition));
                        }
                        let funds_result = backtest_funds(&funds, &options, &sender).await?;

                        funds_result_map.insert(*cv_start_date, funds_result.clone());
                    }

                    let mut search_frequencies: Vec<Frequency> = fof_definition
                        .search
                        .frequency
                        .iter()
                        .filter_map(|f| Frequency::from_str(f).ok())
                        .collect();
                    if search_frequencies.is_empty() {
                        search_frequencies.push(fof_definition.frequency.clone());
                    }

                    let search_funds: Vec<(String, Vec<f64>)> =
                        fof_definition.search.funds.clone().into_iter().collect();

                    let cv_count = search_frequencies.len()
                        * search_funds.iter().map(|(_, v)| v.len()).product::<usize>()
                        * cv_options.cv_start_dates.len();

                    let cv_start = Instant::now();

                    struct Search {
                        frequency: Frequency,
                        funds_weight: Vec<(String, f64)>,
                    }

                    let mut cv_search_results: Vec<(Search, HashMap<NaiveDate, BacktestResult>)> =
                        vec![];
                    for (i, frequency) in search_frequencies.iter().enumerate() {
                        for (j, funds_weight) in search_funds
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
                            let mut cv_results: HashMap<NaiveDate, BacktestResult> = HashMap::new();
                            for (k, cv_start_date) in cv_options.cv_start_dates.iter().enumerate() {
                                let mut fof_definition = fof_definition.clone();
                                fof_definition.frequency = frequency.clone();
                                for (fund_name, weight) in &funds_weight {
                                    fof_definition
                                        .funds
                                        .entry(fund_name.to_string())
                                        .and_modify(|v| *v = *weight);
                                }

                                let mut options = cv_options.base_options.clone();
                                options.start_date = *cv_start_date;

                                if let Some(funds_result) = funds_result_map.get(cv_start_date) {
                                    let order_dates =
                                        calc_order_dates_value_from_funds_result(funds_result);

                                    let trade_dates_value =
                                        calc_trade_dates_value_from_funds_result(
                                            funds_result,
                                            &funds_weight,
                                            fof_definition.frequency.to_days(),
                                            &options,
                                            &sender,
                                        )
                                        .await;

                                    let result = BacktestResult {
                                        title: Some(fof_definition.title.clone()),
                                        options: options.clone(),
                                        final_cash: 0.0,
                                        final_positions_value: HashMap::new(),
                                        metrics: BacktestMetrics::from_daily_value(
                                            &trade_dates_value,
                                            &options,
                                        ),
                                        order_dates,
                                        trade_dates_value,
                                    };

                                    let _ = sender
                                        .send(BacktestEvent::Info {
                                            title: format!(
                                                "[CV {}/{cv_count} {}] [{}~{}]",
                                                i * search_funds.len()
                                                    * cv_options.cv_start_dates.len()
                                                    + j * cv_options.cv_start_dates.len()
                                                    + k
                                                    + 1,
                                                secs_to_human_str(cv_start.elapsed().as_secs()),
                                                date_to_str(&options.start_date),
                                                date_to_str(&options.end_date),
                                            ),
                                            message: format!(
                                                "[ARR={} Sharpe={}] frequency={} {}",
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
                                                frequency.to_str(),
                                                funds_weight
                                                    .iter()
                                                    .map(|(name, weight)| format!(
                                                        "{name}={weight}"
                                                    ))
                                                    .collect::<Vec<_>>()
                                                    .join(" ")
                                            ),
                                            date: None,
                                        })
                                        .await;

                                    cv_results.insert(*cv_start_date, result);
                                }
                            }

                            cv_search_results.push((
                                Search {
                                    frequency: frequency.clone(),
                                    funds_weight: funds_weight.clone(),
                                },
                                cv_results,
                            ));
                        }
                    }

                    if !cv_search_results.is_empty() {
                        let cv_results_list = cv_search_results
                            .iter()
                            .map(|(_, cv_results)| cv_results.clone())
                            .collect::<Vec<_>>();
                        let cv_scores = sort_cv_results_list(&cv_results_list, &cv_options);

                        let best_score = cv_scores
                            .first()
                            .map(|(_, cv_score)| cv_score.score)
                            .unwrap_or(f64::NEG_INFINITY);

                        for (i, (idx, cv_score)) in cv_scores.into_iter().rev().enumerate() {
                            if let Some((search, _)) = cv_search_results.get(idx) {
                                let top = cv_search_results.len() - i - 1;

                                let top_str = if top == 0 {
                                    "Best"
                                } else {
                                    if (best_score - cv_score.score).abs() < best_score.abs() * 1e-2
                                    {
                                        &format!("Top {top} ≈ Best")
                                    } else {
                                        &format!("Top {top}")
                                    }
                                };

                                let _ = sender
                                    .send(BacktestEvent::Info {
                                        title: format!("[CV {top_str}]"),
                                        message: format!(
                                            "[ARR={:.2}% Sharpe={:.3}] frequency={} {}",
                                            cv_score.arr * 100.0,
                                            cv_score.sharpe,
                                            search.frequency.to_str(),
                                            search
                                                .funds_weight
                                                .iter()
                                                .map(|(fund_name, weight)| {
                                                    format!("{fund_name}={weight}")
                                                })
                                                .collect::<Vec<_>>()
                                                .join(" ")
                                        ),
                                        date: None,
                                    })
                                    .await;
                            }
                        }
                    }
                }
            } else if cv_options.cv_window {
                type DateRange = (NaiveDate, NaiveDate);

                let mut windows: Vec<DateRange> = vec![];

                for start_date in &cv_options.cv_start_dates {
                    windows.push((*start_date, cv_options.base_options.end_date));

                    let total_days = (cv_options.base_options.end_date - *start_date).num_days();
                    let i_max = (total_days / cv_options.cv_min_window_days as i64).ilog2() + 1;
                    if i_max >= 1 {
                        for i in 1..=i_max {
                            let n = 2_i64.pow(i);
                            let half_window_days = total_days / (n + 1);
                            let window_days = half_window_days * 2;

                            for j in 0..n {
                                let window_end = cv_options.base_options.end_date
                                    - Duration::days(j * half_window_days);
                                let window_start = window_end - Duration::days(window_days);
                                windows.push((window_start, window_end));
                            }
                        }
                    }
                }

                let cv_count = windows.len();

                let cv_start = Instant::now();

                let mut cv_window_results: Vec<(DateRange, BacktestResult)> = vec![];
                for (i, (window_start, window_end)) in windows.iter().enumerate() {
                    let mut options = cv_options.base_options.clone();
                    options.start_date = *window_start;
                    options.end_date = *window_end;

                    let mut stream = backtest_fof(&fof_definition, &options).await?;

                    while let Some(event) = stream.next().await {
                        match event {
                            BacktestEvent::Result(result) => {
                                let _ = sender
                                    .send(BacktestEvent::Info {
                                        title: format!(
                                            "[CV {}/{cv_count} {}] [{}~{}]",
                                            i + 1,
                                            secs_to_human_str(cv_start.elapsed().as_secs()),
                                            date_to_str(&options.start_date),
                                            date_to_str(&options.end_date),
                                        ),
                                        message: format!(
                                            "[ARR={} Sharpe={}] {}-{}",
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
                                        ),
                                        date: None,
                                    })
                                    .await;

                                cv_window_results.push(((*window_start, *window_end), *result));
                            }
                            _ => {
                                let _ = sender.send(event).await;
                            }
                        }
                    }
                }

                if !cv_window_results.is_empty() {
                    for ((window_start, window_end), result) in cv_window_results.iter() {
                        let _ = sender
                            .send(BacktestEvent::Info {
                                title: format!(
                                    "[CV {}~{}]",
                                    date_to_str(window_start),
                                    date_to_str(window_end),
                                ),
                                message: format!(
                                    "[ARR={} Sharpe={} MDD={}] {}d",
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
                                ),
                                date: None,
                            })
                            .await;
                    }

                    {
                        let arrs: Vec<f64> = cv_window_results
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
                                .send(BacktestEvent::Info {
                                    title: "[CV]".to_string(),
                                    message: format!(
                                        "[ARR Mean={:.2}% Min={:.2}%]",
                                        arr_mean * 100.0,
                                        arr_min * 100.0
                                    ),
                                    date: None,
                                })
                                .await;
                        }
                    }

                    {
                        let sharpes: Vec<f64> = cv_window_results
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
                                .send(BacktestEvent::Info {
                                    title: "[CV]".to_string(),
                                    message: format!(
                                        "[Sharpe Mean={sharpe_mean:.3} Min={sharpe_min:.3}]"
                                    ),
                                    date: None,
                                })
                                .await;
                        }
                    }
                }
            }

            Ok(())
        };

        if let Err(err) = process().await {
            let _ = sender.send(BacktestEvent::Error(err)).await;
        }
    });

    Ok(BacktestStream { receiver })
}

fn calc_order_dates_value_from_funds_result(
    funds_result: &Vec<(String, BacktestResult)>,
) -> Vec<NaiveDate> {
    let mut order_dates_set: HashSet<NaiveDate> = HashSet::new();

    for (_, fund_result) in funds_result {
        for date in &fund_result.order_dates {
            order_dates_set.insert(*date);
        }
    }

    let mut order_dates: Vec<NaiveDate> = order_dates_set.into_iter().collect();
    order_dates.sort_unstable();

    order_dates
}

async fn calc_trade_dates_value_from_funds_result(
    funds_result: &Vec<(String, BacktestResult)>,
    funds_weight: &[(String, f64)],
    period_days: u64,
    options: &BacktestOptions,
    sender: &Sender<BacktestEvent>,
) -> Vec<(NaiveDate, f64)> {
    // All funds value of trade dates based on the same initial cash
    let trade_dates_funds_standard_value: HashMap<NaiveDate, HashMap<String, f64>> = {
        let mut funds_value_map = HashMap::new();

        for (fund_name, fund_result) in funds_result {
            for (date, value) in &fund_result.trade_dates_value {
                funds_value_map
                    .entry(*date)
                    .and_modify(|v: &mut HashMap<String, f64>| {
                        v.insert(fund_name.to_string(), *value);
                    })
                    .or_default()
                    .insert(fund_name.to_string(), *value);
            }
        }

        funds_value_map
    };

    let funds_weight_sum: f64 = funds_weight.iter().map(|(_, w)| *w).sum();

    let mut trade_dates_value: Vec<(NaiveDate, f64)> = vec![];

    struct PeriodStart {
        date: NaiveDate,
        funds_value: HashMap<String, f64>,
        funds_standard_value: HashMap<String, f64>,
    }

    let mut optional_period_start: Option<PeriodStart> = None;
    let days = (options.end_date - options.start_date).num_days() as u64 + 1;
    for date in options.start_date.iter_days().take(days as usize) {
        if let Some(funds_standard_value) = trade_dates_funds_standard_value.get(&date) {
            if let Some(period_start) = &optional_period_start {
                let mut funds_value: HashMap<String, f64> = HashMap::new();
                for (fund_name, fund_standard_value) in funds_standard_value {
                    if let Some(period_start_fund_value) = period_start.funds_value.get(fund_name)
                        && let Some(period_start_fund_standard_value) =
                            period_start.funds_standard_value.get(fund_name)
                    {
                        let fund_value = period_start_fund_value * fund_standard_value
                            / period_start_fund_standard_value;
                        funds_value.insert(fund_name.to_string(), fund_value);
                    }
                }

                // Check frequency
                let days = (date - period_start.date).num_days();
                if period_days > 0 && days >= period_days as i64 {
                    // Rebalance
                    let mut new_funds_value: HashMap<String, f64> = HashMap::new();
                    let mut funds_delta_pct: HashMap<String, f64> = HashMap::new();

                    let total_value = funds_value.values().sum::<f64>();
                    for (fund_name, fund_weight) in funds_weight.iter() {
                        let target_fund_value = *fund_weight / funds_weight_sum * total_value;

                        let new_fund_value = if let Some(fund_value) = funds_value.get(fund_name) {
                            funds_delta_pct.insert(
                                fund_name.to_string(),
                                100.0 * (target_fund_value - fund_value) / fund_value,
                            );

                            let delta_value = (target_fund_value - *fund_value).abs();
                            let fee = calc_buy_fee(delta_value, options)
                                + calc_sell_fee(delta_value, options);

                            target_fund_value - fee
                        } else {
                            target_fund_value
                        };

                        new_funds_value.insert(fund_name.to_string(), new_fund_value);
                    }

                    let _ = sender
                        .send(BacktestEvent::Info {
                            title: "[Rebalance]".to_string(),
                            message: funds_delta_pct
                                .iter()
                                .map(|(fund_name, delta_pct)| {
                                    format!(
                                        "{fund_name}={}{delta_pct:.2}%",
                                        if *delta_pct > 0.0 { "+" } else { "" }
                                    )
                                })
                                .collect::<Vec<_>>()
                                .join(" "),
                            date: Some(date),
                        })
                        .await;

                    trade_dates_value.push((date, new_funds_value.values().sum::<f64>()));

                    optional_period_start = Some(PeriodStart {
                        date,
                        funds_value: new_funds_value,
                        funds_standard_value: funds_standard_value.clone(),
                    });
                } else {
                    trade_dates_value.push((date, funds_value.values().sum::<f64>()));
                }
            } else {
                // Init
                let mut funds_value: HashMap<String, f64> = HashMap::new();
                let mut period_start_funds_standard_value: HashMap<String, f64> = HashMap::new();
                for (fund_name, fund_standard_value) in funds_standard_value {
                    if let Some((_, fund_weight)) =
                        funds_weight.iter().find(|(name, _)| name == fund_name)
                    {
                        funds_value.insert(
                            fund_name.to_string(),
                            *fund_weight / funds_weight_sum * fund_standard_value,
                        );
                        period_start_funds_standard_value
                            .insert(fund_name.to_string(), *fund_standard_value);
                    }
                }

                trade_dates_value.push((date, funds_value.values().sum::<f64>()));

                optional_period_start = Some(PeriodStart {
                    date,
                    funds_value,
                    funds_standard_value: period_start_funds_standard_value,
                });
            }
        }
    }

    trade_dates_value
}
