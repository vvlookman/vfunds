use std::{cmp::Ordering, collections::HashMap};

use chrono::{Duration, NaiveDate};
use itertools::Itertools;
use log::debug;
use serde::{Deserialize, Serialize};
use tokio::sync::{
    mpsc,
    mpsc::{Receiver, Sender},
};

use crate::{
    CHANNEL_BUFFER_DEFAULT,
    error::*,
    financial::{
        Portfolio,
        stock::{StockDividendAdjust, StockKlineField, fetch_stock_detail, fetch_stock_kline},
        tool::fetch_trade_dates,
    },
    rule::Rule,
    spec::FundDefinition,
    ticker::{Ticker, TickersIndex},
    utils::{
        datetime::date_to_str,
        financial::{
            calc_annualized_return_rate, calc_annualized_volatility, calc_max_drawdown,
            calc_profit_factor, calc_sharpe_ratio, calc_sortino_ratio, calc_win_rate,
        },
    },
};

pub struct BacktestContext<'a> {
    pub fund_definition: &'a FundDefinition,
    pub options: &'a BacktestOptions,
    pub portfolio: &'a mut Portfolio,
}

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
    pub cv_score_arr_cap: f64,
}

pub struct BacktestStream {
    receiver: Receiver<BacktestEvent>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BacktestMetrics {
    #[serde(serialize_with = "serialize_option_date")]
    pub last_trade_date: Option<NaiveDate>,
    pub trade_days: usize,
    pub profit: f64,
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

pub fn calc_buy_fee(value: f64, options: &BacktestOptions) -> f64 {
    let broker_commission = value * options.broker_commission_rate;
    if broker_commission > options.broker_commission_min_fee {
        broker_commission
    } else {
        options.broker_commission_min_fee
    }
}

pub fn calc_sell_fee(value: f64, options: &BacktestOptions) -> f64 {
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

pub async fn backtest_fund(
    fund_definition: &FundDefinition,
    options: &BacktestOptions,
) -> VfResult<BacktestStream> {
    if options.init_cash <= 0.0 {
        panic!("init_cash must > 0");
    }

    if options.end_date < options.start_date {
        panic!(
            "The end date {} cannot be earlier than the start date {}",
            date_to_str(&options.end_date),
            date_to_str(&options.start_date)
        );
    }

    if options.buffer_ratio < 0.0 || options.buffer_ratio >= 1.0 {
        panic!("buffer_ratio must >= 0 and < 1");
    }

    if options.risk_free_rate < 0.0 {
        panic!("risk_free_rate must >= 0");
    }

    if options.stamp_duty_rate < 0.0 || options.stamp_duty_rate >= 1.0 {
        panic!("stamp_duty_rate must >= 0 and < 1");
    }

    if options.stamp_duty_min_fee < 0.0 {
        panic!("stamp_duty_min_fee must >= 0");
    }

    if options.broker_commission_rate < 0.0 || options.broker_commission_rate >= 1.0 {
        panic!("broker_commission_rate must >= 0 and < 1");
    }

    if options.broker_commission_min_fee < 0.0 {
        panic!("broker_commission_min_fee must >= 0");
    }

    let (sender, receiver) = mpsc::channel(CHANNEL_BUFFER_DEFAULT);

    let fund_definition = fund_definition.clone();
    let options = options.clone();

    tokio::spawn(async move {
        let single_run = async |fund_definition: &FundDefinition,
                                options: &BacktestOptions|
               -> VfResult<BacktestResult> {
            let mut context = BacktestContext {
                fund_definition,
                options,
                portfolio: &mut Portfolio::new(options.init_cash),
            };

            let mut rules = fund_definition
                .rules
                .iter()
                .map(Rule::from_definition)
                .collect::<Vec<_>>();

            let mut trade_dates_value: Vec<(NaiveDate, f64)> = vec![];
            let mut rule_executed_date: HashMap<usize, NaiveDate> = HashMap::new();
            let days = (options.end_date - options.start_date).num_days() as u64 + 1;
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

                    debug!("[{}] ✔", date_to_str(&date));
                } else {
                    debug!("[{}] ○", date_to_str(&date));
                }
            }

            let _ = context
                .notify_portfolio(&options.end_date, sender.clone())
                .await;

            let mut final_cash = context.portfolio.free_cash;
            for cash in context.portfolio.reserved_cash.values() {
                final_cash += *cash;
            }

            let mut final_positions_value: HashMap<Ticker, f64> = HashMap::new();
            for (ticker, units) in &context.portfolio.positions {
                let kline = fetch_stock_kline(ticker, StockDividendAdjust::ForwardProp).await?;
                if let Some(price) = kline
                    .get_latest_value::<f64>(&options.end_date, &StockKlineField::Close.to_string())
                {
                    let value = *units as f64 * price;
                    final_positions_value.insert(ticker.clone(), value);
                }
            }

            let final_value = trade_dates_value
                .last()
                .map(|(_, v)| *v)
                .unwrap_or(options.init_cash);
            let profit = final_value - options.init_cash;
            let annualized_return_rate =
                calc_annualized_return_rate(options.init_cash, final_value, days);
            let daily_values: Vec<f64> = trade_dates_value.iter().map(|(_, v)| *v).collect();
            let max_drawdown = calc_max_drawdown(&daily_values);
            let annualized_volatility = calc_annualized_volatility(&daily_values);
            let win_rate = calc_win_rate(&daily_values);
            let profit_factor = calc_profit_factor(&daily_values);
            let sharpe_ratio = calc_sharpe_ratio(&daily_values, options.risk_free_rate);
            let calmar_ratio =
                if let (Some(arr), Some(mdd)) = (annualized_return_rate, max_drawdown) {
                    if mdd > 0.0 { Some(arr / mdd) } else { None }
                } else {
                    None
                };
            let sortino_ratio = calc_sortino_ratio(&daily_values, options.risk_free_rate);

            let metrics = BacktestMetrics {
                last_trade_date: trade_dates_value.last().map(|(d, _)| *d),
                trade_days: trade_dates_value.len(),
                profit,
                annualized_return_rate,
                annualized_volatility,
                max_drawdown,
                win_rate,
                profit_factor,
                sharpe_ratio,
                calmar_ratio,
                sortino_ratio,
            };

            Ok(BacktestResult {
                options: options.clone(),
                final_cash,
                final_positions_value,
                metrics,
                trade_dates_value,
            })
        };

        let process = async || -> VfResult<BacktestResult> {
            if options.cv_search {
                type RuleOptionValue = (String, String, serde_json::Value);

                let mut all_search: Vec<(String, String, serde_json::Value)> = vec![];
                for rule_definition in fund_definition.rules.iter() {
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
                    cv_results.sort_by(|(_, a), (_, b)| {
                        let a_score = if let (Some(arr), Some(sharpe)) =
                            (a.metrics.annualized_return_rate, a.metrics.sharpe_ratio)
                        {
                            sharpe * arr.min(options.cv_score_arr_cap).abs()
                        } else {
                            f64::NEG_INFINITY
                        };

                        let b_score = if let (Some(arr), Some(sharpe)) =
                            (b.metrics.annualized_return_rate, b.metrics.sharpe_ratio)
                        {
                            sharpe * arr.min(options.cv_score_arr_cap).abs()
                        } else {
                            f64::NEG_INFINITY
                        };

                        a_score.partial_cmp(&b_score).unwrap_or(Ordering::Equal)
                    });

                    let result_count = cv_results.len();
                    for (i, (rule_option_values, result)) in cv_results.iter().enumerate() {
                        let top = result_count - i - 1;

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
                let i_max = (total_days / 365).ilog2() + 1;
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
                        if let (Some(arr), Some(min_arr)) = (
                            result.metrics.annualized_return_rate,
                            cv_results
                                .iter()
                                .map(|(_, result)| result.metrics.annualized_return_rate)
                                .min_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal))
                                .unwrap_or(None),
                        ) {
                            let _ = sender
                                .send(BacktestEvent::Info(format!(
                                    "[CV] [ARR Max Drop {:.2}%]",
                                    (arr - min_arr) / arr * 100.0
                                )))
                                .await;
                        }

                        if let (Some(sharpe), Some(min_sharpe)) = (
                            result.metrics.sharpe_ratio,
                            cv_results
                                .iter()
                                .map(|(_, result)| result.metrics.sharpe_ratio)
                                .min_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal))
                                .unwrap_or(None),
                        ) {
                            let _ = sender
                                .send(BacktestEvent::Info(format!(
                                    "[CV] [Sharpe Max Drop {:.2}%]",
                                    (sharpe - min_sharpe) / sharpe * 100.0
                                )))
                                .await;
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

impl BacktestContext<'_> {
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
                                if let Some(price) = kline.get_latest_value::<f64>(
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
                        if let Some(price) =
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

        let _ = self.notify_portfolio(date, event_sender.clone()).await;

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
            if let Some(price) =
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
            if let Some(price) =
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
            if let Some(price) =
                kline.get_latest_value::<f64>(date, &StockKlineField::Close.to_string())
            {
                total_value += *units as f64 * price;
            }
        }

        Ok(total_value)
    }

    async fn notify_portfolio(
        &self,
        date: &NaiveDate,
        event_sender: Sender<BacktestEvent>,
    ) -> VfResult<()> {
        let total_value = self.calc_total_value(date).await?;

        let mut position_strs: Vec<String> = vec![];
        for (ticker, position_units) in self.portfolio.positions.iter() {
            let ticker_title = fetch_stock_detail(ticker).await?.title;
            let kline = fetch_stock_kline(ticker, StockDividendAdjust::ForwardProp).await?;
            if let Some(price) =
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

        let mut portfolio_str = String::new();
        {
            let total_cash = self.calc_total_cash().await?;
            let total_cash_pct = total_cash / total_value * 100.0;

            portfolio_str.push_str(&format!("${total_cash:.2}({total_cash_pct:.2}%)"));
        }
        if !position_strs.is_empty() {
            portfolio_str.push(' ');
            portfolio_str.push_str(&position_strs.join(" "));
        }

        let date_str = date_to_str(date);
        let _ = event_sender
            .send(BacktestEvent::Info(format!(
                "[{date_str}] [${total_value:.2}] {portfolio_str}"
            )))
            .await;

        Ok(())
    }

    async fn position_tickers_map(
        &self,
        date: &NaiveDate,
    ) -> VfResult<HashMap<Ticker, (f64, Option<TickersIndex>)>> {
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
        if let Some(price) =
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
