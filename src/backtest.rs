use std::{cmp::Ordering, collections::HashMap};

use chrono::{Duration, NaiveDate};
use itertools::Itertools;
use log::debug;
use serde::Serialize;
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
    ticker::Ticker,
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
    Result(BacktestResult),
    Error(VfError),
}

#[derive(Clone)]
pub struct BacktestOptions {
    pub init_cash: f64,
    pub start_date: NaiveDate,
    pub end_date: NaiveDate,
    pub benchmark: Option<String>,
    pub risk_free_rate: f64,
    pub stamp_duty_rate: f64,
    pub stamp_duty_min_fee: f64,
    pub broker_commission_rate: f64,
    pub broker_commission_min_fee: f64,
    pub cv_search: bool,
    pub cv_window: bool,
    pub cv_score_arr_cap: f64,
}

pub struct BacktestStream {
    receiver: Receiver<BacktestEvent>,
}

#[derive(Clone, Serialize)]
pub struct BacktestMetrics {
    pub init_cash: f64,
    pub start_date: String,
    pub end_date: String,
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

#[derive(Clone)]
pub struct BacktestResult {
    pub trade_date_values: Vec<(NaiveDate, f64)>,
    pub metrics: BacktestMetrics,
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

            let mut trade_date_values: Vec<(NaiveDate, f64)> = vec![];
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
                    trade_date_values.push((date, total_value));

                    debug!("[{}] ✔", date_to_str(&date));
                } else {
                    debug!("[{}] ○", date_to_str(&date));
                }
            }

            let _ = context
                .notify_portfolio(&options.end_date, sender.clone())
                .await;

            let final_cash = trade_date_values
                .last()
                .map(|(_, v)| *v)
                .unwrap_or(options.init_cash);
            let profit = final_cash - options.init_cash;
            let annualized_return_rate =
                calc_annualized_return_rate(options.init_cash, final_cash, days);
            let daily_values: Vec<f64> = trade_date_values.iter().map(|(_, v)| *v).collect();
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
                init_cash: options.init_cash,
                start_date: date_to_str(&options.start_date),
                end_date: date_to_str(&options.end_date),
                trade_days: trade_date_values.len(),
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
                trade_date_values,
                metrics,
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

                        b_score.partial_cmp(&a_score).unwrap_or(Ordering::Equal)
                    });

                    if let Some((rule_option_values, result)) = cv_results.first() {
                        let _ = sender
                            .send(BacktestEvent::Info(format!(
                                "[CV] [Best] {}",
                                rule_option_values
                                    .iter()
                                    .map(|(_, option_name, option_value)| {
                                        format!("{option_name}={option_value}")
                                    })
                                    .collect::<Vec<_>>()
                                    .join(" ")
                            )))
                            .await;

                        return Ok(result.clone());
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
                                    "[CV] [ARR Max Drop]={:.2}%",
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
                                    "[CV] [Sharpe Max Drop]={:.2}%",
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
                let _ = sender.send(BacktestEvent::Result(result)).await;
            }
            Err(err) => {
                let _ = sender.send(BacktestEvent::Error(err)).await;
            }
        }
    });

    Ok(BacktestStream { receiver })
}

impl BacktestContext<'_> {
    pub async fn calc_total_value(&self, date: &NaiveDate) -> VfResult<f64> {
        let mut total_value: f64 = self.portfolio.cash;

        for (ticker, units) in &self.portfolio.positions {
            let kline = fetch_stock_kline(ticker, StockDividendAdjust::ForwardProp).await?;
            if let Some(price) =
                kline.get_latest_value::<f64>(date, &StockKlineField::Close.to_string())
            {
                let value = *units as f64 * price;
                let fee = calc_sell_fee(value, self.options);
                total_value += value - fee;
            }
        }

        Ok(total_value)
    }

    pub async fn rebalance(
        &mut self,
        targets: &Vec<(Ticker, f64)>,
        date: &NaiveDate,
        event_sender: Sender<BacktestEvent>,
    ) -> VfResult<()> {
        let date_str = date_to_str(date);

        // Remove unneeded positions and sidelines
        {
            let position_tickers: Vec<_> = self.portfolio.positions.keys().cloned().collect();
            for ticker in &position_tickers {
                if !targets.iter().any(|(t, _)| t == ticker) {
                    self.portfolio.sidelines.remove(ticker);

                    let kline = fetch_stock_kline(ticker, StockDividendAdjust::ForwardProp).await?;
                    if let Some(price) =
                        kline.get_latest_value::<f64>(date, &StockKlineField::Close.to_string())
                    {
                        let sell_units =
                            *(self.portfolio.positions.get(ticker).unwrap_or(&0)) as f64;
                        if sell_units > 0.0 {
                            let value = sell_units * price;
                            let fee = calc_sell_fee(value, self.options);
                            let cash = value - fee;

                            self.portfolio.cash += cash;
                            self.portfolio.positions.remove(ticker);

                            let ticker_title = fetch_stock_detail(ticker).await?.title;
                            let _ = event_sender
                                .send(BacktestEvent::Sell(format!(
                                    "[{date_str}] {ticker}({ticker_title}) +${cash:.2} (${price:.2}x{sell_units})"
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
            }

            let sideline_tickers: Vec<_> = self.portfolio.sidelines.keys().cloned().collect();
            for ticker in &sideline_tickers {
                if !targets.iter().any(|(t, _)| t == ticker) {
                    self.portfolio.sidelines.remove(ticker);
                }
            }
        }

        for &(ref ticker, ticker_value) in targets {
            if self.portfolio.sidelines.contains_key(ticker) {
                self.portfolio
                    .sidelines
                    .entry(ticker.clone())
                    .and_modify(|v| *v = ticker_value);
            } else {
                self.ticker_scale(ticker, ticker_value, date, event_sender.clone())
                    .await?;
            }
        }

        let _ = self.notify_portfolio(date, event_sender.clone()).await;

        Ok(())
    }

    pub async fn ticker_exit(
        &mut self,
        ticker: &Ticker,
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

                self.portfolio.cash += cash;
                self.portfolio.positions.remove(ticker);

                self.portfolio
                    .sidelines
                    .entry(ticker.clone())
                    .and_modify(|v| *v += cash)
                    .or_insert(cash);

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

    pub async fn ticker_scale(
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
                let buy_value = delta_value - calc_buy_fee(delta_value, self.options);

                let buy_units = (buy_value / price).floor();
                if buy_units > 0.0 {
                    let value = buy_units * price;
                    let fee = calc_buy_fee(value, self.options);
                    let cost = value + fee;

                    self.portfolio.cash -= cost;
                    self.portfolio
                        .positions
                        .entry(ticker.clone())
                        .and_modify(|v| *v += buy_units as u64)
                        .or_insert(buy_units as u64);

                    self.portfolio.sidelines.remove(ticker);

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

                    self.portfolio.cash += cash;
                    self.portfolio
                        .positions
                        .entry(ticker.clone())
                        .and_modify(|v| *v -= sell_units as u64);

                    if *self.portfolio.positions.get(ticker).unwrap_or(&0) == 0 {
                        self.portfolio.positions.remove(ticker);

                        self.portfolio
                            .sidelines
                            .entry(ticker.clone())
                            .and_modify(|v| *v += cash)
                            .or_insert(cash);
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

    pub fn watching_tickers(&self) -> Vec<Ticker> {
        let hold_tickers: Vec<Ticker> = self.portfolio.positions.keys().cloned().collect();
        let sideline_tickers: Vec<Ticker> = self.portfolio.sidelines.keys().cloned().collect();

        hold_tickers.into_iter().chain(sideline_tickers).collect()
    }

    async fn notify_portfolio(
        &self,
        date: &NaiveDate,
        event_sender: Sender<BacktestEvent>,
    ) -> VfResult<()> {
        let mut total_value = self.portfolio.cash;

        let mut position_strs: Vec<String> = vec![];
        for (ticker, position_units) in self.portfolio.positions.iter() {
            let ticker_title = fetch_stock_detail(ticker).await?.title;
            let kline = fetch_stock_kline(ticker, StockDividendAdjust::ForwardProp).await?;
            if let Some(price) =
                kline.get_latest_value::<f64>(date, &StockKlineField::Close.to_string())
            {
                let value = *position_units as f64 * price;
                let fee = calc_sell_fee(value, self.options);
                let net_value = value - fee;
                total_value += net_value;

                position_strs.push(format!(
                    "{ticker}({ticker_title})=${net_value:.2}(${price:.2}x{position_units})"
                ));
            } else {
                position_strs.push(format!("{ticker}({ticker_title})=$?({position_units})"));
            }
        }

        let mut sideline_strs: Vec<String> = vec![];
        for (ticker, cash) in self.portfolio.sidelines.iter() {
            let ticker_title = fetch_stock_detail(ticker).await?.title;
            sideline_strs.push(format!("{ticker}({ticker_title})~(${cash:.2})"));
        }

        let mut portfolio_str = String::new();
        {
            portfolio_str.push_str(&format!("${:.2}", self.portfolio.cash));
        }
        if !position_strs.is_empty() {
            portfolio_str.push(' ');
            portfolio_str.push_str(&position_strs.join(" "));
        }
        if !sideline_strs.is_empty() {
            portfolio_str.push_str(" (");
            portfolio_str.push_str(&sideline_strs.join(" "));
            portfolio_str.push(')');
        }

        let date_str = date_to_str(date);
        let _ = event_sender
            .send(BacktestEvent::Info(format!(
                "[{date_str}] [${total_value:.2}] {portfolio_str}"
            )))
            .await;

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
