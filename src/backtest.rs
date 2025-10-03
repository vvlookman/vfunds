use std::collections::HashMap;

use chrono::NaiveDate;
use log::debug;
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
    utils,
    utils::financial::{
        calc_annual_return_rate, calc_max_drawdown, calc_sharpe_ratio, calc_sortino_ratio,
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
}

pub struct BacktestStream {
    receiver: Receiver<BacktestEvent>,
}

pub struct BacktestResult {
    pub final_trade_date: Option<NaiveDate>,
    pub trade_days: usize,
    pub profit: f64,
    pub annual_return_rate: Option<f64>,
    pub max_drawdown: Option<f64>,
    pub sharpe_ratio: Option<f64>,
    pub sortino_ratio: Option<f64>,
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
        let process = async || {
            let mut context = BacktestContext {
                fund_definition: &fund_definition,
                options: &options,
                portfolio: &mut Portfolio::new(options.init_cash),
            };

            let trade_dates = fetch_trade_dates().await?;
            let mut trade_date_values: Vec<(NaiveDate, f64)> = vec![];
            let days = (options.end_date - options.start_date).num_days() as u64 + 1;

            let mut rules = fund_definition
                .rules
                .iter()
                .map(Rule::from_definition)
                .collect::<Vec<_>>();
            let mut rule_executed_date: HashMap<usize, NaiveDate> = HashMap::new();
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

                    debug!("[{}] ✔", utils::datetime::date_to_str(&date));
                } else {
                    debug!("[{}] ○", utils::datetime::date_to_str(&date));
                }
            }

            let final_trade_date = trade_date_values.last().map(|(d, _)| *d);
            let final_cash = trade_date_values
                .last()
                .map(|(_, v)| *v)
                .unwrap_or(options.init_cash);
            let profit = final_cash - options.init_cash;
            let annual_return_rate = calc_annual_return_rate(options.init_cash, final_cash, days);

            let daily_values: Vec<f64> = trade_date_values.iter().map(|(_, v)| *v).collect();
            let max_drawdown = calc_max_drawdown(&daily_values);
            let sharpe_ratio = calc_sharpe_ratio(&daily_values, options.risk_free_rate);
            let sortino_ratio = calc_sortino_ratio(&daily_values, options.risk_free_rate);

            Ok(BacktestResult {
                final_trade_date,
                trade_days: trade_date_values.len(),
                profit,
                annual_return_rate,
                max_drawdown,
                sharpe_ratio,
                sortino_ratio,
            })
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
            } else {
                return Err(VfError::NoData(
                    "NO_TICKER_PRICE",
                    format!("Price of '{ticker}' not exists"),
                ));
            }
        }

        Ok(total_value)
    }

    pub async fn exit(
        &mut self,
        ticker: &Ticker,
        date: &NaiveDate,
        event_sender: Sender<BacktestEvent>,
    ) -> VfResult<()> {
        let date_str = utils::datetime::date_to_str(date);

        let kline = fetch_stock_kline(ticker, StockDividendAdjust::ForwardProp).await?;
        if let Some(price) =
            kline.get_latest_value::<f64>(date, &StockKlineField::Close.to_string())
        {
            let holding_units = *self.portfolio.positions.get(ticker).unwrap_or(&0);
            if holding_units > 0 {
                let sell_units = holding_units as f64;
                let value = sell_units * price;
                let fee = calc_sell_fee(value, self.options);
                let cash = value - fee;

                self.portfolio.cash += cash;
                self.portfolio.positions.remove(ticker);
                self.portfolio.sideline_cash.insert(ticker.clone(), cash);

                let ticker_title = fetch_stock_detail(ticker).await?.title;
                let _ = event_sender
                                .send(BacktestEvent::Sell(format!(
                                    "[{date_str}] {ticker}({ticker_title}) +${cash:.2} (${price:.2}x{sell_units})"
                                )))
                                .await;
            }
        }

        Ok(())
    }

    pub async fn rebalance(
        &mut self,
        targets: &Vec<(Ticker, f64)>,
        date: &NaiveDate,
        event_sender: Sender<BacktestEvent>,
    ) -> VfResult<()> {
        let date_str = utils::datetime::date_to_str(date);

        let holding_tickers: Vec<_> = self.portfolio.positions.keys().cloned().collect();
        for ticker in &holding_tickers {
            if !targets.iter().any(|(t, _)| t == ticker) {
                let kline = fetch_stock_kline(ticker, StockDividendAdjust::ForwardProp).await?;
                if let Some(price) =
                    kline.get_latest_value::<f64>(date, &StockKlineField::Close.to_string())
                {
                    let sell_units = *(self.portfolio.positions.get(ticker).unwrap_or(&0)) as f64;
                    if sell_units > 0.0 {
                        let value = sell_units * price;
                        let fee = calc_sell_fee(value, self.options);
                        let cash = value - fee;

                        self.portfolio.cash += cash;
                        self.portfolio.positions.remove(ticker);
                        self.portfolio.sideline_cash.remove(ticker);

                        let ticker_title = fetch_stock_detail(ticker).await?.title;
                        let _ = event_sender
                                .send(BacktestEvent::Sell(format!(
                                    "[{date_str}] {ticker}({ticker_title}) +${cash:.2} (${price:.2}x{sell_units})"
                                )))
                                .await;
                    }
                }
            }
        }

        for &(ref ticker, ticker_value) in targets {
            if self.portfolio.sideline_cash.contains_key(ticker) {
                self.portfolio
                    .sideline_cash
                    .entry(ticker.clone())
                    .and_modify(|v| *v = ticker_value);
            } else {
                self.scale(ticker, ticker_value, date, event_sender.clone())
                    .await?;
            }
        }

        Ok(())
    }

    pub async fn scale(
        &mut self,
        ticker: &Ticker,
        ticker_value: f64,
        date: &NaiveDate,
        event_sender: Sender<BacktestEvent>,
    ) -> VfResult<()> {
        let date_str = utils::datetime::date_to_str(date);

        let kline = fetch_stock_kline(ticker, StockDividendAdjust::ForwardProp).await?;
        if let Some(price) =
            kline.get_latest_value::<f64>(date, &StockKlineField::Close.to_string())
        {
            let holding_units = *self.portfolio.positions.get(ticker).unwrap_or(&0);
            let holding_value = holding_units as f64 * price;
            let delta_value = ticker_value - holding_value;

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
                    self.portfolio
                        .sideline_cash
                        .entry(ticker.clone())
                        .and_modify(|v| *v -= cost);

                    let ticker_title = fetch_stock_detail(ticker).await?.title;
                    let _ = event_sender
                                .send(BacktestEvent::Buy(format!(
                                    "[{date_str}] {ticker}({ticker_title}) -${cost:.2} (${price:.2}x{buy_units})"
                                )))
                                .await;
                }
            } else if delta_value < 0.0 {
                let sell_value = delta_value.abs();

                let sell_units = (sell_value / price).floor().min(holding_units as f64);
                if sell_units > 0.0 {
                    let value = sell_units * price;
                    let fee = calc_sell_fee(value, self.options);
                    let cash = value - fee;

                    self.portfolio.cash += cash;
                    self.portfolio
                        .positions
                        .entry(ticker.clone())
                        .and_modify(|v| *v -= sell_units as u64);
                    self.portfolio
                        .sideline_cash
                        .entry(ticker.clone())
                        .and_modify(|v| *v += cash)
                        .or_insert(cash);

                    let ticker_title = fetch_stock_detail(ticker).await?.title;
                    let _ = event_sender
                                .send(BacktestEvent::Sell(format!(
                                    "[{date_str}] {ticker}({ticker_title}) +${cash:.2} (${price:.2}x{sell_units})"
                                )))
                                .await;
                }
            }
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
