use std::{collections::HashMap, str::FromStr};

use chrono::NaiveDate;
use tokio::sync::{mpsc, mpsc::Receiver};

use crate::{
    CHANNEL_BUFFER_DEFAULT,
    error::*,
    financial::{
        Portfolio,
        stock::{StockAdjust, StockField, fetch_stock_daily_price},
    },
    rule::Rule,
    spec::{Frequency, FundDefinition},
    ticker::Ticker,
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
    Result(BacktestResult),
    Error(VfError),
}

#[derive(Clone)]
pub struct BacktestOptions {
    pub init_cash: f64,
    pub start_date: NaiveDate,
    pub end_date: NaiveDate,
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

            let mut trade_date_values: Vec<(NaiveDate, f64)> = vec![];
            let days = (options.end_date - options.start_date).num_days() as u64 + 1;

            let mut rules = fund_definition
                .rules
                .iter()
                .map(Rule::from_definition)
                .collect::<Vec<_>>();
            let mut rule_executed_date: HashMap<usize, NaiveDate> = HashMap::new();
            for date in options.start_date.iter_days().take(days as usize) {
                for (rule_index, rule) in rules.iter_mut().enumerate() {
                    if let Some(executed_date) = rule_executed_date.get(&rule_index) {
                        let executed_days = (date - *executed_date).num_days();
                        match rule.definition().frequency {
                            Frequency::Once => {
                                continue;
                            }
                            Frequency::Daily => {
                                if executed_days < 1 {
                                    continue;
                                }
                            }
                            Frequency::Weekly => {
                                if executed_days < 7 {
                                    continue;
                                }
                            }
                            Frequency::Biweekly => {
                                if executed_days < 14 {
                                    continue;
                                }
                            }
                            Frequency::Monthly => {
                                if executed_days < 31 {
                                    continue;
                                }
                            }
                            Frequency::Quarterly => {
                                if executed_days < 92 {
                                    continue;
                                }
                            }
                            Frequency::Semiannually => {
                                if executed_days < 183 {
                                    continue;
                                }
                            }
                            Frequency::Annually => {
                                if executed_days < 366 {
                                    continue;
                                }
                            }
                        }
                    }

                    rule.exec(&mut context, &date, sender.clone()).await?;
                    rule_executed_date.insert(rule_index, date);
                }

                let tickers = context.fund_definition.all_tickers(&date).await?;
                for ticker in tickers {
                    // Check if today is trade date
                    let stock_daily =
                        fetch_stock_daily_price(&ticker, StockAdjust::Forward).await?;
                    if stock_daily
                        .get_value::<f64>(&date, &StockField::PriceClose.to_string())
                        .is_some()
                    {
                        let total_value = context.calc_total_value(&date).await?;
                        trade_date_values.push((date, total_value));

                        break;
                    }
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

        for (ticker_str, units) in &self.portfolio.positions {
            let ticker = Ticker::from_str(ticker_str)?;

            let stock_daily = fetch_stock_daily_price(&ticker, StockAdjust::Forward).await?;
            if let Some(price) =
                stock_daily.get_latest_value::<f64>(date, &StockField::PriceClose.to_string())
            {
                let value = *units as f64 * price;
                let fee = calc_sell_fee(value, self.options);

                total_value += value - fee;
            } else {
                return Err(VfError::NoData(
                    "NO_TICKER_PRICE",
                    format!("Price of '{ticker_str}' not exists"),
                ));
            }
        }

        Ok(total_value)
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
