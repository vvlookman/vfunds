use std::{collections::HashMap, str::FromStr};

use chrono::NaiveDate;
use log::debug;

use crate::{
    FundDefinition,
    error::*,
    financial::{get_stock_daily_backward_adjust, stock::StockValuationFieldName},
    ticker::Ticker,
    utils::{
        datetime,
        financial::{calc_annual_return_rate, calc_sharpe_ratio, calc_sortino_ratio},
    },
};

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

pub struct BacktestResult {
    pub trade_days: usize,
    pub profit: f64,
    pub annual_return_rate: Option<f64>,
    pub sharpe_ratio: Option<f64>,
    pub sortino_ratio: Option<f64>,
}

pub async fn run_fund(
    fund_definition: &FundDefinition,
    options: &BacktestOptions,
) -> VfResult<BacktestResult> {
    let mut cash = options.init_cash;
    let mut positions: HashMap<String, u64> = HashMap::new();

    let mut trade_date_values: Vec<(NaiveDate, f64)> = vec![];
    let days = (options.end_date - options.start_date).num_days() as u64 + 1;

    for date in options.start_date.iter_days().take(days as usize) {
        let date_str = datetime::date_to_str(&date);

        for once_signal in fund_definition
            .signals
            .iter()
            .filter(|s| s.frequency.to_lowercase() == "once")
        {
            match once_signal.name.to_lowercase().as_str() {
                "buy-equaly" => {
                    let ticker_strs: Vec<_> = fund_definition
                        .tickers
                        .iter()
                        .filter(|t| !positions.contains_key(*t))
                        .collect();
                    if !ticker_strs.is_empty() {
                        let buy_limit = cash * (1.0 - options.broker_commission_rate)
                            / ticker_strs.len() as f64;

                        for ticker_str in ticker_strs {
                            let ticker = Ticker::from_str(ticker_str)?;

                            let stock_daily = get_stock_daily_backward_adjust(&ticker).await?;
                            if let Some(price) = stock_daily.get_latest_value::<f64>(
                                &date,
                                &StockValuationFieldName::Price.to_string(),
                            ) {
                                let amount = (buy_limit / price).floor();
                                if amount > 0.0 {
                                    let cost = amount * price;
                                    let fee = calc_buy_fee(cost, options);
                                    let total_cost = cost + fee;

                                    cash -= total_cost;
                                    positions.insert(ticker_str.to_string(), amount as u64);

                                    debug!(
                                        "[+][{date_str}] {ticker} {price:.2}x{amount} -> {total_cost:.2}"
                                    );
                                }
                            }
                        }
                    }
                }
                _ => todo!(),
            }
        }

        for ticker_str in &fund_definition.tickers {
            let ticker = Ticker::from_str(ticker_str)?;

            let stock_daily = get_stock_daily_backward_adjust(&ticker).await?;
            if stock_daily
                .get_value::<f64>(&date, &StockValuationFieldName::Price.to_string())
                .is_some()
            {
                let mut total_value = cash;
                for (ticker_str, amount) in &positions {
                    let ticker = Ticker::from_str(ticker_str)?;

                    let stock_daily = get_stock_daily_backward_adjust(&ticker).await?;
                    if let Some(price) = stock_daily
                        .get_latest_value::<f64>(&date, &StockValuationFieldName::Price.to_string())
                    {
                        total_value += *amount as f64 * price;
                    }
                }

                trade_date_values.push((date, total_value));

                break;
            }
        }
    }

    let final_cash = trade_date_values
        .last()
        .map(|(_, v)| *v - calc_sell_fee(*v, options))
        .unwrap_or(options.init_cash);
    let profit = final_cash - options.init_cash;
    let annual_return_rate = calc_annual_return_rate(options.init_cash, final_cash, days);

    let daily_values: Vec<f64> = trade_date_values.iter().map(|(_, v)| *v).collect();
    let sharpe_ratio = calc_sharpe_ratio(&daily_values, options.risk_free_rate);
    let sortino_ratio = calc_sortino_ratio(&daily_values, options.risk_free_rate);

    Ok(BacktestResult {
        trade_days: trade_date_values.len(),
        profit,
        annual_return_rate,
        sharpe_ratio,
        sortino_ratio,
    })
}

fn calc_buy_fee(transaction: f64, options: &BacktestOptions) -> f64 {
    let broker_commission = transaction * options.broker_commission_rate;
    if broker_commission > options.broker_commission_min_fee {
        broker_commission
    } else {
        options.broker_commission_min_fee
    }
}

fn calc_sell_fee(transaction: f64, options: &BacktestOptions) -> f64 {
    let stamp_duty = transaction * options.stamp_duty_rate;
    let stamp_duty_fee = if stamp_duty > options.stamp_duty_min_fee {
        stamp_duty
    } else {
        options.stamp_duty_min_fee
    };

    let broker_commission = transaction * options.broker_commission_rate;
    let broker_commission_fee = if broker_commission > options.broker_commission_min_fee {
        broker_commission
    } else {
        options.broker_commission_min_fee
    };

    stamp_duty_fee + broker_commission_fee
}
