use std::{collections::HashMap, str::FromStr};

use chrono::NaiveDate;

use crate::{
    error::*,
    financial::{Portfolio, get_stock_daily_backward_adjust, stock::StockValuationField},
    rule::Rule,
    spec::{Frequency, FundDefinition},
    ticker::Ticker,
    utils::financial::{calc_annual_return_rate, calc_sharpe_ratio, calc_sortino_ratio},
};

pub struct BacktestContext<'a> {
    pub fund_definition: &'a FundDefinition,
    pub options: &'a BacktestOptions,
    pub portfolio: &'a mut Portfolio,
}

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
) -> VfResult<BacktestResult> {
    let mut context = BacktestContext {
        fund_definition,
        options,
        portfolio: &mut Portfolio::new(options.init_cash),
    };

    let mut trade_date_values: Vec<(NaiveDate, f64)> = vec![];
    let days = (options.end_date - options.start_date).num_days() as u64 + 1;

    let mut rule_executed_date: HashMap<usize, NaiveDate> = HashMap::new();
    for date in options.start_date.iter_days().take(days as usize) {
        for (rule_index, rule_definition) in fund_definition.rules.iter().enumerate() {
            let mut rule = Rule::from_definition(rule_definition);

            if let Some(executed_date) = rule_executed_date.get(&rule_index) {
                let executed_days = (date - *executed_date).num_days();
                match rule_definition.frequency {
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
                    Frequency::Yearly => {
                        if executed_days < 366 {
                            continue;
                        }
                    }
                }
            }

            rule.exec(&mut context, &date).await?;
            rule_executed_date.insert(rule_index, date);
        }

        for ticker_str in &fund_definition.tickers {
            let ticker = Ticker::from_str(ticker_str)?;

            let stock_daily = get_stock_daily_backward_adjust(&ticker).await?;
            if stock_daily
                .get_value::<f64>(&date, &StockValuationField::Price.to_string())
                .is_some()
            {
                let mut total_value: f64 = context.portfolio.cash;
                for (ticker_str, units) in &context.portfolio.positions {
                    let ticker = Ticker::from_str(ticker_str)?;

                    let stock_daily = get_stock_daily_backward_adjust(&ticker).await?;
                    if let Some(price) = stock_daily
                        .get_latest_value::<f64>(&date, &StockValuationField::Price.to_string())
                    {
                        let value = *units as f64 * price;
                        let fee = calc_sell_fee(value, options);

                        total_value += value - fee;
                    }
                }

                trade_date_values.push((date, total_value));

                break;
            }
        }
    }

    let final_cash = trade_date_values
        .last()
        .map(|(_, v)| *v)
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
