use std::{
    cmp::Ordering,
    collections::{HashMap, HashSet},
    time::Instant,
};

use chrono::{Datelike, Duration, NaiveDate};
use itertools::Itertools;
use tokio::sync::{mpsc, mpsc::Sender};

use crate::{
    CHANNEL_BUFFER_DEFAULT,
    backtest::*,
    financial::{tool::fetch_trade_dates, *},
    rule::Rule,
    spec::*,
    ticker::Ticker,
    utils::{
        datetime::{date_to_str, secs_to_human_str},
        stats::mean,
    },
};

pub struct FundBacktestContext<'a> {
    pub options: &'a BacktestOptions,
    pub fund_definition: &'a FundDefinition,
    pub portfolio: &'a mut Portfolio,
    pub order_dates: &'a mut HashSet<NaiveDate>,

    suspended_cash: Option<HashMap<Ticker, f64>>,
}

impl FundBacktestContext<'_> {
    pub async fn cash_deploy_free(
        &mut self,
        date: &NaiveDate,
        event_sender: &Sender<BacktestEvent>,
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
                                let buy_price_type = if self.options.pessimistic {
                                    PriceType::High
                                } else {
                                    PriceType::Mid
                                };
                                if let Some(buy_price) =
                                    get_ticker_price(ticker, date, true, &buy_price_type).await?
                                {
                                    let target_units =
                                        *units + (delta_value / buy_price).floor() as u64;

                                    self.position_scale(ticker, target_units, date, event_sender)
                                        .await?;
                                } else {
                                    let _ = event_sender
                                        .send(BacktestEvent::Warning {
                                            title: "".to_string(),
                                            message: format!("Price of '{ticker}' not exists"),
                                            date: Some(*date),
                                        })
                                        .await;
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
        event_sender: &Sender<BacktestEvent>,
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

                        let sell_price_type = if self.options.pessimistic {
                            PriceType::Low
                        } else {
                            PriceType::Mid
                        };
                        if let Some(sell_price) =
                            get_ticker_price(ticker, date, true, &sell_price_type).await?
                        {
                            let sell_units =
                                (delta_value / sell_price).ceil().min(*units as f64) as u64;
                            let target_units = *units - sell_units;
                            if target_units > 0 {
                                self.position_scale(ticker, target_units, date, event_sender)
                                    .await?;
                            } else {
                                self.position_close_with_price_type(
                                    ticker,
                                    false,
                                    &sell_price_type,
                                    date,
                                    event_sender,
                                )
                                .await?;
                            }
                        } else {
                            let _ = event_sender
                                .send(BacktestEvent::Warning {
                                    title: "".to_string(),
                                    message: format!("Price of '{ticker}' not exists"),
                                    date: Some(*date),
                                })
                                .await;
                        }
                    }
                }
            }
        }

        Ok(())
    }

    pub fn is_suspended(&self) -> bool {
        self.suspended_cash.is_some()
    }

    pub async fn rebalance(
        &mut self,
        targets_weight: &[(Ticker, f64)],
        date: &NaiveDate,
        event_sender: &Sender<BacktestEvent>,
    ) -> VfResult<()> {
        // Make sure weight is valid
        let targets_weight: Vec<&(Ticker, f64)> = targets_weight
            .iter()
            .filter(|(_, weight)| weight.is_finite())
            .collect();

        // Close unneeded positions and reserved cash
        {
            let position_tickers: Vec<_> = self.portfolio.positions.keys().cloned().collect();
            for ticker in &position_tickers {
                if !targets_weight.iter().any(|(t, _)| t == ticker) {
                    self.position_close(ticker, false, date, event_sender)
                        .await?;
                }
            }

            let reserved_tickers: Vec<_> = self.portfolio.reserved_cash.keys().cloned().collect();
            for ticker in &reserved_tickers {
                if !targets_weight.iter().any(|(t, _)| t == ticker) {
                    if let Some((cash, _)) = self.portfolio.reserved_cash.get(ticker) {
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

                for (ticker, weight) in &targets_weight {
                    let ticker_value = total_value * (1.0 - self.options.buffer_ratio) * weight
                        / targets_weight_sum;
                    if let Some((current_reserved_cash, _)) =
                        self.portfolio.reserved_cash.get(ticker)
                    {
                        let delta_cash = ticker_value - current_reserved_cash;

                        self.portfolio.free_cash -= delta_cash;
                        self.portfolio
                            .reserved_cash
                            .entry(ticker.clone())
                            .and_modify(|v| v.0 += delta_cash);
                    } else {
                        if let Some(price) =
                            get_ticker_price(ticker, date, true, &PriceType::Close).await?
                        {
                            let target_units = (ticker_value / price).floor() as u64;

                            self.position_scale(ticker, target_units, date, event_sender)
                                .await?;
                        } else {
                            let _ = event_sender
                                .send(BacktestEvent::Warning {
                                    title: "".to_string(),
                                    message: format!("Price of '{ticker}' not exists"),
                                    date: Some(*date),
                                })
                                .await;
                        }
                    }
                }
            }
        }

        let cash = self.calc_cash();
        let positions_value = self.calc_positions_value(date).await?;

        let _ = notify_portfolio(
            event_sender,
            date,
            cash,
            &positions_value,
            self.options.init_cash,
        )
        .await;

        Ok(())
    }

    pub async fn position_close(
        &mut self,
        ticker: &Ticker,
        make_reserved: bool,
        date: &NaiveDate,
        event_sender: &Sender<BacktestEvent>,
    ) -> VfResult<f64> {
        let sell_price_type = if self.options.pessimistic {
            PriceType::Low
        } else {
            PriceType::Mid
        };

        self.position_close_with_price_type(
            ticker,
            make_reserved,
            &sell_price_type,
            date,
            event_sender,
        )
        .await
    }

    pub async fn position_close_with_price(
        &mut self,
        ticker: &Ticker,
        make_reserved: bool,
        sell_price: f64,
        date: &NaiveDate,
        event_sender: &Sender<BacktestEvent>,
    ) -> VfResult<f64> {
        let position_units = *self.portfolio.positions.get(ticker).unwrap_or(&0);
        let cash = if position_units > 0 {
            let sell_units = position_units as f64;
            let value = sell_units * sell_price;
            let fee = calc_sell_fee(value, self.options);
            let amount = value - fee;

            if make_reserved {
                self.portfolio
                    .reserved_cash
                    .entry(ticker.clone())
                    .and_modify(|v| v.0 += amount)
                    .or_insert((amount, *date));
            } else {
                self.portfolio.free_cash += amount;
            }
            self.portfolio.positions.remove(ticker);

            self.order_dates.insert(*date);
            let _ = event_sender
                .send(BacktestEvent::Sell {
                    title: get_ticker_title(ticker).await,
                    amount,
                    price: sell_price,
                    units: sell_units as u64,
                    date: *date,
                })
                .await;

            amount
        } else {
            0.0
        };

        Ok(cash)
    }

    pub async fn position_close_with_price_type(
        &mut self,
        ticker: &Ticker,
        make_reserved: bool,
        sell_price_type: &PriceType,
        date: &NaiveDate,
        event_sender: &Sender<BacktestEvent>,
    ) -> VfResult<f64> {
        if let Some(sell_price) = get_ticker_price(ticker, date, true, sell_price_type).await? {
            self.position_close_with_price(ticker, make_reserved, sell_price, date, event_sender)
                .await
        } else {
            let _ = event_sender
                .send(BacktestEvent::Warning {
                    title: "".to_string(),
                    message: format!("Price of '{ticker}' not exists"),
                    date: Some(*date),
                })
                .await;

            Ok(0.0)
        }
    }

    pub async fn position_entry_reserved(
        &mut self,
        ticker: &Ticker,
        date: &NaiveDate,
        event_sender: &Sender<BacktestEvent>,
    ) -> VfResult<()> {
        let buy_price_type = if self.options.pessimistic {
            PriceType::High
        } else {
            PriceType::Mid
        };

        self.position_entry_reserved_with_price_type(ticker, &buy_price_type, date, event_sender)
            .await
    }

    pub async fn position_entry_reserved_with_price(
        &mut self,
        ticker: &Ticker,
        buy_price: f64,
        date: &NaiveDate,
        event_sender: &Sender<BacktestEvent>,
    ) -> VfResult<()> {
        if let Some((reserved_cash, _)) = self.portfolio.reserved_cash.get(ticker) {
            let delta_value = reserved_cash - calc_buy_fee(*reserved_cash, self.options);

            let buy_units = (delta_value / buy_price).floor() as u64;
            if buy_units > 0 {
                let value = buy_units as f64 * buy_price;
                let fee = calc_buy_fee(value, self.options);
                let amount = value + fee;

                self.portfolio.free_cash += *reserved_cash - amount;
                self.portfolio.reserved_cash.remove(ticker);

                self.portfolio
                    .positions
                    .entry(ticker.clone())
                    .and_modify(|v| *v += buy_units)
                    .or_insert(buy_units);

                self.order_dates.insert(*date);
                let _ = event_sender
                    .send(BacktestEvent::Buy {
                        title: get_ticker_title(ticker).await,
                        amount,
                        price: buy_price,
                        units: buy_units as u64,
                        date: *date,
                    })
                    .await;
            }
        }

        Ok(())
    }

    pub async fn position_entry_reserved_with_price_type(
        &mut self,
        ticker: &Ticker,
        buy_price_type: &PriceType,
        date: &NaiveDate,
        event_sender: &Sender<BacktestEvent>,
    ) -> VfResult<()> {
        if let Some(buy_price) = get_ticker_price(ticker, date, true, buy_price_type).await? {
            self.position_entry_reserved_with_price(ticker, buy_price, date, event_sender)
                .await
        } else {
            let _ = event_sender
                .send(BacktestEvent::Warning {
                    title: "".to_string(),
                    message: format!("Price of '{ticker}' not exists"),
                    date: Some(*date),
                })
                .await;

            Ok(())
        }
    }

    pub async fn position_open(
        &mut self,
        ticker: &Ticker,
        cash: f64,
        date: &NaiveDate,
        event_sender: &Sender<BacktestEvent>,
    ) -> VfResult<()> {
        let buy_price_type = if self.options.pessimistic {
            PriceType::High
        } else {
            PriceType::Mid
        };

        self.position_open_with_price_type(ticker, cash, &buy_price_type, date, event_sender)
            .await
    }

    pub async fn position_open_with_price(
        &mut self,
        ticker: &Ticker,
        cash: f64,
        buy_price: f64,
        date: &NaiveDate,
        event_sender: &Sender<BacktestEvent>,
    ) -> VfResult<()> {
        let delta_value = cash - calc_buy_fee(cash, self.options);

        let buy_units = (delta_value / buy_price).floor() as u64;
        if buy_units > 0 {
            let value = buy_units as f64 * buy_price;
            let fee = calc_buy_fee(value, self.options);
            let amount = value + fee;

            self.portfolio.free_cash -= amount;
            self.portfolio
                .positions
                .entry(ticker.clone())
                .and_modify(|v| *v += buy_units)
                .or_insert(buy_units);

            self.order_dates.insert(*date);
            let _ = event_sender
                .send(BacktestEvent::Buy {
                    title: get_ticker_title(ticker).await,
                    amount,
                    price: buy_price,
                    units: buy_units,
                    date: *date,
                })
                .await;
        }

        Ok(())
    }

    pub async fn position_open_with_price_type(
        &mut self,
        ticker: &Ticker,
        cash: f64,
        buy_price_type: &PriceType,
        date: &NaiveDate,
        event_sender: &Sender<BacktestEvent>,
    ) -> VfResult<()> {
        if let Some(buy_price) = get_ticker_price(ticker, date, true, buy_price_type).await? {
            self.position_open_with_price(ticker, cash, buy_price, date, event_sender)
                .await
        } else {
            let _ = event_sender
                .send(BacktestEvent::Warning {
                    title: "".to_string(),
                    message: format!("Price of '{ticker}' not exists"),
                    date: Some(*date),
                })
                .await;

            Ok(())
        }
    }

    pub async fn position_scale(
        &mut self,
        ticker: &Ticker,
        target_units: u64,
        date: &NaiveDate,
        event_sender: &Sender<BacktestEvent>,
    ) -> VfResult<()> {
        let buy_price_type = if self.options.pessimistic {
            PriceType::High
        } else {
            PriceType::Mid
        };

        let sell_price_type = if self.options.pessimistic {
            PriceType::Low
        } else {
            PriceType::Mid
        };

        self.position_scale_with_price_type(
            ticker,
            target_units,
            &buy_price_type,
            &sell_price_type,
            date,
            event_sender,
        )
        .await
    }

    pub async fn position_scale_with_price_type(
        &mut self,
        ticker: &Ticker,
        target_units: u64,
        buy_price_type: &PriceType,
        sell_price_type: &PriceType,
        date: &NaiveDate,
        event_sender: &Sender<BacktestEvent>,
    ) -> VfResult<()> {
        let position_units = *self.portfolio.positions.get(ticker).unwrap_or(&0);
        let delta_units: i64 = target_units as i64 - position_units as i64;
        if (delta_units as f64).abs()
            < (position_units as f64 * self.options.position_tolerance).abs()
        {
            return Ok(());
        }

        let ticker_title = get_ticker_title(ticker).await;
        if delta_units > 0 {
            if let Some(buy_price) = get_ticker_price(ticker, date, true, buy_price_type).await? {
                let total_value = self.calc_total_value(date).await?;
                let buffer_cash = total_value * self.options.buffer_ratio;
                let available_cash = self.portfolio.free_cash - buffer_cash;

                let value = (delta_units as f64 * buy_price).min(available_cash);
                let buy_units = (value / buy_price).floor() as u64;

                if buy_units > 0 {
                    let fee = calc_buy_fee(value, self.options);
                    let amount = value + fee;

                    self.portfolio.free_cash -= amount;

                    self.portfolio
                        .positions
                        .entry(ticker.clone())
                        .and_modify(|v| *v += buy_units)
                        .or_insert(buy_units);

                    self.order_dates.insert(*date);
                    let _ = event_sender
                        .send(BacktestEvent::Buy {
                            title: ticker_title,
                            amount,
                            price: buy_price,
                            units: buy_units,
                            date: *date,
                        })
                        .await;
                }
            } else {
                let _ = event_sender
                    .send(BacktestEvent::Warning {
                        title: "".to_string(),
                        message: format!("Price of '{ticker}' not exists"),
                        date: Some(*date),
                    })
                    .await;
            }
        } else {
            if let Some(sell_price) = get_ticker_price(ticker, date, true, sell_price_type).await? {
                let sell_units = delta_units.unsigned_abs();
                let value = sell_units as f64 * sell_price;
                let fee = calc_sell_fee(value, self.options);
                let amount = value - fee;

                self.portfolio.free_cash += amount;

                if sell_units == position_units {
                    self.portfolio.positions.remove(ticker);
                } else {
                    self.portfolio
                        .positions
                        .entry(ticker.clone())
                        .and_modify(|v| *v -= sell_units);
                }

                self.order_dates.insert(*date);
                let _ = event_sender
                    .send(BacktestEvent::Sell {
                        title: ticker_title,
                        amount,
                        price: sell_price,
                        units: sell_units,
                        date: *date,
                    })
                    .await;
            } else {
                let _ = event_sender
                    .send(BacktestEvent::Warning {
                        title: "".to_string(),
                        message: format!("Price of '{ticker}' not exists"),
                        date: Some(*date),
                    })
                    .await;
            }
        }

        Ok(())
    }

    pub async fn resume(
        &mut self,
        date: &NaiveDate,
        event_sender: &Sender<BacktestEvent>,
    ) -> VfResult<()> {
        if let Some(suspended_cash) = &self.suspended_cash.clone() {
            let mut suspended_strs: Vec<String> = vec![];
            for (ticker, cash) in suspended_cash {
                self.position_open(ticker, *cash, date, event_sender)
                    .await?;

                let ticker_title = get_ticker_title(ticker).await;
                suspended_strs.push(format!("{ticker_title}=${cash:.2}"));
            }
            self.suspended_cash = None;

            let _ = event_sender
                .send(BacktestEvent::Info {
                    title: "[↑ Resumed]".to_string(),
                    message: suspended_strs.join(" "),
                    date: Some(*date),
                })
                .await;

            let cash = self.calc_cash();
            let positions_value = self.calc_positions_value(date).await?;

            let _ = notify_portfolio(
                event_sender,
                date,
                cash,
                &positions_value,
                self.options.init_cash,
            )
            .await;
        }

        Ok(())
    }

    pub async fn suspend(
        &mut self,
        date: &NaiveDate,
        event_sender: &Sender<BacktestEvent>,
    ) -> VfResult<()> {
        if self.suspended_cash.is_none() {
            let mut suspended_cash: HashMap<Ticker, f64> = HashMap::new();
            let mut suspended_strs: Vec<String> = vec![];
            for ticker in &self.portfolio.positions.keys().cloned().collect::<Vec<_>>() {
                let cash = self
                    .position_close(ticker, false, date, event_sender)
                    .await?;
                suspended_cash.insert(ticker.clone(), cash);

                let ticker_title = get_ticker_title(ticker).await;
                suspended_strs.push(format!("{ticker_title}=${cash:.2}"));
            }
            self.suspended_cash = Some(suspended_cash);

            let _ = event_sender
                .send(BacktestEvent::Info {
                    title: "[↓ Suspended]".to_string(),
                    message: suspended_strs.join(" "),
                    date: Some(*date),
                })
                .await;

            let cash = self.calc_cash();
            let positions_value = self.calc_positions_value(date).await?;

            let _ = notify_portfolio(
                event_sender,
                date,
                cash,
                &positions_value,
                self.options.init_cash,
            )
            .await;
        }

        Ok(())
    }

    pub fn watching_tickers(&self) -> Vec<Ticker> {
        let hold_tickers: Vec<Ticker> = self.portfolio.positions.keys().cloned().collect();
        let reserved_tickers: Vec<Ticker> = self.portfolio.reserved_cash.keys().cloned().collect();

        hold_tickers.into_iter().chain(reserved_tickers).collect()
    }

    fn calc_cash(&self) -> f64 {
        self.portfolio.free_cash
            + self
                .portfolio
                .reserved_cash
                .values()
                .map(|(cash, _)| cash)
                .sum::<f64>()
    }

    async fn calc_positions_value(&self, date: &NaiveDate) -> VfResult<HashMap<Ticker, f64>> {
        let mut positions_value: HashMap<Ticker, f64> = HashMap::new();

        for (ticker, units) in &self.portfolio.positions {
            if let Some(price) = get_ticker_price(ticker, date, true, &PriceType::Close).await? {
                positions_value.insert(ticker.clone(), *units as f64 * price);
            } else {
                return Err(VfError::NoData {
                    code: "NO_PRICE_DATA",
                    message: format!("Price data of '{ticker}' not exists"),
                });
            }
        }

        Ok(positions_value)
    }

    async fn calc_total_value(&self, date: &NaiveDate) -> VfResult<f64> {
        let positions_value = self.calc_positions_value(date).await?;
        let total_value = self.calc_cash() + positions_value.values().sum::<f64>();

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
                order_dates: &mut HashSet::new(),

                suspended_cash: None,
            };

            let mut rules = fund_definition
                .rules
                .iter()
                .map(Rule::from_definition)
                .collect::<Vec<_>>();

            let days = (options.end_date - options.start_date).num_days() as u64 + 1;

            let mut trade_dates_value: Vec<(NaiveDate, f64)> = vec![];

            let mut rules_period_start_date: HashMap<usize, NaiveDate> = HashMap::new();
            let trade_dates = fetch_trade_dates().await?;
            for date in options.start_date.iter_days().take(days as usize) {
                if trade_dates.contains(&date) {
                    // Check suspend, when suspended, keep empty positions
                    if fund_definition
                        .options
                        .suspend_months
                        .contains(&date.month())
                    {
                        if !context.is_suspended() {
                            context.suspend(&date, &sender).await?;
                        }

                        continue;
                    } else {
                        if context.is_suspended() {
                            context.resume(&date, &sender).await?;
                        }
                    }

                    // Excute rules
                    for (rule_index, rule) in rules.iter_mut().enumerate() {
                        if let Some(period_start_date) = rules_period_start_date.get(&rule_index) {
                            // Check frequency
                            let days = (date - *period_start_date).num_days();
                            let period_days = rule.definition().frequency.to_days();
                            if period_days > 0 {
                                if days < period_days as i64 {
                                    continue;
                                }
                            } else {
                                continue;
                            }
                        }

                        match rule.exec(&mut context, &date, &sender).await {
                            Ok(_) => {
                                rules_period_start_date.insert(rule_index, date);
                            }
                            Err(err) => {
                                return Err(err);
                            }
                        }
                    }

                    if let Ok(total_value) = context.calc_total_value(&date).await {
                        trade_dates_value.push((date, total_value));
                    }
                }
            }

            let final_cash = context.calc_cash();
            let final_positions_value = context.calc_positions_value(&options.end_date).await?;

            let _ = notify_portfolio(
                &sender,
                &options.end_date,
                final_cash,
                &final_positions_value,
                options.init_cash,
            )
            .await;

            let mut order_dates: Vec<NaiveDate> = context.order_dates.iter().copied().collect();
            order_dates.sort_unstable();

            Ok(BacktestResult {
                title: Some(fund_definition.title.clone()),
                options: options.clone(),
                final_cash,
                final_positions_value,
                metrics: BacktestMetrics::from_daily_value(&trade_dates_value, options),
                order_dates,
                trade_dates_value,
            })
        };

        match single_run(&fund_definition, &options).await {
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

pub async fn backtest_fund_cv(
    fund_definition: &FundDefinition,
    cv_options: &BacktestCvOptions,
) -> VfResult<BacktestStream> {
    cv_options.base_options.check();

    let (sender, receiver) = mpsc::channel(CHANNEL_BUFFER_DEFAULT);

    let fund_definition = fund_definition.clone();
    let cv_options = cv_options.clone();

    tokio::spawn(async move {
        let process = async || -> VfResult<()> {
            if cv_options.cv_search {
                let mut search_frequencies: Vec<RuleFrequencies> = vec![];
                for rule_definition in &fund_definition.rules {
                    if rule_definition.search.frequency.is_empty() {
                        search_frequencies.push(RuleFrequencies {
                            rule_name: rule_definition.name.to_string(),
                            frequencies: vec![rule_definition.frequency.clone()],
                        });
                    } else {
                        search_frequencies.push(RuleFrequencies {
                            rule_name: rule_definition.name.to_string(),
                            frequencies: rule_definition.search.frequency.clone(),
                        });
                    }
                }

                let mut search_options: Vec<RuleOptionValues> = vec![];
                for rule_definition in &fund_definition.rules {
                    for (k, v) in &rule_definition.search.options {
                        search_options.push(RuleOptionValues {
                            rule_name: rule_definition.name.to_string(),
                            option_name: k.to_string(),
                            option_values: v.clone(),
                        });
                    }
                }

                let cv_count = search_frequencies
                    .iter()
                    .map(|v| v.frequencies.len())
                    .product::<usize>()
                    * search_options
                        .iter()
                        .map(|v| v.option_values.len())
                        .product::<usize>()
                    * cv_options.cv_start_dates.len();

                let cv_start = Instant::now();

                struct Search {
                    rule_frequencies: Vec<RuleFrequency>,
                    rule_options: Vec<RuleOptionValue>,
                }

                let mut cv_search_results: Vec<(Search, HashMap<NaiveDate, BacktestResult>)> =
                    vec![];
                for (i, rule_frequencies) in search_frequencies
                    .iter()
                    .map(|v| {
                        v.frequencies
                            .iter()
                            .map(|frequency| RuleFrequency {
                                rule_name: v.rule_name.to_string(),
                                frequency: frequency.clone(),
                            })
                            .collect::<Vec<_>>()
                    })
                    .multi_cartesian_product()
                    .enumerate()
                {
                    for (j, rule_options) in search_options
                        .iter()
                        .map(|v| {
                            v.option_values
                                .iter()
                                .map(|option_value| RuleOptionValue {
                                    rule_name: v.rule_name.to_string(),
                                    option_name: v.option_name.to_string(),
                                    option_value: option_value.clone(),
                                })
                                .collect::<Vec<_>>()
                        })
                        .multi_cartesian_product()
                        .enumerate()
                    {
                        let mut fund_definition = fund_definition.clone();

                        for rule_frequency in &rule_frequencies {
                            if let Some(rule_definition) = fund_definition
                                .rules
                                .iter_mut()
                                .find(|r| r.name == rule_frequency.rule_name)
                            {
                                rule_definition.frequency = rule_frequency.frequency.clone();
                            }
                        }

                        for rule_option in &rule_options {
                            if let Some(rule_definition) = fund_definition
                                .rules
                                .iter_mut()
                                .find(|r| r.name == rule_option.rule_name)
                            {
                                rule_definition.options.insert(
                                    rule_option.option_name.to_string(),
                                    rule_option.option_value.clone(),
                                );
                            }
                        }

                        let mut cv_results: HashMap<NaiveDate, BacktestResult> = HashMap::new();
                        for (k, cv_start_date) in cv_options.cv_start_dates.iter().enumerate() {
                            let mut options = cv_options.base_options.clone();
                            options.start_date = *cv_start_date;

                            let mut stream = backtest_fund(&fund_definition, &options).await?;

                            while let Some(event) = stream.next().await {
                                match event {
                                    BacktestEvent::Result(result) => {
                                        let _ = sender
                                            .send(BacktestEvent::Info {
                                                title: format!(
                                                    "[CV {}/{cv_count} {}] [{}~{}]",
                                                    i * search_options.len()
                                                        * cv_options.cv_start_dates.len()
                                                        + j * cv_options.cv_start_dates.len()
                                                        + k
                                                        + 1,
                                                    secs_to_human_str(cv_start.elapsed().as_secs()),
                                                    date_to_str(&options.start_date),
                                                    date_to_str(&options.end_date),
                                                ),
                                                message: format!(
                                                    "[ARR={} Sharpe={}] {} {}",
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
                                                    rule_frequencies
                                                        .iter()
                                                        .map(|v| {
                                                            format!(
                                                                "{}.frequency={}",
                                                                v.rule_name,
                                                                v.frequency.to_str(),
                                                            )
                                                        })
                                                        .collect::<Vec<_>>()
                                                        .join(" "),
                                                    rule_options
                                                        .iter()
                                                        .map(|v| {
                                                            format!(
                                                                "{}.{}={}",
                                                                v.rule_name,
                                                                v.option_name,
                                                                v.option_value
                                                            )
                                                        })
                                                        .collect::<Vec<_>>()
                                                        .join(" ")
                                                ),
                                                date: None,
                                            })
                                            .await;

                                        cv_results.insert(*cv_start_date, *result);
                                    }
                                    _ => {
                                        let _ = sender.send(event).await;
                                    }
                                }
                            }
                        }

                        cv_search_results.push((
                            Search {
                                rule_frequencies: rule_frequencies.clone(),
                                rule_options: rule_options.clone(),
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
                                if (best_score - cv_score.score).abs() < best_score.abs() * 1e-2 {
                                    &format!("Top {top} ≈ Best")
                                } else {
                                    &format!("Top {top}")
                                }
                            };

                            let _ = sender
                                .send(BacktestEvent::Info {
                                    title: format!("[CV {top_str}]"),
                                    message: format!(
                                        "[ARR={:.2}% Sharpe={:.3}] {} {}",
                                        cv_score.arr * 100.0,
                                        cv_score.sharpe,
                                        search
                                            .rule_frequencies
                                            .iter()
                                            .map(|v| {
                                                format!(
                                                    "{}.frequency={}",
                                                    v.rule_name,
                                                    v.frequency.to_str()
                                                )
                                            })
                                            .collect::<Vec<_>>()
                                            .join(" "),
                                        search
                                            .rule_options
                                            .iter()
                                            .map(|v| {
                                                format!(
                                                    "{}.{}={}",
                                                    v.rule_name, v.option_name, v.option_value
                                                )
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

                    let mut stream = backtest_fund(&fund_definition, &options).await?;

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

pub async fn backtest_funds(
    funds: &[(String, FundDefinition)],
    options: &BacktestOptions,
    sender: &Sender<BacktestEvent>,
) -> VfResult<Vec<(String, BacktestResult)>> {
    let mut funds_result: Vec<(String, BacktestResult)> = vec![];

    for (fund_name, fund_definition) in funds.iter() {
        let mut stream = backtest_fund(fund_definition, options).await?;

        while let Some(event) = stream.next().await {
            match event {
                BacktestEvent::Buy {
                    title,
                    amount,
                    price,
                    units,
                    date,
                } => {
                    let _ = sender
                        .send(BacktestEvent::Buy {
                            title: format!("[{fund_name}] {title}"),
                            amount,
                            price,
                            units,
                            date,
                        })
                        .await;
                }
                BacktestEvent::Sell {
                    title,
                    amount,
                    price,
                    units,
                    date,
                } => {
                    let _ = sender
                        .send(BacktestEvent::Sell {
                            title: format!("[{fund_name}] {title}"),
                            amount,
                            price,
                            units,
                            date,
                        })
                        .await;
                }
                BacktestEvent::Info {
                    title,
                    message,
                    date,
                } => {
                    let _ = sender
                        .send(BacktestEvent::Info {
                            title: format!("[{fund_name}] {title}"),
                            message,
                            date,
                        })
                        .await;
                }
                BacktestEvent::Warning {
                    title,
                    message,
                    date,
                } => {
                    let _ = sender
                        .send(BacktestEvent::Warning {
                            title: format!("[{fund_name}] {title}"),
                            message,
                            date,
                        })
                        .await;
                }
                BacktestEvent::Toast {
                    title,
                    message,
                    date,
                } => {
                    let _ = sender
                        .send(BacktestEvent::Toast {
                            title: format!("[{fund_name}] {title}"),
                            message,
                            date,
                        })
                        .await;
                }
                BacktestEvent::Result(fund_result) => {
                    funds_result.push((fund_name.to_string(), *fund_result));
                }
                BacktestEvent::Error(_) => {
                    let _ = sender.send(event).await;
                }
            }
        }
    }

    Ok(funds_result)
}

#[derive(Clone)]
struct RuleFrequency {
    rule_name: String,
    frequency: Frequency,
}

#[derive(Clone)]
struct RuleFrequencies {
    rule_name: String,
    frequencies: Vec<Frequency>,
}

#[derive(Clone)]
struct RuleOptionValue {
    rule_name: String,
    option_name: String,
    option_value: serde_json::Value,
}

#[derive(Clone)]
struct RuleOptionValues {
    rule_name: String,
    option_name: String,
    option_values: Vec<serde_json::Value>,
}
