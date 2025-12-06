use std::{
    cmp::Ordering,
    collections::{BTreeMap, HashMap, HashSet},
    fmt::{Display, Formatter},
    str::FromStr,
    time::Instant,
};

use chrono::{Datelike, Duration, NaiveDate};
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use tokio::sync::{
    mpsc,
    mpsc::{Receiver, Sender},
};

use crate::{
    CHANNEL_BUFFER_DEFAULT, POSITION_TOLERANCE, WORKSPACE,
    error::*,
    financial::{Portfolio, get_ticker_price, get_ticker_title, tool::fetch_trade_dates},
    rule::Rule,
    spec::{FofDefinition, Frequency, FundDefinition, TickerSourceDefinition},
    ticker::Ticker,
    utils::{
        datetime::{date_to_str, secs_to_human_str},
        financial::{
            calc_annualized_return_rate_by_start_end, calc_annualized_volatility,
            calc_max_drawdown, calc_profit_factor, calc_sharpe_ratio, calc_sortino_ratio,
            calc_win_rate,
        },
        math::normalize_zscore,
        stats::mean,
    },
};

#[derive(Clone, Debug)]
pub struct BacktestCvOptions {
    pub base_options: BacktestOptions,

    pub cv_start_dates: Vec<NaiveDate>,
    pub cv_search: bool,
    pub cv_window: bool,
    pub cv_min_window_days: u64,
    pub cv_score_arr_weight: f64,
}

pub enum BacktestEvent {
    Buy {
        title: String,
        amount: f64,
        price: f64,
        units: u64,
        date: NaiveDate,
    },
    Sell {
        title: String,
        amount: f64,
        price: f64,
        units: u64,
        date: NaiveDate,
    },
    Info {
        title: String,
        message: String,
        date: Option<NaiveDate>,
    },
    Warning {
        title: String,
        message: String,
        date: Option<NaiveDate>,
    },
    Toast {
        title: String,
        message: String,
        date: Option<NaiveDate>,
    },
    Result(Box<BacktestResult>),
    Error(VfError),
}

impl Display for BacktestEvent {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            BacktestEvent::Buy {
                title,
                amount,
                price,
                units,
                date,
            } => {
                let date_str = date_to_str(date);
                let mut s = format!("[+] [{date_str}] ");
                if !title.is_empty() {
                    s.push_str(title);
                    s.push(' ');
                }
                s.push_str(&format!("-${amount:.2} (${price:.2}x{units})"));
                s
            }
            BacktestEvent::Sell {
                title,
                amount,
                price,
                units,
                date,
            } => {
                let date_str = date_to_str(date);
                let mut s = format!("[-] [{date_str}] ");
                if !title.is_empty() {
                    s.push_str(title);
                    s.push(' ');
                }
                s.push_str(&format!("+${amount:.2} (${price:.2}x{units})"));
                s
            }
            BacktestEvent::Info {
                title,
                message,
                date,
            } => {
                let mut s = "[i] ".to_string();
                if let Some(date) = date {
                    s.push_str(&format!("[{}]", date_to_str(date)));
                    s.push(' ');
                }
                if !title.is_empty() {
                    s.push_str(title);
                    s.push(' ');
                }
                s.push_str(message);
                s
            }
            BacktestEvent::Warning {
                title,
                message,
                date,
            } => {
                let mut s = "[!] ".to_string();
                if let Some(date) = date {
                    s.push_str(&format!("[{}]", date_to_str(date)));
                    s.push(' ');
                }
                if !title.is_empty() {
                    s.push_str(title);
                    s.push(' ');
                }
                s.push_str(message);
                s
            }
            BacktestEvent::Toast {
                title,
                message,
                date,
            } => {
                let mut s = "[i] ".to_string();
                if let Some(date) = date {
                    s.push_str(&format!("[{}]", date_to_str(date)));
                    s.push(' ');
                }
                if !title.is_empty() {
                    s.push_str(title);
                    s.push(' ');
                }
                s.push_str(message);
                s
            }
            BacktestEvent::Result(fund_result) => fund_result.to_string(),
            BacktestEvent::Error(err) => err.to_string(),
        };

        write!(f, "{s}")
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct BacktestMetrics {
    #[serde(serialize_with = "serialize_optional_date")]
    pub last_trade_date: Option<NaiveDate>,
    #[serde(serialize_with = "serialize_optional_date")]
    pub unbroken_date: Option<NaiveDate>,

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

        let mut unbroken_date: Option<NaiveDate> = None;
        for (date, value) in trade_dates_value.iter().rev() {
            if *value > options.init_cash {
                unbroken_date = Some(*date);
            } else {
                break;
            }
        }

        let final_value = trade_dates_value
            .last()
            .map(|(_, v)| *v)
            .unwrap_or(options.init_cash);
        let total_return = final_value - options.init_cash;
        let annualized_return_rate = calc_annualized_return_rate_by_start_end(
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
            unbroken_date,
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

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BacktestOptions {
    pub init_cash: f64,

    #[serde(serialize_with = "serialize_date")]
    pub start_date: NaiveDate,
    #[serde(serialize_with = "serialize_date")]
    pub end_date: NaiveDate,
    #[serde(default)]
    pub pessimistic: bool,
    #[serde(default)]
    pub buffer_ratio: f64,

    pub risk_free_rate: f64,
    pub stamp_duty_rate: f64,
    pub stamp_duty_min_fee: f64,
    pub broker_commission_rate: f64,
    pub broker_commission_min_fee: f64,
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

#[derive(Clone, Debug)]
pub struct BacktestResult {
    pub title: Option<String>,
    pub options: BacktestOptions,
    pub final_cash: f64,
    pub final_positions_value: HashMap<Ticker, f64>,
    pub metrics: BacktestMetrics,
    pub order_dates: Vec<NaiveDate>,
    pub trade_dates_value: Vec<(NaiveDate, f64)>,
}

impl Display for BacktestResult {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}

pub struct BacktestStream {
    receiver: Receiver<BacktestEvent>,
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
                    let price_bias = if self.options.pessimistic { 1 } else { 0 };
                    for (ticker, units) in &self.portfolio.positions.clone() {
                        if let Some((weight, _)) = position_tickers_map.get(ticker) {
                            let deploy_cash = total_deploy_cash * weight / position_weight_sum;

                            let fee = calc_buy_fee(deploy_cash, self.options);
                            let delta_value = deploy_cash - fee;
                            if delta_value > 0.0 {
                                if let Some(price) =
                                    get_ticker_price(ticker, date, true, price_bias).await?
                                {
                                    let ticker_value = *units as f64 * price + delta_value;

                                    self.scale_position(
                                        ticker,
                                        ticker_value,
                                        price_bias,
                                        date,
                                        event_sender,
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
        event_sender: &Sender<BacktestEvent>,
    ) -> VfResult<()> {
        if !self.portfolio.positions.is_empty() {
            let position_tickers_map = self.position_tickers_map(date).await?;
            let position_weight_sum = position_tickers_map
                .iter()
                .map(|(_, (weight, _))| *weight)
                .sum::<f64>();
            if position_weight_sum > 0.0 {
                let price_bias = if self.options.pessimistic { -1 } else { 0 };
                for (ticker, units) in &self.portfolio.positions.clone() {
                    if let Some((weight, _)) = position_tickers_map.get(ticker) {
                        let raise_cash = cash * weight / position_weight_sum;
                        let fee = calc_sell_fee(raise_cash, self.options);
                        let delta_value = raise_cash + fee;

                        if let Some(price) =
                            get_ticker_price(ticker, date, true, price_bias).await?
                        {
                            let sell_units = (delta_value / price).ceil().min(*units as f64);
                            let ticker_value = (*units as f64 - sell_units) * price;
                            if ticker_value > 0.0 {
                                self.scale_position(
                                    ticker,
                                    ticker_value,
                                    price_bias,
                                    date,
                                    event_sender,
                                )
                                .await?;
                            } else {
                                self.position_close(ticker, false, date, event_sender)
                                    .await?;
                            }
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

                let mut nodata_count = 0;
                for (ticker, weight) in &targets_weight {
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
                        if let Some(price) = get_ticker_price(ticker, date, true, 0).await? {
                            let mut price_bias = 0;
                            if self.options.pessimistic {
                                if let Some(current_ticker_value) = self
                                    .portfolio
                                    .positions
                                    .get(ticker)
                                    .map(|units| *units as f64 * price)
                                {
                                    if ticker_value > current_ticker_value {
                                        price_bias = 1;
                                    } else if ticker_value < current_ticker_value {
                                        price_bias = -1;
                                    }
                                }
                            }

                            self.scale_position(
                                ticker,
                                ticker_value,
                                price_bias,
                                date,
                                event_sender,
                            )
                            .await?;
                        } else {
                            nodata_count += 1;
                        }
                    }
                }

                if nodata_count == targets_weight.len() {
                    return Err(VfError::NoData {
                        code: "NO_ANY_TICKET_DATA",
                        message: "All tickers have no data".to_string(),
                    });
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

    pub async fn position_open(
        &mut self,
        ticker: &Ticker,
        cash: f64,
        date: &NaiveDate,
        event_sender: &Sender<BacktestEvent>,
    ) -> VfResult<()> {
        let price_bias = if self.options.pessimistic { 1 } else { 0 };
        if let Some(price) = get_ticker_price(ticker, date, true, price_bias).await? {
            let delta_value = cash - calc_buy_fee(cash, self.options);

            let buy_units = (delta_value / price).floor();
            if buy_units > 0.0 {
                let value = buy_units * price;
                let fee = calc_buy_fee(value, self.options);
                let amount = value + fee;

                self.portfolio.free_cash -= amount;
                self.portfolio
                    .positions
                    .entry(ticker.clone())
                    .and_modify(|v| *v += buy_units as u64)
                    .or_insert(buy_units as u64);

                self.order_dates.insert(*date);
                let _ = event_sender
                    .send(BacktestEvent::Buy {
                        title: get_ticker_title(ticker).await,
                        amount,
                        price,
                        units: buy_units as u64,
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

        Ok(())
    }

    pub async fn position_open_reserved(
        &mut self,
        ticker: &Ticker,
        date: &NaiveDate,
        event_sender: &Sender<BacktestEvent>,
    ) -> VfResult<()> {
        if let Some(reserved_cash) = self.portfolio.reserved_cash.get(ticker) {
            let price_bias = if self.options.pessimistic { 1 } else { 0 };
            if let Some(price) = get_ticker_price(ticker, date, true, price_bias).await? {
                let delta_value = reserved_cash - calc_buy_fee(*reserved_cash, self.options);

                let buy_units = (delta_value / price).floor();
                if buy_units > 0.0 {
                    let value = buy_units * price;
                    let fee = calc_buy_fee(value, self.options);
                    let amount = value + fee;

                    self.portfolio.free_cash += *reserved_cash - amount;
                    self.portfolio.reserved_cash.remove(ticker);

                    self.portfolio
                        .positions
                        .entry(ticker.clone())
                        .and_modify(|v| *v += buy_units as u64)
                        .or_insert(buy_units as u64);

                    self.order_dates.insert(*date);
                    let _ = event_sender
                        .send(BacktestEvent::Buy {
                            title: get_ticker_title(ticker).await,
                            amount,
                            price,
                            units: buy_units as u64,
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
        }

        Ok(())
    }

    pub async fn position_close(
        &mut self,
        ticker: &Ticker,
        make_reserved: bool,
        date: &NaiveDate,
        event_sender: &Sender<BacktestEvent>,
    ) -> VfResult<f64> {
        let position_units = *self.portfolio.positions.get(ticker).unwrap_or(&0);
        let cash = if position_units > 0 {
            let price_bias = if self.options.pessimistic { -1 } else { 0 };
            if let Some(price) = get_ticker_price(ticker, date, true, price_bias).await? {
                let sell_units = position_units as f64;
                let value = sell_units * price;
                let fee = calc_sell_fee(value, self.options);
                let amount = value - fee;

                if make_reserved {
                    self.portfolio
                        .reserved_cash
                        .entry(ticker.clone())
                        .and_modify(|v| *v += amount)
                        .or_insert(amount);
                } else {
                    self.portfolio.free_cash += amount;
                }
                self.portfolio.positions.remove(ticker);

                self.order_dates.insert(*date);
                let _ = event_sender
                    .send(BacktestEvent::Sell {
                        title: get_ticker_title(ticker).await,
                        amount,
                        price,
                        units: sell_units as u64,
                        date: *date,
                    })
                    .await;

                amount
            } else {
                let _ = event_sender
                    .send(BacktestEvent::Warning {
                        title: "".to_string(),
                        message: format!("Price of '{ticker}' not exists"),
                        date: Some(*date),
                    })
                    .await;

                0.0
            }
        } else {
            0.0
        };

        Ok(cash)
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
        self.portfolio.free_cash + self.portfolio.reserved_cash.values().sum::<f64>()
    }

    async fn calc_positions_value(&self, date: &NaiveDate) -> VfResult<HashMap<Ticker, f64>> {
        let mut positions_value: HashMap<Ticker, f64> = HashMap::new();

        for (ticker, units) in &self.portfolio.positions {
            if let Some(price) = get_ticker_price(ticker, date, true, 0).await? {
                positions_value.insert(ticker.clone(), *units as f64 * price);
            } else {
                return Err(VfError::NoData {
                    code: "PRICE_NOT_EXISTS",
                    message: format!("Price of '{ticker}' not exists"),
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

    async fn scale_position(
        &mut self,
        ticker: &Ticker,
        ticker_value: f64,
        price_bias: i32,
        date: &NaiveDate,
        event_sender: &Sender<BacktestEvent>,
    ) -> VfResult<()> {
        if let Some(price) = get_ticker_price(ticker, date, true, price_bias).await? {
            let position_units = *self.portfolio.positions.get(ticker).unwrap_or(&0);
            let position_value = position_units as f64 * price;
            let delta_value = ticker_value - position_value;
            if delta_value.abs() < position_value * POSITION_TOLERANCE {
                return Ok(());
            }

            let ticker_title = get_ticker_title(ticker).await;
            if delta_value > 0.0 {
                let buy_units = (delta_value / price).floor();
                if buy_units > 0.0 {
                    let value = buy_units * price;
                    let fee = calc_buy_fee(value, self.options);
                    let amount = value + fee;

                    self.portfolio.free_cash -= amount;

                    self.portfolio
                        .positions
                        .entry(ticker.clone())
                        .and_modify(|v| *v += buy_units as u64)
                        .or_insert(buy_units as u64);

                    self.order_dates.insert(*date);
                    let _ = event_sender
                        .send(BacktestEvent::Buy {
                            title: ticker_title,
                            amount,
                            price,
                            units: buy_units as u64,
                            date: *date,
                        })
                        .await;
                }
            } else {
                let sell_value = delta_value.abs();

                let sell_units = (sell_value / price).floor().min(position_units as f64);
                if sell_units > 0.0 {
                    let value = sell_units * price;
                    let fee = calc_sell_fee(value, self.options);
                    let amount = value - fee;

                    self.portfolio.free_cash += amount;

                    if sell_units as u64 == position_units {
                        self.portfolio.positions.remove(ticker);
                    } else {
                        self.portfolio
                            .positions
                            .entry(ticker.clone())
                            .and_modify(|v| *v -= sell_units as u64);
                    }

                    self.order_dates.insert(*date);
                    let _ = event_sender
                        .send(BacktestEvent::Sell {
                            title: ticker_title,
                            amount,
                            price,
                            units: sell_units as u64,
                            date: *date,
                        })
                        .await;
                }
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

        Ok(())
    }
}

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

                    let mut stream = backtest_fund(&fund_definition, &fund_options).await?;

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
                    &sender,
                    &options.end_date,
                    final_cash,
                    &final_positions_value,
                    options.init_cash,
                )
                .await;

                let order_dates: Vec<NaiveDate> = {
                    let mut order_dates_set: HashSet<NaiveDate> = HashSet::new();

                    for (_, fund_result) in &funds_result {
                        for date in &fund_result.order_dates {
                            order_dates_set.insert(*date);
                        }
                    }

                    let mut order_dates: Vec<NaiveDate> = order_dates_set.into_iter().collect();
                    order_dates.sort_unstable();

                    order_dates
                };

                let trade_dates_value: Vec<(NaiveDate, f64)> = {
                    let mut trade_dates_value_map: BTreeMap<NaiveDate, f64> = BTreeMap::new();

                    for (_, fund_result) in &funds_result {
                        for (date, value) in &fund_result.trade_dates_value {
                            trade_dates_value_map
                                .entry(*date)
                                .and_modify(|v| *v += value)
                                .or_insert(*value);
                        }
                    }

                    trade_dates_value_map.into_iter().collect::<_>()
                };

                Ok(BacktestResult {
                    title: Some(fof_definition.title.clone()),
                    options: options.clone(),
                    final_cash,
                    final_positions_value,
                    metrics: BacktestMetrics::from_daily_data(&trade_dates_value, options),
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
                let all_search: Vec<(String, Vec<f64>)> = fof_definition
                    .search
                    .clone()
                    .into_iter()
                    .collect::<Vec<_>>();

                let cv_start = Instant::now();
                let cv_count = all_search.iter().map(|(_, v)| v.len()).product::<usize>()
                    * cv_options.cv_start_dates.len();

                type Search = (String, f64);
                let mut cv_search_results: Vec<(Vec<Search>, HashMap<NaiveDate, BacktestResult>)> =
                    vec![];
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

                    let mut cv_results: HashMap<NaiveDate, BacktestResult> = HashMap::new();
                    for (j, cv_start_date) in cv_options.cv_start_dates.iter().enumerate() {
                        let mut options = cv_options.base_options.clone();
                        options.start_date = *cv_start_date;

                        let mut stream = backtest_fof(&fof_definition, &options).await?;

                        while let Some(event) = stream.next().await {
                            match event {
                                BacktestEvent::Result(result) => {
                                    let _ = sender
                                        .send(BacktestEvent::Info {
                                            title: format!(
                                                "[CV {}/{cv_count} {}] [{}~{}]",
                                                i * cv_options.cv_start_dates.len() + j + 1,
                                                secs_to_human_str(cv_start.elapsed().as_secs()),
                                                date_to_str(&options.start_date),
                                                date_to_str(&options.end_date),
                                            ),
                                            message: format!(
                                                "[ARR={} Sharpe={}] {}",
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
                                                    .map(|(fund_name, weight)| {
                                                        format!("{fund_name}={weight}")
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

                    cv_search_results.push((funds_weight.clone(), cv_results));
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
                        if let Some((funds_weight, _)) = cv_search_results.get(idx) {
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
                                        "[ARR={:.2}% Sharpe={:.3}] {}",
                                        cv_score.arr * 100.0,
                                        cv_score.sharpe,
                                        funds_weight
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

                let cv_start = Instant::now();
                let cv_count = windows.len();

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
                            // Check taking profit
                            let frequency_take_profit_pct =
                                rule.definition().frequency_take_profit_pct;
                            if frequency_take_profit_pct > 0 {
                                for ticker in context
                                    .portfolio
                                    .positions
                                    .keys()
                                    .cloned()
                                    .collect::<Vec<_>>()
                                {
                                    if let (Some(price_period_start), Some(price)) = (
                                        get_ticker_price(&ticker, period_start_date, true, 0)
                                            .await?,
                                        get_ticker_price(&ticker, &date, true, 0).await?,
                                    ) {
                                        let period_profit_pct = 100.0
                                            * (price - price_period_start)
                                            / price_period_start;
                                        if period_profit_pct > frequency_take_profit_pct as f64 {
                                            context
                                                .position_close(&ticker, false, &date, &sender)
                                                .await?;
                                        }
                                    }
                                }
                            }

                            // Check frequency
                            let days = (date - *period_start_date).num_days();
                            let period_days = rule.definition().frequency.days;
                            if period_days > 0 {
                                if days < period_days as i64 {
                                    continue;
                                }
                            } else {
                                continue;
                            }
                        }

                        if rule.exec(&mut context, &date, &sender).await.is_ok() {
                            rules_period_start_date.insert(rule_index, date);
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
                metrics: BacktestMetrics::from_daily_data(&trade_dates_value, options),
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
                let mut all_search: Vec<RuleOptionValue> = vec![];
                for rule_definition in &fund_definition.rules {
                    for (k, v) in &rule_definition.search {
                        all_search.push(RuleOptionValue {
                            rule_name: rule_definition.name.to_string(),
                            option_name: k.to_string(),
                            option_value: v.clone(),
                        });
                    }
                }

                let cv_start = Instant::now();
                let cv_count = all_search
                    .iter()
                    .map(|v| {
                        v.option_value
                            .as_array()
                            .map(|array| array.len())
                            .unwrap_or(0)
                    })
                    .product::<usize>()
                    * cv_options.cv_start_dates.len();

                type Search = Vec<RuleOptionValue>;
                let mut cv_search_results: Vec<(Search, HashMap<NaiveDate, BacktestResult>)> =
                    vec![];
                for (i, rule_options) in all_search
                    .iter()
                    .filter_map(|v| {
                        v.option_value.as_array().map(|array| {
                            array
                                .iter()
                                .map(|option_value| RuleOptionValue {
                                    rule_name: v.rule_name.to_string(),
                                    option_name: v.option_name.to_string(),
                                    option_value: option_value.clone(),
                                })
                                .collect::<Vec<_>>()
                        })
                    })
                    .multi_cartesian_product()
                    .enumerate()
                {
                    let mut fund_definition = fund_definition.clone();

                    for rule_option in &rule_options {
                        if let Some(rule_definition) = fund_definition
                            .rules
                            .iter_mut()
                            .find(|r| r.name == rule_option.rule_name)
                        {
                            if rule_option.option_name == "frequency" {
                                if let Ok(frequency) = Frequency::from_str(
                                    rule_option.option_value.as_str().unwrap_or_default(),
                                ) {
                                    rule_definition.frequency = frequency;
                                }
                            } else if rule_option.option_name == "frequency_take_profit_pct" {
                                if let Some(frequency_take_profit_pct) =
                                    rule_option.option_value.as_u64().map(|v| v as u32)
                                {
                                    rule_definition.frequency_take_profit_pct =
                                        frequency_take_profit_pct
                                }
                            } else {
                                rule_definition.options.insert(
                                    rule_option.option_name.to_string(),
                                    rule_option.option_value.clone(),
                                );
                            }
                        }
                    }

                    let mut cv_results: HashMap<NaiveDate, BacktestResult> = HashMap::new();
                    for (j, cv_start_date) in cv_options.cv_start_dates.iter().enumerate() {
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
                                                i * cv_options.cv_start_dates.len() + j + 1,
                                                secs_to_human_str(cv_start.elapsed().as_secs()),
                                                date_to_str(&options.start_date),
                                                date_to_str(&options.end_date),
                                            ),
                                            message: format!(
                                                "[ARR={} Sharpe={}] {}",
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
                                                rule_options
                                                    .iter()
                                                    .map(|v| {
                                                        format!(
                                                            "{}={}",
                                                            v.option_name, v.option_value
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

                    cv_search_results.push((rule_options.clone(), cv_results));
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
                        if let Some((rule_options, _)) = cv_search_results.get(idx) {
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
                                        "[ARR={:.2}% Sharpe={:.3}] {}",
                                        cv_score.arr * 100.0,
                                        cv_score.sharpe,
                                        rule_options
                                            .iter()
                                            .map(|v| {
                                                format!("{}={}", v.option_name, v.option_value)
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

                let cv_start = Instant::now();
                let cv_count = windows.len();

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
    event_sender: &Sender<BacktestEvent>,
    date: &NaiveDate,
    cash: f64,
    positions_value: &HashMap<Ticker, f64>,
    init_cash: f64,
) -> VfResult<()> {
    let total_value = cash + positions_value.values().sum::<f64>();
    if total_value > 0.0 && init_cash > 0.0 {
        let mut portfolio_str = String::new();

        {
            let cash_pct = cash / total_value * 100.0;
            portfolio_str.push_str(&format!("${cash:.2}({cash_pct:.2}%)"));
        }

        for (ticker, value) in positions_value {
            if *value > 0.0 {
                let ticker_title = get_ticker_title(ticker).await;
                let value_pct = value / total_value * 100.0;

                portfolio_str
                    .push_str(format!(" {ticker_title}=${value:.2}({value_pct:.2}%)").as_str());
            }
        }

        let total_pct = total_value / init_cash * 100.0;

        let _ = event_sender
            .send(BacktestEvent::Info {
                title: format!("[${total_value:.2} {total_pct:.2}%]"),
                message: portfolio_str,
                date: Some(*date),
            })
            .await;
    } else {
        let _ = event_sender
            .send(BacktestEvent::Info {
                title: "[$0]".to_string(),
                message: "".to_string(),
                date: Some(*date),
            })
            .await;
    }

    Ok(())
}

struct CvScore {
    score: f64,
    arr: f64,
    sharpe: f64,
}

#[derive(Clone)]
struct RuleOptionValue {
    rule_name: String,
    option_name: String,
    option_value: serde_json::Value,
}

fn serialize_date<S>(date: &NaiveDate, ser: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    ser.serialize_str(&date_to_str(date))
}

fn serialize_optional_date<S>(date: &Option<NaiveDate>, ser: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    if let Some(date) = date {
        ser.serialize_str(&date_to_str(date))
    } else {
        ser.serialize_none()
    }
}

fn sort_cv_results_list(
    cv_results_list: &[HashMap<NaiveDate, BacktestResult>],
    cv_options: &BacktestCvOptions,
) -> Vec<(usize, CvScore)> {
    let mut flat_results: Vec<(usize, NaiveDate, BacktestResult)> = vec![];
    for (idx, results) in cv_results_list.iter().enumerate() {
        for (date, result) in results {
            flat_results.push((idx, *date, result.clone()));
        }
    }

    let arr_values: Vec<f64> = flat_results
        .iter()
        .map(|(_, _, r)| {
            r.metrics
                .annualized_return_rate
                .unwrap_or(f64::NEG_INFINITY)
        })
        .collect();
    let normalized_arr_values = normalize_zscore(&arr_values);

    let sharpe_values: Vec<f64> = flat_results
        .iter()
        .map(|(_, _, r)| r.metrics.sharpe_ratio.unwrap_or(f64::NEG_INFINITY))
        .collect();
    let normalized_sharpe_values = normalize_zscore(&sharpe_values);

    let mut scores_by_idx: HashMap<usize, Vec<(f64, f64, f64)>> = HashMap::new();
    for (i, (idx, _, _)) in flat_results.iter().enumerate() {
        let normalized_arr = normalized_arr_values[i];
        let normalized_sharpe = normalized_sharpe_values[i];
        let score = normalized_arr * cv_options.cv_score_arr_weight
            + normalized_sharpe * (1.0 - cv_options.cv_score_arr_weight);

        let arr = arr_values[i];
        let sharpe = sharpe_values[i];

        scores_by_idx
            .entry(*idx)
            .or_default()
            .push((score, arr, sharpe));
    }

    let mut cv_scores: Vec<(usize, CvScore)> = vec![];
    for (idx, _) in cv_results_list.iter().enumerate() {
        if let Some(scores) = scores_by_idx.get(&idx) {
            if !scores.is_empty() {
                let score = scores.iter().map(|(v, _, _)| *v).sum::<f64>() / scores.len() as f64;
                let arr = scores.iter().map(|(_, v, _)| *v).sum::<f64>() / scores.len() as f64;
                let sharpe = scores.iter().map(|(_, _, v)| *v).sum::<f64>() / scores.len() as f64;

                cv_scores.push((idx, CvScore { score, arr, sharpe }));
            }
        }
    }
    cv_scores.sort_by(|(_, a), (_, b)| b.score.partial_cmp(&a.score).unwrap_or(Ordering::Equal));

    cv_scores
}
