use std::{
    cmp::Ordering,
    collections::HashMap,
    fmt::{Display, Formatter},
};

use chrono::{Datelike, NaiveDate};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::{Receiver, Sender};

use crate::{
    error::*,
    financial::get_ticker_title,
    ticker::Ticker,
    utils::{
        datetime::date_to_str,
        financial::{
            calc_annualized_return_rate_by_start_end, calc_annualized_volatility,
            calc_max_drawdown, calc_profit_factor, calc_sharpe_ratio, calc_sortino_ratio,
            calc_win_rate,
        },
        math::normalize_zscore,
    },
};

pub mod fof;
pub mod fund;

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
    pub fn from_daily_value(
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
            trade_dates_value.len() as u64,
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
    #[serde(default)]
    pub position_tolerance: f64,

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
