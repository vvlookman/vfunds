use async_trait::async_trait;
use chrono::NaiveDate;
use tokio::sync::mpsc::Sender;

use crate::{
    backtest::{BacktestEvent, FundBacktestContext},
    error::VfResult,
    financial::get_ticker_title,
    spec::RuleDefinition,
    ticker::Ticker,
};

pub struct Rule {
    executor: Box<dyn RuleExecutor>,
    definition: RuleDefinition,
}

#[async_trait]
pub trait RuleExecutor: Send {
    async fn exec(
        &mut self,
        context: &mut FundBacktestContext,
        date: &NaiveDate,
        event_sender: &Sender<BacktestEvent>,
    ) -> VfResult<()>;
}

impl Rule {
    pub fn definition(&self) -> &RuleDefinition {
        &self.definition
    }

    pub fn from_definition(definition: &RuleDefinition) -> Self {
        let executor: Box<dyn RuleExecutor> = match definition.name.as_str() {
            "hold" => Box::new(hold::Executor::new(definition)),
            "hold_by_conv_bond_premium" => {
                Box::new(hold_by_conv_bond_premium::Executor::new(definition))
            }
            "hold_by_dividend" => Box::new(hold_by_dividend::Executor::new(definition)),
            "hold_by_factors_boosting" => {
                Box::new(hold_by_factors_boosting::Executor::new(definition))
            }
            "hold_by_momentum" => Box::new(hold_by_momentum::Executor::new(definition)),
            "hold_by_return_px_ratio" => {
                Box::new(hold_by_return_px_ratio::Executor::new(definition))
            }
            "hold_by_risk_parity" => Box::new(hold_by_risk_parity::Executor::new(definition)),
            "hold_by_small_cap" => Box::new(hold_by_small_cap::Executor::new(definition)),
            "hold_by_stablity" => Box::new(hold_by_stablity::Executor::new(definition)),
            "hold_by_trend" => Box::new(hold_by_trend::Executor::new(definition)),
            "size_by_macd_crossover" => Box::new(size_by_macd_crossover::Executor::new(definition)),
            "size_by_valuation" => Box::new(size_by_valuation::Executor::new(definition)),
            _ => panic!("Unsupported rule: {}", definition.name),
        };

        Self {
            executor,
            definition: definition.clone(),
        }
    }

    pub async fn exec(
        &mut self,
        context: &mut FundBacktestContext<'_>,
        date: &NaiveDate,
        event_sender: &Sender<BacktestEvent>,
    ) -> VfResult<()> {
        self.executor.exec(context, date, event_sender).await
    }
}

mod hold;
mod hold_by_conv_bond_premium;
mod hold_by_dividend;
mod hold_by_factors_boosting;
mod hold_by_momentum;
mod hold_by_return_px_ratio;
mod hold_by_risk_parity;
mod hold_by_small_cap;
mod hold_by_stablity;
mod hold_by_trend;
mod size_by_macd_crossover;
mod size_by_valuation;

async fn rule_notify_calc_progress(
    rule_name: &str,
    progress_pct: f64,
    date: &NaiveDate,
    event_sender: &Sender<BacktestEvent>,
) {
    let message = if progress_pct < 100.0 {
        format!("Σ {progress_pct:.2}% ...")
    } else {
        "Σ 100%".to_string()
    };

    rule_send_toast(rule_name, &message, date, event_sender).await;
}

async fn rule_notify_indicators(
    rule_name: &str,
    indicators: &[(Ticker, String)],
    candidates: &[(Ticker, String)],
    date: &NaiveDate,
    event_sender: &Sender<BacktestEvent>,
) {
    if !indicators.is_empty() {
        let mut strs: Vec<String> = vec![];
        for (ticker, indicator) in indicators.iter() {
            let ticker_title = get_ticker_title(ticker).await;
            strs.push(format!("{ticker_title}={indicator}"));
        }

        let mut candidate_strs: Vec<String> = vec![];
        for (ticker, indicator) in candidates.iter() {
            let ticker_title = get_ticker_title(ticker).await;
            candidate_strs.push(format!("{ticker_title}={indicator}"));
        }

        let mut message = String::new();
        message.push_str(&strs.join(" "));
        if !candidate_strs.is_empty() {
            message.push_str(" [[ ");
            message.push_str(&candidate_strs.join(" "));
            message.push_str(" ]]");
        }

        rule_send_info(rule_name, &message, date, event_sender).await;
    }
}

async fn rule_send_info(
    rule_name: &str,
    message: &str,
    date: &NaiveDate,
    event_sender: &Sender<BacktestEvent>,
) {
    let _ = event_sender
        .send(BacktestEvent::Info {
            title: format!("[{rule_name}]"),
            message: message.to_string(),
            date: Some(*date),
        })
        .await;
}

async fn rule_send_toast(
    rule_name: &str,
    message: &str,
    date: &NaiveDate,
    event_sender: &Sender<BacktestEvent>,
) {
    let _ = event_sender
        .send(BacktestEvent::Toast {
            title: format!("[{rule_name}]"),
            message: message.to_string(),
            date: Some(*date),
        })
        .await;
}

async fn rule_send_warning(
    rule_name: &str,
    message: &str,
    date: &NaiveDate,
    event_sender: &Sender<BacktestEvent>,
) {
    let _ = event_sender
        .send(BacktestEvent::Warning {
            title: format!("[{rule_name}]"),
            message: message.to_string(),
            date: Some(*date),
        })
        .await;
}
