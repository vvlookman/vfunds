use async_trait::async_trait;
use chrono::NaiveDate;
use tokio::sync::mpsc::Sender;

use crate::{
    backtest::{BacktestEvent, FundBacktestContext},
    error::VfResult,
    financial::get_ticker_title,
    spec::RuleDefinition,
    ticker::Ticker,
    utils::datetime::date_to_str,
};

pub mod hold;
pub mod hold_risk_parity;
pub mod hold_top_conv_bond;
pub mod hold_top_dividend;
pub mod hold_top_factors_score;
pub mod hold_top_return_px_ratio;
pub mod hold_top_trend;
pub mod size_by_macd_crossover;
pub mod size_by_valuation;

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
        event_sender: Sender<BacktestEvent>,
    ) -> VfResult<()>;
}

impl Rule {
    pub fn definition(&self) -> &RuleDefinition {
        &self.definition
    }

    pub fn from_definition(definition: &RuleDefinition) -> Self {
        let executor: Box<dyn RuleExecutor> = match definition.name.as_str() {
            "hold" => Box::new(hold::Executor::new(definition)),
            "hold_risk_parity" => Box::new(hold_risk_parity::Executor::new(definition)),
            "hold_top_conv_bond" => Box::new(hold_top_conv_bond::Executor::new(definition)),
            "hold_top_dividend" => Box::new(hold_top_dividend::Executor::new(definition)),
            "hold_top_factors_score" => Box::new(hold_top_factors_score::Executor::new(definition)),
            "hold_top_return_px_ratio" => {
                Box::new(hold_top_return_px_ratio::Executor::new(definition))
            }
            "hold_top_trend" => Box::new(hold_top_trend::Executor::new(definition)),
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
        event_sender: Sender<BacktestEvent>,
    ) -> VfResult<()> {
        self.executor.exec(context, date, event_sender).await
    }
}

async fn notify_calc_progress(
    event_sender: Sender<BacktestEvent>,
    date: &NaiveDate,
    rule_name: &str,
    progress_pct: f64,
) {
    let date_str = date_to_str(date);

    let message = if progress_pct < 100.0 {
        format!("{progress_pct:.2}% ...")
    } else {
        "100%".to_string()
    };

    let _ = event_sender
        .send(BacktestEvent::Toast(format!(
            "[{date_str}] [{rule_name}] Σ {message}"
        )))
        .await;
}

async fn notify_tickers_indicator(
    event_sender: Sender<BacktestEvent>,
    date: &NaiveDate,
    rule_name: &str,
    tickers_indicator: &[(Ticker, String)],
    candidates: &[(Ticker, String)],
) {
    if !tickers_indicator.is_empty() {
        let mut strs: Vec<String> = vec![];
        for (ticker, indicator) in tickers_indicator.iter() {
            if let Ok(ticker_title) = get_ticker_title(ticker).await {
                strs.push(format!("{ticker}({ticker_title})={indicator}"));
            } else {
                strs.push(format!("{ticker}={indicator}"));
            }
        }

        let mut candidate_strs: Vec<String> = vec![];
        for (ticker, indicator) in candidates.iter() {
            if let Ok(ticker_title) = get_ticker_title(ticker).await {
                candidate_strs.push(format!("{ticker}({ticker_title})={indicator}"));
            } else {
                candidate_strs.push(format!("{ticker}={indicator}"));
            }
        }

        let mut message_str = String::new();
        message_str.push_str(&strs.join(" "));
        if !candidate_strs.is_empty() {
            message_str.push_str(" [[ ");
            message_str.push_str(&candidate_strs.join(" "));
            message_str.push_str(" ]]");
        }

        let date_str = date_to_str(date);
        let _ = event_sender
            .send(BacktestEvent::Info(format!(
                "[{date_str}] [{rule_name}] {message_str}"
            )))
            .await;
    }
}
