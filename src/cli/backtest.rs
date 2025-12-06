use std::{collections::HashMap, fs, path::PathBuf};

use chrono::{Local, NaiveDate};
use colored::Colorize;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use tabled::settings::{
    Alignment, Color, Width,
    measurement::Percent,
    object::{Columns, Object, Rows},
    peaker::Priority,
};
use tokio::time::Duration;
use vfunds::{
    api,
    api::{BacktestCvOptions, BacktestEvent, BacktestOptions, BacktestResult, BacktestStream},
    error::{VfError, VfResult},
    utils::datetime::{date_from_str, date_to_str},
};

#[derive(clap::Args)]
#[command(group = clap::ArgGroup::new("cv").required(false).args(&["cv_search", "cv_window"]))]
pub struct BacktestCommand {
    #[arg(
        short = 'i',
        long = "init",
        default_value_t = 1000000.0,
        help = "Initial cash, the default value is 1000000"
    )]
    init_cash: f64,

    #[arg(
        short = 's',
        long = "start",
        value_parser = date_from_str,
        help = "Start date of backtest, e.g. -s 2018-01-01 -s 2018-07-01"
    )]
    start_dates: Vec<NaiveDate>,

    #[arg(
        short = 'e',
        long = "end",
        value_parser = date_from_str,
        help = "End date of backtest, the default value is today, e.g. -e 2025-08-08"
    )]
    end_date: Option<NaiveDate>,

    #[arg(
        short = 'f',
        long = "fund",
        help = "Virtual fund requires backtesting, e.g. -f index_fund -f hedge_fund"
    )]
    funds: Vec<String>,

    #[arg(
        short = 'p',
        help = "Buy at the highest price and sell at the lowest price during trading"
    )]
    pessimistic: bool,

    #[arg(
        short = 'b',
        long = "buffer",
        default_value_t = 0.002,
        help = "The buffer ratio, the default value is 0.002"
    )]
    buffer_ratio: f64,

    #[arg(
        short = 'r',
        long = "risk-free",
        default_value_t = 0.02,
        help = "The risk-free rate, the default value is 0.02"
    )]
    risk_free_rate: f64,

    #[arg(
        long = "stamp-duty",
        default_value_t = 0.001,
        help = "The stamp-duty rate, the default value is 0.001"
    )]
    stamp_duty_rate: f64,

    #[arg(
        long = "stamp-duty-min",
        default_value_t = 1.0,
        help = "The stamp-duty minimal fee, the default value is 1"
    )]
    stamp_duty_min_fee: f64,

    #[arg(
        long = "broker-commission",
        default_value_t = 0.0002,
        help = "The broker-commission rate, the default value is 0.0002"
    )]
    broker_commission_rate: f64,

    #[arg(
        long = "broker-commission-min",
        default_value_t = 5.0,
        help = "The broker-commission minimal fee, the default value is 5"
    )]
    broker_commission_min_fee: f64,

    #[arg(
        short = 'o',
        long = "output",
        help = "Output backtest results to the specified directory"
    )]
    output_dir: Option<PathBuf>,

    #[arg(
        short = 'L',
        long = "output-logs",
        help = "Output backtest logs when outputting backtest results"
    )]
    output_logs: bool,

    #[arg(
        short = 'S',
        long = "cv-search",
        group = "cv",
        help = "Search options for cross-validation"
    )]
    cv_search: bool,

    #[arg(
        short = 'W',
        long = "cv-window",
        group = "cv",
        help = "Perform rolling time window partitioning for cross-validation"
    )]
    cv_window: bool,

    #[arg(
        short = 'D',
        long = "cv-min-window-days",
        default_value_t = 365,
        help = "Minimal time window in days for cross-validation, the default value is 365"
    )]
    cv_min_window_days: u64,

    #[arg(
        short = 'A',
        long = "cv-score-arr-weight",
        default_value_t = 0.6,
        help = "score = arr_weight · arr_score + (1 - arr_weight) · sharpe_score, the default value is 0.6"
    )]
    cv_score_arr_weight: f64,
}

impl BacktestCommand {
    pub async fn exec(&self) {
        let multi_progress = MultiProgress::new();

        let logger = multi_progress.add(ProgressBar::new_spinner());
        logger.set_style(ProgressStyle::with_template("{msg}").unwrap());

        let spinner = multi_progress.add(ProgressBar::new_spinner());
        spinner
            .set_style(ProgressStyle::with_template("{msg}[{elapsed}] {spinner:.cyan}").unwrap());
        spinner.enable_steady_tick(Duration::from_millis(100));

        let mut errors: HashMap<String, VfError> = HashMap::new();
        let mut table_data: Vec<Vec<String>> = vec![vec![
            "".to_string(),
            "Final T".to_string(),
            "T Days".to_string(),
            "Return".to_string(),
            "Ann Return".to_string(),
            "Max Drawdown".to_string(),
            "Ann Volatility".to_string(),
            "+Years".to_string(),
            "Wait+".to_string(),
            "Win Rate".to_string(),
            "Profit Factor".to_string(),
            "Sharpe".to_string(),
            "Calmar".to_string(),
            "Sortino".to_string(),
        ]];

        let base_options = BacktestOptions {
            init_cash: self.init_cash,
            start_date: self
                .start_dates
                .first()
                .copied()
                .unwrap_or(Local::now().date_naive()),
            end_date: self.end_date.unwrap_or(Local::now().date_naive()),
            pessimistic: self.pessimistic,
            buffer_ratio: self.buffer_ratio,
            risk_free_rate: self.risk_free_rate,
            stamp_duty_rate: self.stamp_duty_rate,
            stamp_duty_min_fee: self.stamp_duty_min_fee,
            broker_commission_rate: self.broker_commission_rate,
            broker_commission_min_fee: self.broker_commission_min_fee,
        };

        let mut process_streams =
            async |streams: VfResult<Vec<(String, BacktestStream)>>, tranche: Option<String>| {
                match streams {
                    Ok(streams) => {
                        for (vfund_name, mut stream) in streams {
                            let vfund_tranche = if let Some(ref tranche) = tranche {
                                format!("{vfund_name}_{tranche}")
                            } else {
                                vfund_name
                            };

                            let mut backtest_logs: Vec<String> = vec![];

                            while let Some(event) = stream.next().await {
                                match event {
                                    BacktestEvent::Buy { .. } | BacktestEvent::Sell { .. } => {
                                        if self.output_logs {
                                            backtest_logs.push(event.to_string());
                                        }

                                        logger.println(format!("[{vfund_tranche}] {event}"));
                                    }
                                    BacktestEvent::Info { .. } => {
                                        if self.output_logs {
                                            backtest_logs.push(event.to_string());
                                        }

                                        logger.println(format!(
                                            "[{vfund_tranche}] {}",
                                            event.to_string().bright_black()
                                        ));
                                    }
                                    BacktestEvent::Warning { .. } => {
                                        if self.output_logs {
                                            backtest_logs.push(event.to_string());
                                        }

                                        logger.println(format!(
                                            "[{vfund_tranche}] {}",
                                            event.to_string().bright_yellow()
                                        ));
                                    }
                                    BacktestEvent::Toast { .. } => {
                                        spinner.set_message(format!(
                                            "[{vfund_tranche}] {} ",
                                            event.to_string().bright_black()
                                        ));
                                    }
                                    BacktestEvent::Result(backtest_result) => {
                                        if let Some(output_dir) = &self.output_dir {
                                            if !output_dir.exists() {
                                                let _ = fs::create_dir_all(output_dir);
                                            }

                                            if let Err(err) = api::output_backtest(
                                                output_dir,
                                                &vfund_tranche,
                                                &backtest_result,
                                                &backtest_logs,
                                            )
                                            .await
                                            {
                                                errors.insert(vfund_tranche.to_string(), err);
                                            }
                                        }

                                        let BacktestResult {
                                            options, metrics, ..
                                        } = *backtest_result;
                                        table_data.push(vec![
                                            vfund_tranche.to_string(),
                                            metrics
                                                .last_trade_date
                                                .map(|d| date_to_str(&d))
                                                .unwrap_or("-".to_string()),
                                            format!("{}", metrics.trade_days),
                                            format!(
                                                "{:.2}%",
                                                metrics.total_return / options.init_cash * 100.0
                                            ),
                                            metrics
                                                .annualized_return_rate
                                                .map(|v| format!("{:.2}%", v * 100.0))
                                                .unwrap_or("-".to_string()),
                                            metrics
                                                .max_drawdown
                                                .map(|v| format!("{:.2}%", v * 100.0))
                                                .unwrap_or("-".to_string()),
                                            metrics
                                                .annualized_volatility
                                                .map(|v| format!("{:.2}%", v * 100.0))
                                                .unwrap_or("-".to_string()),
                                            format!(
                                                "{}/{}",
                                                metrics
                                                    .calendar_year_returns
                                                    .iter()
                                                    .filter(|&(_, v)| *v > 0.0)
                                                    .count(),
                                                metrics.calendar_year_returns.len()
                                            ),
                                            metrics
                                                .unbroken_date
                                                .map(|v| {
                                                    format!(
                                                        "{:.2}%",
                                                        (v - options.start_date).num_days() as f64
                                                            / ((options.end_date
                                                                - options.start_date)
                                                                .num_days()
                                                                + 1)
                                                                as f64
                                                            * 100.0
                                                    )
                                                })
                                                .unwrap_or("-".to_string()),
                                            metrics
                                                .win_rate
                                                .map(|v| format!("{:.2}%", v * 100.0))
                                                .unwrap_or("-".to_string()),
                                            metrics
                                                .profit_factor
                                                .map(|v| format!("{v:.3}"))
                                                .unwrap_or("-".to_string()),
                                            metrics
                                                .sharpe_ratio
                                                .map(|v| format!("{v:.3}"))
                                                .unwrap_or("-".to_string()),
                                            metrics
                                                .calmar_ratio
                                                .map(|v| format!("{v:.3}"))
                                                .unwrap_or("-".to_string()),
                                            metrics
                                                .sortino_ratio
                                                .map(|v| format!("{v:.3}"))
                                                .unwrap_or("-".to_string()),
                                        ]);
                                    }
                                    BacktestEvent::Error(err) => {
                                        if self.output_logs {
                                            backtest_logs.push(err.to_string());
                                        }

                                        errors.insert(vfund_tranche.to_string(), err);
                                    }
                                }
                            }
                        }
                    }
                    Err(err) => {
                        spinner.finish_with_message(format!("{} ", err.to_string().red()));
                    }
                }
            };

        if self.cv_search || self.cv_window {
            let cv_options = BacktestCvOptions {
                base_options,

                cv_start_dates: self.start_dates.clone(),
                cv_search: self.cv_search,
                cv_window: self.cv_window,
                cv_min_window_days: self.cv_min_window_days,
                cv_score_arr_weight: self.cv_score_arr_weight,
            };

            let streams_result = api::backtest_cv(&self.funds, &cv_options).await;
            process_streams(streams_result, None).await;
        } else {
            for start_date in &self.start_dates {
                let mut options = base_options.clone();
                options.start_date = *start_date;

                let streams_result = api::backtest(&self.funds, &options).await;
                process_streams(
                    streams_result,
                    Some(start_date.format("%Y%m%d").to_string()),
                )
                .await;
            }
        }

        for (vfund_tranche, err) in &errors {
            logger.println(format!("[{vfund_tranche}] [!] {}", err.to_string().red()));
        }

        if errors.is_empty() {
            spinner.finish_with_message(format!("{} ", "✔".to_string().green()));
        } else {
            spinner.finish_with_message(format!("{} ", "!".to_string().yellow()));
        }

        if table_data.len() > 1 {
            let mut table = tabled::builder::Builder::from_iter(&table_data).build();
            table.modify(Rows::first(), Color::FG_BRIGHT_BLACK);
            table.modify(Columns::first().not(Rows::first()), Color::FG_CYAN);
            table.modify(Columns::new(4..5).not(Rows::first()), Color::FG_CYAN);
            table.modify(Columns::new(11..12).not(Rows::first()), Color::FG_CYAN);
            table.modify(Columns::new(1..), Alignment::right());
            table.with(Width::wrap(Percent(100)).priority(Priority::max(true)));
            logger.println(format!("\n{table}"));
        }
    }
}
