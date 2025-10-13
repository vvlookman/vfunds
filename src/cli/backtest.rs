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
    api::{
        BacktestEvent, BacktestOptions, BacktestOutputPortfolio, BacktestOutputResult,
        BacktestResult,
    },
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
        help = "Start date of backtest, e.g. -s 2019-08-08"
    )]
    start_date: NaiveDate,

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
        short = 'B',
        long = "benchmark",
        help = "Benchmark ticker, e.g. -B 510300"
    )]
    benchmark: Option<String>,

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
        long = "cv-score-arr-cap",
        default_value_t = 0.15,
        help = "The annualized return rate cap value for cross-validation, cross-validation uses annualized return rate and sharpe ratio as score, the default value is 0.15"
    )]
    cv_score_arr_cap: f64,
}

impl BacktestCommand {
    pub async fn exec(&self) {
        let options = BacktestOptions {
            init_cash: self.init_cash,
            start_date: self.start_date,
            end_date: self.end_date.unwrap_or(Local::now().date_naive()),
            buffer_ratio: self.buffer_ratio,
            risk_free_rate: self.risk_free_rate,
            stamp_duty_rate: self.stamp_duty_rate,
            stamp_duty_min_fee: self.stamp_duty_min_fee,
            broker_commission_rate: self.broker_commission_rate,
            broker_commission_min_fee: self.broker_commission_min_fee,

            benchmark: self.benchmark.clone(),
            cv_search: self.cv_search,
            cv_window: self.cv_window,
            cv_score_arr_cap: self.cv_score_arr_cap,
        };

        println!(
            "[Initial Cash] {} \t [{} ~ {}] \t [{} Days] \n",
            options.init_cash,
            date_to_str(&options.start_date),
            date_to_str(&options.end_date),
            (options.end_date - options.start_date).num_days() + 1
        );

        let multi_progress = MultiProgress::new();

        let logger = multi_progress.add(ProgressBar::new_spinner());
        logger.set_style(ProgressStyle::with_template("{msg}").unwrap());

        let spinner = multi_progress.add(ProgressBar::new_spinner());
        spinner
            .set_style(ProgressStyle::with_template("{msg}[{elapsed}] {spinner:.cyan}").unwrap());
        spinner.enable_steady_tick(Duration::from_millis(100));

        match api::backtest(&self.funds, &options).await {
            Ok(streams) => {
                let mut errors: HashMap<String, VfError> = HashMap::new();
                let mut table_data: Vec<Vec<String>> = vec![vec![
                    "".to_string(),
                    "Final T".to_string(),
                    "T Days".to_string(),
                    "Profit".to_string(),
                    "Ann Return".to_string(),
                    "Max Drawdown".to_string(),
                    "Ann Volatility".to_string(),
                    "Win Rate".to_string(),
                    "Profit Factor".to_string(),
                    "Sharpe".to_string(),
                    "Calmar".to_string(),
                    "Sortino".to_string(),
                ]];
                for (fund_name, mut stream) in streams {
                    while let Some(event) = stream.next().await {
                        match event {
                            BacktestEvent::Buy(s) => {
                                logger.println(format!("[{fund_name}][+] {s}"));
                            }
                            BacktestEvent::Sell(s) => {
                                logger.println(format!("[{fund_name}][-] {s}"));
                            }
                            BacktestEvent::Info(s) => {
                                logger.println(format!("[{fund_name}][i] {}", s.bright_black()));
                            }
                            BacktestEvent::Toast(s) => {
                                spinner
                                    .set_message(format!("[{fund_name}][i] {} ", s.bright_black()));
                            }
                            BacktestEvent::Result(fund_result) => {
                                if let Some(output_dir) = &self.output_dir {
                                    if !output_dir.exists() {
                                        let _ = fs::create_dir_all(output_dir);
                                    }

                                    let output_daily_values =
                                        |fund_result: &BacktestResult| -> VfResult<()> {
                                            let path =
                                                output_dir.join(format!("{fund_name}.values.csv"));
                                            let mut csv_writer = csv::Writer::from_path(&path)?;
                                            csv_writer.write_record(["date", "value"])?;
                                            for (date, value) in &fund_result.trade_dates_value {
                                                csv_writer.write_record(&[
                                                    date_to_str(date),
                                                    format!("{value:.2}"),
                                                ])?;
                                            }
                                            csv_writer.flush()?;
                                            Ok(())
                                        };
                                    if let Err(err) = output_daily_values(&fund_result) {
                                        errors.insert(fund_name.to_string(), err);
                                    }

                                    let output_result =
                                        |fund_result: &BacktestResult| -> VfResult<()> {
                                            let result = BacktestOutputResult {
                                                options: fund_result.options.clone(),
                                                portfolio: BacktestOutputPortfolio {
                                                    cash: fund_result.final_cash,
                                                    positions_value: fund_result
                                                        .final_positions_value
                                                        .iter()
                                                        .map(|(k, v)| (k.to_string(), *v))
                                                        .collect(),
                                                },
                                                metrics: fund_result.metrics.clone(),
                                            };

                                            let path = output_dir.join(format!("{fund_name}.json"));
                                            let file = fs::File::create(path)?;
                                            serde_json::to_writer_pretty(file, &result)?;

                                            Ok(())
                                        };
                                    if let Err(err) = output_result(&fund_result) {
                                        errors.insert(fund_name.to_string(), err);
                                    }
                                }

                                let BacktestResult { metrics, .. } = *fund_result;
                                table_data.push(vec![
                                    fund_name.to_string(),
                                    metrics
                                        .last_trade_date
                                        .map(|d| date_to_str(&d))
                                        .unwrap_or("-".to_string()),
                                    format!("{}", metrics.trade_days),
                                    format!("{:.2}", metrics.profit),
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
                                errors.insert(fund_name.to_string(), err);
                            }
                        }
                    }
                }

                for (fund_name, err) in &errors {
                    logger.println(format!("[{fund_name}][!] {}", err.to_string().red()));
                }

                if errors.is_empty() {
                    spinner.finish_with_message(format!("{} ", "✔".to_string().green()));
                } else {
                    spinner.finish_with_message(format!("{} ", "!".to_string().yellow()));
                }

                let mut table = tabled::builder::Builder::from_iter(&table_data).build();
                table.modify(Rows::first(), Color::FG_BRIGHT_BLACK);
                table.modify(Columns::first().not(Rows::first()), Color::FG_CYAN);
                table.modify(Columns::new(1..), Alignment::right());
                table.with(Width::wrap(Percent(100)).priority(Priority::max(true)));
                logger.println(format!("\n{table}"));
            }
            Err(err) => {
                spinner.finish_with_message(format!("{} ", err.to_string().red()));
            }
        }
    }
}
