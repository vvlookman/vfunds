use std::{collections::HashMap, path::PathBuf};

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
    api::{BacktestEvent, BacktestOptions},
    error::{VfError, VfResult},
    utils::datetime::{date_from_str, date_to_str},
};

#[derive(clap::Args)]
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
        short = 'b',
        long = "benchmark",
        help = "Benchmark ticker, e.g. -b 510300"
    )]
    benchmark: Option<String>,

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
}

impl BacktestCommand {
    pub async fn exec(&self) {
        let options = BacktestOptions {
            init_cash: self.init_cash,
            start_date: self.start_date,
            end_date: self.end_date.unwrap_or(Local::now().date_naive()),
            benchmark: self.benchmark.clone(),
            risk_free_rate: self.risk_free_rate,
            stamp_duty_rate: self.stamp_duty_rate,
            stamp_duty_min_fee: self.stamp_duty_min_fee,
            broker_commission_rate: self.broker_commission_rate,
            broker_commission_min_fee: self.broker_commission_min_fee,
        };

        if options.end_date <= options.start_date {
            panic!(
                "The end date {} cannot be earlier than the start date {}",
                date_to_str(&options.end_date),
                date_to_str(&options.start_date)
            );
        }

        println!(
            "[Initial cash] {} \t [Days] {} \n",
            options.init_cash,
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
                    date_to_str(&self.start_date),
                    "Final Date".to_string(),
                    "T Days".to_string(),
                    "Profit".to_string(),
                    "Annualized Return".to_string(),
                    "Max Drawdown".to_string(),
                    "Sharpe Ratio".to_string(),
                    "Sortino Ratio".to_string(),
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
                                table_data.push(vec![
                                    fund_name.to_string(),
                                    fund_result
                                        .trade_date_values
                                        .last()
                                        .map(|(d, _)| date_to_str(d))
                                        .unwrap_or("-".to_string()),
                                    format!("{}", fund_result.trade_date_values.len()),
                                    format!("{:.2}", fund_result.profit),
                                    fund_result
                                        .annual_return_rate
                                        .map(|v| format!("{:.2}%", v * 100.0))
                                        .unwrap_or("-".to_string()),
                                    fund_result
                                        .max_drawdown
                                        .map(|v| format!("{:.2}%", v * 100.0))
                                        .unwrap_or("-".to_string()),
                                    fund_result
                                        .sharpe_ratio
                                        .map(|v| format!("{v:.3}"))
                                        .unwrap_or("-".to_string()),
                                    fund_result
                                        .sortino_ratio
                                        .map(|v| format!("{v:.3}"))
                                        .unwrap_or("-".to_string()),
                                ]);

                                if let Some(output_dir) = &self.output_dir {
                                    let output_daily_values =
                                        |values: &Vec<(NaiveDate, f64)>| -> VfResult<()> {
                                            let path = output_dir.join(format!("{fund_name}.csv"));
                                            let mut csv_writer = csv::Writer::from_path(&path)?;
                                            csv_writer.write_record(["date", "value"])?;
                                            for (date, value) in values {
                                                csv_writer.write_record(&[
                                                    date_to_str(date),
                                                    format!("{value:.2}"),
                                                ])?;
                                            }
                                            csv_writer.flush()?;
                                            Ok(())
                                        };
                                    if let Err(err) =
                                        output_daily_values(&fund_result.trade_date_values)
                                    {
                                        errors.insert(fund_name.to_string(), err);
                                    }
                                }
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
