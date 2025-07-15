use chrono::{Local, NaiveDate};
use colored::Colorize;
use indicatif::{ProgressBar, ProgressStyle};
use tabled::settings::{
    Alignment, Color, Width,
    measurement::Percent,
    object::{Columns, Object, Rows},
    peaker::Priority,
};
use tokio::time::Duration;
use vfunds::{api, utils};

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
        value_parser = utils::datetime::date_from_str,
        help = "Start date of backtest, e.g. -s 2022-08-08"
    )]
    start_date: NaiveDate,

    #[arg(
        short = 'e',
        long = "end",
        value_parser = utils::datetime::date_from_str,
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
        short = 'r',
        long = "risk-free",
        default_value_t = 0.02,
        help = "The risk-free rate, the default value is 0.02"
    )]
    risk_free_rate: f64,
}

impl BacktestCommand {
    pub async fn exec(&self) {
        let options = api::BacktestOptions {
            init_cash: self.init_cash,
            start_date: self.start_date,
            end_date: self.end_date.unwrap_or(Local::now().naive_local().into()),
            risk_free_rate: self.risk_free_rate,
        };

        if options.end_date <= options.start_date {
            panic!(
                "The end date {} cannot be earlier than the start date {}",
                utils::datetime::date_to_str(&options.end_date),
                utils::datetime::date_to_str(&options.start_date)
            );
        }

        let spinner = ProgressBar::new_spinner();
        spinner
            .set_style(ProgressStyle::with_template("{msg} {spinner:.cyan} [{elapsed}]").unwrap());
        spinner.enable_steady_tick(Duration::from_millis(100));

        match api::backtest(&self.funds, &options).await {
            Ok(results) => {
                println!(
                    "[Initial cash] {} \t [Days] {}",
                    options.init_cash,
                    (options.end_date - options.start_date).num_days() + 1
                );

                let mut table_data: Vec<Vec<String>> = vec![vec![
                    "".to_string(),
                    "Profit".to_string(),
                    "ARR".to_string(),
                    "Sharpe Ratio".to_string(),
                    "Sortino Ratio".to_string(),
                ]];
                for (fund_name, fund_result) in results {
                    table_data.push(vec![
                        fund_name.to_string(),
                        format!("{:.2}", fund_result.profit),
                        fund_result
                            .annual_return_rate
                            .map(|v| format!("{:.2}%", v * 100.0))
                            .unwrap_or("-".to_string()),
                        fund_result
                            .sharpe_ratio
                            .map(|v| format!("{:.3}", v))
                            .unwrap_or("-".to_string()),
                        fund_result
                            .sortino_ratio
                            .map(|v| format!("{:.3}", v))
                            .unwrap_or("-".to_string()),
                    ]);
                }

                let mut table = tabled::builder::Builder::from_iter(&table_data).build();
                table.modify(Rows::first(), Color::FG_BRIGHT_BLACK);
                table.modify(Columns::first().not(Rows::first()), Color::FG_CYAN);
                table.modify(Columns::new(1..), Alignment::right());
                table.with(Width::wrap(Percent(100)).priority(Priority::max(true)));
                println!("{table}");
            }
            Err(err) => {
                spinner.finish_with_message(format!("{}", err.to_string().red()));
            }
        }
    }
}
