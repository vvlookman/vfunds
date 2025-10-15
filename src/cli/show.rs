use std::path::PathBuf;

use colored::Colorize;
use tabled::settings::{
    Alignment, Color, Width,
    measurement::Percent,
    object::{Columns, Object, Rows},
    peaker::Priority,
};
use vfunds::{api, api::BacktestOutputResult, utils::datetime::date_to_str};

#[derive(clap::Args)]
pub struct ShowCommand {
    #[arg(
        short = 'f',
        long = "fund",
        help = "Virtual fund should be shown, e.g. -f index_fund -f hedge_fund"
    )]
    funds: Vec<String>,

    #[arg(
        short = 'o',
        long = "output",
        help = "Output directory where backtest results are stored"
    )]
    output_dir: PathBuf,
}

impl ShowCommand {
    pub async fn exec(&self) {
        match api::backtest_results(&self.funds, &self.output_dir).await {
            Ok(results) => {
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
                for (fund_name, fund_result) in results {
                    let BacktestOutputResult {
                        options, metrics, ..
                    } = fund_result;
                    table_data.push(vec![
                        fund_name.to_string(),
                        metrics
                            .last_trade_date
                            .map(|d| date_to_str(&d))
                            .unwrap_or("-".to_string()),
                        format!("{}", metrics.trade_days),
                        format!("{:.2}%", metrics.profit / options.init_cash * 100.0),
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

                let mut table = tabled::builder::Builder::from_iter(&table_data).build();
                table.modify(Rows::first(), Color::FG_BRIGHT_BLACK);
                table.modify(Columns::first().not(Rows::first()), Color::FG_CYAN);
                table.modify(Columns::new(1..), Alignment::right());
                table.with(Width::wrap(Percent(100)).priority(Priority::max(true)));
                println!("\n{table}");
            }
            Err(err) => {
                println!("[!] {}", err.to_string().red());
            }
        }
    }
}
