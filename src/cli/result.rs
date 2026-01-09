use std::path::{Path, PathBuf};

use colored::Colorize;
use eframe::{egui, icon_data};
use tabled::{
    Table,
    settings::{
        Alignment, Color, Width,
        measurement::Percent,
        object::{Columns, Object, Rows},
        peaker::Priority,
    },
};
use tokio::sync::mpsc;
use vfunds::{
    CHANNEL_BUFFER_DEFAULT, VERSION, api,
    error::VfResult,
    gui::{GuiEvent, result_viewer::ResultViewer},
    utils::datetime::date_to_str,
};

#[derive(clap::Args)]
pub struct ResultCommand {
    #[arg(
        short = 'f',
        long = "fund",
        help = "Virtual fund should be shown, e.g. -f index_fund -f hedge_fund"
    )]
    vfund_names: Vec<String>,

    #[arg(
        short = 'o',
        long = "output",
        help = "Output directory where backtest results are stored"
    )]
    output_dir: PathBuf,

    #[arg(
        short = 'g',
        help = "Open GUI window to display additional information such as chart"
    )]
    gui: bool,
}

impl ResultCommand {
    pub async fn exec(&self) {
        match load_backtest_results_as_table(&self.output_dir, &self.vfund_names).await {
            Ok(table) => {
                println!("\n{table}");

                if self.gui {
                    let icon = icon_data::from_png_bytes(include_bytes!("../../assets/icon.png"))
                        .unwrap_or_default();

                    let (sender, mut receiver) = mpsc::channel::<GuiEvent>(CHANNEL_BUFFER_DEFAULT);

                    let output_dir = self.output_dir.clone();
                    let vfund_names = self.vfund_names.clone();
                    tokio::spawn(async move {
                        while let Some(event) = receiver.recv().await {
                            match event {
                                GuiEvent::Refresh => {
                                    match load_backtest_results_as_table(&output_dir, &vfund_names)
                                        .await
                                    {
                                        Ok(table) => {
                                            println!("\n{table}");
                                        }
                                        Err(err) => {
                                            println!("[!] {}", err.to_string().red());
                                        }
                                    }
                                }
                            }
                        }
                    });

                    let options = eframe::NativeOptions {
                        viewport: egui::ViewportBuilder::default()
                            .with_icon(icon)
                            .with_maximized(true),
                        persistence_path: Some(self.output_dir.join(".result_viewer")),
                        ..Default::default()
                    };

                    let _ = eframe::run_native(
                        &format!("Vfunds Result Viewer {VERSION}"),
                        options,
                        Box::new(|cc| {
                            Ok(Box::new(ResultViewer::new(
                                cc,
                                sender,
                                &self.output_dir,
                                &self.vfund_names,
                            )))
                        }),
                    );
                }
            }
            Err(err) => {
                println!("[!] {}", err.to_string().red());
            }
        }
    }
}

async fn load_backtest_results_as_table(
    output_dir: &Path,
    vfund_names: &[String],
) -> VfResult<Table> {
    let results = api::load_backtest_results(output_dir, vfund_names).await?;

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

    for (fund_name, fund_result) in &results {
        let api::BacktestOutputResult {
            options, metrics, ..
        } = fund_result;
        table_data.push(vec![
            fund_name.to_string(),
            metrics
                .last_trade_date
                .map(|d| date_to_str(&d))
                .unwrap_or("-".to_string()),
            format!("{}", metrics.trade_days),
            format!("{:.2}%", metrics.total_return / options.init_cash * 100.0),
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
                            / ((options.end_date - options.start_date).num_days() + 1) as f64
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

    let mut table = tabled::builder::Builder::from_iter(&table_data).build();
    table.modify(Rows::first(), Color::FG_BRIGHT_BLACK);
    table.modify(Columns::first().not(Rows::first()), Color::FG_CYAN);
    table.modify(Columns::new(4..5).not(Rows::first()), Color::FG_CYAN);
    table.modify(Columns::new(11..12).not(Rows::first()), Color::FG_CYAN);
    table.modify(Columns::new(1..), Alignment::right());
    table.with(Width::wrap(Percent(100)).priority(Priority::max(true)));

    Ok(table)
}
