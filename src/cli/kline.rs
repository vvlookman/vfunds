use colored::Colorize;
use eframe::{egui, icon_data};
use vfunds::{VERSION, api, gui::kline_viewer::KlineViewer};

#[derive(clap::Args)]
pub struct KlineCommand {
    #[arg(short = 'r', help = "Refresh data, ignore cache expire time")]
    refresh: bool,

    ticker_str: String,
}

impl KlineCommand {
    pub async fn exec(&self) {
        let icon =
            icon_data::from_png_bytes(include_bytes!("../../assets/icon.png")).unwrap_or_default();

        let options = eframe::NativeOptions {
            viewport: egui::ViewportBuilder::default()
                .with_icon(icon)
                .with_maximized(true),
            ..Default::default()
        };

        match api::parse_ticker_title(&self.ticker_str).await {
            Ok((ticker, title)) => {
                let _ = eframe::run_native(
                    &format!("Vfunds Kline Viewer {VERSION}"),
                    options,
                    Box::new(|cc| {
                        Ok(Box::new(KlineViewer::new(
                            cc,
                            &ticker,
                            &title,
                            self.refresh,
                        )))
                    }),
                );
            }
            Err(err) => {
                println!("[!] {}", err.to_string().red());
            }
        }
    }
}
