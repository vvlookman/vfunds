use colored::Colorize;
use eframe::{egui, icon_data};
use vfunds::{VERSION, api, gui::kline_viewer::KlineViewer};

#[derive(clap::Args)]
pub struct KlineCommand {
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

        match api::parse_ticker(&self.ticker_str) {
            Ok(ticker) => {
                let _ = eframe::run_native(
                    &format!("Vfunds Kline Viewer {VERSION}"),
                    options,
                    Box::new(|cc| Ok(Box::new(KlineViewer::new(cc, &ticker)))),
                );
            }
            Err(err) => {
                println!("[!] {}", err.to_string().red());
            }
        }
    }
}
