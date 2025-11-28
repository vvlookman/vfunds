use colored::Colorize;
use tabled::settings::{Color, object::Columns};
use vfunds::api;

#[derive(clap::Args)]
pub struct ConfigShowCommand;

impl ConfigShowCommand {
    pub async fn exec(&self) {
        match api::get_config().await {
            Ok(config) => {
                let table_data: Vec<Vec<String>> = vec![
                    vec!["qmt_api".to_string(), config.qmt_api.to_string()],
                    vec!["tushare_api".to_string(), config.tushare_api.to_string()],
                    vec![
                        "tushare_token".to_string(),
                        config.tushare_token.to_string(),
                    ],
                ];

                let mut table = tabled::builder::Builder::from_iter(&table_data).build();
                table.modify(Columns::first(), Color::FG_CYAN);
                println!("{table}");
            }
            Err(err) => {
                println!("[!] {}", err.to_string().red());
            }
        }
    }
}
