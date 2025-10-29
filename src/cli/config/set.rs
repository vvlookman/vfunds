use colored::Colorize;
use tabled::settings::{Color, object::Columns};
use vfunds::api;

#[derive(clap::Args)]
pub struct ConfigSetCommand {
    key: String,
    value: String,
}

impl ConfigSetCommand {
    pub async fn exec(&self) {
        match api::set_config(&self.key, &self.value).await {
            Ok(_) => {
                let table_data: Vec<Vec<String>> =
                    vec![vec![self.key.to_lowercase(), self.value.clone()]];

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
