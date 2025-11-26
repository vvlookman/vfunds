use colored::Colorize;
use indicatif::{ProgressBar, ProgressStyle};
use tabled::settings::{
    Color,
    object::{Columns, Object, Rows},
};
use tokio::time::Duration;
use vfunds::api;

#[derive(clap::Args)]
pub struct CheckCommand;

impl CheckCommand {
    pub async fn exec(&self) {
        let spinner = ProgressBar::new_spinner();
        spinner
            .set_style(ProgressStyle::with_template("{msg}[{elapsed}] {spinner:.cyan}").unwrap());
        spinner.enable_steady_tick(Duration::from_millis(100));

        match api::check().await {
            Ok(status) => {
                let mut table_data: Vec<Vec<String>> = vec![];
                let mut ok_rows: Vec<usize> = vec![];
                for (i, (title, optional_error)) in status.iter().enumerate() {
                    if let Some(err) = optional_error {
                        table_data.push(vec![title.to_string(), err.to_string()]);
                    } else {
                        table_data.push(vec![title.to_string(), "✔".to_string()]);

                        ok_rows.push(i);
                    }
                }

                spinner.finish();

                let mut table = tabled::builder::Builder::from_iter(&table_data).build();
                table.modify(Columns::first(), Color::FG_CYAN);
                for i in 0..table_data.len() {
                    if ok_rows.contains(&i) {
                        table.modify(Rows::new(i..i + 1).not(Columns::first()), Color::FG_GREEN);
                    } else {
                        table.modify(Rows::new(i..i + 1).not(Columns::first()), Color::FG_RED);
                    }
                }
                println!("{table}");
            }
            Err(err) => {
                spinner.finish_with_message(format!("{} ", err.to_string().red()));
            }
        }
    }
}
