use colored::Colorize;
use tabled::settings::{Color, object::Columns};
use vfunds::api;

#[derive(clap::Args)]
pub struct ListCommand;

impl ListCommand {
    pub async fn exec(&self) {
        match api::funds().await {
            Ok(funds) => {
                if funds.is_empty() {
                    match api::get_workspace() {
                        Ok(workspace) => {
                            println!(
                                "[!] No fund definition in '{}'",
                                workspace.to_string_lossy().yellow()
                            );
                        }
                        Err(err) => {
                            println!("[!] {}", err.to_string().red());
                        }
                    }
                } else {
                    let mut table_data: Vec<Vec<String>> = vec![];
                    for fund in funds {
                        let (fund_name, fund_definition) = fund;
                        table_data.push(vec![fund_name, fund_definition.title]);
                    }

                    let mut table = tabled::builder::Builder::from_iter(&table_data).build();
                    table.modify(Columns::first(), Color::FG_CYAN);
                    println!("{table}");
                }
            }
            Err(err) => {
                println!("[!] {}", err.to_string().red());
            }
        }
    }
}
