use colored::Colorize;
use tabled::settings::{Color, object::Columns};
use vfunds::{api, api::Vfund};

#[derive(clap::Args)]
pub struct ListCommand;

impl ListCommand {
    pub async fn exec(&self) {
        match api::load_vfunds().await {
            Ok(vfunds) => {
                if vfunds.is_empty() {
                    match api::get_workspace().await {
                        Ok(workspace) => {
                            println!(
                                "[!] No vfund defined in '{}'",
                                workspace.to_string_lossy().yellow()
                            );
                        }
                        Err(err) => {
                            println!("[!] {}", err.to_string().red());
                        }
                    }
                } else {
                    let mut table_data: Vec<Vec<String>> = vec![];
                    for (vfund_name, vfund) in vfunds {
                        let (title, description) = match vfund {
                            Vfund::Fof(f) => (f.title, f.description),
                            Vfund::Fund(f) => (f.title, f.description),
                        };

                        table_data.push(vec![
                            vfund_name,
                            title,
                            description.unwrap_or("-".to_string()),
                        ]);
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
