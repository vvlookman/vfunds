//! # vfunds CLI

use std::{env, path::PathBuf};

use clap::Parser;

use crate::cli::Commands;

mod cli;

#[derive(Parser)]
#[command(version = env!("CARGO_PKG_VERSION"))]
struct Cli {
    #[command(subcommand)]
    command: Commands,

    #[arg(
        global = true,
        short = 'w',
        long = "workspace",
        help = "The directory containing the fund definitions"
    )]
    workspace: Option<PathBuf>,
}

#[tokio::main]
async fn main() {
    let mut args: Vec<String> = vec![];
    env::args().for_each(|arg| {
        if let Some(at_stripped) = arg.strip_prefix('@') {
            args.push("--fund".to_string());
            args.push(at_stripped.to_string());
        } else {
            args.push(arg);
        }
    });

    let cli = Cli::parse_from(args);

    vfunds::init(cli.workspace);

    match &cli.command {
        Commands::Backtest(cmd) => {
            cmd.exec().await;
        }
        Commands::List(cmd) => {
            cmd.exec().await;
        }
    }
}
