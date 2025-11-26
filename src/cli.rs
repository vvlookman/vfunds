use clap::Subcommand;

mod backtest;
mod check;
mod config;
mod list;
mod result;

#[derive(Subcommand)]
pub enum Commands {
    #[command(about = "Backtest virtual funds")]
    Backtest(Box<backtest::BacktestCommand>),

    #[command(about = "Check all dependent services")]
    #[clap(visible_aliases = &["chk"])]
    Check(Box<check::CheckCommand>),

    #[command(about = "Show and edit configurations")]
    #[clap(subcommand)]
    Config(Box<config::ConfigCommand>),

    #[command(about = "List all virtual funds")]
    #[clap(visible_aliases = &["ls"])]
    List(Box<list::ListCommand>),

    #[command(about = "Show backtest results of virtual funds")]
    #[clap(visible_aliases = &["show", "view"])]
    Result(Box<result::ResultCommand>),
}
