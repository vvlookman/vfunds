use clap::Subcommand;

mod backtest;
mod list;
mod result;

#[derive(Subcommand)]
pub enum Commands {
    #[command(about = "Backtest virtual funds")]
    #[clap(visible_aliases = &["test"])]
    Backtest(Box<backtest::BacktestCommand>),

    #[command(about = "List all virtual funds")]
    #[clap(visible_aliases = &["ls"])]
    List(Box<list::ListCommand>),

    #[command(about = "Show backtest results of virtual funds")]
    #[clap(visible_aliases = &["show", "view"])]
    Result(Box<result::ResultCommand>),
}
