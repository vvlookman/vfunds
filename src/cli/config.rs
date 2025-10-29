use clap::Subcommand;

mod set;
mod show;

#[derive(Subcommand)]
pub enum ConfigCommand {
    #[command(about = "Set configuration")]
    Set(Box<set::ConfigSetCommand>),

    #[command(about = "Show configurations")]
    Show(Box<show::ConfigShowCommand>),
}

impl ConfigCommand {
    pub async fn exec(&self) {
        match self {
            ConfigCommand::Set(cmd) => {
                cmd.exec().await;
            }
            ConfigCommand::Show(cmd) => {
                cmd.exec().await;
            }
        }
    }
}
