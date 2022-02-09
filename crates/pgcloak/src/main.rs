use anyhow::Result;
use clap::{App, Arg};

mod commands;
mod config;

#[tokio::main]
async fn main() -> Result<()> {
    let matches = App::new("pgcloak")
        .version("0.1.0")
        .about("An anonymizing postgres proxy")
        .arg(
            Arg::new("config")
                .short('c')
                .default_value("./pgcloak.toml")
                .help("Path to the config file to use"),
        )
        .arg(
            Arg::new("verbosity")
                .short('v')
                .default_value("INFO")
                .help("Sets the level of verbosity"),
        )
        .arg(Arg::new("database").help("Connection uri for the database"))
        .subcommand(App::new("inspect").about("Autogenerate a config based on an existing schema"))
        .get_matches();

    match matches.subcommand() {
        Some(("inspect", sub_matches)) => commands::inspect::execute(sub_matches).await,
        _ => commands::default::execute(&matches).await,
    }
}
