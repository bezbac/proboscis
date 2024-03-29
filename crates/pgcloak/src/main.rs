use crate::config::ColumnConfiguration;
use anyhow::Result;
use clap::{App, Arg};
use proboscis_anonymization::{
    AnonymizationCriteria, AnonymizationTransformer, NumericAggregation, StringAggregation,
};
use proboscis_core::Proxy;
use proboscis_resolver_postgres::{PostgresResolver, TargetConfig};
use proboscis_resolver_transformer::TransformingResolver;
use std::{collections::HashMap, path::Path, str::FromStr};
use tokio::net::TcpListener;
use tracing::{subscriber::set_global_default, Level};

mod config;

#[tokio::main]
async fn main() -> Result<()> {
    let matches = App::new("pgcloak")
        .version("0.1.0")
        .about("An anonymizing postgres proxy")
        .arg(
            Arg::with_name("config")
                .short("c")
                .default_value("./pgcloak.toml")
                .help("Path to the config file to use"),
        )
        .arg(
            Arg::with_name("verbosity")
                .short("v")
                .default_value("INFO")
                .help("Sets the level of verbosity"),
        )
        .arg(Arg::with_name("database").help("Connection uri for the database"))
        .get_matches();

    let tracing_level = Level::from_str(
        matches
            .value_of("verbosity")
            .expect("Missing value for 'verbosity' argument"),
    )?;

    let collector = tracing_subscriber::fmt()
        .with_max_level(tracing_level)
        .finish();

    set_global_default(collector)?;

    let config_file_path = Path::new(
        matches
            .value_of("config")
            .expect("Missing value for 'config' argument"),
    );

    let config_file_path = std::env::current_dir()?.join(config_file_path);
    let config = crate::config::load_config(&config_file_path)?;

    let mut identifier_columns = vec![];
    let mut quasi_identifier_columns: HashMap<String, (NumericAggregation, StringAggregation)> =
        HashMap::new();

    for column in config.columns {
        match column {
            ColumnConfiguration::Identifier { name } => identifier_columns.push(name),
            ColumnConfiguration::PseudoIdentifier {
                name,
                string_aggregation,
                numeric_aggregation,
            } => {
                quasi_identifier_columns.insert(
                    name,
                    (numeric_aggregation.into(), string_aggregation.into()),
                );
            }
        }
    }

    let credentials = config
        .credentials
        .iter()
        .cloned()
        .map(|credential| (credential.username, credential.password))
        .collect();

    let tls_config: Option<proboscis_core::TlsConfig> = config.tls.map(|config| config.into());

    let mut proxy = Proxy::new(
        proboscis_core::Config {
            credentials,
            tls_config,
        },
        Box::new(
            TransformingResolver::new(Box::new(
                PostgresResolver::create(
                    TargetConfig::from_uri(&config.connection_uri).unwrap(),
                    config.max_pool_size,
                )
                .await
                .unwrap(),
            ))
            .add_transformer(Box::new(AnonymizationTransformer {
                identifier_columns,
                quasi_identifier_columns,
                criteria: AnonymizationCriteria::KAnonymous { k: config.k },
            })),
        ),
    );

    let listener = TcpListener::bind(config.listener.to_address()).await?;

    proxy.listen(listener).await?;

    Ok(())
}
