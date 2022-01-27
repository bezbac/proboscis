use anyhow::Result;
use clap::{App, Arg};
use config::ConfigError;
use proboscis_anonymization::{
    AnonymizationCriteria, AnonymizationTransformer, NumericAggregation,
};
use proboscis_core::Proxy;
use proboscis_resolver_postgres::{PostgresResolver, TargetConfig};
use proboscis_resolver_transformer::TransformingResolver;
use serde::Deserialize;
use std::{collections::HashMap, path::Path, str::FromStr};
use tokio::net::TcpListener;
use tracing::{subscriber::set_global_default, Level};

#[derive(Debug, Deserialize)]
struct ListenerConfig {
    host: String,
    port: usize,
}

impl Default for ListenerConfig {
    fn default() -> Self {
        Self {
            host: String::from("localhost"),
            port: 5432,
        }
    }
}

impl ListenerConfig {
    fn to_address(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct TlsConfig {
    pub pcks_path: String,
    pub password: String,
}

impl From<TlsConfig> for proboscis_core::TlsConfig {
    fn from(config: TlsConfig) -> Self {
        Self {
            pcks_path: config.pcks_path,
            password: config.password,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum ColumnConfiguration {
    Identifier { name: String },
    PseudoIdentifier { name: String },
}

#[derive(Debug, Deserialize, Clone)]
struct Credential {
    username: String,
    password: String,
}

#[derive(Debug, Deserialize)]
struct ApplicationConfig {
    credentials: Vec<Credential>,
    columns: Vec<ColumnConfiguration>,
    tls: Option<TlsConfig>,
    listener: ListenerConfig,
    max_pool_size: usize,
    connection_uri: String,
    k: usize,
}

fn load_config(path: &Path) -> Result<ApplicationConfig, ConfigError> {
    let mut s = config::Config::default();
    s.merge(config::File::from(path)).unwrap();
    s.try_into()
}

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

    let config = load_config(config_file_path)?;

    let mut identifier_columns = vec![];
    let mut quasi_identifier_columns: HashMap<String, Option<NumericAggregation>> = HashMap::new();

    for column in config.columns {
        match column {
            ColumnConfiguration::Identifier { name } => identifier_columns.push(name),
            ColumnConfiguration::PseudoIdentifier { name } => {
                quasi_identifier_columns.insert(name, None);
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
                PostgresResolver::new(
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

    let listener = TcpListener::bind(config.listener.to_address())
        .await
        .unwrap();

    proxy.listen(listener).await
}
