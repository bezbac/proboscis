use proboscis_resolver_postgres::TargetConfig;
use serde::Serialize;
use std::fs;
use tempdir::TempDir;
use testcontainers::{
    clients::{self, Cli},
    core::Port,
    images::{self, generic::WaitFor},
    Container, Docker, RunArgs,
};

#[derive(Clone, Debug, Serialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
pub enum ColumnConfiguration {
    Identifier { name: String },
    PseudoIdentifier { name: String },
}

#[derive(Debug, Clone, Serialize)]
struct Credential {
    username: String,
    password: String,
}

#[derive(Debug, Serialize)]
struct ListenerConfig {
    host: String,
    port: usize,
}

#[derive(Debug, Serialize)]
struct FullPgcloakConfig {
    max_pool_size: usize,
    connection_uri: String,
    k: usize,
    
    listener: ListenerConfig,

    credentials: Vec<Credential>,
    columns: Vec<ColumnConfiguration>,
}

pub struct PgcloakConfig {
    pub columns: Vec<ColumnConfiguration>,
    pub max_pool_size: usize,
    pub k: usize,
}

pub fn start_pgcloak<'a>(
    docker: &'a Cli,
    connection_url: &str,
    config: &PgcloakConfig,
) -> (
    String,
    Container<'a, clients::Cli, images::generic::GenericImage>,
    TempDir,
) {
    let tempdir = TempDir::new("").unwrap();

    let target_config = TargetConfig::from_uri(connection_url).unwrap();
    let connection_string = format!(
        "postgres://postgres:{}@{}:{}/postgres",
        target_config.password.unwrap(),
        "benchmark-postgres",
        target_config.port,
    );

    let mut columns = vec![ColumnConfiguration::Identifier {
        name: String::from("__placeholder"),
    }];
    columns.append(&mut config.columns.clone());

    let full_config = FullPgcloakConfig {
        columns,
        k: config.k,
        max_pool_size: config.max_pool_size,
        credentials: vec![Credential {
            username: String::from("admin"),
            password: String::from("password"),
        }],
        listener: ListenerConfig {
            host: String::from("0.0.0.0"),
            port: 6432,
        },
        connection_uri: String::from(connection_string),
    };

    let config_content = toml::to_string(&full_config).unwrap();

    let config_file_path = tempdir.path().join("pgcloak.toml");
    fs::write(config_file_path, config_content).expect("Unable to write file");

    let tempdir_str = tempdir.path().as_os_str().to_str().unwrap();

    let pgcloak_image = images::generic::GenericImage::new("pgcloak")
        .with_volume(tempdir_str, "/app")
        .with_wait_for(WaitFor::message_on_stdout("Listening on"));
    // .with_args(vec!["-v".to_string(), "debug".to_string()]);

    let node = docker.run_with_args(
        pgcloak_image,
        RunArgs::default()
            .with_network("benchmark-network")
            .with_mapped_port(Port {
                local: 6432,
                internal: 6432,
            }),
    );

    let connection_string = format!(
        "postgres://{}:{}@0.0.0.0:{}/postgres",
        "admin", "password", 6432,
    );

    (connection_string, node, tempdir)
}
