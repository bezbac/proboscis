use std::fs;

use proboscis_resolver_postgres::TargetConfig;
use tempdir::TempDir;
use testcontainers::{
    clients::{self, Cli},
    core::Port,
    images::{self, generic::WaitFor},
    Container, Docker, RunArgs,
};

/// Returns an available localhost port
pub fn free_local_port() -> Option<u16> {
    let socket = std::net::SocketAddrV4::new(std::net::Ipv4Addr::LOCALHOST, 0);
    std::net::TcpListener::bind(socket)
        .and_then(|listener| listener.local_addr())
        .map(|addr| addr.port())
        .ok()
}

pub fn start_dockerized_postgres<'a>(
    docker: &'a Cli,
) -> (
    String,
    Container<'a, clients::Cli, images::generic::GenericImage>,
) {
    let password = "password";

    let generic_postgres = images::generic::GenericImage::new("postgres:13.4-alpine")
        .with_wait_for(WaitFor::message_on_stderr(
            "database system is ready to accept connections",
        ))
        .with_env_var("POSTGRES_PASSWORD", password);

    let node = docker.run_with_args(
        generic_postgres,
        RunArgs::default()
            .with_network("benchmark-network")
            .with_name("benchmark-postgres")
            .with_mapped_port(Port {
                local: 5432,
                internal: 5432,
            }),
    );

    let connection_string = format!("postgres://postgres:{}@0.0.0.0:{}/postgres", password, 5432,);

    (connection_string, node)
}

pub fn start_pgbouncer<'a>(
    docker: &'a Cli,
    connection_url: &str,
) -> (
    String,
    Container<'a, clients::Cli, images::generic::GenericImage>,
) {
    let target_config = TargetConfig::from_uri(connection_url).unwrap();
    let connection_string = format!(
        "postgres://postgres:{}@{}:{}/postgres",
        target_config.password.unwrap(),
        "benchmark-postgres",
        target_config.port,
    );

    let generic_postgres = images::generic::GenericImage::new("edoburu/pgbouncer")
        .with_wait_for(WaitFor::message_on_stderr("process up"))
        .with_env_var("DATABASE_URL", connection_string)
        .with_env_var("MAX_CLIENT_CONN", "10")
        .with_env_var("POOL_MODE", "session");

    let node = docker.run_with_args(
        generic_postgres,
        RunArgs::default().with_network("benchmark-network"),
    );

    let connection_string = format!(
        "postgres://postgres:password@0.0.0.0:{}/postgres",
        node.get_host_port(5432).unwrap(),
    );

    (connection_string, node)
}

pub fn start_pgpool<'a>(
    docker: &'a Cli,
    connection_url: &str,
) -> (
    String,
    Container<'a, clients::Cli, images::generic::GenericImage>,
) {
    let target_config = TargetConfig::from_uri(connection_url).unwrap();

    let pgpool_image = images::generic::GenericImage::new("bitnami/pgpool:latest")
        .with_wait_for(WaitFor::message_on_stderr("pgpool-II successfully started"))
        .with_env_var(
            "PGPOOL_BACKEND_NODES",
            format!("0:{}:{}", "benchmark-postgres", target_config.port),
        )
        .with_env_var("PGPOOL_SR_CHECK_USER", target_config.user.as_ref().unwrap())
        .with_env_var(
            "PGPOOL_SR_CHECK_PASSWORD",
            target_config.password.as_ref().unwrap(),
        )
        .with_env_var("PGPOOL_ENABLE_LDAP", "no")
        .with_env_var(
            "PGPOOL_POSTGRES_USERNAME",
            target_config.user.as_ref().unwrap(),
        )
        .with_env_var(
            "PGPOOL_POSTGRES_PASSWORD",
            target_config.password.as_ref().unwrap(),
        )
        .with_env_var(
            "PGPOOL_ADMIN_USERNAME",
            target_config.user.as_ref().unwrap(),
        )
        .with_env_var(
            "PGPOOL_ADMIN_PASSWORD",
            target_config.password.as_ref().unwrap(),
        );

    let node = docker.run_with_args(
        pgpool_image,
        RunArgs::default().with_network("benchmark-network"),
    );

    let connection_string = format!(
        "postgres://{}:{}@0.0.0.0:{}/postgres",
        target_config.user.as_ref().unwrap(),
        target_config.password.as_ref().unwrap(),
        node.get_host_port(5432).unwrap(),
    );

    (connection_string, node)
}

pub fn start_pgcloak<'a>(
    docker: &'a Cli,
    connection_url: &str,
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

    let config_content = r#"
max_pool_size = 100
k = 3
connection_uri = "CONNECTION_URI"

[listener]
host = "0.0.0.0"
port = "6432"

[[credentials]]
username = "admin"
password = "password"

[[columns]]
type = "identifier"
name = "contacts.first_name"
"#
    .replace("CONNECTION_URI", &connection_string);

    let config_file_path = tempdir.path().join("pgcloak.toml");
    fs::write(config_file_path, config_content).expect("Unable to write file");

    let tempdir_str = tempdir.path().as_os_str().to_str().unwrap();

    let pgcloak_image = images::generic::GenericImage::new("pgcloak")
        .with_volume(tempdir_str, "/app")
        .with_wait_for(WaitFor::message_on_stdout("Listening on"));

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