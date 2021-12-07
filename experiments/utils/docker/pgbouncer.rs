use proboscis_resolver_postgres::TargetConfig;
use testcontainers::{
    clients::{self, Cli},
    images::{self, generic::WaitFor},
    Container, Docker, RunArgs,
};

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
