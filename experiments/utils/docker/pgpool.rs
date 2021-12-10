use proboscis_resolver_postgres::TargetConfig;
use testcontainers::{
    clients::{self, Cli},
    images::{self, generic::WaitFor},
    Container, Docker, RunArgs,
};

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
