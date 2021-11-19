use criterion::{black_box, criterion_group, criterion_main, Criterion};
use postgres::NoTls;
use proboscis_resolver_postgres::TargetConfig;
use std::time::Duration;
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

    let generic_postgres = images::generic::GenericImage::new("bitnami/pgpool:latest")
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
        generic_postgres,
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
) {
    let target_config = TargetConfig::from_uri(connection_url).unwrap();

    let generic_postgres = images::generic::GenericImage::new("pgcloak");

    let node = docker.run_with_args(
        generic_postgres,
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

fn simple_query(connection_url: &str) {
    let mut client = postgres::Client::connect(connection_url, NoTls).unwrap();

    let rows = client.query("SELECT 0, 'Alex'", &[]).unwrap();

    let sum: &str = rows[0].get(1);
    assert_eq!(sum, "Alex");
}

fn criterion_benchmark(c: &mut Criterion) {
    let docker = clients::Cli::default();

    let (database_connection_url, _postgres_node) = start_dockerized_postgres(&docker);

    let mut group = c.benchmark_group("Simple query");
    group.measurement_time(Duration::from_millis(10000));

    group.bench_function("postgres 13.4 (no proxy)", |b| {
        b.iter(|| simple_query(black_box(&database_connection_url)))
    });

    let (pgpool_connection_url, _pgpool_node) = start_pgpool(&docker, &database_connection_url);
    group.bench_function("postgres 13.4 (pg_pool)", |b| {
        b.iter(|| simple_query(black_box(&pgpool_connection_url)))
    });
    drop(_pgpool_node);

    let (pgbouncer_connection_url, _pgbouncer_node) =
        start_pgbouncer(&docker, &database_connection_url);
    group.bench_function(
        "postgres 13.4 (pg_bouncer - session pooling - 10 max connections)",
        |b| b.iter(|| simple_query(black_box(&pgbouncer_connection_url))),
    );
    drop(_pgbouncer_node);

    let (pgcloak_connection_url, _pgcloak_node) =
        start_pgbouncer(&docker, &database_connection_url);
    group.bench_function(
        "postgres 13.4 (pgcloak - session pooling - 10 max connections)",
        |b| b.iter(|| simple_query(black_box(&pgcloak_connection_url))),
    );
    drop(_pgcloak_node);
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
