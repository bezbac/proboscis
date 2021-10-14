use postgres::NoTls;
use refinery::Runner;
use testcontainers::{
    clients::{self, Cli},
    images::{self, generic::WaitFor},
    Container, Docker,
};

pub async fn apply_migrations(migration_runner: Runner, address: &str) {
    let (mut client, connection) = tokio_postgres::connect(address, NoTls).await.unwrap();

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    migration_runner
        .run_async(&mut client)
        .await
        .expect("Migrations failed");
}

pub async fn start_dockerized_postgres<'a>(
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

    let node = docker.run(generic_postgres);

    let connection_string = format!(
        "postgres://postgres:{}@localhost:{}/postgres",
        password,
        node.get_host_port(5432).unwrap(),
    );

    (connection_string, node)
}
