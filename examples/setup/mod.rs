use std::fs;
use testcontainers::{
    clients::{self, Cli},
    images::{self, generic::WaitFor},
    Container, Docker,
};
use tokio_postgres::NoTls;

pub async fn import_sql_file(database_connection_url: &str, path: &str) {
    let (client, connection) = tokio_postgres::connect(&database_connection_url, NoTls)
        .await
        .unwrap();

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    let contents = fs::read_to_string(path).expect("Something went wrong reading the file");

    client.batch_execute(&contents).await.unwrap();
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
