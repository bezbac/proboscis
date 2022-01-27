use maplit::hashmap;
use postgres::SimpleQueryMessage;
use proboscis_anonymization::{
    AnonymizationCriteria, AnonymizationTransformer, NumericAggregation,
};
use proboscis_core::{Config, Proxy};
use proboscis_resolver_postgres::{PostgresResolver, TargetConfig};
use proboscis_resolver_transformer::TransformingResolver;
use std::collections::HashMap;
use testcontainers::clients;
use tokio::net::TcpListener;
use tokio_postgres::NoTls;
use tracing_subscriber::EnvFilter;

mod setup;

mod embedded {
    use refinery::embed_migrations;
    embed_migrations!("setup/sql_migrations_anonymization");
}

async fn run_proxy(database_connection_url: String) -> String {
    let proxy_user = "admin";
    let proxy_password = "password";

    let identifier_columns = vec![
        String::from("contacts.first_name"),
        String::from("contacts.last_name"),
        String::from("contacts.email"),
    ];

    let quasi_identifier_columns: HashMap<String, Option<NumericAggregation>> = vec![
        (String::from("contacts.gender"), None),
        (String::from("contacts.birth_year"), None),
        (String::from("contacts.street"), None),
        (String::from("contacts.city"), None),
        (String::from("contacts.profession"), None),
    ]
    .iter()
    .cloned()
    .collect();

    let mut proxy = Proxy::new(
        Config {
            credentials: hashmap! {
                proxy_user.to_string() => proxy_password.to_string(),
            },
            tls_config: None,
        },
        Box::new(
            TransformingResolver::new(Box::new(
                PostgresResolver::new(
                    TargetConfig::from_uri(&database_connection_url).unwrap(),
                    10,
                )
                .await
                .unwrap(),
            ))
            .add_transformer(Box::new(AnonymizationTransformer {
                identifier_columns,
                quasi_identifier_columns,
                criteria: AnonymizationCriteria::KAnonymous { k: 3 },
            })),
        ),
    );

    let listener = TcpListener::bind("0.0.0.0:0").await.unwrap();

    let proxy_connection_url = format!(
        "postgres://{}:{}@localhost:{}",
        proxy_user,
        proxy_password,
        listener.local_addr().unwrap().port()
    );

    tokio::spawn(async move {
        if let Err(e) = proxy.listen(listener).await {
            eprintln!("proxy error: {}", e);
        }
    });

    proxy_connection_url
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::default().add_directive("proboscis=trace".parse().unwrap()))
        .init();

    let docker = clients::Cli::default();

    let (database_connection_url, _node) = setup::start_dockerized_postgres(&docker).await;
    setup::apply_migrations(embedded::migrations::runner(), &database_connection_url).await;

    let proxy_connection_url = run_proxy(database_connection_url).await;

    let (client, connection) = tokio_postgres::connect(&proxy_connection_url, NoTls)
        .await
        .unwrap();

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    // Simple query
    let simple_query_result = client
        .simple_query("SELECT id, first_name, last_name, city FROM contacts")
        .await
        .unwrap();

    for message in simple_query_result {
        if let SimpleQueryMessage::Row(row) = message {
            println!(
                "{:?} {:?} {:?} {:?}",
                row.get(0),
                row.get(1),
                row.get(1),
                row.get(2)
            )
        }
    }

    // Normal query
    let result = client
        .query("SELECT id, first_name, last_name, city FROM contacts", &[])
        .await
        .unwrap();

    for row in result {
        let id: Option<i32> = row.get(0);
        let first_name: Option<&str> = row.get(1);
        let last_name: Option<&str> = row.get(2);
        let city: Option<&str> = row.get(3);
        println!("{:?} {:?} {:?} {:?}", id, first_name, last_name, city)
    }
}
