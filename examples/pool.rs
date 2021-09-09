use std::collections::HashMap;
use tokio_postgres::{NoTls, SimpleQueryMessage};

async fn migrations() {
    mod embedded {
        use refinery::embed_migrations;
        embed_migrations!("examples/sql_migrations");
    }

    let (mut client, connection) =
        tokio_postgres::connect("host=0.0.0.0 port=5432 user=admin password=password", NoTls)
            .await
            .unwrap();

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    embedded::migrations::runner()
        .run_async(&mut client)
        .await
        .expect("Migrations failed");
}

async fn proxy() {
    let mut credentials = HashMap::new();
    credentials.insert("admin".to_string(), "password".to_string());

    let config = proboscis::Config {
        credentials,
        tls_config: None,
    };

    let postgres_resolver = proboscis::postgres_resolver::PostgresResolver::new(
        proboscis::postgres_resolver::TargetConfig {
            host: "0.0.0.0".to_string(),
            port: "5432".to_string(),
            user: "admin".to_string(),
            password: "password".to_string(),
            database: "postgres".to_string(),
        },
        20,
    )
    .await
    .unwrap();

    proboscis::Proxy::new(config.clone(), Box::new(postgres_resolver))
        .listen("0.0.0.0:5430")
        .await
        .unwrap();
}

#[tokio::main]
async fn main() {
    tokio::join!(migrations());
    tokio::spawn(proxy());

    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    let (client, connection) =
        tokio_postgres::connect("host=0.0.0.0 port=5430 user=admin password=password", NoTls)
            .await
            .unwrap();

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    for _ in 0..1000 {
        // Simple query
        let simple_query_result = client
            .simple_query("SELECT id, name FROM users")
            .await
            .unwrap();

        let row = match simple_query_result.first().unwrap() {
            SimpleQueryMessage::Row(v) => v,
            _ => panic!("Not a row"),
        };

        let name: &str = row.get(1).unwrap();
        assert_eq!(name, "Max");
    }
}