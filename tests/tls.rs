use proboscis::{PoolConfig, TargetConfig};

#[macro_use]
extern crate serial_test;

mod embedded {
    use refinery::embed_migrations;
    embed_migrations!("tests/sql_migrations");
}

#[tokio::test]
#[serial]
#[cfg(feature = "e2e")]
async fn test_tls() {
    use std::collections::HashMap;

    use tokio_postgres::{NoTls, SimpleQueryMessage};

    // Run migrations
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

    let proxy_process = async {
        let mut credentials = HashMap::new();
        credentials.insert("admin".to_string(), "password".to_string());

        let config = proboscis::Config {
            target_config: TargetConfig {
                host: "0.0.0.0".to_string(),
                port: "5432".to_string(),
                user: "admin".to_string(),
                password: "password".to_string(),
                database: "postgres".to_string(),
            },
            pool_config: PoolConfig { max_size: 1 },
            credentials,
            tls_config: Some(proboscis::TlsConfig {
                pcks_path: "tests/openssl/identity.p12".to_string(),
                password: "password".to_string(),
            }),
        };

        let mut app = proboscis::App::new(config.clone());
        app.listen("0.0.0.0:5430").await.unwrap();
    };

    let client_process = async {
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        let connector = native_tls::TlsConnector::builder()
            .danger_accept_invalid_certs(true)
            .build()
            .unwrap();
        let connector = postgres_native_tls::MakeTlsConnector::new(connector);

        let (client, connection) = tokio_postgres::connect(
            "host=0.0.0.0 port=5430 user=admin password=password sslmode=require",
            connector,
        )
        .await
        .unwrap();

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("connection error: {}", e);
            }
        });

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

        // Normal query
        let rows = client
            .query("SELECT id, name FROM users", &[])
            .await
            .unwrap();

        let name: &str = rows[0].get(1);
        assert_eq!(name, "Max");
    };

    tokio::spawn(proxy_process);

    tokio::join!(client_process);
}
