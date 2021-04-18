#[macro_use]
extern crate serial_test;

mod embedded {
    use refinery::embed_migrations;
    embed_migrations!("tests/sql_migrations");
}

#[tokio::test]
#[serial]
#[cfg(feature = "e2e")]
async fn test_general_use() {
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

    let report = embedded::migrations::runner()
        .run_async(&mut client)
        .await
        .expect("Migrations failed");

    let proxy_process = async {
        let mut authentication = HashMap::new();
        authentication.insert("admin".to_string(), "password".to_string());

        let config = proboscis::Config {
            target_addr: "0.0.0.0:5432".to_string(),
            authentication,
        };

        let app = proboscis::App::new(config.clone());
        app.listen("0.0.0.0:5430").await.unwrap();
    };

    let client_process = async {
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
