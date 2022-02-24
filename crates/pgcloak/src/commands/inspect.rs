use anyhow::Result;
use clap::ArgMatches;
use tokio_postgres::NoTls;

use crate::config::{ApplicationConfig, Credential, ListenerConfig};

pub async fn execute(matches: &ArgMatches) -> Result<()> {
    let connection_uri = matches
        .value_of("connection_uri")
        .expect("Missing value for 'config' argument");

    let (client, connection) = tokio_postgres::connect(connection_uri, NoTls).await?;

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    let query = r#"
    SELECT
        table_schema,
        table_name,
        column_name,
        data_type
    FROM information_schema.columns
    WHERE table_schema = 'public';
    "#;

    let rows = client.query(query, &[]).await?;

    let column_configs = vec![];

    for row in rows {
        let table_schema: &str = row.get(0);
        let table_name: &str = row.get(1);
        let column_name: &str = row.get(2);
        let data_type: &str = row.get(3);

        dbg!(table_name);
        dbg!(column_name);
    }

    let output_config = ApplicationConfig {
        credentials: vec![Credential {
            username: "admin".to_string(),
            password: "password".to_string(),
        }],
        columns: column_configs,
        tls: None,
        listener: ListenerConfig {
            host: "0.0.0.0".to_string(),
            port: 6432,
        },
        max_pool_size: 10,
        connection_uri: connection_uri.to_string(),
        k: 3,
    };

    Ok(())
}
