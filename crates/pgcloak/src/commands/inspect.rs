use crate::config::{ApplicationConfig, ColumnConfiguration, Credential, ListenerConfig};
use anyhow::Result;
use clap::ArgMatches;
use std::{fs::File, io::Write};
use tokio_postgres::NoTls;

const POSSIBLE_IDENTIFIERS: &[&[&str]] = &[
    &["first", "name"],
    &["last", "name"],
    &["full", "name"],
    &["mobile"],
    &["mobile", "number"],
    &["phone", "number"],
    &["email"],
    &["email", "address"],
];

const POSSIBLE_QUASI_IDENTIFIERS: &[&[&str]] = &[
    &["city"],
    &["gender"],
    &["sex"],
    &["zip"],
    &["zip", "code"],
    &["profession"],
    &["birth", "year"],
    &["year", "of", "birth"],
    &["age"],
    &["birth", "day"],
    &["date", "of", "birth"],
];

fn does_column_name_match(column_name: &str, pattern: &[&str]) -> bool {
    for separator in &["", "_", "-"] {
        if column_name.to_lowercase() == pattern.join(separator).to_lowercase() {
            return true;
        }
    }

    false
}

fn get_column_config(table_name: &str, column_name: &str) -> Option<ColumnConfiguration> {
    for possible_identifier in POSSIBLE_IDENTIFIERS {
        if does_column_name_match(column_name, possible_identifier) {
            return Some(ColumnConfiguration::Identifier {
                name: [table_name, column_name].join("."),
            });
        }
    }

    for possible_identifier in POSSIBLE_QUASI_IDENTIFIERS {
        if does_column_name_match(column_name, possible_identifier) {
            return Some(ColumnConfiguration::PseudoIdentifier {
                name: [table_name, column_name].join("."),
                numeric_aggregation: crate::config::NumericAggregationRef::Median,
                string_aggregation: crate::config::StringAggregationRef::Substring,
            });
        }
    }

    None
}

pub async fn execute(matches: &ArgMatches) -> Result<()> {
    let connection_uri = matches
        .value_of("connection_uri")
        .expect("Missing value for 'connection_uri' argument");

    let output_path = matches
        .value_of("output")
        .expect("Missing value for 'output' argument");

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

    let mut column_configs = vec![];

    let rows = client.query(query, &[]).await?;
    for row in rows {
        let table_name: &str = row.get(1);
        let column_name: &str = row.get(2);

        if let Some(config) = get_column_config(table_name, column_name) {
            column_configs.push(config)
        }
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

    let toml = toml::to_string(&output_config).unwrap();

    let mut file = File::create(output_path)?;
    file.write_all(toml.as_bytes())?;

    Ok(())
}
