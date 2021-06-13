use arrow::{
    array::{ArrayRef, GenericStringArray},
    datatypes::{DataType, Field},
};
use postgres::{NoTls, SimpleQueryMessage};
use proboscis::{PoolConfig, TargetConfig, Transformer};
use std::{collections::HashMap, sync::Arc};

struct AnnonymizeTransformer {}

impl Transformer for AnnonymizeTransformer {
    fn transform(&self, data: ArrayRef) -> ArrayRef
    where
        Self: Sized,
    {
        let new_values: Vec<String> = data
            .as_any()
            .downcast_ref::<GenericStringArray<i64>>()
            .unwrap()
            .iter()
            .map(|_| "Annonymous".to_string())
            .collect();

        Arc::new(GenericStringArray::<i64>::from(
            new_values.iter().map(|s| s.as_ref()).collect::<Vec<&str>>(),
        ))
    }

    fn matches(&self, field: &Field) -> bool {
        field.name().as_str() == "name" && field.data_type() == &DataType::LargeUtf8
    }
}

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
        target_config: TargetConfig {
            host: "0.0.0.0".to_string(),
            port: "5432".to_string(),
            user: "admin".to_string(),
            password: "password".to_string(),
            database: "postgres".to_string(),
        },
        pool_config: PoolConfig { max_size: 1 },
        credentials,
        tls_config: None,
    };

    let transformer = AnnonymizeTransformer {};

    let mut app = proboscis::App::new(config.clone()).add_transformer(Box::new(transformer));

    app.listen("0.0.0.0:5430").await.unwrap();
}

#[tokio::main]
#[cfg(feature = "examples")]
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
    assert_eq!(name, "Annonymous");
}
