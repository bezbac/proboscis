use arrow::{
    array::{ArrayRef, LargeStringArray},
    datatypes::DataType,
    record_batch::RecordBatch,
};
use postgres::{NoTls, SimpleQueryMessage};
use proboscis::{PoolConfig, TargetConfig, Transformer};
use std::{collections::HashMap, sync::Arc};

struct AnonymizeTransformer {}

impl Transformer for AnonymizeTransformer {
    fn transform_data(&self, data: &RecordBatch) -> RecordBatch
    where
        Self: Sized,
    {
        // Select which columns to transform
        let columns: Vec<usize> = data
            .schema()
            .fields()
            .into_iter()
            .enumerate()
            .filter_map(|(idx, field)| {
                match field.data_type() == &DataType::LargeUtf8 && field.name().as_str() == "name" {
                    true => Some(idx),
                    _ => None,
                }
            })
            .collect();

        // If there's noting to transform return the untransformed records
        if columns.len() == 0 {
            return data.clone();
        }

        // Replace values within matched colums with "Annonymous"
        let arrays: Vec<ArrayRef> = (0..data.num_columns())
            .map(|idx| {
                if columns.contains(&idx) {
                    Arc::new(LargeStringArray::from(vec!["Annonymous"; data.num_rows()]))
                } else {
                    data.column(idx).clone()
                }
            })
            .collect();

        // Return updated records
        RecordBatch::try_new(data.schema(), arrays).unwrap()
    }

    fn transform_query(&self, query: String) -> String {
        query
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

    let transformer = AnonymizeTransformer {};

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
