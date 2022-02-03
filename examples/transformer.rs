use arrow::{array::LargeStringArray, datatypes::Schema, record_batch::RecordBatch};
use maplit::hashmap;
use proboscis_core::{Config, Proxy};
use proboscis_resolver_postgres::{PostgresResolver, TargetConfig};
use proboscis_resolver_transformer::{
    projection::ProjectedOrigin, Transformer, TransformingResolver,
};
use std::sync::Arc;
use testcontainers::clients;
use tokio::net::TcpListener;
use tokio_postgres::{NoTls, SimpleQueryMessage};
use tracing_subscriber::EnvFilter;

mod setup;

struct ExampleTransformer;

impl Transformer for ExampleTransformer {
    fn transform_schema(
        &self,
        schema: &Schema,
        _origins: &[ProjectedOrigin],
    ) -> proboscis_resolver_transformer::TransformationResult<Schema> {
        Ok(schema.clone())
    }

    fn transform_records(
        &self,
        data: &RecordBatch,
        _origins: &[ProjectedOrigin],
    ) -> proboscis_resolver_transformer::TransformationResult<RecordBatch> {
        let new_data = data
            .schema()
            .fields()
            .iter()
            .enumerate()
            .map(|(index, field)| {
                let column_data = data.column(index).clone();

                let transformed_data = match field.name().as_str() {
                    "name" => Arc::new(LargeStringArray::from(vec!["Anon"; column_data.len()])),
                    _ => column_data,
                };

                transformed_data
            })
            .collect();

        Ok(RecordBatch::try_new(data.schema(), new_data)?)
    }
}

async fn run_proxy(database_connection_url: String) -> String {
    let proxy_user = "admin";
    let proxy_password = "password";

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
            .add_transformer(Box::new(ExampleTransformer)),
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
    setup::import_sql_file(&database_connection_url, "examples/resources/sql/users.sql").await;

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
        .simple_query("SELECT id, name FROM users")
        .await
        .unwrap();

    let row = match simple_query_result.first().unwrap() {
        SimpleQueryMessage::Row(v) => v,
        _ => panic!("Not a row"),
    };

    let name: &str = row.get(1).unwrap();
    assert_eq!(name, "Anon");

    // Normal query
    let rows = client
        .query("SELECT id, name FROM users", &[])
        .await
        .unwrap();

    let name: &str = rows[0].get(1);
    assert_eq!(name, "Anon");
}
