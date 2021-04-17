use std::sync::Arc;

use arrow::{
    array::{ArrayRef, GenericStringArray},
    datatypes::{DataType, Field},
};
use proboscis::Transformer;

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

#[tokio::test]
#[cfg(feature = "e2e")]
async fn test_general_use() {
    use std::collections::HashMap;
    use tokio_postgres::{NoTls, SimpleQueryMessage};

    let proxy_process = async {
        let mut authentication = HashMap::new();
        authentication.insert("admin".to_string(), "password".to_string());

        let config = proboscis::Config {
            target_addr: "0.0.0.0:5432".to_string(),
            authentication,
        };

        let transformer = AnnonymizeTransformer {};

        let app = proboscis::App::new(config.clone()).add_transformer(Box::new(transformer));

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
            .simple_query("SELECT id, name FROM person")
            .await
            .unwrap();

        let row = match simple_query_result.first().unwrap() {
            SimpleQueryMessage::Row(v) => v,
            _ => panic!("Not a row"),
        };

        let name: &str = row.get(1).unwrap();
        assert_eq!(name, "Annonymous");
    };

    tokio::spawn(proxy_process);
    tokio::join!(client_process);
}
