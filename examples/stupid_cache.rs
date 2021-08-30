// use anyhow::Result;
// use arrow::record_batch::RecordBatch;
// use async_trait::async_trait;
// use deadpool::managed::PoolConfig;
// use proboscis::{postgres_resolver::TargetConfig, ResolverChain, SimpleResolver};
// use std::collections::HashMap;
// use tokio_postgres::{NoTls, SimpleQueryMessage};

// pub struct StupidCache {
//     store: HashMap<String, RecordBatch>,
// }

// impl StupidCache {
//     pub fn new() -> StupidCache {
//         StupidCache {
//             store: HashMap::new(),
//         }
//     }
// }

// #[async_trait]
// impl SimpleResolver for StupidCache {
//     async fn lookup(&mut self, query: &String) -> Result<RecordBatch> {
//         println!("Cache Lookup: {}", query);
//         match self.store.get(query) {
//             Some(data) => {
//                 println!("Cache Hit: {}", query);
//                 Ok(data.clone())
//             }
//             None => Err(anyhow::anyhow!("Cache Miss")),
//         }
//     }

//     async fn inform(&mut self, query: &String, data: RecordBatch) {
//         println!("Cache Inform: {}", query);
//         self.store.insert(query.clone(), data);
//     }
// }

// async fn migrations() {
//     mod embedded {
//         use refinery::embed_migrations;
//         embed_migrations!("examples/sql_migrations");
//     }

//     let (mut client, connection) =
//         tokio_postgres::connect("host=0.0.0.0 port=5432 user=admin password=password", NoTls)
//             .await
//             .unwrap();

//     tokio::spawn(async move {
//         if let Err(e) = connection.await {
//             eprintln!("connection error: {}", e);
//         }
//     });

//     embedded::migrations::runner()
//         .run_async(&mut client)
//         .await
//         .expect("Migrations failed");
// }

// async fn proxy() {
//     let mut credentials = HashMap::new();
//     credentials.insert("admin".to_string(), "password".to_string());

//     let config = proboscis::Config {
//         credentials,
//         tls_config: None,
//     };

//     let cache_resolver = StupidCache::new();

//     let postres_resolver = proboscis::postgres_resolver::PostgresResolver::initialize(
//         TargetConfig {
//             host: "0.0.0.0".to_string(),
//             port: "5432".to_string(),
//             user: "admin".to_string(),
//             password: "password".to_string(),
//             database: "postgres".to_string(),
//         },
//         PoolConfig::new(1),
//     );

//     let resolver_chain = ResolverChain {
//         resolvers: vec![Box::new(cache_resolver), Box::new(postres_resolver)],
//     };

//     proboscis::App::new(config.clone(), Box::new(resolver_chain))
//         .listen("0.0.0.0:5430")
//         .await
//         .unwrap();
// }

// #[tokio::main]
// async fn main() {
//     tokio::join!(migrations());
//     tokio::spawn(proxy());

//     tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

//     let (client, connection) =
//         tokio_postgres::connect("host=0.0.0.0 port=5430 user=admin password=password", NoTls)
//             .await
//             .unwrap();

//     tokio::spawn(async move {
//         if let Err(e) = connection.await {
//             eprintln!("connection error: {}", e);
//         }
//     });

//     // Simple query
//     let simple_query_result = client
//         .simple_query("SELECT id, name FROM users")
//         .await
//         .unwrap();

//     let row = match simple_query_result.first().unwrap() {
//         SimpleQueryMessage::Row(v) => v,
//         _ => panic!("Not a row"),
//     };

//     let name: &str = row.get(1).unwrap();
//     assert_eq!(name, "Max");

//     // Second query, expecting cached result
//     let simple_query_result = client
//         .simple_query("SELECT id, name FROM users")
//         .await
//         .unwrap();

//     let row = match simple_query_result.first().unwrap() {
//         SimpleQueryMessage::Row(v) => v,
//         _ => panic!("Not a row"),
//     };

//     let name: &str = row.get(1).unwrap();
//     assert_eq!(name, "Max");
// }
