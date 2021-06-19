use arrow::record_batch::RecordBatch;
use proboscis::{PoolConfig, TargetConfig, Resolver, ResolverResult};
use std::collections::HashMap;
use tokio_postgres::{NoTls, SimpleQueryMessage};
use async_trait::async_trait;

pub struct StupidCache {
    store: HashMap<String, RecordBatch>,
}

impl StupidCache {
    pub fn new() -> StupidCache {
        StupidCache {
            store: HashMap::new(),
        }
    }
}

#[async_trait]
impl Resolver for StupidCache {
    async fn lookup(&self, query: &String) -> ResolverResult<RecordBatch> {
        println!("Cache Lookup: {}", query);
        match self.store.get(query) {
            Some(data) => {
                println!("Cache Hit: {}", query);
                ResolverResult::Hit(data.clone())
            }
            None => ResolverResult::Miss,
        }
    }

    async fn inform(&mut self, query: &String, data: RecordBatch) {
        println!("Cache Inform: {}", query);
        self.store.insert(query.clone(), data);
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

    let resolver = StupidCache::new();

    let mut app = proboscis::App::new(config.clone()).add_resolver(Box::new(resolver));

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
    assert_eq!(name, "Max");

    // Second query, expecting cached result
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
