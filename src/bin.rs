extern crate proboscis;

use proboscis::{postgres_resolver, App};
use std::collections::HashMap;

#[tokio::main]
async fn main() {
    let mut credentials = HashMap::new();
    credentials.insert("admin".to_string(), "password".to_string());

    let config = proboscis::Config {
        credentials,
        tls_config: Some(proboscis::TlsConfig {
            pcks_path: "tests/openssl/identity.p12".to_string(),
            password: "password".to_string(),
        }),
    };

    let postgres_resolver =
        postgres_resolver::PostgresResolver::new(proboscis::postgres_resolver::TargetConfig {
            host: "0.0.0.0".to_string(),
            database: "postgres".to_string(),
            port: "5432".to_string(),
            user: "admin".to_string(),
            password: "password".to_string(),
        })
        .await
        .unwrap();

    App::new(config, Box::new(postgres_resolver))
        .listen("0.0.0.0:5430")
        .await;
}
