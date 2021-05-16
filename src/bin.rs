extern crate proboscis;

use proboscis::App;
use std::collections::HashMap;

#[tokio::main]
async fn main() {
    let mut credentials = HashMap::new();
    credentials.insert("admin".to_string(), "password".to_string());

    let config = proboscis::Config {
        credentials,
        target_config: proboscis::TargetConfig {
            host: "0.0.0.0".to_string(),
            database: "postgres".to_string(),
            port: "5432".to_string(),
            user: "admin".to_string(),
            password: "password".to_string(),
        },
        pool_config: proboscis::PoolConfig { max_size: 100 },
        tls_config: Some(proboscis::TlsConfig {
            pcks_path: "tests/openssl/identity.p12".to_string(),
            password: "password".to_string(),
        }),
    };

    let app = App::new(config);
    app.listen("0.0.0.0:5430").await;
}
