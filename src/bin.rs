extern crate proboscis;

use proboscis::App;
use std::collections::HashMap;

#[tokio::main]
async fn main() {
    let mut credentials = HashMap::new();
    credentials.insert("admin".to_string(), "password".to_string());

    let config = proboscis::Config {
        target_addr: "0.0.0.0:5432".to_string(),
        credentials,
        tls_config: Some(proboscis::TlsConfig {
            pcks_path: "tests/openssl/identity.p12".to_string(),
            password: "password".to_string(),
        }),
    };

    let app = App::new(config);
    app.listen("0.0.0.0:5430").await;
}
