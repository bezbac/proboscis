extern crate proboscis;

use proboscis::App;
use std::collections::HashMap;

#[tokio::main]
async fn main() {
    let mut authentication = HashMap::new();
    authentication.insert("admin".to_string(), "password".to_string());

    let config = proboscis::Config {
        target_addr: "0.0.0.0:5432".to_string(),
        authentication,
    };

    let app = App::new(config);
    app.listen("0.0.0.0:5430").await;
}
