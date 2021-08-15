use std::collections::HashMap;

#[derive(Clone)]
pub struct TlsConfig {
    pub pcks_path: String,
    pub password: String,
}

#[derive(Clone)]
pub struct Config {
    pub tls_config: Option<TlsConfig>,
    pub credentials: HashMap<String, String>,
}
