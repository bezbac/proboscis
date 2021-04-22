use std::collections::HashMap;

#[derive(Clone)]
pub struct TlsConfig {
    pub pcks_path: String,
    pub password: String,
}

#[derive(Clone)]
pub struct Config {
    pub target_addr: String,
    pub credentials: HashMap<String, String>,
    pub tls_config: Option<TlsConfig>,
}
