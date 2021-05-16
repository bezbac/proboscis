use std::collections::HashMap;

#[derive(Clone)]
pub struct TlsConfig {
    pub pcks_path: String,
    pub password: String,
}

#[derive(Clone)]
pub struct TargetConfig {
    pub host: String,
    pub port: String,
    pub database: String,
    pub user: String,
    pub password: String,
}

#[derive(Clone)]
pub struct PoolConfig {
    pub max_size: usize,
}

#[derive(Clone)]
pub struct Config {
    pub target_config: TargetConfig,
    pub pool_config: PoolConfig,
    pub tls_config: Option<TlsConfig>,
    pub credentials: HashMap<String, String>,
}
