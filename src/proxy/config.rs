use std::collections::HashMap;

#[derive(Clone)]
pub struct Config {
    pub target_addr: String,
    pub authentication: HashMap<String, String>,
}
