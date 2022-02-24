use ::config::ConfigError;
use proboscis_anonymization::{NumericAggregation, StringAggregation};
use serde::{Deserialize, Serialize};
use std::path::Path;

const DEFAULT_STRING_AGG: StringAggregationRef = StringAggregationRef::Join;
const DEFAULT_NUMERIC_AGG: NumericAggregationRef = NumericAggregationRef::Median;

#[derive(Debug, Deserialize, Serialize)]
pub struct ListenerConfig {
    pub host: String,
    pub port: usize,
}

impl Default for ListenerConfig {
    fn default() -> Self {
        Self {
            host: String::from("localhost"),
            port: 5432,
        }
    }
}

impl ListenerConfig {
    pub fn to_address(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct TlsConfig {
    pub pcks_path: String,
    pub password: String,
}

impl From<TlsConfig> for proboscis_core::TlsConfig {
    fn from(config: TlsConfig) -> Self {
        Self {
            pcks_path: config.pcks_path,
            password: config.password,
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum NumericAggregationRef {
    Range,
    Median,
}

impl From<NumericAggregationRef> for NumericAggregation {
    fn from(def: NumericAggregationRef) -> NumericAggregation {
        match def {
            NumericAggregationRef::Median => NumericAggregation::Median,
            NumericAggregationRef::Range => NumericAggregation::Range,
        }
    }
}

impl Default for NumericAggregationRef {
    fn default() -> Self {
        DEFAULT_NUMERIC_AGG
    }
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum StringAggregationRef {
    Join,
    Substring,
}

impl From<StringAggregationRef> for StringAggregation {
    fn from(def: StringAggregationRef) -> StringAggregation {
        match def {
            StringAggregationRef::Join => StringAggregation::Join,
            StringAggregationRef::Substring => StringAggregation::Substring,
        }
    }
}

impl Default for StringAggregationRef {
    fn default() -> Self {
        DEFAULT_STRING_AGG
    }
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
pub enum ColumnConfiguration {
    Identifier {
        name: String,
    },
    PseudoIdentifier {
        name: String,
        #[serde(default)]
        numeric_aggregation: NumericAggregationRef,
        #[serde(default)]
        string_aggregation: StringAggregationRef,
    },
}

#[derive(Debug, Deserialize, Clone, Serialize)]
pub struct Credential {
    pub username: String,
    pub password: String,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ApplicationConfig {
    pub connection_uri: String,

    pub k: usize,
    pub max_pool_size: usize,

    pub listener: ListenerConfig,
    pub tls: Option<TlsConfig>,

    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub credentials: Vec<Credential>,

    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub columns: Vec<ColumnConfiguration>,
}

pub fn load_config(path: &Path) -> Result<ApplicationConfig, ConfigError> {
    let mut s = config::Config::default();
    s.merge(config::File::from(path)).unwrap();
    s.try_into()
}
