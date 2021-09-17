mod core;
mod postgres_protocol;

pub mod postgres_resolver;

pub use crate::core::proxy::Config;
pub use crate::core::proxy::Proxy;
pub use crate::core::proxy::TlsConfig;
pub use crate::core::resolver::Resolver;
