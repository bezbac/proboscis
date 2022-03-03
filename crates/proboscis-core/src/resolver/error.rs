use thiserror::Error;

#[derive(Error, Debug)]
pub enum ResolveError {
    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error(transparent)]
    Parse(#[from] proboscis_postgres_protocol::ParseError),

    #[error(transparent)]
    Arrow(#[from] arrow::error::ArrowError),

    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

impl From<&str> for ResolveError {
    fn from(error: &str) -> Self {
        ResolveError::Other(anyhow::anyhow!(String::from(error)))
    }
}
