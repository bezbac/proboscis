use proboscis_core::resolver::ResolveError;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum TransformerError {
    #[error(transparent)]
    Arrow(#[from] arrow::error::ArrowError),

    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

impl From<TransformerError> for ResolveError {
    fn from(error: TransformerError) -> Self {
        match error {
            TransformerError::Arrow(err) => ResolveError::Arrow(err),
            _ => ResolveError::Other(anyhow::anyhow!(error)),
        }
    }
}
