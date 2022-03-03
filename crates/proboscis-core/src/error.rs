use thiserror::Error;

#[derive(Error, Debug)]
pub enum ProboscisError {
    #[error(transparent)]
    Io(#[from] tokio::io::Error),

    #[error(transparent)]
    TLS(#[from] native_tls::Error),

    #[error(transparent)]
    Arrow(#[from] arrow::error::ArrowError),

    #[error(transparent)]
    Parse(#[from] proboscis_postgres_protocol::ParseError),

    #[error(transparent)]
    Resolve(#[from] crate::resolver::ResolveError),

    #[error("expected message: {0}")]
    ExpectedMessage(&'static str),

    #[error("incorrect password")]
    IncorrectPassword,

    #[error("frontend requested tls, but tls is not configured")]
    FrontendRequestedTLS,

    #[error("missing password for user {0} in config")]
    MissingPasswordInConfig(String),
}
