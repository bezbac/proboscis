use thiserror::Error;

#[derive(Error, Debug)]
pub enum ParseError {
    #[error(transparent)]
    Io(#[from] tokio::io::Error),

    #[error(transparent)]
    Utf8(#[from] std::string::FromUtf8Error),

    #[error("unknown char tag: {char}")]
    UnknownCharTag { char: char },

    #[error("invalid describe kind: {char}")]
    InvalidDescribeKind { char: char },

    #[error("invalid close kind: {tag}")]
    InvalidCloseKind { tag: String },

    #[error("invalid bind parameter format")]
    InvalidBindParameterFormat,
}
