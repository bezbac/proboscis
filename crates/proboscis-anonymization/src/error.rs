#[derive(Error, Debug)]
pub enum ColumnTransformationError {
    #[error("unsupported type: {0}")]
    UnsupportedType(&'static str),
}
