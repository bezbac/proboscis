mod agg_median;
mod agg_range;
mod agg_string_common_prefix;
mod agg_string_join_unique;
mod randomize;

pub use agg_median::AggMedian;
pub use agg_range::AggRange;
pub use agg_string_common_prefix::AggStringCommonPrefix;
pub use agg_string_join_unique::AggStringJoinUnique;
use proboscis_resolver_transformer::TransformerError;
pub use randomize::Randomize;

use arrow::{array::ArrayRef, datatypes::DataType};
use thiserror::Error;

pub struct ColumnTransformationOutput {
    pub data_type: DataType,
    pub nullable: bool,
}

type ColumnTransformationResult<R> = Result<R, ColumnTransformationError>;

#[derive(Error, Debug)]
pub enum ColumnTransformationError {
    #[error("unsupported type: {0}")]
    UnsupportedType(DataType),

    #[error("downcast failed")]
    DowncastFailed,
}

impl From<ColumnTransformationError> for TransformerError {
    fn from(error: ColumnTransformationError) -> Self {
        TransformerError::Other(anyhow::anyhow!(error))
    }
}

pub trait ColumnTransformation: Send + Sync {
    fn transform_data(&self, data: ArrayRef) -> ColumnTransformationResult<ArrayRef>;
    fn output_format(
        &self,
        input: &DataType,
    ) -> ColumnTransformationResult<ColumnTransformationOutput>;
}
