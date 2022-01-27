mod agg_median;
mod agg_range;
mod agg_string_common_prefix;
mod agg_string_join_unique;
mod randomize;

pub use agg_median::AggMedian;
pub use agg_range::AggRange;
pub use agg_string_common_prefix::AggStringCommonPrefix;
pub use agg_string_join_unique::AggStringJoinUnique;
pub use randomize::Randomize;

use anyhow::Result;
use arrow::{array::ArrayRef, datatypes::DataType};

pub struct ColumnTransformationOutput {
    pub data_type: DataType,
    pub nullable: bool,
}

pub trait ColumnTransformation: Send + Sync {
    fn transform_data(&self, data: ArrayRef) -> Result<ArrayRef>;
    fn output_format(&self, input: &DataType) -> Result<ColumnTransformationOutput>;
}
