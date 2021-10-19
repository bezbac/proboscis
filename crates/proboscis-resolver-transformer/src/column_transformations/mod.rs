mod agg_median;
mod agg_range;
mod randomize;
mod replace_string;

pub use agg_median::AggMedian;
pub use agg_range::AggRange;
pub use randomize::Randomize;
pub use replace_string::ReplaceString;

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
