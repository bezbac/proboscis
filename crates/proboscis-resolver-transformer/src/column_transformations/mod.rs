mod replace_string;

use arrow::{array::ArrayRef, datatypes::DataType};
pub use replace_string::ReplaceString;

pub struct ColumnTransformationOutput {
    pub data_type: DataType,
    pub nullable: bool,
}

pub trait ColumnTransformation: Send + Sync {
    fn transform_data(&self, data: ArrayRef) -> ArrayRef;
    fn output_format(&self) -> ColumnTransformationOutput;
}
