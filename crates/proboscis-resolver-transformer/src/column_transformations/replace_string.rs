use arrow::{
    array::{ArrayRef, LargeStringArray},
    datatypes::DataType,
};
use std::sync::Arc;

use super::{ColumnTransformation, ColumnTransformationOutput};

pub struct ReplaceString {
    pub new_string: String,
}

impl ColumnTransformation for ReplaceString {
    fn transform_data(&self, data: ArrayRef) -> ArrayRef {
        Arc::new(LargeStringArray::from(vec![
            self.new_string.as_str();
            data.len()
        ]))
    }

    fn output_format(&self) -> ColumnTransformationOutput {
        ColumnTransformationOutput {
            data_type: DataType::LargeUtf8,
            nullable: false,
        }
    }
}
