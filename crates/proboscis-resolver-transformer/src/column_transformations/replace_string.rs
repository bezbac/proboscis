use super::{ColumnTransformation, ColumnTransformationOutput};
use anyhow::Result;
use arrow::{
    array::{ArrayRef, LargeStringArray},
    datatypes::DataType,
};
use std::sync::Arc;

pub struct ReplaceString {
    pub new_string: String,
}

impl ColumnTransformation for ReplaceString {
    fn transform_data(&self, data: ArrayRef) -> Result<ArrayRef> {
        Ok(Arc::new(LargeStringArray::from(vec![
            self.new_string
                .as_str();
            data.len()
        ])))
    }

    fn output_format(&self, _input: &DataType) -> Result<ColumnTransformationOutput> {
        Ok(ColumnTransformationOutput {
            data_type: DataType::LargeUtf8,
            nullable: false,
        })
    }
}
