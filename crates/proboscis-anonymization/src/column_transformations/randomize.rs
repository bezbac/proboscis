use super::{ColumnTransformation, ColumnTransformationOutput, ColumnTransformationResult};
use arrow::{
    array::{ArrayRef, GenericStringArray},
    datatypes::DataType,
};
use rand::{distributions::Alphanumeric, thread_rng, Rng};
use std::sync::Arc;

fn randomize_string_array<T: arrow::array::StringOffsetSizeTrait>(
    input: ArrayRef,
) -> ColumnTransformationResult<ArrayRef> {
    Ok(Arc::new(
        input
            .as_any()
            .downcast_ref::<GenericStringArray<T>>()
            .ok_or(super::ColumnTransformationError::DowncastFailed)?
            .iter()
            .map(|v| {
                v.map(|_| {
                    thread_rng()
                        .sample_iter(&Alphanumeric)
                        .take(30)
                        .map(char::from)
                        .collect::<String>()
                })
            })
            .collect::<GenericStringArray<T>>(),
    ))
}

pub struct Randomize;

impl ColumnTransformation for Randomize {
    fn transform_data(&self, data: ArrayRef) -> ColumnTransformationResult<ArrayRef> {
        match data.data_type() {
            DataType::Utf8 => Ok(randomize_string_array::<i32>(data)?),
            DataType::LargeUtf8 => Ok(randomize_string_array::<i64>(data)?),
            _ => Err(super::ColumnTransformationError::UnsupportedType(
                data.data_type().clone(),
            )),
        }
    }

    fn output_format(
        &self,
        input: &DataType,
    ) -> ColumnTransformationResult<ColumnTransformationOutput> {
        Ok(ColumnTransformationOutput {
            data_type: input.clone(),
            nullable: false,
        })
    }
}
