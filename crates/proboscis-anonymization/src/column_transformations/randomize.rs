use super::{ColumnTransformation, ColumnTransformationOutput};
use anyhow::Result;
use arrow::{
    array::{ArrayRef, GenericStringArray, LargeStringArray},
    datatypes::DataType,
};
use rand::{distributions::Alphanumeric, thread_rng, Rng};
use std::sync::Arc;

pub struct Randomize {}

impl ColumnTransformation for Randomize {
    fn transform_data(&self, data: ArrayRef) -> Result<ArrayRef> {
        match data.data_type() {
            DataType::Utf8 => Ok(Arc::new(
                data.as_any()
                    .downcast_ref::<GenericStringArray<i32>>()
                    .unwrap()
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
                    .collect::<GenericStringArray<i32>>(),
            )),
            DataType::LargeUtf8 => Ok(Arc::new(
                data.as_any()
                    .downcast_ref::<LargeStringArray>()
                    .unwrap()
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
                    .collect::<LargeStringArray>(),
            )),
            _ => todo!("{:?}", data.data_type()),
        }
    }

    fn output_format(&self, input: &DataType) -> Result<ColumnTransformationOutput> {
        Ok(ColumnTransformationOutput {
            data_type: input.clone(),
            nullable: false,
        })
    }
}
