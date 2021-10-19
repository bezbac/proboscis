use super::{ColumnTransformation, ColumnTransformationOutput};
use anyhow::Result;
use arrow::{
    array::{
        ArrayRef, Int16Array, Int32Array, Int64Array, Int8Array, PrimitiveArray, StringArray,
        UInt16Array, UInt32Array, UInt64Array, UInt8Array,
    },
    datatypes::{ArrowPrimitiveType, DataType},
};
use std::{fmt::Display, sync::Arc};

fn aggregated_value<T>(array: &PrimitiveArray<T>) -> Option<String>
where
    T: ArrowPrimitiveType,
    <T as ArrowPrimitiveType>::Native: Ord + Display,
{
    let min = array.iter().min().unwrap();
    let max = array.iter().max().unwrap();
    let agg = if max == min {
        max.map(|v| format!("{}", v))
    } else {
        Some(format!(
            "{} - {}",
            min.map_or("null".to_string(), |f| format!("{}", f)),
            max.map_or("null".to_string(), |f| format!("{}", f))
        ))
    };

    agg
}

pub struct AggRange {}

impl ColumnTransformation for AggRange {
    fn transform_data(&self, data: ArrayRef) -> Result<ArrayRef> {
        match data.data_type() {
            DataType::UInt8 => {
                let array = data.as_any().downcast_ref::<UInt8Array>().unwrap();
                Ok(Arc::new(
                    vec![aggregated_value(array); array.len()]
                        .into_iter()
                        .collect::<StringArray>(),
                ))
            }
            DataType::UInt16 => {
                let array = data.as_any().downcast_ref::<UInt16Array>().unwrap();
                Ok(Arc::new(
                    vec![aggregated_value(array); array.len()]
                        .into_iter()
                        .collect::<StringArray>(),
                ))
            }
            DataType::UInt32 => {
                let array = data.as_any().downcast_ref::<UInt32Array>().unwrap();
                Ok(Arc::new(
                    vec![aggregated_value(array); array.len()]
                        .into_iter()
                        .collect::<StringArray>(),
                ))
            }
            DataType::UInt64 => {
                let array = data.as_any().downcast_ref::<UInt64Array>().unwrap();
                Ok(Arc::new(
                    vec![aggregated_value(array); array.len()]
                        .into_iter()
                        .collect::<StringArray>(),
                ))
            }
            DataType::Int8 => {
                let array = data.as_any().downcast_ref::<Int8Array>().unwrap();
                Ok(Arc::new(
                    vec![aggregated_value(array); array.len()]
                        .into_iter()
                        .collect::<StringArray>(),
                ))
            }
            DataType::Int16 => {
                let array = data.as_any().downcast_ref::<Int16Array>().unwrap();
                Ok(Arc::new(
                    vec![aggregated_value(array); array.len()]
                        .into_iter()
                        .collect::<StringArray>(),
                ))
            }
            DataType::Int32 => {
                let array = data.as_any().downcast_ref::<Int32Array>().unwrap();
                Ok(Arc::new(
                    vec![aggregated_value(array); array.len()]
                        .into_iter()
                        .collect::<StringArray>(),
                ))
            }
            DataType::Int64 => {
                let array = data.as_any().downcast_ref::<Int64Array>().unwrap();
                Ok(Arc::new(
                    vec![aggregated_value(array); array.len()]
                        .into_iter()
                        .collect::<StringArray>(),
                ))
            }
            _ => todo!("{:?}", data.data_type()),
        }
    }

    fn output_format(&self, _input: &DataType) -> Result<ColumnTransformationOutput> {
        Ok(ColumnTransformationOutput {
            data_type: DataType::Utf8,
            nullable: false,
        })
    }
}
