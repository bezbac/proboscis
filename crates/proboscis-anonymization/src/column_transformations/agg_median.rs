use super::{ColumnTransformation, ColumnTransformationOutput};
use arrow::{
    array::{
        ArrayRef, Int16Array, Int32Array, Int64Array, Int8Array, PrimitiveArray, UInt16Array,
        UInt32Array, UInt64Array, UInt8Array,
    },
    datatypes::{ArrowNativeType, ArrowNumericType, DataType},
};
use std::{ops::Add, sync::Arc};

fn median<T>(array: &PrimitiveArray<T>) -> Option<T::Native>
where
    T: ArrowNumericType,
    T::Native: Add<Output = T::Native> + std::ops::Div<Output = T::Native>,
{
    arrow::compute::kernels::aggregate::sum(array)
        .map(|v| v as T::Native / ArrowNativeType::from_usize(array.len()).unwrap())
}

pub struct AggMedian {}

impl ColumnTransformation for AggMedian {
    fn transform_data(&self, data: ArrayRef) -> super::ColumnTransformationResult<ArrayRef> {
        match data.data_type() {
            DataType::UInt8 => {
                let array = data
                    .as_any()
                    .downcast_ref::<UInt8Array>()
                    .ok_or(super::ColumnTransformationError::DowncastFailed)?;

                Ok(Arc::new(
                    vec![median(array); array.len()]
                        .into_iter()
                        .collect::<UInt8Array>(),
                ))
            }
            DataType::UInt16 => {
                let array = data
                    .as_any()
                    .downcast_ref::<UInt16Array>()
                    .ok_or(super::ColumnTransformationError::DowncastFailed)?;
                Ok(Arc::new(
                    vec![median(array); array.len()]
                        .into_iter()
                        .collect::<UInt16Array>(),
                ))
            }
            DataType::UInt32 => {
                let array = data
                    .as_any()
                    .downcast_ref::<UInt32Array>()
                    .ok_or(super::ColumnTransformationError::DowncastFailed)?;
                Ok(Arc::new(
                    vec![median(array); array.len()]
                        .into_iter()
                        .collect::<UInt32Array>(),
                ))
            }
            DataType::UInt64 => {
                let array = data
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .ok_or(super::ColumnTransformationError::DowncastFailed)?;
                Ok(Arc::new(
                    vec![median(array); array.len()]
                        .into_iter()
                        .collect::<UInt64Array>(),
                ))
            }
            DataType::Int8 => {
                let array = data
                    .as_any()
                    .downcast_ref::<Int8Array>()
                    .ok_or(super::ColumnTransformationError::DowncastFailed)?;
                Ok(Arc::new(
                    vec![median(array); array.len()]
                        .into_iter()
                        .collect::<Int8Array>(),
                ))
            }
            DataType::Int16 => {
                let array = data
                    .as_any()
                    .downcast_ref::<Int16Array>()
                    .ok_or(super::ColumnTransformationError::DowncastFailed)?;
                Ok(Arc::new(
                    vec![median(array); array.len()]
                        .into_iter()
                        .collect::<Int16Array>(),
                ))
            }
            DataType::Int32 => {
                let array = data
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .ok_or(super::ColumnTransformationError::DowncastFailed)?;
                Ok(Arc::new(
                    vec![median(array); array.len()]
                        .into_iter()
                        .collect::<Int32Array>(),
                ))
            }
            DataType::Int64 => {
                let array = data
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or(super::ColumnTransformationError::DowncastFailed)?;
                Ok(Arc::new(
                    vec![median(array); array.len()]
                        .into_iter()
                        .collect::<Int64Array>(),
                ))
            }
            _ => todo!("{:?}", data.data_type()),
        }
    }

    fn output_format(
        &self,
        input: &DataType,
    ) -> super::ColumnTransformationResult<ColumnTransformationOutput> {
        match input {
            DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64 => Ok(ColumnTransformationOutput {
                data_type: input.clone(),
                nullable: false,
            }),
            _ => Err(super::ColumnTransformationError::UnsupportedType(
                input.clone(),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_number_agg_range_equal() {
        let aggreagtion = AggMedian {};
        let array = Arc::new(Int32Array::from(vec![10 as i32, 10 as i32, 10 as i32]));
        let result = aggreagtion.transform_data(array).unwrap();

        assert_eq!(
            vec![Some(10), Some(10), Some(10)],
            result
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .iter()
                .collect::<Vec<Option<i32>>>()
        );
    }
}
