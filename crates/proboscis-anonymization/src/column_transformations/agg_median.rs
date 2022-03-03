use super::{ColumnTransformation, ColumnTransformationOutput, ColumnTransformationResult};
use arrow::{
    array::{ArrayRef, PrimitiveArray},
    datatypes::{
        ArrowNativeType, ArrowNumericType, DataType, Int16Type, Int32Type, Int64Type, Int8Type,
        UInt16Type, UInt32Type, UInt64Type, UInt8Type,
    },
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

fn agg_numeric_array<T>(input: ArrayRef) -> ColumnTransformationResult<ArrayRef>
where
    T: ArrowNumericType,
    T::Native: Add<Output = T::Native> + std::ops::Div<Output = T::Native>,
{
    let array = input
        .as_any()
        .downcast_ref::<PrimitiveArray<T>>()
        .ok_or(super::ColumnTransformationError::DowncastFailed)?;

    Ok(Arc::new(
        vec![median(array); array.len()]
            .into_iter()
            .collect::<PrimitiveArray<T>>(),
    ))
}

pub struct AggMedian;

impl ColumnTransformation for AggMedian {
    fn transform_data(&self, data: ArrayRef) -> super::ColumnTransformationResult<ArrayRef> {
        match data.data_type() {
            DataType::UInt8 => agg_numeric_array::<UInt8Type>(data),
            DataType::UInt16 => agg_numeric_array::<UInt16Type>(data),
            DataType::UInt32 => agg_numeric_array::<UInt32Type>(data),
            DataType::UInt64 => agg_numeric_array::<UInt64Type>(data),
            DataType::Int8 => agg_numeric_array::<Int8Type>(data),
            DataType::Int16 => agg_numeric_array::<Int16Type>(data),
            DataType::Int32 => agg_numeric_array::<Int32Type>(data),
            DataType::Int64 => agg_numeric_array::<Int64Type>(data),
            _ => Err(super::ColumnTransformationError::UnsupportedType(
                data.data_type().clone(),
            )),
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
    use arrow::array::Int32Array;

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
