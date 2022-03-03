use super::{ColumnTransformation, ColumnTransformationOutput, ColumnTransformationResult};
use arrow::{
    array::{ArrayRef, PrimitiveArray, StringArray},
    datatypes::{
        ArrowPrimitiveType, DataType, Int16Type, Int32Type, Int64Type, Int8Type, UInt16Type,
        UInt32Type, UInt64Type, UInt8Type,
    },
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

fn agg_numeric_array<T>(input: ArrayRef) -> ColumnTransformationResult<ArrayRef>
where
    T: ArrowPrimitiveType,
    <T as ArrowPrimitiveType>::Native: Ord + Display,
{
    let array = input
        .as_any()
        .downcast_ref::<PrimitiveArray<T>>()
        .ok_or(super::ColumnTransformationError::DowncastFailed)?;

    Ok(Arc::new(
        vec![aggregated_value(array); array.len()]
            .into_iter()
            .collect::<StringArray>(),
    ))
}

pub struct AggRange;

impl ColumnTransformation for AggRange {
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
        _input: &DataType,
    ) -> super::ColumnTransformationResult<ColumnTransformationOutput> {
        Ok(ColumnTransformationOutput {
            data_type: DataType::Utf8,
            nullable: false,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int32Array;

    #[test]
    fn test_number_agg_range_equal() {
        let aggreagtion = AggRange {};
        let array = Arc::new(Int32Array::from(vec![10 as i32, 10 as i32, 10 as i32]));
        let result = aggreagtion.transform_data(array).unwrap();

        assert_eq!(
            vec![Some("10"), Some("10"), Some("10")],
            result
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .iter()
                .collect::<Vec<Option<&str>>>()
        );
    }

    #[test]
    fn test_number_agg_range() {
        let aggreagtion = AggRange {};
        let array = Arc::new(Int32Array::from(vec![10 as i32, 20 as i32, 30 as i32]));
        let result = aggreagtion.transform_data(array).unwrap();

        assert_eq!(
            vec![Some("10 - 30"), Some("10 - 30"), Some("10 - 30")],
            result
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .iter()
                .collect::<Vec<Option<&str>>>()
        );
    }
}
