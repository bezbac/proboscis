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
    if array.is_empty() {
        return None;
    }

    let min = array.iter().min().unwrap();
    let max = array.iter().max().unwrap();

    match (min, max) {
        (Some(min), Some(max)) => {
            let agg = if max == min {
                format!("{}", max)
            } else {
                format!("{} - {}", min, max)
            };

            Some(agg)
        }
        _ => None,
    }
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
        let items = vec![10, 10, 10];
        let array = Arc::new(Int32Array::from(items));
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
        let items = vec![10, 20, 30];
        let array = Arc::new(Int32Array::from(items));
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

    #[test]
    fn test_number_agg_range_empty() {
        let aggreagtion = AggRange {};
        let items: Vec<i32> = vec![];
        let array = Arc::new(Int32Array::from(items));
        let result = aggreagtion.transform_data(array).unwrap();

        assert_eq!(
            Vec::<Option<&str>>::new(),
            result
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .iter()
                .collect::<Vec<Option<&str>>>()
        );
    }

    #[test]
    fn test_number_agg_range_none() {
        let aggreagtion = AggRange {};
        let items = vec![None, None, None];
        let array = Arc::new(Int32Array::from(items));
        let result = aggreagtion.transform_data(array).unwrap();

        assert_eq!(
            vec![None, None, None],
            result
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .iter()
                .collect::<Vec<Option<&str>>>()
        );
    }

    #[test]
    fn test_number_agg_range_mixed() {
        let aggreagtion = AggRange {};
        let items = vec![None, None, None, Some(10)];
        let array = Arc::new(Int32Array::from(items));
        let result = aggreagtion.transform_data(array).unwrap();

        assert_eq!(
            vec![None, None, None, None],
            result
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .iter()
                .collect::<Vec<Option<&str>>>()
        );
    }
}
