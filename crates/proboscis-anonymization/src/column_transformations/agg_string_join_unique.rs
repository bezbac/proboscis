use super::{ColumnTransformation, ColumnTransformationOutput, ColumnTransformationResult};
use arrow::{
    array::{ArrayRef, GenericStringArray},
    datatypes::DataType,
};
use itertools::Itertools;
use std::sync::Arc;

fn agg_string_array<T: arrow::array::StringOffsetSizeTrait>(
    input: ArrayRef,
) -> ColumnTransformationResult<ArrayRef> {
    let unique_strings: Vec<&str> = input
        .as_any()
        .downcast_ref::<GenericStringArray<T>>()
        .ok_or(super::ColumnTransformationError::DowncastFailed)?
        .iter()
        .unique()
        .map(|v| v.map_or("None", |v| v))
        .sorted()
        .collect();

    let new_string = unique_strings.join(", ");

    Ok(Arc::new(GenericStringArray::<T>::from(vec![
        new_string;
        input.len()
    ])))
}

pub struct AggStringJoinUnique;

impl ColumnTransformation for AggStringJoinUnique {
    fn transform_data(&self, data: ArrayRef) -> super::ColumnTransformationResult<ArrayRef> {
        match data.data_type() {
            DataType::Utf8 => agg_string_array::<i32>(data),
            DataType::LargeUtf8 => agg_string_array::<i64>(data),
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
    use arrow::array::StringArray;

    #[test]
    fn test_string_aggregation() {
        let aggreagtion = AggStringJoinUnique {};
        let array = Arc::new(StringArray::from(vec!["Müller", "Müller", "Schidt"]));
        let result = aggreagtion.transform_data(array).unwrap();

        assert_eq!(
            vec![
                Some("Müller, Schidt"),
                Some("Müller, Schidt"),
                Some("Müller, Schidt"),
            ],
            result
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .iter()
                .collect::<Vec<Option<&str>>>()
        );
    }
}
