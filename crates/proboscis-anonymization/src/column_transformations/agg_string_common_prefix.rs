use super::{ColumnTransformation, ColumnTransformationOutput, ColumnTransformationResult};
use arrow::{
    array::{ArrayRef, GenericStringArray},
    datatypes::DataType,
};
use std::{cmp, sync::Arc};

pub fn longest_common_prefix(strings: Vec<&str>) -> &str {
    if strings.is_empty() {
        return "";
    }
    let str0 = strings[0];
    let str0bytes = str0.as_bytes();
    let mut len = str0.len();
    for str in &strings[1..] {
        len = cmp::min(
            len,
            str.as_bytes()
                .iter()
                .zip(str0bytes)
                .take_while(|&(a, b)| a == b)
                .count(),
        );
    }
    &strings[0][..len]
}

fn agg_string_array<T: arrow::array::StringOffsetSizeTrait>(
    input: ArrayRef,
) -> ColumnTransformationResult<ArrayRef> {
    let longest_common_prefix = longest_common_prefix(
        input
            .as_any()
            .downcast_ref::<GenericStringArray<T>>()
            .ok_or(super::ColumnTransformationError::DowncastFailed)?
            .iter()
            .map(|s| s.map_or("", |s| s))
            .collect(),
    );

    Ok(Arc::new(GenericStringArray::<T>::from(vec![
        format!(
            "{}*",
            longest_common_prefix
        );
        input.len()
    ])))
}

pub struct AggStringCommonPrefix {}

impl ColumnTransformation for AggStringCommonPrefix {
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
    use arrow::array::StringArray;
    use super::*;

    #[test]
    fn test_string_aggregation() {
        let aggreagtion = AggStringCommonPrefix {};
        let array = Arc::new(StringArray::from(vec![
            "Berlin", "Berlin", "Bern", "Bergen",
        ]));
        let result = aggreagtion.transform_data(array).unwrap();

        assert_eq!(
            vec![Some("Ber*"), Some("Ber*"), Some("Ber*"), Some("Ber*")],
            result
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .iter()
                .collect::<Vec<Option<&str>>>()
        );
    }
}
