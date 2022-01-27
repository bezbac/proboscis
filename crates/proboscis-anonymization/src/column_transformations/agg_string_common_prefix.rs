use super::{ColumnTransformation, ColumnTransformationOutput};
use anyhow::Result;
use arrow::{
    array::{ArrayRef, LargeStringArray, StringArray},
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

pub struct AggStringCommonPrefix {}

impl ColumnTransformation for AggStringCommonPrefix {
    fn transform_data(&self, data: ArrayRef) -> Result<ArrayRef> {
        match data.data_type() {
            DataType::Utf8 => {
                let longest_common_prefix = longest_common_prefix(
                    data.as_any()
                        .downcast_ref::<StringArray>()
                        .unwrap()
                        .iter()
                        .map(|s| s.map_or("", |s| s))
                        .collect(),
                );

                Ok(Arc::new(StringArray::from(vec![
                    format!(
                        "{}*",
                        longest_common_prefix
                    );
                    data.len()
                ])))
            }
            DataType::LargeUtf8 => {
                let longest_common_prefix = longest_common_prefix(
                    data.as_any()
                        .downcast_ref::<LargeStringArray>()
                        .unwrap()
                        .iter()
                        .map(|s| s.map_or("", |s| s))
                        .collect(),
                );

                Ok(Arc::new(LargeStringArray::from(vec![
                    format!(
                        "{}*",
                        longest_common_prefix
                    );
                    data.len()
                ])))
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

#[cfg(test)]
mod tests {
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
