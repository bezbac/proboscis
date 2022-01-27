use super::{ColumnTransformation, ColumnTransformationOutput};
use anyhow::Result;
use arrow::{
    array::{ArrayRef, LargeStringArray, StringArray},
    datatypes::DataType,
};
use itertools::Itertools;
use std::sync::Arc;

pub struct AggStringJoinUnique {}

impl ColumnTransformation for AggStringJoinUnique {
    fn transform_data(&self, data: ArrayRef) -> Result<ArrayRef> {
        match data.data_type() {
            DataType::Utf8 => {
                let unique_strings: Vec<&str> = data
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .iter()
                    .unique()
                    .map(|v| v.map_or("None", |v| v))
                    .sorted()
                    .collect();

                let new_string = unique_strings.join(", ");

                Ok(Arc::new(StringArray::from(vec![new_string; data.len()])))
            }
            DataType::LargeUtf8 => {
                let unique_strings: Vec<&str> = data
                    .as_any()
                    .downcast_ref::<LargeStringArray>()
                    .unwrap()
                    .iter()
                    .unique()
                    .map(|v| v.map_or("None", |v| v))
                    .sorted()
                    .collect();

                let new_string = unique_strings.join(", ");

                Ok(Arc::new(LargeStringArray::from(vec![
                    new_string;
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
