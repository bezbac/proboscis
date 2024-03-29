use super::AnonymizationCriteria;
use crate::{
    algorithm::{anonymize, NumericAggregation, StringAggregation},
    conversion::{data_frame_to_record_batch, record_batch_to_data_frame},
};
use arrow::record_batch::RecordBatch;
use proboscis_resolver_transformer::{
    projection::{ProjectedOrigin, TableColumn},
    Transformer, TransformerError,
};
use std::collections::HashMap;

pub struct AnonymizationTransformer {
    pub identifier_columns: Vec<String>,
    pub quasi_identifier_columns: HashMap<String, (NumericAggregation, StringAggregation)>,
    pub criteria: AnonymizationCriteria,
}

// The identifier & pseudo identifiers contained in the query
type RelevantColumns = (
    Vec<String>,
    HashMap<String, (NumericAggregation, StringAggregation)>,
);

impl AnonymizationTransformer {
    fn get_relevant_columns(
        &self,
        origins: &[ProjectedOrigin],
        schema: &arrow::datatypes::Schema,
    ) -> Result<RelevantColumns, TransformerError> {
        let quasi_identifier_columns: HashMap<String, (NumericAggregation, StringAggregation)> =
            origins
                .iter()
                .enumerate()
                .filter_map(|(idx, origin)| match origin {
                    ProjectedOrigin::Function => None,
                    ProjectedOrigin::Value => None,
                    ProjectedOrigin::TableColumn(TableColumn { table, column }) => {
                        let normalized_column_name = &format!("{}.{}", table, column);

                        self.quasi_identifier_columns
                            .get(normalized_column_name)
                            .map(|aggregations| {
                                (schema.field(idx).name().to_string(), *aggregations)
                            })
                    }
                })
                .collect();

        let identifier_columns: Vec<String> = origins
            .iter()
            .enumerate()
            .filter_map(|(idx, origin)| match origin {
                ProjectedOrigin::Function => None,
                ProjectedOrigin::Value => None,
                ProjectedOrigin::TableColumn(TableColumn { table, column }) => {
                    let normalized_column_name = &format!("{}.{}", table, column);

                    let is_identifier = self.identifier_columns.contains(normalized_column_name);

                    if !is_identifier {
                        return None;
                    }

                    Some(schema.field(idx).name().to_string())
                }
            })
            .collect();

        Ok((identifier_columns, quasi_identifier_columns))
    }
}

impl Transformer for AnonymizationTransformer {
    fn transform_schema(
        &self,
        schema: &arrow::datatypes::Schema,
        origins: &[ProjectedOrigin],
    ) -> Result<arrow::datatypes::Schema, TransformerError> {
        let (identifier_columns, quasi_identifiers) = self.get_relevant_columns(origins, schema)?;

        if quasi_identifiers.is_empty() && identifier_columns.is_empty() {
            return Ok(schema.clone());
        }

        let mut updated_fields = vec![];
        for field in schema.fields() {
            let updated_field = match field.data_type() {
                arrow::datatypes::DataType::UInt8
                | arrow::datatypes::DataType::UInt16
                | arrow::datatypes::DataType::UInt32
                | arrow::datatypes::DataType::UInt64
                | arrow::datatypes::DataType::Int8
                | arrow::datatypes::DataType::Int16
                | arrow::datatypes::DataType::Int32
                | arrow::datatypes::DataType::Int64 => match quasi_identifiers.get(field.name()) {
                    Some((numeric_aggregation, _)) => {
                        let output_format = numeric_aggregation
                            .transformation()
                            .output_format(field.data_type())?;

                        arrow::datatypes::Field::new(
                            field.name(),
                            output_format.data_type,
                            output_format.nullable,
                        )
                    }
                    None => field.clone(),
                },
                _ => field.clone(),
            };

            updated_fields.push(updated_field);
        }

        let updated_schema = arrow::datatypes::Schema::new(updated_fields);

        Ok(updated_schema)
    }

    fn transform_records(
        &self,
        data: &RecordBatch,
        origins: &[ProjectedOrigin],
    ) -> Result<RecordBatch, TransformerError> {
        let (identifier_columns, quasi_identifiers) =
            self.get_relevant_columns(origins, &data.schema())?;

        if quasi_identifiers.is_empty() && identifier_columns.is_empty() {
            return Ok(data.clone());
        }

        let dataframe = record_batch_to_data_frame(data)
            .map_err(|err| TransformerError::Other(anyhow::anyhow!(err)))?;

        let identifier_columns_strs: Vec<&str> =
            identifier_columns.iter().map(|f| f.as_str()).collect();

        let anonymized = anonymize(
            &dataframe,
            &identifier_columns_strs,
            &quasi_identifiers,
            &[self.criteria.clone()],
        )?;

        let updated_schema = self.transform_schema(&data.schema(), origins)?;

        let result = data_frame_to_record_batch(&anonymized, updated_schema)?;

        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::{
        array::{Int32Array, StringArray},
        datatypes::{DataType, Field, Schema},
    };
    use itertools::Itertools;
    use std::sync::Arc;

    #[test]
    fn with_median_aggregation() {
        let id_array = Int32Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8, 9]);
        let age_array = Int32Array::from(vec![18, 40, 46, 22, 22, 26, 32, 17, 29]);
        let empty_array =
            Int32Array::from(vec![None, None, None, None, None, None, None, None, None]);
        let profession_array = StringArray::from(vec![
            "Sales",
            "Sales",
            "Engineering",
            "Engineering",
            "Engineering",
            "Marketing",
            "Marketing",
            "Sales",
            "Engineering",
        ]);

        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("age", DataType::Int32, false),
            Field::new("profession", DataType::Utf8, false),
            Field::new("empty", DataType::Int32, false),
        ]);

        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(id_array),
                Arc::new(age_array),
                Arc::new(profession_array),
                Arc::new(empty_array),
            ],
        )
        .unwrap();

        let quasi_identifier_columns = vec![
            (
                "contacts.age".to_string(),
                (NumericAggregation::Median, StringAggregation::Join),
            ),
            (
                "contacts.profession".to_string(),
                (NumericAggregation::Median, StringAggregation::Join),
            ),
            (
                "contacts.empty".to_string(),
                (NumericAggregation::Median, StringAggregation::Join),
            ),
        ]
        .iter()
        .cloned()
        .collect();

        let transformer = AnonymizationTransformer {
            quasi_identifier_columns,
            identifier_columns: vec![],
            criteria: AnonymizationCriteria::KAnonymous { k: 2 },
        };

        let origins = vec![
            ProjectedOrigin::TableColumn(TableColumn {
                table: String::from("contacts"),
                column: String::from("id"),
            }),
            ProjectedOrigin::TableColumn(TableColumn {
                table: String::from("contacts"),
                column: String::from("age"),
            }),
            ProjectedOrigin::TableColumn(TableColumn {
                table: String::from("contacts"),
                column: String::from("profession"),
            }),
            ProjectedOrigin::TableColumn(TableColumn {
                table: String::from("contacts"),
                column: String::from("empty"),
            }),
        ];

        let transformed_schema = transformer
            .transform_schema(&batch.schema(), &origins)
            .unwrap();

        assert_eq!(
            vec![
                &DataType::Int32,
                &DataType::Int32,
                &DataType::Utf8,
                &DataType::Int32
            ],
            transformed_schema
                .fields()
                .iter()
                .map(|f| f.data_type())
                .collect_vec()
        );
    }

    #[test]
    fn with_range_aggregation() {
        let id_array = Int32Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8, 9]);
        let age_array = Int32Array::from(vec![18, 40, 46, 22, 22, 26, 32, 17, 29]);
        let empty_array =
            Int32Array::from(vec![None, None, None, None, None, None, None, None, None]);
        let profession_array = StringArray::from(vec![
            "Sales",
            "Sales",
            "Engineering",
            "Engineering",
            "Engineering",
            "Marketing",
            "Marketing",
            "Sales",
            "Engineering",
        ]);

        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("age", DataType::Int32, false),
            Field::new("profession", DataType::Utf8, false),
            Field::new("empty", DataType::Int32, false),
        ]);

        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(id_array),
                Arc::new(age_array),
                Arc::new(profession_array),
                Arc::new(empty_array),
            ],
        )
        .unwrap();

        let quasi_identifier_columns = vec![
            (
                "contacts.age".to_string(),
                (NumericAggregation::Range, StringAggregation::Join),
            ),
            (
                "contacts.profession".to_string(),
                (NumericAggregation::Range, StringAggregation::Join),
            ),
            (
                "contacts.empty".to_string(),
                (NumericAggregation::Range, StringAggregation::Join),
            ),
        ]
        .iter()
        .cloned()
        .collect();

        let transformer = AnonymizationTransformer {
            quasi_identifier_columns,
            identifier_columns: vec![],
            criteria: AnonymizationCriteria::KAnonymous { k: 2 },
        };

        let origins = vec![
            ProjectedOrigin::TableColumn(TableColumn {
                table: String::from("contacts"),
                column: String::from("id"),
            }),
            ProjectedOrigin::TableColumn(TableColumn {
                table: String::from("contacts"),
                column: String::from("age"),
            }),
            ProjectedOrigin::TableColumn(TableColumn {
                table: String::from("contacts"),
                column: String::from("profession"),
            }),
            ProjectedOrigin::TableColumn(TableColumn {
                table: String::from("contacts"),
                column: String::from("empty"),
            }),
        ];

        let transformed_schema = transformer
            .transform_schema(&batch.schema(), &origins)
            .unwrap();

        assert_eq!(
            vec![
                &DataType::Int32,
                &DataType::Utf8,
                &DataType::Utf8,
                &DataType::Utf8
            ],
            transformed_schema
                .fields()
                .iter()
                .map(|f| f.data_type())
                .collect_vec()
        );
    }
}
