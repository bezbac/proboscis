use super::AnonymizationCriteria;
use crate::{
    traits::Transformer,
    transformers::anonymization::{
        algorithm::{anonymize, NumericAggregation},
        conversion::{data_frame_to_record_batch, record_batch_to_data_frame},
    },
    util::get_schema_fields,
};
use anyhow::Result;
use arrow::record_batch::RecordBatch;
use itertools::Itertools;
use sqlparser::ast::Statement;
use std::collections::HashMap;

pub struct AnonymizationTransformer {
    pub identifier_columns: Vec<String>,
    pub quasi_identifier_columns: HashMap<String, Option<NumericAggregation>>,
    pub criteria: AnonymizationCriteria,
}

// The identifier & pseudo identifiers contained in the query
type RelevantColumns = (Vec<String>, HashMap<String, Option<NumericAggregation>>);

impl AnonymizationTransformer {
    fn get_relevant_columns(
        &self,
        query: &[Statement],
        schema: &arrow::datatypes::Schema,
    ) -> Result<RelevantColumns> {
        let normalized_field_names = get_schema_fields(
            query
                .first()
                .ok_or_else(|| anyhow::anyhow!("Missing query"))?,
        )?;

        let quasi_identifier_columns: HashMap<String, Option<NumericAggregation>> =
            normalized_field_names
                .iter()
                .enumerate()
                .filter(|(_, normalized_column_name)| {
                    self.quasi_identifier_columns
                        .keys()
                        .contains(normalized_column_name)
                })
                .map(|(idx, normalized_column_name)| {
                    (
                        schema.field(idx).name().to_string(),
                        *self
                            .quasi_identifier_columns
                            .get(normalized_column_name)
                            .unwrap(),
                    )
                })
                .collect();

        let identifier_columns: Vec<String> = normalized_field_names
            .iter()
            .enumerate()
            .filter(|(_, column_name)| self.identifier_columns.contains(column_name))
            .map(|(idx, _)| schema.field(idx).name().to_string())
            .collect();

        Ok((identifier_columns, quasi_identifier_columns))
    }
}

impl Transformer for AnonymizationTransformer {
    fn transform_schema(
        &self,
        query: &[Statement],
        schema: &arrow::datatypes::Schema,
    ) -> Result<arrow::datatypes::Schema> {
        let (identifier_columns, quasi_identifiers) = self.get_relevant_columns(query, schema)?;

        if quasi_identifiers.is_empty() && identifier_columns.is_empty() {
            return Ok(schema.clone());
        }

        let updated_fields = schema
            .fields()
            .iter()
            .map(|field| {
                match quasi_identifiers.get(field.name()) {
                    Some(aggregation) => {
                        let aggregation = if let Some(aggregation) = aggregation {
                            aggregation
                        } else {
                            // TODO: Make this more logical for non-numeric types
                            &NumericAggregation::Median
                        };

                        let new_type = aggregation.output_type(field.data_type());

                        let mut updated_field = arrow::datatypes::Field::new(
                            field.name(),
                            new_type,
                            field.is_nullable(),
                        );
                        updated_field.set_metadata(field.metadata().clone());

                        updated_field
                    }
                    None => field.clone(),
                }
            })
            .collect();

        let updated_schema = arrow::datatypes::Schema::new(updated_fields);

        Ok(updated_schema)
    }

    fn transform_records(&self, query: &[Statement], data: &RecordBatch) -> Result<RecordBatch> {
        let (identifier_columns, quasi_identifiers) =
            self.get_relevant_columns(query, &data.schema())?;

        if quasi_identifiers.is_empty() && identifier_columns.is_empty() {
            return Ok(data.clone());
        }

        let dataframe = record_batch_to_data_frame(data);

        let identifier_columns_strs: Vec<&str> =
            identifier_columns.iter().map(|f| f.as_str()).collect();

        let anonymized = anonymize(
            &dataframe,
            &identifier_columns_strs,
            &quasi_identifiers,
            &[self.criteria.clone()],
            &NumericAggregation::Median,
        )?;

        let updated_schema = self.transform_schema(query, &data.schema())?;

        let result = data_frame_to_record_batch(&anonymized, updated_schema);
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::{
        array::{Int32Array, StringArray},
        datatypes::{DataType, Field, Schema},
    };
    use sqlparser::{dialect::PostgreSqlDialect, parser::Parser};

    use super::*;

    #[test]
    fn with_default_aggretation() {
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

        let quasi_identifier_columns: HashMap<String, Option<NumericAggregation>> = vec![
            ("contacts.age".to_string(), None),
            ("contacts.profession".to_string(), None),
            ("contacts.empty".to_string(), None),
        ]
        .iter()
        .cloned()
        .collect();

        let transformer = AnonymizationTransformer {
            quasi_identifier_columns,
            identifier_columns: vec![],
            criteria: AnonymizationCriteria::KAnonymous { k: 2 },
        };

        let dialect = PostgreSqlDialect {};
        let query = Parser::parse_sql(&dialect, "SELECT id, age, profession, empty FROM contacts;")
            .unwrap()
            .pop()
            .unwrap();

        let transformed_schema = transformer
            .transform_schema(&[query], &batch.schema())
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
    fn with_schema_transforming_aggregation() {
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

        let quasi_identifier_columns: HashMap<String, Option<NumericAggregation>> = vec![
            ("contacts.age".to_string(), Some(NumericAggregation::Range)),
            ("contacts.profession".to_string(), None),
            ("contacts.empty".to_string(), None),
        ]
        .iter()
        .cloned()
        .collect();

        let transformer = AnonymizationTransformer {
            quasi_identifier_columns,
            identifier_columns: vec![],
            criteria: AnonymizationCriteria::KAnonymous { k: 2 },
        };

        let dialect = PostgreSqlDialect {};
        let query = Parser::parse_sql(&dialect, "SELECT id, age, profession, empty FROM contacts;")
            .unwrap()
            .pop()
            .unwrap();

        let transformed_schema = transformer
            .transform_schema(&[query], &batch.schema())
            .unwrap();

        assert_eq!(
            vec![
                &DataType::Int32,
                &DataType::Utf8,
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
}
