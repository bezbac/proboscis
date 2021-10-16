use arrow::record_batch::RecordBatch;
use sqlparser::ast::Statement;

use crate::{
    traits::Transformer,
    transformers::anonymization::{
        algorithm::{anonymize, NumericAggregation},
        conversion::{data_frame_to_record_batch, record_batch_to_data_frame},
    },
    util::get_schema_fields,
};

use super::AnonymizationCriteria;

pub struct AnonymizationTransformer {
    pub identifier_columns: Vec<String>,
    pub quasi_identifier_columns: Vec<String>,
    pub criteria: AnonymizationCriteria,
}

impl Transformer for AnonymizationTransformer {
    fn transform_schema(
        &self,
        _query: &[Statement],
        schema: &arrow::datatypes::Schema,
    ) -> arrow::datatypes::Schema {
        // TODO: Correct this
        schema.clone()
    }

    fn transform_records(&self, query: &[Statement], data: &RecordBatch) -> RecordBatch {
        let normalized_field_names = get_schema_fields(query.first().unwrap()).unwrap();

        let dataframe = record_batch_to_data_frame(data);

        let quasi_identifier_columns: Vec<String> = normalized_field_names
            .iter()
            .enumerate()
            .filter(|(_, column_name)| self.quasi_identifier_columns.contains(column_name))
            .map(|(idx, _)| data.schema().field(idx).name().to_string())
            .collect();

        let quasi_identifier_columns_strs: Vec<&str> = quasi_identifier_columns
            .iter()
            .map(|f| f.as_str())
            .collect();

        let identifier_columns: Vec<String> = normalized_field_names
            .iter()
            .enumerate()
            .filter(|(_, column_name)| self.identifier_columns.contains(column_name))
            .map(|(idx, _)| data.schema().field(idx).name().to_string())
            .collect();

        let identifier_columns_strs: Vec<&str> =
            identifier_columns.iter().map(|f| f.as_str()).collect();

        let anonymized = anonymize(
            &dataframe,
            &identifier_columns_strs,
            &quasi_identifier_columns_strs,
            &[self.criteria.clone()],
            &NumericAggregation::Median,
        );

        let updated_schema = self.transform_schema(query, &data.schema());

        data_frame_to_record_batch(&anonymized, updated_schema)
    }
}
