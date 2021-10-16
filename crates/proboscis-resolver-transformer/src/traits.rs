use anyhow::Result;
use arrow::{datatypes::Schema, record_batch::RecordBatch};
use sqlparser::ast::Statement;

pub trait Transformer: Send + Sync {
    fn transform_schema(&self, query: &[Statement], schema: &Schema) -> Result<Schema>;
    fn transform_records(&self, query: &[Statement], data: &RecordBatch) -> Result<RecordBatch>;
}
