use crate::projection::ProjectedOrigin;
use anyhow::Result;
use arrow::{datatypes::Schema, record_batch::RecordBatch};

pub trait Transformer: Send + Sync {
    fn transform_schema(&self, schema: &Schema, origins: &[ProjectedOrigin]) -> Result<Schema>;
    fn transform_records(
        &self,
        data: &RecordBatch,
        origins: &[ProjectedOrigin],
    ) -> Result<RecordBatch>;
}
