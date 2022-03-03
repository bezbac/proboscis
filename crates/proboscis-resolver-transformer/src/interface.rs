use crate::{error::TransformerError, projection::ProjectedOrigin};
use arrow::{datatypes::Schema, record_batch::RecordBatch};

pub trait Transformer: Send + Sync {
    fn transform_schema(
        &self,
        schema: &Schema,
        origins: &[ProjectedOrigin],
    ) -> Result<Schema, TransformerError>;
    fn transform_records(
        &self,
        data: &RecordBatch,
        origins: &[ProjectedOrigin],
    ) -> Result<RecordBatch, TransformerError>;
}
