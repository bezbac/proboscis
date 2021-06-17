use arrow::{record_batch::RecordBatch};

pub trait Transformer: Sync + Send {
    fn transform(&self, data: &RecordBatch) -> RecordBatch;
}
