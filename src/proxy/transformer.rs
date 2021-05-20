use arrow::record_batch::RecordBatch;

pub trait Transformer: Sync + Send {
    fn transform_data(&self, data: &RecordBatch) -> RecordBatch;

    fn transform_query(&self, query: String) -> String {
        query
    }
}
