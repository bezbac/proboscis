use anyhow::Result;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;

#[async_trait]
pub trait Resolver: Sync + Send {
    async fn query(&self, query: &String) -> Result<RecordBatch>;

    async fn inform(&mut self, query: &String, data: RecordBatch);
}

#[async_trait]
pub trait SimpleResolver: Sync + Send {
    async fn lookup(&self, query: &String) -> Result<RecordBatch>;
    async fn inform(&mut self, query: &String, data: RecordBatch);
}

#[async_trait]
impl<T: SimpleResolver> Resolver for T {
    async fn query(&self, query: &String) -> Result<RecordBatch> {
        self.lookup(query).await
    }

    async fn inform(&mut self, query: &String, data: RecordBatch) {
        self.inform(query, data).await
    }
}
