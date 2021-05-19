use arrow::record_batch::RecordBatch;
use async_trait::async_trait;

use super::data::SimpleQueryResponse;

pub enum ResolverResult<T> {
    Hit(T),
    Miss,
}

#[async_trait]
pub trait Resolver: Sync + Send {
    async fn lookup(&self, query: &String) -> ResolverResult<SimpleQueryResponse>;
    async fn inform(&mut self, query: &String, data: SimpleQueryResponse);
}
