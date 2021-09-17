use anyhow::Result;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use postgres_protocol::{CloseKind, DescribeKind, Message};
use uuid::Uuid;

#[async_trait]
pub trait Resolver: Sync + Send {
    async fn initialize(&mut self, client_id: Uuid) -> Result<()>;

    async fn query(&mut self, client_id: Uuid, query: String) -> Result<RecordBatch>;

    async fn parse(
        &mut self,
        client_id: Uuid,
        statement_name: String,
        query: String,
        param_types: Vec<u32>,
    ) -> Result<()>;

    async fn describe(&mut self, client_id: Uuid, kind: DescribeKind, name: String) -> Result<()>;

    async fn bind(
        &mut self,
        client_id: Uuid,
        statement: String,
        portal: String,
        params: Vec<Vec<u8>>,
        formats: Vec<i16>,
        results: Vec<i16>,
    ) -> Result<()>;

    async fn execute(&mut self, client_id: Uuid, portal: String, row_limit: i32) -> Result<()>;

    async fn sync(&mut self, client_id: Uuid) -> Result<Vec<Message>>;

    async fn close(&mut self, client_id: Uuid, kind: CloseKind, name: String) -> Result<()>;

    async fn terminate(&mut self, client_id: Uuid) -> Result<()>;
}
