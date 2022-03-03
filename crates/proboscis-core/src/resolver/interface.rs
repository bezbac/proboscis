use super::{error::ResolveError, response::SyncResponse};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use uuid::Uuid;

pub use proboscis_postgres_protocol::message::{Bind, Close, Describe, Execute, Parse};

pub type ClientId = Uuid;

#[async_trait]
pub trait Resolver: Sync + Send {
    async fn initialize(&mut self, client_id: ClientId) -> Result<(), ResolveError>;
    async fn query(
        &mut self,
        client_id: ClientId,
        query: String,
    ) -> Result<RecordBatch, ResolveError>;
    async fn parse(&mut self, client_id: ClientId, parse: Parse) -> Result<(), ResolveError>;
    async fn describe(
        &mut self,
        client_id: ClientId,
        describe: Describe,
    ) -> Result<(), ResolveError>;
    async fn bind(&mut self, client_id: ClientId, bind: Bind) -> Result<(), ResolveError>;
    async fn execute(&mut self, client_id: ClientId, execute: Execute) -> Result<(), ResolveError>;
    async fn sync(&mut self, client_id: ClientId) -> Result<Vec<SyncResponse>, ResolveError>;
    async fn close(&mut self, client_id: ClientId, close: Close) -> Result<(), ResolveError>;
    async fn terminate(&mut self, client_id: ClientId) -> Result<(), ResolveError>;
}
