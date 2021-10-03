use crate::utils::arrow::{
    serialize_record_batch_schema_to_row_description, serialize_record_batch_to_data_rows,
};
use anyhow::Result;
use arrow::{datatypes::Schema, record_batch::RecordBatch};
use async_trait::async_trait;
use postgres_protocol::message::{CommandCompleteTag, Message, ParameterDescription};
use uuid::Uuid;

pub use postgres_protocol::message::{Bind, Close, Describe, Execute, Parse};

pub type ClientId = Uuid;

impl SyncResponse {
    pub fn as_messages(self) -> Vec<Message> {
        match self {
            SyncResponse::Schema { schema, query: _ } => {
                let row_description = serialize_record_batch_schema_to_row_description(&schema);
                vec![Message::RowDescription(row_description)]
            }
            SyncResponse::Records { data, query: _ } => {
                let messages = serialize_record_batch_to_data_rows(&data)
                    .iter()
                    .map(|data_row| Message::DataRow(data_row.clone()))
                    .collect();

                messages
            }
            SyncResponse::CommandComplete(tag) => vec![Message::CommandComplete(tag)],
            SyncResponse::ParameterDescription(parameter_description) => {
                vec![Message::ParameterDescription(parameter_description)]
            }
            SyncResponse::BindComplete => vec![Message::BindComplete],
            SyncResponse::ParseComplete => vec![Message::ParseComplete],
            SyncResponse::ReadyForQuery => vec![Message::ReadyForQuery],
        }
    }
}

pub enum SyncResponse {
    Schema { schema: Schema, query: String },
    Records { data: RecordBatch, query: String },
    CommandComplete(CommandCompleteTag),
    BindComplete,
    ParseComplete,
    ReadyForQuery,
    ParameterDescription(ParameterDescription),
}

#[async_trait]
pub trait Resolver: Sync + Send {
    async fn initialize(&mut self, client_id: ClientId) -> Result<()>;
    async fn query(&mut self, client_id: ClientId, query: String) -> Result<RecordBatch>;
    async fn parse(&mut self, client_id: ClientId, parse: Parse) -> Result<()>;
    async fn describe(&mut self, client_id: ClientId, describe: Describe) -> Result<()>;
    async fn bind(&mut self, client_id: ClientId, bind: Bind) -> Result<()>;
    async fn execute(&mut self, client_id: ClientId, execute: Execute) -> Result<()>;
    async fn sync(&mut self, client_id: ClientId) -> Result<Vec<SyncResponse>>;
    async fn close(&mut self, client_id: ClientId, close: Close) -> Result<()>;
    async fn terminate(&mut self, client_id: ClientId) -> Result<()>;
}
