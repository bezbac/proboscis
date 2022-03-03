use crate::data::arrow::{
    serialize_record_batch_schema_to_row_description, serialize_record_batch_to_data_rows,
};
use arrow::{datatypes::Schema, record_batch::RecordBatch};
use proboscis_postgres_protocol::message::{
    BackendMessage, CommandCompleteTag, ParameterDescription, ReadyForQueryTransactionStatus,
};

pub enum SyncResponse {
    Schema { schema: Schema, query: String },
    Records { data: RecordBatch, query: String },
    CommandComplete(CommandCompleteTag),
    BindComplete,
    ParseComplete,
    ReadyForQuery,
    ParameterDescription(ParameterDescription),
    NoData,
    EmptyQueryResponse,
    PortalSuspended,
}

impl SyncResponse {
    pub fn as_messages(self) -> Vec<BackendMessage> {
        match self {
            SyncResponse::Schema { schema, query: _ } => {
                let row_description = serialize_record_batch_schema_to_row_description(&schema);
                vec![BackendMessage::RowDescription(row_description)]
            }
            SyncResponse::Records { data, query: _ } => {
                let messages = serialize_record_batch_to_data_rows(&data)
                    .unwrap()
                    .iter()
                    .map(|data_row| BackendMessage::DataRow(data_row.clone()))
                    .collect();

                messages
            }
            SyncResponse::CommandComplete(tag) => vec![BackendMessage::CommandComplete(tag)],
            SyncResponse::ParameterDescription(parameter_description) => {
                vec![BackendMessage::ParameterDescription(parameter_description)]
            }
            SyncResponse::BindComplete => vec![BackendMessage::BindComplete],
            SyncResponse::ParseComplete => vec![BackendMessage::ParseComplete],
            SyncResponse::ReadyForQuery => vec![BackendMessage::ReadyForQuery(
                ReadyForQueryTransactionStatus::NotInTransaction,
            )],
            SyncResponse::NoData => vec![BackendMessage::NoData],
            SyncResponse::EmptyQueryResponse => vec![BackendMessage::EmptyQueryResponse],
            SyncResponse::PortalSuspended => vec![BackendMessage::PortalSuspended],
        }
    }
}
