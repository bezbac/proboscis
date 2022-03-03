mod pool;
mod target_config;

use crate::pool::Manager;
use crate::pool::Pool;
use arrow::{datatypes::Schema, record_batch::RecordBatch};
use async_trait::async_trait;
use deadpool::managed::BuildError;
use proboscis_core::resolver::ResolveError;
use proboscis_core::{
    data::arrow::{
        protocol_fields_to_schema, serialize_record_batch_schema_to_row_description,
        simple_query_response_to_record_batch,
    },
    resolver::Resolver,
    resolver::{ClientId, SyncResponse},
};
use proboscis_postgres_protocol::message::{
    BackendMessage, Bind, Close, CommandCompleteTag, DataRow, Describe, Execute, FrontendMessage,
    Parse, RowDescription,
};
use std::collections::hash_map::Entry::Occupied;
use std::collections::hash_map::Entry::Vacant;
use std::collections::{HashMap, VecDeque};

pub use target_config::TargetConfig;

#[derive(Debug)]
enum ClientOperation {
    Parse,
    Bind { statement: String, portal: String },
    Describe { statement: String },
    Execute { portal: String },
}

#[derive(Debug)]
struct ActiveConnection {
    connection: deadpool::managed::Object<Manager>,
    requested_ops: VecDeque<ClientOperation>,
}

impl ActiveConnection {
    fn new(connection: deadpool::managed::Object<Manager>) -> ActiveConnection {
        ActiveConnection {
            connection,
            requested_ops: VecDeque::new(),
        }
    }
}

pub struct PostgresResolver {
    // Active connections are remove from the pool.
    // To add them back to the pool, drop them.
    active_connections: HashMap<ClientId, ActiveConnection>,
    pool: Pool,

    // Maps a statement to a schema
    statement_schema_cache: HashMap<String, Schema>,

    // Maps a statement to an sql string
    statement_query_cache: HashMap<String, String>,

    // Maps a portal to a statement
    portal_cache: HashMap<String, String>,
}

impl PostgresResolver {
    pub async fn create(
        target_config: TargetConfig,
        max_pool_size: usize,
    ) -> Result<PostgresResolver, BuildError<ResolveError>> {
        let manager = Manager::new(target_config);
        let pool = Pool::builder(manager).max_size(max_pool_size).build()?;

        Ok(PostgresResolver {
            active_connections: HashMap::new(),
            pool,
            statement_schema_cache: HashMap::new(),
            portal_cache: HashMap::new(),
            statement_query_cache: HashMap::new(),
        })
    }

    fn terminate_connection(&mut self, client_id: ClientId) {
        self.active_connections.remove(&client_id);
    }
}

macro_rules! get_connection {
    ($resolver:ident, $client_id:ident) => {
        match $resolver.active_connections.entry($client_id) {
            Vacant(entry) => {
                let connection = $resolver
                    .pool
                    .get()
                    .await
                    .map_err(|err| ResolveError::Other(anyhow::anyhow!(err)))?;

                let value = ActiveConnection::new(connection);

                entry.insert(value)
            }
            Occupied(entry) => entry.into_mut(),
        }
    };
}

#[async_trait]
impl Resolver for PostgresResolver {
    async fn query(
        &mut self,
        client_id: ClientId,
        query: String,
    ) -> Result<RecordBatch, ResolveError> {
        let connection = get_connection!(self, client_id);

        connection
            .connection
            .write_message(FrontendMessage::SimpleQuery(query).into())
            .await?;

        let mut fields = vec![];
        let mut data_rows = vec![];
        loop {
            let response = connection.connection.read_backend_message().await?;
            match response {
                BackendMessage::ReadyForQuery(_) => break,
                BackendMessage::RowDescription(RowDescription {
                    fields: mut message_fields,
                }) => fields.append(&mut message_fields),
                BackendMessage::DataRow(data_row) => {
                    data_rows.push(data_row);
                }
                BackendMessage::CommandComplete(CommandCompleteTag(_)) => {
                    // TODO: Handle this
                }
                _ => unimplemented!(""),
            }
        }

        let data = simple_query_response_to_record_batch(&fields, &data_rows)?;

        Ok(data)
    }

    async fn parse(&mut self, client_id: ClientId, parse: Parse) -> Result<(), ResolveError> {
        let connection = get_connection!(self, client_id);

        let statement_name = parse.statement_name.clone();
        let query = parse.query.clone();

        connection
            .connection
            .write_message(FrontendMessage::Parse(parse).into())
            .await?;

        connection.requested_ops.push_back(ClientOperation::Parse);

        self.statement_query_cache.insert(statement_name, query);

        Ok(())
    }

    async fn describe(
        &mut self,
        client_id: ClientId,
        describe: Describe,
    ) -> Result<(), ResolveError> {
        let connection = get_connection!(self, client_id);

        let statement = describe.name.clone();

        connection
            .connection
            .write_message(FrontendMessage::Describe(describe).into())
            .await?;

        connection
            .requested_ops
            .push_back(ClientOperation::Describe { statement });

        Ok(())
    }

    async fn bind(&mut self, client_id: ClientId, bind: Bind) -> Result<(), ResolveError> {
        let connection = get_connection!(self, client_id);

        let statement = bind.statement.clone();
        let portal = bind.portal.clone();

        connection
            .connection
            .write_message(FrontendMessage::Bind(bind).into())
            .await?;

        connection
            .requested_ops
            .push_back(ClientOperation::Bind { statement, portal });

        Ok(())
    }

    async fn execute(&mut self, client_id: ClientId, execute: Execute) -> Result<(), ResolveError> {
        let connection = get_connection!(self, client_id);

        let portal = execute.portal.clone();

        connection
            .connection
            .write_message(FrontendMessage::Execute(execute).into())
            .await?;

        connection
            .requested_ops
            .push_back(ClientOperation::Execute { portal });

        Ok(())
    }

    async fn sync(&mut self, client_id: ClientId) -> Result<Vec<SyncResponse>, ResolveError> {
        let connection = get_connection!(self, client_id);

        connection
            .connection
            .write_message(FrontendMessage::Sync.into())
            .await?;

        let mut responses = vec![];
        'client_request: while let Some(operation) = &connection.requested_ops.pop_front() {
            match operation {
                ClientOperation::Parse => {
                    let read_message = connection.connection.read_backend_message().await?;

                    match read_message {
                        BackendMessage::ParseComplete => {
                            responses.push(SyncResponse::ParseComplete)
                        }
                        _ => todo!(),
                    }
                }
                ClientOperation::Describe { statement } => loop {
                    let read_message = connection.connection.read_backend_message().await?;

                    match read_message {
                        BackendMessage::RowDescription(RowDescription { fields }) => {
                            let schema = protocol_fields_to_schema(&fields)?;

                            responses.push(SyncResponse::Schema {
                                schema: schema.clone(),
                                query: self.statement_query_cache.get(statement).unwrap().clone(),
                            });

                            self.statement_schema_cache
                                .insert(statement.clone(), schema.clone());

                            break;
                        }
                        BackendMessage::ParameterDescription(parameter_description) => responses
                            .push(SyncResponse::ParameterDescription(parameter_description)),
                        BackendMessage::NoData => {
                            responses.push(SyncResponse::NoData);
                            break;
                        }
                        _ => todo!(),
                    }
                },
                ClientOperation::Bind { statement, portal } => {
                    let read_message = connection.connection.read_backend_message().await?;

                    self.portal_cache.insert(portal.clone(), statement.clone());

                    match read_message {
                        BackendMessage::BindComplete => responses.push(SyncResponse::BindComplete),
                        _ => todo!(),
                    }
                }
                ClientOperation::Execute { portal } => {
                    let mut data_rows: Vec<DataRow> = vec![];
                    let command_complete_tag;

                    loop {
                        let read_message = connection.connection.read_backend_message().await?;

                        match read_message {
                            BackendMessage::DataRow(data_row) => data_rows.push(data_row),
                            BackendMessage::CommandComplete(tag) => {
                                command_complete_tag = Some(tag);
                                break;
                            }
                            BackendMessage::PortalSuspended => {
                                command_complete_tag = None;
                                break;
                            }
                            BackendMessage::EmptyQueryResponse => {
                                responses.push(SyncResponse::EmptyQueryResponse);
                                break 'client_request;
                            }
                            _ => todo!(),
                        }
                    }

                    let statement = self.portal_cache.get(portal).unwrap();
                    let schema = self.statement_schema_cache.get(statement).unwrap();

                    let RowDescription { fields } =
                        serialize_record_batch_schema_to_row_description(schema);

                    let record_batch = simple_query_response_to_record_batch(&fields, &data_rows)?;

                    responses.push(SyncResponse::Records {
                        data: record_batch,
                        query: self.statement_query_cache.get(statement).unwrap().clone(),
                    });

                    match command_complete_tag {
                        Some(tag) => responses.push(SyncResponse::CommandComplete(tag)),
                        None => responses.push(SyncResponse::PortalSuspended),
                    }
                }
            }
        }

        let read_message = connection.connection.read_backend_message().await?;
        match read_message {
            BackendMessage::ReadyForQuery(_) => SyncResponse::ReadyForQuery,
            _ => todo!(),
        };
        responses.push(SyncResponse::ReadyForQuery);

        Ok(responses)
    }

    async fn close(&mut self, client_id: ClientId, close: Close) -> Result<(), ResolveError> {
        let connection = get_connection!(self, client_id);

        connection
            .connection
            .write_message(FrontendMessage::Close(close).into())
            .await?;

        let _read_message = connection.connection.read_backend_message().await?;
        // TODO: Handle response

        Ok(())
    }

    async fn initialize(&mut self, _client_id: ClientId) -> Result<(), ResolveError> {
        Ok(())
    }

    async fn terminate(&mut self, client_id: ClientId) -> Result<(), ResolveError> {
        self.terminate_connection(client_id);

        Ok(())
    }
}
