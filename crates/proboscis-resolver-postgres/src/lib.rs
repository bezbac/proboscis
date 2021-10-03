mod target_config;
use anyhow::Result;
use arrow::{datatypes::Schema, record_batch::RecordBatch};
use async_trait::async_trait;
use deadpool::managed::RecycleResult;
use futures::TryFutureExt;
use postgres_protocol::{
    message::{
        BackendMessage, Bind, Close, CommandCompleteTag, DataRow, Describe, Execute,
        FrontendMessage, MD5Hash, MD5Salt, Parse, RowDescription,
    },
    StartupMessage,
};
use proboscis_core::{
    resolver::Resolver,
    resolver::{ClientId, SyncResponse},
    utils::arrow::{
        protocol_fields_to_schema, serialize_record_batch_schema_to_row_description,
        simple_query_response_to_record_batch,
    },
    utils::connection::{Connection, MaybeTlsStream, ProtocolStream},
    utils::password::encode_md5_password_hash,
};
use std::collections::{HashMap, VecDeque};
pub use target_config::TargetConfig;

type Pool = deadpool::managed::Pool<Manager>;

#[derive(Debug)]
pub struct Manager {
    target_config: TargetConfig,
}

#[async_trait]
impl deadpool::managed::Manager for Manager {
    type Type = Connection;
    type Error = anyhow::Error;
    async fn create(&self) -> Result<Connection, anyhow::Error> {
        establish_connection(&self.target_config).await
    }

    async fn recycle(&self, _conn: &mut Connection) -> RecycleResult<anyhow::Error> {
        Ok(())
    }
}

pub async fn establish_connection(target_config: &TargetConfig) -> Result<Connection> {
    let stream =
        tokio::net::TcpStream::connect(&format!("{}:{}", target_config.host, target_config.port))
            .await?;

    let mut params: HashMap<String, String> = HashMap::new();

    if let Some(user) = target_config.user.as_ref() {
        params.insert("user".to_string(), user.to_string());
    }

    params.insert("client_encoding".to_string(), "UTF8".to_string());

    let mut connection = Connection::new(MaybeTlsStream::Left(stream), params.clone());

    connection
        .write_startup_message(StartupMessage::Startup { params })
        .await?;

    let response = connection.read_backend_message().await?;
    match response {
        BackendMessage::AuthenticationRequestMD5Password(MD5Salt(salt)) => {
            let hash = encode_md5_password_hash(
                target_config
                    .user
                    .as_ref()
                    .expect("Missing username in target_config"),
                target_config
                    .password
                    .as_ref()
                    .expect("Missing password in target_config"),
                &salt[..],
            );

            connection
                .write_message(FrontendMessage::MD5HashedPassword(MD5Hash(hash)).into())
                .await?;

            let response = connection.read_backend_message().await?;

            match response {
                BackendMessage::AuthenticationOk => {}
                _ => return Err(anyhow::anyhow!("Expected AuthenticationOk")),
            }
        }
        BackendMessage::AuthenticationOk => {}
        _ => unimplemented!(),
    }

    loop {
        let response = connection.read_backend_message().await?;

        match response {
            BackendMessage::ReadyForQuery => break,
            BackendMessage::ParameterStatus(_) => {
                // TODO: Handle this
            }
            BackendMessage::BackendKeyData(_) => {
                // TODO: Handle this
            }
            _ => unimplemented!("Unexpected message"),
        }
    }

    Ok(connection)
}

#[derive(Debug)]
enum ClientOperation {
    Parse,
    Bind { statement: String, portal: String },
    Describe { statement: String },
    Execute { portal: String },
}

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
    pub async fn new(
        target_config: TargetConfig,
        max_pool_size: usize,
    ) -> Result<PostgresResolver> {
        let manager = Manager { target_config };
        let pool = Pool::new(manager, max_pool_size);

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

#[async_trait]
impl Resolver for PostgresResolver {
    async fn query(&mut self, client_id: ClientId, query: String) -> Result<RecordBatch> {
        let connection = self.active_connections.entry(client_id).or_insert({
            let connection = self.pool.get().map_err(|err| anyhow::anyhow!(err)).await?;
            ActiveConnection::new(connection)
        });

        connection
            .connection
            .write_message(FrontendMessage::SimpleQuery(query).into())
            .await?;

        let mut fields = vec![];
        let mut data_rows = vec![];
        loop {
            let response = connection.connection.read_backend_message().await?;
            match response {
                BackendMessage::ReadyForQuery => break,
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

    async fn parse(&mut self, client_id: ClientId, parse: Parse) -> Result<()> {
        let connection = self.active_connections.entry(client_id).or_insert({
            let connection = self.pool.get().map_err(|err| anyhow::anyhow!(err)).await?;
            ActiveConnection::new(connection)
        });

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

    async fn describe(&mut self, client_id: ClientId, describe: Describe) -> Result<()> {
        let connection = self.active_connections.entry(client_id).or_insert({
            let connection = self.pool.get().map_err(|err| anyhow::anyhow!(err)).await?;
            ActiveConnection::new(connection)
        });

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

    async fn bind(&mut self, client_id: ClientId, bind: Bind) -> Result<()> {
        let connection = self.active_connections.entry(client_id).or_insert({
            let connection = self.pool.get().map_err(|err| anyhow::anyhow!(err)).await?;
            ActiveConnection::new(connection)
        });

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

    async fn execute(&mut self, client_id: ClientId, execute: Execute) -> Result<()> {
        let connection = self.active_connections.entry(client_id).or_insert({
            let connection = self.pool.get().map_err(|err| anyhow::anyhow!(err)).await?;
            ActiveConnection::new(connection)
        });

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

    async fn sync(&mut self, client_id: ClientId) -> Result<Vec<SyncResponse>> {
        let connection = self.active_connections.entry(client_id).or_insert({
            let connection = self.pool.get().map_err(|err| anyhow::anyhow!(err)).await?;
            ActiveConnection::new(connection)
        });

        connection
            .connection
            .write_message(FrontendMessage::Sync.into())
            .await?;

        let mut responses = vec![];
        while let Some(operation) = &connection.requested_ops.pop_front() {
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
                            let schema = protocol_fields_to_schema(&fields);

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
                                command_complete_tag = tag;
                                break;
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

                    responses.push(SyncResponse::CommandComplete(command_complete_tag));
                }
            }
        }

        let read_message = connection.connection.read_backend_message().await?;
        match read_message {
            BackendMessage::ReadyForQuery => SyncResponse::ReadyForQuery,
            _ => todo!(),
        };
        responses.push(SyncResponse::ReadyForQuery);

        Ok(responses)
    }

    async fn close(&mut self, client_id: ClientId, close: Close) -> Result<()> {
        let connection = self.active_connections.entry(client_id).or_insert({
            let connection = self.pool.get().map_err(|err| anyhow::anyhow!(err)).await?;
            ActiveConnection::new(connection)
        });

        connection
            .connection
            .write_message(FrontendMessage::Close(close).into())
            .await?;

        let _read_message = connection.connection.read_backend_message().await?;
        // TODO: Handle response

        Ok(())
    }

    async fn initialize(&mut self, _client_id: ClientId) -> Result<()> {
        Ok(())
    }

    async fn terminate(&mut self, client_id: ClientId) -> Result<()> {
        self.terminate_connection(client_id);

        Ok(())
    }
}
