mod target_config;
use anyhow::Result;
use arrow::{datatypes::Schema, record_batch::RecordBatch};
use async_trait::async_trait;
use deadpool::managed::RecycleResult;
use futures::TryFutureExt;
use postgres_protocol::{
    message::{
        Bind, Close, CommandCompleteTag, DataRow, Describe, Execute, MD5Hash, MD5Salt, Parse,
        RowDescription,
    },
    Message, StartupMessage,
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

    let response = connection.read_message().await?;
    match response {
        Message::AuthenticationRequestMD5Password(MD5Salt(salt)) => {
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
                .write_message(Message::MD5HashedPassword(MD5Hash(hash)))
                .await?;

            let response = connection.read_message().await?;

            match response {
                Message::AuthenticationOk => {}
                _ => return Err(anyhow::anyhow!("Expected AuthenticationOk")),
            }
        }
        Message::AuthenticationOk => {}
        _ => unimplemented!(),
    }

    loop {
        let response = connection.read_message().await?;

        match response {
            Message::ReadyForQuery => break,
            Message::ParameterStatus(_) => {
                // TODO: Handle this
            }
            Message::BackendKeyData(_) => {
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
    Bind,
    Describe,
    Execute,
}

struct ActiveConnection {
    connection: deadpool::managed::Object<Manager>,
    requested_ops: Vec<ClientOperation>,
}

pub struct PostgresResolver {
    // Active connections are remove from the pool.
    // To add them back to the pool, drop them.
    active_connections: HashMap<ClientId, ActiveConnection>,
    pool: Pool,

    schema_cache: HashMap<ClientId, VecDeque<Schema>>,
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
            schema_cache: HashMap::new(),
        })
    }

    async fn get_connection(&mut self, client_id: ClientId) -> Result<&mut ActiveConnection> {
        Ok(self.active_connections.entry(client_id).or_insert({
            let connection = self.pool.get().map_err(|err| anyhow::anyhow!(err)).await?;

            ActiveConnection {
                connection,
                requested_ops: vec![],
            }
        }))
    }

    fn terminate_connection(&mut self, client_id: ClientId) {
        self.active_connections.remove(&client_id);
    }
}

#[async_trait]
impl Resolver for PostgresResolver {
    async fn query(&mut self, client_id: ClientId, query: String) -> Result<RecordBatch> {
        let connection = self.get_connection(client_id).await?;

        connection
            .connection
            .write_message(Message::SimpleQuery(query))
            .await?;

        let mut fields = vec![];
        let mut data_rows = vec![];
        loop {
            let response = connection.connection.read_message().await?;
            match response {
                Message::ReadyForQuery => break,
                Message::RowDescription(RowDescription {
                    fields: mut message_fields,
                }) => fields.append(&mut message_fields),
                Message::DataRow(data_row) => {
                    data_rows.push(data_row);
                }
                Message::CommandComplete(CommandCompleteTag(_)) => {
                    // TODO: Handle this
                }
                _ => unimplemented!(""),
            }
        }

        let data = simple_query_response_to_record_batch(&fields, &data_rows)?;

        Ok(data)
    }

    async fn parse(&mut self, client_id: ClientId, parse: Parse) -> Result<()> {
        let connection = self.get_connection(client_id).await?;

        connection
            .connection
            .write_message(Message::Parse(parse))
            .await?;

        connection.requested_ops.push(ClientOperation::Parse);

        Ok(())
    }

    async fn describe(&mut self, client_id: ClientId, describe: Describe) -> Result<()> {
        let connection = self.get_connection(client_id).await?;

        connection
            .connection
            .write_message(Message::Describe(describe))
            .await?;

        connection.requested_ops.push(ClientOperation::Describe);

        Ok(())
    }

    async fn bind(&mut self, client_id: ClientId, bind: Bind) -> Result<()> {
        let connection = self.get_connection(client_id).await?;

        connection
            .connection
            .write_message(Message::Bind(bind))
            .await?;

        connection.requested_ops.push(ClientOperation::Bind);

        Ok(())
    }

    async fn execute(&mut self, client_id: ClientId, execute: Execute) -> Result<()> {
        let connection = self.get_connection(client_id).await?;

        connection
            .connection
            .write_message(Message::Execute(execute))
            .await?;

        connection.requested_ops.push(ClientOperation::Execute);

        Ok(())
    }

    async fn sync(&mut self, client_id: ClientId) -> Result<Vec<SyncResponse>> {
        let mut schema_cache = self.schema_cache.entry(client_id).or_default().clone();

        let mut connection = self.get_connection(client_id).await?;

        connection.connection.write_message(Message::Sync).await?;

        let mut responses = vec![];
        for operation in &connection.requested_ops {
            match operation {
                ClientOperation::Parse => {
                    let read_message = connection.connection.read_message().await?;

                    match read_message {
                        Message::ParseComplete => responses.push(SyncResponse::ParseComplete),
                        _ => todo!(),
                    }
                }
                ClientOperation::Describe => loop {
                    let read_message = connection.connection.read_message().await?;

                    match read_message {
                        Message::RowDescription(RowDescription { fields }) => {
                            let schema = protocol_fields_to_schema(&fields);

                            responses.push(SyncResponse::Schema(schema.clone()));

                            schema_cache.push_back(schema);

                            break;
                        }
                        Message::ParameterDescription(parameter_description) => responses
                            .push(SyncResponse::ParameterDescription(parameter_description)),
                        _ => todo!(),
                    }
                },
                ClientOperation::Bind => {
                    let read_message = connection.connection.read_message().await?;

                    match read_message {
                        Message::BindComplete => responses.push(SyncResponse::BindComplete),
                        _ => todo!(),
                    }
                }
                ClientOperation::Execute => {
                    let mut data_rows: Vec<DataRow> = vec![];
                    let command_complete_tag;

                    loop {
                        let read_message = connection.connection.read_message().await?;

                        match read_message {
                            Message::DataRow(data_row) => data_rows.push(data_row),
                            Message::CommandComplete(tag) => {
                                command_complete_tag = tag;
                                break;
                            }
                            _ => todo!(),
                        }
                    }

                    let schema = schema_cache.pop_front();

                    match schema {
                        Some(schema) => {
                            let RowDescription { fields } =
                                serialize_record_batch_schema_to_row_description(&schema);

                            let record_batch =
                                simple_query_response_to_record_batch(&fields, &data_rows)?;

                            responses.push(SyncResponse::Records(record_batch))
                        }
                        _ => todo!(),
                    }

                    responses.push(SyncResponse::CommandComplete(command_complete_tag));
                }
            }
        }

        let read_message = connection.connection.read_message().await?;
        match read_message {
            Message::ReadyForQuery => SyncResponse::ReadyForQuery,
            _ => todo!(),
        };
        responses.push(SyncResponse::ReadyForQuery);

        connection.requested_ops = vec![];

        self.schema_cache.insert(client_id, schema_cache);

        Ok(responses)
    }

    async fn close(&mut self, client_id: ClientId, close: Close) -> Result<()> {
        let connection = self.get_connection(client_id).await?;

        connection
            .connection
            .write_message(Message::Close(close))
            .await?;

        let _read_message = connection.connection.read_message().await?;
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
