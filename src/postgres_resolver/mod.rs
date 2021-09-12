mod target_config;
pub use target_config::TargetConfig;

use crate::{
    arrow::simple_query_response_to_record_batch,
    connection::{Connection, MaybeTlsStream, ProtocolStream},
    postgres_protocol::{CloseKind, Message, StartupMessage},
    util::encode_md5_password_hash,
    Resolver,
};
use anyhow::Result;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use deadpool::managed::RecycleResult;
use futures::TryFutureExt;
use std::collections::HashMap;
use uuid::Uuid;

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
        Message::AuthenticationRequestMD5Password { salt } => {
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
                .write_message(Message::MD5HashedPassword { hash })
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
            Message::ParameterStatus { key: _, value: _ } => {
                // TODO: Handle this
            }
            Message::BackendKeyData {
                process_id: _,
                secret_key: _,
                additional: _,
            } => {
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
    active_connections: HashMap<Uuid, ActiveConnection>,
    pool: Pool,
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
        })
    }

    async fn get_connection(&mut self, client_id: Uuid) -> Result<&mut ActiveConnection> {
        Ok(self.active_connections.entry(client_id).or_insert({
            let connection = self.pool.get().map_err(|err| anyhow::anyhow!(err)).await?;

            ActiveConnection {
                connection,
                requested_ops: vec![],
            }
        }))
    }

    fn terminate_connection(&mut self, client_id: Uuid) {
        self.active_connections.remove(&client_id);
    }
}

#[async_trait]
impl Resolver for PostgresResolver {
    async fn query(&mut self, client_id: Uuid, query: String) -> Result<RecordBatch> {
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
                Message::RowDescription {
                    fields: mut message_fields,
                } => fields.append(&mut message_fields),
                Message::DataRow { field_data: _ } => {
                    data_rows.push(response);
                }
                Message::CommandComplete { tag: _ } => {}
                _ => unimplemented!(""),
            }
        }

        let data = simple_query_response_to_record_batch(&fields, &data_rows).await?;

        Ok(data)
    }

    async fn parse(
        &mut self,
        client_id: Uuid,
        statement_name: String,
        query: String,
        param_types: Vec<u32>,
    ) -> Result<()> {
        let connection = self.get_connection(client_id).await?;

        connection
            .connection
            .write_message(Message::Parse {
                statement_name: statement_name.clone(),
                query: query.clone(),
                param_types: param_types.clone(),
            })
            .await?;

        connection.requested_ops.push(ClientOperation::Parse);

        Ok(())
    }

    async fn describe(
        &mut self,
        client_id: Uuid,
        kind: crate::postgres_protocol::DescribeKind,
        name: String,
    ) -> Result<()> {
        let connection = self.get_connection(client_id).await?;

        connection
            .connection
            .write_message(Message::Describe {
                kind,
                name: name.clone(),
            })
            .await?;

        connection.requested_ops.push(ClientOperation::Describe);

        Ok(())
    }

    async fn bind(
        &mut self,
        client_id: Uuid,
        statement: String,
        portal: String,
        params: Vec<Vec<u8>>,
        formats: Vec<i16>,
        results: Vec<i16>,
    ) -> Result<()> {
        let connection = self.get_connection(client_id).await?;

        connection
            .connection
            .write_message(Message::Bind {
                statement: statement.clone(),
                portal: portal.clone(),
                params: params.clone(),
                formats: formats.clone(),
                results: results.clone(),
            })
            .await?;

        connection.requested_ops.push(ClientOperation::Bind);

        Ok(())
    }

    async fn execute(&mut self, client_id: Uuid, portal: String, row_limit: i32) -> Result<()> {
        let connection = self.get_connection(client_id).await?;

        connection
            .connection
            .write_message(Message::Execute {
                portal: portal.clone(),
                row_limit,
            })
            .await?;

        connection.requested_ops.push(ClientOperation::Execute);

        Ok(())
    }

    async fn sync(&mut self, client_id: Uuid) -> Result<Vec<Message>> {
        let mut connection = self.get_connection(client_id).await?;

        connection.connection.write_message(Message::Sync).await?;

        let mut messages = vec![];

        for operation in &connection.requested_ops {
            match operation {
                ClientOperation::Parse => {
                    let read_message = connection.connection.read_message().await?;

                    match read_message {
                        Message::ParseComplete => messages.push(read_message),
                        _ => todo!(),
                    }
                }
                ClientOperation::Describe => loop {
                    let read_message = connection.connection.read_message().await?;

                    match read_message {
                        Message::RowDescription { fields } => {
                            messages.push(Message::RowDescription { fields });
                            break;
                        }
                        Message::ParameterDescription { param_types } => {
                            messages.push(Message::ParameterDescription { param_types })
                        }
                        _ => todo!(),
                    }
                },
                ClientOperation::Bind => {
                    let read_message = connection.connection.read_message().await?;

                    match read_message {
                        Message::BindComplete => messages.push(read_message),
                        _ => todo!(),
                    }
                }
                ClientOperation::Execute => loop {
                    let read_message = connection.connection.read_message().await?;

                    match read_message {
                        Message::DataRow { field_data } => {
                            messages.push(Message::DataRow { field_data })
                        }
                        Message::CommandComplete { tag } => {
                            messages.push(Message::CommandComplete { tag });
                            break;
                        }
                        _ => todo!(),
                    }
                },
            }
        }

        let read_message = connection.connection.read_message().await?;

        match read_message {
            Message::ReadyForQuery => messages.push(read_message),
            _ => todo!(),
        }

        connection.requested_ops = vec![];

        Ok(messages)
    }

    async fn close(&mut self, client_id: Uuid, kind: CloseKind, name: String) -> Result<()> {
        let connection = self.get_connection(client_id).await?;

        connection
            .connection
            .write_message(Message::Close { kind, name })
            .await?;

        let _read_message = connection.connection.read_message().await?;
        // TODO: Handle response

        Ok(())
    }

    async fn initialize(&mut self, _client_id: Uuid) -> Result<()> {
        Ok(())
    }

    async fn terminate(&mut self, client_id: Uuid) -> Result<()> {
        self.terminate_connection(client_id);

        Ok(())
    }
}
