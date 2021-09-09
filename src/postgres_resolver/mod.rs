use std::collections::HashMap;

use anyhow::Result;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use uuid::Uuid;

use crate::{
    arrow::simple_query_response_to_record_batch,
    connection::{Connection, ConnectionKind, MaybeTlsStream, ProtocolStream},
    postgres_protocol::{Message, StartupMessage},
    util::encode_md5_password_hash,
    Resolver,
};

#[derive(Clone)]
pub struct TargetConfig {
    pub host: String,
    pub port: String,
    pub database: String,
    pub user: String,
    pub password: String,
}

pub async fn establish_connection(target_config: &TargetConfig) -> Result<Connection> {
    let stream =
        tokio::net::TcpStream::connect(&format!("{}:{}", target_config.host, target_config.port))
            .await?;

    let mut params: HashMap<String, String> = HashMap::new();
    params.insert("user".to_string(), target_config.user.clone());
    params.insert("client_encoding".to_string(), "UTF8".to_string());

    let mut connection = Connection::new(
        MaybeTlsStream::Left(stream),
        ConnectionKind::Backend,
        params.clone(),
    );

    connection
        .write_startup_message(StartupMessage::Startup { params })
        .await?;

    let response = connection.read_message().await?;
    match response {
        Message::AuthenticationRequestMD5Password { salt } => {
            let hash =
                encode_md5_password_hash(&target_config.user, &target_config.password, &salt[..]);

            connection
                .write_message(Message::MD5HashedPassword { hash })
                .await?;

            let response = connection.read_message().await?;

            match response {
                Message::AuthenticationOk => {}
                _ => return Err(anyhow::anyhow!("Expected AuthenticationOk")),
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
        }
        _ => unimplemented!(),
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

struct ConnectionWithState {
    connection: Connection,
    requested_ops: Vec<ClientOperation>,
}

pub struct PostgresResolver {
    target_config: TargetConfig,
    connections: HashMap<Uuid, ConnectionWithState>,
}

impl PostgresResolver {
    pub async fn new(target_config: TargetConfig) -> Result<PostgresResolver> {
        Ok(PostgresResolver {
            target_config,
            connections: HashMap::new(),
        })
    }
}

#[async_trait]
impl Resolver for PostgresResolver {
    async fn query(&mut self, client_id: Uuid, query: String) -> Result<RecordBatch> {
        let connection = self.connections.get_mut(&client_id).unwrap();

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
        let connection = self.connections.get_mut(&client_id).unwrap();

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
        let connection = self.connections.get_mut(&client_id).unwrap();

        connection
            .connection
            .write_message(Message::Describe {
                kind: kind.clone(),
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
        let connection = self.connections.get_mut(&client_id).unwrap();

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
        let connection = self.connections.get_mut(&client_id).unwrap();

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
        let connection = self.connections.get_mut(&client_id).unwrap();

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

    async fn initialize(&mut self, client_id: Uuid) -> Result<()> {
        let db = establish_connection(&self.target_config).await?;

        let connection = ConnectionWithState {
            connection: db,
            requested_ops: vec![],
        };

        self.connections.insert(client_id, connection);

        Ok(())
    }

    async fn terminate(&mut self, client_id: Uuid) -> Result<()> {
        let connection = self.connections.get_mut(&client_id).unwrap();

        connection
            .connection
            .write_message(Message::Terminate)
            .await?;

        self.connections.remove(&client_id);

        Ok(())
    }
}
