use super::{config::TargetConfig, connection::Connection};
use crate::{
    protocol::{Message, StartupMessage},
    proxy::{
        connection::{ConnectionKind, MaybeTlsStream, ProtocolStream},
        util::encode_md5_password_hash,
    },
};
use anyhow::Error;
use anyhow::Result;
use async_trait::async_trait;
use std::collections::HashMap;

pub struct ConnectionManager {
    target_config: TargetConfig,
}

impl ConnectionManager {
    pub fn new(target_config: TargetConfig) -> ConnectionManager {
        ConnectionManager { target_config }
    }
}

pub type ConnectionPool = deadpool::managed::Pool<Connection, Error>;

#[async_trait]
impl deadpool::managed::Manager<Connection, Error> for ConnectionManager {
    async fn create(&self) -> Result<Connection, Error> {
        let stream = tokio::net::TcpStream::connect(&format!(
            "{}:{}",
            self.target_config.host, self.target_config.port
        ))
        .await?;

        let mut params: HashMap<String, String> = HashMap::new();
        params.insert("user".to_string(), self.target_config.user.clone());
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
                let hash = encode_md5_password_hash(
                    &self.target_config.user,
                    &self.target_config.password,
                    &salt[..],
                );

                connection
                    .write_message(Message::MD5HashedPasswordMessage { hash })
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

    async fn recycle(&self, _: &mut Connection) -> deadpool::managed::RecycleResult<Error> {
        Ok(())
    }
}
