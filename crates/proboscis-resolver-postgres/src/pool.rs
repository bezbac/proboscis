use crate::target_config::TargetConfig;
use async_trait::async_trait;
use deadpool::managed::RecycleResult;
use proboscis_core::{
    resolver::ResolveError,
    utils::connection::{Connection, MaybeTlsStream},
    utils::password::encode_md5_password_hash,
};
use proboscis_postgres_protocol::{
    message::{BackendMessage, FrontendMessage, MD5Hash, MD5Salt},
    StartupMessage,
};
use std::collections::HashMap;

pub type Pool = deadpool::managed::Pool<Manager>;

#[derive(Debug)]
pub struct Manager {
    target_config: TargetConfig,
}

impl Manager {
    pub fn new(target_config: TargetConfig) -> Self {
        Self { target_config }
    }
}

#[async_trait]
impl deadpool::managed::Manager for Manager {
    type Type = Connection;
    type Error = ResolveError;

    async fn create(&self) -> Result<Connection, ResolveError> {
        establish_connection(&self.target_config).await
    }

    async fn recycle(&self, _conn: &mut Connection) -> RecycleResult<ResolveError> {
        Ok(())
    }
}

pub async fn establish_connection(
    target_config: &TargetConfig,
) -> Result<Connection, ResolveError> {
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
                _ => {
                    return Err(ResolveError::Other(anyhow::anyhow!(
                        "Expected AuthenticationOk"
                    )))
                }
            }
        }
        BackendMessage::AuthenticationOk => {}
        _ => unimplemented!(),
    }

    loop {
        let response = connection.read_backend_message().await?;

        match response {
            BackendMessage::ReadyForQuery(_) => break,
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
