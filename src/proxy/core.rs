use super::connection::{Connection, ConnectionKind};
use super::data::SimpleQueryResponse;
use super::util::encode_md5_password_hash;
use super::{config::Config, connection::MaybeTlsStream};
use crate::proxy::connection::ProtocolStream;
use crate::{
    protocol::{Message, StartupMessage},
    Transformer,
};
use anyhow::Result;
use native_tls::Identity;
use std::collections::HashMap;
use std::fs::File;
use std::io::Read;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;

pub struct App {
    config: Config,
    transformers: Vec<Box<dyn Transformer>>,
}

impl App {
    pub async fn listen(&self, address: &str) -> Result<()> {
        let listener = TcpListener::bind(&address).await?;
        println!("Server running on {}!", &address);

        let tls_acceptor: Option<tokio_native_tls::TlsAcceptor> = match &self.config.tls_config {
            Some(tls_config) => {
                let mut file = File::open(tls_config.pcks_path.clone())?;
                let mut identity = vec![];
                file.read_to_end(&mut identity).unwrap();

                let certificate =
                    Identity::from_pkcs12(&identity, tls_config.password.as_str()).unwrap();
                let acceptor = tokio_native_tls::TlsAcceptor::from(
                    native_tls::TlsAcceptor::builder(certificate).build()?,
                );

                Some(acceptor)
            }
            _ => None,
        };

        loop {
            let (stream, _) = listener.accept().await?;
            println!("New connection established!");

            let (mut frontend_connection, mut backend_connection) =
                setup_connections(stream, &self.config.target_addr, &tls_acceptor).await?;

            handle_authentication(
                &mut frontend_connection,
                &mut backend_connection,
                &self.config.credentials,
            )
            .await?;

            handle_connection(
                &mut frontend_connection,
                &mut backend_connection,
                &self.transformers,
            )
            .await?;
        }
    }

    pub fn new(config: Config) -> App {
        App {
            config,
            transformers: vec![],
        }
    }

    pub fn add_transformer(mut self, transformer: Box<dyn Transformer>) -> App {
        self.transformers.push(transformer);
        self
    }
}

pub async fn setup_connections(
    mut frontend_stream: tokio::net::TcpStream,
    target_addr: &String,
    tls_acceptor: &Option<tokio_native_tls::TlsAcceptor>,
) -> Result<(Connection, Connection)> {
    let mut startup_message = frontend_stream.read_startup_message().await?;

    let mut frontend: MaybeTlsStream;
    match startup_message {
        StartupMessage::SslRequest => {
            // TLS not supported
            if tls_acceptor.is_none() {
                frontend_stream.write(&[b'N']).await?;
                return Err(anyhow::anyhow!("TLS is not enabled on the server"));
            }

            let tls_acceptor = tls_acceptor.as_ref().unwrap();

            // Confirm TLS request
            frontend_stream.write(&[b'S']).await?;
            let tls_stream = tls_acceptor.accept(frontend_stream).await?;

            frontend = MaybeTlsStream::Right(tls_stream);
            startup_message = frontend.read_startup_message().await?;
        }
        _ => frontend = MaybeTlsStream::Left(frontend_stream),
    };

    let frontend_params = match startup_message {
        StartupMessage::Startup { params } => params,
        _ => panic!(""),
    };

    let frontend = Connection::new(frontend, ConnectionKind::Frontend, frontend_params.clone());

    let user = frontend_params
        .get("user")
        .expect("Missing user parameter")
        .clone();

    let mut backend_params: HashMap<String, String> = HashMap::new();
    backend_params.insert("user".to_string(), user.clone());
    backend_params.insert("client_encoding".to_string(), "UTF8".to_string());

    let mut backend_stream = tokio::net::TcpStream::connect(&target_addr).await?;
    backend_stream
        .write_startup_message(StartupMessage::Startup {
            params: backend_params.clone(),
        })
        .await?;

    let backend = Connection::new(
        MaybeTlsStream::Left(backend_stream),
        ConnectionKind::Backend,
        backend_params,
    );

    return Ok((frontend, backend));
}

pub async fn handle_authentication(
    frontend: &mut Connection,
    backend: &mut Connection,
    credentials: &HashMap<String, String>,
) -> Result<()> {
    let response = backend.read_message().await?;
    match response {
        Message::AuthenticationRequestMD5Password { salt } => {
            let user = frontend
                .parameters
                .get("user")
                .expect("Missing user parameter")
                .clone();

            let password = credentials
                .get(&user.clone())
                .expect(&format!("Password for {} not found inside config", &user));

            frontend
                .write_message(Message::AuthenticationRequestMD5Password { salt: salt.clone() })
                .await?;
            let frontend_response = frontend.read_message().await?;

            let frontend_hash = match frontend_response {
                Message::MD5HashedPasswordMessage { hash } => hash,
                _ => return Err(anyhow::anyhow!("Expected Password Message")),
            };

            let hash = encode_md5_password_hash(&user, password, &salt[..]);

            if frontend_hash != hash {
                return Err(anyhow::anyhow!("Incorrect password"));
            }

            let message = Message::MD5HashedPasswordMessage { hash };
            backend.write_message(message).await?;
            let response = backend.read_message().await?;

            match response {
                Message::AuthenticationOk => {}
                _ => return Err(anyhow::anyhow!("Expected AuthenticationOk")),
            }

            frontend.write_message(Message::AuthenticationOk).await?;

            loop {
                let response = backend.read_message().await?;

                match response {
                    Message::ReadyForQuery => break,
                    Message::ParameterStatus { key, value } => {
                        frontend
                            .write_message(Message::ParameterStatus { key, value })
                            .await?;
                    }
                    Message::BackendKeyData {
                        process_id,
                        secret_key,
                        additional,
                    } => {
                        frontend
                            .write_message(Message::BackendKeyData {
                                process_id,
                                secret_key,
                                additional,
                            })
                            .await?;
                    }
                    _ => unimplemented!("Unexpected message"),
                }
            }
        }
        _ => unimplemented!(),
    }

    frontend.write_message(Message::ReadyForQuery).await?;

    Ok(())
}

pub async fn pass_through_simple_query_response(
    frontend: &mut Connection,
    backend: &mut Connection,
) -> Result<()> {
    loop {
        let response = backend.read_message().await?;
        match response {
            Message::ReadyForQuery => break,
            Message::RowDescription { fields } => {
                frontend
                    .write_message(Message::RowDescription { fields })
                    .await?;
            }
            Message::DataRow { field_data } => {
                frontend
                    .write_message(Message::DataRow { field_data })
                    .await?;
            }
            Message::CommandComplete { tag } => {
                frontend
                    .write_message(Message::CommandComplete { tag })
                    .await?;
            }
            _ => unimplemented!(""),
        }
    }

    Ok(())
}

pub async fn pass_through_query_response(
    frontend: &mut Connection,
    backend: &mut Connection,
) -> Result<()> {
    loop {
        let request = frontend.read_message().await?;

        match request {
            Message::Describe { kind, name } => {
                backend
                    .write_message(Message::Describe { kind, name })
                    .await?;
            }
            Message::Sync => {
                backend.write_message(Message::Sync).await?;

                loop {
                    let response = backend.read_message().await?;

                    match response {
                        Message::ParseComplete => {
                            frontend.write_message(Message::ParseComplete).await?;
                        }
                        Message::ParameterDescription { param_types } => {
                            frontend
                                .write_message(Message::ParameterDescription { param_types })
                                .await?;
                        }
                        Message::ReadyForQuery => {
                            frontend.write_message(Message::ReadyForQuery).await?;
                            break;
                        }
                        Message::RowDescription { fields } => {
                            frontend
                                .write_message(Message::RowDescription { fields })
                                .await?;
                        }
                        Message::BindComplete => {
                            frontend.write_message(Message::BindComplete).await?;
                        }
                        Message::DataRow { field_data } => {
                            frontend
                                .write_message(Message::DataRow { field_data })
                                .await?;
                        }
                        Message::CommandComplete { tag } => {
                            frontend
                                .write_message(Message::CommandComplete { tag })
                                .await?;
                        }
                        Message::CloseComplete => {
                            frontend.write_message(Message::CloseComplete).await?;
                        }
                        _ => unimplemented!(""),
                    }
                }
            }
            Message::Bind {
                portal,
                statement,
                formats,
                params,
                results,
            } => {
                backend
                    .write_message(Message::Bind {
                        portal,
                        statement,
                        params,
                        formats,
                        results,
                    })
                    .await?;
            }
            Message::Execute { portal, row_limit } => {
                backend
                    .write_message(Message::Execute { portal, row_limit })
                    .await?;
            }
            Message::CommandComplete { tag } => {
                backend
                    .write_message(Message::CommandComplete { tag })
                    .await?;
            }
            _ => unimplemented!(),
        }
    }
}

pub async fn handle_connection(
    frontend: &mut Connection,
    backend: &mut Connection,
    transformers: &Vec<Box<dyn Transformer>>,
) -> Result<()> {
    loop {
        let request = frontend.read_message().await?;

        match request {
            Message::Terminate => {
                backend.write_message(Message::Terminate).await?;
                break;
            }
            Message::SimpleQuery(query_string) => {
                backend
                    .write_message(Message::SimpleQuery(query_string))
                    .await?;

                if transformers.len() == 0 {
                    pass_through_simple_query_response(frontend, backend).await?;
                } else {
                    let response = SimpleQueryResponse::read(backend).await?;
                    let messages = response.serialize(transformers).await?;

                    for message in messages {
                        frontend.write_message(message).await?;
                    }
                }

                frontend.write_message(Message::ReadyForQuery).await?;
            }
            Message::Parse {
                query,
                param_types,
                statement,
            } => {
                backend
                    .write_message(Message::Parse {
                        query,
                        param_types,
                        statement,
                    })
                    .await?;

                pass_through_query_response(frontend, backend).await?;
            }
            _ => unimplemented!(),
        }
    }

    Ok(())
}
