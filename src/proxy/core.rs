use super::connection::{Connection, ConnectionKind};
use super::data::SimpleQueryResponse;
use super::util::encode_md5_password_hash;
use super::{config::Config, connection::MaybeTlsStream};
use crate::{
    protocol::{Message, StartupMessage},
    Transformer,
};
use anyhow::Result;
use native_tls::Identity;
use std::collections::HashMap;
use std::fs::File;
use std::io::Read;
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

            let (mut frontend_connection, mut backend_connection) = setup_tunnel(
                stream,
                &self.config.target_addr,
                &self.config.credentials,
                &tls_acceptor,
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

pub async fn setup_tunnel(
    frontend_stream: tokio::net::TcpStream,
    target_addr: &String,
    credentials: &HashMap<String, String>,
    tls_acceptor: &Option<tokio_native_tls::TlsAcceptor>,
) -> Result<(Connection, Connection)> {
    let mut frontend = Connection::new(
        MaybeTlsStream::Left(frontend_stream),
        ConnectionKind::Frontend,
    );

    let frontend_params = loop {
        let startup_message = frontend.read_startup_message().await?;
        match startup_message {
            StartupMessage::Startup { params } => {
                break params;
            }
            StartupMessage::SslRequest => {
                match tls_acceptor {
                    Some(tls_acceptor) => {
                        // Confirm TLS request
                        frontend.write(&[b'S']).await?;

                        let tls_stream = tls_acceptor
                            .accept(match frontend.stream {
                                tokio_util::either::Either::Left(stream) => stream,
                                _ => panic!("Already tls stream"),
                            })
                            .await?;

                        frontend = Connection::new(
                            MaybeTlsStream::Right(tls_stream),
                            ConnectionKind::Frontend,
                        )
                    }
                    _ => {
                        // TLS not supported
                        frontend.write(&[b'N']).await?;
                    }
                }
            }
            _ => return Err(anyhow::anyhow!("Recieved unknown startup message")),
        }
    };

    let user = frontend_params
        .get("user")
        .expect("Missing user parameter")
        .clone();

    let mut backend = Connection::connect(&target_addr, ConnectionKind::Backend).await?;

    let mut backend_params: HashMap<String, String> = HashMap::new();
    backend_params.insert("user".to_string(), user.clone());
    backend_params.insert("client_encoding".to_string(), "UTF8".to_string());

    backend
        .write_startup_message(StartupMessage::Startup {
            params: backend_params,
        })
        .await?;

    let response = backend.read_message().await?;
    match response {
        Message::AuthenticationRequestMD5Password { salt } => {
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

    return Ok((frontend, backend));
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
