use super::config::Config;
use super::connection::{Connection, ConnectionKind};
use super::util::encode_md5_password_hash;
use crate::protocol::{Message, StartupMessage};
use anyhow::Result;
use std::collections::HashMap;
use tokio::net::TcpListener;

pub struct App {
    config: Config,
}

impl App {
    pub async fn listen(&self, address: &str) -> Result<()> {
        let listener = TcpListener::bind(&address).await?;
        println!("Server running on {}!", &address);

        loop {
            let (stream, _) = listener.accept().await?;
            let frontend_connection = Connection::new(stream, ConnectionKind::Frontend);
            handle_connection(frontend_connection, self.config.clone()).await?;
        }
    }

    pub fn new(config: Config) -> App {
        App { config }
    }
}

pub async fn setup_tunnel(frontend: &mut Connection, config: Config) -> Result<Connection> {
    let startup_message = frontend.read_startup_message().await?;

    let frontend_params = match startup_message {
        StartupMessage::Startup { params } => params,
        _ => return Err(anyhow::anyhow!("Didn't recieve startup message")),
    };

    let user = frontend_params
        .get("user")
        .expect("Missing user parameter")
        .clone();

    let mut backend = Connection::connect(&config.target_addr, ConnectionKind::Backend).await?;

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
            let password = config
                .authentication
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

    return Ok(backend);
}

pub async fn handle_connection(mut frontend: Connection, config: Config) -> Result<()> {
    println!("New connection established!");

    let mut backend = setup_tunnel(&mut frontend, config).await?;

    'connection: loop {
        let request = frontend.read_message().await?;

        match request {
            Message::Terminate => break,
            Message::SimpleQuery(query_string) => {
                backend
                    .write_message(Message::SimpleQuery(query_string))
                    .await?;

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
                                            .write_message(Message::ParameterDescription {
                                                param_types,
                                            })
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
                        Message::Terminate => {
                            backend.write_message(Message::Terminate).await?;
                            break 'connection;
                        }

                        _ => unimplemented!(""),
                    }
                }
            }
            _ => unimplemented!(),
        }
    }

    Ok(())
}
