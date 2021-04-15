use super::config::Config;
use super::connection::{Connection, ConnectionKind};
use super::data::SimpleQueryResponse;
use super::util::encode_md5_password_hash;
use crate::{
    protocol::{Message, StartupMessage},
    Transformer,
};
use anyhow::Result;
use std::collections::HashMap;
use tokio::net::TcpListener;

pub struct App {
    config: Config,
    transformers: Vec<Box<dyn Transformer>>,
}

impl App {
    pub async fn listen(&self, address: &str) -> Result<()> {
        let listener = TcpListener::bind(&address).await?;
        println!("Server running on {}!", &address);

        loop {
            let (stream, _) = listener.accept().await?;
            let mut frontend_connection = Connection::new(stream, ConnectionKind::Frontend);

            println!("New connection established!");

            let mut backend_connection =
                setup_tunnel(&mut frontend_connection, self.config.clone()).await?;

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

                    let transformed = SimpleQueryResponse {
                        data: transformers.first().unwrap().transform(&response.data),
                        fields: response.fields,
                        tag: response.tag,
                    };

                    let messages = transformed.serialize().await?;

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
