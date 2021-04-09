use super::stream_wrapper::StreamWrapperKind;
use super::util::encode_md5_password_hash;
use super::{config::Config, stream_wrapper::StreamWrapper};
use crate::protocol::{Message, StartupMessage};
use anyhow::Result;
use std::collections::HashMap;
use std::net::{TcpListener, TcpStream};

pub struct App {
    config: Config,
}

impl App {
    pub fn listen(&self, address: &str) -> Result<()> {
        let listener = TcpListener::bind(&address).unwrap();
        println!("Server running on {}!", &address);

        for stream in listener.incoming() {
            let stream = stream.unwrap();
            let wrapper = StreamWrapper::new(stream, StreamWrapperKind::Frontend);
            handle_connection(wrapper, self.config.clone())?;
        }

        Ok(())
    }

    pub fn new(config: Config) -> App {
        App { config }
    }
}

pub fn setup_tunnel(
    frontend: &mut StreamWrapper,
    config: Config,
) -> Result<StreamWrapper, anyhow::Error> {
    let startup_message = frontend.read_startup_message()?;

    let frontend_params = match startup_message {
        StartupMessage::Startup { params } => params,
        _ => return Err(anyhow::anyhow!("Didn't recieve startup message")),
    };

    let user = frontend_params
        .get("user")
        .expect("Missing user parameter")
        .clone();

    let backend_stream =
        TcpStream::connect(&config.target_addr).expect("Connecting to backend failed");
    let mut backend = StreamWrapper::new(backend_stream, StreamWrapperKind::Backend);

    let mut backend_params: HashMap<String, String> = HashMap::new();
    backend_params.insert("user".to_string(), user.clone());
    backend_params.insert("client_encoding".to_string(), "UTF8".to_string());

    backend.write_startup_message(StartupMessage::Startup {
        params: backend_params,
    })?;

    let response = backend.read_message()?;
    match response {
        Message::AuthenticationRequestMD5Password { salt } => {
            let password = config
                .authentication
                .get(&user.clone())
                .expect(&format!("Password for {} not found inside config", &user));

            frontend
                .write_message(Message::AuthenticationRequestMD5Password { salt: salt.clone() })?;
            let frontend_response = frontend.read_message()?;

            let frontend_hash = match frontend_response {
                Message::MD5HashedPasswordMessage { hash } => hash,
                _ => return Err(anyhow::anyhow!("Expected Password Message")),
            };

            let hash = encode_md5_password_hash(&user, password, &salt[..]);

            if frontend_hash != hash {
                return Err(anyhow::anyhow!("Incorrect password"));
            }

            let message = Message::MD5HashedPasswordMessage { hash };
            backend.write_message(message)?;
            let response = backend.read_message()?;

            match response {
                Message::AuthenticationOk => {}
                _ => return Err(anyhow::anyhow!("Expected AuthenticationOk")),
            }

            frontend.write_message(Message::AuthenticationOk)?;

            loop {
                let response = backend.read_message()?;

                match response {
                    Message::ReadyForQuery => break,
                    Message::ParameterStatus { key, value } => {
                        frontend.write_message(Message::ParameterStatus { key, value })?;
                    }
                    Message::BackendKeyData {
                        process_id,
                        secret_key,
                        additional,
                    } => {
                        frontend.write_message(Message::BackendKeyData {
                            process_id,
                            secret_key,
                            additional,
                        })?;
                    }
                    _ => unimplemented!("Unexpected message"),
                }
            }
        }
        _ => unimplemented!(),
    }

    frontend.write_message(Message::ReadyForQuery)?;

    return Ok(backend);
}

pub fn handle_connection(mut frontend: StreamWrapper, config: Config) -> Result<(), anyhow::Error> {
    println!("New connection established!");

    let mut backend = setup_tunnel(&mut frontend, config)?;

    'connection: loop {
        let request = frontend.read_message()?;

        match request {
            Message::Terminate => break,
            Message::SimpleQuery(query_string) => {
                backend.write_message(Message::SimpleQuery(query_string))?;

                loop {
                    let response = backend.read_message()?;
                    match response {
                        Message::ReadyForQuery => break,
                        Message::RowDescription { fields } => {
                            frontend.write_message(Message::RowDescription { fields })?;
                        }
                        Message::DataRow { field_data } => {
                            frontend.write_message(Message::DataRow { field_data })?;
                        }
                        Message::CommandComplete { tag } => {
                            frontend.write_message(Message::CommandComplete { tag })?;
                        }
                        _ => unimplemented!(""),
                    }
                }

                frontend.write_message(Message::ReadyForQuery)?;
            }
            Message::Parse {
                query,
                param_types,
                statement,
            } => {
                backend.write_message(Message::Parse {
                    query,
                    param_types,
                    statement,
                })?;

                let mut stage = "parse";

                loop {
                    let request = frontend.read_message()?;

                    println!("{}", stage);

                    match request {
                        Message::Describe { kind, name } => {
                            backend.write_message(Message::Describe { kind, name })?;
                        }
                        Message::Sync => {
                            backend.write_message(Message::Sync)?;

                            loop {
                                let response = backend.read_message()?;

                                match response {
                                    Message::ParseComplete => {
                                        frontend.write_message(Message::ParseComplete)?;
                                    }
                                    Message::ParameterDescription { param_types } => {
                                        frontend.write_message(Message::ParameterDescription {
                                            param_types,
                                        })?;
                                    }
                                    Message::ReadyForQuery => {
                                        frontend.write_message(Message::ReadyForQuery)?;
                                        stage = "bind";
                                        break;
                                    }
                                    Message::RowDescription { fields } => {
                                        frontend
                                            .write_message(Message::RowDescription { fields })?;
                                    }
                                    Message::BindComplete => {
                                        frontend.write_message(Message::BindComplete)?;
                                    }
                                    Message::DataRow { field_data } => {
                                        frontend.write_message(Message::DataRow { field_data })?;
                                    }
                                    Message::CommandComplete { tag } => {
                                        frontend.write_message(Message::CommandComplete { tag })?;
                                    }
                                    Message::CloseComplete => {
                                        frontend.write_message(Message::CloseComplete)?;
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
                            backend.write_message(Message::Bind {
                                portal,
                                statement,
                                params,
                                formats,
                                results,
                            })?;
                        }
                        Message::Execute { portal, row_limit } => {
                            backend.write_message(Message::Execute { portal, row_limit })?;
                        }
                        Message::CommandComplete { tag } => {
                            backend.write_message(Message::CommandComplete { tag })?;
                        }
                        Message::Terminate => {
                            backend.write_message(Message::Terminate)?;
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
