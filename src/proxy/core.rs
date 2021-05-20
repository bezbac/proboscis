use super::connection::{Connection, ConnectionKind};
use super::{config::Config, connection::MaybeTlsStream};
use crate::{
    protocol::{Message, StartupMessage},
    proxy::data::{
        serialize_record_batch_schema_to_row_description, serialize_record_batch_to_data_rows,
        simple_query_response_to_record_batch,
    },
    Transformer,
};
use crate::{
    proxy::{
        connection::ProtocolStream,
        connection_pool::{ConnectionManager, ConnectionPool},
        resolver::ResolverResult,
        util::encode_md5_password_hash,
    },
    Resolver,
};
use anyhow::Result;
use arrow::record_batch::RecordBatch;
use deadpool::managed::PoolConfig;
use futures::future::join_all;
use native_tls::Identity;
use rand::Rng;
use std::io::Read;
use std::{collections::HashMap, fs::File};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;

pub struct App {
    config: Config,
    transformers: Vec<Box<dyn Transformer>>,
    resolvers: Vec<Box<dyn Resolver>>,
}

impl App {
    pub async fn listen(&mut self, address: &str) -> Result<()> {
        let listener = TcpListener::bind(&address).await?;
        println!("Server running on {}!", &address);

        let manager = ConnectionManager::new(self.config.target_config.clone());

        let pool = ConnectionPool::from_config(manager, PoolConfig::new(100));

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

            let mut frontend_connection = accept_frontend_connection(stream, &tls_acceptor).await?;

            handle_authentication(&mut frontend_connection, &self.config.credentials).await?;

            handle_connection(
                &mut frontend_connection,
                &pool,
                &self.transformers,
                &mut self.resolvers,
            )
            .await?;
        }
    }

    pub fn new(config: Config) -> App {
        App {
            config,
            transformers: vec![],
            resolvers: vec![],
        }
    }

    pub fn add_transformer(mut self, transformer: Box<dyn Transformer>) -> App {
        self.transformers.push(transformer);
        self
    }

    pub fn add_resolver(mut self, resolver: Box<dyn Resolver>) -> App {
        self.resolvers.push(resolver);
        self
    }
}

pub async fn handle_authentication(
    frontend: &mut Connection,
    credentials: &HashMap<String, String>,
) -> Result<()> {
    let salt = rand::thread_rng().gen::<[u8; 4]>().to_vec();

    frontend
        .write_message(Message::AuthenticationRequestMD5Password { salt: salt.clone() })
        .await?;

    let response = frontend.read_message().await?;

    let received_hash = match response {
        Message::MD5HashedPasswordMessage { hash } => hash,
        _ => return Err(anyhow::anyhow!("Expected Password Message")),
    };

    let user = frontend
        .parameters
        .get("user")
        .expect("Missing user parameter")
        .clone();

    let password = credentials
        .get(&user.clone())
        .expect(&format!("Password for {} not found inside config", &user));

    let actual_hash = encode_md5_password_hash(&user, password, &salt[..]);

    if received_hash != actual_hash {
        return Err(anyhow::anyhow!("Incorrect password"));
    }

    frontend.write_message(Message::AuthenticationOk).await?;
    frontend.write_message(Message::ReadyForQuery).await?;

    Ok(())
}

pub async fn accept_frontend_connection(
    mut frontend_stream: tokio::net::TcpStream,
    tls_acceptor: &Option<tokio_native_tls::TlsAcceptor>,
) -> Result<Connection> {
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

    Ok(frontend)
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
    pool: &ConnectionPool,
    transformers: &Vec<Box<dyn Transformer>>,
    resolvers: &mut Vec<Box<dyn Resolver>>,
) -> Result<()> {
    loop {
        let request = frontend.read_message().await?;

        let mut backend = pool.get().await.map_err(|err| anyhow::anyhow!(err))?;

        match request {
            Message::Terminate => {
                backend.write_message(Message::Terminate).await?;
                break;
            }
            Message::SimpleQuery(query_string) => {
                if transformers.len() == 0 && resolvers.len() == 0 {
                    backend
                        .write_message(Message::SimpleQuery(query_string.clone()))
                        .await?;
                    pass_through_simple_query_response(frontend, &mut backend).await?;
                    frontend.write_message(Message::ReadyForQuery).await?;
                    continue;
                }

                let resolver_responses =
                    join_all(resolvers.iter().map(|r| r.lookup(&query_string))).await;

                let resolver_data: Vec<RecordBatch> = resolver_responses
                    .iter()
                    .filter_map(|result| match result {
                        ResolverResult::Hit(data) => Some(data),
                        ResolverResult::Miss => None,
                    })
                    .cloned()
                    .collect();

                let record_batch = {
                    if resolver_data.len() != 0 {
                        resolver_data.first().unwrap().clone()
                    } else {
                        backend
                            .write_message(Message::SimpleQuery(query_string.clone()))
                            .await?;

                        let mut fields = vec![];
                        let mut data_rows = vec![];
                        loop {
                            let response = backend.read_message().await?;
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

                        simple_query_response_to_record_batch(&fields, &data_rows).await?
                    }
                };

                join_all(
                    resolvers
                        .iter_mut()
                        .map(|r| r.inform(&query_string, record_batch.clone())),
                )
                .await;

                let transformed = transformers.iter().fold(record_batch, |data, transformer| {
                    transformer.transform_data(&data)
                });

                let row_description =
                    serialize_record_batch_schema_to_row_description(transformed.schema());
                frontend.write_message(row_description).await?;

                let data_rows = serialize_record_batch_to_data_rows(transformed);
                for message in data_rows {
                    frontend.write_message(message).await?;
                }

                // TODO: Fix the command complete tag
                frontend
                    .write_message(Message::CommandComplete {
                        tag: "C".to_string(),
                    })
                    .await?;
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

                pass_through_query_response(frontend, &mut backend).await?;
            }
            _ => unimplemented!(),
        }
    }

    Ok(())
}
