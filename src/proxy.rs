use crate::{
    connection::{Connection, MaybeTlsStream, ProtocolStream},
    postgres_protocol::{Message, StartupMessage},
    util::encode_md5_password_hash,
    Resolver,
};
use anyhow::Result;
use native_tls::Identity;
use rand::Rng;
use std::{collections::HashMap, fs::File, io::Read};
use tokio::{io::AsyncWriteExt, net::TcpListener};
use tracing::{info, trace_span, Instrument};
use uuid::Uuid;

#[derive(Clone)]
pub struct TlsConfig {
    pub pcks_path: String,
    pub password: String,
}

#[derive(Clone)]
pub struct Config {
    pub tls_config: Option<TlsConfig>,
    pub credentials: HashMap<String, String>,
}

pub struct Proxy {
    config: Config,
    resolver: Box<dyn Resolver>,
}

impl Proxy {
    pub async fn listen(&mut self, listener: TcpListener) -> Result<()> {
        info!("Listening on: {}", &listener.local_addr().unwrap());

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
            let (stream, client_addr) = listener.accept().await?;
            let client_id = Uuid::new_v4();

            let span =
                trace_span!("connection", client.addr = %client_addr, client.id = %client_id);

            info!(parent: &span, "connection established");

            let mut frontend_connection = accept_frontend_connection(stream, &tls_acceptor)
                .instrument(tracing::info_span!(
                    parent: &span,
                    "accept_frontend_connection"
                ))
                .await?;

            handle_authentication(&mut frontend_connection, &self.config.credentials)
                .instrument(tracing::info_span!(parent: &span, "handle_authentication"))
                .await?;

            handle_connection(client_id, &mut frontend_connection, &mut self.resolver)
                .instrument(span)
                .await?;
        }
    }

    pub fn new(config: Config, resolver: Box<dyn Resolver>) -> Proxy {
        Proxy { config, resolver }
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
        Message::MD5HashedPassword { hash } => hash,
        _ => return Err(anyhow::anyhow!("Expected Password Message")),
    };

    let user = frontend
        .parameters
        .get("user")
        .expect("Missing user parameter")
        .clone();

    let password = credentials
        .get(&user.clone())
        .unwrap_or_else(|| panic!("Password for {} not found inside config", &user));

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

    let frontend = Connection::new(frontend, frontend_params);

    Ok(frontend)
}

pub async fn handle_connection(
    client_id: Uuid,
    frontend: &mut Connection,
    resolver: &mut Box<dyn Resolver>,
) -> Result<()> {
    resolver.initialize(client_id).await?;

    loop {
        let request = frontend.read_message().await?;

        match request {
            Message::Terminate => {
                async {
                    resolver
                        .terminate(client_id)
                        .instrument(tracing::trace_span!("resolver"))
                        .await?;

                    Ok::<(), anyhow::Error>(())
                }
                .instrument(tracing::trace_span!("terminate"))
                .await?;

                break;
            }
            Message::SimpleQuery(query) => {
                async {
                    let result = resolver
                        .query(client_id, query)
                        .instrument(tracing::trace_span!("resolver"))
                        .await?;

                    frontend.write_data(result).await?;

                    // TODO: Fix the command complete tag
                    frontend
                        .write_message(Message::CommandComplete {
                            tag: "C".to_string(),
                        })
                        .await?;

                    frontend.write_message(Message::ReadyForQuery).await?;

                    Ok::<(), anyhow::Error>(())
                }
                .instrument(tracing::trace_span!("query"))
                .await?;
            }
            Message::Parse {
                statement_name,
                query,
                param_types,
            } => {
                async {
                    resolver
                        .parse(client_id, statement_name, query, param_types)
                        .instrument(tracing::trace_span!("resolver"))
                        .await?;

                    Ok::<(), anyhow::Error>(())
                }
                .instrument(tracing::trace_span!("parse"))
                .await?;
            }
            Message::Describe { kind, name } => {
                async {
                    resolver
                        .describe(client_id, kind, name)
                        .instrument(tracing::trace_span!("resolver"))
                        .await?;

                    Ok::<(), anyhow::Error>(())
                }
                .instrument(tracing::trace_span!("describe"))
                .await?;
            }
            Message::Bind {
                statement,
                portal,
                params,
                formats,
                results,
            } => {
                async {
                    resolver
                        .bind(client_id, statement, portal, params, formats, results)
                        .instrument(tracing::trace_span!("resolver"))
                        .await?;

                    Ok::<(), anyhow::Error>(())
                }
                .instrument(tracing::trace_span!("bind"))
                .await?;
            }
            Message::Execute { portal, row_limit } => {
                async {
                    resolver
                        .execute(client_id, portal, row_limit)
                        .instrument(tracing::trace_span!("resolver"))
                        .await?;

                    Ok::<(), anyhow::Error>(())
                }
                .instrument(tracing::trace_span!("execute"))
                .await?;
            }
            Message::Sync => {
                async {
                    let messages = resolver
                        .sync(client_id)
                        .instrument(tracing::trace_span!("resolver"))
                        .await?;

                    for message in messages {
                        frontend.write_message(message).await?;
                    }

                    Ok::<(), anyhow::Error>(())
                }
                .instrument(tracing::trace_span!("sync"))
                .await?;
            }
            Message::Close { kind, name } => {
                async {
                    resolver
                        .close(client_id, kind, name)
                        .instrument(tracing::trace_span!("resolver"))
                        .await?;

                    frontend.write_message(Message::CloseComplete).await?;

                    Ok::<(), anyhow::Error>(())
                }
                .instrument(tracing::trace_span!("close"))
                .await?;
            }
            _ => unimplemented!(),
        }
    }

    Ok(())
}
