use super::connection::MaybeTlsStream;
use super::connection::{Connection, ConnectionKind};
use crate::{connection::ProtocolStream, util::encode_md5_password_hash, Resolver};
use crate::{
    postgres_protocol::{Message, StartupMessage},
    Transformer,
};
use anyhow::Result;
use rand::Rng;
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;
use std::collections::HashMap;
use tokio::io::AsyncWriteExt;

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

pub async fn handle_connection(
    frontend: &mut Connection,
    resolver: &mut Box<dyn Resolver>,
    transformers: &Vec<Box<dyn Transformer>>,
) -> Result<()> {
    let mut parse_cache: HashMap<String, Vec<sqlparser::ast::Statement>> = HashMap::new();
    let mut describe_cache: HashMap<String, String> = HashMap::new();

    loop {
        let request = frontend.read_message().await?;

        match request {
            Message::Terminate => {
                break;
            }
            Message::SimpleQuery(query) => {
                let record_batch = resolver.query(&query).await?;

                resolver.inform(&query, record_batch.clone()).await;

                let transformed = transformers.iter().fold(record_batch, |data, transformer| {
                    transformer.transform(&data)
                });

                frontend.write_data(transformed).await?;

                // TODO: Fix the command complete tag
                frontend
                    .write_message(Message::CommandComplete {
                        tag: "C".to_string(),
                    })
                    .await?;
                frontend.write_message(Message::ReadyForQuery).await?;
            }
            Message::Parse {
                statement_name,
                query,
                param_types,
            } => {
                if !parse_cache.contains_key(&statement_name) {
                    let dialect = PostgreSqlDialect {};
                    let ast = Parser::parse_sql(&dialect, &query)?;
                    parse_cache.insert(statement_name, ast);
                }

                frontend.write_message(Message::ParseComplete).await?;
            }
            Message::Describe { kind, name } => {
                if !parse_cache.contains_key(&name) {
                    // TODO: Return real error
                    panic!("Statement not found");
                }

                unimplemented!();
            }
            _ => unimplemented!(),
        }
    }

    Ok(())
}
