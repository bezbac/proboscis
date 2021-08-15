use super::connection::MaybeTlsStream;
use super::connection::{Connection, ConnectionKind};
use crate::{connection::ProtocolStream, util::encode_md5_password_hash, Resolver};
use crate::{
    data::{serialize_record_batch_schema_to_row_description, serialize_record_batch_to_data_rows},
    postgres_protocol::{Message, StartupMessage},
    Transformer,
};
use anyhow::Result;
use arrow::record_batch::RecordBatch;
use futures::future::join_all;
use rand::Rng;
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
    transformers: &Vec<Box<dyn Transformer>>,
    resolvers: &mut Vec<Box<dyn Resolver>>,
) -> Result<()> {
    loop {
        let request = frontend.read_message().await?;

        match request {
            Message::Terminate => {
                break;
            }
            Message::SimpleQuery(query_string) => {
                let resolver_responses =
                    join_all(resolvers.iter().map(|r| r.query(&query_string))).await;

                let resolver_data: Vec<RecordBatch> = resolver_responses
                    .iter()
                    .filter_map(|result| match result {
                        Ok(data) => Some(data),
                        _ => None,
                    })
                    .cloned()
                    .collect();

                // TODO: Handle no results returned
                let record_batch = resolver_data.first().unwrap().clone();

                join_all(
                    resolvers
                        .iter_mut()
                        .map(|r| r.inform(&query_string, record_batch.clone())),
                )
                .await;

                let transformed = transformers.iter().fold(record_batch, |data, transformer| {
                    transformer.transform(&data)
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
            _ => unimplemented!(),
        }
    }

    Ok(())
}
