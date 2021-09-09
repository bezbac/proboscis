use crate::{
    arrow::{
        serialize_record_batch_schema_to_row_description, serialize_record_batch_to_data_rows,
    },
    postgres_protocol::{Message, StartupMessage},
};
use anyhow::Result;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use std::collections::HashMap;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};

pub enum ConnectionKind {
    Backend,
    Frontend,
}

impl ConnectionKind {
    fn log_char(&self) -> String {
        match self {
            Self::Backend => "->".to_string(),
            Self::Frontend => "<-".to_string(),
        }
    }
}

pub type MaybeTlsStream = tokio_util::either::Either<
    tokio::net::TcpStream,
    tokio_native_tls::TlsStream<tokio::net::TcpStream>,
>;

#[async_trait]
pub trait ProtocolStream {
    async fn write_message(&mut self, message: Message) -> tokio::io::Result<usize>;
    async fn write_startup_message(&mut self, message: StartupMessage) -> tokio::io::Result<usize>;
    async fn read_message(&mut self) -> Result<Message>;
    async fn read_startup_message(&mut self) -> Result<StartupMessage>;
}

#[async_trait]
impl<T> ProtocolStream for T
where
    T: AsyncWrite + AsyncRead + Unpin + Send,
{
    async fn write_message(&mut self, message: Message) -> tokio::io::Result<usize> {
        let (_, mut wr) = tokio::io::split(&mut self);
        wr.write(&message.as_vec()[..]).await
    }

    async fn write_startup_message(&mut self, message: StartupMessage) -> tokio::io::Result<usize> {
        let (_, mut wr) = tokio::io::split(&mut self);
        wr.write(&message.as_vec()[..]).await
    }

    async fn read_message(&mut self) -> Result<Message> {
        let result = Message::read_async(&mut self).await?;
        Ok(result)
    }

    async fn read_startup_message(&mut self) -> Result<StartupMessage> {
        let result = StartupMessage::read_async(&mut self).await?;
        Ok(result)
    }
}

pub struct Connection {
    kind: ConnectionKind,
    stream: MaybeTlsStream,

    pub parameters: HashMap<String, String>,
}

impl Connection {
    pub fn new(
        stream: MaybeTlsStream,
        kind: ConnectionKind,
        parameters: HashMap<String, String>,
    ) -> Connection {
        Connection {
            stream,
            kind,
            parameters,
        }
    }

    pub async fn write(&mut self, bytes: &[u8]) -> tokio::io::Result<usize> {
        let (_, mut wr) = tokio::io::split(&mut self.stream);
        println!("{} Writing bytes", self.kind.log_char());
        wr.write(bytes).await
    }

    pub async fn write_data(&mut self, data: RecordBatch) -> tokio::io::Result<()> {
        let row_description = serialize_record_batch_schema_to_row_description(data.schema());

        self.write_message(row_description).await?;

        let data_rows = serialize_record_batch_to_data_rows(data);
        for message in data_rows {
            self.write_message(message).await?;
        }

        Ok(())
    }
}

#[async_trait]
impl ProtocolStream for Connection {
    async fn write_message(&mut self, message: Message) -> tokio::io::Result<usize> {
        println!("{} Writing message: {:?}", self.kind.log_char(), message);
        self.stream.write_message(message).await
    }

    async fn write_startup_message(&mut self, message: StartupMessage) -> tokio::io::Result<usize> {
        println!(
            "{} Writing startup message: {:?}",
            self.kind.log_char(),
            message
        );
        self.stream.write_startup_message(message).await
    }

    async fn read_message(&mut self) -> Result<Message> {
        let result = self.stream.read_message().await;
        println!("{} Read message: {:?}", self.kind.log_char(), result);
        result
    }

    async fn read_startup_message(&mut self) -> Result<StartupMessage> {
        let result = self.stream.read_startup_message().await;
        println!(
            "{} Read startup message: {:?}",
            self.kind.log_char(),
            result
        );
        result
    }
}
