use crate::utils::arrow::{
    serialize_record_batch_schema_to_row_description, serialize_record_batch_to_data_rows,
};
use anyhow::Result;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use postgres_protocol::{
    message::{BackendMessage, FrontendMessage},
    Message, StartupMessage,
};
use std::collections::HashMap;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use tracing::debug;

pub type MaybeTlsStream = tokio_util::either::Either<
    tokio::net::TcpStream,
    tokio_native_tls::TlsStream<tokio::net::TcpStream>,
>;

#[async_trait]
pub trait ProtocolStream {
    async fn write_message(&mut self, message: Message) -> Result<usize>;
    async fn write_startup_message(&mut self, message: StartupMessage) -> Result<()>;
    async fn read_frontend_message(&mut self) -> Result<FrontendMessage>;
    async fn read_backend_message(&mut self) -> Result<BackendMessage>;
    async fn read_startup_message(&mut self) -> Result<StartupMessage>;
}

#[async_trait]
impl<T> ProtocolStream for T
where
    T: AsyncWrite + AsyncRead + Unpin + Send,
{
    async fn write_message(&mut self, message: Message) -> Result<usize> {
        let (_, mut wr) = tokio::io::split(&mut self);
        message.write(&mut wr).await
    }

    async fn write_startup_message(&mut self, message: StartupMessage) -> Result<()> {
        let (_, mut wr) = tokio::io::split(&mut self);
        message.write(&mut wr).await
    }

    async fn read_frontend_message(&mut self) -> Result<FrontendMessage> {
        let result = FrontendMessage::read(&mut self).await?;
        Ok(result)
    }

    async fn read_backend_message(&mut self) -> Result<BackendMessage> {
        let result = BackendMessage::read(&mut self).await?;
        Ok(result)
    }

    async fn read_startup_message(&mut self) -> Result<StartupMessage> {
        let result = StartupMessage::read(&mut self).await?;
        Ok(result)
    }
}

#[derive(Debug)]
pub struct Connection {
    stream: MaybeTlsStream,

    pub parameters: HashMap<String, String>,
}

impl Connection {
    pub fn new(stream: MaybeTlsStream, parameters: HashMap<String, String>) -> Connection {
        Connection { stream, parameters }
    }

    pub async fn write(&mut self, bytes: &[u8]) -> tokio::io::Result<usize> {
        let (_, mut wr) = tokio::io::split(&mut self.stream);
        wr.write(bytes).await
    }

    pub async fn write_data(&mut self, data: RecordBatch) -> Result<()> {
        let row_description = serialize_record_batch_schema_to_row_description(&data.schema());

        self.write_message(BackendMessage::RowDescription(row_description).into())
            .await?;

        let data_rows = serialize_record_batch_to_data_rows(&data);
        for message in data_rows {
            self.write_message(BackendMessage::DataRow(message).into())
                .await?;
        }

        Ok(())
    }
}

#[async_trait]
impl ProtocolStream for Connection {
    async fn write_message(&mut self, message: Message) -> Result<usize> {
        debug!(message = ?message, "writing message");
        self.stream.write_message(message).await
    }

    async fn write_startup_message(&mut self, message: StartupMessage) -> Result<()> {
        debug!(message = ?message, "writing startup message");
        self.stream.write_startup_message(message).await
    }

    async fn read_frontend_message(&mut self) -> Result<FrontendMessage> {
        let message = self.stream.read_frontend_message().await;
        debug!(message = ?message, "read frontend message");
        message
    }

    async fn read_backend_message(&mut self) -> Result<BackendMessage> {
        let message = self.stream.read_backend_message().await;
        debug!(message = ?message, "read backend message");
        message
    }

    async fn read_startup_message(&mut self) -> Result<StartupMessage> {
        let message = self.stream.read_startup_message().await;
        debug!(message = ?message, "read startup message");
        message
    }
}
