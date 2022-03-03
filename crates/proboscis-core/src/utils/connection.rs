use crate::data::arrow::{
    serialize_record_batch_schema_to_row_description, serialize_record_batch_to_data_rows,
};
use arrow::record_batch::RecordBatch;
use proboscis_postgres_protocol::{
    message::{BackendMessage, FrontendMessage},
    Message, ParseError, StartupMessage,
};
use std::collections::HashMap;
use tokio::io::{AsyncWriteExt, BufWriter};
use tracing::debug;

pub type MaybeTlsStream = tokio_util::either::Either<
    tokio::net::TcpStream,
    tokio_native_tls::TlsStream<tokio::net::TcpStream>,
>;

#[derive(Debug)]
pub struct Connection {
    stream: BufWriter<MaybeTlsStream>,
    pub parameters: HashMap<String, String>,
}

impl Connection {
    pub fn new(stream: MaybeTlsStream, parameters: HashMap<String, String>) -> Connection {
        Connection {
            stream: BufWriter::new(stream),
            parameters,
        }
    }

    pub async fn write_data(&mut self, data: RecordBatch) -> Result<(), std::io::Error> {
        let row_description = serialize_record_batch_schema_to_row_description(&data.schema());

        self.write_message(BackendMessage::RowDescription(row_description).into())
            .await?;

        let data_rows = serialize_record_batch_to_data_rows(&data)?;

        for message in data_rows {
            self.write_message(BackendMessage::DataRow(message).into())
                .await?;
        }

        Ok(())
    }

    pub async fn write_message(&mut self, message: Message) -> Result<(), std::io::Error> {
        debug!(message = ?message, "writing message");
        message.write(&mut self.stream).await?;
        self.stream.flush().await
    }

    pub async fn write_startup_message(
        &mut self,
        message: StartupMessage,
    ) -> Result<(), std::io::Error> {
        debug!(message = ?message, "writing startup message");
        message.write(&mut self.stream).await?;
        self.stream.flush().await
    }

    pub async fn read_frontend_message(&mut self) -> Result<FrontendMessage, ParseError> {
        let message = FrontendMessage::read(&mut self.stream).await;
        debug!(message = ?message, "read frontend message");
        message
    }

    pub async fn read_backend_message(&mut self) -> Result<BackendMessage, ParseError> {
        let message = BackendMessage::read(&mut self.stream).await;
        debug!(message = ?message, "read backend message");
        message
    }

    pub async fn read_startup_message(&mut self) -> Result<StartupMessage, ParseError> {
        let message = StartupMessage::read(&mut self.stream).await;
        debug!(message = ?message, "read startup message");
        message
    }
}
