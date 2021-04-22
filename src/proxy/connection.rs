use crate::protocol::{Message, StartupMessage};
use anyhow::Result;
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpStream, ToSocketAddrs};

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

pub struct Connection {
    pub stream: MaybeTlsStream,
    kind: ConnectionKind,
}

impl Connection {
    pub fn new(stream: MaybeTlsStream, kind: ConnectionKind) -> Connection {
        Connection { stream, kind }
    }

    pub async fn connect<A: ToSocketAddrs>(address: A, kind: ConnectionKind) -> Result<Connection> {
        let stream = TcpStream::connect(address).await?;
        Ok(Connection {
            stream: MaybeTlsStream::Left(stream),
            kind,
        })
    }

    pub async fn write(&mut self, bytes: &[u8]) -> tokio::io::Result<usize> {
        let (_, mut wr) = tokio::io::split(&mut self.stream);
        println!("{} Writing bytes", self.kind.log_char());
        wr.write(bytes).await
    }

    pub async fn write_message(&mut self, message: Message) -> tokio::io::Result<usize> {
        let (_, mut wr) = tokio::io::split(&mut self.stream);
        println!("{} Writing message: {:?}", self.kind.log_char(), message);
        wr.write(&message.as_vec()[..]).await
    }

    pub async fn write_startup_message(
        &mut self,
        message: StartupMessage,
    ) -> tokio::io::Result<usize> {
        let (_, mut wr) = tokio::io::split(&mut self.stream);
        println!(
            "{} Writing startup message: {:?}",
            self.kind.log_char(),
            message
        );
        wr.write(&message.as_vec()[..]).await
    }

    pub async fn read_message(&mut self) -> Result<Message> {
        let result = Message::read_async(&mut self.stream).await?;
        println!("{} Read message: {:?}", self.kind.log_char(), result);
        Ok(result)
    }

    pub async fn read_startup_message(&mut self) -> Result<StartupMessage> {
        let result = StartupMessage::read_async(&mut self.stream).await?;
        println!(
            "{} Read startup message: {:?}",
            self.kind.log_char(),
            result
        );
        Ok(result)
    }
}
