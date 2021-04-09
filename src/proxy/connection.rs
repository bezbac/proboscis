use crate::protocol::{Message, StartupMessage};
use anyhow::Result;
use std::net::{TcpStream, ToSocketAddrs};

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

pub struct Connection {
    pub stream: TcpStream,
    kind: ConnectionKind,
}

impl Connection {
    pub fn new(stream: std::net::TcpStream, kind: ConnectionKind) -> Connection {
        Connection { stream, kind }
    }

    pub fn connect<A: ToSocketAddrs>(address: A, kind: ConnectionKind) -> Connection {
        let stream = TcpStream::connect(address).expect("Connecting failed");
        Self::new(stream, kind)
    }

    pub fn write_message(&mut self, message: Message) -> Result<usize> {
        println!("{} Writing message: {:?}", self.kind.log_char(), message);
        message.write(self)
    }

    pub fn write_startup_message(&mut self, message: StartupMessage) -> Result<()> {
        println!(
            "{} Writing startup message: {:?}",
            self.kind.log_char(),
            message
        );
        message.write(self)
    }

    pub fn read_message(&mut self) -> Result<Message> {
        let result = Message::read(self)?;
        println!("{} Read message: {:?}", self.kind.log_char(), result);
        Ok(result)
    }

    pub fn read_startup_message(&mut self) -> Result<StartupMessage> {
        let result = StartupMessage::read(self)?;
        println!(
            "{} Read startup message: {:?}",
            self.kind.log_char(),
            result
        );
        Ok(result)
    }
}

impl std::io::Write for Connection {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.stream.write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.stream.flush()
    }
}

impl std::io::Read for Connection {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.stream.read(buf)
    }
}
