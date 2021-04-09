use crate::protocol::{Message, StartupMessage};
use anyhow::Result;
use std::net::TcpStream;

pub enum StreamWrapperKind {
    Backend,
    Frontend,
}

impl StreamWrapperKind {
    fn log_char(&self) -> String {
        match self {
            Self::Backend => "->".to_string(),
            Self::Frontend => "<-".to_string(),
        }
    }
}

pub struct StreamWrapper {
    pub stream: TcpStream,
    kind: StreamWrapperKind,
}

impl StreamWrapper {
    pub fn new(stream: std::net::TcpStream, kind: StreamWrapperKind) -> StreamWrapper {
        StreamWrapper { stream, kind }
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

impl std::io::Write for StreamWrapper {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.stream.write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.stream.flush()
    }
}

impl std::io::Read for StreamWrapper {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.stream.read(buf)
    }
}
