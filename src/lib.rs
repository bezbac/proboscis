use anyhow::Result;
use std::collections::HashMap;
use std::net::{TcpListener, TcpStream};

mod protocol;

use protocol::{Message, StartupMessage};

pub struct App {
    target_addr: String,
}

impl App {
    pub fn listen(&self, address: &str) -> Result<()> {
        let listener = TcpListener::bind(&address).unwrap();
        println!("Server running on {}!", &address);

        for stream in listener.incoming() {
            let stream = stream.unwrap();
            let wrapper = StreamWrapper::new(stream, StreamWrapperKind::Frontend);
            handle_connection(wrapper, self.target_addr.clone())?;
        }

        Ok(())
    }
}

pub fn new(target_addr: &str) -> App {
    App {
        target_addr: target_addr.to_string(),
    }
}

enum StreamWrapperKind {
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

struct StreamWrapper {
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

fn handle_connection(
    mut frontend: StreamWrapper,
    target_addr: String,
) -> Result<(), anyhow::Error> {
    println!("New connection established!");

    let startup_message = frontend.read_startup_message()?;

    let frontend_params = match startup_message {
        StartupMessage::Startup { params } => params,
        _ => return Err(anyhow::anyhow!("Didn't recieve startup message")),
    };

    let backend_stream = TcpStream::connect(&target_addr)?;
    let mut backend = StreamWrapper::new(backend_stream, StreamWrapperKind::Backend);

    let mut backend_params: HashMap<String, String> = HashMap::new();
    backend_params.insert(
        "user".to_string(),
        frontend_params
            .get("user")
            .expect("Missing user parameter")
            .clone(),
    );
    backend_params.insert("client_encoding".to_string(), "UTF8".to_string());

    backend.write_startup_message(StartupMessage::Startup {
        params: backend_params,
    })?;

    let response = backend.read_message()?;

    // Skip auth for now
    frontend.write_message(Message::AuthenticationOk)?;
    frontend.write_message(Message::ReadyForQuery)?;

    loop {
        let request = frontend.read_message()?;

        match request {
            Message::SimpleQuery(query_string) => {
                // TODO: Execute simple query

                // Next message
                // Row Description
            }
            _ => unimplemented!(),
        }
    }
}
