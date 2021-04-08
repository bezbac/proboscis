use anyhow::Result;
use md5::{Digest, Md5};
use std::collections::HashMap;
use std::net::{TcpListener, TcpStream};

mod protocol;

use protocol::{Message, StartupMessage};

#[derive(Clone)]
pub struct Config {
    pub target_addr: String,
    pub authentication: HashMap<String, String>,
}

pub struct App {
    config: Config,
}

impl App {
    pub fn listen(&self, address: &str) -> Result<()> {
        let listener = TcpListener::bind(&address).unwrap();
        println!("Server running on {}!", &address);

        for stream in listener.incoming() {
            let stream = stream.unwrap();
            let wrapper = StreamWrapper::new(stream, StreamWrapperKind::Frontend);
            handle_connection(wrapper, self.config.clone())?;
        }

        Ok(())
    }
}

pub fn new(config: Config) -> App {
    App { config }
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

fn encode_md5_password_hash(username: &str, password: &str, salt: &[u8]) -> String {
    let mut md5 = Md5::new();
    md5.update(password.as_bytes());
    md5.update(username.as_bytes());
    let output = md5.finalize_reset();
    md5.update(format!("{:x}", output));
    md5.update(&salt);
    format!("md5{:x}", md5.finalize())
}

fn setup_tunnel(
    frontend: &mut StreamWrapper,
    config: Config,
) -> Result<StreamWrapper, anyhow::Error> {
    let startup_message = frontend.read_startup_message()?;

    let frontend_params = match startup_message {
        StartupMessage::Startup { params } => params,
        _ => return Err(anyhow::anyhow!("Didn't recieve startup message")),
    };

    let user = frontend_params
        .get("user")
        .expect("Missing user parameter")
        .clone();

    let backend_stream =
        TcpStream::connect(&config.target_addr).expect("Connecting to backend failed");
    let mut backend = StreamWrapper::new(backend_stream, StreamWrapperKind::Backend);

    let mut backend_params: HashMap<String, String> = HashMap::new();
    backend_params.insert("user".to_string(), user.clone());
    backend_params.insert("client_encoding".to_string(), "UTF8".to_string());

    backend.write_startup_message(StartupMessage::Startup {
        params: backend_params,
    })?;

    let response = backend.read_message()?;
    match response {
        Message::AuthenticationRequestMD5Password { salt } => {
            let password = config
                .authentication
                .get(&user.clone())
                .expect(&format!("Password for {} not found inside config", &user));

            frontend
                .write_message(Message::AuthenticationRequestMD5Password { salt: salt.clone() })?;
            let frontend_response = frontend.read_message()?;

            let frontend_hash = match frontend_response {
                Message::MD5HashedPasswordMessage { hash } => hash,
                _ => return Err(anyhow::anyhow!("Expected Password Message")),
            };

            let hash = encode_md5_password_hash(&user, password, &salt[..]);

            if frontend_hash != hash {
                return Err(anyhow::anyhow!("Incorrect password"));
            }

            let message = Message::MD5HashedPasswordMessage { hash };
            backend.write_message(message)?;
            let response = backend.read_message()?;

            match response {
                Message::AuthenticationOk => {}
                _ => return Err(anyhow::anyhow!("Expected AuthenticationOk")),
            }

            frontend.write_message(Message::AuthenticationOk)?;

            loop {
                let response = backend.read_message()?;

                match response {
                    Message::ReadyForQuery => break,
                    Message::ParameterStatus { key, value } => {
                        frontend.write_message(Message::ParameterStatus { key, value })?;
                    }
                    Message::BackendKeyData {
                        process_id,
                        secret_key,
                        additional,
                    } => {
                        frontend.write_message(Message::BackendKeyData {
                            process_id,
                            secret_key,
                            additional,
                        })?;
                    }
                    _ => unimplemented!("Unexpected message"),
                }
            }
        }
        _ => unimplemented!(),
    }

    frontend.write_message(Message::ReadyForQuery)?;

    return Ok(backend);
}

fn handle_connection(mut frontend: StreamWrapper, config: Config) -> Result<(), anyhow::Error> {
    println!("New connection established!");

    let mut backend = setup_tunnel(&mut frontend, config)?;

    loop {
        let request = frontend.read_message()?;

        match request {
            Message::Terminate => break,
            Message::SimpleQuery(query_string) => {
                backend.write_message(Message::SimpleQuery(query_string))?;

                loop {
                    let response = backend.read_message()?;
                    match response {
                        Message::ReadyForQuery => break,
                        Message::RowDescription { fields } => {
                            frontend.write_message(Message::RowDescription { fields })?;
                        }
                        Message::DataRow { field_data } => {
                            frontend.write_message(Message::DataRow { field_data })?;
                        }
                        Message::CommandComplete { tag } => {
                            frontend.write_message(Message::CommandComplete { tag })?;
                        }
                        _ => unimplemented!(""),
                    }
                }

                frontend.write_message(Message::ReadyForQuery)?;
            }
            Message::Parse {
                query,
                params_types,
                statement,
            } => {
                backend.write_message(Message::Parse {
                    query,
                    params_types,
                    statement,
                })?;

                loop {
                    let response = backend.read_message()?;
                    match response {
                        Message::DataRow { field_data } => {
                            frontend.write_message(Message::DataRow { field_data })?;
                        }
                        Message::Describe { name, kind } => {
                            frontend.write_message(Message::Describe { name, kind })?;
                        }
                        Message::CommandComplete { tag } => {
                            frontend.write_message(Message::CommandComplete { tag })?;
                        }
                        _ => unimplemented!(""),
                    }
                }
            }
            _ => unimplemented!(),
        }
    }

    Ok(())
}
