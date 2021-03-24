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
            handle_connection(stream, self.target_addr.clone())?;
        }

        Ok(())
    }
}

pub fn new(target_addr: &str) -> App {
    App {
        target_addr: target_addr.to_string(),
    }
}

fn handle_connection(mut stream: TcpStream, target_addr: String) -> Result<(), anyhow::Error> {
    println!("Connection established!");

    let startup_message = StartupMessage::read(&mut stream)?;
    println!("{:?}", startup_message);

    let frontend_params = match startup_message {
        StartupMessage::Startup { params } => params,
        _ => return Err(anyhow::anyhow!("Didn't recieve startup message")),
    };

    let mut backend_stream = TcpStream::connect(&target_addr)?;
    let mut backend_params: HashMap<String, String> = HashMap::new();
    backend_params.insert(
        "user".to_string(),
        frontend_params
            .get("user")
            .expect("Missing user parameter")
            .clone(),
    );
    backend_params.insert("client_encoding".to_string(), "UTF8".to_string());

    StartupMessage::write(&mut backend_stream, backend_params)?;
    let response = Message::read(&mut backend_stream)?;

    println!("{:?}", response);

    // Skip auth for now
    Message::AuthenticationOk.write(&mut stream)?;
    Message::ReadyForQuery.write(&mut stream)?;

    loop {
        let request = Message::read(&mut stream)?;

        match request {
            Message::SimpleQuery(query_string) => {
                println!("{:?}", query_string);

                // TODO: Execute simple query

                // Next message
                // Row Description
            }
            _ => unimplemented!(),
        }
    }
}
