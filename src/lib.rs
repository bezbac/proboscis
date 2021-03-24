use anyhow::Result;
use byteorder::{ByteOrder, NetworkEndian};
use std::collections::HashMap;
use std::convert::TryFrom;
use std::io::prelude::*;
use std::net::{TcpListener, TcpStream};

mod protocol;

use protocol::{BackendMessage, StartupMessage};

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

    // TODO: Read auth response from server
    let mut bytes = vec![0; 1];
    backend_stream.read(&mut bytes)?;

    println!("{:?}", bytes);

    // Skip auth for now
    stream.write(&BackendMessage::AuthenticationOk.as_vec()[..])?;
    stream.write(&BackendMessage::ReadyForQuery.as_vec()[..])?;

    loop {
        let mut bytes = vec![0; 1];
        stream.read(&mut bytes)?;

        match bytes[0] {
            b'Q' => {
                // Query

                // Read the length of the following string as an i32
                let mut bytes = vec![0; 4];
                bytes = stream.read_exact(&mut bytes).map(|_| bytes)?;

                // TODO: Why do I need the - 4 or 5?
                let string_len = usize::try_from(NetworkEndian::read_u32(&bytes))? - 5;

                // Read the query to a string
                let mut bytes = vec![0; string_len];
                bytes = stream.read_exact(&mut bytes).map(|_| bytes)?;

                let query_string = std::str::from_utf8(&bytes[..])?;

                // Read another byte (should be a zero)
                let mut bytes = vec![0; 1];
                stream.read_exact(&mut bytes)?;

                println!("{:?}", query_string);

                // TODO: Execute simple query

                // Next message
                // Row Description
            }
            _ => unimplemented!("Recieved byte {:?}", bytes[0]),
        }
    }
}
