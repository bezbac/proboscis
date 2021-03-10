use anyhow::Result;
use byteorder::{ByteOrder, NetworkEndian};
use std::convert::TryFrom;
use std::io::prelude::*;
use std::net::{TcpListener, TcpStream};

mod client;
mod protocol;

use client::ProxyClient;
use protocol::BackendMessage;

pub struct App {
    target_addr: String,
}

impl App {
    pub fn listen(&self, address: &str) -> Result<()> {
        let mut proxy_client = ProxyClient::new(self.target_addr.clone());

        let listener = TcpListener::bind(&address).unwrap();
        println!("Server running on {}!", &address);

        for stream in listener.incoming() {
            let stream = stream.unwrap();
            handle_connection(stream, &mut proxy_client);
        }

        Ok(())
    }
}

pub fn new(target_addr: &str) -> App {
    App {
        target_addr: target_addr.to_string(),
    }
}

fn handle_connection(
    mut stream: TcpStream,
    target_client: &mut ProxyClient,
) -> Result<(), anyhow::Error> {
    println!("Connection established!");

    let startup_message = protocol::read_startup_message(&mut stream);
    println!("{:?}", startup_message);

    // Skip auth for now
    stream.write(&BackendMessage::AuthenticationOk.as_vec()[..])?;
    stream.write(&BackendMessage::ReadyForQuery.as_vec()[..])?;

    let proxy_connection = target_client.connect()?;

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

                let response_messages = proxy_connection.simple_query(query_string)?;

                // Next message
                // Row Description
            }
            _ => unimplemented!("Recieved byte {:?}", bytes[0]),
        }
    }

    Ok(())
}
