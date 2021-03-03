use anyhow::{anyhow, Result};
use byteorder::{ByteOrder, NetworkEndian};
use bytes::{Buf, BytesMut};
use std::collections::HashMap;
use std::convert::TryFrom;
use std::io::prelude::*;
use std::net::{TcpListener, TcpStream};

struct Config {
    host: String,
    port: String,
}

fn main() {
    let config: Config = Config {
        host: "0.0.0.0".to_string(),
        port: "5430".to_string(),
    };

    let address = format!("{}:{}", config.host, config.port);

    let listener = TcpListener::bind(&address).unwrap();
    println!("Server running on {}!", &address);

    for stream in listener.incoming() {
        let stream = stream.unwrap();
        handle_connection(stream);
    }
}

const CODE_STARTUP_CANCEL: i32 = 80877102;
const CODE_STARTUP_SSL_REQUEST: i32 = 80877103;
const CODE_STARTUP_GSSENC_REQUEST: i32 = 80877104;

#[derive(Debug)]
enum StartupMessage {
    CancelRequest { connection_id: u32, secret_key: u32 },
    SslRequest,
    GssEncRequest,
    Startup { params: HashMap<String, String> },
}

fn read_startup_message(stream: &mut TcpStream) -> Result<StartupMessage> {
    // Init handshake

    let mut bytes = vec![0; 4];
    bytes = stream.read_exact(&mut bytes).map(|_| bytes)?;

    let frame_len = usize::try_from(NetworkEndian::read_u32(&bytes))? - 4;

    let mut buf = BytesMut::new();
    buf.resize(frame_len, b'0');
    stream.read_exact(&mut buf)?;

    let protocol_version = buf.get_i32();
    let message = match protocol_version {
        CODE_STARTUP_CANCEL => StartupMessage::CancelRequest {
            connection_id: buf.get_u32(),
            secret_key: buf.get_u32(),
        },
        CODE_STARTUP_SSL_REQUEST => StartupMessage::SslRequest,
        CODE_STARTUP_GSSENC_REQUEST => StartupMessage::GssEncRequest,
        _ => {
            // TODO: Find a better way to read these strings

            let bytes = buf.to_vec();

            let mut strings: Vec<String> = vec![];
            let mut current: Vec<u8> = vec![];

            for b in bytes {
                if b == 0 {
                    let string = std::str::from_utf8(&current[..])?;
                    strings.push(string.to_string());
                    current = vec![];
                } else {
                    current.push(b)
                }
            }

            let mut params = HashMap::new();
            for (i, key) in strings.iter().step_by(2).enumerate() {
                let value = strings.get(i + 1).expect("Missing value for key");
                params.insert(key.clone(), value.clone());
            }

            StartupMessage::Startup { params }
        }
    };

    println!("{:?}", message);

    Ok(message)
}

fn handle_connection(mut stream: TcpStream) -> Result<(), anyhow::Error> {
    println!("Connection established!");

    let startup_message = read_startup_message(&mut stream);

    println!("{:?}", startup_message);

    Ok(())
}
