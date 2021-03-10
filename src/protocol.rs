use anyhow::Result;
use byteorder::{ByteOrder, NetworkEndian};
use bytes::{Buf, BytesMut};
use std::collections::HashMap;
use std::convert::TryFrom;
use std::io::prelude::*;
use std::net::TcpStream;

const CODE_STARTUP_CANCEL: i32 = 80877102;
const CODE_STARTUP_SSL_REQUEST: i32 = 80877103;
const CODE_STARTUP_GSSENC_REQUEST: i32 = 80877104;

#[derive(Debug)]
pub enum StartupMessage {
    CancelRequest { connection_id: u32, secret_key: u32 },
    SslRequest,
    GssEncRequest,
    Startup { params: HashMap<String, String> },
}

pub enum BackendMessage {
    AuthenticationOk,
    ReadyForQuery,
}

impl BackendMessage {
    pub fn as_vec(&self) -> Vec<u8> {
        match self {
            BackendMessage::AuthenticationOk => {
                vec![CharTag::Authentication.as_u8(), 0, 0, 0, 8, 0, 0, 0, 0]
            }
            BackendMessage::ReadyForQuery => {
                vec![
                    CharTag::ReadyForQuery.as_u8(),
                    0,
                    0,
                    0,
                    5,
                    CharTag::EmptyQueryResponse.as_u8(),
                ]
            }
        }
    }
}

pub enum CharTag {
    Authentication,
    ReadyForQuery,
    EmptyQueryResponse,
}

impl CharTag {
    pub fn as_u8(&self) -> u8 {
        match self {
            CharTag::Authentication => b'R',
            CharTag::ReadyForQuery => b'Z',
            CharTag::EmptyQueryResponse => b'I',
        }
    }
}

pub fn read_startup_message(stream: &mut TcpStream) -> Result<StartupMessage> {
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

    Ok(message)
}
