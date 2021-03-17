use anyhow::Result;
use byteorder::{BigEndian, ByteOrder, NetworkEndian, WriteBytesExt};
use bytes::{Buf, BytesMut};
use std::collections::HashMap;
use std::convert::TryFrom;
use std::io::prelude::*;
use std::net::TcpStream;

const CODE_STARTUP_CANCEL: i32 = 80877102;
const CODE_STARTUP_SSL_REQUEST: i32 = 80877103;
const CODE_STARTUP_GSSENC_REQUEST: i32 = 80877104;
const CODE_STARTUP_POSTGRESQLV3: i32 = 0x00_03_00_00; // postgres protocol version 3.0(196608)

#[derive(Debug, std::cmp::PartialEq)]
pub enum StartupMessage {
    CancelRequest { connection_id: u32, secret_key: u32 },
    SslRequest,
    GssEncRequest,
    Startup { params: HashMap<String, String> },
}

impl StartupMessage {
    pub fn write<T: Write>(buf: &mut T, parameters: HashMap<String, String>) -> Result<()> {
        let mut writer = vec![];

        writer.write_i32::<BigEndian>(CODE_STARTUP_POSTGRESQLV3)?;

        for (key, value) in parameters {
            writer.write(key.as_bytes())?;
            writer.write_i8(0)?; // Delimiter

            writer.write(value.as_bytes())?;
            writer.write_i8(0)?; // Delimiter
        }

        let len_of_message: i32 = writer.len() as i32;

        buf.write_i32::<BigEndian>(len_of_message)?;
        buf.write(&mut writer[..])?;

        Ok(())
    }

    pub fn read<T: Read>(stream: &mut T) -> Result<StartupMessage> {
        // Init handshake

        let mut bytes = vec![0; 4];
        bytes = stream.read_exact(&mut bytes).map(|_| bytes)?;

        let frame_len = usize::try_from(NetworkEndian::read_u32(&bytes))?;

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
    return StartupMessage::read(stream);
}

pub fn send_and_handle_startup(stream: &mut TcpStream) -> Result<()> {
    // Send startup message
    StartupMessage::write(stream, HashMap::new())?;

    // TODO: Hndle response
    let mut bytes = vec![0; 1];
    stream.read(&mut bytes)?;

    println!("{:?}", bytes);

    return Ok(());
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn startup_message_without_parameters() {
        let mut buf = vec![];
        StartupMessage::write(&mut buf, HashMap::new()).unwrap();
        StartupMessage::read(&mut buf.as_slice()).unwrap();
    }

    #[test]
    fn startup_message_with_parameters() {
        let mut params: HashMap<String, String> = HashMap::new();
        params.insert("Test Key".to_string(), "Test Value".to_string());

        let mut buf = vec![];
        StartupMessage::write(&mut buf, params.clone()).unwrap();
        let parsed = StartupMessage::read(&mut buf.as_slice()).unwrap();

        assert_eq!(parsed, StartupMessage::Startup { params })
    }
}
