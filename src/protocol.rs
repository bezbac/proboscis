use anyhow::Result;
use byteorder::{BigEndian, ByteOrder, NetworkEndian, WriteBytesExt};
use bytes::{Buf, BytesMut};
use std::collections::HashMap;
use std::convert::TryFrom;
use std::io::prelude::*;

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

        writer.write_i8(0)?; // Delimiter

        let len_of_message: u32 = writer.len() as u32 + 4; // Add 4 bytes for the u32 containing the total message length

        buf.write_u32::<BigEndian>(len_of_message)?;
        buf.write(&mut writer[..])?;

        Ok(())
    }

    pub fn read<T: Read>(stream: &mut T) -> Result<Self> {
        // Init handshake

        let mut bytes = vec![0; 4];
        bytes = stream.read_exact(&mut bytes).map(|_| bytes)?;

        let message_length = usize::try_from(NetworkEndian::read_u32(&bytes))?;
        let frame_legth = message_length - 4; // Remove 4 bytes for the u32 containing the total message length

        let mut buf = BytesMut::new();
        buf.resize(frame_legth, b'0');
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

                let mut params: HashMap<String, String> = HashMap::new();

                let mut last_string: Option<String> = None;
                let mut current: Vec<u8> = vec![];

                for b in bytes {
                    if b == 0 {
                        let string = std::str::from_utf8(&current.as_slice())?.to_string();

                        match &last_string {
                            Some(ls) => {
                                params.insert(ls.clone(), string.clone());
                                last_string = None;
                            }
                            None => {
                                last_string = Some(string.clone());
                            }
                        }

                        current = vec![];
                    } else {
                        current.push(b)
                    }
                }

                StartupMessage::Startup { params }
            }
        };

        Ok(message)
    }
}

#[derive(Debug)]
pub enum AuthenticationRequest {
    MD5Password,
}

impl AuthenticationRequest {
    pub fn new(method: u32, additional_bytes: Vec<u8>) -> Result<AuthenticationRequest> {
        if method == 5 {
            return Ok(AuthenticationRequest::MD5Password);
        }

        unimplemented!();
    }
}

#[derive(Debug)]
pub enum BackendMessage {
    AuthenticationRequest(AuthenticationRequest),
    AuthenticationOk,
    ReadyForQuery,
    SimpleQuery(String),
}

impl BackendMessage {
    pub fn as_vec(&self) -> Vec<u8> {
        match self {
            BackendMessage::AuthenticationOk => {
                vec![CharTag::Authentication.into(), 0, 0, 0, 8, 0, 0, 0, 0]
            }
            BackendMessage::ReadyForQuery => {
                vec![
                    CharTag::ReadyForQuery.into(),
                    0,
                    0,
                    0,
                    5,
                    CharTag::EmptyQueryResponse.into(),
                ]
            }
            _ => unimplemented!(),
        }
    }

    pub fn write<T: Write>(self: Self, buf: &mut T) -> Result<usize> {
        buf.write(&self.as_vec()[..])
            .map_err(|e| anyhow::anyhow!(e))
    }

    pub fn read<T: Read>(stream: &mut T) -> Result<Self> {
        let tag = CharTag::read(stream)?;

        match tag {
            CharTag::Query => {
                let mut bytes = vec![0; 4];
                bytes = stream.read_exact(&mut bytes).map(|_| bytes)?;
                let message_len = u32::try_from(NetworkEndian::read_u32(&bytes))?;
                let string_len = message_len - 1 - 4; // -1 for CharTag, -4 for length u32

                // Read the query to a string
                let mut bytes = vec![0; string_len as usize];
                bytes = stream.read_exact(&mut bytes).map(|_| bytes)?;

                let query_string = std::str::from_utf8(&bytes[..])?;

                // Read another byte (should be a zero)
                let mut bytes = vec![0; 1];
                stream.read_exact(&mut bytes)?;

                Ok(Self::SimpleQuery(query_string.to_string()))
            }
            CharTag::Authentication => {
                let mut bytes = vec![0; 4];
                bytes = stream.read_exact(&mut bytes).map(|_| bytes)?;
                let message_len = u32::try_from(NetworkEndian::read_u32(&bytes))?;

                let mut bytes = vec![0; 4];
                bytes = stream.read_exact(&mut bytes).map(|_| bytes)?;
                let method = u32::try_from(NetworkEndian::read_u32(&bytes))?;

                let additional_bytes = match message_len > 9 {
                    true => {
                        let mut b = vec![0; message_len as usize - 9];
                        stream.read_exact(&mut b).map(|_| bytes)?;
                        b
                    }
                    false => vec![],
                };

                Ok(Self::AuthenticationRequest(AuthenticationRequest::new(
                    method,
                    additional_bytes,
                )?))
            }
            _ => unimplemented!("Recieved tag {:?}", tag),
        }
    }
}

#[derive(Debug, std::cmp::PartialEq)]
pub enum CharTag {
    Authentication,
    ReadyForQuery,
    EmptyQueryResponse,
    Query,
}

impl CharTag {
    pub fn read<T: Read>(stream: &mut T) -> Result<CharTag> {
        let mut bytes = vec![0; 1];
        stream.read(&mut bytes)?;
        return CharTag::try_from(bytes[0]).map_err(|e| anyhow::anyhow!(e));
    }
}

impl From<CharTag> for u8 {
    fn from(value: CharTag) -> Self {
        match value {
            CharTag::Authentication => b'R',
            CharTag::ReadyForQuery => b'Z',
            CharTag::EmptyQueryResponse => b'I',
            CharTag::Query => b'Q',
        }
    }
}

impl TryFrom<u8> for CharTag {
    type Error = &'static str;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            b'R' => Ok(CharTag::Authentication),
            b'Z' => Ok(CharTag::ReadyForQuery),
            b'I' => Ok(CharTag::EmptyQueryResponse),
            b'Q' => Ok(CharTag::Query),
            _ => Err("Unknown char tag {:?}"),
        }
    }
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
        params.insert("Test Key 2".to_string(), "Test Value 2".to_string());
        params.insert("Test Key 3".to_string(), "Test Value 3".to_string());

        let mut buf = vec![];
        StartupMessage::write(&mut buf, params.clone()).unwrap();
        let parsed = StartupMessage::read(&mut buf.as_slice()).unwrap();

        assert_eq!(parsed, StartupMessage::Startup { params })
    }
}
