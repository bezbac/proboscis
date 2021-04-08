use anyhow::Result;
use byteorder::{ByteOrder, NetworkEndian};
use bytes::{Buf, BytesMut};
use omnom::prelude::*;
use std::collections::HashMap;
use std::convert::TryFrom;
use std::io::prelude::*;

pub const CODE_STARTUP_CANCEL: i32 = 80877102;
pub const CODE_STARTUP_SSL_REQUEST: i32 = 80877103;
pub const CODE_STARTUP_GSSENC_REQUEST: i32 = 80877104;
pub const CODE_STARTUP_POSTGRESQLV3: i32 = 0x00_03_00_00; // postgres protocol version 3.0(196608)

#[derive(Debug, std::cmp::PartialEq)]
pub enum StartupMessage {
    CancelRequest { connection_id: u32, secret_key: u32 },
    SslRequest,
    GssEncRequest,
    Startup { params: HashMap<String, String> },
}

impl StartupMessage {
    pub fn write<T: Write>(self, buf: &mut T) -> Result<()> {
        match self {
            Self::Startup { params } => {
                let mut writer = vec![];

                writer.write_be(CODE_STARTUP_POSTGRESQLV3)?;

                for (key, value) in params {
                    writer.write(key.as_bytes())?;
                    writer.write_be(0 as u8)?; // Delimiter

                    writer.write(value.as_bytes())?;
                    writer.write_be(0 as u8)?; // Delimiter
                }

                writer.write_be(0 as u8)?; // Delimiter

                let len_of_message: u32 = writer.len() as u32 + 4; // Add 4 bytes for the u32 containing the total message length

                buf.write_be(len_of_message)?;
                buf.write(&mut writer[..])?;

                Ok(())
            }
            _ => unimplemented!(),
        }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn startup_message_without_parameters() {
        let mut buf = vec![];
        StartupMessage::Startup {
            params: HashMap::new(),
        }
        .write(&mut buf)
        .unwrap();
        StartupMessage::read(&mut buf.as_slice()).unwrap();
    }

    #[test]
    fn startup_message_with_parameters() {
        let mut params: HashMap<String, String> = HashMap::new();
        params.insert("Test Key".to_string(), "Test Value".to_string());
        params.insert("Test Key 2".to_string(), "Test Value 2".to_string());
        params.insert("Test Key 3".to_string(), "Test Value 3".to_string());

        let mut buf = vec![];
        StartupMessage::Startup {
            params: params.clone(),
        }
        .write(&mut buf)
        .unwrap();
        let parsed = StartupMessage::read(&mut buf.as_slice()).unwrap();

        assert_eq!(parsed, StartupMessage::Startup { params })
    }
}
