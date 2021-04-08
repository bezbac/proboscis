use anyhow::Result;
use byteorder::{ByteOrder, NetworkEndian, WriteBytesExt};
use bytes::{Buf, BytesMut};
use omnom::prelude::*;
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
    pub fn write<T: Write>(self, buf: &mut T) -> Result<()> {
        match self {
            Self::Startup { params } => {
                let mut writer = vec![];

                writer.write_be(CODE_STARTUP_POSTGRESQLV3)?;

                for (key, value) in params {
                    writer.write(key.as_bytes())?;
                    writer.write_i8(0)?; // Delimiter

                    writer.write(value.as_bytes())?;
                    writer.write_i8(0)?; // Delimiter
                }

                writer.write_i8(0)?; // Delimiter

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
#[derive(Debug, PartialEq, Clone)]
pub struct Field {
    name: String,
    table_oid: i32,
    column_number: i16,
    type_oid: i32,
    type_length: i16,
    type_modifier: i32,
    format: i16,
}

#[derive(Debug, PartialEq, Clone)]
pub enum DescribeKind {
    Statement,
    Portal,
}

impl From<DescribeKind> for u8 {
    fn from(value: DescribeKind) -> Self {
        match value {
            DescribeKind::Statement => b'S',
            DescribeKind::Portal => b'P',
        }
    }
}

#[derive(Debug, PartialEq, Clone)]
pub enum Message {
    AuthenticationRequestMD5Password {
        salt: Vec<u8>,
    },
    AuthenticationOk,
    MD5HashedPasswordMessage {
        hash: String,
    },
    ReadyForQuery,
    SimpleQuery(String),
    ParameterStatus {
        key: String,
        value: String,
    },
    BackendKeyData {
        process_id: u32,
        secret_key: u32,
        additional: Vec<u8>,
    },
    RowDescription {
        fields: Vec<Field>,
    },
    DataRow {
        field_data: Vec<Vec<u8>>,
    },
    CommandComplete {
        tag: String,
    },
    Terminate,
    Parse {
        statement: String,
        query: String,
        params_types: Vec<u32>,
    },
    Describe {
        kind: DescribeKind,
        name: String,
    },
}

pub fn read_until_zero<T: Read>(stream: &mut T) -> Result<Vec<u8>> {
    let mut result = vec![];

    loop {
        let mut bytes = vec![0; 1];
        bytes = stream.read_exact(&mut bytes).map(|_| bytes)?;

        match bytes[..] {
            [0] => break,
            _ => result.extend_from_slice(&bytes[..]),
        }
    }

    Ok(result)
}

/// Higher order function to write a message with some arbitraty number bytes to a buffer
/// and automatically prefix it the char_tag and with the message length as a BigEndian 32 bit integer
fn write_message_with_prefixed_message_len<T: Write>(
    buf: &mut T,
    char_tag: CharTag,
    writer: Box<dyn Fn(&mut Vec<u8>) -> Result<()>>,
) -> Result<usize> {
    let mut body: Vec<u8> = vec![];
    (*writer)(&mut body)?;

    let mut written_bytes_count = 0;
    written_bytes_count += buf.write(&[char_tag.into()])?;
    written_bytes_count += buf.write_be(body.len() as i32 + 4)?;
    written_bytes_count += buf.write(&body[..])?;

    return Ok(written_bytes_count);
}

impl Message {
    pub fn write<T: Write>(self: Self, buf: &mut T) -> Result<usize> {
        match self {
            Self::AuthenticationOk => {
                let vec = vec![CharTag::Authentication.into(), 0, 0, 0, 8, 0, 0, 0, 0];
                buf.write(&vec[..]).map_err(|err| anyhow::anyhow!(err))
            }
            Self::ReadyForQuery => write_message_with_prefixed_message_len(
                buf,
                CharTag::ReadyForQuery,
                Box::new(move |body: &mut Vec<u8>| {
                    body.write(&[CharTag::EmptyQueryResponse.into()])?;
                    Ok(())
                }),
            ),
            Self::AuthenticationRequestMD5Password { salt } => {
                write_message_with_prefixed_message_len(
                    buf,
                    CharTag::Authentication,
                    Box::new(move |body| -> Result<()> {
                        body.write_be(5 as i32)?;
                        body.write(&salt[..])?;
                        Ok(())
                    }),
                )
            }
            Self::MD5HashedPasswordMessage { hash } => write_message_with_prefixed_message_len(
                buf,
                CharTag::Password,
                Box::new(move |body| -> Result<()> {
                    body.extend_from_slice(&hash.as_bytes());
                    body.push(0);
                    Ok(())
                }),
            ),
            Self::SimpleQuery(query) => write_message_with_prefixed_message_len(
                buf,
                CharTag::Query,
                Box::new(move |body| -> Result<()> {
                    body.extend_from_slice(&query.as_bytes());
                    body.push(0);
                    Ok(())
                }),
            ),
            Self::RowDescription { fields } => write_message_with_prefixed_message_len(
                buf,
                CharTag::RowDescription,
                Box::new(move |body| -> Result<()> {
                    body.write_be(fields.len() as i16)?;

                    for field in &fields {
                        body.write(field.name.as_bytes())?;
                        body.push(0);

                        body.write_be(field.table_oid)?;
                        body.write_be(field.column_number)?;
                        body.write_be(field.type_oid)?;
                        body.write_be(field.type_length)?;
                        body.write_be(field.type_modifier)?;
                        body.write_be(field.format)?;
                    }
                    Ok(())
                }),
            ),
            Self::DataRow { field_data } => write_message_with_prefixed_message_len(
                buf,
                CharTag::DataRowOrDescribe,
                Box::new(move |body| -> Result<()> {
                    body.write_be(field_data.len() as i16)?;

                    for data in &field_data {
                        body.write_be(data.len() as i32)?;
                        body.extend_from_slice(&data[..]);
                    }

                    Ok(())
                }),
            ),
            Self::CommandComplete { tag } => write_message_with_prefixed_message_len(
                buf,
                CharTag::CommandComplete,
                Box::new(move |body| -> Result<()> {
                    body.extend_from_slice(tag.as_bytes());
                    body.push(0);

                    Ok(())
                }),
            ),
            Self::Terminate => write_message_with_prefixed_message_len(
                buf,
                CharTag::Terminate,
                Box::new(move |body| -> Result<()> {
                    body.write_be(0 as i32)?;

                    Ok(())
                }),
            ),

            Self::ParameterStatus { key, value } => write_message_with_prefixed_message_len(
                buf,
                CharTag::ParameterStatus,
                Box::new(move |body| -> Result<()> {
                    body.extend_from_slice(key.as_bytes());
                    body.push(0);

                    body.extend_from_slice(value.as_bytes());
                    body.push(0);

                    Ok(())
                }),
            ),
            Self::BackendKeyData {
                process_id,
                secret_key,
                additional,
            } => write_message_with_prefixed_message_len(
                buf,
                CharTag::BackendKeyData,
                Box::new(move |body| -> Result<()> {
                    body.write_be(process_id as i32)?;
                    body.write_be(secret_key as i32)?;
                    body.extend_from_slice(&additional[..]);

                    Ok(())
                }),
            ),
            Self::Parse {
                statement,
                query,
                params_types,
            } => write_message_with_prefixed_message_len(
                buf,
                CharTag::Parse,
                Box::new(move |body| -> Result<()> {
                    body.extend_from_slice(query.as_bytes());
                    body.push(0);

                    body.extend_from_slice(statement.as_bytes());
                    body.push(0);

                    for param in &params_types {
                        body.write_be(*param)?;
                    }

                    Ok(())
                }),
            ),
            Self::Describe { name, kind } => write_message_with_prefixed_message_len(
                buf,
                CharTag::DataRowOrDescribe,
                Box::new(move |body| -> Result<()> {
                    body.push(kind.clone().into());

                    body.extend_from_slice(name.as_bytes());
                    body.push(0);

                    Ok(())
                }),
            ),
        }
    }

    pub fn read<T: Read>(stream: &mut T) -> Result<Self> {
        let tag = CharTag::read(stream)?;

        match tag {
            CharTag::Query => {
                let message_len: u32 = stream.read_be()?;

                let query_string_bytes = read_until_zero(stream)?;
                let query_string = String::from_utf8(query_string_bytes.clone())?;

                Ok(Self::SimpleQuery(query_string.to_string()))
            }
            CharTag::Authentication => {
                let message_len: u32 = stream.read_be()?;
                let method: u32 = stream.read_be()?;

                if method == 5 {
                    let mut bytes = vec![0 as u8; 4];
                    bytes = stream.read_exact(&mut bytes).map(|_| bytes)?;
                    return Ok(Self::AuthenticationRequestMD5Password { salt: bytes });
                }

                if method == 0 {
                    return Ok(Self::AuthenticationOk);
                }

                unimplemented!();
            }
            CharTag::Password => {
                let message_len: u32 = stream.read_be()?;

                let hash_bytes = read_until_zero(stream)?;
                let hash = String::from_utf8(hash_bytes.clone())?;

                Ok(Self::MD5HashedPasswordMessage { hash })
            }
            CharTag::ParameterStatus => {
                let message_len: u32 = stream.read_be()?;

                let key_bytes = read_until_zero(stream)?;
                let key = String::from_utf8(key_bytes)?;

                let value_bytes = read_until_zero(stream)?;
                let value = String::from_utf8(value_bytes)?;

                Ok(Self::ParameterStatus { key, value })
            }
            CharTag::BackendKeyData => {
                let message_len: u32 = stream.read_be()?;
                let process_id: u32 = stream.read_be()?;
                let secret_key: u32 = stream.read_be()?;

                let remaining_bytes = message_len - 12; // -4 for length u32, -4 process_id, -4 secret_key

                let mut bytes = vec![0; remaining_bytes as usize];
                bytes = stream.read_exact(&mut bytes).map(|_| bytes)?;

                Ok(Self::BackendKeyData {
                    process_id,
                    secret_key,
                    additional: bytes,
                })
            }
            CharTag::ReadyForQuery => {
                let message_len: u32 = stream.read_be()?;

                let string_len = message_len - 4; // -4 for length u32

                let mut bytes = vec![0; string_len as usize];
                bytes = stream.read_exact(&mut bytes).map(|_| bytes)?;

                // TODO: Use the parsed data

                Ok(Message::ReadyForQuery)
            }
            CharTag::RowDescription => {
                let message_len: u32 = stream.read_be()?;
                let num_fields: u16 = stream.read_be()?;

                let mut fields = vec![];

                while fields.len() < num_fields as usize {
                    let name_bytes = read_until_zero(stream)?;
                    let name = String::from_utf8(name_bytes.clone())?;

                    let table_oid: i32 = stream.read_be()?;
                    let column_number: i16 = stream.read_be()?;
                    let type_oid: i32 = stream.read_be()?;
                    let type_length: i16 = stream.read_be()?;
                    let type_modifier: i32 = stream.read_be()?;
                    let format: i16 = stream.read_be()?;

                    let field = Field {
                        name,
                        table_oid,
                        column_number,
                        type_oid,
                        type_length,
                        type_modifier,
                        format,
                    };

                    fields.push(field);
                }

                Ok(Message::RowDescription { fields })
            }
            CharTag::DataRowOrDescribe => {
                let message_len: u32 = stream.read_be()?;

                let mut bytes: Vec<u8> = vec![0; message_len as usize - 4];
                bytes = stream.read_exact(&mut bytes).map(|_| bytes)?;

                let describe_identifier = bytes[5];

                let mut cursor = std::io::Cursor::new(bytes);

                let num_fields: u16 = cursor.read_be()?;

                let mut fields = vec![];

                while fields.len() < num_fields as usize {
                    let field_len: u32 = cursor.read_be()?;

                    let mut field_bytes = vec![0; field_len as usize];
                    cursor.read_exact(&mut field_bytes)?;

                    fields.push(field_bytes[..].to_vec())
                }

                Ok(Message::DataRow { field_data: fields })
            }
            CharTag::CommandComplete => {
                let message_len: u32 = stream.read_be()?;

                let tag_bytes = read_until_zero(stream)?;
                let tag = String::from_utf8(tag_bytes.clone())?;

                Ok(Message::CommandComplete { tag })
            }
            CharTag::Terminate => {
                let message_len: u32 = stream.read_be()?;

                Ok(Message::Terminate)
            }
            CharTag::Parse => {
                let message_len: u32 = stream.read_be()?;

                let statement_bytes = read_until_zero(stream)?;
                let statement = String::from_utf8(statement_bytes.clone())?;

                let query_bytes = read_until_zero(stream)?;
                let query = String::from_utf8(query_bytes.clone())?;

                let num_param_types: u16 = stream.read_be()?;

                let mut params_types = vec![];

                while params_types.len() < num_param_types as usize {
                    let param_oid: u32 = stream.read_be()?;

                    params_types.push(param_oid)
                }

                Ok(Message::Parse {
                    statement,
                    query,
                    params_types,
                })
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
    Password,
    CommandComplete,
    RowDescription,
    DataRowOrDescribe,
    ParameterStatus,
    BackendKeyData,
    Terminate,
    Parse,
    Bind,
    ParameterDescription,
    Execute,
}

impl CharTag {
    pub fn read<T: Read>(stream: &mut T) -> Result<CharTag> {
        let mut bytes = vec![0; 1];
        stream.read(&mut bytes)?;

        let tag = CharTag::try_from(bytes[0]);

        println!(
            "Encountered Tag: {} ({:?})",
            String::from_utf8(bytes)?,
            match &tag {
                Ok(tag) => format!("{:?}", tag),
                Err(_) => "Error".to_string(),
            }
        );

        Ok(tag.expect("Could not read char tag"))
    }
}

impl From<CharTag> for u8 {
    fn from(value: CharTag) -> Self {
        match value {
            CharTag::Authentication => b'R',
            CharTag::ReadyForQuery => b'Z',
            CharTag::EmptyQueryResponse => b'I',
            CharTag::Query => b'Q',
            CharTag::Password => b'p',
            CharTag::RowDescription => b'T',
            CharTag::DataRowOrDescribe => b'D',
            CharTag::CommandComplete => b'C',
            CharTag::ParameterStatus => b'S',
            CharTag::BackendKeyData => b'K',
            CharTag::Terminate => b'X',
            CharTag::Parse => b'P',
            CharTag::Bind => b'B',
            CharTag::ParameterDescription => b't',
            CharTag::Execute => b'E',
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
            b'p' => Ok(CharTag::Password),
            b'T' => Ok(CharTag::RowDescription),
            b'D' => Ok(CharTag::DataRowOrDescribe),
            b'C' => Ok(CharTag::CommandComplete),
            b'S' => Ok(CharTag::ParameterStatus),
            b'K' => Ok(CharTag::BackendKeyData),
            b'X' => Ok(CharTag::Terminate),
            b'P' => Ok(CharTag::Parse),
            b'B' => Ok(CharTag::Bind),
            b't' => Ok(CharTag::ParameterDescription),
            b'E' => Ok(CharTag::Execute),
            _ => Err("Unknown char tag"),
        }
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

    #[test]
    fn parameter_status() {
        let mut buf = vec![];
        let message = Message::ParameterStatus {
            key: "Test Key".to_string(),
            value: "Test Value".to_string(),
        };

        message.clone().write(&mut buf).unwrap();
        let parsed = Message::read(&mut buf.as_slice()).unwrap();

        assert_eq!(parsed, message)
    }

    #[test]
    fn empty_backend_key_data() {
        let mut buf = vec![];
        let message = Message::BackendKeyData {
            process_id: 1,
            secret_key: 1,
            additional: vec![],
        };

        message.clone().write(&mut buf).unwrap();

        let parsed = Message::read(&mut buf.as_slice()).unwrap();

        assert_eq!(parsed, message)
    }

    #[test]
    fn ready_for_query() {
        let mut buf = vec![];
        let message = Message::ReadyForQuery;

        message.clone().write(&mut buf).unwrap();
        let parsed = Message::read(&mut buf.as_slice()).unwrap();

        assert_eq!(parsed, message)
    }

    #[test]
    fn row_description() {
        let mut buf = vec![];
        let message = Message::RowDescription {
            fields: vec![Field {
                name: "test".to_string(),
                column_number: 1,
                table_oid: -1,
                type_length: -1,
                type_modifier: -1,
                type_oid: -1,
                format: -1,
            }],
        };

        message.clone().write(&mut buf).unwrap();
        let parsed = Message::read(&mut buf.as_slice()).unwrap();

        assert_eq!(parsed, message)
    }
}
