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
    pub fn write<T: Write>(self, buf: &mut T) -> Result<()> {
        match self {
            Self::Startup { params } => {
                let mut writer = vec![];

                writer.write_i32::<BigEndian>(CODE_STARTUP_POSTGRESQLV3)?;

                for (key, value) in params {
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
#[derive(Debug)]
pub struct Field {
    name: String,
    table_oid: i32,
    column_number: i16,
    type_oid: i32,
    type_length: i16,
    type_modifier: i32,
    format: i16,
}

#[derive(Debug)]
pub enum Message {
    AuthenticationRequestMD5Password { salt: Vec<u8> },
    AuthenticationOk,
    MD5HashedPasswordMessage { hash: String },
    ReadyForQuery,
    SimpleQuery(String),
    ParameterStatus,
    BackendKeyData,
    RowDescription { fields: Vec<Field> },
    DataRow { field_data: Vec<Vec<u8>> },
    CommandComplete { tag: String },
    Terminate,
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

impl Message {
    pub fn as_vec(&self) -> Vec<u8> {
        match self {
            Self::AuthenticationOk => vec![CharTag::Authentication.into(), 0, 0, 0, 8, 0, 0, 0, 0],
            Self::ReadyForQuery => vec![
                CharTag::ReadyForQuery.into(),
                0,
                0,
                0,
                5,
                CharTag::EmptyQueryResponse.into(),
            ],
            Self::AuthenticationRequestMD5Password { salt } => {
                let mut result = vec![CharTag::Authentication.into(), 0, 0, 0, 12, 0, 0, 0, 5];
                result.extend_from_slice(salt);
                result
            }
            Self::MD5HashedPasswordMessage { hash } => {
                let mut result = vec![CharTag::Password.into()];

                let hash_bytes = hash.as_bytes();

                let message_len = hash_bytes.len() as u32 + 4 + 1; // +1 for delimiter, +4 for message len u32
                result.write_u32::<BigEndian>(message_len).unwrap();

                result.extend_from_slice(&hash_bytes);
                result.push(0);

                result
            }
            Self::SimpleQuery(query) => {
                let mut result = vec![CharTag::Query.into()];

                let query_bytes = query.as_bytes();

                let message_len = query_bytes.len() as u32 + 4 + 1; // +1 for delimiter, +4 for message len u32
                result.write_u32::<BigEndian>(message_len).unwrap();

                result.extend_from_slice(&query_bytes);
                result.push(0);

                result
            }
            Self::RowDescription { fields } => {
                let mut result = vec![CharTag::RowDescription.into()];

                let mut body = vec![];

                body.write_i16::<BigEndian>(fields.len() as i16).unwrap();
                for field in fields {
                    body.write(field.name.as_bytes()).unwrap();
                    body.write_i32::<BigEndian>(field.table_oid).unwrap();
                    body.write_i16::<BigEndian>(field.column_number).unwrap();
                    body.write_i32::<BigEndian>(field.type_oid).unwrap();
                    body.write_i16::<BigEndian>(field.type_length).unwrap();
                    body.write_i32::<BigEndian>(field.type_modifier).unwrap();
                    body.write_i16::<BigEndian>(field.format).unwrap();
                }

                result
                    .write_i32::<BigEndian>(body.len() as i32 + 4) // + 4 for message_len (this i32)
                    .unwrap();
                result.extend_from_slice(&body[..]);

                result
            }
            Self::DataRow { field_data } => {
                let mut result = vec![CharTag::DataRow.into()];

                let mut body = vec![];

                body.write_i16::<BigEndian>(field_data.len() as i16)
                    .unwrap();

                for data in field_data {
                    body.write_i32::<BigEndian>(data.len() as i32).unwrap();
                    body.extend_from_slice(&data[..]);
                }

                result
                    .write_i32::<BigEndian>(body.len() as i32 + 4) // + 4 for message_len (this i32)
                    .unwrap();
                result.extend_from_slice(&body[..]);

                result
            }
            Self::CommandComplete { tag } => {
                let mut result = vec![CharTag::CommandComplete.into()];

                result
                    .write_i32::<BigEndian>(tag.as_bytes().len() as i32 + 4 + 1) // + 4 for message_len, +1 for 0 byte
                    .unwrap();

                result.extend_from_slice(tag.as_bytes());
                result.push(0);

                result
            }
            Self::Terminate => {
                let mut result = vec![CharTag::Terminate.into()];

                result.write_i32::<BigEndian>(0).unwrap();

                result
            }
            _ => unimplemented!("as_vec not implemented for this messsage"),
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
                let mut bytes = vec![0; 4];
                bytes = stream.read_exact(&mut bytes).map(|_| bytes)?;
                let message_len = u32::try_from(NetworkEndian::read_u32(&bytes))?;

                let string_len = message_len - 4 - 1; // -4 for length u32, -1 for trailing delimiter

                let mut bytes = vec![0; string_len as usize];
                bytes = stream.read_exact(&mut bytes).map(|_| bytes)?;

                let password_hash = std::str::from_utf8(&bytes[..])?;

                // Read another byte (should be a zero)
                let mut bytes = vec![0; 1];
                stream.read_exact(&mut bytes)?;

                Ok(Self::MD5HashedPasswordMessage {
                    hash: password_hash.to_string(),
                })
            }
            CharTag::ParameterStatus => {
                let mut bytes = vec![0; 4];
                bytes = stream.read_exact(&mut bytes).map(|_| bytes)?;
                let message_len = u32::try_from(NetworkEndian::read_u32(&bytes))?;

                let string_len = message_len - 4; // -4 for length u32

                let mut bytes = vec![0; string_len as usize];
                bytes = stream.read_exact(&mut bytes).map(|_| bytes)?;

                // TODO: Parse the message

                Ok(Self::ParameterStatus)
            }
            CharTag::BackendKeyData => {
                let mut bytes = vec![0; 4];
                bytes = stream.read_exact(&mut bytes).map(|_| bytes)?;
                let message_len = u32::try_from(NetworkEndian::read_u32(&bytes))?;

                let mut bytes = vec![0; 4];
                bytes = stream.read_exact(&mut bytes).map(|_| bytes)?;
                let process_id = u32::try_from(NetworkEndian::read_u32(&bytes))?;

                let mut bytes = vec![0; 4];
                bytes = stream.read_exact(&mut bytes).map(|_| bytes)?;
                let secret_key = u32::try_from(NetworkEndian::read_u32(&bytes))?;

                let string_len = message_len - 12; // -4 for length u32, -4 process_id, -4 secret_key

                let mut bytes = vec![0; string_len as usize];
                bytes = stream.read_exact(&mut bytes).map(|_| bytes)?;

                // TODO: Use the parsed data

                Ok(Self::BackendKeyData)
            }
            CharTag::ReadyForQuery => {
                let mut bytes = vec![0; 4];
                bytes = stream.read_exact(&mut bytes).map(|_| bytes)?;
                let message_len = u32::try_from(NetworkEndian::read_u32(&bytes))?;

                let string_len = message_len - 4; // -4 for length u32

                let mut bytes = vec![0; string_len as usize];
                bytes = stream.read_exact(&mut bytes).map(|_| bytes)?;

                // TODO: Use the parsed data

                Ok(Message::ReadyForQuery)
            }
            CharTag::RowDescription => {
                let mut bytes = vec![0; 4];
                bytes = stream.read_exact(&mut bytes).map(|_| bytes)?;
                let message_len = u32::try_from(NetworkEndian::read_u32(&bytes))?;

                let mut bytes = vec![0; 2];
                bytes = stream.read_exact(&mut bytes).map(|_| bytes)?;
                let num_fields = u16::try_from(NetworkEndian::read_u16(&bytes))?;

                let mut fields = vec![];

                while fields.len() < num_fields as usize {
                    let name_bytes = read_until_zero(stream)?;

                    let mut bytes = vec![0; 4];
                    bytes = stream.read_exact(&mut bytes).map(|_| bytes)?;
                    let table_oid = i32::try_from(NetworkEndian::read_i32(&bytes))?;

                    let mut bytes = vec![0; 2];
                    bytes = stream.read_exact(&mut bytes).map(|_| bytes)?;
                    let column_number = i16::try_from(NetworkEndian::read_i16(&bytes))?;

                    let mut bytes = vec![0; 4];
                    bytes = stream.read_exact(&mut bytes).map(|_| bytes)?;
                    let type_oid = i32::try_from(NetworkEndian::read_i32(&bytes))?;

                    let mut bytes = vec![0; 2];
                    bytes = stream.read_exact(&mut bytes).map(|_| bytes)?;
                    let type_length = i16::try_from(NetworkEndian::read_i16(&bytes))?;

                    let mut bytes = vec![0; 4];
                    bytes = stream.read_exact(&mut bytes).map(|_| bytes)?;
                    let type_modifier = i32::try_from(NetworkEndian::read_i32(&bytes))?;

                    let mut bytes = vec![0; 2];
                    bytes = stream.read_exact(&mut bytes).map(|_| bytes)?;
                    let format = i16::try_from(NetworkEndian::read_i16(&bytes))?;

                    let field = Field {
                        name: String::from_utf8(name_bytes.clone())?,
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
            CharTag::DataRow => {
                let mut bytes = vec![0; 4];
                bytes = stream.read_exact(&mut bytes).map(|_| bytes)?;
                let message_len = u32::try_from(NetworkEndian::read_u32(&bytes))?;

                let mut bytes = vec![0; 2];
                bytes = stream.read_exact(&mut bytes).map(|_| bytes)?;
                let num_fields = u16::try_from(NetworkEndian::read_u16(&bytes))?;

                let mut fields = vec![];

                while fields.len() < num_fields as usize {
                    let mut bytes = vec![0; 4];
                    bytes = stream.read_exact(&mut bytes).map(|_| bytes)?;
                    let field_len = u32::try_from(NetworkEndian::read_u32(&bytes))?;

                    let mut bytes = vec![0; field_len as usize];
                    bytes = stream.read_exact(&mut bytes).map(|_| bytes)?;

                    fields.push(bytes[..].to_vec())
                }

                Ok(Message::DataRow { field_data: fields })
            }
            CharTag::CommandComplete => {
                let mut bytes = vec![0; 4];
                bytes = stream.read_exact(&mut bytes).map(|_| bytes)?;
                let message_len = u32::try_from(NetworkEndian::read_u32(&bytes))?;

                let string_len = message_len - 4 - 1; // -4 for length u32, -1 for delimiter

                let mut bytes = vec![0; string_len as usize];
                bytes = stream.read_exact(&mut bytes).map(|_| bytes)?;
                let tag = String::from_utf8(bytes)?;

                // Read another byte (should be a zero)
                let mut bytes = vec![0; 1];
                stream.read_exact(&mut bytes)?;

                Ok(Message::CommandComplete { tag })
            }
            CharTag::Terminate => {
                let mut bytes = vec![0; 4];
                bytes = stream.read_exact(&mut bytes).map(|_| bytes)?;
                let message_len = u32::try_from(NetworkEndian::read_u32(&bytes))?;

                Ok(Message::Terminate)
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
    DataRow,
    ParameterStatus,
    BackendKeyData,
    Terminate,
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
            CharTag::DataRow => b'D',
            CharTag::CommandComplete => b'C',
            CharTag::ParameterStatus => b'S',
            CharTag::BackendKeyData => b'K',
            CharTag::Terminate => b'X',
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
            b'D' => Ok(CharTag::DataRow),
            b'C' => Ok(CharTag::CommandComplete),
            b'S' => Ok(CharTag::ParameterStatus),
            b'K' => Ok(CharTag::BackendKeyData),
            b'X' => Ok(CharTag::Terminate),
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
}
