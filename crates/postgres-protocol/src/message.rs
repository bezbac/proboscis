use super::util::{read_until_zero, write_message_with_prefixed_message_len};
use super::CharTag;
use anyhow::Result;
use byteorder::{ByteOrder, NetworkEndian};
use omnom::prelude::*;
use std::io::{prelude::*, Cursor};
use tokio::io::AsyncRead;
use tokio::io::AsyncReadExt;

#[derive(Debug, PartialEq, Clone)]
pub struct Field {
    pub name: String,
    pub table_oid: i32,
    pub column_number: i16,
    pub type_oid: i32,
    pub type_length: i16,
    pub type_modifier: i32,
    pub format: i16,
}

#[derive(Debug, PartialEq, Clone, Copy)]
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

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum CloseKind {
    Statement,
    Portal,
}

impl From<CloseKind> for u8 {
    fn from(value: CloseKind) -> Self {
        match value {
            CloseKind::Statement => b'S',
            CloseKind::Portal => b'P',
        }
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct DataRow {
    pub field_data: Vec<Vec<u8>>,
}

#[derive(Debug, PartialEq, Clone)]
pub struct RowDescription {
    pub fields: Vec<Field>,
}

#[derive(Debug, PartialEq, Clone)]
pub struct CommandCompleteTag(pub String);

#[derive(Debug, PartialEq, Clone)]
pub struct BackendKeyData {
    pub process_id: u32,
    pub secret_key: u32,
    pub additional: Vec<u8>,
}

#[derive(Debug, PartialEq, Clone)]

pub struct MD5Hash(pub String);

#[derive(Debug, PartialEq, Clone)]

pub struct MD5Salt(pub Vec<u8>);

#[derive(Debug, PartialEq, Clone)]
pub struct ParameterStatus {
    pub key: String,
    pub value: String,
}

#[derive(Debug, PartialEq, Clone)]
pub struct Parse {
    pub statement_name: String,
    pub query: String,
    pub param_types: Vec<u32>,
}

#[derive(Debug, PartialEq, Clone)]
pub struct Describe {
    pub kind: DescribeKind,
    pub name: String,
}

#[derive(Debug, PartialEq, Clone)]
pub struct Execute {
    pub portal: String,
    pub row_limit: i32,
}

#[derive(Debug, PartialEq, Clone)]
pub struct Close {
    pub kind: CloseKind,
    pub name: String,
}

#[derive(Debug, PartialEq, Clone)]
pub struct Bind {
    pub statement: String,
    pub portal: String,
    pub formats: Vec<i16>,
    pub params: Vec<Vec<u8>>,
    pub results: Vec<i16>,
}

#[derive(Debug, PartialEq, Clone)]
pub struct ParameterDescription {
    pub types: Vec<u32>,
}

#[derive(Debug, PartialEq, Clone)]
pub struct Error {
    pub messages: Vec<(u8, String)>,
}

#[derive(Debug, PartialEq, Clone)]
pub enum BackendMessage {
    AuthenticationRequestMD5Password(MD5Salt),
    AuthenticationOk,
    ReadyForQuery,
    ParameterStatus(ParameterStatus),
    BackendKeyData(BackendKeyData),
    RowDescription(RowDescription),
    DataRow(DataRow),
    CommandComplete(CommandCompleteTag),
    ParseComplete,
    BindComplete,
    CloseComplete,
    Error(Error),
    ParameterDescription(ParameterDescription),
    NoData,
}

#[derive(Debug, PartialEq, Clone)]
pub enum FrontendMessage {
    MD5HashedPassword(MD5Hash),
    SimpleQuery(String),
    Terminate,
    Parse(Parse),
    Describe(Describe),
    Bind(Bind),
    Execute(Execute),
    Close(Close),
    Sync,
}

#[derive(Debug, PartialEq, Clone)]
pub enum Message {
    Frontend(FrontendMessage),
    Backend(BackendMessage),
}

impl Message {
    pub fn as_vec(&self) -> Vec<u8> {
        let mut vec = vec![];
        self.clone().write(&mut vec).unwrap();
        vec
    }

    pub fn write<T: Write>(self, buf: &mut T) -> Result<usize> {
        match self {
            Self::Backend(message) => message.write(buf),
            Self::Frontend(message) => message.write(buf),
        }
    }
}

impl From<FrontendMessage> for Message {
    fn from(message: FrontendMessage) -> Self {
        Message::Frontend(message)
    }
}

impl From<BackendMessage> for Message {
    fn from(message: BackendMessage) -> Self {
        Message::Backend(message)
    }
}

async fn read_meta_async<T: AsyncRead + Unpin>(
    stream: &mut T,
) -> Result<(Cursor<Vec<u8>>, CharTag, u32)> {
    let mut bytes = [0; 1];
    stream.read_exact(&mut bytes).await?;
    let mut cursor = std::io::Cursor::new(bytes);
    let tag = CharTag::read(&mut cursor)?;

    let mut message_len_bytes = vec![0; 4];
    stream.read_exact(&mut message_len_bytes).await?;
    let message_length = NetworkEndian::read_u32(&message_len_bytes);

    let mut body_bytes = vec![0; message_length as usize - 4];
    stream.read_exact(&mut body_bytes).await?;

    let cursor = std::io::Cursor::new(body_bytes);

    Ok((cursor, tag, message_length))
}

fn read_meta<T: Read>(stream: &mut T) -> Result<(CharTag, u32)> {
    let tag = CharTag::read(stream)?;
    let message_len: u32 = stream.read_be()?;
    Ok((tag, message_len))
}

impl FrontendMessage {
    pub fn as_vec(&self) -> Vec<u8> {
        let mut vec = vec![];
        self.clone().write(&mut vec).unwrap();
        vec
    }

    pub fn write<T: Write>(self, buf: &mut T) -> Result<usize> {
        match self {
            Self::MD5HashedPassword(MD5Hash(hash)) => write_message_with_prefixed_message_len(
                buf,
                CharTag::Password,
                Box::new(move |body| -> Result<()> {
                    body.extend_from_slice(hash.as_bytes());
                    body.push(0);
                    Ok(())
                }),
            ),
            Self::SimpleQuery(query) => write_message_with_prefixed_message_len(
                buf,
                CharTag::Query,
                Box::new(move |body| -> Result<()> {
                    body.extend_from_slice(query.as_bytes());
                    body.push(0);
                    Ok(())
                }),
            ),
            Self::Terminate => write_message_with_prefixed_message_len(
                buf,
                CharTag::Terminate,
                Box::new(move |body| -> Result<()> {
                    body.write_be(0_i32)?;

                    Ok(())
                }),
            ),

            Self::Parse(Parse {
                statement_name: statement,
                query,
                param_types,
            }) => write_message_with_prefixed_message_len(
                buf,
                CharTag::Parse,
                Box::new(move |body| -> Result<()> {
                    body.extend_from_slice(statement.as_bytes());
                    body.push(0);

                    body.extend_from_slice(query.as_bytes());
                    body.push(0);

                    body.write_be(param_types.clone().len() as i16)?;

                    for param in &param_types {
                        body.write_be(*param)?;
                    }

                    Ok(())
                }),
            ),
            Self::Describe(Describe { name, kind }) => write_message_with_prefixed_message_len(
                buf,
                CharTag::DataRowOrDescribe,
                Box::new(move |body| -> Result<()> {
                    body.push(kind.into());

                    body.extend_from_slice(name.as_bytes());
                    body.push(0);

                    Ok(())
                }),
            ),
            Self::Execute(Execute { portal, row_limit }) => {
                write_message_with_prefixed_message_len(
                    buf,
                    CharTag::ExecuteOrError,
                    Box::new(move |body| -> Result<()> {
                        body.extend_from_slice(portal.as_bytes());
                        body.push(0);

                        body.write_be(row_limit)?;

                        Ok(())
                    }),
                )
            }
            Self::Sync => write_message_with_prefixed_message_len(
                buf,
                CharTag::ParameterStatusOrSync,
                Box::new(move |_| -> Result<()> { Ok(()) }),
            ),
            Self::Bind(Bind {
                portal,
                statement,
                params,
                formats,
                results,
            }) => write_message_with_prefixed_message_len(
                buf,
                CharTag::Bind,
                Box::new(move |body| -> Result<()> {
                    body.extend_from_slice(portal.as_bytes());
                    body.push(0);

                    body.extend_from_slice(statement.as_bytes());
                    body.push(0);

                    body.write_be((&formats).len() as i16)?;
                    for format in &formats {
                        body.write_be(*format)?;
                    }

                    body.write_be((&params).len() as i16)?;
                    for param in &params {
                        body.extend_from_slice(param);
                    }

                    body.write_be((&results).len() as i16)?;
                    for result_format in &results {
                        body.write_be(*result_format)?;
                    }

                    Ok(())
                }),
            ),
            Self::Close(Close { kind, name }) => write_message_with_prefixed_message_len(
                buf,
                CharTag::CommandCompleteOrClose,
                Box::new(move |body| -> Result<()> {
                    body.push(kind.into());
                    body.extend_from_slice(name.as_bytes());

                    Ok(())
                }),
            ),
        }
    }

    pub fn read<T: Read>(stream: &mut T) -> Result<Self> {
        let (tag, message_len) = read_meta(stream)?;
        Self::read_body(stream, tag, message_len - 4)
    }

    pub async fn read_async<T: AsyncRead + Unpin>(stream: &mut T) -> Result<Self> {
        let (mut cursor, tag, message_length) = read_meta_async(stream).await?;
        Self::read_body(&mut cursor, tag, message_length - 4)
    }

    fn read_body<T: Read>(stream: &mut T, tag: CharTag, remaining_bytes_len: u32) -> Result<Self> {
        match tag {
            CharTag::Query => {
                let query_string_bytes = read_until_zero(stream)?;
                let query_string = String::from_utf8(query_string_bytes)?;

                Ok(Self::SimpleQuery(query_string))
            }
            CharTag::Password => {
                let hash_bytes = read_until_zero(stream)?;
                let hash = String::from_utf8(hash_bytes)?;

                Ok(Self::MD5HashedPassword(MD5Hash(hash)))
            }
            CharTag::ParameterStatusOrSync => Ok(Self::Sync),
            CharTag::DataRowOrDescribe => {
                let mut bytes: Vec<u8> = vec![0; remaining_bytes_len as usize];
                bytes = stream.read_exact(&mut bytes).map(|_| bytes)?;

                let describe_identifier = bytes[0];

                let mut cursor = std::io::Cursor::new(&bytes[1..]);

                let kind = match describe_identifier {
                    b'S' => DescribeKind::Statement,
                    b'P' => DescribeKind::Portal,
                    _ => return Err(anyhow::anyhow!("Invalid describe kind")),
                };

                let name_bytes = read_until_zero(&mut cursor)?;
                let name = String::from_utf8(name_bytes)?;

                Ok(Self::Describe(Describe { kind, name }))
            }
            CharTag::CommandCompleteOrClose => {
                let tag_bytes = read_until_zero(stream)?;
                let tag = String::from_utf8(tag_bytes)?;

                let kind = if tag.starts_with('S') {
                    CloseKind::Statement
                } else if tag.starts_with('P') {
                    CloseKind::Portal
                } else {
                    return Err(anyhow::anyhow!(""));
                };

                Ok(Self::Close(Close {
                    kind,
                    name: tag[1..].to_string(),
                }))
            }
            CharTag::Terminate => Ok(Self::Terminate),
            CharTag::Parse => {
                let statement_bytes = read_until_zero(stream)?;
                let statement = String::from_utf8(statement_bytes)?;

                let query_bytes = read_until_zero(stream)?;
                let query = String::from_utf8(query_bytes)?;

                let mut param_types = vec![];
                let num_param_types: u16 = stream.read_be()?;
                while param_types.len() < num_param_types as usize {
                    let param_oid: u32 = stream.read_be()?;
                    param_types.push(param_oid)
                }

                Ok(Self::Parse(Parse {
                    statement_name: statement,
                    query,
                    param_types,
                }))
            }
            CharTag::ExecuteOrError => {
                let portal_bytes = read_until_zero(stream)?;
                let portal = String::from_utf8(portal_bytes)?;

                let row_limit: i32 = stream.read_be()?;

                Ok(Self::Execute(Execute { portal, row_limit }))
            }
            CharTag::Bind => {
                let portal_bytes = read_until_zero(stream)?;
                let portal = String::from_utf8(portal_bytes)?;

                let statement_bytes = read_until_zero(stream)?;
                let statement = String::from_utf8(statement_bytes)?;

                let mut formats = vec![];
                let num_formats: u16 = stream.read_be()?;
                while formats.len() < num_formats as usize {
                    let format: i16 = stream.read_be()?;
                    formats.push(format)
                }

                let mut params = vec![];
                let num_params: u16 = stream.read_be()?;
                while params.len() < num_params as usize {
                    let param_len: u32 = stream.read_be()?;

                    let mut param_bytes: Vec<u8> = vec![0; param_len as usize];
                    stream.read_exact(&mut param_bytes)?;
                    params.push(param_bytes);
                }

                let mut results = vec![];
                let num_results: u16 = stream.read_be()?;
                while results.len() < num_results as usize {
                    let result_format: i16 = stream.read_be()?;
                    results.push(result_format)
                }

                Ok(Self::Bind(Bind {
                    portal,
                    statement,
                    formats,
                    params,
                    results,
                }))
            }
            _ => todo!(),
        }
    }
}

impl BackendMessage {
    pub fn as_vec(&self) -> Vec<u8> {
        let mut vec = vec![];
        self.clone().write(&mut vec).unwrap();
        vec
    }

    pub fn write<T: Write>(self, buf: &mut T) -> Result<usize> {
        match self {
            Self::AuthenticationOk => {
                let vec = vec![CharTag::Authentication.into(), 0, 0, 0, 8, 0, 0, 0, 0];
                buf.write(&vec[..]).map_err(|err| anyhow::anyhow!(err))
            }
            Self::ReadyForQuery => write_message_with_prefixed_message_len(
                buf,
                CharTag::ReadyForQuery,
                Box::new(move |body: &mut Vec<u8>| {
                    body.write_all(&[CharTag::EmptyQueryResponse.into()])?;
                    Ok(())
                }),
            ),
            Self::AuthenticationRequestMD5Password(MD5Salt(salt)) => {
                write_message_with_prefixed_message_len(
                    buf,
                    CharTag::Authentication,
                    Box::new(move |body| -> Result<()> {
                        body.write_be(5_i32)?;
                        body.write_all(&salt[..])?;
                        Ok(())
                    }),
                )
            }
            Self::RowDescription(RowDescription { fields }) => {
                write_message_with_prefixed_message_len(
                    buf,
                    CharTag::RowDescription,
                    Box::new(move |body| -> Result<()> {
                        body.write_be(fields.len() as i16)?;

                        for field in &fields {
                            body.write_all(field.name.as_bytes())?;
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
                )
            }
            Self::DataRow(DataRow { field_data }) => write_message_with_prefixed_message_len(
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
            Self::CommandComplete(CommandCompleteTag(tag)) => {
                write_message_with_prefixed_message_len(
                    buf,
                    CharTag::CommandCompleteOrClose,
                    Box::new(move |body| -> Result<()> {
                        body.extend_from_slice(tag.as_bytes());
                        body.push(0);

                        Ok(())
                    }),
                )
            }
            Self::ParameterStatus(ParameterStatus { key, value }) => {
                write_message_with_prefixed_message_len(
                    buf,
                    CharTag::ParameterStatusOrSync,
                    Box::new(move |body| -> Result<()> {
                        body.extend_from_slice(key.as_bytes());
                        body.push(0);

                        body.extend_from_slice(value.as_bytes());
                        body.push(0);

                        Ok(())
                    }),
                )
            }
            Self::BackendKeyData(BackendKeyData {
                process_id,
                secret_key,
                additional,
            }) => write_message_with_prefixed_message_len(
                buf,
                CharTag::BackendKeyData,
                Box::new(move |body| -> Result<()> {
                    body.write_be(process_id as i32)?;
                    body.write_be(secret_key as i32)?;
                    body.extend_from_slice(&additional[..]);

                    Ok(())
                }),
            ),
            Self::ParseComplete => write_message_with_prefixed_message_len(
                buf,
                CharTag::ParseComplete,
                Box::new(move |_| -> Result<()> { Ok(()) }),
            ),
            Self::BindComplete => write_message_with_prefixed_message_len(
                buf,
                CharTag::BindComplete,
                Box::new(move |_| -> Result<()> { Ok(()) }),
            ),
            Self::ParameterDescription(ParameterDescription { types }) => {
                write_message_with_prefixed_message_len(
                    buf,
                    CharTag::ParameterDescription,
                    Box::new(move |body| -> Result<()> {
                        body.write_be((&types).len() as i16)?;

                        for param in &types {
                            body.write_be(*param)?;
                        }

                        Ok(())
                    }),
                )
            }
            Self::CloseComplete => write_message_with_prefixed_message_len(
                buf,
                CharTag::CloseComplete,
                Box::new(move |_| -> Result<()> { Ok(()) }),
            ),
            Self::NoData => write_message_with_prefixed_message_len(
                buf,
                CharTag::NoData,
                Box::new(move |_| -> Result<()> { Ok(()) }),
            ),
            Self::Error(messages) => {
                unimplemented!()
            }
        }
    }

    pub fn read<T: Read>(stream: &mut T) -> Result<Self> {
        let (tag, message_len) = read_meta(stream)?;
        Self::read_body(stream, tag, message_len - 4)
    }

    pub async fn read_async<T: AsyncRead + Unpin>(stream: &mut T) -> Result<Self> {
        let (mut cursor, tag, message_length) = read_meta_async(stream).await?;
        Self::read_body(&mut cursor, tag, message_length - 4)
    }

    fn read_body<T: Read>(stream: &mut T, tag: CharTag, remaining_bytes_len: u32) -> Result<Self> {
        match tag {
            CharTag::Authentication => {
                let method: u32 = stream.read_be()?;

                if method == 5 {
                    let mut bytes = vec![0_u8; 4];
                    bytes = stream.read_exact(&mut bytes).map(|_| bytes)?;
                    return Ok(Self::AuthenticationRequestMD5Password(MD5Salt(bytes)));
                }

                if method == 0 {
                    return Ok(Self::AuthenticationOk);
                }

                unimplemented!();
            }
            CharTag::ParameterStatusOrSync => {
                let key_bytes = read_until_zero(stream)?;
                let key = String::from_utf8(key_bytes)?;

                let value_bytes = read_until_zero(stream)?;
                let value = String::from_utf8(value_bytes)?;

                Ok(Self::ParameterStatus(ParameterStatus { key, value }))
            }
            CharTag::BackendKeyData => {
                let process_id: u32 = stream.read_be()?;
                let secret_key: u32 = stream.read_be()?;

                let remaining_bytes = remaining_bytes_len - 8; // -4 process_id, -4 secret_key

                let mut bytes = vec![0; remaining_bytes as usize];
                bytes = stream.read_exact(&mut bytes).map(|_| bytes)?;

                Ok(Self::BackendKeyData(BackendKeyData {
                    process_id,
                    secret_key,
                    additional: bytes,
                }))
            }
            CharTag::ReadyForQuery => {
                let mut bytes = vec![0; remaining_bytes_len as usize];
                stream.read_exact(&mut bytes).map(|_| bytes)?;

                // TODO: Use the parsed data

                Ok(Self::ReadyForQuery)
            }
            CharTag::RowDescription => {
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

                Ok(Self::RowDescription(RowDescription { fields }))
            }
            CharTag::DataRowOrDescribe => {
                let num_fields: u16 = stream.read_be()?;

                let mut field_data = vec![];
                for _ in 0..num_fields {
                    let field_len: i32 = stream.read_be()?;

                    let mut field_bytes = vec![0; field_len as usize];
                    stream.read_exact(&mut field_bytes)?;

                    field_data.push(field_bytes)
                }

                Ok(Self::DataRow(DataRow { field_data }))
            }
            CharTag::CommandCompleteOrClose => {
                let tag_bytes = read_until_zero(stream)?;
                let tag = String::from_utf8(tag_bytes)?;

                Ok(Self::CommandComplete(CommandCompleteTag(tag)))
            }
            CharTag::ExecuteOrError => {
                let mut messages = vec![];
                while let Ok(identifier) = stream.read_be::<u8>() {
                    match identifier {
                        0 => break,
                        _ => {
                            let message_bytes = read_until_zero(stream)?;
                            let message = String::from_utf8(message_bytes)?;
                            messages.push((identifier, message))
                        }
                    }
                }

                Ok(Self::Error(Error { messages }))
            }
            CharTag::ParseComplete => Ok(Self::ParseComplete),
            CharTag::BindComplete => Ok(Self::BindComplete),
            CharTag::ParameterDescription => {
                let num_param_types: u16 = stream.read_be()?;

                let mut types = vec![];

                while types.len() < num_param_types as usize {
                    let param_oid: u32 = stream.read_be()?;
                    types.push(param_oid)
                }

                Ok(Self::ParameterDescription(ParameterDescription { types }))
            }
            CharTag::CloseComplete => Ok(Self::CloseComplete),
            CharTag::EmptyQueryResponse => unimplemented!(),
            CharTag::NoData => Ok(Self::NoData),
            _ => todo!(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_frontend_symmetric_serialization_deserialization(message: Message) {
        let mut buf = vec![];
        let mut cursor = std::io::Cursor::new(&mut buf);
        message.clone().write(&mut cursor).unwrap();
        let bytes_written = cursor.position();

        cursor.set_position(0);
        let parsed: Message = FrontendMessage::read(&mut cursor).unwrap().into();
        let bytes_read = cursor.position();

        assert_eq!(parsed, message);
        assert_eq!(bytes_read, bytes_written);
    }

    fn test_backend_symmetric_serialization_deserialization(message: Message) {
        let mut buf = vec![];
        let mut cursor = std::io::Cursor::new(&mut buf);
        message.clone().write(&mut cursor).unwrap();
        let bytes_written = cursor.position();

        cursor.set_position(0);
        let parsed: Message = BackendMessage::read(&mut cursor).unwrap().into();
        let bytes_read = cursor.position();

        assert_eq!(parsed, message);
        assert_eq!(bytes_read, bytes_written);
    }

    #[test]
    fn parameter_status() {
        let message = BackendMessage::ParameterStatus(ParameterStatus {
            key: "Test Key".to_string(),
            value: "Test Value".to_string(),
        });

        test_backend_symmetric_serialization_deserialization(message.into());
    }

    #[test]
    fn empty_backend_key_data() {
        let message = BackendMessage::BackendKeyData(BackendKeyData {
            process_id: 1,
            secret_key: 1,
            additional: vec![],
        });

        test_backend_symmetric_serialization_deserialization(message.into());
    }

    #[test]
    fn ready_for_query() {
        let message = BackendMessage::ReadyForQuery;

        test_backend_symmetric_serialization_deserialization(message.into());
    }

    #[test]
    fn row_description() {
        let message = BackendMessage::RowDescription(RowDescription {
            fields: vec![Field {
                name: "test".to_string(),
                column_number: 1,
                table_oid: -1,
                type_length: -1,
                type_modifier: -1,
                type_oid: -1,
                format: -1,
            }],
        });

        test_backend_symmetric_serialization_deserialization(message.into());
    }

    #[test]
    fn describe_statement() {
        let message = FrontendMessage::Describe(Describe {
            kind: DescribeKind::Statement,
            name: "Test".to_string(),
        });

        test_frontend_symmetric_serialization_deserialization(message.into());
    }

    #[test]
    fn describe_portal() {
        let message = FrontendMessage::Describe(Describe {
            kind: DescribeKind::Portal,
            name: "Test".to_string(),
        });

        test_frontend_symmetric_serialization_deserialization(message.into());
    }

    #[test]
    fn execute() {
        let message = FrontendMessage::Execute(Execute {
            portal: "Test".to_string(),
            row_limit: 0,
        });

        test_frontend_symmetric_serialization_deserialization(message.into());
    }

    #[test]
    fn parse() {
        let message = FrontendMessage::Parse(Parse {
            statement_name: "s0".to_string(),
            query: "SELECT id, name FROM person".to_string(),
            param_types: vec![],
        });

        test_frontend_symmetric_serialization_deserialization(message.into());
    }
}
