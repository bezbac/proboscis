use crate::ParseError;

use super::util::{read_until_zero, write_message_with_prefixed_message_len};
use super::CharTag;
use std::convert::TryFrom;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

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
    pub field_data: Vec<Option<Vec<u8>>>,
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
pub enum BindParameter {
    Binary(Vec<u8>),
    Text(String),
}

#[derive(Debug, PartialEq, Clone)]
pub struct Bind {
    pub statement: String,
    pub portal: String,
    pub params: Vec<BindParameter>,
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
pub enum ReadyForQueryTransactionStatus {
    NotInTransaction,
    InTransaction,
    InFailedTransaction,
}

impl TryFrom<u8> for ReadyForQueryTransactionStatus {
    type Error = ParseError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            b'I' => Ok(Self::NotInTransaction),
            b'T' => Ok(Self::InTransaction),
            b'E' => Ok(Self::InFailedTransaction),
            _ => Err(ParseError::UnknownTransactionStatus {
                char: value as char,
            }),
        }
    }
}

impl From<ReadyForQueryTransactionStatus> for u8 {
    fn from(value: ReadyForQueryTransactionStatus) -> Self {
        match value {
            ReadyForQueryTransactionStatus::NotInTransaction => b'I',
            ReadyForQueryTransactionStatus::InTransaction => b'T',
            ReadyForQueryTransactionStatus::InFailedTransaction => b'E',
        }
    }
}

#[derive(Debug, PartialEq, Clone)]
pub enum BackendMessage {
    AuthenticationRequestMD5Password(MD5Salt),
    AuthenticationOk,
    ReadyForQuery(ReadyForQueryTransactionStatus),
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
    EmptyQueryResponse,
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
    pub async fn write<T: AsyncWrite + std::marker::Unpin>(
        self,
        buf: &mut T,
    ) -> tokio::io::Result<usize> {
        match self {
            Self::Backend(message) => message.write(buf).await,
            Self::Frontend(message) => message.write(buf).await,
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
) -> Result<(CharTag, u32), ParseError> {
    let tag = AsyncReadExt::read_u8(stream).await?;
    let tag = CharTag::try_from(tag)?;
    let message_length = AsyncReadExt::read_u32(stream).await?;
    Ok((tag, message_length))
}

impl FrontendMessage {
    pub async fn write<T: AsyncWrite + std::marker::Unpin>(
        self,
        buf: &mut T,
    ) -> tokio::io::Result<usize> {
        match self {
            Self::MD5HashedPassword(MD5Hash(hash)) => {
                let mut body = vec![];
                body.extend_from_slice(hash.as_bytes());
                body.push(0);

                write_message_with_prefixed_message_len(buf, CharTag::Password, &body).await
            }
            Self::SimpleQuery(query) => {
                let mut body = vec![];
                body.extend_from_slice(query.as_bytes());
                body.push(0);

                write_message_with_prefixed_message_len(buf, CharTag::Query, &body).await
            }
            Self::Terminate => {
                let mut body = vec![];
                body.write_i32(0).await?;

                write_message_with_prefixed_message_len(buf, CharTag::Terminate, &body).await
            }

            Self::Parse(Parse {
                statement_name: statement,
                query,
                param_types,
            }) => {
                let mut body = vec![];

                body.extend_from_slice(statement.as_bytes());
                body.push(0);

                body.extend_from_slice(query.as_bytes());
                body.push(0);

                body.write_i16(param_types.clone().len() as i16).await?;

                for param in &param_types {
                    body.write_u32(*param).await?;
                }

                write_message_with_prefixed_message_len(buf, CharTag::Parse, &body).await
            }
            Self::Describe(Describe { name, kind }) => {
                let mut body = vec![kind.into()];

                body.extend_from_slice(name.as_bytes());
                body.push(0);

                write_message_with_prefixed_message_len(buf, CharTag::DataRowOrDescribe, &body)
                    .await
            }
            Self::Execute(Execute { portal, row_limit }) => {
                let mut body = vec![];

                body.extend_from_slice(portal.as_bytes());
                body.push(0);

                body.write_i32(row_limit).await?;

                write_message_with_prefixed_message_len(buf, CharTag::ExecuteOrError, &body).await
            }
            Self::Sync => {
                write_message_with_prefixed_message_len(buf, CharTag::ParameterStatusOrSync, &[])
                    .await
            }
            Self::Bind(Bind {
                portal,
                statement,
                params,
                results,
            }) => {
                let mut body = vec![];

                body.extend_from_slice(portal.as_bytes());
                body.push(0);

                body.extend_from_slice(statement.as_bytes());
                body.push(0);

                let are_all_parameters_text =
                    params.iter().all(|p| matches!(p, BindParameter::Text(_)));

                if params.is_empty() || are_all_parameters_text {
                    body.write_i16(0_i16).await?;
                } else {
                    body.write_i16(params.len() as i16).await?;
                    for param in &params {
                        body.write_i16(match param {
                            BindParameter::Text(_) => 0_i16,
                            BindParameter::Binary(_) => 1_i16,
                        })
                        .await?;
                    }
                };

                body.write_i16(params.len() as i16).await?;
                for param in &params {
                    let bytes = match param {
                        BindParameter::Text(string) => string.as_bytes(),
                        BindParameter::Binary(data) => data,
                    };

                    body.write_i32(bytes.len() as i32).await?;
                    body.extend_from_slice(bytes);
                }

                body.write_i16(results.len() as i16).await?;
                for result_format in &results {
                    body.write_i16(*result_format).await?;
                }

                write_message_with_prefixed_message_len(buf, CharTag::Bind, &body).await
            }
            Self::Close(Close { kind, name }) => {
                let mut body = vec![kind.into()];

                body.extend_from_slice(name.as_bytes());

                write_message_with_prefixed_message_len(buf, CharTag::CommandCompleteOrClose, &body)
                    .await
            }
        }
    }

    pub async fn read<T: AsyncRead + Unpin>(stream: &mut T) -> Result<Self, ParseError> {
        let (tag, message_length) = read_meta_async(stream).await?;
        Self::read_body(stream, tag, message_length - 4).await
    }

    async fn read_body<T: AsyncRead + Unpin>(
        stream: &mut T,
        tag: CharTag,
        remaining_bytes_len: u32,
    ) -> Result<Self, ParseError> {
        match tag {
            CharTag::Query => {
                let query_string_bytes = read_until_zero(stream).await?;
                let query_string = String::from_utf8(query_string_bytes)?;

                Ok(Self::SimpleQuery(query_string))
            }
            CharTag::Password => {
                let hash_bytes = read_until_zero(stream).await?;
                let hash = String::from_utf8(hash_bytes)?;

                Ok(Self::MD5HashedPassword(MD5Hash(hash)))
            }
            CharTag::ParameterStatusOrSync => Ok(Self::Sync),
            CharTag::DataRowOrDescribe => {
                let mut bytes: Vec<u8> = vec![0; remaining_bytes_len as usize];
                bytes = stream.read_exact(&mut bytes).await.map(|_| bytes)?;

                let describe_identifier = bytes[0];

                let mut cursor = std::io::Cursor::new(&bytes[1..]);

                let kind = match describe_identifier {
                    b'S' => DescribeKind::Statement,
                    b'P' => DescribeKind::Portal,
                    _ => {
                        return Err(ParseError::InvalidDescribeKind {
                            char: describe_identifier as char,
                        })
                    }
                };

                let name_bytes = read_until_zero(&mut cursor).await?;
                let name = String::from_utf8(name_bytes)?;

                Ok(Self::Describe(Describe { kind, name }))
            }
            CharTag::CommandCompleteOrClose => {
                let tag_bytes = read_until_zero(stream).await?;
                let tag = String::from_utf8(tag_bytes)?;

                let kind = match tag.chars().next() {
                    Some('S') => CloseKind::Statement,
                    Some('P') => CloseKind::Portal,
                    _ => return Err(ParseError::InvalidCloseKind { tag }),
                };

                Ok(Self::Close(Close {
                    kind,
                    name: tag[1..].to_string(),
                }))
            }
            CharTag::Terminate => Ok(Self::Terminate),
            CharTag::Parse => {
                let statement_bytes = read_until_zero(stream).await?;
                let statement = String::from_utf8(statement_bytes)?;

                let query_bytes = read_until_zero(stream).await?;
                let query = String::from_utf8(query_bytes)?;

                let mut param_types = vec![];
                let num_param_types: u16 = AsyncReadExt::read_u16(stream).await?;
                while param_types.len() < num_param_types as usize {
                    let param_oid: u32 = AsyncReadExt::read_u32(stream).await?;
                    param_types.push(param_oid)
                }

                Ok(Self::Parse(Parse {
                    statement_name: statement,
                    query,
                    param_types,
                }))
            }
            CharTag::ExecuteOrError => {
                let portal_bytes = read_until_zero(stream).await?;
                let portal = String::from_utf8(portal_bytes)?;

                let row_limit: i32 = AsyncReadExt::read_i32(stream).await?;

                Ok(Self::Execute(Execute { portal, row_limit }))
            }
            CharTag::Bind => {
                let portal_bytes = read_until_zero(stream).await?;
                let portal = String::from_utf8(portal_bytes)?;

                let statement_bytes = read_until_zero(stream).await?;
                let statement = String::from_utf8(statement_bytes)?;

                let mut formats = vec![];
                let num_formats: u16 = AsyncReadExt::read_u16(stream).await?;
                while formats.len() < num_formats as usize {
                    let format: i16 = AsyncReadExt::read_i16(stream).await?;
                    formats.push(format)
                }

                let mut params: Vec<BindParameter> = vec![];
                let num_params: u16 = AsyncReadExt::read_u16(stream).await?;
                for index in 0..num_params {
                    // 0 means text, 1 means binary
                    let format = if !formats.is_empty() {
                        formats[index as usize]
                    } else {
                        0
                    };

                    let param_len: u32 = AsyncReadExt::read_u32(stream).await?;

                    let mut param_bytes: Vec<u8> = vec![0; param_len as usize];
                    stream.read_exact(&mut param_bytes).await?;

                    let param = match format {
                        0 => BindParameter::Text(String::from_utf8(param_bytes)?),
                        1 => BindParameter::Binary(param_bytes),
                        _ => return Err(ParseError::InvalidBindParameterFormat),
                    };

                    params.push(param)
                }

                let mut results = vec![];
                let num_results: u16 = AsyncReadExt::read_u16(stream).await?;
                while results.len() < num_results as usize {
                    let result_format: i16 = AsyncReadExt::read_i16(stream).await?;
                    results.push(result_format)
                }

                Ok(Self::Bind(Bind {
                    portal,
                    statement,
                    params,
                    results,
                }))
            }
            _ => todo!(),
        }
    }
}

impl BackendMessage {
    pub async fn write<T: AsyncWrite + std::marker::Unpin>(
        self,
        buf: &mut T,
    ) -> tokio::io::Result<usize> {
        match self {
            Self::AuthenticationOk => {
                let vec = vec![CharTag::Authentication.into(), 0, 0, 0, 8, 0, 0, 0, 0];
                buf.write(&vec[..]).await
            }
            Self::ReadyForQuery(status) => {
                write_message_with_prefixed_message_len(
                    buf,
                    CharTag::ReadyForQuery,
                    &[status.into()],
                )
                .await
            }
            Self::AuthenticationRequestMD5Password(MD5Salt(salt)) => {
                let mut body = vec![];
                body.write_i32(5_i32).await?;
                body.write_all(&salt[..]).await?;

                write_message_with_prefixed_message_len(buf, CharTag::Authentication, &body).await
            }
            Self::RowDescription(RowDescription { fields }) => {
                let mut body = vec![];

                body.write_i16(fields.len() as i16).await?;

                for field in &fields {
                    body.write_all(field.name.as_bytes()).await?;
                    body.push(0);

                    body.write_i32(field.table_oid).await?;
                    body.write_i16(field.column_number).await?;
                    body.write_i32(field.type_oid).await?;
                    body.write_i16(field.type_length).await?;
                    body.write_i32(field.type_modifier).await?;
                    body.write_i16(field.format).await?;
                }

                write_message_with_prefixed_message_len(buf, CharTag::RowDescription, &body).await
            }
            Self::DataRow(DataRow { field_data }) => {
                let mut body = vec![];

                body.write_i16(field_data.len() as i16).await?;

                for data in &field_data {
                    match data {
                        Some(data) => {
                            body.write_i32(data.len() as i32).await?;
                            body.extend_from_slice(&data[..]);
                        }
                        None => {
                            body.write_i32(-1_i32).await?;
                        }
                    }
                }

                write_message_with_prefixed_message_len(buf, CharTag::DataRowOrDescribe, &body)
                    .await
            }
            Self::CommandComplete(CommandCompleteTag(tag)) => {
                let mut body = vec![];
                body.extend_from_slice(tag.as_bytes());
                body.push(0);

                write_message_with_prefixed_message_len(buf, CharTag::CommandCompleteOrClose, &body)
                    .await
            }
            Self::ParameterStatus(ParameterStatus { key, value }) => {
                let mut body = vec![];

                body.extend_from_slice(key.as_bytes());
                body.push(0);

                body.extend_from_slice(value.as_bytes());
                body.push(0);

                write_message_with_prefixed_message_len(buf, CharTag::ParameterStatusOrSync, &body)
                    .await
            }
            Self::BackendKeyData(BackendKeyData {
                process_id,
                secret_key,
                additional,
            }) => {
                let mut body = vec![];
                body.write_i32(process_id as i32).await?;
                body.write_i32(secret_key as i32).await?;
                body.extend_from_slice(&additional[..]);

                write_message_with_prefixed_message_len(buf, CharTag::BackendKeyData, &body).await
            }
            Self::ParseComplete => {
                write_message_with_prefixed_message_len(buf, CharTag::ParseComplete, &[]).await
            }
            Self::BindComplete => {
                write_message_with_prefixed_message_len(buf, CharTag::BindComplete, &[]).await
            }
            Self::ParameterDescription(ParameterDescription { types }) => {
                let mut body = vec![];

                body.write_i16((&types).len() as i16).await?;

                for param in &types {
                    body.write_u32(*param).await?;
                }

                write_message_with_prefixed_message_len(buf, CharTag::ParameterDescription, &body)
                    .await
            }
            Self::CloseComplete => {
                write_message_with_prefixed_message_len(buf, CharTag::CloseComplete, &[]).await
            }
            Self::NoData => {
                write_message_with_prefixed_message_len(buf, CharTag::NoData, &[]).await
            }
            Self::EmptyQueryResponse => {
                write_message_with_prefixed_message_len(buf, CharTag::EmptyQueryResponse, &[]).await
            }
            Self::Error(_) => {
                unimplemented!()
            }
        }
    }

    pub async fn read<T: AsyncRead + Unpin>(stream: &mut T) -> Result<Self, ParseError> {
        let (tag, message_length) = read_meta_async(stream).await?;
        Self::read_body(stream, tag, message_length - 4).await
    }

    async fn read_body<T: AsyncRead + Unpin>(
        stream: &mut T,
        tag: CharTag,
        remaining_bytes_len: u32,
    ) -> Result<Self, ParseError> {
        match tag {
            CharTag::Authentication => {
                let method: u32 = AsyncReadExt::read_u32(stream).await?;

                if method == 5 {
                    let mut bytes = vec![0_u8; 4];
                    bytes = stream.read_exact(&mut bytes).await.map(|_| bytes)?;
                    return Ok(Self::AuthenticationRequestMD5Password(MD5Salt(bytes)));
                }

                if method == 0 {
                    return Ok(Self::AuthenticationOk);
                }

                unimplemented!();
            }
            CharTag::ParameterStatusOrSync => {
                let key_bytes = read_until_zero(stream).await?;
                let key = String::from_utf8(key_bytes)?;

                let value_bytes = read_until_zero(stream).await?;
                let value = String::from_utf8(value_bytes)?;

                Ok(Self::ParameterStatus(ParameterStatus { key, value }))
            }
            CharTag::BackendKeyData => {
                let process_id = AsyncReadExt::read_u32(stream).await?;
                let secret_key = AsyncReadExt::read_u32(stream).await?;

                let remaining_bytes = remaining_bytes_len - 8; // -4 process_id, -4 secret_key

                let mut bytes = vec![0; remaining_bytes as usize];
                bytes = stream.read_exact(&mut bytes).await.map(|_| bytes)?;

                Ok(Self::BackendKeyData(BackendKeyData {
                    process_id,
                    secret_key,
                    additional: bytes,
                }))
            }
            CharTag::ReadyForQuery => {
                let status_byte = AsyncReadExt::read_u8(stream).await?;
                let status = ReadyForQueryTransactionStatus::try_from(status_byte)?;
                Ok(Self::ReadyForQuery(status))
            }
            CharTag::RowDescription => {
                let num_fields: u16 = AsyncReadExt::read_u16(stream).await?;

                let mut fields = vec![];

                while fields.len() < num_fields as usize {
                    let name_bytes = read_until_zero(stream).await?;
                    let name = String::from_utf8(name_bytes.clone())?;

                    let table_oid: i32 = AsyncReadExt::read_i32(stream).await?;
                    let column_number: i16 = AsyncReadExt::read_i16(stream).await?;
                    let type_oid: i32 = AsyncReadExt::read_i32(stream).await?;
                    let type_length: i16 = AsyncReadExt::read_i16(stream).await?;
                    let type_modifier: i32 = AsyncReadExt::read_i32(stream).await?;
                    let format: i16 = AsyncReadExt::read_i16(stream).await?;

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
                let num_fields: u16 = AsyncReadExt::read_u16(stream).await?;

                let mut field_data = vec![];
                for _ in 0..num_fields {
                    let field_len: i32 = AsyncReadExt::read_i32(stream).await?;

                    let field_bytes = if field_len != -1 {
                        let mut field_bytes = vec![0; field_len as usize];
                        stream.read_exact(&mut field_bytes).await?;
                        Some(field_bytes)
                    } else {
                        None
                    };

                    field_data.push(field_bytes)
                }

                Ok(Self::DataRow(DataRow { field_data }))
            }
            CharTag::CommandCompleteOrClose => {
                let tag_bytes = read_until_zero(stream).await?;
                let tag = String::from_utf8(tag_bytes)?;

                Ok(Self::CommandComplete(CommandCompleteTag(tag)))
            }
            CharTag::ExecuteOrError => {
                let mut messages = vec![];
                while let Ok(identifier) = AsyncReadExt::read_u8(stream).await {
                    match identifier {
                        0 => break,
                        _ => {
                            let message_bytes = read_until_zero(stream).await?;
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
                let num_param_types: u16 = AsyncReadExt::read_u16(stream).await?;

                let mut types = vec![];

                while types.len() < num_param_types as usize {
                    let param_oid: u32 = AsyncReadExt::read_u32(stream).await?;
                    types.push(param_oid)
                }

                Ok(Self::ParameterDescription(ParameterDescription { types }))
            }
            CharTag::CloseComplete => Ok(Self::CloseComplete),
            CharTag::EmptyQueryResponse => Ok(Self::EmptyQueryResponse),
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
        tokio_test::block_on(message.clone().write(&mut cursor)).unwrap();
        let bytes_written = cursor.position();

        cursor.set_position(0);
        let parsed: Message = tokio_test::block_on(FrontendMessage::read(&mut cursor))
            .unwrap()
            .into();
        let bytes_read = cursor.position();

        assert_eq!(parsed, message);
        assert_eq!(bytes_read, bytes_written);
    }

    fn test_backend_symmetric_serialization_deserialization(message: Message) {
        let mut buf = vec![];
        let mut cursor = std::io::Cursor::new(&mut buf);
        tokio_test::block_on(message.clone().write(&mut cursor)).unwrap();
        let bytes_written = cursor.position();

        cursor.set_position(0);
        let parsed: Message = tokio_test::block_on(BackendMessage::read(&mut cursor))
            .unwrap()
            .into();
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
        let message =
            BackendMessage::ReadyForQuery(ReadyForQueryTransactionStatus::NotInTransaction);

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
    fn bind() {
        let message = FrontendMessage::Bind(Bind {
            statement: "test".to_string(),
            portal: "test".to_string(),
            params: vec![],
            results: vec![],
        });

        test_frontend_symmetric_serialization_deserialization(message.into());
    }

    #[test]
    fn bind_with_parameters() {
        let message = FrontendMessage::Bind(Bind {
            statement: "test".to_string(),
            portal: "test".to_string(),
            params: vec![BindParameter::Binary(vec![0, 0, 3, 235])],
            results: vec![],
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
