use super::util::{read_until_zero, write_message_with_prefixed_message_len};
use super::CharTag;
use anyhow::Result;
use omnom::prelude::*;
use std::collections::HashMap;
use std::io::prelude::*;

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
    Execute {
        portal: String,
        row_limit: i32,
    },
    Sync {
        len: u32,
    },
    Error {
        messages: HashMap<String, String>,
    },
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
                CharTag::ParameterStatusOrSync,
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
                    body.extend_from_slice(statement.as_bytes());
                    body.push(0);

                    body.extend_from_slice(query.as_bytes());
                    body.push(0);

                    body.write_be(params_types.clone().len() as i16)?;

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
            Self::Execute { portal, row_limit } => write_message_with_prefixed_message_len(
                buf,
                CharTag::ExecuteOrError,
                Box::new(move |body| -> Result<()> {
                    body.extend_from_slice(portal.as_bytes());
                    body.push(0);

                    body.write_be(row_limit)?;

                    Ok(())
                }),
            ),
            Self::Sync { len } => write_message_with_prefixed_message_len(
                buf,
                CharTag::ParameterStatusOrSync,
                Box::new(move |body| -> Result<()> {
                    body.write_be(len)?;

                    Ok(())
                }),
            ),
            Self::Error { messages } => {
                unimplemented!()
            }
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
            CharTag::ParameterStatusOrSync => {
                let message_len: u32 = stream.read_be()?;

                if message_len == 8 {
                    let len: u32 = stream.read_be()?;
                    return Ok(Self::Sync { len });
                }

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

                let describe_identifier = bytes[0];

                match describe_identifier {
                    b'S' | b'P' => {
                        let mut cursor = std::io::Cursor::new(&bytes[1..]);

                        let kind = match describe_identifier {
                            b'S' => DescribeKind::Statement,
                            b'P' => DescribeKind::Portal,
                            _ => return Err(anyhow::anyhow!("Invalid describe kind")),
                        };

                        let name_bytes = read_until_zero(&mut cursor)?;
                        let name = String::from_utf8(name_bytes)?;

                        Ok(Message::Describe { kind, name })
                    }
                    _ => {
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
                }
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
            CharTag::ExecuteOrError => {
                let message_len: u32 = stream.read_be()?;

                let portal_bytes = read_until_zero(stream)?;
                let portal = String::from_utf8(portal_bytes.clone())?;

                let row_limit: i32 = stream.read_be()?;

                Ok(Message::Execute { portal, row_limit })
            }
            _ => unimplemented!("Recieved tag {:?}", tag),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_symmetric_serialization_deserialization(message: Message) {
        let mut buf = vec![];
        let mut cursor = std::io::Cursor::new(&mut buf);
        message.clone().write(&mut cursor).unwrap();
        let bytes_written = cursor.position();

        cursor.set_position(0);
        let parsed = Message::read(&mut cursor).unwrap();
        let bytes_read = cursor.position();

        assert_eq!(parsed, message);
        assert_eq!(bytes_read, bytes_written);
    }

    #[test]
    fn parameter_status() {
        let message = Message::ParameterStatus {
            key: "Test Key".to_string(),
            value: "Test Value".to_string(),
        };

        test_symmetric_serialization_deserialization(message);
    }

    #[test]
    fn empty_backend_key_data() {
        let message = Message::BackendKeyData {
            process_id: 1,
            secret_key: 1,
            additional: vec![],
        };

        test_symmetric_serialization_deserialization(message);
    }

    #[test]
    fn ready_for_query() {
        let message = Message::ReadyForQuery;

        test_symmetric_serialization_deserialization(message);
    }

    #[test]
    fn row_description() {
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

        test_symmetric_serialization_deserialization(message);
    }

    #[test]
    fn describe_statement() {
        let message = Message::Describe {
            kind: DescribeKind::Statement,
            name: "Test".to_string(),
        };

        test_symmetric_serialization_deserialization(message);
    }

    #[test]
    fn describe_portal() {
        let message = Message::Describe {
            kind: DescribeKind::Portal,
            name: "Test".to_string(),
        };

        test_symmetric_serialization_deserialization(message);
    }

    #[test]
    fn execute() {
        let message = Message::Execute {
            portal: "Test".to_string(),
            row_limit: 0,
        };

        test_symmetric_serialization_deserialization(message);
    }

    #[test]
    fn parse() {
        let message = Message::Parse {
            statement: "s0".to_string(),
            query: "SELECT id, name FROM person".to_string(),
            params_types: vec![],
        };

        test_symmetric_serialization_deserialization(message);
    }
}
