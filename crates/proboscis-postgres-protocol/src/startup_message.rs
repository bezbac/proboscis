use std::collections::HashMap;
use tokio::io::AsyncRead;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWrite;
use tokio::io::AsyncWriteExt;

use crate::ParseError;

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
    pub async fn write<T: AsyncWrite + Unpin>(&self, buf: &mut T) -> tokio::io::Result<()> {
        match self {
            Self::Startup { params } => {
                let mut writer = vec![];

                writer.write_i32(CODE_STARTUP_POSTGRESQLV3).await?;

                for (key, value) in params {
                    AsyncWriteExt::write_all(&mut writer, key.as_bytes()).await?;
                    writer.write_u8(0_u8).await?; // Delimiter

                    AsyncWriteExt::write_all(&mut writer, value.as_bytes()).await?;
                    writer.write_u8(0_u8).await?; // Delimiter
                }

                writer.write_u8(0_u8).await?; // Delimiter

                let len_of_message: u32 = writer.len() as u32 + 4; // Add 4 bytes for the u32 containing the total message length

                buf.write_u32(len_of_message).await?;
                buf.write_all(&writer[..]).await?;

                Ok(())
            }
            _ => unimplemented!(),
        }
    }

    pub async fn read<T: AsyncRead + Unpin>(stream: &mut T) -> Result<Self, ParseError> {
        let message_length = AsyncReadExt::read_u32(stream).await?;

        let mut body_bytes = vec![0; message_length as usize - 4];
        stream.read_exact(&mut body_bytes).await?;

        let mut cursor = std::io::Cursor::new(body_bytes);

        Self::read_body(&mut cursor, message_length - 4).await
    }

    pub async fn read_body<T: AsyncRead + Unpin>(
        stream: &mut T,
        remaining_bytes_len: u32,
    ) -> Result<Self, ParseError> {
        let protocol_version: i32 = stream.read_i32().await?;

        let message = match protocol_version {
            CODE_STARTUP_CANCEL => StartupMessage::CancelRequest {
                connection_id: stream.read_u32().await?,
                secret_key: stream.read_u32().await?,
            },
            CODE_STARTUP_SSL_REQUEST => StartupMessage::SslRequest,
            CODE_STARTUP_GSSENC_REQUEST => StartupMessage::GssEncRequest,
            _ => {
                let mut params: HashMap<String, String> = HashMap::new();

                let mut buf = vec![0; remaining_bytes_len as usize - 4];
                stream.read_exact(&mut buf).await?;
                let mut iter = buf.iter();

                let mut last_string: Option<String> = None;
                loop {
                    let string_bytes = iter.by_ref().take_while(|&&v| v != 0).cloned().collect();

                    let string = String::from_utf8(string_bytes)?;

                    if string.len() == 0 {
                        break;
                    }

                    match last_string {
                        Some(key) => {
                            params.insert(key, string);
                            last_string = None;
                        }
                        None => {
                            last_string = Some(string);
                        }
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
        tokio_test::block_on(
            StartupMessage::Startup {
                params: HashMap::new(),
            }
            .write(&mut buf),
        )
        .unwrap();

        let mut cursor = std::io::Cursor::new(&mut buf);
        tokio_test::block_on(StartupMessage::read(&mut cursor)).unwrap();
    }

    #[test]
    fn startup_message_with_parameters() {
        let mut params: HashMap<String, String> = HashMap::new();
        params.insert("Test Key".to_string(), "Test Value".to_string());
        params.insert("Test Key 2".to_string(), "Test Value 2".to_string());
        params.insert("Test Key 3".to_string(), "Test Value 3".to_string());

        let mut buf = vec![];
        tokio_test::block_on(
            StartupMessage::Startup {
                params: params.clone(),
            }
            .write(&mut buf),
        )
        .unwrap();

        let mut cursor = std::io::Cursor::new(&mut buf);
        let parsed = tokio_test::block_on(StartupMessage::read(&mut cursor)).unwrap();

        assert_eq!(parsed, StartupMessage::Startup { params })
    }
}
