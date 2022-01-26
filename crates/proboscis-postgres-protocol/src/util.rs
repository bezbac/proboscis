use super::char_tag::CharTag;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

pub async fn read_until_zero<T: AsyncRead + Unpin>(stream: &mut T) -> tokio::io::Result<Vec<u8>> {
    let mut result = vec![];

    loop {
        let mut bytes = vec![0; 1];
        bytes = stream.read_exact(&mut bytes).await.map(|_| bytes)?;

        match bytes[..] {
            [0] => break,
            _ => result.extend_from_slice(&bytes[..]),
        }
    }

    Ok(result)
}

/// Higher order function to write a message with some arbitraty number bytes to a buffer
/// and automatically prefix it the char_tag and with the message length as a BigEndian 32 bit integer
pub async fn write_message_with_prefixed_message_len<T: AsyncWrite + std::marker::Unpin>(
    buf: &mut T,
    char_tag: CharTag,
    body: &[u8],
) -> tokio::io::Result<usize> {
    let mut written_bytes_count = 0;
    written_bytes_count += buf.write(&[char_tag.into()]).await?;

    buf.write_i32(body.len() as i32 + 4).await?;
    written_bytes_count += 4;

    written_bytes_count += buf.write(body).await?;

    Ok(written_bytes_count)
}
