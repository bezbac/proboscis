use super::char_tag::CharTag;
use anyhow::Result;
use omnom::prelude::*;
use std::io::prelude::*;

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
pub fn write_message_with_prefixed_message_len<T: Write>(
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
