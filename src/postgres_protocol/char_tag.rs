use anyhow::Result;
use std::convert::TryFrom;
use std::io::Read;

#[derive(Debug, std::cmp::PartialEq)]
pub enum CharTag {
    Authentication,
    ReadyForQuery,
    EmptyQueryResponse,
    Query,
    Password,
    CommandCompleteOrClose,
    RowDescription,
    DataRowOrDescribe,
    BackendKeyData,
    Terminate,
    Parse,
    Bind,
    ParameterDescription,
    ExecuteOrError,
    ParameterStatusOrSync,
    ParseComplete,
    BindComplete,
    CloseComplete,
}

impl CharTag {
    pub fn read<T: Read>(stream: &mut T) -> Result<CharTag> {
        let mut bytes = vec![0; 1];
        stream.read_exact(&mut bytes)?;

        let tag = CharTag::try_from(bytes[0]);

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
            CharTag::CommandCompleteOrClose => b'C',
            CharTag::ParameterStatusOrSync => b'S',
            CharTag::BackendKeyData => b'K',
            CharTag::Terminate => b'X',
            CharTag::Parse => b'P',
            CharTag::Bind => b'B',
            CharTag::ParameterDescription => b't',
            CharTag::ExecuteOrError => b'E',
            CharTag::ParseComplete => b'1',
            CharTag::BindComplete => b'2',
            CharTag::CloseComplete => b'3',
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
            b'C' => Ok(CharTag::CommandCompleteOrClose),
            b'S' => Ok(CharTag::ParameterStatusOrSync),
            b'K' => Ok(CharTag::BackendKeyData),
            b'X' => Ok(CharTag::Terminate),
            b'P' => Ok(CharTag::Parse),
            b'B' => Ok(CharTag::Bind),
            b't' => Ok(CharTag::ParameterDescription),
            b'E' => Ok(CharTag::ExecuteOrError),
            b'1' => Ok(CharTag::ParseComplete),
            b'2' => Ok(CharTag::BindComplete),
            b'3' => Ok(CharTag::CloseComplete),
            _ => Err("Unknown char tag"),
        }
    }
}
