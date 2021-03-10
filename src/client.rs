use crate::protocol::BackendMessage;
use anyhow::Result;
use std::net::TcpStream;

pub struct ProxyClient {
    target_addr: String,
}

impl ProxyClient {
    pub fn new(target_addr: String) -> Self {
        ProxyClient { target_addr }
    }

    pub fn connect(&self) -> Result<ProxyConnection> {
        let stream = TcpStream::connect(&self.target_addr)?;
        Ok(ProxyConnection { stream })
    }
}

pub struct ProxyConnection {
    stream: TcpStream,
}

impl ProxyConnection {
    pub fn simple_query(&self, query: &str) -> Result<Vec<BackendMessage>> {
        unimplemented!()
    }
}
