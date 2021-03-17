use crate::protocol::{send_and_handle_startup, BackendMessage};
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
        let mut stream = TcpStream::connect(&self.target_addr)?;
        println!("Proxy connection established");

        send_and_handle_startup(&mut stream);

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
