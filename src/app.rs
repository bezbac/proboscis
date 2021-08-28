use crate::{
    core::{accept_frontend_connection, handle_authentication, handle_connection},
    Config, Resolver, Transformer,
};
use anyhow::Result;
use native_tls::Identity;
use std::fs::File;
use std::io::Read;
use tokio::net::TcpListener;

pub struct App {
    config: Config,
    transformers: Vec<Box<dyn Transformer>>,
    resolver: Box<dyn Resolver>,
}

impl App {
    pub async fn listen(&mut self, address: &str) -> Result<()> {
        let listener = TcpListener::bind(&address).await?;
        println!("Server running on {}!", &address);

        let tls_acceptor: Option<tokio_native_tls::TlsAcceptor> = match &self.config.tls_config {
            Some(tls_config) => {
                let mut file = File::open(tls_config.pcks_path.clone())?;
                let mut identity = vec![];
                file.read_to_end(&mut identity).unwrap();

                let certificate =
                    Identity::from_pkcs12(&identity, tls_config.password.as_str()).unwrap();
                let acceptor = tokio_native_tls::TlsAcceptor::from(
                    native_tls::TlsAcceptor::builder(certificate).build()?,
                );

                Some(acceptor)
            }
            _ => None,
        };

        loop {
            let (stream, _) = listener.accept().await?;
            println!("New connection established!");

            let mut frontend_connection = accept_frontend_connection(stream, &tls_acceptor).await?;

            handle_authentication(&mut frontend_connection, &self.config.credentials).await?;

            handle_connection(
                &mut frontend_connection,
                &mut self.resolver,
                &self.transformers
            )
            .await?;
        }
    }

    pub fn new(config: Config, resolver: Box<dyn Resolver>) -> App {
        App {
            config,
            resolver,
            transformers: vec![],
        }
    }

    pub fn add_transformer(mut self, transformer: Box<dyn Transformer>) -> App {
        self.transformers.push(transformer);
        self
    }
}
