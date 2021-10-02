use crate::traits::Transformer;
use anyhow::Result;
use async_trait::async_trait;
use proboscis_core::resolver::{
    Bind, ClientId, Close, Describe, Execute, Parse, Resolver, SyncResponse,
};
use sqlparser::{ast::Statement, dialect::PostgreSqlDialect, parser::Parser};

pub struct TransformingResolver {
    resolver: Box<dyn Resolver>,
    transformers: Vec<Box<dyn Transformer>>,
}

impl TransformingResolver {
    pub fn new(resolver: Box<dyn Resolver>) -> TransformingResolver {
        TransformingResolver {
            resolver,
            transformers: Vec::new(),
        }
    }

    pub fn add_transformer(mut self, transformer: Box<dyn Transformer>) -> TransformingResolver {
        self.transformers.push(transformer);
        self
    }
}

impl TransformingResolver {
    fn parse_sql(&self, query: &str) -> Result<Vec<Statement>> {
        let dialect = PostgreSqlDialect {};
        Ok(Parser::parse_sql(&dialect, query)?)
    }
}

#[async_trait]
impl Resolver for TransformingResolver {
    async fn initialize(&mut self, client_id: ClientId) -> anyhow::Result<()> {
        self.resolver.initialize(client_id).await
    }

    async fn query(
        &mut self,
        client_id: ClientId,
        query: String,
    ) -> anyhow::Result<arrow::record_batch::RecordBatch> {
        let query_ast = self.parse_sql(&query)?;

        if query_ast.len() != 1 {
            todo!("Mismatched number of statements");
        }

        let result = self.resolver.query(client_id, query).await;
        result.map(|data| {
            self.transformers.iter().fold(data, |data, transformer| {
                transformer.transform_records(&query_ast, &data)
            })
        })
    }

    async fn parse(&mut self, client_id: ClientId, parse: Parse) -> anyhow::Result<()> {
        self.resolver.parse(client_id, parse).await
    }

    async fn describe(&mut self, client_id: ClientId, describe: Describe) -> anyhow::Result<()> {
        self.resolver.describe(client_id, describe).await
    }

    async fn bind(&mut self, client_id: ClientId, bind: Bind) -> anyhow::Result<()> {
        self.resolver.bind(client_id, bind).await
    }

    async fn execute(&mut self, client_id: ClientId, execute: Execute) -> anyhow::Result<()> {
        self.resolver.execute(client_id, execute).await
    }

    async fn sync(&mut self, client_id: ClientId) -> anyhow::Result<Vec<SyncResponse>> {
        let responses = self.resolver.sync(client_id).await?;

        let transformed_responses = responses
            .into_iter()
            .map(|response| match response {
                SyncResponse::Schema { schema, query } => {
                    let query_ast = self.parse_sql(&query).unwrap();

                    let transformed = self
                        .transformers
                        .iter()
                        .fold(schema, |schema, transformer| {
                            transformer.transform_schema(&query_ast, &schema)
                        });

                    SyncResponse::Schema {
                        schema: transformed,
                        query,
                    }
                }
                SyncResponse::Records { data, query } => {
                    let query_ast = self.parse_sql(&query).unwrap();

                    let transformed = self.transformers.iter().fold(data, |data, transformer| {
                        transformer.transform_records(&query_ast, &data)
                    });

                    SyncResponse::Records {
                        data: transformed,
                        query,
                    }
                }
                _ => response,
            })
            .collect();

        Ok(transformed_responses)
    }

    async fn close(&mut self, client_id: ClientId, close: Close) -> anyhow::Result<()> {
        self.resolver.close(client_id, close).await
    }

    async fn terminate(&mut self, client_id: ClientId) -> anyhow::Result<()> {
        self.resolver.terminate(client_id).await
    }
}
