use std::sync::Arc;

use crate::Resolver;
use arrow::{
    array::{ArrayRef, LargeStringArray},
    datatypes::DataType,
    record_batch::RecordBatch,
};
use async_trait::async_trait;

pub trait Transformation {
    fn apply(&self, data: ArrayRef) -> ArrayRef;
}

pub struct ReplaceString {
    pub new_string: String,
}

impl Transformation for ReplaceString {
    fn apply(&self, data: ArrayRef) -> ArrayRef {
        Arc::new(LargeStringArray::from(vec![
            self.new_string.as_str();
            data.len()
        ]))
    }
}

pub struct TransformingResolver {
    resolver: Box<dyn Resolver>,
}

impl TransformingResolver {
    pub fn new(resolver: Box<dyn Resolver>) -> TransformingResolver {
        TransformingResolver { resolver }
    }

    pub fn add_transformation(
        self,
        target_field: &str,
        transformation: Box<dyn Transformation>,
    ) -> TransformingResolver {
        self
    }
}

#[async_trait]
impl Resolver for TransformingResolver {
    async fn initialize(&mut self, client_id: uuid::Uuid) -> anyhow::Result<()> {
        self.resolver.initialize(client_id).await
    }

    async fn query(
        &mut self,
        client_id: uuid::Uuid,
        query: String,
    ) -> anyhow::Result<arrow::record_batch::RecordBatch> {
        let result = self.resolver.query(client_id, query).await;
        result.map(|data| {
            for field in data.schema().fields() {
                println!("{:?}", field)
            }

            data
        })
    }

    async fn parse(
        &mut self,
        client_id: uuid::Uuid,
        statement_name: String,
        query: String,
        param_types: Vec<u32>,
    ) -> anyhow::Result<()> {
        self.resolver
            .parse(client_id, statement_name, query, param_types)
            .await
    }

    async fn describe(
        &mut self,
        client_id: uuid::Uuid,
        kind: crate::postgres_protocol::DescribeKind,
        name: String,
    ) -> anyhow::Result<()> {
        self.resolver.describe(client_id, kind, name).await
    }

    async fn bind(
        &mut self,
        client_id: uuid::Uuid,
        statement: String,
        portal: String,
        params: Vec<Vec<u8>>,
        formats: Vec<i16>,
        results: Vec<i16>,
    ) -> anyhow::Result<()> {
        self.resolver
            .bind(client_id, statement, portal, params, formats, results)
            .await
    }

    async fn execute(
        &mut self,
        client_id: uuid::Uuid,
        portal: String,
        row_limit: i32,
    ) -> anyhow::Result<()> {
        self.resolver.execute(client_id, portal, row_limit).await
    }

    async fn sync(
        &mut self,
        client_id: uuid::Uuid,
    ) -> anyhow::Result<Vec<crate::postgres_protocol::Message>> {
        self.resolver.sync(client_id).await
    }

    async fn close(
        &mut self,
        client_id: uuid::Uuid,
        kind: crate::postgres_protocol::CloseKind,
        name: String,
    ) -> anyhow::Result<()> {
        self.resolver.close(client_id, kind, name).await
    }

    async fn terminate(&mut self, client_id: uuid::Uuid) -> anyhow::Result<()> {
        self.resolver.terminate(client_id).await
    }
}
