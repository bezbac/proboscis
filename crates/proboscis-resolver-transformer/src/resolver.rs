use crate::{
    interface::Transformer,
    projection::{trace_projection_origin, ProjectedOrigin},
};
use arrow::{datatypes::Schema, record_batch::RecordBatch};
use async_trait::async_trait;
use proboscis_core::resolver::{
    Bind, ClientId, Close, Describe, Execute, Parse, ResolveError, Resolver, SyncResponse,
};
use sqlparser::{
    ast::Statement,
    dialect::PostgreSqlDialect,
    parser::{Parser, ParserError},
};
use std::{
    collections::{BTreeMap, HashMap},
    convert::TryFrom,
    sync::Arc,
    vec,
};

pub struct TransformingResolver {
    resolver: Box<dyn Resolver>,
    transformers: Vec<Box<dyn Transformer>>,
    skip_if_cannot_parse: bool,
    skip_if_cannot_trace: bool,
}

impl TransformingResolver {
    pub fn new(resolver: Box<dyn Resolver>) -> TransformingResolver {
        TransformingResolver {
            resolver,
            skip_if_cannot_parse: true,
            skip_if_cannot_trace: true,
            transformers: Vec::new(),
        }
    }

    pub fn add_transformer(mut self, transformer: Box<dyn Transformer>) -> TransformingResolver {
        self.transformers.push(transformer);
        self
    }
}

impl TransformingResolver {
    fn parse_sql(&self, query: &str) -> Result<Vec<Statement>, ParserError> {
        let dialect = PostgreSqlDialect {};
        Parser::parse_sql(&dialect, query)
    }
}

fn re_apply_metadata(original_schema: &Schema, new_schema: &Schema) -> Result<Schema, String> {
    let mut original_metadata: HashMap<String, BTreeMap<String, String>> = HashMap::new();
    for field in original_schema.fields().iter() {
        let key = field.name().clone();
        let value = field
            .metadata()
            .clone()
            .ok_or_else(|| String::from("missing metadata"))?;
        original_metadata.insert(key, value);
    }

    let mut fields_with_metadata = vec![];
    for field in new_schema.fields() {
        let mut field_with_metadata = arrow::datatypes::Field::new(
            field.name(),
            field.data_type().clone(),
            field.is_nullable(),
        );

        let field_metadata = original_metadata
            .get(field.name())
            .ok_or_else(|| {
                format!(
                    "No field with name {} contained in original schema",
                    field.name()
                )
            })?
            .clone();

        field_with_metadata.set_metadata(Some(field_metadata));

        fields_with_metadata.push(field_with_metadata)
    }

    Ok(arrow::datatypes::Schema::new_with_metadata(
        fields_with_metadata,
        original_schema.metadata().clone(),
    ))
}

impl TransformingResolver {
    fn with_traced_projection<T: Clone, F: Fn(Vec<ProjectedOrigin>) -> Result<T, ResolveError>>(
        &self,
        query: &str,
        schema: &Schema,
        fallback: &T,
        transformation: F,
    ) -> Result<T, ResolveError> {
        let query_ast: Vec<Statement> = match self.parse_sql(query) {
            Ok(ast) => ast,
            Err(err) => {
                return if self.skip_if_cannot_parse {
                    tracing::warn!("Could not parse query, skipping transformation");
                    Ok(fallback.clone())
                } else {
                    return Err(ResolveError::Other(anyhow::anyhow!(err)));
                }
            }
        };

        let mut fields = vec![];
        for f in schema.fields().iter() {
            let field = proboscis_core::data::field::Field::try_from(f)?;
            fields.push(field);
        }

        let origins = match trace_projection_origin(query_ast.first().unwrap(), &fields) {
            Ok(ast) => ast,
            Err(err) => {
                return if self.skip_if_cannot_trace {
                    tracing::warn!(
                        "Could not trace origin of projected columns, skipping transformation"
                    );
                    Ok(fallback.clone())
                } else {
                    return Err(ResolveError::Other(anyhow::anyhow!(err)));
                }
            }
        };

        transformation(origins)
    }

    fn transform_records(
        &self,
        query: &str,
        data: &RecordBatch,
    ) -> Result<RecordBatch, ResolveError> {
        self.with_traced_projection(query, &data.schema(), data, |origins| {
            let mut transformed = data.clone();

            for transformer in &self.transformers {
                transformed = transformer.transform_records(&transformed, &origins)?;
            }

            let transformed_schema_with_metadata =
                re_apply_metadata(&data.schema(), &transformed.schema())
                    .map_err(|err| ResolveError::Other(anyhow::anyhow!(err)))?;

            let transformed_with_metadata = RecordBatch::try_new(
                Arc::new(transformed_schema_with_metadata),
                transformed.columns().to_vec(),
            )?;

            Ok(transformed_with_metadata)
        })
    }

    fn transform_schema(&self, query: &str, schema: &Schema) -> Result<Schema, ResolveError> {
        self.with_traced_projection(query, schema, schema, |origins| {
            let mut transformed = schema.clone();

            for transformer in &self.transformers {
                transformed = transformer.transform_schema(&transformed, &origins)?;
            }

            let transformed_with_metadata = re_apply_metadata(schema, &transformed)
                .map_err(|err| ResolveError::Other(anyhow::anyhow!(err)))?;

            Ok(transformed_with_metadata)
        })
    }
}

#[async_trait]
impl Resolver for TransformingResolver {
    async fn initialize(&mut self, client_id: ClientId) -> Result<(), ResolveError> {
        self.resolver.initialize(client_id).await
    }

    async fn query(
        &mut self,
        client_id: ClientId,
        query: String,
    ) -> Result<arrow::record_batch::RecordBatch, ResolveError> {
        let records = self.resolver.query(client_id, query.clone()).await?;
        let transformed = self.transform_records(&query, &records)?;
        Ok(transformed)
    }

    async fn parse(&mut self, client_id: ClientId, parse: Parse) -> Result<(), ResolveError> {
        self.resolver.parse(client_id, parse).await
    }

    async fn describe(
        &mut self,
        client_id: ClientId,
        describe: Describe,
    ) -> Result<(), ResolveError> {
        self.resolver.describe(client_id, describe).await
    }

    async fn bind(&mut self, client_id: ClientId, bind: Bind) -> Result<(), ResolveError> {
        self.resolver.bind(client_id, bind).await
    }

    async fn execute(&mut self, client_id: ClientId, execute: Execute) -> Result<(), ResolveError> {
        self.resolver.execute(client_id, execute).await
    }

    async fn sync(&mut self, client_id: ClientId) -> Result<Vec<SyncResponse>, ResolveError> {
        let responses = self.resolver.sync(client_id).await?;

        let mut transformed_responses = vec![];
        for response in responses {
            let transformed_response = match response {
                SyncResponse::Schema { schema, query } => {
                    let transformed_schema = self.transform_schema(&query, &schema)?;

                    SyncResponse::Schema {
                        schema: transformed_schema,
                        query,
                    }
                }
                SyncResponse::Records { data, query } => {
                    let transformed_data = self.transform_records(&query, &data)?;

                    SyncResponse::Records {
                        data: transformed_data,
                        query,
                    }
                }
                _ => response,
            };

            transformed_responses.push(transformed_response)
        }

        Ok(transformed_responses)
    }

    async fn close(&mut self, client_id: ClientId, close: Close) -> Result<(), ResolveError> {
        self.resolver.close(client_id, close).await
    }

    async fn terminate(&mut self, client_id: ClientId) -> Result<(), ResolveError> {
        self.resolver.terminate(client_id).await
    }
}
