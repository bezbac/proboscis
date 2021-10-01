use arrow::{
    array::{ArrayRef, LargeStringArray},
    record_batch::RecordBatch,
};
use async_trait::async_trait;
use proboscis_core::resolver::{
    Bind, ClientId, Close, Describe, Execute, Parse, Resolver, SyncResponse,
};
use sqlparser::{
    ast::{Expr, Ident, ObjectName, SelectItem, SetExpr, Statement, TableAlias, TableFactor},
    dialect::PostgreSqlDialect,
    parser::Parser,
};
use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
};

pub trait Transformation: Send + Sync {
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
    transformations: HashMap<String, Vec<Box<dyn Transformation>>>,

    // Maps a client_id to a vec of string queries
    parse_cache: HashMap<ClientId, VecDeque<Parse>>,
}

impl TransformingResolver {
    pub fn new(resolver: Box<dyn Resolver>) -> TransformingResolver {
        TransformingResolver {
            resolver,
            transformations: HashMap::new(),
            parse_cache: HashMap::new(),
        }
    }

    pub fn add_transformation(
        mut self,
        target_field: &str,
        transformation: Box<dyn Transformation>,
    ) -> TransformingResolver {
        let field_transformations = self
            .transformations
            .entry(target_field.to_string())
            .or_default();

        field_transformations.push(transformation);

        self
    }
}

fn transform_record_batch(
    data: RecordBatch,
    column_transformations: HashMap<usize, &Vec<Box<dyn Transformation>>>,
) -> RecordBatch {
    if column_transformations.keys().len() == 0 {
        return data;
    }

    let arrays: Vec<ArrayRef> = (0..data.num_columns())
        .map(|idx| {
            let column_data = data.column(idx).clone();
            match column_transformations.get(&idx) {
                Some(transformations) => transformations
                    .iter()
                    .fold(column_data, |data, transformation| {
                        transformation.apply(data)
                    }),
                None => column_data,
            }
        })
        .collect();

    RecordBatch::try_new(data.schema(), arrays).unwrap()
}

fn get_schema_fields(ast: &Statement) -> anyhow::Result<Vec<String>> {
    match ast {
        Statement::Query(query) => match &query.body {
            SetExpr::Select(select) => {
                let mut fields: Vec<String> = select
                    .projection
                    .iter()
                    .map(|item| match item {
                        SelectItem::UnnamedExpr(Expr::Identifier(Ident {
                            value,
                            quote_style: _,
                        })) => value.clone(),
                        SelectItem::UnnamedExpr(Expr::CompoundIdentifier(identifiers)) => {
                            identifiers
                                .iter()
                                .map(|item| item.value.to_string())
                                .collect::<Vec<String>>()
                                .join(".")
                        }
                        SelectItem::ExprWithAlias {
                            expr:
                                Expr::Identifier(Ident {
                                    value,
                                    quote_style: _,
                                }),
                            alias: _,
                        } => value.clone(),
                        _ => unimplemented!(),
                    })
                    .collect();

                if select.from.len() == 1 {
                    let mut tables: Vec<TableFactor> =
                        vec![select.from.first().unwrap().clone().relation];

                    for joined in select.from.first().unwrap().clone().joins {
                        tables.push(joined.relation)
                    }

                    for table in tables {
                        match table {
                            TableFactor::Table {
                                name: ObjectName(mut name_identifiers),
                                alias,
                                args: _,
                                with_hints: _,
                            } => {
                                if name_identifiers.len() != 1 {
                                    unimplemented!()
                                }

                                let original_name = name_identifiers.pop().unwrap().value;

                                match alias {
                                    Some(TableAlias {
                                        name: aliased_name,
                                        columns: _,
                                    }) => {
                                        fields = fields
                                            .iter()
                                            .map(|field| {
                                                field.replace(
                                                    format!("{}.", aliased_name.value).as_str(),
                                                    format!("{}.", original_name).as_str(),
                                                )
                                            })
                                            .collect()
                                    }
                                    None => {
                                        fields = fields
                                            .iter()
                                            .map(|field| format!("{}.{}", original_name, field))
                                            .collect()
                                    }
                                }
                            }
                            _ => unimplemented!(),
                        }
                    }
                } else {
                    unimplemented!()
                }

                Ok(fields)
            }
            _ => unimplemented!(),
        },
        _ => unimplemented!(),
    }
}

impl TransformingResolver {
    fn get_column_transformations(
        &self,
        statement: &Statement,
        data: &RecordBatch,
    ) -> HashMap<usize, &Vec<Box<dyn Transformation>>> {
        let normalized_field_names = get_schema_fields(statement).unwrap();

        data.schema()
            .fields()
            .iter()
            .zip(normalized_field_names.iter())
            .enumerate()
            .filter_map(|(index, (_, normalized_field_name))| {
                self.transformations
                    .get(normalized_field_name)
                    .map(|transformations| (index, transformations))
            })
            .collect()
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
        let dialect = PostgreSqlDialect {};
        let query_ast = Parser::parse_sql(&dialect, &query).unwrap();

        if query_ast.len() != 1 {
            todo!("Mismatched number of statements");
        }

        let result = self.resolver.query(client_id, query).await;
        result.map(|data| {
            let column_transformations =
                self.get_column_transformations(query_ast.first().unwrap(), &data);
            transform_record_batch(data, column_transformations)
        })
    }

    async fn parse(&mut self, client_id: ClientId, parse: Parse) -> anyhow::Result<()> {
        self.resolver
            .parse(client_id, parse.clone())
            .await
            .map(|_| {
                self.parse_cache
                    .entry(client_id)
                    .or_default()
                    .push_back(parse)
            })
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
            .iter()
            .map(|response| match response {
                SyncResponse::Schema(_) => todo!(),
                SyncResponse::Records(record_batch) => {
                    let parse_vec = self.parse_cache.entry(client_id).or_default();
                    let Parse {
                        query,
                        statement_name: _,
                        param_types: _,
                    } = parse_vec.pop_front().unwrap();

                    let dialect = PostgreSqlDialect {};
                    let query_ast = Parser::parse_sql(&dialect, &query).unwrap();

                    let column_transformations =
                        self.get_column_transformations(query_ast.first().unwrap(), &record_batch);

                    let transformed =
                        transform_record_batch(record_batch.clone(), column_transformations);

                    SyncResponse::Records(transformed)
                }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_schema_fields() {
        let dialect = PostgreSqlDialect {};
        let query_ast = Parser::parse_sql(&dialect, "SELECT id FROM users")
            .unwrap()
            .pop()
            .unwrap();

        let unnested_fields = get_schema_fields(&query_ast).unwrap();
        assert_eq!(unnested_fields, vec!["users.id"])
    }

    #[test]
    fn test_schema_fields_renamed() {
        let dialect = PostgreSqlDialect {};
        let query_ast = Parser::parse_sql(&dialect, "SELECT id AS 'user_id' FROM users")
            .unwrap()
            .pop()
            .unwrap();

        let unnested_fields = get_schema_fields(&query_ast).unwrap();
        assert_eq!(unnested_fields, vec!["users.id"])
    }

    #[test]
    fn test_schema_fields_table_alias() {
        let dialect = PostgreSqlDialect {};
        let query_ast = Parser::parse_sql(&dialect, "SELECT u.id FROM users u")
            .unwrap()
            .pop()
            .unwrap();

        let unnested_fields = get_schema_fields(&query_ast).unwrap();
        assert_eq!(unnested_fields, vec!["users.id"])
    }

    #[test]
    fn test_schema_fields_join() {
        let dialect = PostgreSqlDialect {};
        let query_ast = Parser::parse_sql(
            &dialect,
            "SELECT u.id, p.id, p.title FROM users u JOIN posts p",
        )
        .unwrap()
        .pop()
        .unwrap();

        let unnested_fields = get_schema_fields(&query_ast).unwrap();
        assert_eq!(unnested_fields, vec!["users.id", "posts.id", "posts.title"])
    }
}
