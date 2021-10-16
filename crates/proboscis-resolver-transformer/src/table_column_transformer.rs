use crate::{
    column_transformations::{ColumnTransformation, ColumnTransformationOutput},
    traits::Transformer,
};
use arrow::{
    datatypes::{Field, Schema},
    record_batch::RecordBatch,
};
use sqlparser::ast::{
    Expr, Ident, ObjectName, SelectItem, SetExpr, Statement, TableAlias, TableFactor,
};
use std::{collections::HashMap, sync::Arc};

pub fn get_schema_fields(ast: &Statement) -> anyhow::Result<Vec<String>> {
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

#[derive(Default)]
pub struct TableColumnTransformer {
    transformations: HashMap<String, Vec<Box<dyn ColumnTransformation>>>,
}

impl TableColumnTransformer {
    pub fn add_transformation(
        mut self,
        target_column: &str,
        transformation: Box<dyn ColumnTransformation>,
    ) -> TableColumnTransformer {
        let field_transformations = self
            .transformations
            .entry(target_column.to_string())
            .or_default();

        field_transformations.push(transformation);

        self
    }

    fn get_column_transformations(
        &self,
        statement: &Statement,
        schema: &Schema,
    ) -> HashMap<usize, &Vec<Box<dyn ColumnTransformation>>> {
        let normalized_field_names = get_schema_fields(statement).unwrap();

        schema
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

fn transform_field(field: &Field, transformation: &dyn ColumnTransformation) -> Field {
    let ColumnTransformationOutput {
        data_type,
        nullable,
    } = transformation.output_format();
    let mut new_field = Field::new(field.name(), data_type, nullable);
    new_field.set_metadata(field.metadata().clone());
    new_field
}

impl Transformer for TableColumnTransformer {
    fn transform_schema(&self, query: &[Statement], schema: &Schema) -> Schema {
        let column_transformations =
            self.get_column_transformations(query.first().unwrap(), schema);

        if column_transformations.keys().len() == 0 {
            return schema.clone();
        }

        let new_fields = schema
            .fields()
            .iter()
            .enumerate()
            .map(|(index, field)| match column_transformations.get(&index) {
                Some(transformations) => transformations
                    .iter()
                    .fold(field.clone(), |field, transformation| {
                        transform_field(&field, transformation.as_ref())
                    }),
                None => field.clone(),
            })
            .collect();

        Schema::new_with_metadata(new_fields, schema.metadata().clone())
    }

    fn transform_records(&self, query: &[Statement], data: &RecordBatch) -> RecordBatch {
        let column_transformations =
            self.get_column_transformations(query.first().unwrap(), &data.schema());

        if column_transformations.keys().len() == 0 {
            return data.clone();
        }

        let (new_fields, new_data) = data
            .schema()
            .fields()
            .iter()
            .enumerate()
            .map(|(idx, field)| {
                let column_data = data.column(idx).clone();
                match column_transformations.get(&idx) {
                    Some(transformations) => transformations.iter().fold(
                        (field.clone(), column_data),
                        |(field, data), transformation| {
                            (
                                transform_field(&field, transformation.as_ref()),
                                transformation.transform_data(data),
                            )
                        },
                    ),
                    None => (field.clone(), column_data),
                }
            })
            .unzip();

        let new_schema = Schema::new_with_metadata(new_fields, data.schema().metadata().clone());

        RecordBatch::try_new(Arc::new(new_schema), new_data).unwrap()
    }
}

#[cfg(test)]
mod tests {
    use sqlparser::{dialect::PostgreSqlDialect, parser::Parser};

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
