use proboscis_core::data::field::Field;
use sqlparser::ast::{
    Expr, Ident, SelectItem, SetExpr, Statement, TableAlias, TableFactor, TableWithJoins,
};
use std::collections::VecDeque;

#[derive(Clone, Debug, PartialEq)]
pub struct TableColumn {
    pub table: String,
    pub column: String,
}

#[derive(Clone, Debug, PartialEq)]
pub enum ProjectedOrigin {
    TableColumn(TableColumn),
    Value,
    Function,
}

pub fn trace_projection_origin(
    ast: &Statement,
    fields: &[Field],
) -> anyhow::Result<Vec<ProjectedOrigin>> {
    match ast {
        Statement::Query(query) => match &query.body {
            SetExpr::Select(select) => {
                let mut result = vec![];
                let mut remaining_fields = fields.iter().collect::<VecDeque<_>>();

                let get_table_column = |identifiers: &[String]| -> anyhow::Result<TableColumn> {
                    if identifiers.is_empty() {
                        // Wildcard
                        anyhow::bail!("")
                    }

                    if identifiers.len() == 1 {
                        let identifier = identifiers[0].clone();
                        match &select.from.as_slice() {
                            [TableWithJoins {
                                relation:
                                    TableFactor::Table {
                                        name,
                                        alias: _,
                                        args: _,
                                        with_hints: _,
                                    },
                                joins: _,
                            }] => {
                                return Ok(TableColumn {
                                    table: name.to_string(),
                                    column: identifier,
                                })
                            }
                            _ => anyhow::bail!(""),
                        }
                    }

                    if identifiers.len() == 2 {
                        let table_identifier = identifiers[0].clone();
                        let column_identifier = identifiers[1].clone();

                        for table in &select.from {
                            for factor in vec![
                                vec![&table.relation],
                                table.joins.iter().map(|join| &join.relation).collect(),
                            ]
                            .concat()
                            {
                                if let TableFactor::Table {
                                    name,
                                    alias,
                                    args: _,
                                    with_hints: _,
                                } = factor
                                {
                                    let alias_name = alias
                                        .as_ref()
                                        .map(|TableAlias { name, columns: _ }| name.to_string());

                                    if name.to_string() == table_identifier
                                        || alias_name == Some(table_identifier.clone())
                                    {
                                        return Ok(TableColumn {
                                            table: name.to_string(),
                                            column: column_identifier,
                                        });
                                    }
                                };
                            }
                        }
                    }

                    anyhow::bail!("");
                };

                for item in &select.projection {
                    match item {
                        SelectItem::Wildcard => {
                            let mut table_index = 0;
                            let mut last_table_oid: Option<i32> = None;
                            while let Some(field) = remaining_fields.pop_front() {
                                let current_table_oid = field.table_oid;

                                if let Some(oid) = &last_table_oid {
                                    if *oid != current_table_oid {
                                        table_index += 1;
                                    }
                                }

                                let table_name = match &select.from.get(table_index) {
                                    Some(TableWithJoins {
                                        relation:
                                            TableFactor::Table {
                                                name,
                                                alias: _,
                                                args: _,
                                                with_hints: _,
                                            },
                                        joins: _,
                                    }) => name.to_string(),
                                    _ => anyhow::bail!(""),
                                };

                                result.push(ProjectedOrigin::TableColumn(TableColumn {
                                    column: field.name.clone(),
                                    table: table_name,
                                }));

                                last_table_oid = Some(current_table_oid);
                            }
                        }

                        SelectItem::ExprWithAlias {
                            expr:
                                Expr::Identifier(Ident {
                                    value,
                                    quote_style: _,
                                }),
                            alias: _,
                        } => {
                            let column = value.clone();
                            let table_column = get_table_column(&[column])?;
                            result.push(ProjectedOrigin::TableColumn(table_column))
                        }
                        SelectItem::ExprWithAlias {
                            expr: Expr::Value(_),
                            alias: _,
                        } => {
                            remaining_fields.pop_front();
                            result.push(ProjectedOrigin::Value)
                        }
                        SelectItem::ExprWithAlias {
                            expr: Expr::Function(_),
                            alias: _,
                        } => {
                            remaining_fields.pop_front();
                            result.push(ProjectedOrigin::Function)
                        }

                        SelectItem::UnnamedExpr(Expr::Identifier(Ident {
                            value,
                            quote_style: _,
                        })) => {
                            let column = value.clone();
                            let table_column = get_table_column(&[column])?;
                            result.push(ProjectedOrigin::TableColumn(table_column))
                        }
                        SelectItem::UnnamedExpr(Expr::CompoundIdentifier(identifiers)) => {
                            if identifiers.len() > 2 {
                                anyhow::bail!("unsupported")
                            }

                            let identifiers: Vec<String> = identifiers
                                .iter()
                                .map(|ident| ident.value.to_string())
                                .collect();

                            let table_column = get_table_column(&identifiers)?;
                            result.push(ProjectedOrigin::TableColumn(table_column))
                        }
                        SelectItem::UnnamedExpr(Expr::Function(_)) => {
                            remaining_fields.pop_front();
                            result.push(ProjectedOrigin::Function)
                        }
                        SelectItem::UnnamedExpr(Expr::Value(_)) => {
                            remaining_fields.pop_front();
                            result.push(ProjectedOrigin::Value)
                        }

                        _ => anyhow::bail!("unimplemented {:?}", item),
                    }
                }

                Ok(result)
            }
            _ => anyhow::bail!("unsupported"),
        },
        _ => anyhow::bail!("unsupported"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlparser::{dialect::PostgreSqlDialect, parser::Parser};

    #[test]
    fn test_single_field() {
        let dialect = PostgreSqlDialect {};
        let query_ast = Parser::parse_sql(&dialect, "SELECT id FROM users")
            .unwrap()
            .pop()
            .unwrap();

        let unnested_fields = trace_projection_origin(
            &query_ast,
            &[Field {
                name: "id".to_string(),
                table_oid: 0,
                column_number: 0,
                data_type: arrow::datatypes::DataType::Int64,
            }],
        )
        .unwrap();

        assert_eq!(
            unnested_fields,
            vec![ProjectedOrigin::TableColumn(TableColumn {
                table: String::from("users"),
                column: String::from("id"),
            })]
        )
    }

    #[test]
    fn test_wildcard() {
        let dialect = PostgreSqlDialect {};
        let query_ast = Parser::parse_sql(&dialect, "SELECT * FROM users")
            .unwrap()
            .pop()
            .unwrap();

        let unnested_fields = trace_projection_origin(
            &query_ast,
            &[
                Field {
                    name: "id".to_string(),
                    table_oid: 0,
                    column_number: 0,
                    data_type: arrow::datatypes::DataType::Int64,
                },
                Field {
                    name: "name".to_string(),
                    table_oid: 0,
                    column_number: 0,
                    data_type: arrow::datatypes::DataType::Int64,
                },
            ],
        )
        .unwrap();

        assert_eq!(
            unnested_fields,
            vec![
                ProjectedOrigin::TableColumn(TableColumn {
                    table: String::from("users"),
                    column: String::from("id"),
                }),
                ProjectedOrigin::TableColumn(TableColumn {
                    table: String::from("users"),
                    column: String::from("name"),
                })
            ]
        )
    }

    #[test]
    fn test_single_field_renamed() {
        let dialect = PostgreSqlDialect {};
        let query_ast = Parser::parse_sql(&dialect, "SELECT id AS 'user_id' FROM users")
            .unwrap()
            .pop()
            .unwrap();

        let unnested_fields = trace_projection_origin(
            &query_ast,
            &[Field {
                name: "user_id".to_string(),
                table_oid: 0,
                column_number: 0,
                data_type: arrow::datatypes::DataType::Int64,
            }],
        )
        .unwrap();

        assert_eq!(
            unnested_fields,
            vec![ProjectedOrigin::TableColumn(TableColumn {
                table: String::from("users"),
                column: String::from("id"),
            })]
        )
    }

    #[test]
    fn test_single_field_from_aliased_table() {
        let dialect = PostgreSqlDialect {};
        let query_ast = Parser::parse_sql(&dialect, "SELECT u.id FROM users u")
            .unwrap()
            .pop()
            .unwrap();

        let unnested_fields = trace_projection_origin(
            &query_ast,
            &[Field {
                name: "id".to_string(),
                table_oid: 0,
                column_number: 0,
                data_type: arrow::datatypes::DataType::Int64,
            }],
        )
        .unwrap();

        assert_eq!(
            unnested_fields,
            vec![ProjectedOrigin::TableColumn(TableColumn {
                table: String::from("users"),
                column: String::from("id"),
            })]
        )
    }

    #[test]
    fn test_multiple_fields_with_join() {
        let dialect = PostgreSqlDialect {};
        let query_ast = Parser::parse_sql(
            &dialect,
            "SELECT u.id, p.id, p.title FROM users u JOIN posts p ON p.author = u.id",
        )
        .unwrap()
        .pop()
        .unwrap();

        let unnested_fields = trace_projection_origin(
            &query_ast,
            &[
                Field {
                    name: "id".to_string(),
                    table_oid: 0,
                    column_number: 0,
                    data_type: arrow::datatypes::DataType::Int64,
                },
                Field {
                    name: "id".to_string(),
                    table_oid: 0,
                    column_number: 0,
                    data_type: arrow::datatypes::DataType::Int64,
                },
                Field {
                    name: "title".to_string(),
                    table_oid: 0,
                    column_number: 0,
                    data_type: arrow::datatypes::DataType::Int64,
                },
            ],
        )
        .unwrap();

        assert_eq!(
            unnested_fields,
            vec![
                ProjectedOrigin::TableColumn(TableColumn {
                    table: String::from("users"),
                    column: String::from("id"),
                }),
                ProjectedOrigin::TableColumn(TableColumn {
                    table: String::from("posts"),
                    column: String::from("id"),
                }),
                ProjectedOrigin::TableColumn(TableColumn {
                    table: String::from("posts"),
                    column: String::from("title"),
                })
            ]
        )
    }

    #[test]
    fn test_function() {
        let dialect = PostgreSqlDialect {};
        let query_ast = Parser::parse_sql(&dialect, "SELECT version()")
            .unwrap()
            .pop()
            .unwrap();

        let unnested_fields = trace_projection_origin(
            &query_ast,
            &[Field {
                name: "version".to_string(),
                table_oid: -1,
                column_number: 0,
                data_type: arrow::datatypes::DataType::Int64,
            }],
        )
        .unwrap();

        assert_eq!(unnested_fields, vec![ProjectedOrigin::Function])
    }

    #[test]
    fn test_value() {
        let dialect = PostgreSqlDialect {};
        let query_ast = Parser::parse_sql(&dialect, "SELECT 'Hello world' as greeting")
            .unwrap()
            .pop()
            .unwrap();

        let unnested_fields = trace_projection_origin(
            &query_ast,
            &[Field {
                name: "greeting".to_string(),
                table_oid: 0,
                column_number: 0,
                data_type: arrow::datatypes::DataType::Utf8,
            }],
        )
        .unwrap();

        assert_eq!(unnested_fields, vec![ProjectedOrigin::Value])
    }

    #[test]
    fn test_wildcard_multiple_tables() {
        let dialect = PostgreSqlDialect {};
        let query_ast = Parser::parse_sql(&dialect, "SELECT * FROM users, posts")
            .unwrap()
            .pop()
            .unwrap();

        let unnested_fields = trace_projection_origin(
            &query_ast,
            &[
                Field {
                    name: "id".to_string(),
                    table_oid: 1,
                    column_number: 0,
                    data_type: arrow::datatypes::DataType::Int64,
                },
                Field {
                    name: "name".to_string(),
                    table_oid: 1,
                    column_number: 0,
                    data_type: arrow::datatypes::DataType::Int64,
                },
                Field {
                    name: "id".to_string(),
                    table_oid: 2,
                    column_number: 0,
                    data_type: arrow::datatypes::DataType::Int64,
                },
                Field {
                    name: "author".to_string(),
                    table_oid: 2,
                    column_number: 0,
                    data_type: arrow::datatypes::DataType::Int64,
                },
                Field {
                    name: "text".to_string(),
                    table_oid: 2,
                    column_number: 0,
                    data_type: arrow::datatypes::DataType::Int64,
                },
            ],
        )
        .unwrap();

        assert_eq!(
            unnested_fields,
            vec![
                ProjectedOrigin::TableColumn(TableColumn {
                    table: String::from("users"),
                    column: String::from("id")
                }),
                ProjectedOrigin::TableColumn(TableColumn {
                    table: String::from("users"),
                    column: String::from("name")
                }),
                ProjectedOrigin::TableColumn(TableColumn {
                    table: String::from("posts"),
                    column: String::from("id")
                }),
                ProjectedOrigin::TableColumn(TableColumn {
                    table: String::from("posts"),
                    column: String::from("author")
                }),
                ProjectedOrigin::TableColumn(TableColumn {
                    table: String::from("posts"),
                    column: String::from("text")
                })
            ]
        )
    }

    // #[test]
    // fn test_aggregation_sum() {
    //     let dialect = PostgreSqlDialect {};
    //     let query_ast = Parser::parse_sql(&dialect, "SELECT SUM(u.id) FROM users u")
    //         .unwrap()
    //         .pop()
    //         .unwrap();

    //     let unnested_fields = get_projected_origin(
    //         &query_ast,
    //         &[Field::new(
    //             "greeting",
    //             arrow::datatypes::DataType::Utf8,
    //             false,
    //         )],
    //     )
    //     .unwrap();
    // }

    // #[test]
    // fn test_subquery() {
    //     let dialect = PostgreSqlDialect {};
    //     let query = r#"
    //         SELECT u.id, u.name, (
    //             SELECT p.text
    //             FROM posts p
    //             WHERE p.author = u.id
    //             LIMIT 1
    //         ) last_post
    //         FROM users u
    //     "#;

    //     let query_ast = Parser::parse_sql(&dialect, query).unwrap().pop().unwrap();

    //     let unnested_fields = get_projected_origin(
    //         &query_ast,
    //         &[
    //             Field::new("id", arrow::datatypes::DataType::Int64, false),
    //             Field::new("name", arrow::datatypes::DataType::Utf8, false),
    //             Field::new("last_post", arrow::datatypes::DataType::Utf8, false),
    //         ],
    //     )
    //     .unwrap();

    //     assert_eq!(
    //         unnested_fields,
    //         vec![
    //             ProjectedOrigin::TableColumn(TableColumn {
    //                 table: String::from("users"),
    //                 column: String::from("id"),
    //             }),
    //             ProjectedOrigin::TableColumn(TableColumn {
    //                 table: String::from("users"),
    //                 column: String::from("name"),
    //             }),
    //             ProjectedOrigin::TableColumn(TableColumn {
    //                 table: String::from("posts"),
    //                 column: String::from("text"),
    //             }),
    //         ]
    //     )
    // }
}