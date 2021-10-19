use sqlparser::ast::{
    Expr, Ident, ObjectName, SelectItem, SetExpr, Statement, TableAlias, TableFactor,
};

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
                        SelectItem::UnnamedExpr(expression) => match expression {
                            Expr::Function(_) => String::from("function"),
                            _ => todo!("{:?}", expression),
                        },
                        _ => todo!("{:?}", item),
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
                }

                Ok(fields)
            }
            _ => unimplemented!(),
        },
        _ => unimplemented!(),
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

        let unnested_fields = get_schema_fields(&query_ast).unwrap();
        assert_eq!(unnested_fields, vec!["users.id"])
    }

    #[test]
    fn test_all_fields() {
        let dialect = PostgreSqlDialect {};
        let _query_ast = Parser::parse_sql(&dialect, "SELECT * FROM users")
            .unwrap()
            .pop()
            .unwrap();

        // TODO: Refactor function to take schema as well, in order to handle these
    }

    #[test]
    fn test_single_field_renamed() {
        let dialect = PostgreSqlDialect {};
        let query_ast = Parser::parse_sql(&dialect, "SELECT id AS 'user_id' FROM users")
            .unwrap()
            .pop()
            .unwrap();

        let unnested_fields = get_schema_fields(&query_ast).unwrap();
        assert_eq!(unnested_fields, vec!["users.id"])
    }

    #[test]
    fn test_single_field_from_aliased_table() {
        let dialect = PostgreSqlDialect {};
        let query_ast = Parser::parse_sql(&dialect, "SELECT u.id FROM users u")
            .unwrap()
            .pop()
            .unwrap();

        let unnested_fields = get_schema_fields(&query_ast).unwrap();
        assert_eq!(unnested_fields, vec!["users.id"])
    }

    #[test]
    fn test_multiple_fields_with_join() {
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

    #[test]
    fn test_function() {
        let dialect = PostgreSqlDialect {};
        let query_ast = Parser::parse_sql(&dialect, "SELECT version()")
            .unwrap()
            .pop()
            .unwrap();

        let unnested_fields = get_schema_fields(&query_ast).unwrap();
        assert_eq!(unnested_fields, vec!["function"])
    }
}
