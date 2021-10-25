use arrow::datatypes::Field;
use itertools::Itertools;
use sqlparser::ast::{
    Expr, Ident, ObjectName, SelectItem, SetExpr, Statement, TableAlias, TableFactor,
};

#[derive(Clone, Debug, PartialEq)]
pub enum ProjectedOrigin {
    TableColumn { table: String, column: String },
    Value,
    Function,
}

pub fn get_projected_origin(
    ast: &Statement,
    fields: &[Field],
) -> anyhow::Result<Vec<ProjectedOrigin>> {
    match ast {
        Statement::Query(query) => match &query.body {
            SetExpr::Select(select) => {
                unimplemented!();
            }
            _ => unimplemented!(),
        },
        _ => anyhow::bail!("get_projected_origin is only implemented for queries"),
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

        let unnested_fields = get_projected_origin(
            &query_ast,
            &[Field::new("id", arrow::datatypes::DataType::Int64, false)],
        )
        .unwrap();

        assert_eq!(
            unnested_fields,
            vec![ProjectedOrigin::TableColumn {
                table: String::from("users"),
                column: String::from("id"),
            }]
        )
    }

    #[test]
    fn test_all_fields() {
        let dialect = PostgreSqlDialect {};
        let query_ast = Parser::parse_sql(&dialect, "SELECT * FROM users")
            .unwrap()
            .pop()
            .unwrap();

        let unnested_fields = get_projected_origin(
            &query_ast,
            &[
                Field::new("id", arrow::datatypes::DataType::Int64, false),
                Field::new("name", arrow::datatypes::DataType::Utf8, false),
            ],
        )
        .unwrap();

        assert_eq!(
            unnested_fields,
            vec![ProjectedOrigin::TableColumn {
                table: String::from("users"),
                column: String::from("id"),
            }]
        )
    }

    #[test]
    fn test_single_field_renamed() {
        let dialect = PostgreSqlDialect {};
        let query_ast = Parser::parse_sql(&dialect, "SELECT id AS 'user_id' FROM users")
            .unwrap()
            .pop()
            .unwrap();

        let unnested_fields = get_projected_origin(
            &query_ast,
            &[Field::new(
                "user_id",
                arrow::datatypes::DataType::Int64,
                false,
            )],
        )
        .unwrap();

        assert_eq!(
            unnested_fields,
            vec![ProjectedOrigin::TableColumn {
                table: String::from("users"),
                column: String::from("id"),
            }]
        )
    }

    #[test]
    fn test_single_field_from_aliased_table() {
        let dialect = PostgreSqlDialect {};
        let query_ast = Parser::parse_sql(&dialect, "SELECT u.id FROM users u")
            .unwrap()
            .pop()
            .unwrap();

        let unnested_fields = get_projected_origin(
            &query_ast,
            &[Field::new("u.id", arrow::datatypes::DataType::Int64, false)],
        )
        .unwrap();

        assert_eq!(
            unnested_fields,
            vec![ProjectedOrigin::TableColumn {
                table: String::from("users"),
                column: String::from("id"),
            }]
        )
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

        let unnested_fields = get_projected_origin(
            &query_ast,
            &[
                Field::new("u.id", arrow::datatypes::DataType::Int64, false),
                Field::new("p.id", arrow::datatypes::DataType::Int64, false),
                Field::new("p.title", arrow::datatypes::DataType::Utf8, false),
            ],
        )
        .unwrap();

        assert_eq!(
            unnested_fields,
            vec![
                ProjectedOrigin::TableColumn {
                    table: String::from("users"),
                    column: String::from("id"),
                },
                ProjectedOrigin::TableColumn {
                    table: String::from("posts"),
                    column: String::from("id"),
                },
                ProjectedOrigin::TableColumn {
                    table: String::from("posts"),
                    column: String::from("title"),
                }
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

        let unnested_fields = get_projected_origin(
            &query_ast,
            &[Field::new(
                "version()",
                arrow::datatypes::DataType::Int64,
                false,
            )],
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

        let unnested_fields = get_projected_origin(
            &query_ast,
            &[Field::new(
                "greeting",
                arrow::datatypes::DataType::Utf8,
                false,
            )],
        )
        .unwrap();

        assert_eq!(unnested_fields, vec![ProjectedOrigin::Value])
    }
}
