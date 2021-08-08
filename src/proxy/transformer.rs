use arrow::record_batch::RecordBatch;
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;

pub trait ResultTransformer: Sync + Send {
    fn transform_data(&self, data: &RecordBatch) -> RecordBatch;
}

pub trait QueryTransformer: Sync + Send {
    fn transform_query(&self, query: String) -> String {
        query
    }
}

pub trait QueryAstTransformer: Sync + Send {
    fn transform_ast(
        &self,
        query: Vec<sqlparser::ast::Statement>,
    ) -> Vec<sqlparser::ast::Statement>;
}

impl<T> QueryTransformer for T
where
    T: QueryAstTransformer,
{
    fn transform_query(&self, query: String) -> String {
        let dialect = PostgreSqlDialect {};
        let input_ast = Parser::parse_sql(&dialect, &query).unwrap();

        println!("Parsed AST: {:?}", input_ast);

        let output_ast = self.transform_ast(input_ast);

        output_ast
            .iter()
            .map(std::string::ToString::to_string)
            .collect::<Vec<_>>()
            .join("\n")
    }
}
