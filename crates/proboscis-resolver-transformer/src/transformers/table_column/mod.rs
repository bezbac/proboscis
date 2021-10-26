use crate::{column_transformations::ColumnTransformation, traits::Transformer, util::{TableColumn, get_projected_origin}};
use anyhow::Result;
use arrow::{
    datatypes::{Field, Schema},
    record_batch::RecordBatch,
};
use sqlparser::ast::Statement;
use std::{collections::HashMap, sync::Arc};

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
        let origins = get_projected_origin(statement, schema.fields()).unwrap();

        schema
            .fields()
            .iter()
            .zip(origins.iter())
            .enumerate()
            .filter_map(|(index, (_, origin))| {
                // TODO: Reconsider using a string here
                let normalized_field_name = match origin {
                    crate::util::ProjectedOrigin::Function => return None,
                    crate::util::ProjectedOrigin::Value => return None,
                    crate::util::ProjectedOrigin::TableColumn(TableColumn { table, column }) => {
                        format!("{}.{}", table, column)
                    }
                    _ => todo!(),
                };

                self.transformations
                    .get(&normalized_field_name)
                    .map(|transformations| (index, transformations))
            })
            .collect()
    }
}

fn transform_field(field: &Field, transformation: &dyn ColumnTransformation) -> Result<Field> {
    transformation
        .output_format(field.data_type())
        .map(|output| Field::new(field.name(), output.data_type, output.nullable))
}

impl Transformer for TableColumnTransformer {
    fn transform_schema(&self, query: &[Statement], schema: &Schema) -> Result<Schema> {
        let column_transformations =
            self.get_column_transformations(query.first().unwrap(), schema);

        if column_transformations.keys().len() == 0 {
            return Ok(schema.clone());
        }

        let mut new_fields = vec![];
        for (index, field) in schema.fields().iter().enumerate() {
            let new_field = match column_transformations.get(&index) {
                Some(transformations) => {
                    let mut transformed = field.clone();
                    for transformation in transformations.iter() {
                        transformed = transform_field(field, transformation.as_ref())?;
                    }

                    transformed
                }
                None => field.clone(),
            };

            new_fields.push(new_field)
        }

        Ok(Schema::new(new_fields))
    }

    fn transform_records(&self, query: &[Statement], data: &RecordBatch) -> Result<RecordBatch> {
        let column_transformations =
            self.get_column_transformations(query.first().unwrap(), &data.schema());

        if column_transformations.keys().len() == 0 {
            return Ok(data.clone());
        }

        let mut new_fields = vec![];
        let mut new_data = vec![];
        for (index, field) in data.schema().fields().iter().enumerate() {
            let column_data = data.column(index).clone();
            match column_transformations.get(&index) {
                Some(transformations) => {
                    let mut transformed_field = field.clone();
                    let mut transformed_data = column_data;
                    for transformation in transformations.iter() {
                        transformed_field =
                            transform_field(&transformed_field, transformation.as_ref())?;
                        transformed_data = transformation.transform_data(transformed_data)?;
                    }

                    new_fields.push(transformed_field);
                    new_data.push(transformed_data);
                }
                None => {
                    new_fields.push(field.clone());
                    new_data.push(column_data);
                }
            };
        }

        let new_schema = Schema::new(new_fields);

        Ok(RecordBatch::try_new(Arc::new(new_schema), new_data)?)
    }
}
