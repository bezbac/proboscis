use crate::{
    column_transformations::{ColumnTransformation, ColumnTransformationOutput},
    traits::Transformer,
    util::get_schema_fields,
};
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
