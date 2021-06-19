use crate::{protocol::Message};
use anyhow::Result;
use arrow::array::as_primitive_array;
use arrow::array::{ArrayRef, GenericStringArray, Int16Array, Int32Array, Int64Array, Int8Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use omnom::prelude::*;
use std::{collections::BTreeMap, sync::Arc, vec};

fn column_data_to_array(data: &Vec<Vec<u8>>, data_type: &DataType) -> ArrayRef {
    match data_type {
        DataType::Int8 => {
            let data_array = data
                .iter()
                .map(|d| {
                    let mut cursor = std::io::Cursor::new(d);
                    let value: i8 = cursor.read_be().unwrap();
                    value
                })
                .collect::<Vec<i8>>();

            Arc::new(Int8Array::from(data_array))
        }
        DataType::Int16 => {
            let data_array = data
                .iter()
                .map(|d| {
                    let mut cursor = std::io::Cursor::new(d);
                    let value: i8 = cursor.read_be().unwrap(); // TODO: This should not be i8
                    value as i16
                })
                .collect::<Vec<i16>>();

            Arc::new(Int16Array::from(data_array))
        }
        DataType::Int32 => {
            let data_array = data
                .iter()
                .map(|d| {
                    let mut cursor = std::io::Cursor::new(d);
                    let value: i32 = cursor.read_be().unwrap();
                    value
                })
                .collect::<Vec<i32>>();

            Arc::new(Int32Array::from(data_array))
        }
        DataType::Int64 => {
            let data_array = data
                .iter()
                .map(|d| {
                    let mut cursor = std::io::Cursor::new(d);
                    let value: i64 = cursor.read_be().unwrap();
                    value
                })
                .collect::<Vec<i64>>();

            Arc::new(Int64Array::from(data_array))
        }
        DataType::Utf8 => {
            let data_array: Vec<String> = data
                .iter()
                .map(|d| String::from_utf8(d.clone()).unwrap())
                .collect();

            Arc::new(GenericStringArray::<i32>::from(
                data_array.iter().map(|s| s.as_ref()).collect::<Vec<&str>>(),
            ))
        }
        DataType::LargeUtf8 => {
            let data_array: Vec<String> = data
                .iter()
                .map(|d| String::from_utf8(d.clone()).unwrap())
                .collect();

            Arc::new(GenericStringArray::<i64>::from(
                data_array.iter().map(|s| s.as_ref()).collect::<Vec<&str>>(),
            ))
        }
        _ => unimplemented!(),
    }
}

fn protocol_rows_to_arrow_columns(
    schema: &Schema,
    rows: Vec<Vec<Vec<u8>>>,
) -> Result<Vec<ArrayRef>> {
    let mut columns_data: Vec<Vec<Vec<u8>>> = schema.fields().iter().map(|f| vec![]).collect();
    let columns_data_types: Vec<DataType> = schema
        .fields()
        .iter()
        .map(|f| f.data_type().clone())
        .collect();

    for row in rows {
        for (index, field) in row.iter().enumerate() {
            columns_data[index].push(field.clone());
        }
    }

    let result = columns_data
        .iter()
        .zip(columns_data_types.iter())
        .map(|(column_data, data_type)| column_data_to_array(column_data, data_type))
        .collect();

    Ok(result)
}

fn message_field_to_arrow_field(value: &crate::protocol::message::Field) -> Field {
    let postgres_type = postgres::types::Type::from_oid(value.type_oid as u32).unwrap();

    let arrow_type = match postgres_type {
        postgres::types::Type::BOOL => DataType::Boolean,
        postgres::types::Type::INT2 => DataType::Int8,
        postgres::types::Type::INT4 => DataType::Int16,
        postgres::types::Type::INT8 => DataType::Int32,
        postgres::types::Type::TEXT => DataType::LargeUtf8,
        postgres::types::Type::VARCHAR => DataType::Utf8,
        _ => unimplemented!(),
    };

    let mut metadata = BTreeMap::new();
    metadata.insert("table_oid".to_string(), format!("{}", value.table_oid));
    metadata.insert("format".to_string(), format!("{}", value.format));
    metadata.insert("type_length".to_string(), format!("{}", value.type_length));
    metadata.insert("type_modifier".to_string(), format!("{}", value.type_modifier));
    
    let mut field = Field::new(&value.name, arrow_type, false);
    field.set_metadata(Some(metadata));

    field
}

fn arrow_field_to_message_field(value: &Field, column_number: i16) -> crate::protocol::message::Field {
    let postgres_type = match value.data_type() {
        DataType::Boolean => postgres::types::Type::BOOL,
        DataType::Int8 => postgres::types::Type::INT2,
        DataType::Int16 => postgres::types::Type::INT4,
        DataType::Int32 => postgres::types::Type::INT8,
        DataType::LargeUtf8 => postgres::types::Type::TEXT,
        DataType::Utf8 => postgres::types::Type::VARCHAR,
        _ => unimplemented!(),
    };

    let metadata = value.metadata().clone().unwrap();

    let table_oid = metadata.get("table_oid").unwrap().clone().parse().unwrap();
    let format = metadata.get("format").unwrap().clone().parse().unwrap();

    // TODO: These can probably be inferred based on some criteria
    let type_length = metadata.get("type_length").unwrap().clone().parse().unwrap();
    let type_modifier = metadata.get("type_modifier").unwrap().clone().parse().unwrap();

    crate::protocol::message::Field {
        type_oid: postgres_type.oid() as i32,
        name: value.name().clone(),
        column_number,
        table_oid,
        format,
        type_length,
        type_modifier
    }
}

pub async fn simple_query_response_to_record_batch(fields: &Vec<crate::protocol::message::Field> , data: &Vec<Message>) -> Result<RecordBatch> {
    let fields = fields
            .iter()
            .map(|message_field| message_field_to_arrow_field(message_field))
            .collect::<Vec<Field>>();

    let schema = Schema::new(fields);

    let protocol_row_data = data.iter().map(|message| {
        if let Message::DataRow { field_data } = message {
            field_data.clone()
        } else {
            panic!()
        }
    }).collect();

    let columns = protocol_rows_to_arrow_columns(&schema, protocol_row_data)?;

    RecordBatch::try_new(Arc::new(schema), columns).map_err(|err| anyhow::anyhow!(err))
}

pub fn serialize_record_batch_to_data_rows(batch: RecordBatch) -> Vec<Message> {
    (0..batch.num_rows()).map(|row_index| {
        let mut row_data = vec![];

        for column in batch.columns() {
            let mut cell: Vec<u8> = vec![];
            match column.data_type() {
                DataType::Int8 => {
                    let values: &Int8Array = as_primitive_array(&column);
                    let value: i8 = values.value(row_index);

                    value.write_be_bytes(&mut cell).unwrap();
                }
                DataType::Int16 => {
                    let values: &Int16Array = as_primitive_array(&column);
                    let value: i16 = values.value(row_index);
                    value.write_be_bytes(&mut cell).unwrap();
                }
                DataType::Int32 => {
                    let values: &Int32Array = as_primitive_array(&column);
                    let value: i32 = values.value(row_index);
                    value.write_be_bytes(&mut cell).unwrap();
                }
                DataType::Int64 => {
                    let values: &Int64Array = as_primitive_array(&column);
                    let value: i64 = values.value(row_index);
                    value.write_be_bytes(&mut cell).unwrap();
                }
                DataType::LargeUtf8 => {
                    let values = &column
                        .as_any()
                        .downcast_ref::<GenericStringArray<i64>>()
                        .unwrap();
                    let value = values.value(row_index);
                    cell.extend_from_slice(value.as_bytes())
                }
                DataType::Utf8 => {
                    let values = &column
                        .as_any()
                        .downcast_ref::<GenericStringArray<i32>>()
                        .unwrap();
                    let value = values.value(row_index);
                    cell.extend_from_slice(value.as_bytes())
                }
                _ => unimplemented!(),
            }
            row_data.push(cell)
        }

        Message::DataRow {
            field_data: row_data,
        }
    }).collect()
}

pub fn serialize_record_batch_schema_to_row_description(schema: Arc<Schema>) -> Message {
    let fields: Vec<crate::protocol::message::Field> = schema.fields().into_iter().enumerate().map(|(idx, arrow_field)| arrow_field_to_message_field(arrow_field, idx as i16)).collect();
    Message::RowDescription {
        fields,
    }
}