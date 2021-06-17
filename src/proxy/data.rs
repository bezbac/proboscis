use super::connection::Connection;
use crate::proxy::{connection::ProtocolStream};
use crate::{protocol::Message, Transformer};
use anyhow::Result;
use arrow::array::as_primitive_array;
use arrow::array::{ArrayRef, GenericStringArray, Int16Array, Int32Array, Int64Array, Int8Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use omnom::prelude::*;
use std::{sync::Arc, vec};

impl From<crate::protocol::message::Field> for Field {
    fn from(value: crate::protocol::message::Field) -> Self {
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

        Field::new(&value.name, arrow_type, false)
    }
}

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

async fn protocol_rows_to_arrow_columns(
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

#[derive(Clone)]
pub struct SimpleQueryResponse {
    pub data: RecordBatch,
    pub fields: Vec<crate::protocol::message::Field>,
    pub tag: String,
}

impl SimpleQueryResponse {
    pub async fn read(connection: &mut Connection) -> Result<Self> {
        let mut protocol_fields: Vec<crate::protocol::message::Field> = vec![];
        let mut protocol_row_data: Vec<Vec<Vec<u8>>> = vec![];
        let mut protocol_tag: Option<String> = Option::None;

        loop {
            let response = connection.read_message().await?;
            match response {
                Message::ReadyForQuery => break,
                Message::RowDescription { fields } => protocol_fields = fields,
                Message::DataRow { field_data } => {
                    protocol_row_data.push(field_data);
                }
                Message::CommandComplete { tag } => {
                    protocol_tag = Some(tag);
                }
                _ => unimplemented!(""),
            }
        }

        let fields = protocol_fields
            .iter()
            .map(|message_field| message_field.clone().into())
            .collect::<Vec<Field>>();

        let schema = Schema::new(fields);

        let columns = protocol_rows_to_arrow_columns(&schema, protocol_row_data).await?;

        let batch = RecordBatch::try_new(Arc::new(schema), columns)?;

        Ok(SimpleQueryResponse {
            data: batch,
            fields: protocol_fields,
            tag: protocol_tag.unwrap(),
        })
    }

    pub async fn serialize(
        &self,
        transformers: &Vec<Box<dyn Transformer>>,
    ) -> Result<Vec<Message>> {
        let transformed = transformers.iter().fold(self.data.clone(), |data, transformer| transformer.transform(&data));

        let mut messages = vec![];

        messages.push(Message::RowDescription {
            fields: self.fields.clone(),
        });

        for row_index in 0..transformed.num_rows() {
            let mut row_data = vec![];

            for column in transformed.columns() {
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

            messages.push(Message::DataRow {
                field_data: row_data,
            })
        }

        messages.push(Message::CommandComplete {
            tag: self.tag.clone(),
        });

        Ok(messages)
    }
}
