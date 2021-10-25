use anyhow::Result;
use arrow::array::{as_primitive_array, make_array, ArrayData, ListArray};
use arrow::array::{Array, GenericListArray, UInt8Array};
use arrow::array::{ArrayRef, GenericStringArray, Int16Array, Int32Array, Int64Array, Int8Array};
use arrow::buffer::Buffer;
use arrow::datatypes::{DataType, Field, Schema, ToByteSlice, UInt8Type};
use arrow::record_batch::RecordBatch;
use omnom::prelude::*;
use postgres_protocol::message::{DataRow, RowDescription};
use std::{collections::BTreeMap, sync::Arc, vec};

fn column_data_to_array(data: &[Option<Vec<u8>>], data_type: &DataType) -> ArrayRef {
    match data_type {
        DataType::Int8 => {
            let data_array = data
                .iter()
                .map(|d| {
                    d.as_ref().map(|d| {
                        let mut cursor = std::io::Cursor::new(d);
                        let value: i8 = cursor.read_be().unwrap();
                        value
                    })
                })
                .collect::<Vec<Option<i8>>>();

            Arc::new(Int8Array::from(data_array))
        }
        DataType::Int16 => {
            let data_array = data
                .iter()
                .map(|d| {
                    d.as_ref().map(|d| {
                        let mut bytes = d.clone();
                        while bytes.len() < 2 {
                            bytes.push(0);
                        }

                        let mut cursor = std::io::Cursor::new(bytes);
                        let value: i16 = cursor.read_be().unwrap();
                        value
                    })
                })
                .collect::<Vec<Option<i16>>>();

            Arc::new(Int16Array::from(data_array))
        }
        DataType::Int32 => {
            let data_array = data
                .iter()
                .map(|d| {
                    d.as_ref().map(|d| {
                        let mut bytes = d.clone();
                        while bytes.len() < 4 {
                            bytes.push(0);
                        }

                        let mut cursor = std::io::Cursor::new(bytes);
                        let value: i32 = cursor.read_be().unwrap();
                        value
                    })
                })
                .collect::<Vec<Option<i32>>>();

            Arc::new(Int32Array::from(data_array))
        }
        DataType::Int64 => {
            let data_array = data
                .iter()
                .map(|d| {
                    d.as_ref().map(|d| {
                        let mut bytes = d.clone();
                        while bytes.len() < 8 {
                            bytes.push(0);
                        }

                        let mut cursor = std::io::Cursor::new(bytes);
                        let value: i64 = cursor.read_be().unwrap();
                        value
                    })
                })
                .collect::<Vec<Option<i64>>>();

            Arc::new(Int64Array::from(data_array))
        }
        DataType::Utf8 => {
            let data_array: Vec<Option<String>> = data
                .iter()
                .map(|d| d.as_ref().map(|d| String::from_utf8(d.to_vec()).unwrap()))
                .collect();

            Arc::new(GenericStringArray::<i32>::from(
                data_array
                    .iter()
                    .map(|s| s.as_ref().map(|s| s.as_ref()))
                    .collect::<Vec<Option<&str>>>(),
            ))
        }
        DataType::LargeUtf8 => {
            let data_array: Vec<Option<String>> = data
                .iter()
                .map(|d| d.as_ref().map(|d| String::from_utf8(d.to_vec()).unwrap()))
                .collect();

            Arc::new(GenericStringArray::<i64>::from(
                data_array
                    .iter()
                    .map(|s| s.as_ref().map(|s| s.as_ref()))
                    .collect::<Vec<Option<&str>>>(),
            ))
        }
        DataType::List(field) => {
            let data_array: Vec<Option<Vec<Option<u8>>>> = data
                .iter()
                .map(|d| d.as_ref().map(|d| d.iter().map(|d| Some(*d)).collect()))
                .collect();

            let array = ListArray::from_iter_primitive::<UInt8Type, _, _>(data_array);

            let list_data_type = DataType::List(field.clone());

            let list_data = ArrayData::builder(list_data_type)
                .len(array.len())
                .add_buffer(Buffer::from(array.value_offsets().to_byte_slice()))
                .add_child_data(array.values().data().clone())
                .build();

            make_array(list_data)
        }
        _ => todo!("{}", data_type),
    }
}

fn protocol_rows_to_arrow_columns(
    schema: &Schema,
    rows: Vec<Vec<Option<Vec<u8>>>>,
) -> Result<Vec<ArrayRef>> {
    let mut columns_data: Vec<Vec<Option<Vec<u8>>>> =
        schema.fields().iter().map(|_| vec![]).collect();
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

fn postgres_type_for_arrow_type(
    arrow_type: &DataType,
    original_type: Option<postgres::types::Type>,
) -> postgres::types::Type {
    match arrow_type {
        DataType::Boolean => postgres::types::Type::BOOL,
        DataType::Int8 => postgres::types::Type::INT2,
        DataType::Int16 => postgres::types::Type::INT4,
        DataType::Int32 => postgres::types::Type::INT8,
        DataType::LargeUtf8 => postgres::types::Type::TEXT,
        DataType::Utf8 => match original_type {
            Some(postgres::types::Type::NAME) => postgres::types::Type::NAME,
            _ => postgres::types::Type::VARCHAR,
        },
        DataType::List(field) => match field.name().as_str() {
            "unnamed_name_array" => postgres::types::Type::NAME_ARRAY,
            _ => todo!("{}", arrow_type),
        },
        _ => todo!("{}", arrow_type),
    }
}

fn message_field_to_arrow_field(value: &postgres_protocol::message::Field) -> Field {
    let postgres_type = postgres::types::Type::from_oid(value.type_oid as u32).unwrap();

    let arrow_type = match postgres_type {
        postgres::types::Type::BOOL => DataType::Boolean,
        postgres::types::Type::INT2 => DataType::Int8,
        postgres::types::Type::INT4 => DataType::Int16,
        postgres::types::Type::INT8 => DataType::Int32,
        postgres::types::Type::TEXT => DataType::LargeUtf8,
        postgres::types::Type::VARCHAR => DataType::Utf8,
        postgres::types::Type::NAME => DataType::Utf8,
        postgres::types::Type::NAME_ARRAY => DataType::List(Box::new(Field::new(
            "unnamed_name_array",
            DataType::UInt8,
            true,
        ))),
        _ => todo!("{}", postgres_type),
    };

    let mut metadata = BTreeMap::new();
    metadata.insert("table_oid".to_string(), format!("{}", value.table_oid));
    metadata.insert(
        "column_number".to_string(),
        format!("{}", value.column_number),
    );
    metadata.insert("format".to_string(), format!("{}", value.format));
    metadata.insert("type_length".to_string(), format!("{}", value.type_length));
    metadata.insert(
        "type_modifier".to_string(),
        format!("{}", value.type_modifier),
    );

    metadata.insert(
        "original_type_oid".to_string(),
        postgres_type.oid().to_string(),
    );

    let mut field = Field::new(&value.name, arrow_type, false);
    field.set_metadata(Some(metadata));

    field
}

fn arrow_field_to_message_field(value: &Field) -> postgres_protocol::message::Field {
    let original_type = postgres::types::Type::from_oid(
        value
            .metadata()
            .clone()
            .unwrap()
            .get("original_type_oid")
            .unwrap()
            .parse()
            .unwrap(),
    );

    let postgres_type = postgres_type_for_arrow_type(value.data_type(), original_type);

    let metadata = value.metadata().clone().unwrap();

    let table_oid = metadata.get("table_oid").unwrap().clone().parse().unwrap();
    let column_number = metadata
        .get("column_number")
        .unwrap()
        .clone()
        .parse()
        .unwrap();
    let format = metadata.get("format").unwrap().clone().parse().unwrap();

    // TODO: These can probably be inferred based on some criteria
    let type_length = metadata
        .get("type_length")
        .unwrap()
        .clone()
        .parse()
        .unwrap();
    let type_modifier = metadata
        .get("type_modifier")
        .unwrap()
        .clone()
        .parse()
        .unwrap();

    postgres_protocol::message::Field {
        type_oid: postgres_type.oid() as i32,
        name: value.name().clone(),
        column_number,
        table_oid,
        format,
        type_length,
        type_modifier,
    }
}

pub fn protocol_fields_to_schema(fields: &[postgres_protocol::message::Field]) -> Schema {
    let fields = fields
        .iter()
        .map(|message_field| message_field_to_arrow_field(message_field))
        .collect::<Vec<Field>>();

    Schema::new(fields)
}

pub fn simple_query_response_to_record_batch(
    fields: &[postgres_protocol::message::Field],
    data: &[DataRow],
) -> Result<RecordBatch> {
    let schema = protocol_fields_to_schema(fields);

    let protocol_row_data = data
        .iter()
        .map(|DataRow { field_data }| field_data.clone())
        .collect();

    let columns = protocol_rows_to_arrow_columns(&schema, protocol_row_data)?;

    RecordBatch::try_new(Arc::new(schema), columns).map_err(|err| anyhow::anyhow!(err))
}

fn write_be_without_trailing_zeros<V: omnom::WriteBytes, W: std::io::Write>(
    value: V,
    buffer: &mut W,
) -> std::io::Result<usize> {
    let mut bytes = vec![];
    value.write_be_bytes(&mut bytes).unwrap();

    while bytes.last() == Some(&0) {
        bytes.pop();
    }

    buffer.write(&bytes)
}

pub fn serialize_record_batch_to_data_rows(batch: &RecordBatch) -> Vec<DataRow> {
    (0..batch.num_rows())
        .map(|row_index| {
            let mut row_data = vec![];

            for column in batch.columns() {
                let mut cell: Vec<u8> = vec![];
                match column.data_type() {
                    DataType::Int8 => {
                        let values: &Int8Array = as_primitive_array(column);
                        let value: i8 = values.value(row_index);
                        value.write_be_bytes(&mut cell).unwrap();
                    }
                    DataType::Int16 => {
                        let values: &Int16Array = as_primitive_array(column);
                        let value: i16 = values.value(row_index);
                        write_be_without_trailing_zeros(value, &mut cell).unwrap();
                    }
                    DataType::Int32 => {
                        let values: &Int32Array = as_primitive_array(column);
                        let value: i32 = values.value(row_index);
                        write_be_without_trailing_zeros(value, &mut cell).unwrap();
                    }
                    DataType::Int64 => {
                        let values: &Int64Array = as_primitive_array(column);
                        let value: i64 = values.value(row_index);
                        write_be_without_trailing_zeros(value, &mut cell).unwrap();
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
                    DataType::List(_) => {
                        let values = &column
                            .as_any()
                            .downcast_ref::<GenericListArray<i32>>()
                            .unwrap();

                        let row_value = values.value(row_index);

                        let value = row_value
                            .as_any()
                            .downcast_ref::<UInt8Array>()
                            .unwrap()
                            .values();

                        cell.extend_from_slice(value)
                    }
                    _ => unimplemented!(),
                }
                row_data.push(Some(cell))
            }

            DataRow {
                field_data: row_data,
            }
        })
        .collect()
}

pub fn serialize_record_batch_schema_to_row_description(schema: &Schema) -> RowDescription {
    let fields: Vec<postgres_protocol::message::Field> = schema
        .fields()
        .iter()
        .map(|arrow_field| arrow_field_to_message_field(arrow_field))
        .collect();
    RowDescription { fields }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::Rng;
    use std::iter;

    #[test]
    fn test_name_array() {
        let row_data: Vec<Vec<Option<Vec<u8>>>> =
            iter::repeat(vec![Some(rand::thread_rng().gen::<[u8; 32]>().to_vec())])
                .take(4)
                .collect();

        let list_data_type = DataType::List(Box::new(Field::new(
            "unnamed_name_array",
            DataType::UInt8,
            true,
        )));

        let schema = Schema::new(vec![Field::new("some_list", list_data_type, true)]);
        let columns = protocol_rows_to_arrow_columns(&schema, row_data.clone()).unwrap();
        let batch = RecordBatch::try_new(Arc::new(schema), columns).unwrap();

        let deserialized = serialize_record_batch_to_data_rows(&batch);

        assert_eq!(4, deserialized.len());

        let byte_rows: Vec<Vec<Option<Vec<u8>>>> = deserialized
            .iter()
            .map(|data_row| data_row.field_data.clone())
            .collect();

        assert_eq!(row_data, byte_rows);
    }

    #[test]
    fn test_symmetric_serialization_deserialization() {
        let fields = vec![
            postgres_protocol::message::Field {
                name: "a".to_string(),
                table_oid: 0,
                column_number: 0,
                type_oid: 19,
                type_length: 64,
                type_modifier: -1,
                format: 0,
            },
            postgres_protocol::message::Field {
                name: "b".to_string(),
                table_oid: 0,
                column_number: 0,
                type_oid: 1003,
                type_length: -1,
                type_modifier: -1,
                format: 0,
            },
        ];

        let data = vec![DataRow {
            field_data: vec![
                Some(vec![112, 111, 115, 116, 103, 114, 101, 115]),
                Some(vec![123, 112, 117, 98, 108, 105, 99, 125]),
            ],
        }];

        let batch = simple_query_response_to_record_batch(&fields, &data).unwrap();

        let deserialized_row_description =
            serialize_record_batch_schema_to_row_description(&batch.schema());
        let deserialized_data = serialize_record_batch_to_data_rows(&batch);

        assert_eq!(fields, deserialized_row_description.fields);
        assert_eq!(data, deserialized_data);
    }
}
