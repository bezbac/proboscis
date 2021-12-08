use anyhow::Result;
use arrow::array::{
    as_primitive_array, make_array, ArrayData, BooleanArray, ListArray, UInt16Array, UInt32Array,
    UInt64Array,
};
use arrow::array::{Array, GenericListArray, UInt8Array};
use arrow::array::{ArrayRef, GenericStringArray, Int16Array, Int32Array, Int64Array, Int8Array};
use arrow::buffer::Buffer;
use arrow::datatypes::{DataType, Field, Schema, ToByteSlice, UInt8Type};
use arrow::record_batch::RecordBatch;
use omnom::prelude::*;
use postgres_protocol::message::{DataRow, RowDescription};
use std::{collections::BTreeMap, sync::Arc, vec};

macro_rules! create_numerical_column_data_to_array_function {
    ($func_name:ident, $Array:ident, $type:ty, $byte_count:literal) => {
        fn $func_name(data: &[Option<Vec<u8>>]) -> ArrayRef {
            Arc::new(
                data.iter()
                    .map(|d| {
                        d.as_ref().map(|d| {
                            let mut bytes = d.clone();
                            while bytes.len() < $byte_count {
                                bytes.push(0);
                            }

                            let mut cursor = std::io::Cursor::new(bytes);
                            let value: $type = cursor.read_be().unwrap();
                            value
                        })
                    })
                    .collect::<$Array>(),
            )
        }
    };
}

create_numerical_column_data_to_array_function!(column_data_to_array_i8, Int8Array, i8, 1);
create_numerical_column_data_to_array_function!(column_data_to_array_i16, Int16Array, i16, 2);
create_numerical_column_data_to_array_function!(column_data_to_array_i32, Int32Array, i32, 4);
create_numerical_column_data_to_array_function!(column_data_to_array_i64, Int64Array, i64, 8);

create_numerical_column_data_to_array_function!(column_data_to_array_u8, UInt8Array, u8, 1);
create_numerical_column_data_to_array_function!(column_data_to_array_u16, UInt16Array, u16, 2);
create_numerical_column_data_to_array_function!(column_data_to_array_u32, UInt32Array, u32, 4);
create_numerical_column_data_to_array_function!(column_data_to_array_u64, UInt64Array, u64, 8);

fn column_data_to_array(data: &[Option<Vec<u8>>], data_type: &DataType) -> ArrayRef {
    match data_type {
        DataType::Int8 => column_data_to_array_i8(data),
        DataType::Int16 => column_data_to_array_i16(data),
        DataType::Int32 => column_data_to_array_i32(data),
        DataType::Int64 => column_data_to_array_i64(data),

        DataType::UInt8 => column_data_to_array_u8(data),
        DataType::UInt16 => column_data_to_array_u16(data),
        DataType::UInt32 => column_data_to_array_u32(data),
        DataType::UInt64 => column_data_to_array_u64(data),

        DataType::Utf8 => Arc::new(
            data.iter()
                .map(|d| d.as_ref().map(|d| String::from_utf8(d.to_vec()).unwrap()))
                .collect::<GenericStringArray<i32>>(),
        ),
        DataType::LargeUtf8 => Arc::new(
            data.iter()
                .map(|d| d.as_ref().map(|d| String::from_utf8(d.to_vec()).unwrap()))
                .collect::<GenericStringArray<i64>>(),
        ),
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
        DataType::Boolean => Arc::new(BooleanArray::from(
            data.iter()
                .map(|d| d.as_ref().map(|d| d == &[0]))
                .collect::<Vec<Option<bool>>>(),
        )),
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
        DataType::Int8 => postgres::types::Type::CHAR,
        DataType::Int16 => postgres::types::Type::INT2,
        DataType::Int32 => postgres::types::Type::INT4,
        DataType::Int64 => postgres::types::Type::INT8,
        DataType::UInt16 => postgres::types::Type::OID,
        DataType::LargeUtf8 => postgres::types::Type::TEXT,
        DataType::Utf8 => match original_type {
            Some(postgres::types::Type::NAME) => postgres::types::Type::NAME,
            Some(postgres::types::Type::CHAR) => postgres::types::Type::CHAR,
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
        postgres::types::Type::CHAR => DataType::Int8,
        postgres::types::Type::INT2 => DataType::Int16,
        postgres::types::Type::INT4 => DataType::Int32,
        postgres::types::Type::INT8 => DataType::Int64,
        postgres::types::Type::TEXT => DataType::LargeUtf8,
        postgres::types::Type::VARCHAR => DataType::Utf8,
        postgres::types::Type::NAME => DataType::Utf8,
        postgres::types::Type::NAME_ARRAY => DataType::List(Box::new(Field::new(
            "unnamed_name_array",
            DataType::UInt8,
            true,
        ))),
        postgres::types::Type::OID => DataType::UInt16,
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
    // TODO: Fix this function

    return value.write_be_bytes(buffer);

    // let mut bytes = vec![];
    // value.write_be_bytes(&mut bytes).unwrap();

    // while bytes.last() == Some(&0) {
    //     bytes.pop();
    // }

    // buffer.write(&bytes)
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
                        values.value(row_index).write_be_bytes(&mut cell).unwrap();
                    }
                    DataType::Int16 => {
                        let values: &Int16Array = as_primitive_array(column);
                        write_be_without_trailing_zeros(values.value(row_index), &mut cell)
                            .unwrap();
                    }
                    DataType::Int32 => {
                        let values: &Int32Array = as_primitive_array(column);
                        write_be_without_trailing_zeros(values.value(row_index), &mut cell)
                            .unwrap();
                    }
                    DataType::Int64 => {
                        let values: &Int64Array = as_primitive_array(column);
                        write_be_without_trailing_zeros(values.value(row_index), &mut cell)
                            .unwrap();
                    }
                    DataType::UInt8 => {
                        let values: &UInt8Array = as_primitive_array(column);
                        write_be_without_trailing_zeros(values.value(row_index), &mut cell)
                            .unwrap();
                    }
                    DataType::UInt16 => {
                        let values: &UInt16Array = as_primitive_array(column);
                        write_be_without_trailing_zeros(values.value(row_index), &mut cell)
                            .unwrap();
                    }
                    DataType::UInt32 => {
                        let values: &UInt32Array = as_primitive_array(column);
                        write_be_without_trailing_zeros(values.value(row_index), &mut cell)
                            .unwrap();
                    }
                    DataType::UInt64 => {
                        let values: &UInt64Array = as_primitive_array(column);
                        write_be_without_trailing_zeros(values.value(row_index), &mut cell)
                            .unwrap();
                    }
                    DataType::LargeUtf8 => {
                        let values = &column
                            .as_any()
                            .downcast_ref::<GenericStringArray<i64>>()
                            .unwrap();
                        cell.extend_from_slice(values.value(row_index).as_bytes())
                    }
                    DataType::Utf8 => {
                        let values = &column
                            .as_any()
                            .downcast_ref::<GenericStringArray<i32>>()
                            .unwrap();
                        cell.extend_from_slice(values.value(row_index).as_bytes())
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
                    DataType::Boolean => {
                        let values = &column.as_any().downcast_ref::<BooleanArray>().unwrap();
                        let boolean_value = values.value(row_index);
                        let byte_value = if boolean_value { 1 } else { 0 };
                        cell.extend_from_slice(&[byte_value])
                    }
                    _ => todo!("{:?}", column.data_type()),
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
                name: "id".to_string(),
                table_oid: 16394,
                column_number: 1,
                type_oid: 23,
                type_length: 4,
                type_modifier: -1,
                format: 0,
            },
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
                Some(vec![0, 0, 0, 1]),
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
