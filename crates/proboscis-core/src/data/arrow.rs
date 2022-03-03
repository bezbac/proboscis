use arrow::array::{
    as_primitive_array, make_array, ArrayData, BooleanArray, FixedSizeBinaryArray, Float32Array,
    Float64Array, ListArray, UInt16Array, UInt32Array, UInt64Array,
};
use arrow::array::{Array, GenericListArray, UInt8Array};
use arrow::array::{ArrayRef, GenericStringArray, Int16Array, Int32Array, Int64Array, Int8Array};
use arrow::buffer::{Buffer, MutableBuffer};
use arrow::datatypes::{DataType, Schema, ToByteSlice, UInt8Type};
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use byteorder::{BigEndian, ByteOrder, ReadBytesExt, WriteBytesExt};
use proboscis_postgres_protocol::message::{DataRow, RowDescription};
use std::convert::TryFrom;
use std::{sync::Arc, vec};

macro_rules! create_numerical_column_data_to_array_function {
    ($func_name:ident, $Array:ident, $type:ty, $byte_count:literal, $read_closure:tt) => {
        #[allow(clippy::redundant_closure_call)]
        fn $func_name(data: &[Option<Vec<u8>>]) -> std::io::Result<ArrayRef> {
            let mut new_data = vec![];

            for d in data {
                let new_d = match d.as_ref() {
                    Some(d) => {
                        let mut bytes = d.clone();
                        while bytes.len() < $byte_count {
                            bytes.push(0);
                        }

                        Some($read_closure(&bytes)?)
                    }
                    _ => None,
                };

                new_data.push(new_d);
            }

            Ok(Arc::new(new_data.iter().collect::<$Array>()))
        }
    };
}

create_numerical_column_data_to_array_function!(
    column_data_to_array_f32,
    Float32Array,
    f32,
    4,
    (|buffer: &[u8]| -> std::io::Result<f32> { Ok(BigEndian::read_f32(buffer)) })
);

create_numerical_column_data_to_array_function!(
    column_data_to_array_f64,
    Float64Array,
    f64,
    8,
    (|buffer: &[u8]| -> std::io::Result<f64> { Ok(BigEndian::read_f64(buffer)) })
);

create_numerical_column_data_to_array_function!(
    column_data_to_array_i8,
    Int8Array,
    i8,
    1,
    (|mut buffer: &[u8]| -> std::io::Result<i8> { buffer.read_i8() })
);

create_numerical_column_data_to_array_function!(
    column_data_to_array_i16,
    Int16Array,
    i16,
    2,
    (|mut buffer: &[u8]| -> std::io::Result<i16> { buffer.read_i16::<BigEndian>() })
);
create_numerical_column_data_to_array_function!(
    column_data_to_array_i32,
    Int32Array,
    i32,
    4,
    (|mut buffer: &[u8]| -> std::io::Result<i32> { buffer.read_i32::<BigEndian>() })
);
create_numerical_column_data_to_array_function!(
    column_data_to_array_i64,
    Int64Array,
    i64,
    8,
    (|mut buffer: &[u8]| -> std::io::Result<i64> { buffer.read_i64::<BigEndian>() })
);

create_numerical_column_data_to_array_function!(
    column_data_to_array_u8,
    UInt8Array,
    u8,
    1,
    (|mut buffer: &[u8]| -> std::io::Result<u8> { buffer.read_u8() })
);
create_numerical_column_data_to_array_function!(
    column_data_to_array_u16,
    UInt16Array,
    u16,
    2,
    (|mut buffer: &[u8]| -> std::io::Result<u16> { buffer.read_u16::<BigEndian>() })
);
create_numerical_column_data_to_array_function!(
    column_data_to_array_u32,
    UInt32Array,
    u32,
    4,
    (|mut buffer: &[u8]| -> std::io::Result<u32> { buffer.read_u32::<BigEndian>() })
);
create_numerical_column_data_to_array_function!(
    column_data_to_array_u64,
    UInt64Array,
    u64,
    8,
    (|mut buffer: &[u8]| -> std::io::Result<u64> { buffer.read_u64::<BigEndian>() })
);

fn column_data_to_array(
    data: &[Option<Vec<u8>>],
    data_type: &DataType,
) -> std::io::Result<ArrayRef> {
    match data_type {
        DataType::Int8 => column_data_to_array_i8(data),
        DataType::Int16 => column_data_to_array_i16(data),
        DataType::Int32 => column_data_to_array_i32(data),
        DataType::Int64 => column_data_to_array_i64(data),

        DataType::UInt8 => column_data_to_array_u8(data),
        DataType::UInt16 => column_data_to_array_u16(data),
        DataType::UInt32 => column_data_to_array_u32(data),
        DataType::UInt64 => column_data_to_array_u64(data),

        DataType::Float32 => column_data_to_array_f32(data),
        DataType::Float64 => column_data_to_array_f64(data),

        DataType::Utf8 => Ok(Arc::new(
            data.iter()
                .map(|d| d.as_ref().map(|d| String::from_utf8(d.to_vec()).unwrap()))
                .collect::<GenericStringArray<i32>>(),
        )),
        DataType::LargeUtf8 => Ok(Arc::new(
            data.iter()
                .map(|d| d.as_ref().map(|d| String::from_utf8(d.to_vec()).unwrap()))
                .collect::<GenericStringArray<i64>>(),
        )),
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

            Ok(make_array(list_data))
        }
        DataType::Boolean => Ok(Arc::new(BooleanArray::from(
            data.iter()
                .map(|d| d.as_ref().map(|d| d == &[0]))
                .collect::<Vec<Option<bool>>>(),
        ))),
        DataType::FixedSizeBinary(size) => {
            if data.is_empty() {
                let array_data = ArrayData::new(
                    DataType::FixedSizeBinary(*size as i32),
                    0,
                    None,
                    Some(MutableBuffer::from_len_zeroed(0).into()),
                    0,
                    vec![MutableBuffer::from_len_zeroed(0).into()],
                    vec![],
                );

                return Ok(Arc::new(FixedSizeBinaryArray::from(array_data)));
            }

            return Ok(Arc::new(
                FixedSizeBinaryArray::try_from_sparse_iter(data.iter().map(|d| {
                    d.as_ref().map(|value| {
                        let mut new_value = value.clone();
                        new_value.extend(vec![0; 64 - value.len()]);
                        new_value
                    })
                }))
                .unwrap(),
            ));
        }
        _ => todo!("{}", data_type),
    }
}

fn protocol_rows_to_arrow_columns(
    schema: &Schema,
    rows: Vec<Vec<Option<Vec<u8>>>>,
) -> std::io::Result<Vec<ArrayRef>> {
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

    let mut result = vec![];
    for (column_data, data_type) in columns_data.iter().zip(columns_data_types.iter()) {
        result.push(column_data_to_array(column_data, data_type)?)
    }

    Ok(result)
}

pub fn protocol_fields_to_schema(
    fields: &[proboscis_postgres_protocol::message::Field],
) -> Result<Schema, &str> {
    let mut result = vec![];

    for field in fields {
        let proboscis_field = crate::data::field::Field::try_from(field)?;
        result.push(arrow::datatypes::Field::try_from(&proboscis_field).unwrap())
    }

    Ok(Schema::new(result))
}

pub fn simple_query_response_to_record_batch(
    fields: &[proboscis_postgres_protocol::message::Field],
    data: &[DataRow],
) -> Result<RecordBatch, ArrowError> {
    let schema =
        protocol_fields_to_schema(fields).map_err(|err| ArrowError::CastError(err.to_string()))?;

    let protocol_row_data = data
        .iter()
        .map(|DataRow { field_data }| field_data.clone())
        .collect();

    let columns = protocol_rows_to_arrow_columns(&schema, protocol_row_data)?;

    RecordBatch::try_new(Arc::new(schema), columns)
}

pub fn serialize_record_batch_to_data_rows(batch: &RecordBatch) -> std::io::Result<Vec<DataRow>> {
    let mut result = vec![];

    for row_index in 0..batch.num_rows() {
        let mut row_data = vec![];

        for column in batch.columns() {
            let mut cell: Vec<u8> = vec![];
            match column.data_type() {
                DataType::Int8 => {
                    let values: &Int8Array = as_primitive_array(column);
                    cell.write_i8(values.value(row_index))?
                }
                DataType::Int16 => {
                    let values: &Int16Array = as_primitive_array(column);
                    cell.write_i16::<BigEndian>(values.value(row_index))?
                }
                DataType::Int32 => {
                    let values: &Int32Array = as_primitive_array(column);
                    cell.write_i32::<BigEndian>(values.value(row_index))?
                }
                DataType::Int64 => {
                    let values: &Int64Array = as_primitive_array(column);
                    cell.write_i64::<BigEndian>(values.value(row_index))?
                }
                DataType::UInt8 => {
                    let values: &UInt8Array = as_primitive_array(column);
                    cell.write_u8(values.value(row_index))?
                }
                DataType::UInt16 => {
                    let values: &UInt16Array = as_primitive_array(column);
                    cell.write_u16::<BigEndian>(values.value(row_index))?
                }
                DataType::UInt32 => {
                    let values: &UInt32Array = as_primitive_array(column);
                    cell.write_u32::<BigEndian>(values.value(row_index))?
                }
                DataType::UInt64 => {
                    let values: &UInt64Array = as_primitive_array(column);
                    cell.write_u64::<BigEndian>(values.value(row_index))?
                }
                DataType::Float32 => {
                    let values: &Float32Array = as_primitive_array(column);
                    cell.write_f32::<BigEndian>(values.value(row_index))?
                }
                DataType::Float64 => {
                    let values: &Float64Array = as_primitive_array(column);
                    cell.write_f64::<BigEndian>(values.value(row_index))?
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
                DataType::FixedSizeBinary(_) => {
                    let values = &column
                        .as_any()
                        .downcast_ref::<FixedSizeBinaryArray>()
                        .unwrap();

                    let mut row_value = values.value(row_index).to_vec();
                    row_value.reverse();
                    let end = row_value.iter_mut().take_while(|p| **p == 0).count();
                    row_value.reverse();

                    cell.extend_from_slice(&row_value[0..row_value.len() - end])
                }
                _ => todo!("{:?}", column.data_type()),
            }
            row_data.push(Some(cell))
        }

        result.push(DataRow {
            field_data: row_data,
        })
    }

    Ok(result)
}

pub fn serialize_record_batch_schema_to_row_description(schema: &Schema) -> RowDescription {
    let fields: Vec<proboscis_postgres_protocol::message::Field> = schema
        .fields()
        .iter()
        .map(|field| {
            let proboscis_field = crate::data::field::Field::try_from(field).unwrap();
            proboscis_postgres_protocol::message::Field::try_from(&proboscis_field).unwrap()
        })
        .collect();
    RowDescription { fields }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::Field;
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

        let deserialized = serialize_record_batch_to_data_rows(&batch).unwrap();

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
            proboscis_postgres_protocol::message::Field {
                name: "id".to_string(),
                table_oid: 16394,
                column_number: 1,
                type_oid: 23,
                type_length: 4,
                type_modifier: -1,
                format: 0,
            },
            proboscis_postgres_protocol::message::Field {
                name: "a".to_string(),
                table_oid: 0,
                column_number: 0,
                type_oid: 19,
                type_length: 64,
                type_modifier: -1,
                format: 0,
            },
            proboscis_postgres_protocol::message::Field {
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

        let deserialized_data = serialize_record_batch_to_data_rows(&batch).unwrap();

        assert_eq!(fields, deserialized_row_description.fields);
        assert_eq!(data, deserialized_data);
    }
}
