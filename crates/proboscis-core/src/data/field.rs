use arrow::datatypes::DataType;
use std::{collections::BTreeMap, convert::TryFrom};

fn postgres_type_for_arrow_type(arrow_type: &DataType) -> postgres::types::Type {
    match arrow_type {
        DataType::Boolean => postgres::types::Type::BOOL,
        DataType::Int8 => postgres::types::Type::CHAR,
        DataType::Int16 => postgres::types::Type::INT2,
        DataType::Int32 => postgres::types::Type::INT4,
        DataType::Int64 => postgres::types::Type::INT8,
        DataType::UInt16 => postgres::types::Type::OID,
        DataType::LargeUtf8 => postgres::types::Type::TEXT,
        DataType::Utf8 => postgres::types::Type::VARCHAR,
        DataType::FixedSizeBinary(size) => match size {
            64 => postgres::types::Type::NAME,
            _ => todo!("{}", arrow_type),
        },
        DataType::List(field) => match field.name().as_str() {
            "unnamed_name_array" => postgres::types::Type::NAME_ARRAY,
            _ => todo!("{}", arrow_type),
        },
        _ => todo!("{}", arrow_type),
    }
}

fn arrow_type_for_postgres_type(postgres_type: &postgres::types::Type) -> DataType {
    match *postgres_type {
        postgres::types::Type::BOOL => DataType::Boolean,
        postgres::types::Type::CHAR => DataType::Int8,
        postgres::types::Type::INT2 => DataType::Int16,
        postgres::types::Type::INT4 => DataType::Int32,
        postgres::types::Type::INT8 => DataType::Int64,
        postgres::types::Type::TEXT => DataType::LargeUtf8,
        postgres::types::Type::VARCHAR => DataType::Utf8,
        postgres::types::Type::NAME => DataType::FixedSizeBinary(64),
        postgres::types::Type::NAME_ARRAY => DataType::List(Box::new(
            arrow::datatypes::Field::new("unnamed_name_array", DataType::UInt8, true),
        )),
        postgres::types::Type::OID => DataType::UInt16,
        _ => todo!("{}", postgres_type),
    }
}

fn typelen_for_postgres_type(postgres_type: &postgres::types::Type) -> i16 {
    match *postgres_type {
        postgres::types::Type::BOOL => -1,
        postgres::types::Type::CHAR => 1,
        postgres::types::Type::INT2 => 2,
        postgres::types::Type::INT4 => 4,
        postgres::types::Type::INT8 => 8,
        postgres::types::Type::TEXT => -1,
        postgres::types::Type::VARCHAR => -1,
        postgres::types::Type::NAME => 64,
        postgres::types::Type::NAME_ARRAY => -1,
        postgres::types::Type::OID => 2,
        _ => todo!("{}", postgres_type),
    }
}

fn format_for_postgres_type(_postgres_type: &postgres::types::Type) -> i16 {
    0
}

pub struct Field {
    pub name: String,
    pub table_oid: i32,
    pub column_number: i16,
    pub data_type: DataType,
}

impl TryFrom<&Field> for proboscis_postgres_protocol::message::Field {
    type Error = &'static str;

    fn try_from(value: &Field) -> Result<Self, Self::Error> {
        let postgres_type = postgres_type_for_arrow_type(&value.data_type);
        let type_length = typelen_for_postgres_type(&postgres_type);
        let format = format_for_postgres_type(&postgres_type);

        Ok(proboscis_postgres_protocol::message::Field {
            name: value.name.clone(),
            table_oid: value.table_oid,
            column_number: value.column_number,
            type_oid: postgres_type.oid(),
            type_length,
            type_modifier: -1,
            format,
        })
    }
}

impl TryFrom<&proboscis_postgres_protocol::message::Field> for Field {
    type Error = &'static str;

    fn try_from(value: &proboscis_postgres_protocol::message::Field) -> Result<Self, Self::Error> {
        let postgres_type = postgres::types::Type::from_oid(value.type_oid)
            .ok_or("couldn't match oid with type")?;
        let data_type = arrow_type_for_postgres_type(&postgres_type);

        Ok(Field {
            name: value.name.clone(),
            data_type,
            table_oid: value.table_oid,
            column_number: value.column_number,
        })
    }
}

impl From<&Field> for arrow::datatypes::Field {
    fn from(value: &Field) -> Self {
        let mut metadata = BTreeMap::new();
        metadata.insert("table_oid".to_string(), value.table_oid.to_string());
        metadata.insert("column_number".to_string(), value.column_number.to_string());

        let mut field = arrow::datatypes::Field::new(&value.name, value.data_type.clone(), false);
        field.set_metadata(Some(metadata));

        field
    }
}

impl TryFrom<&arrow::datatypes::Field> for Field {
    type Error = &'static str;

    fn try_from(value: &arrow::datatypes::Field) -> Result<Self, Self::Error> {
        let metadata = value.metadata().clone().ok_or("Metadata missing")?;

        let table_oid = metadata
            .get("table_oid")
            .ok_or("Missing table_oid")?
            .parse()
            .map_err(|_| "parse error")?;
        let column_number = metadata
            .get("column_number")
            .ok_or("Missing column_number")?
            .parse()
            .map_err(|_| "parse error")?;

        Ok(Field {
            data_type: value.data_type().clone(),
            name: value.name().clone(),
            table_oid,
            column_number,
        })
    }
}
