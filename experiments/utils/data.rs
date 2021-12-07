use polars::frame::DataFrame;
use polars::prelude::NamedFrom;
use polars::prelude::Series;
use postgres::types::Type;
use postgres::{Client, NoTls};

pub fn query_data_into_dataframe(
    postgres_connection_string: &str,
    query: &str,
) -> polars::frame::DataFrame {
    let mut client = Client::connect(&postgres_connection_string, NoTls).unwrap();
    let rows = client.query(query, &[]).unwrap();

    let series = rows
        .first()
        .unwrap()
        .columns()
        .iter()
        .enumerate()
        .map(|(i, column)| -> Series {
            match column.type_() {
                &Type::BOOL => Series::new(
                    column.name(),
                    rows.iter()
                        .map(|row| row.try_get(i).ok())
                        .collect::<Vec<Option<bool>>>(),
                ),
                &Type::INT2 => Series::new(
                    column.name(),
                    rows.iter()
                        .map(|row| row.try_get(i).ok())
                        .collect::<Vec<Option<i16>>>(),
                ),
                &Type::INT4 => Series::new(
                    column.name(),
                    rows.iter()
                        .map(|row| row.try_get(i).ok())
                        .collect::<Vec<Option<i32>>>(),
                ),
                &Type::OID => Series::new(
                    column.name(),
                    rows.iter()
                        .map(|row| row.try_get(i).ok())
                        .collect::<Vec<Option<u32>>>(),
                ),
                &Type::CHAR => Series::new(
                    column.name(),
                    rows.iter()
                        .map(|row| row.try_get(i).ok())
                        .collect::<Vec<Option<i8>>>(),
                ),
                &Type::TEXT => Series::new(
                    column.name(),
                    rows.iter()
                        .map(|row| row.try_get(i).ok())
                        .collect::<Vec<Option<&str>>>(),
                ),
                _ => unimplemented!("{:?}", column.type_()),
            }
        })
        .collect();

    DataFrame::new(series).unwrap()
}
