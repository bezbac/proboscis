use arrow::{
    array::{Array, Int32Array, StringArray, UInt16Array, UInt8Array},
    datatypes::DataType,
    record_batch::RecordBatch,
};
use polars::prelude::{DataFrame, UInt32Chunked};
use polars::prelude::{NewChunkedArray, Series};
use std::{collections::HashSet, iter::FromIterator, ops::Range, sync::Arc};

fn get_span(series: &Series) -> i64 {
    match series.dtype() {
        &polars::prelude::DataType::UInt8
        | &polars::prelude::DataType::UInt16
        | &polars::prelude::DataType::Int32 => {
            (series.arg_max().unwrap() - series.arg_min().unwrap()) as i64
        }
        &polars::prelude::DataType::Utf8 => series.arg_unique().unwrap().len() as i64,
        _ => todo!(),
    }
}

fn get_spans(df: &DataFrame, partition: &[u32]) -> Vec<i64> {
    let relevant_section = df
        .take(&UInt32Chunked::new_from_slice("idx", partition))
        .unwrap();

    relevant_section
        .columns(df.fields().iter().map(|f| f.name()))
        .unwrap()
        .iter()
        .map(|column| get_span(column.clone()))
        .collect()
}

fn record_batch_to_data_frame(data: &RecordBatch) -> DataFrame {
    todo!();
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::{
        array::{Int32Array, StringArray},
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    };
    use std::{convert::TryFrom, sync::Arc};

    #[test]
    fn it_works() {
        let first_name_array = StringArray::from(vec![
            "Max", "Lukas", "Alex", "Alex", "Lukas", "Mia", "Noa", "Noah", "Mia",
        ]);
        let last_name_array = StringArray::from(vec![
            "Müller",
            "Müller",
            "Schidt",
            "Schidt",
            "Gierlind",
            "Ludwigs",
            "Petry",
            "Schindler",
            "Müller",
        ]);
        let age_array = Int32Array::from(vec![18, 40, 46, 22, 22, 26, 32, 17, 29]);
        let profession_array = StringArray::from(vec![
            "Sales",
            "Sales",
            "Engineering",
            "Engineering",
            "Engineering",
            "Marketing",
            "Marketing",
            "Sales",
            "Engineering",
        ]);
        let pay_array = Int32Array::from(vec![54, 54, 120, 70, 32, 42, 56, 140, 38]);

        let schema = Schema::new(vec![
            Field::new("first_name", DataType::Utf8, false),
            Field::new("last_name", DataType::Utf8, false),
            Field::new("age", DataType::Int32, false),
            Field::new("profession", DataType::Utf8, false),
            Field::new("pay", DataType::Int32, false),
        ]);

        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(first_name_array),
                Arc::new(last_name_array),
                Arc::new(age_array),
                Arc::new(profession_array),
                Arc::new(pay_array),
            ],
        )
        .unwrap();

        let df = record_batch_to_data_frame(&batch);

        let partition = (0..batch.num_rows())
            .map(|i| i as u32)
            .collect::<Vec<u32>>();

        let spans = get_spans(&df, &partition);

        println!("{:?}", spans);

        // let quasi_identifiers = vec!["first_name", "last_name", "age", "profession"];
    }
}
