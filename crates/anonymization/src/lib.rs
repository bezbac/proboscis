use arrow::{
    array::{Int32Array, StringArray},
    record_batch::RecordBatch,
};
use polars::prelude::{DataFrame, UInt32Chunked};
use polars::prelude::{NewChunkedArray, Series};
use std::collections::{HashSet, VecDeque};

fn get_span(series: &Series) -> i64 {
    match series.dtype() {
        &polars::prelude::DataType::UInt8 => {
            let max = series.max::<u8>().unwrap() - series.min::<u8>().unwrap();
            max as i64
        }
        &polars::prelude::DataType::Int32 => {
            let max = series.max::<i32>().unwrap() - series.min::<i32>().unwrap();
            max as i64
        }
        &polars::prelude::DataType::Utf8 => series.arg_unique().unwrap().len() as i64,
        _ => todo!(),
    }
}

fn get_spans(series: &[&Series], partition: &[u32]) -> Vec<i64> {
    series
        .iter()
        .map(|series| {
            let relevant_section = series
                .take(&UInt32Chunked::new_from_slice("idx", partition))
                .unwrap();
            get_span(&relevant_section)
        })
        .collect()
}

pub fn is_k_anonymous(partition: &[u32], k: usize) -> bool {
    if partition.len() < k {
        return false;
    }
    return true;
}

fn split(df: &DataFrame, partition: &[u32], column_index: usize) -> (Vec<u32>, Vec<u32>) {
    let dfp = df[column_index]
        .take(&UInt32Chunked::new_from_slice("idx", partition))
        .unwrap();

    println!("Series: {:?}", dfp);

    match dfp.dtype() {
        &polars::prelude::DataType::Int32 => {
            let median = dfp.median().unwrap();

            let dfl: Vec<u32> = partition
                .iter()
                .zip(dfp.i32().unwrap())
                .filter(|(_, value)| match value {
                    Some(v) => (*v as f64) < median,
                    None => true,
                })
                .map(|(index, _)| *index)
                .collect();

            let dfr: Vec<u32> = partition
                .iter()
                .zip(dfp.i32().unwrap())
                .filter(|(_, value)| match value {
                    Some(v) => (*v as f64) >= median,
                    None => false,
                })
                .map(|(index, _)| *index)
                .collect();

            (dfl, dfr)
        }
        &polars::prelude::DataType::Utf8 => {
            let unique = dfp.unique().unwrap();
            let values: Vec<Option<&str>> = unique.utf8().unwrap().into_iter().collect();

            let lv: HashSet<&Option<&str>> = values[..values.len() / 2].iter().collect();
            let rv: HashSet<&Option<&str>> = values[values.len() / 2..].iter().collect();

            let left_indices = partition
                .iter()
                .zip(dfp.utf8().unwrap())
                .filter(|(_, element)| lv.contains(element))
                .map(|(idx, _)| idx)
                .cloned()
                .collect();

            let right_indices = partition
                .iter()
                .zip(dfp.utf8().unwrap())
                .filter(|(_, element)| rv.contains(element))
                .map(|(idx, _)| idx)
                .cloned()
                .collect();

            (left_indices, right_indices)
        }
        _ => todo!(),
    }
}

pub fn partition_dataset(
    df: &DataFrame,
    feature_columns: &[&str],
    is_valid: &dyn Fn(&DataFrame, &[u32]) -> bool,
) -> Vec<Vec<u32>> {
    let mut partitions: VecDeque<Vec<u32>> =
        vec![(0..df[0].len()).map(|i| i as u32).collect::<Vec<u32>>()].into();

    let relevant_dataframe = df.columns(feature_columns).unwrap();

    let mut finished_partitions = vec![];
    while let Some(partition) = partitions.pop_front() {
        println!("PARTITION");

        println!("remaining_partitions: {:?}", partitions);

        let spans = get_spans(&relevant_dataframe, &partition);

        println!("Spans: {:?}", spans);

        let mut column_index_span_vec: Vec<(usize, i64)> = feature_columns
            .iter()
            .enumerate()
            .zip(spans)
            .map(|((column_index, _), span)| (column_index, span))
            .collect();

        column_index_span_vec.sort_by_key(|(_, span)| -*span);

        for (column_index, _) in column_index_span_vec {
            println!("Column: {:?}", column_index);

            let (lp, rp) = split(df, &partition, column_index);

            println!("{:?} {:?}", lp, rp);

            if !is_valid(df, &lp) || !is_valid(df, &rp) {
                continue;
            }

            partitions.push_back(lp);
            partitions.push_back(rp);
        }

        finished_partitions.push(partition);
    }

    return finished_partitions;
}

pub fn record_batch_to_data_frame(data: &RecordBatch) -> DataFrame {
    use polars::prelude::*;

    let series: Vec<Series> = data
        .columns()
        .iter()
        .zip(data.schema().fields())
        .map(|(column, field)| match column.data_type() {
            arrow::datatypes::DataType::Int32 => Series::new(
                field.name(),
                column
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .iter()
                    .collect::<Vec<Option<i32>>>(),
            ),
            arrow::datatypes::DataType::Utf8 => Series::new(
                field.name(),
                column
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .iter()
                    .map(|val| val.map(|v| v.to_string()))
                    .collect::<Vec<Option<String>>>(),
            ),
            _ => todo!(),
        })
        .collect();

    DataFrame::new(series).unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::{
        array::{Int32Array, StringArray},
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    };
    use std::sync::Arc;

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

        let relevant_series: Vec<&Series> = df[..].into_iter().map(|f| f).collect();
        let spans = get_spans(&relevant_series, &partition);

        println!("Spans: {:?}", spans);
        println!("---");
        println!();

        let (lp, rp) = split(&df, &partition, 0);
        println!("Col 0 lp: {:?}", lp);
        println!("Col 0 rp: {:?}", rp);
        println!("---");
        println!();

        let (lp, rp) = split(&df, &partition, 2);
        println!("Col 2 lp: {:?}", lp);
        println!("Col 2 rp: {:?}", rp);
        println!("---");
        println!();

        let quasi_identifiers = vec!["first_name", "last_name", "age", "profession"];
        let finished_partitions = partition_dataset(&df, &quasi_identifiers, &|_, partition| {
            is_k_anonymous(partition, 3)
        });

        println!("Final partitions: {:?}", finished_partitions);
    }
}
