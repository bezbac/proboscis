use anyhow::Result;
use arrow::array::Array;
use itertools::Itertools;
use polars::prelude::NewChunkedArray;
use polars::prelude::{ChunkCompare, DataFrame, NamedFrom, Series, UInt32Chunked};
use std::collections::{HashMap, HashSet, VecDeque};
use std::convert::TryFrom;
use std::ops::Deref;

use crate::column_transformations::ColumnTransformation;

fn get_span(series: &Series) -> Result<Option<i64>> {
    match series.dtype() {
        &polars::prelude::DataType::UInt8
        | &polars::prelude::DataType::UInt16
        | &polars::prelude::DataType::UInt32
        | &polars::prelude::DataType::UInt64
        | &polars::prelude::DataType::Int16
        | &polars::prelude::DataType::Int32
        | &polars::prelude::DataType::Int64 => Ok({
            let max = series.max::<i64>();
            let min = series.min::<i64>();

            match (max, min) {
                (None, _) | (_, None) => None,
                (Some(max), Some(min)) => Some({
                    let span = max - min;
                    match span {
                        0 => 1,
                        _ => span,
                    }
                }),
            }
        }),
        &polars::prelude::DataType::Utf8 => {
            Ok(series.arg_unique().map(|x| Some(x.len() as i64))?)
        }
        _ => todo!(),
    }
}

fn get_spans(series: &[&Series], partition: &[u32]) -> Result<Vec<Option<i64>>> {
    let mut spans = vec![];

    for series in series {
        let relevant_section = series.take(&UInt32Chunked::new_from_slice("idx", partition))?;
        let span = get_span(&relevant_section)?;
        spans.push(span);
    }

    Ok(spans)
}

fn scale_spans(spans: &[Option<i64>], scale: &[i64]) -> Vec<Option<i64>> {
    spans
        .iter()
        .zip(scale)
        .map(|(value, scale)| value.map(|v| v / scale))
        .collect()
}

fn split(df: &DataFrame, partition: &[u32], column_index: usize) -> Result<(Vec<u32>, Vec<u32>)> {
    let dfp = df[column_index].take(&UInt32Chunked::new_from_slice("idx", partition))?;

    match dfp.dtype() {
        &polars::prelude::DataType::UInt8
        | &polars::prelude::DataType::UInt16
        | &polars::prelude::DataType::UInt32
        | &polars::prelude::DataType::UInt64
        | &polars::prelude::DataType::Int16
        | &polars::prelude::DataType::Int32
        | &polars::prelude::DataType::Int64 => {
            let median = dfp.median().ok_or_else(|| anyhow::anyhow!("No median"))?;

            let mask = dfp.lt_eq(median);

            let mut dfl = vec![];
            let mut dfr = vec![];
            for (index, mask_val) in partition.iter().zip(&mask) {
                match mask_val.map_or(false, |v| v) {
                    true => dfl.push(*index),
                    false => dfr.push(*index),
                }
            }

            Ok((dfl, dfr))
        }
        &polars::prelude::DataType::Utf8 => {
            let values: Vec<Option<&str>> = dfp.utf8()?.into_iter().unique().collect();

            let lv: HashSet<&Option<&str>> = values[..values.len() / 2].iter().collect();
            let rv: HashSet<&Option<&str>> = values[values.len() / 2..].iter().collect();

            let left_indices = partition
                .iter()
                .zip(dfp.utf8()?)
                .filter(|(_, element)| lv.contains(element))
                .map(|(idx, _)| idx)
                .cloned()
                .collect();

            let right_indices = partition
                .iter()
                .zip(dfp.utf8()?)
                .filter(|(_, element)| rv.contains(element))
                .map(|(idx, _)| idx)
                .cloned()
                .collect();

            Ok((left_indices, right_indices))
        }
        _ => todo!(),
    }
}

pub fn partition_dataset(
    df: &DataFrame,
    quasi_identifiers: &[&str],
    is_valid: &dyn Fn(&DataFrame, &[u32]) -> Result<bool>,
) -> Result<Vec<Vec<u32>>> {
    let mut partitions: VecDeque<Vec<u32>> =
        vec![(0..df[0].len()).map(|i| i as u32).collect::<Vec<u32>>()].into();

    let mut relevant_dataframe = df.columns(quasi_identifiers)?;

    let overall_spans = get_spans(&relevant_dataframe, &partitions[0].clone())?;

    // Remove all empty columns from the relevant dataframe
    for (index, span) in overall_spans.iter().enumerate() {
        if span.is_none() {
            relevant_dataframe.remove(index);
        }
    }

    let relevant_quasi_identifiers: Vec<&str> = relevant_dataframe
        .iter()
        .map(|series| series.name())
        .collect();

    let overall_spans: Vec<i64> = overall_spans.iter().flatten().cloned().collect();

    let mut finished_partitions = vec![];
    while let Some(partition) = partitions.pop_front() {
        let spans = get_spans(&relevant_dataframe, &partition)?;
        let scaled_spans = &scale_spans(&spans, &overall_spans);

        let mut column_index_span_vec: Vec<(usize, Option<i64>)> = relevant_quasi_identifiers
            .iter()
            .enumerate()
            .zip(scaled_spans.iter())
            .map(|((column_index, _), span)| (column_index, *span))
            .collect();

        column_index_span_vec.sort_by_key(|(_, span)| *span);
        column_index_span_vec.reverse();

        let mut did_break = false;
        for (column_index, _) in column_index_span_vec {
            let column_name = relevant_quasi_identifiers[column_index];
            let df_column_index = df
                .get_column_names()
                .into_iter()
                .position(|name| name == column_name)
                .ok_or_else(|| anyhow::anyhow!("Column not found {:?}", column_name))?;

            let (lp, rp) = split(df, &partition, df_column_index)?;

            if !is_valid(df, &lp)? || !is_valid(df, &rp)? {
                continue;
            }

            partitions.push_back(lp);
            partitions.push_back(rp);

            did_break = true;
            break;
        }

        if !did_break {
            finished_partitions.push(partition);
        }
    }

    Ok(finished_partitions)
}

#[derive(Clone, Copy, Debug)]
pub enum NumericAggregation {
    Median,
    Range,
}

impl NumericAggregation {
    pub fn transformation(&self) -> Box<dyn ColumnTransformation> {
        match self {
            NumericAggregation::Median => Box::new(crate::column_transformations::AggMedian {}),
            NumericAggregation::Range => Box::new(crate::column_transformations::AggRange {}),
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub enum StringAggregation {
    Join,
    Substring,
}

impl StringAggregation {
    pub fn transformation(&self) -> Box<dyn ColumnTransformation> {
        match self {
            Self::Join => Box::new(crate::column_transformations::AggStringJoinUnique {}),
            Self::Substring => Box::new(crate::column_transformations::AggStringCommonPrefix {}),
        }
    }
}

fn apply_column_transformation_to_series(
    series: &Series,
    transformation: &dyn ColumnTransformation,
) -> Result<Series> {
    let array_refs: Vec<&dyn Array> = series.chunks().iter().map(|a| a.deref()).collect();
    let array = arrow::compute::kernels::concat::concat(&array_refs).unwrap();
    let transformed_data = transformation.transform_data(array)?;
    let updated = Series::try_from((series.name(), vec![transformed_data]))?;
    Ok(updated)
}

fn deidentify_column(series: &Series) -> Result<Series> {
    apply_column_transformation_to_series(series, &crate::column_transformations::Randomize {})
}

fn agg_column(
    series: &Series,
    numeric_aggregation: &NumericAggregation,
    string_aggregation: &StringAggregation,
) -> Result<Series> {
    match series.dtype() {
        polars::prelude::DataType::UInt8
        | polars::prelude::DataType::Int16
        | polars::prelude::DataType::Int32
        | polars::prelude::DataType::Int64 => apply_column_transformation_to_series(
            series,
            numeric_aggregation.transformation().as_ref(),
        ),
        polars::prelude::DataType::Utf8 => apply_column_transformation_to_series(
            series,
            string_aggregation.transformation().as_ref(),
        ),
        _ => todo!("{}", series.dtype()),
    }
}

pub fn is_k_anonymous(partition: &[u32], k: usize) -> bool {
    partition.len() >= k
}

pub fn is_l_diverse(
    df: &DataFrame,
    partition: &[u32],
    sensitive_column: &str,
    l: usize,
) -> Result<bool> {
    Ok(df
        .column(sensitive_column)?
        .take(&UInt32Chunked::new_from_slice("idx", partition))?
        .unique()?
        .len()
        >= l)
}

#[derive(Clone)]
pub enum AnonymizationCriteria {
    KAnonymous { k: usize },
    LDiverse { l: usize, sensitive_column: String },
}

impl AnonymizationCriteria {
    fn is_anonymous(&self, df: &DataFrame, partition: &[u32]) -> Result<bool> {
        match self {
            Self::KAnonymous { k } => Ok(is_k_anonymous(partition, *k)),
            Self::LDiverse {
                l,
                sensitive_column,
            } => is_l_diverse(df, partition, sensitive_column, *l),
        }
    }
}

pub fn anonymize(
    df: &DataFrame,
    identifiers: &[&str],
    quasi_identifiers: &HashMap<String, (NumericAggregation, StringAggregation)>,
    criteria: &[AnonymizationCriteria],
) -> Result<DataFrame> {
    let quasi_identifier_strs: Vec<&str> = quasi_identifiers.keys().map(|k| k.as_str()).collect();

    let partitions = partition_dataset(df, &quasi_identifier_strs, &|_, partition| {
        for criterium in criteria {
            if !criterium.is_anonymous(df, partition)? {
                return Ok(false);
            }
        }

        Ok(true)
    })?;

    let mut updated: Vec<Series> = vec![];

    for partition in &partitions {
        for (index, column) in df.get_columns().iter().enumerate() {
            let is_quasi_identifier = quasi_identifier_strs.contains(&column.name());
            let is_identifier = identifiers.contains(&column.name());

            let data = column.take(&UInt32Chunked::new_from_slice("idx", partition))?;

            let new_data: Series = if is_quasi_identifier {
                let (numeric_aggregation, string_aggregation) =
                    quasi_identifiers.get(column.name()).unwrap();
                agg_column(&data, numeric_aggregation, string_aggregation)?
            } else if is_identifier {
                deidentify_column(&data)?
            } else {
                data
            };

            let entry = updated.get_mut(index);

            match entry {
                Some(s) => updated[index] = s.append(&new_data)?.clone(),
                None => updated.push(new_data),
            };
        }
    }

    let original_indices_of_updated_rows: Vec<u32> = partitions.concat();

    // Reorder according to original indices
    let mut result = DataFrame::new(updated)?;
    result.insert_at_idx(
        0,
        Series::new("original_indices", original_indices_of_updated_rows),
    )?;
    result.sort_in_place("original_indices", false)?;
    result.drop_in_place("original_indices")?;

    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::conversion::record_batch_to_data_frame;
    use arrow::{
        array::{Int32Array, StringArray},
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    };
    use polars::prelude::AnyValue;
    use std::sync::Arc;

    #[test]
    fn k_anonymization() {
        let id_array = Int32Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8, 9]);
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
        let empty_array =
            Int32Array::from(vec![None, None, None, None, None, None, None, None, None]);
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
            Field::new("id", DataType::Int32, false),
            Field::new("first_name", DataType::Utf8, false),
            Field::new("last_name", DataType::Utf8, false),
            Field::new("age", DataType::Int32, false),
            Field::new("empty", DataType::Int32, false),
            Field::new("profession", DataType::Utf8, false),
            Field::new("pay", DataType::Int32, false),
        ]);

        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(id_array),
                Arc::new(first_name_array),
                Arc::new(last_name_array),
                Arc::new(age_array),
                Arc::new(empty_array),
                Arc::new(profession_array),
                Arc::new(pay_array),
            ],
        )
        .unwrap();

        let df = record_batch_to_data_frame(&batch);
        println!("{:?}", df.head(Some(10)));

        let identifiers = vec!["first_name", "last_name"];
        let quasi_identifiers = vec![
            (
                "age".to_string(),
                (NumericAggregation::Range, StringAggregation::Join),
            ),
            (
                "profession".to_string(),
                (NumericAggregation::Range, StringAggregation::Join),
            ),
            (
                "empty".to_string(),
                (NumericAggregation::Range, StringAggregation::Join),
            ),
        ]
        .iter()
        .cloned()
        .collect();

        let anonymized = anonymize(
            &df,
            &identifiers,
            &quasi_identifiers,
            &[AnonymizationCriteria::KAnonymous { k: 2 }],
        )
        .unwrap();

        assert_eq!(AnyValue::Int32(1), anonymized.column("id").unwrap().get(0));
        assert_eq!(AnyValue::Int32(2), anonymized.column("id").unwrap().get(1));
        assert_eq!(AnyValue::Int32(3), anonymized.column("id").unwrap().get(2));
        assert_eq!(AnyValue::Int32(4), anonymized.column("id").unwrap().get(3));
        assert_eq!(AnyValue::Int32(5), anonymized.column("id").unwrap().get(4));
        assert_eq!(AnyValue::Int32(6), anonymized.column("id").unwrap().get(5));
        assert_eq!(AnyValue::Int32(7), anonymized.column("id").unwrap().get(6));
        assert_eq!(AnyValue::Int32(8), anonymized.column("id").unwrap().get(7));
        assert_eq!(AnyValue::Int32(9), anonymized.column("id").unwrap().get(8));
    }
}