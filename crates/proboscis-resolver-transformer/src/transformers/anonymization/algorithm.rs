use anyhow::Result;
use arrow::datatypes::DataType;
use itertools::Itertools;
use polars::prelude::NewChunkedArray;
use polars::prelude::{ChunkAgg, ChunkCompare, DataFrame, NamedFrom, Series, UInt32Chunked};
use rand::{distributions::Alphanumeric, thread_rng, Rng};
use std::collections::{HashMap, HashSet, VecDeque};

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
    pub fn output_type(&self, input: &DataType) -> DataType {
        match self {
            NumericAggregation::Median => input.clone(),
            NumericAggregation::Range => DataType::Utf8,
        }
    }
}

fn deidentify_column(series: &Series) -> Result<Series> {
    match series.dtype() {
        polars::prelude::DataType::Utf8 => {
            let unique_strings: Vec<Option<String>> = series
                .utf8()?
                .into_iter()
                .map(|v| {
                    v.map(|_| {
                        thread_rng()
                            .sample_iter(&Alphanumeric)
                            .take(30)
                            .map(char::from)
                            .collect()
                    })
                })
                .collect();

            Ok(Series::new(series.name(), unique_strings))
        }
        _ => todo!("{:?}", series.dtype()),
    }
}

fn agg_column(series: &Series, numeric_aggregation: &NumericAggregation) -> Result<Series> {
    match series.dtype() {
        polars::prelude::DataType::UInt8 => match numeric_aggregation {
            NumericAggregation::Median => Ok(Series::new(
                series.name(),
                vec![series.mean(); series.u8()?.len()],
            )),
            NumericAggregation::Range => {
                let min = series.u8()?.min();
                let max = series.u8()?.max();
                let agg = if max == min {
                    max.map(|v| format!("{}", v))
                } else {
                    Some(format!(
                        "{} - {}",
                        min.map_or("null".to_string(), |f| format!("{}", f)),
                        max.map_or("null".to_string(), |f| format!("{}", f))
                    ))
                };
                Ok(Series::new(series.name(), vec![agg; series.u8()?.len()]))
            }
        },
        polars::prelude::DataType::Int16 => match numeric_aggregation {
            NumericAggregation::Median => Ok(Series::new(
                series.name(),
                vec![series.mean(); series.i16()?.len()],
            )),
            NumericAggregation::Range => {
                let min = series.i16()?.min();
                let max = series.i16()?.max();

                let agg = if max == min {
                    max.map(|v| format!("{}", v))
                } else {
                    Some(format!(
                        "{} - {}",
                        min.map_or("null".to_string(), |f| format!("{}", f)),
                        max.map_or("null".to_string(), |f| format!("{}", f))
                    ))
                };

                Ok(Series::new(series.name(), vec![agg; series.i16()?.len()]))
            }
        },
        polars::prelude::DataType::Int32 => match numeric_aggregation {
            NumericAggregation::Median => Ok(Series::new(
                series.name(),
                vec![series.mean(); series.i32()?.len()],
            )),
            NumericAggregation::Range => {
                let min = series.i32()?.min();
                let max = series.i32()?.max();

                let agg = if max == min {
                    max.map(|v| format!("{}", v))
                } else {
                    Some(format!(
                        "{} - {}",
                        min.map_or("null".to_string(), |f| format!("{}", f)),
                        max.map_or("null".to_string(), |f| format!("{}", f))
                    ))
                };

                Ok(Series::new(series.name(), vec![agg; series.i32()?.len()]))
            }
        },
        polars::prelude::DataType::Int64 => match numeric_aggregation {
            NumericAggregation::Median => Ok(Series::new(
                series.name(),
                vec![series.mean(); series.i64()?.len()],
            )),
            NumericAggregation::Range => {
                let min = series.i64()?.min();
                let max = series.i64()?.max();

                let agg = if max == min {
                    max.map(|v| format!("{}", v))
                } else {
                    Some(format!(
                        "{} - {}",
                        min.map_or("null".to_string(), |f| format!("{}", f)),
                        max.map_or("null".to_string(), |f| format!("{}", f))
                    ))
                };

                Ok(Series::new(series.name(), vec![agg; series.i64()?.len()]))
            }
        },
        polars::prelude::DataType::Utf8 => {
            let unique_strings: Vec<&str> = series
                .utf8()?
                .into_iter()
                .unique()
                .map(|v| v.map_or("None", |v| v))
                .collect();

            let new_string = unique_strings.join(", ");

            Ok(Series::new(
                series.name(),
                vec![new_string; series.utf8()?.len()],
            ))
        }
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
    quasi_identifiers: &HashMap<String, Option<NumericAggregation>>,
    criteria: &[AnonymizationCriteria],
    numeric_aggregation: &NumericAggregation,
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
                agg_column(&data, numeric_aggregation)?
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
    result.sort("original_indices", false)?;

    result.drop_in_place("original_indices")?;

    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transformers::anonymization::conversion::record_batch_to_data_frame;
    use arrow::{
        array::{Int32Array, StringArray},
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    };
    use std::sync::Arc;

    #[test]
    fn string_aggregation() {
        let series = Series::new("last_name", vec!["Müller", "Müller", "Schidt"]);

        let aggregated = agg_column(&series, &NumericAggregation::Median).unwrap();

        assert_eq!(
            aggregated
                .utf8()
                .unwrap()
                .into_iter()
                .collect::<Vec<Option<&str>>>(),
            vec![
                Some("Müller, Schidt"),
                Some("Müller, Schidt"),
                Some("Müller, Schidt"),
            ]
        );
    }

    #[test]
    fn number_agg_median() {
        let series = Series::new("last_name", vec![10 as i32, 10 as i32, 10 as i32]);

        let aggregated = agg_column(&series, &NumericAggregation::Median).unwrap();

        assert_eq!(
            aggregated
                .f64()
                .unwrap()
                .into_iter()
                .collect::<Vec<Option<f64>>>(),
            vec![Some(10.0), Some(10.0), Some(10.0)]
        );
    }

    #[test]
    fn number_agg_range_equal() {
        let series = Series::new("last_name", vec![10 as i32, 10 as i32, 10 as i32]);

        let aggregated = agg_column(&series, &NumericAggregation::Range).unwrap();

        assert_eq!(
            aggregated
                .utf8()
                .unwrap()
                .into_iter()
                .collect::<Vec<Option<&str>>>(),
            vec![Some("10"), Some("10"), Some("10")]
        );
    }

    #[test]
    fn number_agg_range() {
        let series = Series::new("last_name", vec![10 as i32, 20 as i32, 30 as i32]);

        let aggregated = agg_column(&series, &NumericAggregation::Range).unwrap();

        assert_eq!(
            aggregated
                .utf8()
                .unwrap()
                .into_iter()
                .collect::<Vec<Option<&str>>>(),
            vec![Some("10 - 30"), Some("10 - 30"), Some("10 - 30")]
        );
    }

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
            ("age".to_string(), None),
            ("profession".to_string(), None),
            ("empty".to_string(), None),
        ]
        .iter()
        .cloned()
        .collect();

        let anonymized = anonymize(
            &df,
            &identifiers,
            &quasi_identifiers,
            &[AnonymizationCriteria::KAnonymous { k: 2 }],
            &NumericAggregation::Range,
        )
        .unwrap();

        println!("Annonymized: {:?}", anonymized.head(Some(10)));
    }
}
