use crate::{table_column_transformer::get_schema_fields, traits::Transformer};
use arrow::{
    array::{GenericStringArray, Int16Array, Int32Array, StringArray},
    record_batch::RecordBatch,
};
use itertools::Itertools;
use polars::prelude::{ChunkAgg, ChunkCompare, DataFrame, NamedFrom, UInt32Chunked};
use polars::prelude::{NewChunkedArray, Series};
use sqlparser::ast::Statement;
use std::collections::{HashSet, VecDeque};

pub struct AnonymizationTransformer {
    pub identifier_columns: Vec<String>,
    pub quasi_identifier_columns: Vec<String>,
    pub criteria: AnonymizationCriteria,
}

impl Transformer for AnonymizationTransformer {
    fn transform_schema(
        &self,
        query: &[Statement],
        schema: &arrow::datatypes::Schema,
    ) -> arrow::datatypes::Schema {
        // TODO: Correct this
        schema.clone()
    }

    fn transform_records(&self, query: &[Statement], data: &RecordBatch) -> RecordBatch {
        let normalized_field_names = get_schema_fields(query.first().unwrap()).unwrap();

        let dataframe = record_batch_to_data_frame(data);

        let quasi_identifier_columns: Vec<String> = normalized_field_names
            .iter()
            .enumerate()
            .filter(|(_, column_name)| self.quasi_identifier_columns.contains(column_name))
            .map(|(idx, _)| data.schema().field(idx).name().to_string())
            .collect();

        let strs: Vec<&str> = quasi_identifier_columns
            .iter()
            .map(|f| f.as_str())
            .collect();

        let anonymized = anonymize(
            &dataframe,
            &strs,
            &[self.criteria.clone()],
            &NumericAggregation::Median,
        );

        println!("{:?}", anonymized.head(None));

        let batch = data_frame_to_record_batch(&anonymized);

        batch
    }
}

fn get_span(series: &Series) -> i64 {
    match series.dtype() {
        &polars::prelude::DataType::UInt8
        | &polars::prelude::DataType::UInt16
        | &polars::prelude::DataType::UInt32
        | &polars::prelude::DataType::UInt64
        | &polars::prelude::DataType::Int16
        | &polars::prelude::DataType::Int32
        | &polars::prelude::DataType::Int64 => {
            let max = series.max::<i64>().unwrap() - series.min::<i64>().unwrap();
            max
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

fn scale_spans(spans: &[i64], scale: &[i64]) -> Vec<i64> {
    spans
        .iter()
        .zip(scale)
        .map(|(value, scale)| value / scale)
        .collect()
}

fn split(df: &DataFrame, partition: &[u32], column_index: usize) -> (Vec<u32>, Vec<u32>) {
    let dfp = df[column_index]
        .take(&UInt32Chunked::new_from_slice("idx", partition))
        .unwrap();

    match dfp.dtype() {
        &polars::prelude::DataType::UInt8
        | &polars::prelude::DataType::UInt16
        | &polars::prelude::DataType::UInt32
        | &polars::prelude::DataType::UInt64
        | &polars::prelude::DataType::Int16
        | &polars::prelude::DataType::Int32
        | &polars::prelude::DataType::Int64 => {
            let median = dfp.median().unwrap();

            let mask = dfp.lt_eq(median);

            let mut dfl = vec![];
            let mut dfr = vec![];
            for (index, mask_val) in partition.iter().zip(&mask) {
                match mask_val.map_or(false, |v| v) {
                    true => dfl.push(*index),
                    false => dfr.push(*index),
                }
            }

            (dfl, dfr)
        }
        &polars::prelude::DataType::Utf8 => {
            let values: Vec<Option<&str>> = dfp.utf8().unwrap().into_iter().unique().collect();

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
    quasi_identifiers: &[&str],
    is_valid: &dyn Fn(&DataFrame, &[u32]) -> bool,
) -> Vec<Vec<u32>> {
    let mut partitions: VecDeque<Vec<u32>> =
        vec![(0..df[0].len()).map(|i| i as u32).collect::<Vec<u32>>()].into();

    let relevant_dataframe = df.columns(quasi_identifiers).unwrap();

    let scale = get_spans(&relevant_dataframe, &partitions[0].clone());

    let mut finished_partitions = vec![];
    while let Some(partition) = partitions.pop_front() {
        let spans = get_spans(&relevant_dataframe, &partition);
        let scaled_spans = &scale_spans(&spans, &scale);

        let mut column_index_span_vec: Vec<(usize, i64)> = quasi_identifiers
            .iter()
            .enumerate()
            .zip(scaled_spans.iter())
            .map(|((column_index, _), span)| (column_index, *span))
            .collect();

        column_index_span_vec.sort_by_key(|(_, span)| *span);
        column_index_span_vec.reverse();

        // println!("Spans: {:?}. (Sorted {:?})", spans, column_index_span_vec);

        let mut did_break = false;
        for (column_index, _) in column_index_span_vec {
            let column_name = quasi_identifiers[column_index];
            let df_column_index = df
                .get_column_names()
                .into_iter()
                .position(|name| name == column_name)
                .unwrap();

            // println!("Column: {:?}", column_index);

            let (lp, rp) = split(df, &partition, df_column_index);

            // println!("{:?} {:?}", lp, rp);

            if !is_valid(df, &lp) || !is_valid(df, &rp) {
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

    return finished_partitions;
}

pub enum NumericAggregation {
    Median,
    Range,
}

fn agg_column(series: &Series, numeric_aggregation: &NumericAggregation) -> Series {
    match series.dtype() {
        &polars::prelude::DataType::UInt8 => match numeric_aggregation {
            NumericAggregation::Median => Series::new(
                series.name(),
                vec![series.mean().unwrap(); series.u8().unwrap().len()],
            ),
            NumericAggregation::Range => {
                let min = series
                    .u8()
                    .unwrap()
                    .min()
                    .map_or("Null".to_string(), |f| format!("{}", f));
                let max = series
                    .u8()
                    .unwrap()
                    .max()
                    .map_or("Null".to_string(), |f| format!("{}", f));
                let agg = if max == min {
                    max
                } else {
                    format!("{} - {}", min, max)
                };
                Series::new(series.name(), vec![agg; series.u8().unwrap().len()])
            }
        },
        &polars::prelude::DataType::Int32 => match numeric_aggregation {
            NumericAggregation::Median => Series::new(
                series.name(),
                vec![series.mean().unwrap(); series.i32().unwrap().len()],
            ),
            NumericAggregation::Range => {
                let min = series
                    .i32()
                    .unwrap()
                    .min()
                    .map_or("Null".to_string(), |f| format!("{}", f));
                let max = series
                    .i32()
                    .unwrap()
                    .max()
                    .map_or("Null".to_string(), |f| format!("{}", f));
                let agg = if max == min {
                    max
                } else {
                    format!("{} - {}", min, max)
                };

                Series::new(series.name(), vec![agg; series.i32().unwrap().len()])
            }
        },
        &polars::prelude::DataType::Int64 => match numeric_aggregation {
            NumericAggregation::Median => Series::new(
                series.name(),
                vec![series.mean().unwrap(); series.i64().unwrap().len()],
            ),
            NumericAggregation::Range => {
                let min = series
                    .i64()
                    .unwrap()
                    .min()
                    .map_or("Null".to_string(), |f| format!("{}", f));
                let max = series
                    .i64()
                    .unwrap()
                    .max()
                    .map_or("Null".to_string(), |f| format!("{}", f));
                let agg = if max == min {
                    max
                } else {
                    format!("{} - {}", min, max)
                };

                Series::new(series.name(), vec![agg; series.i64().unwrap().len()])
            }
        },
        &polars::prelude::DataType::Utf8 => {
            let unique_strings: Vec<&str> = series
                .utf8()
                .unwrap()
                .into_iter()
                .unique()
                .map(|v| v.map_or("None", |v| v))
                .collect();

            let new_string = unique_strings.join(", ");

            Series::new(
                series.name(),
                vec![new_string; series.utf8().unwrap().len()],
            )
        }
        _ => todo!(),
    }
}

pub fn build_anonymized_dataset(
    df: &DataFrame,
    partitions: &[Vec<u32>],
    feature_columns: &[&str],
    numeric_aggregation: &NumericAggregation,
) -> DataFrame {
    let mut updated: Vec<Series> = vec![];
    let mut original_indices_of_updated_rows: Vec<u32> = vec![];

    for partition in partitions {
        for (index, column) in df.get_columns().iter().enumerate() {
            let new_data: Series = if feature_columns.contains(&column.name()) {
                let original_data = column
                    .take(&UInt32Chunked::new_from_slice("idx", partition))
                    .unwrap();

                agg_column(&original_data, numeric_aggregation)
            } else {
                column
                    .take(&UInt32Chunked::new_from_slice("idx", partition))
                    .unwrap()
            };

            let entry = updated.get_mut(index);

            match entry {
                Some(s) => updated[index] = s.append(&new_data).unwrap().clone(),
                None => updated.push(new_data),
            };

            for index in partition {
                original_indices_of_updated_rows.push(*index);
            }
        }
    }

    // TODO: Reorder according to original indices

    DataFrame::new(updated).unwrap()
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
            arrow::datatypes::DataType::Int16 => {
                let vec = column
                    .as_any()
                    .downcast_ref::<Int16Array>()
                    .unwrap()
                    .iter()
                    .collect::<Vec<Option<i16>>>();

                let array =
                    ChunkedArray::<Int16Type>::new_from_opt_slice(field.name(), vec.as_slice());

                let array: ChunkedArray<Int32Type> = array.cast().unwrap();

                array.into_series()
            }
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
            arrow::datatypes::DataType::LargeUtf8 => Series::new(
                field.name(),
                column
                    .as_any()
                    .downcast_ref::<GenericStringArray<i64>>()
                    .unwrap()
                    .iter()
                    .map(|val| val.map(|v| v.to_string()))
                    .collect::<Vec<Option<String>>>(),
            ),
            _ => todo!("{:?}", column.data_type()),
        })
        .collect();

    DataFrame::new(series).unwrap()
}

pub fn data_frame_to_record_batch(df: &DataFrame) -> RecordBatch {
    todo!();
}

pub fn is_k_anonymous(partition: &[u32], k: usize) -> bool {
    if partition.len() < k {
        return false;
    }
    return true;
}

pub fn is_l_diverse(df: &DataFrame, partition: &[u32], sensitive_column: &str, l: usize) -> bool {
    df.column(sensitive_column)
        .unwrap()
        .take(&UInt32Chunked::new_from_slice("idx", partition))
        .unwrap()
        .unique()
        .unwrap()
        .len()
        >= l
}

#[derive(Clone)]
pub enum AnonymizationCriteria {
    KAnonymous { k: usize },
    LDiverse { l: usize, sensitive_column: String },
}

impl AnonymizationCriteria {
    fn is_anonymous(&self, df: &DataFrame, partition: &[u32]) -> bool {
        match self {
            Self::KAnonymous { k } => is_k_anonymous(partition, *k),
            Self::LDiverse {
                l,
                sensitive_column,
            } => is_l_diverse(df, partition, &sensitive_column, *l),
        }
    }
}

pub fn anonymize(
    df: &DataFrame,
    quasi_identifiers: &[&str],
    criteria: &[AnonymizationCriteria],
    numeric_aggregation: &NumericAggregation,
) -> DataFrame {
    let finished_partitions = partition_dataset(&df, &quasi_identifiers, &|_, partition|
        // If any criteria returns false, return false
        criteria
            .iter()
            .map(|criteria| criteria.is_anonymous(df, partition))
            .position(|b| !b)
            .is_none());

    let anonymized = build_anonymized_dataset(
        &df,
        &finished_partitions,
        &quasi_identifiers,
        numeric_aggregation,
    );

    anonymized
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
    fn string_aggregation() {
        let series = Series::new("last_name", vec!["Müller", "Müller", "Schidt"]);

        let aggregated = agg_column(&series, &NumericAggregation::Median);

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

        let aggregated = agg_column(&series, &NumericAggregation::Median);

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

        let aggregated = agg_column(&series, &NumericAggregation::Range);

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

        let aggregated = agg_column(&series, &NumericAggregation::Range);

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
        println!("{:?}", df.head(Some(10)));

        let quasi_identifiers = vec!["age", "profession"];

        let anonymized = anonymize(
            &df,
            &quasi_identifiers,
            &[AnonymizationCriteria::KAnonymous { k: 2 }],
            &NumericAggregation::Range,
        );

        println!("Annonymized: {:?}", anonymized.head(Some(10)));
    }
}