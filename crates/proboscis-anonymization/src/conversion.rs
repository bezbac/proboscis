use arrow::{
    array::{make_array, Array, ArrayRef},
    record_batch::RecordBatch,
};
use polars::prelude::{DataFrame, Series};
use std::{convert::TryFrom, ops::Deref, sync::Arc};

pub fn record_batch_to_data_frame(data: &RecordBatch) -> DataFrame {
    use polars::prelude::*;

    let series: Vec<Series> = data
        .columns()
        .iter()
        .zip(data.schema().fields())
        .map(|(column, field)| {
            Series::try_from((field.name().as_str(), vec![column.clone()])).unwrap()
        })
        .collect();

    DataFrame::new(series).unwrap()
}

pub fn series_to_arrow_array(series: &Series) -> ArrayRef {
    let arrays: Vec<Arc<dyn Array>> = series
        .array_data()
        .iter()
        .cloned()
        .map(|data| make_array(data.clone()))
        .collect();

    let array_refs: Vec<&dyn Array> = arrays.iter().map(|a| a.deref()).collect();

    arrow::compute::kernels::concat::concat(&array_refs).unwrap()
}

pub fn data_frame_to_record_batch(df: &DataFrame, schema: arrow::datatypes::Schema) -> RecordBatch {
    let columns: Vec<ArrayRef> = df.get_columns().iter().map(series_to_arrow_array).collect();

    RecordBatch::try_new(Arc::new(schema), columns).unwrap()
}
