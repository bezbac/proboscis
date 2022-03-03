use arrow::{
    array::{make_array, Array, ArrayRef},
    error::ArrowError,
    record_batch::RecordBatch,
};
use polars::prelude::{DataFrame, PolarsError, Series};
use std::{convert::TryFrom, ops::Deref, sync::Arc};

pub fn record_batch_to_data_frame(data: &RecordBatch) -> Result<DataFrame, PolarsError> {
    use polars::prelude::*;

    let mut series_vec = vec![];

    for (column, field) in data.columns().iter().zip(data.schema().fields()) {
        let series = Series::try_from((field.name().as_str(), vec![column.clone()]))?;
        series_vec.push(series)
    }

    DataFrame::new(series_vec)
}

pub fn series_to_arrow_array(series: &Series) -> Result<ArrayRef, ArrowError> {
    let arrays: Vec<Arc<dyn Array>> = series
        .array_data()
        .iter()
        .cloned()
        .map(|data| make_array(data.clone()))
        .collect();

    let array_refs: Vec<&dyn Array> = arrays.iter().map(|a| a.deref()).collect();

    arrow::compute::kernels::concat::concat(&array_refs)
}

pub fn data_frame_to_record_batch(
    df: &DataFrame,
    schema: arrow::datatypes::Schema,
) -> Result<RecordBatch, ArrowError> {
    let mut columns = vec![];

    for column in df.get_columns().iter() {
        columns.push(series_to_arrow_array(column)?);
    }

    RecordBatch::try_new(Arc::new(schema), columns)
}
