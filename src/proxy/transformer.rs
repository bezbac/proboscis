use arrow::{array::ArrayRef, datatypes::Field};

pub trait Transformer: Sync + Send {
    fn transform(&self, column: ArrayRef) -> ArrayRef;
    fn matches(&self, field: &Field) -> bool;
}
