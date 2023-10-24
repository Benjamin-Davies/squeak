use anyhow::Result;

use super::{deserialize_row, Table, TableHandle};

pub trait TableRange<T: Table> {
    type Output;

    fn range(self, table: &TableHandle<T>) -> Result<Self::Output>;
}

impl<T: Table> TableRange<T> for u64 {
    type Output = Option<T>;

    fn range(self, table: &TableHandle<T>) -> Result<Self::Output> {
        table
            .rootpage()?
            .find_entry(self)?
            .map(|entry| deserialize_row((self, entry)))
            .transpose()
    }
}

impl<T: Table> TableHandle<T> {
    pub fn get<R: TableRange<T>>(&self, id: R) -> Result<R::Output> {
        id.range(self)
    }
}
