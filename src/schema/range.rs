use std::{
    iter::Map,
    ops::{
        Bound, Range, RangeBounds, RangeFrom, RangeFull, RangeInclusive, RangeTo, RangeToInclusive,
    },
};

use anyhow::Result;

use crate::physical::{btree::iter::BTreeEntries, buf::ArcBufSlice};

use super::{deserialize_row, Table, TableHandle};

pub trait TableRange<T: Table> {
    type Output;

    fn range(self, table: &TableHandle<T>) -> Result<Self::Output>;
}

type MappedTableEntries<T> = Map<BTreeEntries, fn(Result<(u64, ArcBufSlice)>) -> Result<T>>;

fn table_range_impl<T: Table>(
    table: &TableHandle<T>,
    range: impl RangeBounds<u64>,
) -> Result<MappedTableEntries<T>> {
    let start = match range.start_bound() {
        Bound::Included(&start) => Some(start),
        Bound::Excluded(&start) => Some(start + 1),
        Bound::Unbounded => None,
    };
    let end = match range.end_bound() {
        Bound::Included(&end) => Some(end + 1),
        Bound::Excluded(&end) => Some(end),
        Bound::Unbounded => None,
    };

    let records = table.rootpage()?.into_entries_range(start..end)?;
    let rows = records.map::<_, fn(_) -> _>(|record| deserialize_row(record?));
    Ok(rows)
}

macro_rules! impl_for_range_types {
    ($($ty:ty),* $(,)?) => {
        $(
            impl<T: Table> TableRange<T> for $ty {
                type Output = MappedTableEntries<T>;

                fn range(self, table: &TableHandle<T>) -> Result<Self::Output> {
                    table_range_impl(table, self)
                }
            }
        )*
    };
}

impl_for_range_types!(
    Range<u64>,
    RangeInclusive<u64>,
    RangeFrom<u64>,
    RangeTo<u64>,
    RangeToInclusive<u64>,
    RangeFull,
);

impl<T: Table> TableRange<T> for u64 {
    type Output = Option<T>;

    fn range(self, table: &TableHandle<T>) -> Result<Self::Output> {
        table_range_impl(table, self..)?.next().transpose()
    }
}

impl<T: Table> TableHandle<T> {
    pub fn get<R: TableRange<T>>(&self, id: R) -> Result<R::Output> {
        id.range(self)
    }
}
