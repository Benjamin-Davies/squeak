use std::{
    cmp::Ordering,
    iter::Map,
    marker::PhantomData,
    ops::{
        Bound, Range, RangeBounds, RangeFrom, RangeFull, RangeInclusive, RangeTo, RangeToInclusive,
    },
};

use anyhow::Result;

use crate::physical::{
    btree::iter::{BTreeIndexEntries, BTreeTableEntries},
    buf::ArcBufSlice,
};

use super::{
    deserialize_record, deserialize_record_with_row_id, Index, IndexHandle, Table, TableHandle,
};

pub trait TableRange<T: Table> {
    type Output;

    fn range(self, table: &TableHandle<T>) -> Result<Self::Output>;
}

pub trait IndexRange<T: Index> {
    type Output;

    fn range(self, index: &IndexHandle<T>) -> Result<Self::Output>;
}

pub struct IndexComparator<I, T> {
    inner: T,
    _marker: PhantomData<I>,
}

type MappedTableEntries<T> = Map<BTreeTableEntries, fn(Result<(u64, ArcBufSlice)>) -> Result<T>>;

type MappedIndexEntries<T, F = fn(ArcBufSlice) -> Ordering> =
    Map<BTreeIndexEntries<F>, fn(Result<ArcBufSlice>) -> Result<T>>;

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

    let records = table.rootpage()?.into_table_entries_range(start..end)?;
    let rows = records.map::<_, fn(_) -> _>(|record| deserialize_record_with_row_id(record?));
    Ok(rows)
}

fn range_cmp<'a, T: Ord + 'a>(range: &impl RangeBounds<&'a T>, other: &T) -> Ordering {
    match range.start_bound() {
        Bound::Included(&start) => {
            if start > other {
                return Ordering::Greater;
            }
        }
        Bound::Excluded(&start) => {
            if start >= other {
                return Ordering::Greater;
            }
        }
        Bound::Unbounded => {}
    }

    match range.end_bound() {
        Bound::Included(&end) => {
            if end < other {
                return Ordering::Less;
            }
        }
        Bound::Excluded(&end) => {
            if end >= other {
                return Ordering::Less;
            }
        }
        Bound::Unbounded => {}
    }

    Ordering::Equal
}

fn index_cmp_impl<'a, I: Index + 'a>(
    range: &impl RangeBounds<&'a I::IndexedFields>,
    record: &ArcBufSlice,
) -> Option<Ordering> {
    let row = deserialize_record::<I>(record.clone()).ok()?;
    let indexed_fields = row.into_indexed_fields();

    Some(range_cmp(range, &indexed_fields))
}

macro_rules! impl_for_range_types {
    ($($range:ident),*) => {
        $(
            impl<T: Table> TableRange<T> for $range<u64> {
                type Output = MappedTableEntries<T>;

                fn range(self, table: &TableHandle<T>) -> Result<Self::Output> {
                    table_range_impl(table, self)
                }
            }

            impl<I: Index> PartialEq<ArcBufSlice> for IndexComparator<I, $range<&I::IndexedFields>> {
                fn eq(&self, other: &ArcBufSlice) -> bool {
                    self.partial_cmp(other) == Some(Ordering::Equal)
                }
            }

            impl<I: Index> PartialOrd<ArcBufSlice> for IndexComparator<I, $range<&I::IndexedFields>> {
                fn partial_cmp(&self, other: &ArcBufSlice) -> Option<Ordering> {
                    index_cmp_impl::<I>(&self.inner, other)
                }
            }
        )*
    };
}

impl_for_range_types!(Range, RangeInclusive, RangeFrom, RangeTo, RangeToInclusive);

impl<T: Table> TableRange<T> for RangeFull {
    type Output = MappedTableEntries<T>;

    fn range(self, table: &TableHandle<T>) -> Result<Self::Output> {
        table_range_impl(table, 0..)
    }
}

impl<T: Table> TableRange<T> for u64 {
    type Output = Option<T>;

    fn range(self, table: &TableHandle<T>) -> Result<Self::Output> {
        table_range_impl(table, self..)?.next().transpose()
    }
}

impl<I: Index> PartialEq<ArcBufSlice> for IndexComparator<I, RangeFull> {
    fn eq(&self, _other: &ArcBufSlice) -> bool {
        true
    }
}

impl<I: Index> PartialOrd<ArcBufSlice> for IndexComparator<I, RangeFull> {
    fn partial_cmp(&self, _other: &ArcBufSlice) -> Option<Ordering> {
        Some(Ordering::Equal)
    }
}

impl<I: Index, T> IndexRange<I> for T
where
    IndexComparator<I, T>: PartialOrd<ArcBufSlice>,
{
    type Output = MappedIndexEntries<I, IndexComparator<I, Self>>;

    fn range(self, index: &IndexHandle<I>) -> Result<Self::Output> {
        let records = index
            .rootpage()?
            .into_index_entries_range(IndexComparator {
                inner: self,
                _marker: PhantomData,
            })?;
        let rows = records.map::<_, fn(_) -> _>(|record| deserialize_record(record?));
        Ok(rows)
    }
}

impl<I: Index> IndexRange<I> for &I::IndexedFields
where
    I::IndexedFields: Ord,
{
    type Output = Option<I>;

    fn range(self, index: &IndexHandle<I>) -> Result<Self::Output> {
        (self..).range(index)?.next().transpose()
    }
}

impl<T: Table> TableHandle<T> {
    pub fn get<R: TableRange<T>>(&self, id: R) -> Result<R::Output> {
        id.range(self)
    }
}

impl<I: Index> IndexHandle<I> {
    pub fn get<R: IndexRange<I>>(&self, id: R) -> Result<R::Output> {
        id.range(self)
    }
}
