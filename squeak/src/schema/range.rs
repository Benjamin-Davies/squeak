use std::{
    cmp::Ordering,
    iter::Map,
    marker::PhantomData,
    ops::{Bound, Range, RangeBounds, RangeFrom, RangeInclusive, RangeTo, RangeToInclusive},
};

use anyhow::Result;

use crate::physical::{
    btree::iter::{BTreeIndexEntries, BTreeTableEntries},
    buf::ArcBufSlice,
};

use super::{
    deserialize_record, deserialize_record_with_row_id, Table, TableHandle, WithRowId, WithoutRowId,
};

pub trait TableRange<'db, T: Table> {
    type Output;

    fn range(self, table: &'db TableHandle<'db, T>) -> Result<Self::Output>;
}

pub struct IndexComparator<I, T> {
    inner: T,
    _marker: PhantomData<I>,
}

struct EqComparator;

type MappedTableEntries<'db, T> =
    Map<BTreeTableEntries<'db>, fn(Result<(u64, ArcBufSlice)>) -> Result<T>>;

type MappedIndexEntries<'db, T, C> =
    Map<BTreeIndexEntries<'db, C>, fn(Result<ArcBufSlice>) -> Result<T>>;

fn table_range_impl<'db, T: WithRowId>(
    table: &'db TableHandle<'db, T>,
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

fn index_range_impl<'db, I: WithoutRowId, C: PartialOrd<ArcBufSlice>>(
    index: &'db TableHandle<'db, I>,
    comparator: C,
) -> Result<MappedIndexEntries<I, C>> {
    let records = index.rootpage()?.into_index_entries_range(comparator)?;
    let rows = records.map::<_, fn(_) -> _>(|record| deserialize_record(record?));
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

fn index_cmp_impl<'a, I: WithoutRowId + 'a>(
    range: &impl RangeBounds<&'a I::SortedFields>,
    record: &ArcBufSlice,
) -> Option<Ordering> {
    let row = deserialize_record::<I>(record.clone()).ok()?;
    let indexed_fields = row.into_sorted_fields();

    Some(range_cmp(range, &indexed_fields))
}

macro_rules! impl_for_range_types {
    ($($range:ident),*) => {
        $(
            impl<'db, T: WithRowId> TableRange<'db, T> for $range<u64> {
                type Output = MappedTableEntries<'db, T>;

                fn range(self, table: &'db TableHandle<'db, T>) -> Result<Self::Output> {
                    table_range_impl(table, self)
                }
            }

            impl<I: WithoutRowId> PartialEq<ArcBufSlice> for IndexComparator<I, $range<&I::SortedFields>> {
                fn eq(&self, other: &ArcBufSlice) -> bool {
                    self.partial_cmp(other) == Some(Ordering::Equal)
                }
            }

            impl<I: WithoutRowId> PartialOrd<ArcBufSlice> for IndexComparator<I, $range<&I::SortedFields>> {
                fn partial_cmp(&self, other: &ArcBufSlice) -> Option<Ordering> {
                    index_cmp_impl::<I>(&self.inner, other)
                }
            }
        )*
    };
}

impl_for_range_types!(Range, RangeInclusive, RangeFrom, RangeTo, RangeToInclusive);

impl PartialEq<ArcBufSlice> for EqComparator {
    fn eq(&self, _other: &ArcBufSlice) -> bool {
        true
    }
}

impl PartialOrd<ArcBufSlice> for EqComparator {
    fn partial_cmp(&self, _other: &ArcBufSlice) -> Option<Ordering> {
        Some(Ordering::Equal)
    }
}

impl<'db, T: WithRowId> TableRange<'db, T> for u64 {
    type Output = Option<T>;

    fn range(self, table: &TableHandle<T>) -> Result<Self::Output> {
        table_range_impl(table, self..)?.next().transpose()
    }
}

impl<'db, I: WithoutRowId, T> TableRange<'db, I> for T
where
    IndexComparator<I, T>: PartialOrd<ArcBufSlice>,
{
    type Output = MappedIndexEntries<'db, I, IndexComparator<I, Self>>;

    fn range(self, index: &'db TableHandle<'db, I>) -> Result<Self::Output> {
        index_range_impl(
            index,
            IndexComparator {
                inner: self,
                _marker: PhantomData::<I>,
            },
        )
    }
}

impl<'db, I: WithoutRowId> TableRange<'db, I> for &I::SortedFields
where
    I::SortedFields: Ord,
{
    type Output = Option<I>;

    fn range(self, index: &TableHandle<I>) -> Result<Self::Output> {
        (self..).range(index)?.next().transpose()
    }
}

impl<'db, T: Table> TableHandle<'db, T> {
    pub fn get<R: TableRange<'db, T>>(&'db self, id: R) -> Result<R::Output> {
        id.range(self)
    }

    pub fn iter(&'db self) -> Result<impl Iterator<Item = Result<T>> + 'db>
    where
        T: WithRowId,
    {
        table_range_impl(self, ..)
    }

    pub fn iter_without_row_id(&'db self) -> Result<impl Iterator<Item = Result<T>> + 'db>
    where
        T: WithoutRowId,
    {
        index_range_impl(self, EqComparator)
    }
}
