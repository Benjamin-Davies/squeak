use std::{
    cmp::Ordering,
    iter::Map,
    marker::PhantomData,
    ops::{Bound, Range, RangeBounds, RangeFrom, RangeInclusive, RangeTo, RangeToInclusive},
};

use anyhow::Result;

use crate::physical::{
    btree::iter::{BTreeIndexEntries, BTreeTableEntries},
    db::ReadDB,
};

use super::{
    deserialize_record, deserialize_record_with_row_id, Table, TableHandle, WithRowId, WithoutRowId,
};

pub trait TableRange<'db, T: Table, DB> {
    type Output;

    fn range(self, table: &'db TableHandle<'db, T, DB>) -> Result<Self::Output>;
}

pub struct IndexComparator<I, T> {
    inner: T,
    _marker: PhantomData<I>,
}

struct EqComparator;

type MappedTableEntries<'db, T, DB> =
    Map<BTreeTableEntries<'db, DB>, fn(Result<(i64, &'db [u8])>) -> Result<T>>;

type MappedIndexEntries<'db, T, C, DB> =
    Map<BTreeIndexEntries<'db, C, DB>, fn(Result<&'db [u8]>) -> Result<T>>;

fn table_range_impl<'db, T: WithRowId, DB: ReadDB>(
    table: &'db TableHandle<'db, T, DB>,
    range: impl RangeBounds<i64>,
) -> Result<MappedTableEntries<T, DB>> {
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

fn index_range_impl<'db, I: WithoutRowId, C: PartialOrd<[u8]>, DB: ReadDB>(
    index: &'db TableHandle<'db, I, DB>,
    comparator: C,
) -> Result<MappedIndexEntries<I, C, DB>> {
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
    record: &[u8],
) -> Option<Ordering> {
    let row = deserialize_record::<I>(record).ok()?;
    let indexed_fields = row.into_sorted_fields();

    Some(range_cmp(range, &indexed_fields))
}

macro_rules! impl_for_range_types {
    ($($range:ident),*) => {
        $(
            impl<'db, T: WithRowId, DB: ReadDB + 'db> TableRange<'db, T, DB> for $range<i64> {
                type Output = MappedTableEntries<'db, T, DB>;

                fn range(self, table: &'db TableHandle<'db, T, DB>) -> Result<Self::Output> {
                    table_range_impl(table, self)
                }
            }

            impl<I: WithoutRowId> PartialEq<[u8]> for IndexComparator<I, $range<&I::SortedFields>> {
                fn eq(&self, other: &[u8]) -> bool {
                    self.partial_cmp(other) == Some(Ordering::Equal)
                }
            }

            impl<I: WithoutRowId> PartialOrd<[u8]> for IndexComparator<I, $range<&I::SortedFields>> {
                fn partial_cmp(&self, other: &[u8]) -> Option<Ordering> {
                    index_cmp_impl::<I>(&self.inner, other)
                }
            }
        )*
    };
}

impl_for_range_types!(Range, RangeInclusive, RangeFrom, RangeTo, RangeToInclusive);

impl PartialEq<[u8]> for EqComparator {
    fn eq(&self, _other: &[u8]) -> bool {
        true
    }
}

impl PartialOrd<[u8]> for EqComparator {
    fn partial_cmp(&self, _other: &[u8]) -> Option<Ordering> {
        Some(Ordering::Equal)
    }
}

impl<'db, T: WithRowId, DB: ReadDB> TableRange<'db, T, DB> for i64 {
    type Output = Option<T>;

    fn range(self, table: &TableHandle<T, DB>) -> Result<Self::Output> {
        table_range_impl(table, self..)?.next().transpose()
    }
}

impl<'db, I: WithoutRowId, T, DB: ReadDB + 'db> TableRange<'db, I, DB> for T
where
    IndexComparator<I, T>: PartialOrd<[u8]>,
{
    type Output = MappedIndexEntries<'db, I, IndexComparator<I, Self>, DB>;

    fn range(self, index: &'db TableHandle<'db, I, DB>) -> Result<Self::Output> {
        index_range_impl(
            index,
            IndexComparator {
                inner: self,
                _marker: PhantomData::<I>,
            },
        )
    }
}

impl<'db, I: WithoutRowId, DB: ReadDB> TableRange<'db, I, DB> for &I::SortedFields
where
    I::SortedFields: Ord,
{
    type Output = Option<I>;

    fn range(self, index: &TableHandle<I, DB>) -> Result<Self::Output> {
        (self..).range(index)?.next().transpose()
    }
}

impl<'db, T: Table, DB: ReadDB> TableHandle<'db, T, DB> {
    pub fn get<R: TableRange<'db, T, DB>>(&'db self, id: R) -> Result<R::Output> {
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
