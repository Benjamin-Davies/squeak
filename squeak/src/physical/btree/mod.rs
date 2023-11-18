use std::{mem, ops::Range};

use anyhow::Result;
use zerocopy::{
    big_endian::{U16, U32},
    AsBytes, FromBytes,
};

use crate::physical::{
    buf::{Buf, BufMut},
    db::ReadDB,
    header as db_header, varint,
};

use self::iter::{BTreeIndexEntries, BTreeTableEntries};

use super::transaction::Transaction;

pub mod iter;

#[derive(Debug, Clone)]
pub struct BTreePage<'db, DB: ?Sized> {
    db: &'db DB,
    page_number: u32,
    header: BTreePageHeader,
    data: &'db [u8],
}

#[derive(Debug)]
pub struct BTreePageMut<'a> {
    page_number: u32,
    header: BTreePageHeader,
    data: &'a mut [u8],
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BTreePageType {
    InteriorIndex,
    InteriorTable,
    LeafIndex,
    LeafTable,
}
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    zerocopy::FromZeroes,
    zerocopy::FromBytes,
    zerocopy::AsBytes,
    zerocopy::Unaligned,
)]
#[repr(C)]
pub struct BTreePageHeader {
    /// The b-tree page type.
    flags: u8,
    /// The start of the first freeblock on the page, or 0 if there are no freeblocks.
    first_freeblock: U16,
    /// The number of cells on this page.
    cell_count: U16,
    /// The start of the cell content area. A zero value for this is interpreted as 65536.
    cell_content_start: U16,
    /// The number of fragmented free bytes within the cell content area.
    fragmented_free_bytes: u8,
    /// The right-most pointer. Only valid for interior (non-leaf) pages.
    right_most_pointer: U16,
}

impl<'db, DB: ReadDB> BTreePage<'db, DB> {
    pub(crate) fn new(db: &'db DB, page_number: u32) -> Result<Self> {
        let data = db.page(page_number)?;

        let start = db_header::reserved(page_number);
        let header = BTreePageHeader::read_from_prefix(&data[start..]).unwrap();
        header.validate();

        Ok(Self {
            db,
            page_number,
            header,
            data,
        })
    }

    pub fn page_type(&self) -> BTreePageType {
        self.header.page_type()
    }

    pub fn cell_count(&self) -> u16 {
        self.header.cell_count.get()
    }

    fn cell_pointer(&self, cell_index: u16) -> u16 {
        assert!(cell_index < self.cell_count());
        let start = db_header::reserved(self.page_number)
            + self.header.size() as usize
            + cell_index as usize * 2;
        U16::read_from_prefix(&self.data[start..]).unwrap().get()
    }

    fn cell(&self, cell_index: u16) -> &'db [u8] {
        let ptr = self.cell_pointer(cell_index);
        let mut data = self.data;
        data.consume_bytes(ptr as usize);
        data
    }

    pub(crate) fn leaf_table_cell(&self, cell_index: u16) -> (i64, &'db [u8]) {
        assert_eq!(self.page_type(), BTreePageType::LeafTable);

        // TODO: Handle when a cell overflows onto a separate page.
        let mut cell = self.cell(cell_index);
        let payload_size = cell.consume_varint();
        let row_id = cell.consume_varint();
        cell.truncate(payload_size as usize);

        (row_id, cell)
    }

    pub(crate) fn interior_table_cell(&self, cell_index: u16) -> (u32, i64) {
        assert_eq!(self.page_type(), BTreePageType::InteriorTable);

        let cell = self.cell(cell_index);
        let left_child_page_number = U32::read_from_prefix(cell).unwrap().get();
        let (row_id, _) = varint::read(&cell[4..]);

        (left_child_page_number, row_id)
    }

    pub(crate) fn leaf_index_cell(&self, cell_index: u16) -> &'db [u8] {
        assert_eq!(self.page_type(), BTreePageType::LeafIndex);

        // TODO: Handle when a cell overflows onto a separate page.
        let mut cell = self.cell(cell_index);
        let payload_size = cell.consume_varint();
        cell.truncate(payload_size as usize);

        cell
    }

    pub(crate) fn interior_index_cell(&self, cell_index: u16) -> (u32, &'db [u8]) {
        assert_eq!(self.page_type(), BTreePageType::InteriorIndex);

        // TODO: Handle when a cell overflows onto a separate page.
        let mut cell = self.cell(cell_index);
        let left_child_page_number = U32::read_from_prefix(cell).unwrap().get();
        let payload_size = cell.consume_varint();
        cell.truncate(payload_size as usize);

        (left_child_page_number, cell)
    }

    pub(crate) fn into_table_entries_range(
        self,
        range: Range<Option<i64>>,
    ) -> Result<BTreeTableEntries<'db, DB>> {
        BTreeTableEntries::with_range(self, range)
    }

    pub(crate) fn into_index_entries_range<C: PartialOrd<[u8]>>(
        self,
        comparator: C,
    ) -> Result<BTreeIndexEntries<'db, C, DB>> {
        BTreeIndexEntries::with_range(self, comparator)
    }
}

impl<'a> BTreePageMut<'a> {
    pub fn new(transaction: &'a mut Transaction, page_number: u32) -> Result<Self> {
        let data = transaction.page_mut(page_number)?;

        let start = db_header::reserved(page_number);
        let header = BTreePageHeader::read_from_prefix(&data[start..]).unwrap();
        header.validate();

        Ok(Self {
            page_number,
            header,
            data,
        })
    }

    pub fn empty(page_number: u32, page_type: BTreePageType, data: &'a mut [u8]) -> Self {
        let start = db_header::reserved(page_number);

        let header = BTreePageHeader {
            flags: page_type.into(),
            first_freeblock: (start as u16 + page_type.header_size()).into(),
            cell_count: 0.into(),
            cell_content_start: (data.len() as u16).into(),
            fragmented_free_bytes: 0,
            right_most_pointer: 0.into(),
        };
        // Writing the header is defered until the page is dropped.

        Self {
            page_number,
            header,
            data,
        }
    }

    pub fn insert_table_record(&mut self, row_id: i64, record: &[u8]) -> Result<()> {
        assert_eq!(self.header.page_type(), BTreePageType::LeafTable); // TODO: support inner nodes

        let mut cell = Vec::with_capacity(18 + record.len());
        cell.write_varint(record.len() as i64);
        cell.write_varint(row_id);
        cell.extend_from_slice(record);

        // TODO: check cell order and avoid overflow (too many cells or too large of cells)
        self.append_cell(&cell);

        Ok(())
    }

    fn append_cell(&mut self, cell: &[u8]) {
        let ptr = self.header.cell_content_start.get() - cell.len() as u16;
        self.data[ptr as usize..][..cell.len()].copy_from_slice(&cell);

        let ptr = U16::from(ptr);
        self.header.cell_content_start = ptr;

        let cell_index = self.header.cell_count.get();
        let start = db_header::reserved(self.page_number)
            + self.header.size() as usize
            + cell_index as usize * 2;
        ptr.write_to_prefix(&mut self.data[start..]).unwrap();

        self.header.cell_count = (cell_index + 1).into();
    }
}

impl<'a> Drop for BTreePageMut<'a> {
    fn drop(&mut self) {
        let mut header_buf = [0; mem::size_of::<BTreePageHeader>()];
        self.header.write_to(&mut header_buf).unwrap();

        let start = db_header::reserved(self.page_number);
        let header_size = self.header.size() as usize;
        self.data[start..][..header_size].copy_from_slice(&header_buf[..header_size]);
    }
}

impl BTreePageType {
    fn is_leaf(self) -> bool {
        match self {
            BTreePageType::InteriorIndex | BTreePageType::InteriorTable => false,
            BTreePageType::LeafIndex | BTreePageType::LeafTable => true,
        }
    }

    fn header_size(self) -> u16 {
        if self.is_leaf() {
            8
        } else {
            12
        }
    }
}

impl TryFrom<u8> for BTreePageType {
    type Error = anyhow::Error;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0x02 => Ok(BTreePageType::InteriorIndex),
            0x05 => Ok(BTreePageType::InteriorTable),
            0x0a => Ok(BTreePageType::LeafIndex),
            0x0d => Ok(BTreePageType::LeafTable),
            _ => Err(anyhow::anyhow!("Invalid b-tree page type: {}", value)),
        }
    }
}

impl From<BTreePageType> for u8 {
    fn from(value: BTreePageType) -> Self {
        match value {
            BTreePageType::InteriorIndex => 0x02,
            BTreePageType::InteriorTable => 0x05,
            BTreePageType::LeafIndex => 0x0a,
            BTreePageType::LeafTable => 0x0d,
        }
    }
}

impl BTreePageHeader {
    fn validate(&self) {
        let _ = self.page_type();
    }

    fn page_type(&self) -> BTreePageType {
        self.flags.try_into().unwrap()
    }

    fn size(&self) -> u16 {
        self.page_type().header_size()
    }
}
