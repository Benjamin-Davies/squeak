use std::{mem, ops::Range};

use anyhow::Result;
use zerocopy::{
    big_endian::{U16, U32},
    AsBytes, FromBytes,
};

use crate::physical::{buf::Buf, db::ReadDB, header::HEADER_SIZE, varint};

use self::iter::{BTreeIndexEntries, BTreeTableEntries};

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

        let start = if page_number == 1 { HEADER_SIZE } else { 0 };
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
        let start = if self.page_number == 1 {
            HEADER_SIZE
        } else {
            0
        } + self.header.size() as usize
            + cell_index as usize * 2;
        U16::read_from_prefix(&self.data[start..]).unwrap().get()
    }

    fn cell(&self, cell_index: u16) -> &'db [u8] {
        let ptr = self.cell_pointer(cell_index);
        let mut data = self.data;
        data.consume_bytes(ptr as usize);
        data
    }

    pub(crate) fn leaf_table_cell(&self, cell_index: u16) -> (u64, &'db [u8]) {
        assert_eq!(self.page_type(), BTreePageType::LeafTable);

        // TODO: Handle when a cell overflows onto a separate page.
        let mut cell = self.cell(cell_index);
        let payload_size = cell.consume_varint();
        let row_id = cell.consume_varint();
        cell.truncate(payload_size as usize);

        (row_id, cell)
    }

    pub(crate) fn interior_table_cell(&self, cell_index: u16) -> (u32, u64) {
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
        range: Range<Option<u64>>,
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
    pub fn empty(page_number: u32, page_type: BTreePageType, data: &'a mut [u8]) -> Self {
        let start = if page_number == 1 { HEADER_SIZE } else { 0 };

        let header = BTreePageHeader {
            flags: page_type.into(),
            first_freeblock: ((start + mem::size_of::<BTreePageHeader>()) as u16).into(),
            cell_count: 0.into(),
            cell_content_start: (data.len() as u16).into(),
            fragmented_free_bytes: 0,
            right_most_pointer: 0.into(),
        };

        header.write_to_prefix(&mut data[start..]).unwrap();

        Self {
            page_number,
            header,
            data,
        }
    }
}

impl BTreePageType {
    fn is_leaf(self) -> bool {
        match self {
            BTreePageType::InteriorIndex | BTreePageType::InteriorTable => false,
            BTreePageType::LeafIndex | BTreePageType::LeafTable => true,
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
        if self.page_type().is_leaf() {
            8
        } else {
            12
        }
    }
}
