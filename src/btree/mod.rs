use anyhow::Result;
use serde::de::IntoDeserializer;
use zerocopy::{
    big_endian::{U16, U32},
    FromBytes,
};

use crate::{db::DB, header::HEADER_SIZE, row::Row, varint};

use self::iter::TableRowsIterator;

pub mod iter;

#[derive(Debug, Clone)]
pub struct BTreePage {
    db: DB,
    page_number: u32,
    header: BTreePageHeader,
    data: Box<[u8]>,
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
struct BTreePageHeader {
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

impl BTreePage {
    pub(crate) fn new(db: DB, page_number: u32, data: &[u8]) -> BTreePage {
        let start = if page_number == 1 { HEADER_SIZE } else { 0 };
        let header = BTreePageHeader::read_from_prefix(&data[start..]).unwrap();
        header.validate();

        BTreePage {
            db,
            page_number,
            header,
            data: data.into(),
        }
    }

    pub fn page_type(&self) -> BTreePageType {
        self.header.page_type()
    }

    fn cell_pointer(&self, cell_index: u16) -> u16 {
        let start = if self.page_number == 1 {
            HEADER_SIZE
        } else {
            0
        } + self.header.size() as usize
            + cell_index as usize * 2;
        U16::read_from_prefix(&self.data[start..]).unwrap().get()
    }

    fn cell(&self, cell_index: u16) -> &[u8] {
        let ptr = self.cell_pointer(cell_index);
        &self.data[ptr as usize..]
    }

    pub(crate) fn leaf_table_cell(&self, cell_index: u16) -> (u64, &'_ [u8]) {
        assert_eq!(self.page_type(), BTreePageType::LeafTable);

        // TODO: Handle cell overflow.
        let cell = self.cell(cell_index);
        let (payload_size, cell) = varint::read(cell);
        let (row_id, cell) = varint::read(cell);
        (row_id, &cell[..payload_size as usize])
    }

    pub(crate) fn interior_table_cell(&self, cell_index: u16) -> (u32, u64) {
        assert_eq!(self.page_type(), BTreePageType::InteriorTable);

        let cell = self.cell(cell_index);
        let left_child_page_number = U32::read_from_prefix(cell).unwrap().get();
        let (row_id, _) = varint::read(&cell[4..]);

        (left_child_page_number, row_id)
    }

    pub fn rows_dyn(&self) -> TableRowsIterator<'_> {
        TableRowsIterator::new(self)
    }

    pub fn rows<T: Row>(&self) -> impl Iterator<Item = Result<T>> + '_ {
        self.rows_dyn().map(|res| {
            res.map(|(row_id, record)| {
                let mut row = T::deserialize(record.into_deserializer()).unwrap();
                row.set_db(&self.db);
                row.set_row_id(row_id);
                row
            })
        })
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

impl BTreePageHeader {
    fn validate(&self) {
        assert!([0x02, 0x05, 0x0a, 0x0d].contains(&self.flags));
    }

    fn page_type(&self) -> BTreePageType {
        match self.flags {
            0x02 => BTreePageType::InteriorIndex,
            0x05 => BTreePageType::InteriorTable,
            0x0a => BTreePageType::LeafIndex,
            0x0d => BTreePageType::LeafTable,
            _ => unreachable!(),
        }
    }

    fn size(&self) -> u16 {
        if self.page_type().is_leaf() {
            8
        } else {
            12
        }
    }
}
