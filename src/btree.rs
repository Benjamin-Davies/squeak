use std::mem;

use zerocopy::{big_endian::U16, FromBytes};

use crate::db::DB;

#[derive(Debug, Clone)]
pub struct BTree {
    db: DB,
    page_number: u32,
    header: BTreePageHeader,
    data: Box<[u8]>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BTreeRole {
    Index,
    Table,
}

impl BTree {
    pub(crate) fn new(db: DB, page_number: u32, page: &[u8]) -> BTree {
        let header = BTreePageHeader::read_from_prefix(page).unwrap();
        header.validate();

        let data = page[mem::size_of::<BTreePageHeader>()..].into();

        BTree {
            db,
            page_number,
            header,
            data,
        }
    }

    pub fn is_leaf(&self) -> bool {
        self.header.is_leaf()
    }

    pub fn role(&self) -> BTreeRole {
        self.header.role()
    }
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

impl BTreePageHeader {
    fn validate(&self) {
        assert!([0x02, 0x05, 0x0a, 0x0d].contains(&self.flags));
    }

    fn is_leaf(&self) -> bool {
        self.flags & 0x08 != 0
    }

    fn role(&self) -> BTreeRole {
        match self.flags & 0x07 {
            0x02 => BTreeRole::Index,
            0x05 => BTreeRole::Table,
            _ => unreachable!(),
        }
    }
}
