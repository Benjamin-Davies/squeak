use std::{cmp::Ordering, mem, ops::Range};

use anyhow::Result;

use super::{BTreePage, BTreePageType};

pub struct BTreeTableEntries<'db> {
    page: BTreePage<'db>,
    index: u16,
    stack: Vec<(BTreePage<'db>, u16)>,
    // Exclusive upper bound
    max_row_id: Option<u64>,
}

pub struct BTreeIndexEntries<'db, C> {
    page: BTreePage<'db>,
    index: u16,
    stack: Vec<(BTreePage<'db>, u16)>,
    // Used to see if we're inside of the specified range
    comparator: C,
}

impl<'db> BTreeTableEntries<'db> {
    pub(super) fn new(page: BTreePage<'db>) -> Self {
        Self {
            page,
            index: 0,
            stack: Vec::new(),
            max_row_id: None,
        }
    }

    pub(super) fn with_range(page: BTreePage<'db>, range: Range<Option<u64>>) -> Result<Self> {
        let mut entries = Self::new(page);

        if let Some(start) = range.start {
            entries.seek(start)?;
        }
        entries.max_row_id = range.end;

        Ok(entries)
    }

    fn seek(&mut self, row_id: u64) -> Result<()> {
        loop {
            match self.page.page_type() {
                BTreePageType::InteriorTable => {
                    // TODO: binary search
                    let mut child_page_index = 0;
                    for index in 0..self.page.header.cell_count.get() {
                        let (_page_number, current_id) = self.page.interior_table_cell(index);
                        if current_id > row_id {
                            break;
                        } else {
                            child_page_index = index + 1;
                        }
                    }

                    let (child_page_number, _id) = self.page.interior_table_cell(child_page_index);
                    let child_page = self.page.db.btree_page(child_page_number)?;
                    let parent_page = mem::replace(&mut self.page, child_page);
                    self.stack.push((parent_page, child_page_index + 1));
                }
                BTreePageType::LeafTable => {
                    // TODO: binary search
                    let mut leaf_index = 0;
                    for index in 0..self.page.header.cell_count.get() {
                        let (current_id, _data) = self.page.leaf_table_cell(index);
                        if current_id > row_id {
                            break;
                        } else {
                            leaf_index = index;
                        }
                    }
                    self.index = leaf_index;
                    return Ok(());
                }
                ty => todo!("{ty:?}"),
            }
        }
    }
}

impl<'db> Iterator for BTreeTableEntries<'db> {
    type Item = Result<(u64, &'db [u8])>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.index < self.page.header.cell_count.get() {
                match self.page.page_type() {
                    BTreePageType::InteriorTable => {
                        let (page_number, _row_id) = self.page.interior_table_cell(self.index);
                        self.index += 1;

                        let mut page = match self.page.db.btree_page(page_number) {
                            Ok(page) => page,
                            Err(err) => return Some(Err(err)),
                        };

                        mem::swap(&mut self.page, &mut page);
                        self.stack.push((page, self.index));
                        self.index = 0;
                    }
                    BTreePageType::LeafTable => {
                        let (row_id, record) = self.page.leaf_table_cell(self.index);
                        self.index += 1;

                        if let Some(max_row_id) = self.max_row_id {
                            if row_id >= max_row_id {
                                return None;
                            }
                        }

                        return Some(Ok((row_id, record)));
                    }
                    _ => todo!("{:?}", self.page.page_type()),
                }
            } else if let Some(popped) = self.stack.pop() {
                (self.page, self.index) = popped;
            } else {
                return None;
            }
        }
    }
}

impl<'db, C: PartialOrd<[u8]>> BTreeIndexEntries<'db, C> {
    pub(super) fn with_range(page: BTreePage<'db>, comparator: C) -> Result<Self> {
        let mut entries = Self {
            page,
            index: 0,
            stack: Vec::new(),
            comparator,
        };

        entries.seek_start()?;

        Ok(entries)
    }

    fn seek_start(&mut self) -> Result<()> {
        loop {
            match self.page.page_type() {
                BTreePageType::InteriorIndex => {
                    // TODO: binary search
                    let mut child_page_index = 0;
                    for index in 0..self.page.header.cell_count.get() {
                        let (_page_number, current_key) = self.page.interior_index_cell(index);
                        if self.comparator < *current_key {
                            child_page_index = index;
                        } else {
                            break;
                        }
                    }

                    let (child_page_number, _key) = self.page.interior_index_cell(child_page_index);
                    let child_page = self.page.db.btree_page(child_page_number)?;
                    let parent_page = mem::replace(&mut self.page, child_page);
                    self.stack.push((parent_page, child_page_index + 1));
                }
                BTreePageType::LeafIndex => {
                    // TODO: binary search
                    let mut leaf_index = 0;
                    for index in 0..self.page.header.cell_count.get() {
                        let current_key = self.page.leaf_index_cell(index);
                        if self.comparator < *current_key {
                            leaf_index = index;
                        } else {
                            break;
                        }
                    }
                    self.index = leaf_index;
                    return Ok(());
                }
                ty => todo!("{ty:?}"),
            }
        }
    }
}

impl<'db, C: PartialOrd<[u8]>> Iterator for BTreeIndexEntries<'db, C> {
    type Item = Result<&'db [u8]>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.index < self.page.header.cell_count.get() {
                match self.page.page_type() {
                    BTreePageType::InteriorIndex => {
                        let (page_number, _payload) = self.page.interior_index_cell(self.index);
                        self.index += 1;

                        let mut page = match self.page.db.btree_page(page_number) {
                            Ok(page) => page,
                            Err(err) => return Some(Err(err)),
                        };

                        mem::swap(&mut self.page, &mut page);
                        self.stack.push((page, self.index));
                        self.index = 0;
                    }
                    BTreePageType::LeafIndex => {
                        let record = self.page.leaf_index_cell(self.index);
                        self.index += 1;

                        match self.comparator.partial_cmp(record) {
                            Some(Ordering::Less) => return None,
                            Some(Ordering::Equal) => return Some(Ok(record)),
                            _ => continue,
                        }
                    }
                    _ => todo!("{:?}", self.page.page_type()),
                }
            } else if let Some(popped) = self.stack.pop() {
                (self.page, self.index) = popped;
            } else {
                return None;
            }
        }
    }
}
