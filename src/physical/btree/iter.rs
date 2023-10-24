use std::{mem, ops::Range};

use anyhow::Result;

use crate::physical::buf::ArcBufSlice;

use super::{BTreePage, BTreePageType};

pub struct BTreeEntries {
    page: BTreePage,
    index: u16,
    stack: Vec<(BTreePage, u16)>,
    // Exclusive upper bound
    max_row_id: Option<u64>,
}

impl BTreeEntries {
    pub(super) fn new(page: BTreePage) -> Self {
        Self {
            page,
            index: 0,
            stack: Vec::new(),
            max_row_id: None,
        }
    }

    pub(super) fn with_range(page: BTreePage, range: Range<Option<u64>>) -> Result<Self> {
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

impl Iterator for BTreeEntries {
    type Item = Result<(u64, ArcBufSlice)>;

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
