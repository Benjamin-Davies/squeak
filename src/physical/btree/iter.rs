use std::mem;

use anyhow::Result;

use crate::physical::buf::ArcBufSlice;

use super::{BTreePage, BTreePageType};

pub struct BTreeEntries {
    page: BTreePage,
    index: u16,
    stack: Vec<(BTreePage, u16)>,
}

impl BTreeEntries {
    pub(super) fn new(page: BTreePage) -> Self {
        Self {
            page,
            index: 0,
            stack: Vec::new(),
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
