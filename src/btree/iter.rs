use std::{borrow::Cow, mem};

use crate::{db::DB, record::Record};

use super::{BTreePage, BTreePageType};

pub struct TableRowsIterator<'a> {
    db: &'a DB,
    page: Cow<'a, BTreePage>,
    index: u16,
    stack: Vec<(Cow<'a, BTreePage>, u16)>,
}

impl<'a> TableRowsIterator<'a> {
    pub(super) fn new(page: &'a BTreePage) -> Self {
        Self {
            db: &page.db,
            page: Cow::Borrowed(page),
            index: 0,
            stack: Vec::new(),
        }
    }
}

impl<'a> Iterator for TableRowsIterator<'a> {
    type Item = (u64, Record);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.index < self.page.header.cell_count.get() {
                match self.page.page_type() {
                    BTreePageType::InteriorTable => {
                        let (page_number, _row_id) = self.page.interior_table_cell(self.index);
                        let page = self.db.btree(page_number).unwrap();

                        let mut page = Cow::Owned(page);
                        mem::swap(&mut self.page, &mut page);

                        self.index += 1;
                        self.stack.push((page, self.index));
                        self.index = 0;
                    }
                    BTreePageType::LeafTable => {
                        let (row_id, record) = self.page.leaf_table_cell(self.index);
                        self.index += 1;

                        return Some((row_id, Record::from(record)));
                    }
                    _ => todo!("{:?}", self.page.page_type()),
                }
            } else {
                if let Some(popped) = self.stack.pop() {
                    (self.page, self.index) = popped;
                } else {
                    return None;
                }
            }
        }
    }
}
