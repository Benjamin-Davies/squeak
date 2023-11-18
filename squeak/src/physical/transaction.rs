use std::collections::{btree_map::Entry, BTreeMap};

use anyhow::Result;
use zerocopy::AsBytes;

use crate::physical::{
    db::{ReadDB, DB},
    freelist,
    header::Header,
};

#[derive(Debug)]
pub struct Transaction<'a> {
    db: &'a mut DB,
    database_size: u32,
    pub(super) freelist_head: u32,
    pub(super) freelist_count: u32,
    dirty_pages: BTreeMap<u32, Box<[u8]>>,
}

impl DB {
    pub fn begin_transaction(&mut self) -> Result<Transaction> {
        let db = self;

        if let Some(file) = db.file.as_mut() {
            let file = file.get_mut().unwrap();
            let new_header = Header::read(file)?;
            if new_header.file_change_counter() != db.header.file_change_counter() {
                db.clear_cache();
            }
            db.header = new_header;
        }

        let database_size = db.header.database_size();
        let freelist_head = db.header.freelist_head();
        let freelist_count = db.header.freelist_count();
        Ok(Transaction {
            db,
            database_size,
            freelist_head,
            freelist_count,
            dirty_pages: BTreeMap::new(),
        })
    }
}

impl<'a> ReadDB for Transaction<'a> {
    fn page(&self, page_number: u32) -> Result<&[u8]> {
        if let Some(dirty_page) = self.dirty_pages.get(&page_number) {
            Ok(dirty_page)
        } else {
            let page = self.db.page(page_number)?;

            Ok(page)
        }
    }
}

impl<'a> Transaction<'a> {
    pub(crate) fn page_mut(&mut self, page_number: u32) -> Result<&mut [u8]> {
        match self.dirty_pages.entry(page_number) {
            Entry::Vacant(entry) => {
                let page = self.db.page(page_number)?;
                let page = entry.insert(page.to_vec().into_boxed_slice());
                Ok(page)
            }
            Entry::Occupied(entry) => Ok(entry.into_mut()),
        }
    }

    pub(crate) fn new_page(&mut self) -> Result<(u32, &mut [u8])> {
        if let Some(free_page) = freelist::pop_page(self)? {
            let page = self.page_mut(free_page)?;
            return Ok((free_page, page));
        }

        let page_size = self.db.header.page_size() as usize;
        let page_number = self.database_size + 1;

        let page = self
            .dirty_pages
            .entry(page_number)
            .or_insert(vec![0; page_size].into_boxed_slice());
        self.database_size = page_number;

        Ok((page_number, page))
    }

    pub fn commit(self) {
        let db = self.db;
        for (page_num, page) in self.dirty_pages {
            dbg!(page_num, page.len());
            // TODO: Write page to disk
            db.pages.insert_or_replace(page_num, page);
        }

        db.header.set_database_size(self.database_size);
        db.header.set_freelist_head(self.freelist_head);
        db.header.set_freelist_count(self.freelist_count);
        db.header.write_to_prefix(db.pages.get_mut(&1).unwrap());

        // TODO: Update db header and flush journal or WAL
    }
}

#[cfg(test)]
mod tests {
    use crate::physical::db::DB;

    #[test]
    fn test_new_page() {
        let mut db = DB::open("examples/empty.db").unwrap();

        let mut transaction = db.begin_transaction().unwrap();

        let (page_number, page) = transaction.new_page().unwrap();
        assert_eq!(page_number, 3);
        assert_eq!(page.len(), transaction.db.header.page_size() as usize);

        drop(transaction);
    }
}
