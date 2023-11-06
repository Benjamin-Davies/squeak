use std::{
    fmt,
    fs::File,
    io::{Read, Seek, SeekFrom},
    sync::Mutex,
};

use anyhow::{anyhow, Result};

use crate::physical::{
    btree::BTreePage,
    header::Header,
    shared_append_map::{Entry, SharedAppendMap},
};

pub struct DB {
    pub(super) file: Mutex<File>,
    pub(super) pages: SharedAppendMap<u32, [u8]>,
    pub(super) header: Header,
}

impl DB {
    pub fn open(path: &str) -> Result<Self> {
        let file = File::open(path)?;

        let mut db = Self {
            file: Mutex::new(file),
            pages: SharedAppendMap::new(),
            header: Header::default(),
        };

        let header: Header = db.page(1)?.as_ref().into();
        header.validate();
        db.header = header;

        // Clear the pages cache as the page size may have changed.
        db.pages = SharedAppendMap::new();

        Ok(db)
    }

    pub(crate) fn btree_page(&self, page_number: u32) -> Result<BTreePage> {
        let page = self.page(page_number)?;

        Ok(BTreePage::new(self, page_number, page))
    }

    pub(crate) fn page(&self, page_number: u32) -> Result<&[u8]> {
        let entry = self.pages.entry(page_number);
        let page = match entry {
            Entry::Occupied(entry) => entry,
            Entry::Vacant(entry) => {
                if !(1..=self.header.database_size()).contains(&page_number) {
                    return Err(anyhow!("page number out of bounds"));
                }

                let page_size = self.header.page_size();

                let mut file = self.file.lock().unwrap();
                let mut page = vec![0; page_size as usize];
                file.seek(SeekFrom::Start((page_number as u64 - 1) * page_size as u64))?;
                file.read_exact(&mut page)?;

                entry.insert(page)
            }
        };

        Ok(page)
    }
}

impl fmt::Debug for DB {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("DB")
    }
}

#[cfg(test)]
mod tests {
    use crate::physical::btree::BTreePageType;

    use super::*;

    #[test]
    fn test_open() {
        let db = DB::open("examples/empty.db").unwrap();
        assert_eq!(db.header.page_size(), 4096);
    }

    #[test]
    fn test_read_btree() {
        let db = DB::open("examples/empty.db").unwrap();

        let root = db.btree_page(1).unwrap();
        assert_eq!(root.page_type(), BTreePageType::LeafTable);

        let cell = root.leaf_table_cell(0);
        assert_eq!(cell.0, 1);
    }
}
