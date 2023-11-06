use std::{
    collections::{btree_map::Entry, BTreeMap},
    fmt,
    fs::File,
    io::{Read, Seek, SeekFrom},
    sync::Mutex,
};

use anyhow::{anyhow, Result};

use crate::physical::{btree::BTreePage, buf::ArcBuf, header::Header};

pub struct DB {
    pub(super) state: Mutex<DBState>,
}

#[derive(Debug)]
pub(crate) struct DBState {
    pub(super) file: File,
    pub(super) pages: BTreeMap<u32, ArcBuf>,
    pub(super) header: Header,
}

impl DB {
    pub fn open(path: &str) -> Result<Self> {
        let file = File::open(path)?;

        let mut state = DBState {
            file,
            pages: BTreeMap::new(),
            header: Header::default(),
        };

        let header: Header = state.page(1)?.as_ref().into();
        header.validate();
        state.header = header;

        Ok(Self {
            state: Mutex::new(state),
        })
    }

    pub(crate) fn btree_page(&self, page_number: u32) -> Result<BTreePage> {
        let mut inner = self.state.lock().unwrap();
        let page = inner.page(page_number)?;

        Ok(BTreePage::new(self, page_number, page.clone().into()))
    }
}

impl DBState {
    pub(crate) fn page(&mut self, page_number: u32) -> Result<&ArcBuf> {
        fn inner(file: &mut File, header: &Header, page_number: u32) -> Result<ArcBuf> {
            if !(1..=header.database_size()).contains(&page_number) {
                return Err(anyhow!("page number out of bounds"));
            }

            let page_size = header.page_size();

            let mut page = vec![0; page_size as usize];
            file.seek(SeekFrom::Start((page_number as u64 - 1) * page_size as u64))?;
            file.read_exact(&mut page)?;

            Ok(page.into())
        }

        let entry = self.pages.entry(page_number);
        let page = match entry {
            Entry::Occupied(entry) => {
                let page = entry.into_mut();
                if page.len() != self.header.page_size() as usize {
                    *page = inner(&mut self.file, &self.header, page_number)?;
                }
                page
            }
            Entry::Vacant(entry) => {
                let page = inner(&mut self.file, &self.header, page_number)?;
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
        assert_eq!(db.state.lock().unwrap().header.page_size(), 4096);
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
