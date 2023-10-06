use std::{
    collections::{btree_map::Entry, BTreeMap},
    fs::File,
    io::{Read, Seek, SeekFrom},
    sync::{Arc, Mutex},
};

use anyhow::{anyhow, Result};

use crate::{btree::BTreePage, header::Header};

#[derive(Debug, Clone)]
pub struct DB {
    pub(crate) state: Arc<Mutex<DBState>>,
}

#[derive(Debug)]
pub(crate) struct DBState {
    file: File,
    pages: BTreeMap<u32, Box<[u8]>>,
    header: Header,
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
            state: Arc::new(Mutex::new(state)),
        })
    }

    pub fn root(&self) -> Result<BTreePage> {
        self.btree_at(1)
    }

    pub fn btree_at(&self, page_number: u32) -> Result<BTreePage> {
        let mut inner = self.state.lock().unwrap();
        let page = inner.page(page_number)?;

        Ok(BTreePage::new(self.clone(), page_number, page))
    }
}

impl DBState {
    pub(crate) fn page<'a>(&'a mut self, page_number: u32) -> Result<&'a mut [u8]> {
        fn inner(file: &mut File, header: &Header, page_number: u32) -> Result<Box<[u8]>> {
            if !(1..=header.database_size()).contains(&page_number) {
                return Err(anyhow!("page number out of bounds"));
            }

            let page_size = header.page_size();

            let mut page = vec![0; page_size as usize].into_boxed_slice();
            file.seek(SeekFrom::Start((page_number as u64 - 1) * page_size as u64))?;
            file.read_exact(&mut page)?;

            Ok(page)
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

#[cfg(test)]
mod tests {
    use crate::btree::BTreePageType;

    use super::*;

    #[test]
    fn test_open() {
        let db = DB::open("examples/empty.db").unwrap();
        assert_eq!(db.state.lock().unwrap().header.page_size(), 4096);
    }

    #[test]
    fn test_read_btree() {
        let db = DB::open("examples/empty.db").unwrap();

        let root = db.root().unwrap();
        assert_eq!(root.page_type(), BTreePageType::LeafTable);

        let cells: Vec<_> = root.table_leaf_cells().collect();
        assert_eq!(cells.len(), 1);
        assert_eq!(cells[0].0, 1);
    }
}
