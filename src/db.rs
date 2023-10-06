use std::{
    collections::{btree_map::Entry, BTreeMap},
    fs::File,
    io::{Read, Seek, SeekFrom},
    sync::{Arc, Mutex},
};

use anyhow::{anyhow, Result};

use crate::{
    btree::BTree,
    header::{Header, HEADER_SIZE},
};

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

        let header: Header = state.page_raw(1)?.as_ref().into();
        header.validate();

        Ok(Self {
            state: Arc::new(Mutex::new(state)),
        })
    }

    pub fn root(&self) -> Result<BTree> {
        self.btree_at(1)
    }

    pub fn btree_at(&self, page_number: u32) -> Result<BTree> {
        let mut inner = self.state.lock().unwrap();
        let page = inner.page(page_number)?;

        Ok(BTree::new(self.clone(), page_number, page))
    }
}

impl DBState {
    /// Gets the page data, including the header.
    fn page_raw<'a>(&'a mut self, page_number: u32) -> Result<&'a mut [u8]> {
        let entry = self.pages.entry(page_number);
        let page = match entry {
            Entry::Occupied(entry) => entry.into_mut(),
            Entry::Vacant(entry) => {
                if !(1..=self.header.database_size()).contains(&page_number) {
                    return Err(anyhow!("page number out of bounds"));
                }

                let page_size = self.header.page_size();

                let mut page = vec![0; page_size as usize].into_boxed_slice();
                self.file
                    .seek(SeekFrom::Start((page_number as u64 - 1) * page_size as u64))?;
                self.file.read_exact(&mut page)?;
                entry.insert(page)
            }
        };

        Ok(page)
    }

    /// Gets the page data, excluding the header (for page 1).
    pub(crate) fn page<'a>(&'a mut self, page_number: u32) -> Result<&'a mut [u8]> {
        let mut page = self.page_raw(page_number)?;
        if page_number == 1 {
            page = &mut page[HEADER_SIZE..];
        }
        Ok(page)
    }
}

#[cfg(test)]
mod tests {
    use crate::btree::BTreeRole;

    use super::*;

    #[test]
    fn test_open() {
        let _db = DB::open("examples/empty.db").unwrap();
    }

    #[test]
    fn test_read_btree() {
        let db = DB::open("examples/empty.db").unwrap();
        let root = db.root().unwrap();
        assert!(root.is_leaf());
        assert_eq!(root.role(), BTreeRole::Table);
    }
}
