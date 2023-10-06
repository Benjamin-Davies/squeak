use std::{
    fs::File,
    io::{Read, Seek, SeekFrom},
};

use anyhow::Result;
use zerocopy::FromBytes;

use crate::header::Header;

pub struct DBFile {
    file: File,
    header: Header,
}

impl DBFile {
    pub fn open(path: &str) -> Result<Self> {
        let file = File::open(path)?;

        let mut db_file = Self {
            file,
            header: Header::default(),
        };

        db_file.header = db_file.read_page(1)?;
        db_file.header.validate();

        Ok(db_file)
    }

    pub fn read_page_raw(&mut self, n: u32) -> Result<Box<[u8]>> {
        assert_ne!(n, 0, "page number must be non-zero");
        assert!(
            n <= self.header.database_size(),
            "page number must be less than or equal to the database size"
        );
        let page_size = self.header.page_size();

        let mut page = vec![0; page_size as usize].into_boxed_slice();
        self.file
            .seek(SeekFrom::Start((n as u64 - 1) * page_size as u64))?;
        self.file.read_exact(&mut page)?;

        Ok(page)
    }

    pub fn read_page<T: FromBytes>(&mut self, n: u32) -> Result<T> {
        let page = self.read_page_raw(n)?;
        Ok(T::read_from_prefix(&page).expect("type is larger than page size"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_open() {
        let mut _db_file = DBFile::open("examples/empty.db").unwrap();
    }
}
