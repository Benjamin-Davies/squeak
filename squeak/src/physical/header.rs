use std::{
    array,
    io::{Read, Seek, SeekFrom},
};

use anyhow::Result;
use zerocopy::{
    big_endian::{I32, U32},
    little_endian, FromBytes,
};

const HEADER_STRING: [u8; 16] = *b"SQLite format 3\0";
const SQLITE_VERSION_NUMBER: u32 = 3_042_000;
pub const HEADER_SIZE: usize = 100;

#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    zerocopy::FromZeroes,
    zerocopy::FromBytes,
    zerocopy::AsBytes,
    zerocopy::Unaligned,
)]
#[repr(C)]
pub struct Header {
    /// The header string: "SQLite format 3\0"
    header_string: [u8; 16],
    /// The database page size as a multiple of 256 bytes. Must be a power of two between 512 and 65536 bytes inclusive.
    page_size: little_endian::U16,
    /// File format write version. 1 for legacy; 2 for WAL.
    write_version: u8,
    /// File format read version. 1 for legacy; 2 for WAL.
    read_version: u8,
    /// Bytes of unused "reserved" space at the end of each page. Usually 0.
    reserved_space: u8,
    /// Maximum embedded payload fraction. Must be 64.
    max_payload_fraction: u8,
    /// Minimum embedded payload fraction. Must be 32.
    min_payload_fraction: u8,
    /// Leaf payload fraction. Must be 32.
    leaf_payload_fraction: u8,
    /// File change counter.
    file_change_counter: U32,
    /// Size of the database file in pages. The "in-header database size".
    database_size: U32,
    /// Page number of the first freelist trunk page.
    freelist_head: U32,
    /// Total number of freelist pages.
    freelist_count: U32,
    /// The schema cookie.
    schema_cookie: U32,
    /// The schema format number. Supported schema formats are 1, 2, 3, and 4.
    schema_format: U32,
    /// Default page cache size.
    page_cache_size: I32,
    /// The page number of the largest root b-tree page when in auto-vacuum or incremental-vacuum modes, or zero otherwise.
    largest_root_btree_page_number: U32,
    /// The database text encoding. A value of 1 means UTF-8. A value of 2 means UTF-16le. A value of 3 means UTF-16be.
    database_text_encoding: U32,
    /// The user version.
    user_version: U32,
    /// True (non-zero) for incremental-vacuum mode. False (zero) otherwise.
    incremental_vacuum_mode: U32,
    /// The application ID.
    application_id: U32,
    /// Reserved for expansion. Must be zero.
    reserved: [u8; 20],
    /// The version-valid-for number.
    version_valid_for: U32,
    /// The SQLite version number.
    sqlite_version_number: U32,
}

impl Default for Header {
    fn default() -> Self {
        Self {
            header_string: HEADER_STRING,
            // https://www.sqlite.org/pgszchng2016.html
            // 4096 is the default page size for SQLite 3.12.0 and later.
            // 16 * 256 = 4096
            page_size: 16.into(),
            write_version: 1,
            read_version: 1,
            reserved_space: 0,
            max_payload_fraction: 64,
            min_payload_fraction: 32,
            leaf_payload_fraction: 32,
            file_change_counter: 1.into(),
            database_size: 1.into(),
            freelist_head: 0.into(),
            freelist_count: 0.into(),
            schema_cookie: 0.into(),
            schema_format: 4.into(),
            page_cache_size: 0.into(),
            largest_root_btree_page_number: 0.into(),
            database_text_encoding: 1.into(),
            user_version: 0.into(),
            incremental_vacuum_mode: 0.into(),
            application_id: 0.into(),
            reserved: array::from_fn(|_| 0),
            version_valid_for: 0.into(),
            sqlite_version_number: SQLITE_VERSION_NUMBER.into(),
        }
    }
}

impl Header {
    pub(crate) fn read<R: Read + Seek>(mut reader: R) -> Result<Self> {
        let mut bytes = [0; HEADER_SIZE];
        reader.seek(SeekFrom::Start(0))?;
        reader.read_exact(&mut bytes)?;

        let header = Self::read_from(&bytes).unwrap();
        header.validate();
        Ok(header)
    }

    pub(crate) fn validate(&self) {
        assert_eq!(self.header_string, HEADER_STRING);

        let page_size = self.page_size();
        assert!(page_size.is_power_of_two());
        assert!((512..=65536).contains(&page_size));

        assert_eq!(self.write_version, 1);
        assert_eq!(self.read_version, 1);
        assert_eq!(self.reserved_space, 0);
        assert_eq!(self.max_payload_fraction, 64);
        assert_eq!(self.min_payload_fraction, 32);
        assert_eq!(self.leaf_payload_fraction, 32);
        assert_eq!(self.schema_format.get(), 4);
        assert_eq!(self.largest_root_btree_page_number.get(), 0);
        assert_eq!(self.database_text_encoding.get(), 1);
        assert_eq!(self.incremental_vacuum_mode.get(), 0);
    }

    pub(crate) fn page_size(&self) -> u32 {
        self.page_size.get() as u32 * 256
    }

    pub(crate) fn file_change_counter(&self) -> u32 {
        self.file_change_counter.get()
    }

    pub(crate) fn database_size(&self) -> u32 {
        self.database_size.get()
    }

    pub(crate) fn set_database_size(&mut self, database_size: u32) {
        self.database_size.set(database_size);
    }

    pub(crate) fn freelist_head(&self) -> u32 {
        self.freelist_head.get()
    }

    pub(crate) fn set_freelist_head(&mut self, freelist_head: u32) {
        self.freelist_head.set(freelist_head);
    }

    pub(crate) fn freelist_count(&self) -> u32 {
        self.freelist_count.get()
    }

    pub(crate) fn set_freelist_count(&mut self, freelist_count: u32) {
        self.freelist_count.set(freelist_count);
    }
}
