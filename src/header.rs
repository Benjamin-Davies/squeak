use zerocopy::{big_endian::U32, little_endian, FromZeroes};

const HEADER_STRING: [u8; 16] = *b"SQLite format 3\0";

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
    // The rest of the header is irrelevant for our purposes.
}

impl Default for Header {
    fn default() -> Self {
        Self {
            header_string: HEADER_STRING,
            page_size: 2.into(),
            database_size: 1.into(),
            ..FromZeroes::new_zeroed()
        }
    }
}

impl Header {
    pub fn validate(&self) {
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
    }

    pub fn page_size(&self) -> u32 {
        self.page_size.get() as u32 * 256
    }

    pub fn database_size(&self) -> u32 {
        self.database_size.get()
    }
}
