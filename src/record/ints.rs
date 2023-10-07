use std::fmt;

#[derive(Clone, Copy, PartialEq, Eq, zerocopy::FromZeroes, zerocopy::FromBytes)]
pub struct I24([u8; 3]);

#[derive(Clone, Copy, PartialEq, Eq, zerocopy::FromZeroes, zerocopy::FromBytes)]
pub struct I48([u8; 6]);

impl I24 {
    pub fn get(&self) -> i32 {
        let bytes = self.0;
        let sign_extend = if bytes[0] & 0x80 == 0 { 0 } else { 0xff };
        i32::from_be_bytes([sign_extend, bytes[0], bytes[1], bytes[2]])
    }
}

impl I48 {
    pub fn get(&self) -> i64 {
        let bytes = self.0;
        let sign_extend = if bytes[0] & 0x80 == 0 { 0 } else { 0xff };
        i64::from_be_bytes([
            sign_extend,
            sign_extend,
            bytes[0],
            bytes[1],
            bytes[2],
            bytes[3],
            bytes[4],
            bytes[5],
        ])
    }
}

impl fmt::Debug for I24 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("I24").field(&self.get()).finish()
    }
}

impl fmt::Debug for I48 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("I24").field(&self.get()).finish()
    }
}
