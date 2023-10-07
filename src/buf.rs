use std::{fmt, ops::Deref, sync::Arc};

use crate::varint;

pub type ArcBuf = Arc<[u8]>;

#[derive(Clone)]
pub struct ArcBufSlice {
    buf: ArcBuf,
    start: usize,
    end: usize,
}

impl ArcBufSlice {
    pub fn consume_bytes(&mut self, count: usize) -> &[u8] {
        let consume_to = self.start + count;
        debug_assert!(consume_to <= self.end);
        let bytes = &self.buf[self.start..consume_to];

        self.start = consume_to;
        bytes
    }

    pub fn consume_varint(&mut self) -> u64 {
        let (result, len) = varint::read(&self);
        self.consume_bytes(len);
        result
    }

    pub fn truncate(&mut self, new_len: usize) {
        let new_end = self.start + new_len;
        assert!(new_end <= self.end);
        self.end = new_end;
    }
}

impl From<ArcBuf> for ArcBufSlice {
    fn from(buf: ArcBuf) -> Self {
        let len = buf.len();
        Self {
            buf,
            start: 0,
            end: len,
        }
    }
}

impl Deref for ArcBufSlice {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.buf[self.start..self.end]
    }
}

impl PartialEq for ArcBufSlice {
    fn eq(&self, other: &Self) -> bool {
        self.deref() == other.deref()
    }
}

impl Eq for ArcBufSlice {}

impl fmt::Debug for ArcBufSlice {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("ArcBufSlice").field(&*self).finish()
    }
}
