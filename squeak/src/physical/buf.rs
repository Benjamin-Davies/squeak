use std::{mem, ops::Deref};

use crate::physical::varint;

pub trait Buf: Deref<Target = [u8]> {
    fn consume_bytes(&mut self, count: usize) -> &[u8];
    fn truncate(&mut self, new_len: usize);

    fn consume_varint(&mut self) -> u64 {
        let (result, len) = varint::read(self);
        self.consume_bytes(len);
        result
    }

    fn consume<T: zerocopy::FromBytes>(&mut self) -> T {
        let bytes = self.consume_bytes(mem::size_of::<T>());
        T::read_from(bytes).unwrap()
    }
}

impl Buf for &[u8] {
    fn consume_bytes(&mut self, count: usize) -> &[u8] {
        let (result, rest) = self.split_at(count);
        *self = rest;
        result
    }

    fn truncate(&mut self, new_len: usize) {
        *self = &(*self)[..new_len];
    }
}
