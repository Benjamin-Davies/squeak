use std::{
    iter, mem,
    ops::{Deref, DerefMut},
};

use crate::physical::varint;

pub trait Buf: Deref<Target = [u8]> {
    fn consume_bytes(&mut self, count: usize) -> &[u8];
    fn truncate(&mut self, new_len: usize);

    fn consume_varint(&mut self) -> i64 {
        let (result, len) = varint::read(self);
        self.consume_bytes(len);
        result
    }

    fn consume<T: zerocopy::FromBytes>(&mut self) -> T {
        let bytes = self.consume_bytes(mem::size_of::<T>());
        T::read_from(bytes).unwrap()
    }
}

pub trait BufMut: DerefMut<Target = [u8]> + Extend<u8> {
    fn write_varint(&mut self, value: i64) {
        let mut buf = [0; 10];
        let len = varint::write(value, &mut buf);
        self.extend(buf[..len].iter().copied());
    }

    fn write<T: zerocopy::AsBytes>(&mut self, value: T) {
        self.extend(value.as_bytes().iter().copied());
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

impl BufMut for Vec<u8> {}
