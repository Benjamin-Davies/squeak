use crate::physical::{buf::Buf, varint};

use super::{SerialType, SerialValue};

pub struct SerialTypeIterator<'a> {
    header_len: u64,
    data: &'a [u8],
}

pub struct SerialValueIterator<'a> {
    types: SerialTypeIterator<'a>,
    data: &'a [u8],
}

impl<'a> SerialTypeIterator<'a> {
    pub(super) fn new(mut data: &'a [u8]) -> Self {
        let (header_len, len) = varint::read(data);
        data.truncate(header_len as usize);
        data.consume_bytes(len);
        Self { header_len, data }
    }
}

impl<'a> SerialValueIterator<'a> {
    pub(super) fn new(mut data: &'a [u8]) -> Self {
        let types = SerialTypeIterator::new(data);
        data.consume_bytes(types.header_len as usize);
        Self { types, data }
    }
}

impl<'a> Iterator for SerialTypeIterator<'a> {
    type Item = SerialType;

    fn next(&mut self) -> Option<Self::Item> {
        if !self.data.is_empty() {
            let ty = self.data.consume_varint();
            Some(SerialType::from(ty))
        } else {
            None
        }
    }
}

impl<'a> Iterator for SerialValueIterator<'a> {
    type Item = SerialValue;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(ty) = self.types.next() {
            let value = SerialValue::consume(ty, &mut self.data);
            Some(value)
        } else {
            None
        }
    }
}
