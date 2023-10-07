use crate::{buf::ArcBufSlice, varint};

use super::{SerialType, SerialValue};

pub struct SerialTypeIterator {
    header_len: u64,
    data: ArcBufSlice,
}

pub struct SerialValueIterator {
    types: SerialTypeIterator,
    data: ArcBufSlice,
}

impl SerialTypeIterator {
    pub(super) fn new(mut data: ArcBufSlice) -> Self {
        let (header_len, len) = varint::read(&data);
        data.truncate(header_len as usize);
        data.consume_bytes(len);
        Self { header_len, data }
    }
}

impl SerialValueIterator {
    pub(super) fn new(mut data: ArcBufSlice) -> Self {
        let types = SerialTypeIterator::new(data.clone());
        data.consume_bytes(types.header_len as usize);
        Self { types, data }
    }
}

impl Iterator for SerialTypeIterator {
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

impl Iterator for SerialValueIterator {
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
