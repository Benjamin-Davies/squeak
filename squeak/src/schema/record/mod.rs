use std::fmt;

use zerocopy::big_endian::{F64, I16, I32, I64};

use crate::physical::buf::{Buf, BufMut};

use self::{
    ints::{I24, I48},
    iter::{SerialTypeIterator, SerialValueIterator},
};

pub mod ints;
pub mod iter;

#[derive(Clone, Copy, Default, PartialEq, Eq)]
pub struct Record<'a> {
    data: &'a [u8],
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SerialType {
    Null,
    I8,
    I16,
    I24,
    I32,
    I48,
    I64,
    F64,
    Zero,
    One,
    Blob(u64),
    Text(u64),
}

#[derive(Debug, Clone, PartialEq)]
pub enum SerialValue {
    Null,
    I8(i8),
    I16(I16),
    I24(I24),
    I32(I32),
    I48(I48),
    I64(I64),
    F64(F64),
    Zero,
    One,
    Blob(Vec<u8>),
    Text(String),
}

impl<'a> From<&'a [u8]> for Record<'a> {
    fn from(data: &'a [u8]) -> Self {
        Self { data }
    }
}

impl<'a> Record<'a> {
    pub fn types(self) -> SerialTypeIterator<'a> {
        SerialTypeIterator::new(self.data)
    }

    pub fn values(self) -> SerialValueIterator<'a> {
        SerialValueIterator::new(self.data)
    }
}

impl<'a> fmt::Debug for Record<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Record")
            .field("columns", &self.values().collect::<Vec<_>>())
            .finish()
    }
}

impl From<i64> for SerialType {
    fn from(value: i64) -> Self {
        match value {
            0 => Self::Null,
            1 => Self::I8,
            2 => Self::I16,
            3 => Self::I24,
            4 => Self::I32,
            5 => Self::I48,
            6 => Self::I64,
            7 => Self::F64,
            8 => Self::Zero,
            9 => Self::One,
            10 | 11 => panic!("encountered internal use column type"),
            n if n % 2 == 0 => Self::Blob((n - 12) as u64 / 2),
            n => Self::Text((n - 13) as u64 / 2),
        }
    }
}

impl From<SerialType> for i64 {
    fn from(value: SerialType) -> Self {
        match value {
            SerialType::Null => 0,
            SerialType::I8 => 1,
            SerialType::I16 => 2,
            SerialType::I24 => 3,
            SerialType::I32 => 4,
            SerialType::I48 => 5,
            SerialType::I64 => 6,
            SerialType::F64 => 7,
            SerialType::Zero => 8,
            SerialType::One => 9,
            SerialType::Blob(n) => n as i64 * 2 + 12,
            SerialType::Text(n) => n as i64 * 2 + 13,
        }
    }
}

impl SerialValue {
    pub fn consume(ty: SerialType, data: &mut &[u8]) -> Self {
        match ty {
            SerialType::Null => Self::Null,
            SerialType::I8 => Self::I8(data.consume()),
            SerialType::I16 => Self::I16(data.consume()),
            SerialType::I24 => Self::I24(data.consume()),
            SerialType::I32 => Self::I32(data.consume()),
            SerialType::I48 => Self::I48(data.consume()),
            SerialType::I64 => Self::I64(data.consume()),
            SerialType::F64 => Self::F64(data.consume()),
            SerialType::Zero => Self::Zero,
            SerialType::One => Self::One,
            SerialType::Blob(n) => Self::Blob(data.consume_bytes(n as usize).to_vec()),
            SerialType::Text(n) => {
                Self::Text(String::from_utf8(data.consume_bytes(n as usize).to_vec()).unwrap())
            }
        }
    }

    pub fn serial_type(&self) -> SerialType {
        match self {
            Self::Null => SerialType::Null,
            Self::I8(_) => SerialType::I8,
            Self::I16(_) => SerialType::I16,
            Self::I24(_) => SerialType::I24,
            Self::I32(_) => SerialType::I32,
            Self::I48(_) => SerialType::I48,
            Self::I64(_) => SerialType::I64,
            Self::F64(_) => SerialType::F64,
            Self::Zero => SerialType::Zero,
            Self::One => SerialType::One,
            Self::Blob(value) => SerialType::Blob(value.len() as u64),
            Self::Text(value) => SerialType::Text(value.len() as u64),
        }
    }

    pub fn write(&self, result: &mut impl BufMut) {
        match self {
            Self::Null | Self::Zero | Self::One => {}
            &Self::I8(value) => result.write(value),
            &Self::I16(value) => result.write(value),
            &Self::I24(value) => result.write(value),
            &Self::I32(value) => result.write(value),
            &Self::I48(value) => result.write(value),
            &Self::I64(value) => result.write(value),
            &Self::F64(value) => result.write(value),
            Self::Blob(value) => result.extend(value.iter().copied()),
            Self::Text(value) => result.extend(value.bytes()),
        }
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    const EXAMPLE_RECORD: &[u8] = &[
        6, 23, 23, 23, 1, 117, 116, 97, 98, 108, 101, 101, 109, 112, 116, 121, 101, 109, 112, 116,
        121, 2, 67, 82, 69, 65, 84, 69, 32, 84, 65, 66, 76, 69, 32, 101, 109, 112, 116, 121, 32,
        40, 105, 100, 32, 105, 110, 116, 101, 103, 101, 114, 32, 110, 111, 116, 32, 110, 117, 108,
        108, 32, 112, 114, 105, 109, 97, 114, 121, 32, 107, 101, 121, 41,
    ];

    #[test]
    fn test_read_types() {
        let record = Record::from(EXAMPLE_RECORD);

        let types = record.types().collect::<Vec<_>>();
        assert_eq!(
            types,
            vec![
                SerialType::Text(5),
                SerialType::Text(5),
                SerialType::Text(5),
                SerialType::I8,
                SerialType::Text(52),
            ]
        );
    }

    #[test]
    fn test_read_columns() {
        let record = Record::from(EXAMPLE_RECORD);

        let columns = record.values().collect::<Vec<_>>();
        assert_eq!(
            columns,
            vec![
                SerialValue::Text("table".to_owned()),
                SerialValue::Text("empty".to_owned()),
                SerialValue::Text("empty".to_owned()),
                SerialValue::I8(2),
                SerialValue::Text(
                    "CREATE TABLE empty (id integer not null primary key)".to_owned()
                ),
            ]
        );
    }
}
