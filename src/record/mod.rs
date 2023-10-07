use std::{fmt, iter};

use zerocopy::big_endian::{F64, I16, I32, I64};

use crate::{buf::ArcBufSlice, varint};

use self::ints::{I24, I48};

pub mod ints;
pub mod serialization;

#[derive(Clone, PartialEq, Eq)]
pub struct Record {
    data: ArcBufSlice,
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

impl From<ArcBufSlice> for Record {
    fn from(data: ArcBufSlice) -> Self {
        Self { data }
    }
}

impl Record {
    pub fn serial_types(&self) -> impl Iterator<Item = SerialType> {
        let mut data = self.data.clone();
        let header_len = data.consume_varint();
        let content_len = self.data.len() - header_len as usize;

        iter::from_fn(move || {
            if data.len() <= content_len {
                return None;
            }

            let type_ = data.consume_varint();
            Some(type_.into())
        })
    }

    pub fn columns(&self) -> impl Iterator<Item = SerialValue> {
        let mut data = self.data.clone();
        let (header_len, _) = varint::read(&data);
        data.consume_bytes(header_len as usize);

        self.serial_types().scan(data, |data, ty| {
            let value = SerialValue::consume(ty, data);
            Some(value)
        })
    }
}

impl fmt::Debug for Record {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Record")
            .field("columns", &self.columns().collect::<Vec<_>>())
            .finish()
    }
}

impl From<u64> for SerialType {
    fn from(value: u64) -> Self {
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
            n if n % 2 == 0 => Self::Blob((n - 12) / 2),
            n => Self::Text((n - 13) / 2),
        }
    }
}

impl SerialValue {
    pub fn consume(ty: SerialType, data: &mut ArcBufSlice) -> Self {
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
}

#[cfg(test)]
mod tests {
    use crate::buf::ArcBuf;

    use super::*;

    const EXAMPLE_RECORD: &[u8] = &[
        6, 23, 23, 23, 1, 117, 116, 97, 98, 108, 101, 101, 109, 112, 116, 121, 101, 109, 112, 116,
        121, 2, 67, 82, 69, 65, 84, 69, 32, 84, 65, 66, 76, 69, 32, 101, 109, 112, 116, 121, 32,
        40, 105, 100, 32, 105, 110, 116, 101, 103, 101, 114, 32, 110, 111, 116, 32, 110, 117, 108,
        108, 32, 112, 114, 105, 109, 97, 114, 121, 32, 107, 101, 121, 41,
    ];

    #[test]
    fn test_read_types() {
        let data: ArcBuf = EXAMPLE_RECORD.to_vec().into();
        let record = Record::from(ArcBufSlice::from(data));

        let types = record.serial_types().collect::<Vec<_>>();
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
        let data: ArcBuf = EXAMPLE_RECORD.to_vec().into();
        let record = Record::from(ArcBufSlice::from(data));

        let columns = record.columns().collect::<Vec<_>>();
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
