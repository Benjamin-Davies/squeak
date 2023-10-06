pub(crate) fn read(mut bytes: &[u8]) -> (u64, &[u8]) {
    let mut result = 0;
    let mut i = 0;

    loop {
        let byte = bytes[0];
        bytes = &bytes[1..];

        if i >= 8 {
            result <<= 8;
            result |= byte as u64;
            break;
        }

        result <<= 7;
        result |= (byte & 0x7f) as u64;
        if byte & 0x80 == 0 {
            break;
        }

        i += 1;
    }

    (result, bytes)
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    #[test]
    fn test_read_varint() {
        assert_eq!(read(&[0x01]), (1, &[] as &[u8]));
        assert_eq!(read(&[0x80, 0x40]), (64, &[] as &[u8]));
        assert_eq!(read(&[0x80; 9]), (128, &[] as &[u8]));
        assert_eq!(read(&[0xff; 9]), (u64::MAX, &[] as &[u8]));
    }
}
