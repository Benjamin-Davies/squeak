pub fn read(bytes: &[u8]) -> (i64, usize) {
    let mut result = 0;
    let mut i = 0;

    loop {
        let byte = bytes[i];

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

    (result as i64, i + 1)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_read_varint() {
        assert_eq!(read(&[0x01]), (1, 1));
        assert_eq!(read(&[0x80, 0x40]), (64, 2));
        assert_eq!(read(&[0x80; 9]), (128, 9));
        assert_eq!(read(&[0xff; 9]), (-1, 9));
    }
}
