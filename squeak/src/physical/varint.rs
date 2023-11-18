use serde::de::value;

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

pub fn write(value: i64, bytes: &mut [u8]) -> usize {
    let bits_needed = i64::BITS - value.abs().leading_zeros() + 1;
    let bytes_needed = (bits_needed as usize + 7) / 7;
    let value = value as u64;

    if bytes_needed >= 9 {
        for i in 0..9 {
            let j = 8 - i;

            if j == 0 {
                bytes[i] = value as u8;
            } else {
                bytes[i] = ((value >> (7 * j + 1)) & 0x7f) as u8 | 0x80;
            }
        }

        9
    } else {
        for i in 0..bytes_needed {
            let j = bytes_needed - i - 1;

            if j == 0 {
                bytes[i] = (value & 0x7f) as u8;
            } else {
                bytes[i] = ((value >> (7 * j)) & 0x7f) as u8 | 0x80;
            }
        }

        bytes_needed
    }
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
        assert_eq!(
            read(&[0xd5, 0xaa, 0xd5, 0xaa, 0xd5, 0xaa, 0xd5, 0xaa, 0xaa]),
            (0xaaaa_aaaa_aaaa_aaaau64 as i64, 9)
        );
    }

    #[test]
    fn test_write_varint() {
        let mut bytes = [0; 9];

        assert_eq!(write(1, &mut bytes), 1);
        assert_eq!(&bytes[..1], [0x01]);

        assert_eq!(write(64, &mut bytes), 2);
        assert_eq!(&bytes[..2], [0x80, 0x40]);

        assert_eq!(write(128, &mut bytes), 2);
        assert_eq!(&bytes[..2], [0x81, 0x00]);

        assert_eq!(write(-1, &mut bytes), 1);
        assert_eq!(&bytes[..1], [0x7f]);

        assert_eq!(write(-128, &mut bytes), 2);
        assert_eq!(&bytes[..2], [0xff, 0x00]);

        assert_eq!(write(0xaaaa_aaaa_aaaa_aaaau64 as i64, &mut bytes), 9);
        assert_eq!(
            bytes,
            [0xd5, 0xaa, 0xd5, 0xaa, 0xd5, 0xaa, 0xd5, 0xaa, 0xaa]
        );
    }
}
