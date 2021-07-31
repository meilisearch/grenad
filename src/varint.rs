pub fn varint_length_packed(data: &[u8]) -> u32 {
    let mut i = 0;
    for _ in 0..data.len() {
        if (data[i] & 0x80) == 0 {
            break;
        }
        i += 1;
    }
    if i == data.len() {
        0
    } else {
        i as u32 + 1
    }
}

#[must_use]
pub fn varint_encode32(bytes: &mut [u8], value: u32) -> &[u8] {
    let b = 128;

    if value < (1 << 7) {
        bytes[0] = value as u8;
        &bytes[..1]
    } else if value < (1 << 14) {
        bytes[0] = (value | b) as u8;
        bytes[1] = (value >> 7) as u8;
        &bytes[..2]
    } else if value < (1 << 21) {
        bytes[0] = (value | b) as u8;
        bytes[1] = ((value >> 7) | b) as u8;
        bytes[2] = (value >> 14) as u8;
        &bytes[..3]
    } else if value < (1 << 28) {
        bytes[0] = (value | b) as u8;
        bytes[1] = ((value >> 7) | b) as u8;
        bytes[2] = ((value >> 14) | b) as u8;
        bytes[3] = (value >> 21) as u8;
        &bytes[..4]
    } else {
        bytes[0] = (value | b) as u8;
        bytes[1] = ((value >> 7) | b) as u8;
        bytes[2] = ((value >> 14) | b) as u8;
        bytes[3] = ((value >> 21) | b) as u8;
        bytes[4] = (value >> 28) as u8;
        &bytes[..5]
    }
}

pub fn varint_decode32(data: &[u8], value: &mut u32) -> usize {
    let len = varint_length_packed(&data[..data.len().min(5)]);
    let mut val = (data[0] & 0x7f) as u32;
    if len > 1 {
        val |= ((data[1] & 0x7f) as u32) << 7;
        if len > 2 {
            val |= ((data[2] & 0x7f) as u32) << 14;
            if len > 3 {
                val |= ((data[3] & 0x7f) as u32) << 21;
                if len > 4 {
                    val |= (data[4] as u32) << 28;
                }
            }
        }
    }
    *value = val;
    len as usize
}

#[cfg(test)]
mod tests {
    use super::*;

    quickcheck! {
        fn qc_codec_u32(num: u32) -> bool {
            let mut buf = [0; 10];
            let mut val = 0;
            let buf = varint_encode32(&mut buf, num);
            varint_decode32(buf, &mut val);

            num == val
        }
    }
}
