use bytes::{Buf, BufMut, Bytes, BytesMut};

#[derive(Debug, Clone, Copy)]
pub struct Header {
    pub pubid: usize,
    pub ts: i64,
    pub seq: u64,
    pub max_seq: u64,
}

impl Header {
    pub fn new(pubid: usize) -> Self {
        Self {
            pubid,
            ts: 0,
            seq: 0,
            max_seq: 0,
        }
    }

    pub fn decode(buf: &mut Bytes) -> Self {
        Self {
            pubid: buf.get_u32() as usize,
            ts: buf.get_i64(),
            seq: buf.get_u64(),
            max_seq: buf.get_u64(),
        }
    }
}

pub fn encode_binary(header: &Header, content: &[u8], padding_to_size: usize, buf: &mut BytesMut) {
    let len = 24 + content.len();

    buf.reserve(len);

    buf.put_u32(header.pubid as u32);
    buf.put_i64(header.ts);
    buf.put_u64(header.seq);
    buf.put_u64(header.max_seq);
    buf.put_u32(content.len() as u32);
    if content.len() > 0 {
        buf.put(content);
    }

    if len < padding_to_size {
        let remaining = padding_to_size - len;
        buf.reserve(remaining);
        unsafe {
            buf.advance_mut(remaining);
        }
    }
}
