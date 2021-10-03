// use crate::*;
use super::*;

use bytes::{BufMut, BytesMut};

#[derive(Debug)]
pub struct PingReq;

impl PingReq {
    pub fn write(&self, payload: &mut BytesMut) -> Result<usize, Error> {
        payload.put_slice(&[0xC0, 0x00]);
        Ok(2)
    }
}

#[derive(Debug)]
pub struct PingResp;

impl PingResp {
    pub fn write(&self, payload: &mut BytesMut) -> Result<usize, Error> {
        payload.put_slice(&[0xD0, 0x00]);
        Ok(2)
    }
}
