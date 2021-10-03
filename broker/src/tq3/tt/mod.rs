use super::tbytes;
use anyhow::Result;
use std::{num::Wrapping, slice::Iter};

pub mod client;

// pub mod config;

pub mod topic;

pub mod mqtree;

pub type Error = tbytes::Error;

pub type Protocol = tbytes::Protocol;

pub type FixedHeader = tbytes::FixedHeader;

pub type PacketType = tbytes::PacketType;

pub type QoS = tbytes::QoS;

// use bytes::Buf;
pub use tbytes::v5::*;

// pub type Message = Publish;

#[derive(Debug, Clone)]
pub struct Message {
    pub id: u64,
    pub packet: Publish,
}

impl Message {
    pub fn new(id: u64, packet: Publish) -> Self {
        Self { id, packet }
    }
}

pub fn decode_len_len(stream: Iter<u8>) -> Result<(usize, usize), Error> {
    let mut len: usize = 0;
    let mut len_len = 0;
    let mut done = false;
    let mut shift = 0;

    // Use continuation bit at position 7 to continue reading next
    // byte to frame 'length'.
    // Stream 0b1xxx_xxxx 0b1yyy_yyyy 0b1zzz_zzzz 0b0www_wwww will
    // be framed as number 0bwww_wwww_zzz_zzzz_yyy_yyyy_xxx_xxxx
    for byte in stream {
        len_len += 1;
        let byte = *byte as usize;
        len += (byte & 0x7F) << shift;

        // stop when continue bit is 0
        done = (byte & 0x80) == 0;
        if done {
            break;
        }

        shift += 7;

        // Only a max of 4 bytes allowed for remaining length
        // more than 4 shifts (0, 7, 14, 21) implies bad length
        if shift > 21 {
            return Err(Error::MalformedRemainingLength);
        }
    }

    // Not enough bytes to frame remaining length. wait for
    // one more byte
    if !done {
        return Err(Error::InsufficientBytes(1));
    }

    Ok((len_len, len))
}

pub fn check(mut stream: Iter<u8>, max_packet_size: usize) -> Result<FixedHeader, Error> {
    // Create fixed header if there are enough bytes in the stream
    // to frame full packet
    let stream_len = stream.len();

    if stream_len < 2 {
        return Err(Error::InsufficientBytes(2 - stream_len));
    }

    let byte1 = stream.next().unwrap();
    let (len_len, len) = decode_len_len(stream)?;

    // Don't let rogue connections attack with huge payloads.
    // Disconnect them before reading all that data
    if len > max_packet_size {
        return Err(Error::PayloadSizeLimitExceeded(len));
    }

    // If the current call fails due to insufficient bytes in the stream,
    // after calculating remaining length, we extend the stream
    let frame_length = len_len + 1 + len;
    if stream_len < frame_length {
        return Err(Error::InsufficientBytes(frame_length - stream_len));
    }

    // let packet_type = decode_type(*byte1)?;

    Ok(FixedHeader::new(*byte1, len_len, len))
}

#[derive(Debug)]
pub struct PacketId(Wrapping<u16>);

impl Default for PacketId {
    #[inline(always)]
    fn default() -> Self {
        Self(Wrapping(0))
    }
}

impl Iterator for PacketId {
    type Item = u16;
    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        self.0 += Wrapping(1);
        if self.0 .0 == 0 {
            self.0 .0 = 1;
        }
        Some(self.0 .0)
    }
}

impl PacketId {
    #[inline(always)]
    fn get(&self) -> u16 {
        self.0 .0
    }
}

// pub trait Decoder: Sized {
//     fn decode<B: Buf + ExactSizeIterator>(buf: &mut B) -> Result<Self>;
// }

// // pub trait Decoder: Sized {
// //     fn decode<B: Buf>(protocol: Protocol, fixed_header: FixedHeader, buf: &mut B) -> Result<Self>;
// // }

// pub trait Encoder: Sized {
//     fn encode<B: Buf>(buf: &mut B) -> Result<Self>;
// }
