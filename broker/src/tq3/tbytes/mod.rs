extern crate alloc;

use anyhow::Result;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use core::fmt::{self, Display, Formatter};

mod topic;
pub mod v4;
pub mod v5;

pub use topic::*;

/// Error during serialization and deserialization
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum Error {
    NotConnect(PacketType),
    UnexpectedConnect,
    InvalidConnectReturnCode(u8),
    InvalidReason(u8),
    InvalidProtocol,
    InvalidProtocolLevel(u8),
    IncorrectPacketFormat,
    InvalidPacketType(u8),
    InvalidPropertyType(u8),
    InvalidRetainForwardRule(u8),
    InvalidQoS(u8),
    InvalidSubscribeReasonCode(u8),
    PacketIdZero,
    SubscriptionIdZero,
    PayloadSizeIncorrect,
    PayloadTooLong,
    PayloadSizeLimitExceeded(usize),
    PayloadRequired,
    TopicNotUtf8,
    BoundaryCrossed(usize),
    MalformedPacket,
    MalformedRemainingLength,
    /// More bytes required to frame packet. Argument
    /// implies minimum additional bytes required to
    /// proceed further
    InsufficientBytes(usize),
}

/// MQTT packet type
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, num_enum::TryFromPrimitive)]
pub enum PacketType {
    Connect = 1,
    ConnAck,
    Publish,
    PubAck,
    PubRec,
    PubRel,
    PubComp,
    Subscribe,
    SubAck,
    Unsubscribe,
    UnsubAck,
    PingReq,
    PingResp,
    Disconnect,
}

/// Protocol type
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
pub enum Protocol {
    V4 = 4,
    V5 = 5,
}

impl Default for Protocol {
    fn default() -> Self {
        Protocol::V4
    }
}

impl Protocol {
    pub fn from_u8(num: u8) -> Result<Self, Error> {
        match num {
            4 => Ok(Protocol::V4),
            5 => Ok(Protocol::V5),
            num => return Err(Error::InvalidProtocolLevel(num)),
        }
    }
}

/// Quality of service
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Deserialize, Serialize)]
pub enum QoS {
    AtMostOnce = 0,
    AtLeastOnce = 1,
    ExactlyOnce = 2,
}

impl Default for QoS {
    fn default() -> Self {
        QoS::AtMostOnce
    }
}

impl std::str::FromStr for QoS {
    type Err = String;

    fn from_str(input: &str) -> Result<QoS, Self::Err> {
        match input {
            "QoS0" => Ok(QoS::AtMostOnce),
            "QoS1" => Ok(QoS::AtLeastOnce),
            "QoS2" => Ok(QoS::ExactlyOnce),
            s => Err(format!("Unknown QoS {}", s)),
        }
    }
}

/// Packet type from a byte
///
/// ```ignore
///          7                          3                          0
///          +--------------------------+--------------------------+
/// byte 1   | MQTT Control Packet Type | Flags for each type      |
///          +--------------------------+--------------------------+
///          |         Remaining Bytes Len  (1/2/3/4 bytes)        |
///          +-----------------------------------------------------+
///
/// http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html#_Figure_2.2_-
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd)]
pub struct FixedHeader {
    /// First byte of the stream. Used to identify packet types and
    /// several flags
    byte1: u8,
    /// Length of fixed header. Byte 1 + (1..4) bytes. So fixed header
    /// len can vary from 2 bytes to 5 bytes
    /// 1..4 bytes are variable length encoded to represent remaining length
    fixed_header_len: usize,
    /// Remaining length of the packet. Doesn't include fixed header bytes
    /// Represents variable header + payload size
    remaining_len: usize,
}

impl FixedHeader {
    pub fn new(byte1: u8, remaining_len_len: usize, remaining_len: usize) -> FixedHeader {
        FixedHeader {
            byte1,
            fixed_header_len: remaining_len_len + 1,
            remaining_len,
        }
    }

    pub fn get_type_byte(&self) -> u8 {
        self.byte1 >> 4
    }

    pub fn packet_type(&self) -> Result<PacketType, Error> {
        let num = self.byte1 >> 4;
        match num {
            1 => Ok(PacketType::Connect),
            2 => Ok(PacketType::ConnAck),
            3 => Ok(PacketType::Publish),
            4 => Ok(PacketType::PubAck),
            5 => Ok(PacketType::PubRec),
            6 => Ok(PacketType::PubRel),
            7 => Ok(PacketType::PubComp),
            8 => Ok(PacketType::Subscribe),
            9 => Ok(PacketType::SubAck),
            10 => Ok(PacketType::Unsubscribe),
            11 => Ok(PacketType::UnsubAck),
            12 => Ok(PacketType::PingReq),
            13 => Ok(PacketType::PingResp),
            14 => Ok(PacketType::Disconnect),
            _ => Err(Error::InvalidPacketType(num)),
        }
    }

    /// Returns the size of full packet (fixed header + variable header + payload)
    /// Fixed header is enough to get the size of a frame in the stream
    pub fn frame_length(&self) -> usize {
        self.fixed_header_len + self.remaining_len
    }
}

// /// Checks if the stream has enough bytes to frame a packet and returns fixed header
// /// only if a packet can be framed with existing bytes in the `stream`.
// /// The passed stream doesn't modify parent stream's cursor. If this function
// /// returned an error, next `check` on the same parent stream is forced start
// /// with cursor at 0 again (Iter is owned. Only Iter's cursor is changed internally)
// pub fn check(stream: Iter<u8>, max_packet_size: usize) -> Result<FixedHeader, Error> {
//     // Create fixed header if there are enough bytes in the stream
//     // to frame full packet
//     let stream_len = stream.len();
//     let fixed_header = parse_fixed_header(stream)?;

//     // Don't let rogue connections attack with huge payloads.
//     // Disconnect them before reading all that data
//     if fixed_header.remaining_len > max_packet_size {
//         return Err(Error::PayloadSizeLimitExceeded(fixed_header.remaining_len));
//     }

//     // If the current call fails due to insufficient bytes in the stream,
//     // after calculating remaining length, we extend the stream
//     let frame_length = fixed_header.frame_length();
//     if stream_len < frame_length {
//         return Err(Error::InsufficientBytes(frame_length - stream_len));
//     }

//     Ok(fixed_header)
// }

// /// Parses fixed header
// pub fn parse_fixed_header(mut stream: Iter<u8>) -> Result<FixedHeader, Error> {
//     // At least 2 bytes are necessary to frame a packet
//     let stream_len = stream.len();
//     if stream_len < 2 {
//         return Err(Error::InsufficientBytes(2 - stream_len));
//     }

//     let byte1 = stream.next().unwrap();
//     let (len_len, len) = length(stream)?;

//     Ok(FixedHeader::new(*byte1, len_len, len))
// }

/// Checks if the stream has enough bytes to frame a packet and returns fixed header
/// only if a packet can be framed with existing bytes in the `stream`.
/// The passed stream doesn't modify parent stream's cursor. If this function
/// returned an error, next `check` on the same parent stream is forced start
/// with cursor at 0 again (Iter is owned. Only Iter's cursor is changed internally)
pub fn check<B: Buf>(stream: B, max_packet_size: usize) -> Result<FixedHeader, Error> {
    // Create fixed header if there are enough bytes in the stream
    // to frame full packet
    let stream_len = stream.remaining();
    let fixed_header = parse_fixed_header(stream)?;

    // Don't let rogue connections attack with huge payloads.
    // Disconnect them before reading all that data
    if fixed_header.remaining_len > max_packet_size {
        return Err(Error::PayloadSizeLimitExceeded(fixed_header.remaining_len));
    }

    // If the current call fails due to insufficient bytes in the stream,
    // after calculating remaining length, we extend the stream
    let frame_length = fixed_header.frame_length();
    if stream_len < frame_length {
        return Err(Error::InsufficientBytes(frame_length - stream_len));
    }

    Ok(fixed_header)
}

/// Parses fixed header
pub fn parse_fixed_header<B: Buf>(mut stream: B) -> Result<FixedHeader, Error> {
    // At least 2 bytes are necessary to frame a packet
    let stream_len = stream.remaining();
    if stream_len < 2 {
        return Err(Error::InsufficientBytes(2 - stream_len));
    }

    let byte1 = stream.get_u8();
    let (len_len, len) = parse_length(&mut stream)?;

    Ok(FixedHeader::new(byte1, len_len, len))
}

// /// Parses variable byte integer in the stream and returns the length
// /// and number of bytes that make it. Used for remaining length calculation
// /// as well as for calculating property lengths
// fn length(stream: Iter<u8>) -> Result<(usize, usize), Error> {
//     let mut len: usize = 0;
//     let mut len_len = 0;
//     let mut done = false;
//     let mut shift = 0;

//     // Use continuation bit at position 7 to continue reading next
//     // byte to frame 'length'.
//     // Stream 0b1xxx_xxxx 0b1yyy_yyyy 0b1zzz_zzzz 0b0www_wwww will
//     // be framed as number 0bwww_wwww_zzz_zzzz_yyy_yyyy_xxx_xxxx
//     for byte in stream {
//         len_len += 1;
//         let byte = *byte as usize;
//         len += (byte & 0x7F) << shift;

//         // stop when continue bit is 0
//         done = (byte & 0x80) == 0;
//         if done {
//             break;
//         }

//         shift += 7;

//         // Only a max of 4 bytes allowed for remaining length
//         // more than 4 shifts (0, 7, 14, 21) implies bad length
//         if shift > 21 {
//             return Err(Error::MalformedRemainingLength);
//         }
//     }

//     // Not enough bytes to frame remaining length. wait for
//     // one more byte
//     if !done {
//         return Err(Error::InsufficientBytes(1));
//     }

//     Ok((len_len, len))
// }

/// Parses variable byte integer in the stream and returns the length
/// and number of bytes that make it. Used for remaining length calculation
/// as well as for calculating property lengths
fn parse_length<B: Buf>(stream: &mut B) -> Result<(usize, usize), Error> {
    let mut len: usize = 0;
    let mut len_len = 0;
    let mut done = false;
    let mut shift = 0;

    // Use continuation bit at position 7 to continue reading next
    // byte to frame 'length'.
    // Stream 0b1xxx_xxxx 0b1yyy_yyyy 0b1zzz_zzzz 0b0www_wwww will
    // be framed as number 0bwww_wwww_zzz_zzzz_yyy_yyyy_xxx_xxxx
    while stream.has_remaining() {
        let byte = stream.get_u8();
        len_len += 1;
        let byte = byte as usize;
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

// /// Reads a series of bytes with a length from a byte stream
// fn read_mqtt_bytes(stream: &mut Bytes) -> Result<Bytes, Error> {
//     let len = read_u16(stream)? as usize;

//     // Prevent attacks with wrong remaining length. This method is used in
//     // `packet.assembly()` with (enough) bytes to frame packet. Ensures that
//     // reading variable len string or bytes doesn't cross promised boundary
//     // with `read_fixed_header()`
//     if len > stream.len() {
//         return Err(Error::BoundaryCrossed(len));
//     }

//     Ok(stream.split_to(len))
// }

// /// Reads a string from bytes stream
// fn read_mqtt_string(stream: &mut Bytes) -> Result<String, Error> {
//     let s = read_mqtt_bytes(stream)?;
//     match String::from_utf8(s.to_vec()) {
//         Ok(v) => Ok(v),
//         Err(_e) => Err(Error::TopicNotUtf8),
//     }
// }

/// Reads a series of bytes with a length from a byte stream
fn read_mqtt_bytes<B: Buf>(stream: &mut B) -> Result<Bytes, Error> {
    let len = read_u16(stream)? as usize;

    // Prevent attacks with wrong remaining length. This method is used in
    // `packet.assembly()` with (enough) bytes to frame packet. Ensures that
    // reading variable len string or bytes doesn't cross promised boundary
    // with `read_fixed_header()`
    if len > stream.remaining() {
        return Err(Error::BoundaryCrossed(len));
    }

    Ok(stream.copy_to_bytes(len))
}

/// Reads a string from bytes stream
fn read_mqtt_string<B: Buf>(stream: &mut B) -> Result<String, Error> {
    let s = read_mqtt_bytes(stream)?;
    match String::from_utf8(s.to_vec()) {
        Ok(v) => Ok(v),
        Err(_e) => Err(Error::TopicNotUtf8),
    }

    // let len = read_u16(stream)? as usize;
    // if len > stream.remaining() {
    //     return Err(Error::BoundaryCrossed(len));
    // }

    // let s = stream.copy_to_bytes(len);
    // match String::from_utf8(s.to_vec()) {
    //     Ok(v) => Ok(v),
    //     Err(_e) => Err(Error::TopicNotUtf8),
    // }
}

/// Serializes bytes to stream (including length)
fn write_mqtt_bytes(stream: &mut BytesMut, bytes: &[u8]) {
    stream.put_u16(bytes.len() as u16);
    stream.extend_from_slice(bytes);
}

/// Serializes a string to stream
fn write_mqtt_string(stream: &mut BytesMut, string: &str) {
    write_mqtt_bytes(stream, string.as_bytes());
}

/// Writes remaining length to stream and returns number of bytes for remaining length
fn write_remaining_length(stream: &mut BytesMut, len: usize) -> Result<usize, Error> {
    if len > 268_435_455 {
        return Err(Error::PayloadTooLong);
    }

    let mut done = false;
    let mut x = len;
    let mut count = 0;

    while !done {
        let mut byte = (x % 128) as u8;
        x /= 128;
        if x > 0 {
            byte |= 128;
        }

        stream.put_u8(byte);
        count += 1;
        done = x == 0;
    }

    Ok(count)
}

/// Return number of remaining length bytes required for encoding length
fn len_len(len: usize) -> usize {
    if len >= 2_097_152 {
        4
    } else if len >= 16_384 {
        3
    } else if len >= 128 {
        2
    } else {
        1
    }
}

/// Maps a number to QoS
pub fn qos(num: u8) -> Result<QoS, Error> {
    match num {
        0 => Ok(QoS::AtMostOnce),
        1 => Ok(QoS::AtLeastOnce),
        2 => Ok(QoS::ExactlyOnce),
        qos => Err(Error::InvalidQoS(qos)),
    }
}

/// After collecting enough bytes to frame a packet (packet's frame())
/// , It's possible that content itself in the stream is wrong. Like expected
/// packet id or qos not being present. In cases where `read_mqtt_string` or
/// `read_mqtt_bytes` exhausted remaining length but packet framing expects to
/// parse qos next, these pre checks will prevent `bytes` crashes
fn read_u16<B: Buf>(stream: &mut B) -> Result<u16, Error> {
    if stream.remaining() < 2 {
        return Err(Error::MalformedPacket);
    }

    Ok(stream.get_u16())
}

fn read_u8<B: Buf>(stream: &mut B) -> Result<u8, Error> {
    if stream.remaining() < 1 {
        return Err(Error::MalformedPacket);
    }

    Ok(stream.get_u8())
}

fn read_u32<B: Buf>(stream: &mut B) -> Result<u32, Error> {
    if stream.remaining() < 4 {
        return Err(Error::MalformedPacket);
    }

    Ok(stream.get_u32())
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "Error = {:?}", self)
    }
}

// pub trait SizedBuf: Buf+ExactSizeIterator{}
// impl<T:Buf+ExactSizeIterator> SizedBuf for T {}

pub trait PacketDecoder: Sized {
    fn decode<B: Buf>(protocol: Protocol, fixed_header: &FixedHeader, buf: &mut B) -> Result<Self>;
}

pub trait PacketEncoder: Sized {
    fn encode<B: BufMut>(protocol: Protocol, buf: &mut B) -> Result<()>;
}
