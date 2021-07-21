use super::tbytes;
use std::slice::Iter;

pub mod client;

pub mod config;

pub mod topic;

pub type Error = tbytes::Error;

pub type Protocol = tbytes::Protocol;

pub type FixedHeader = tbytes::FixedHeader;

pub type PacketType = tbytes::PacketType;

pub type QoS = tbytes::QoS;

pub type Message = Publish;

// pub type Packet = tbytes::v5::Packet;

// pub type Connect = tbytes::v5::Connect;

// pub type ConnAck = tbytes::v5::ConnAck;

// pub type Publish = tbytes::v5::Publish;

// pub type PubAck = tbytes::v5::PubAck;

// pub type PubRec = tbytes::v5::PubRec;

// pub type PubRel = tbytes::v5::PubRel;

// pub type PubComp = tbytes::v5::PubComp;

// pub type Subscribe = tbytes::v5::Subscribe;

// pub type SubAck = tbytes::v5::SubAck;

// pub type Unsubscribe = tbytes::v5::Unsubscribe;

// pub type UnsubAck = tbytes::v5::UnsubAck;

// pub type PingReq = tbytes::v5::PingReq;

// pub type PingResp = tbytes::v5::PingResp;

// pub type Disconnect = tbytes::v5::Disconnect;

// pub type ConnectReturnCode = tbytes::v5::ConnectReturnCode;

// pub type ConnAckProperties = tbytes::v5::ConnAckProperties;

// pub type SubscribeReasonCode = tbytes::v5::SubscribeReasonCode;

// pub type UnsubAckReason = tbytes::v5::UnsubAckReason;

// pub type ConnectProperties = tbytes::v5::ConnectProperties;

// pub type LastWill = tbytes::v5::LastWill;

pub use tbytes::v5::*;

// pub fn decode_protocol_level(buf : & BytesMut) -> std::io::Result<tbytes::Protocol>{
//     // Connect Packet
//     //      byte 0: packet-type:4bit, reserved:4bit
//     //      byte 1: Remaining Length
//     //      byte 2~3: 4
//     //      byte 4~7: 'MQTT'
//     //      byte 8: level
//     if buf.len() < 9 {
//         let error = format!("parsing protocol level: too short {}", buf.len());
//             return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, error));
//     }

//     let protocol_level = buf[8];
//     match protocol_level {
//         4 => return Ok(tbytes::Protocol::V4),
//         5 => return Ok(tbytes::Protocol::V5),
//         num => {
//             let error = format!("unknown mqtt protocol level {}", num);
//             return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, error));
//         },
//     };
// }

// pub fn decode_type(byte1: u8)-> Result<PacketType, Error>{
//     let num = byte1 >> 4;
//     match num {
//         1 => Ok(PacketType::Connect),
//         2 => Ok(PacketType::ConnAck),
//         3 => Ok(PacketType::Publish),
//         4 => Ok(PacketType::PubAck),
//         5 => Ok(PacketType::PubRec),
//         6 => Ok(PacketType::PubRel),
//         7 => Ok(PacketType::PubComp),
//         8 => Ok(PacketType::Subscribe),
//         9 => Ok(PacketType::SubAck),
//         10 => Ok(PacketType::Unsubscribe),
//         11 => Ok(PacketType::UnsubAck),
//         12 => Ok(PacketType::PingReq),
//         13 => Ok(PacketType::PingResp),
//         14 => Ok(PacketType::Disconnect),
//         _ => Err(Error::InvalidPacketType(num)),
//     }
// }

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
