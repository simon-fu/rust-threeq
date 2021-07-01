use bytes::Bytes;
use std::slice::Iter;

pub type FixedHeader = mqttbytes::FixedHeader;

pub type Connect = mqttbytes::v5::Connect;

pub type Publish = mqttbytes::v5::Publish;

pub type Packet = mqttbytes::v5::Packet;

pub type PacketType = mqttbytes::PacketType;

pub type Error = mqttbytes::Error;

pub fn decode_type(byte1: u8)-> Result<PacketType, Error>{
    let num = byte1 >> 4;
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


pub fn check(mut stream: Iter<u8>, max_packet_size: usize) -> Result<(PacketType, FixedHeader), Error> {
    // Create fixed header if there are enough bytes in the stream
    // to frame full packet
    let stream_len = stream.len();

    if stream_len < 2 {
        return Err(Error::InsufficientBytes(2 - stream_len));
    }

    let byte1 = stream.next().unwrap();
    let (len_len, len) = decode_len_len(stream)?;

    let packet_type = decode_type(*byte1)?;

    let fixed_header = FixedHeader::new(*byte1, len_len, len);

    // Don't let rogue connections attack with huge payloads.
    // Disconnect them before reading all that data
    if len > max_packet_size {
        return Err(Error::PayloadSizeLimitExceeded(len));
    }

    // If the current call fails due to insufficient bytes in the stream,
    // after calculating remaining length, we extend the stream
    let frame_length = fixed_header.frame_length();
    if stream_len < frame_length {
        return Err(Error::InsufficientBytes(frame_length - stream_len));
    }

    Ok((packet_type, fixed_header))
}


pub trait Decode<T> {
    fn decode(fix_header : FixedHeader, src: &mut Bytes) -> Result<T, Error>;
}

trait Printable {
    fn stringify(&self) -> String;
}

pub struct Dummy;


fn decode_dummy(fix_header : FixedHeader, src: &mut Bytes) -> Result<Dummy, Error>{
    return Err(Error::InvalidPacketType(99));
}

type Decoder<T> = fn (fix_header : FixedHeader, packet: Bytes) -> Result<T, Error>;

pub struct Decoders {
    pub connect : Decoder<Connect>,

    pub publish : Decoder<Publish>,

}


pub type PacketDecoder = fn(stream: &mut Bytes, max_size: usize) -> Result<Packet, Error>;

pub mod v4;

// pub mod v5;