// use crate::*;
use super::*;

mod connack;
mod connect;
mod disconnect;
mod ping;
mod puback;
mod pubcomp;
mod publish;
mod pubrec;
mod pubrel;
mod suback;
mod subscribe;
mod unsuback;
mod unsubscribe;

pub use connack::*;
pub use connect::*;
pub use disconnect::*;
pub use ping::*;
pub use puback::*;
pub use pubcomp::*;
pub use publish::*;
pub use pubrec::*;
pub use pubrel::*;
pub use suback::*;
pub use subscribe::*;
pub use unsuback::*;
pub use unsubscribe::*;

/// Encapsulates all MQTT packet types
#[derive(Debug, Clone, PartialEq)]
pub enum Packet {
    Connect(Connect),
    ConnAck(ConnAck),
    Publish(Publish),
    PubAck(PubAck),
    PubRec(PubRec),
    PubRel(PubRel),
    PubComp(PubComp),
    Subscribe(Subscribe),
    SubAck(SubAck),
    Unsubscribe(Unsubscribe),
    UnsubAck(UnsubAck),
    PingReq,
    PingResp,
    Disconnect(Disconnect),
}

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PropertyType {
    PayloadFormatIndicator = 1,
    MessageExpiryInterval = 2,
    ContentType = 3,
    ResponseTopic = 8,
    CorrelationData = 9,
    SubscriptionIdentifier = 11,
    SessionExpiryInterval = 17,
    AssignedClientIdentifier = 18,
    ServerKeepAlive = 19,
    AuthenticationMethod = 21,
    AuthenticationData = 22,
    RequestProblemInformation = 23,
    WillDelayInterval = 24,
    RequestResponseInformation = 25,
    ResponseInformation = 26,
    ServerReference = 28,
    ReasonString = 31,
    ReceiveMaximum = 33,
    TopicAliasMaximum = 34,
    TopicAlias = 35,
    MaximumQos = 36,
    RetainAvailable = 37,
    UserProperty = 38,
    MaximumPacketSize = 39,
    WildcardSubscriptionAvailable = 40,
    SubscriptionIdentifierAvailable = 41,
    SharedSubscriptionAvailable = 42,
}

fn property(num: u8) -> Result<PropertyType, Error> {
    let property = match num {
        1 => PropertyType::PayloadFormatIndicator,
        2 => PropertyType::MessageExpiryInterval,
        3 => PropertyType::ContentType,
        8 => PropertyType::ResponseTopic,
        9 => PropertyType::CorrelationData,
        11 => PropertyType::SubscriptionIdentifier,
        17 => PropertyType::SessionExpiryInterval,
        18 => PropertyType::AssignedClientIdentifier,
        19 => PropertyType::ServerKeepAlive,
        21 => PropertyType::AuthenticationMethod,
        22 => PropertyType::AuthenticationData,
        23 => PropertyType::RequestProblemInformation,
        24 => PropertyType::WillDelayInterval,
        25 => PropertyType::RequestResponseInformation,
        26 => PropertyType::ResponseInformation,
        28 => PropertyType::ServerReference,
        31 => PropertyType::ReasonString,
        33 => PropertyType::ReceiveMaximum,
        34 => PropertyType::TopicAliasMaximum,
        35 => PropertyType::TopicAlias,
        36 => PropertyType::MaximumQos,
        37 => PropertyType::RetainAvailable,
        38 => PropertyType::UserProperty,
        39 => PropertyType::MaximumPacketSize,
        40 => PropertyType::WildcardSubscriptionAvailable,
        41 => PropertyType::SubscriptionIdentifierAvailable,
        42 => PropertyType::SharedSubscriptionAvailable,
        num => return Err(Error::InvalidPropertyType(num)),
    };

    Ok(property)
}

/// Reads a stream of bytes and extracts next MQTT packet out of it
pub fn read(stream: &mut BytesMut, max_size: usize) -> Result<Packet> {
    let fixed_header = check(&stream[..], max_size)?;

    // Test with a stream with exactly the size to check border panics
    let packet = stream.split_to(fixed_header.frame_length());
    let packet_type = fixed_header.packet_type()?;

    if fixed_header.remaining_len == 0 {
        // no payload packets
        return match packet_type {
            PacketType::PingReq => Ok(Packet::PingReq),
            PacketType::PingResp => Ok(Packet::PingResp),
            _ => Err(anyhow::Error::from(Error::PayloadRequired)),
        };
    }

    let mut packet = packet.freeze();
    let packet = match packet_type {
        PacketType::Connect => {
            Packet::Connect(Connect::decode(Protocol::V5, &fixed_header, &mut packet)?)
        }
        PacketType::ConnAck => {
            Packet::ConnAck(ConnAck::decode(Protocol::V5, &fixed_header, &mut packet)?)
        }
        PacketType::Publish => {
            Packet::Publish(Publish::decode(Protocol::V5, &fixed_header, &mut packet)?)
        }
        PacketType::PubAck => {
            Packet::PubAck(PubAck::decode(Protocol::V5, &fixed_header, &mut packet)?)
        }
        PacketType::PubRec => {
            Packet::PubRec(PubRec::decode(Protocol::V5, &fixed_header, &mut packet)?)
        }
        PacketType::PubRel => {
            Packet::PubRel(PubRel::decode(Protocol::V5, &fixed_header, &mut packet)?)
        }
        PacketType::PubComp => {
            Packet::PubComp(PubComp::decode(Protocol::V5, &fixed_header, &mut packet)?)
        }
        PacketType::Subscribe => {
            Packet::Subscribe(Subscribe::decode(Protocol::V5, &fixed_header, &mut packet)?)
        }
        PacketType::SubAck => {
            Packet::SubAck(SubAck::decode(Protocol::V5, &fixed_header, &mut packet)?)
        }
        PacketType::Unsubscribe => Packet::Unsubscribe(Unsubscribe::decode(
            Protocol::V5,
            &fixed_header,
            &mut packet,
        )?),
        PacketType::UnsubAck => {
            Packet::UnsubAck(UnsubAck::decode(Protocol::V5, &fixed_header, &mut packet)?)
        }
        PacketType::PingReq => Packet::PingReq,
        PacketType::PingResp => Packet::PingResp,
        PacketType::Disconnect => Packet::Disconnect(Disconnect::decode(
            Protocol::V5,
            &fixed_header,
            &mut packet,
        )?),
    };

    Ok(packet)
}
