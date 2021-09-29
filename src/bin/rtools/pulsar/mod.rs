use crate::pulsar::pulsar_util::TopicConsumerBuilder;
use anyhow::{bail, Context, Result};
use bytes::{Buf, Bytes};
use chrono::{DateTime, Local, TimeZone};
use clap::{Clap, ValueHint};
use enumflags2::{bitflags, BitFlags};
use futures::TryStreamExt;
use log::{debug, info};
use prost::Message;
use pulsar::{DeserializeMessage, Payload, Pulsar, TokioExecutor};
use regex::Regex;
use rust_threeq::{
    here,
    tq3::{tbytes::PacketDecoder, tt},
};
use std::time::{Duration, SystemTime};

// use async_trait::async_trait;
use self::pulsar_util::{Admin, EntryIndex, TopicParts};

mod pulsar_util;

#[derive(Clap, Debug, Clone)]
pub struct ReadArgs {
    #[clap(long = "url", long_about = "pulsar broker url. ", default_value = "pulsar://127.0.0.1:6650", value_hint = ValueHint::Url,)]
    url: String,

    #[clap(long = "rest", long_about = "pulsar admin url. ", default_value = "http://127.0.0.1:8080", value_hint = ValueHint::Url,)]
    rest: String,

    #[clap(
        long = "topic",
        long_about = "pulsar topic, for example persistent://em/default/ev0"
    )]
    topic: String,

    #[clap(
        long = "begin",
        long_about = "optional, begin position to read from, one of formats:\n1. pulsar message id in format of {ledger_id}:{entry_id}\n2. time format, for example 2021-09-20T12:35:57"
    )]
    begin: Option<PosBegin>,

    #[clap(long = "num", long_about = "optional, max messages to read")]
    num: Option<u64>,

    #[clap(
        long = "end-time",
        long_about = "optional, end position in time format, for example 2021-09-20T12:35:57"
    )]
    end_time: Option<TimeArg>,

    #[clap(long = "match-msgid", long_about = "optional, full match message id")]
    match_msgid: Option<u64>,

    #[clap(
        long = "match-connid",
        long_about = "optional, regex match connection id"
    )]
    match_connid: Option<u64>,

    #[clap(
        long = "match-clientid",
        long_about = "optional, regex match client id"
    )]
    match_clientid: Option<RegexArg>,

    #[clap(long = "match-user", long_about = "optional, regex match user name")]
    match_user: Option<RegexArg>,

    #[clap(long = "match-topic", long_about = "optional, regex match topic")]
    match_topic: Option<RegexArg>,

    #[clap(
        long = "match-pl-text",
        long_about = "optional, regex match payload text"
    )]
    match_pl_text: Option<RegexArg>,
}

const ARG_TIME_FORMAT: &'static str = "%Y-%m-%dT%H:%M:%S";

#[derive(Debug, PartialEq, Clone)]
enum PosBegin {
    Index(EntryIndex),
    Timestamp(TimeArg),
}

impl std::str::FromStr for PosBegin {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if let Ok(ei) = EntryIndex::from_str(s) {
            return Ok(Self::Index(ei));
        }

        if let Ok(t) = TimeArg::from_str(s) {
            return Ok(Self::Timestamp(t));
        }
        bail!("can't parse begin postion [{}]", s);
    }
}

#[derive(Debug, PartialEq, Clone)]
struct TimeArg(u64);

impl std::str::FromStr for TimeArg {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let t = Local.datetime_from_str(s, ARG_TIME_FORMAT)?;
        Ok(Self(t.timestamp_millis() as u64))
    }
}

#[derive(Debug, Clone)]
struct RegexArg(Regex);

impl std::str::FromStr for RegexArg {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(Regex::new(s)?))
    }
}

mod msg {
    include!(concat!(env!("OUT_DIR"), "/mqtt.data.rs"));
}

type Consumer = pulsar::Consumer<Data, TokioExecutor>;

trait Decoder<T: Sized> {
    fn decode<B: Buf>(ver: u8, buf: &mut B) -> Result<T>;
}

struct DMqttMessage<RT: AgentField<MT>, MT> {
    raw: RawAgent<RT>,
    proto: tt::Protocol,
    packet: MT,
}

impl<RT: prost::Message + Default + AgentField<MT>, MT: PacketDecoder> Decoder<Self>
    for DMqttMessage<RT, MT>
{
    fn decode<B: Buf>(ver: u8, buf: &mut B) -> Result<Self> {
        let raw = RT::decode(buf)?;
        let packet = raw.decode_field(ver)?;
        Ok(Self {
            raw: RawAgent::<RT>(raw),
            proto: tt::Protocol::from_u8(ver)?,
            packet,
        })
    }
}

impl<RT: AgentDebug + AgentField<MT>, MT: std::fmt::Debug> std::fmt::Debug
    for DMqttMessage<RT, MT>
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("")
            .field("raw", &self.raw)
            .field("proto", &self.proto)
            .field("packet", &self.packet)
            .finish()
    }
}

struct RawAgent<T>(T);
impl<T: AgentDebug> std::fmt::Debug for RawAgent<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        T::debug_fmt(&self.0, f)
    }
}

pub trait AgentDebug {
    fn debug_fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result;
}

pub trait AgentField<T> {
    fn header(&self) -> &Option<msg::ControlHeaderInfo>;
    fn decode_field(&self, ver: u8) -> Result<T>;
}

impl AgentDebug for msg::UplinkMessage {
    fn debug_fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UplinkMessage")
            .field("header", &self.header)
            .field("code", &self.code)
            .finish()
    }
}

impl AgentField<tt::Publish> for msg::UplinkMessage {
    fn header(&self) -> &Option<msg::ControlHeaderInfo> {
        &self.header
    }

    fn decode_field(&self, ver: u8) -> Result<tt::Publish> {
        let mqtt_pkt: tt::Publish = parse_mqtt_packet(ver, &self.packet)?;
        Ok(mqtt_pkt)
    }
}

impl AgentDebug for msg::DownlinkMessage {
    fn debug_fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DownlinkMessage")
            .field("header", &self.header)
            .field("code", &self.code)
            .field("size", &self.size)
            .finish()
    }
}

impl AgentField<tt::Publish> for msg::DownlinkMessage {
    fn header(&self) -> &Option<msg::ControlHeaderInfo> {
        &self.header
    }

    fn decode_field(&self, ver: u8) -> Result<tt::Publish> {
        let mqtt_pkt: tt::Publish = parse_mqtt_packet(ver, &self.packet)?;
        Ok(mqtt_pkt)
    }
}

impl AgentDebug for msg::Subscription {
    fn debug_fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Subscription")
            .field("header", &self.header)
            .field("codes", &self.codes)
            .field("count", &self.count)
            .finish()
    }
}

impl AgentField<tt::Subscribe> for msg::Subscription {
    fn header(&self) -> &Option<msg::ControlHeaderInfo> {
        &self.header
    }

    fn decode_field(&self, ver: u8) -> Result<tt::Subscribe> {
        let mqtt_pkt: tt::Subscribe = parse_mqtt_packet(ver, &self.packet)?;
        Ok(mqtt_pkt)
    }
}

impl AgentDebug for msg::UnSubscription {
    fn debug_fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UnSubscription")
            .field("header", &self.header)
            .field("codes", &self.codes)
            .field("count", &self.count)
            .finish()
    }
}

impl AgentField<tt::Unsubscribe> for msg::UnSubscription {
    fn header(&self) -> &Option<msg::ControlHeaderInfo> {
        &self.header
    }

    fn decode_field(&self, ver: u8) -> Result<tt::Unsubscribe> {
        let mqtt_pkt: tt::Unsubscribe = parse_mqtt_packet(ver, &self.packet)?;
        Ok(mqtt_pkt)
    }
}

impl AgentDebug for msg::Connection {
    fn debug_fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Connection")
            .field("header", &self.header)
            .field("oss", &self.oss)
            .field("code", &self.code)
            .field("network", &self.network)
            .field("ip", &self.ip)
            .finish()
    }
}

impl AgentField<tt::Connect> for msg::Connection {
    fn header(&self) -> &Option<msg::ControlHeaderInfo> {
        &self.header
    }

    fn decode_field(&self, ver: u8) -> Result<tt::Connect> {
        let mqtt_pkt: tt::Connect = parse_mqtt_packet(ver, &self.packet)?;
        Ok(mqtt_pkt)
    }
}

impl AgentDebug for msg::Disconnection {
    fn debug_fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Disconnection")
            .field("header", &self.header)
            .field("code", &self.code)
            .finish()
    }
}

impl AgentField<tt::Disconnect> for msg::Disconnection {
    fn header(&self) -> &Option<msg::ControlHeaderInfo> {
        &self.header
    }

    fn decode_field(&self, ver: u8) -> Result<tt::Disconnect> {
        let mqtt_pkt: tt::Disconnect = parse_mqtt_packet(ver, &self.packet)?;
        Ok(mqtt_pkt)
    }
}

#[derive(Debug)]
enum Data {
    ULink(DMqttMessage<msg::UplinkMessage, tt::Publish>),
    DLink(DMqttMessage<msg::DownlinkMessage, tt::Publish>),
    Sub(DMqttMessage<msg::Subscription, tt::Subscribe>),
    Unsub(DMqttMessage<msg::UnSubscription, tt::Unsubscribe>),
    Conn(DMqttMessage<msg::Connection, tt::Connect>),
    Disconn(DMqttMessage<msg::Disconnection, tt::Disconnect>),
    Close(u8, msg::SessionClose),
    Malformed(u8, msg::MalformedPackage),
    Unknown(i32),
}

// impl Data {
//     fn encode(&self) -> Result<Vec<u8>> {
//         let mut buf = BytesMut::new();
//         match self {
//             // Data::ULink(v, m) => {
//             //     msg::Events::Uplink.encode(*v, &mut buf);
//             //     m.encode(&mut buf)?;
//             // }
//             Data::ULink(d) => {
//                 d.encode(&mut buf)?;
//             }
//             Data::DLink(v, m) => {
//                 msg::Events::Downlink.encode(*v, &mut buf);
//                 m.encode(&mut buf)?;
//             }
//             Data::Sub(v, m) => {
//                 msg::Events::Subscription.encode(*v, &mut buf);
//                 m.encode(&mut buf)?;
//             }
//             Data::Unsub(v, m) => {
//                 msg::Events::Unsubscription.encode(*v, &mut buf);
//                 m.encode(&mut buf)?;
//             }
//             Data::Conn(v, m) => {
//                 msg::Events::Connection.encode(*v, &mut buf);
//                 m.encode(&mut buf)?;
//             }
//             Data::Disconn(v, m) => {
//                 msg::Events::Disconnection.encode(*v, &mut buf);
//                 m.encode(&mut buf)?;
//             }
//             Data::Close(v, m) => {
//                 msg::Events::Close.encode(*v, &mut buf);
//                 m.encode(&mut buf)?;
//             }
//             Data::Malformed(v, m) => {
//                 msg::Events::Malformed.encode(*v, &mut buf);
//                 m.encode(&mut buf)?;
//             }
//             Data::Unknown(m) => {
//                 bail!(format!("unknown type {}", m));
//             }
//         }
//         Ok(buf.to_vec())
//     }
// }

// impl SerializeMessage for Data {
//     fn serialize_message(input: Self) -> Result<producer::Message, pulsar::Error> {
//         let r = input.encode();
//         match r {
//             Ok(v) => Ok(producer::Message {
//                 payload: v,
//                 ..Default::default()
//             }),
//             Err(e) => Err(pulsar::Error::Custom(e.to_string())),
//         }
//     }
// }

impl DeserializeMessage for Data {
    type Output = Result<Data>;

    fn deserialize_message(payload: &Payload) -> Self::Output {
        let mut stream = payload.data.iter();
        let ver = *stream.next().unwrap();

        let mut dw: i32 = 0;
        dw |= (*stream.next().unwrap() as i32) << 16;
        dw |= (*stream.next().unwrap() as i32) << 8;
        dw |= (*stream.next().unwrap() as i32) << 0;

        let ptype = msg::Events::from_i32(dw);
        if ptype.is_none() {
            return Ok(Self::Unknown(dw));
        }
        let ev = ptype.unwrap();

        let mut bytes = &payload.data[4..];
        match ev {
            msg::Events::Uplink => Ok(Self::ULink(DMqttMessage::decode(ver, &mut bytes)?)),

            msg::Events::Downlink => Ok(Self::DLink(DMqttMessage::decode(ver, &mut bytes)?)),

            msg::Events::Subscription => Ok(Self::Sub(DMqttMessage::decode(ver, &mut bytes)?)),

            msg::Events::Unsubscription => Ok(Self::Unsub(DMqttMessage::decode(ver, &mut bytes)?)),

            msg::Events::Connection => Ok(Self::Conn(DMqttMessage::decode(ver, &mut bytes)?)),

            msg::Events::Disconnection => Ok(Self::Disconn(DMqttMessage::decode(ver, &mut bytes)?)),

            msg::Events::Close => Ok(Self::Close(
                ver,
                msg::SessionClose::decode(bytes).context(here!())?,
            )),
            msg::Events::Malformed => Ok(Self::Malformed(
                ver,
                msg::MalformedPackage::decode(bytes).context(here!())?,
            )),
        }
    }
}

fn parse_mqtt_packet<P: PacketDecoder>(ver: u8, packet: &Vec<u8>) -> Result<P> {
    let protocol = tt::Protocol::from_u8(ver)?;
    let h = tt::check(packet.iter(), 65536).context(here!())?;
    if h.frame_length() != packet.len() {
        bail!(
            "inconsist mqtt packet len, expect {}, but {}",
            h.frame_length(),
            packet.len()
        );
    }
    let mut buf = &packet[..];
    P::decode(protocol, &h, &mut buf)
}

#[bitflags]
#[repr(u8)]
#[derive(Copy, Clone, Debug, PartialEq)]
enum AMatchs {
    ClientId = 0b000001,
    MsgId = 0b000010,
    PayloadText = 0b000100,
    ConnId = 0b001000,
    Topic = 0b010000,
    User = 0b100000,
}

fn check_bytes_text_match(
    arg: &Option<RegexArg>,
    bytes: &Bytes,
    mask: AMatchs,
    flags: &mut BitFlags<AMatchs>,
) {
    if let Some(m) = arg {
        let r = String::from_utf8(bytes.to_vec());
        if let Ok(text) = r {
            if m.0.is_match(&text) {
                *flags |= mask
            }
        }
    }
}

fn check_text_match(
    arg: &Option<RegexArg>,
    text: &str,
    mask: AMatchs,
    flags: &mut BitFlags<AMatchs>,
) {
    if let Some(m) = arg {
        if m.0.is_match(text) {
            *flags |= mask
        }
    }
}

fn check_matchs(
    args: &ReadArgs,
    header: &Option<msg::ControlHeaderInfo>,
    flags: &mut BitFlags<AMatchs>,
) {
    if header.is_none() {
        return;
    }

    let header = header.as_ref().unwrap();

    if let Some(m) = &args.match_clientid {
        if let Some(cid) = &header.clientid {
            if m.0.is_match(cid) {
                *flags |= AMatchs::ClientId
            }
        }
    }

    if let Some(m) = &args.match_user {
        if let Some(cid) = &header.user {
            if m.0.is_match(cid) {
                *flags |= AMatchs::User
            }
        }
    }

    if let Some(m) = &args.match_msgid {
        if let Some(cid) = &header.msgid {
            if *m == *cid {
                *flags |= AMatchs::MsgId
            }
        }
    }

    if let Some(m) = &args.match_connid {
        if *m == header.connid {
            *flags |= AMatchs::ConnId
        }
    }
}

fn handle_msg(args: &ReadArgs, data: &Data, flags: &mut BitFlags<AMatchs>) -> Result<msg::Events> {
    match data {
        Data::ULink(d) => {
            check_text_match(&args.match_topic, &d.packet.topic, AMatchs::Topic, flags);
            check_bytes_text_match(
                &args.match_pl_text,
                &d.packet.payload,
                AMatchs::PayloadText,
                flags,
            );
            check_matchs(args, &d.raw.0.header, flags);
            Ok(msg::Events::Uplink)
        }

        Data::DLink(d) => {
            check_text_match(&args.match_topic, &d.packet.topic, AMatchs::Topic, flags);
            check_bytes_text_match(
                &args.match_pl_text,
                &d.packet.payload,
                AMatchs::PayloadText,
                flags,
            );
            check_matchs(args, &d.raw.0.header, flags);
            Ok(msg::Events::Downlink)
        }

        Data::Sub(d) => {
            for f in &d.packet.filters {
                check_text_match(&args.match_topic, &f.path, AMatchs::Topic, flags);
            }
            check_matchs(args, &d.raw.0.header, flags);
            Ok(msg::Events::Downlink)
        }

        Data::Unsub(d) => {
            for text in &d.packet.filters {
                check_text_match(&args.match_topic, text, AMatchs::Topic, flags);
            }
            check_matchs(args, &d.raw.0.header, flags);
            Ok(msg::Events::Unsubscription)
        }

        Data::Conn(d) => {
            check_matchs(args, &d.raw.0.header, flags);
            Ok(msg::Events::Connection)
        }

        Data::Disconn(d) => {
            check_matchs(args, &d.raw.0.header, flags);
            Ok(msg::Events::Disconnection)
        }

        Data::Close(_v, _d) => Ok(msg::Events::Close),
        Data::Malformed(_v, _d) => Ok(msg::Events::Malformed),
        Data::Unknown(m) => {
            bail!(format!("unexpect type {}", m));
        }
    }
}

pub async fn run_read(args: &ReadArgs) -> Result<()> {
    let pulsar: Pulsar<_> = Pulsar::builder(&args.url, TokioExecutor).build().await?;
    let admin = Admin::new(args.rest.clone());

    info!("clusters: {:?}", admin.list_clusters().await?);

    let tparts: TopicParts = args.topic.parse()?;
    let last_index = admin.get_last_msgid(&tparts).await?;
    info!("get info of {:?}", args.topic);
    info!("  - location: {:?}", admin.lookup_topic(&tparts).await?);
    info!("  - last-msg: {:?}", last_index);

    let mut consumer: Consumer = {
        let mut builder = TopicConsumerBuilder::new(&pulsar, &admin)
            .with_consumer_name("rtools-consumer".to_string())
            .with_sub_name("rtools-sub".to_string())
            .with_topic(&args.topic);

        builder = match &args.begin {
            Some(begin) => match begin {
                PosBegin::Index(index) => builder.with_begin_entry(index.clone()),
                PosBegin::Timestamp(ts) => builder.with_begin_timestamp(ts.0),
            },
            None => builder.with_begin_timestamp(0),
        };

        builder.build().await?
    };

    let mut num_matchs = 0;
    let mut num = 0;
    loop {
        let r = consumer.try_next().await?;
        if r.is_none() {
            break;
        }
        let msg = r.unwrap();
        consumer.ack(&msg).await?;

        if args.end_time.is_some()
            && msg.metadata().publish_time > args.end_time.as_ref().unwrap().0
        {
            info!("reach end time");
            break;
        }

        let time1 = SystemTime::UNIX_EPOCH + Duration::from_millis(msg.metadata().publish_time);
        let time1: DateTime<Local> = time1.into();
        let time_str = time1.format("%m-%d %H:%M:%S%.3f");

        let str = format!(
            "====== No.{}, [{}], [{}:{}]",
            num,
            time_str,
            msg.message_id().ledger_id,
            msg.message_id().entry_id,
        );

        let data = msg.deserialize().context(here!())?;

        let mut flags: BitFlags<AMatchs> = BitFlags::EMPTY;
        let etype = handle_msg(args, &data, &mut flags)
            .with_context(|| str.clone())
            .context(here!())?;

        if !flags.is_empty() {
            num_matchs += 1;
            info!(
                "{}, [{:?}], {:?}",
                str,
                etype,
                flags.iter().collect::<Vec<_>>()
            );
            info!("  {:?}\n", data);
        } else {
            debug!(
                "{}, [{:?}], {:?}",
                str,
                etype,
                flags.iter().collect::<Vec<_>>()
            );
            debug!("  {:?}\n", data);
        }

        num += 1;

        if args.num.is_some() && num >= *args.num.as_ref().unwrap() {
            info!("reach max messages");
            break;
        }

        if last_index.equal_to(msg.message_id()) {
            info!("reach last message");
            break;
        }
    }
    info!("matchs [{}]", num_matchs);
    Ok(())
}
