use crate::{
    pulsar::pulsar_util::TopicConsumerBuilder,
    util::{AMatchs, MatchClientId, RegexArg},
};
use anyhow::{bail, Context, Result};
use bytes::{Buf, Bytes};
use chrono::{DateTime, Local, TimeZone};
use clap::{Clap, ValueHint};
use enumflags2::BitFlags;
use futures::TryStreamExt;
use log::{debug, info};
use prost::Message;
use pulsar::{proto::MessageIdData, DeserializeMessage, Payload, Pulsar, TokioExecutor};
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
        long_about = "pulsar topic, for example persistent://em/default/ev0",
        multiple = true
    )]
    topics: Vec<String>,

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
    match_clientid: Option<MatchClientId>,

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

#[inline]
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

#[inline]
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

#[derive(Default)]
struct EndChecker {
    end_indexes: Vec<(EntryIndex, bool)>,
    num_finished: usize,
}

impl EndChecker {
    fn conver_index(p: i32) -> usize {
        if p < 0 {
            0
        } else {
            p as usize
        }
    }

    fn set_partitions(&mut self, p: i32) {
        let n = Self::conver_index(p);
        self.end_indexes.clear();
        self.end_indexes.reserve(n);
        for _ in 0..n {
            self.end_indexes.push((EntryIndex::default(), false));
        }
    }

    fn set_index(&mut self, eindex: EntryIndex) -> Result<()> {
        let index = Self::conver_index(eindex.partition_index);

        if self.end_indexes.len() <= index {
            bail!(
                "EndChecker::set_index out of range, expect {} but {}",
                self.end_indexes.len(),
                index
            );
        }

        self.end_indexes[index].0 = eindex;

        Ok(())
    }

    fn check(&mut self, d: &MessageIdData) -> Result<bool> {
        let index = Self::conver_index(d.partition.unwrap_or(0));

        if self.end_indexes.len() <= index {
            bail!(
                "EndChecker::set_index out of range, expect {} but {}",
                self.end_indexes.len(),
                index
            );
        }

        if !self.end_indexes[index].1 {
            if self.end_indexes[index].0.equal_to(&d) {
                self.end_indexes[index].1 = true;
                self.num_finished += 1;
            }
        }

        Ok(self.num_finished >= self.end_indexes.len())
    }
}

#[derive(Debug, Default)]
struct TaskOutput {
    num_matchs: u64,
    num_packets: u64,
}

async fn run_topic(
    args: &ReadArgs,
    topic: &str,
    topic_inex: usize,
    output: &mut TaskOutput,
) -> Result<()> {
    let pulsar: Pulsar<_> = Pulsar::builder(&args.url, TokioExecutor)
        .build()
        .await
        .with_context(|| format!("pulsar url {}", args.url))?;

    let admin = Admin::new(args.rest.clone());

    let tparts: TopicParts = topic.parse()?;

    let mut end_ck = EndChecker::default();

    {
        info!("get info of {:?}", topic);

        let location = admin.lookup_topic(&tparts).await?;
        info!("  - location: {:?}", location);

        let partis = admin.topic_get_partitions(&tparts).await?.partitions;
        info!("  - partitions: {:?}", partis);

        // let state = admin.topic_internal_state(&tparts).await?;
        // info!("  - internal-state: {:?}", state);

        if partis > 0 {
            end_ck.set_partitions(partis);
            for n in 0..partis {
                let child_topic = format!("{}-partition-{}", topic, n);
                let child_tparts = child_topic.parse()?;
                let last_index = admin.get_last_msgid(&child_tparts).await?;
                info!("  - last-msg: {:?}", last_index);
                end_ck.set_index(last_index)?;
            }
        } else {
            end_ck.set_partitions(1);
            let last_index = admin.get_last_msgid(&tparts).await?;
            info!("  - last-msg: {:?}", last_index);
            end_ck.set_index(last_index)?;
        }
    };

    let mut consumer: Consumer = {
        let mut builder = TopicConsumerBuilder::new(&pulsar, &admin)
            .with_consumer_name("rtools-consumer".to_string())
            .with_sub_name("rtools-sub".to_string())
            .with_topic(topic);

        builder = match &args.begin {
            Some(begin) => match begin {
                PosBegin::Index(index) => builder.with_begin_entry(index.clone()),
                PosBegin::Timestamp(ts) => builder.with_begin_timestamp(ts.0),
            },
            None => builder, //builder.with_begin_timestamp(0),
        };

        builder
            .build()
            .await
            .with_context(|| "fail to build consumer")?
    };

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

        output.num_packets += 1;

        let time1 = SystemTime::UNIX_EPOCH + Duration::from_millis(msg.metadata().publish_time);
        let time1: DateTime<Local> = time1.into();
        let time_str = time1.format("%m-%d %H:%M:%S%.3f");

        let str = format!(
            "====== No.{}, [T{}], [{}], [{}:{}:{}]",
            output.num_packets,
            topic_inex,
            time_str,
            msg.message_id().ledger_id,
            msg.message_id().entry_id,
            match msg.message_id().partition {
                Some(n) => n,
                None => -1,
            },
        );

        let data = msg.deserialize().context(here!())?;

        let mut flags: BitFlags<AMatchs> = BitFlags::EMPTY;
        let etype = handle_msg(args, &data, &mut flags)
            .with_context(|| str.clone())
            .context(here!())?;

        if !flags.is_empty() {
            output.num_matchs += 1;
            info!(
                "{}, [{:?}], [{}]-{:?}",
                str,
                etype,
                output.num_matchs,
                flags.iter().collect::<Vec<_>>(),
            );
            info!("  {:?}\n", data);
        } else {
            debug!(
                "{}, [{:?}], [{}]-{:?}",
                str,
                etype,
                output.num_matchs,
                flags.iter().collect::<Vec<_>>(),
            );
            debug!("  {:?}\n", data);
        }

        if args.num.is_some() && output.num_packets >= *args.num.as_ref().unwrap() {
            info!("reach max messages");
            break;
        }

        if end_ck.check(msg.message_id())? {
            info!("reach last message");
            break;
        }
    }
    Ok(())
}

async fn read_task(args: &ReadArgs, output: &mut TaskOutput) -> Result<()> {
    info!("args=[{:?}]", args);
    info!("args[url]=[{:?}]", args.url);
    info!("args[rest]=[{:?}]", args.rest);

    {
        let admin = Admin::new(args.rest.clone());
        info!("clusters: {:?}", admin.list_clusters().await?);

        let _pulsar: Pulsar<_> = Pulsar::builder(&args.url, TokioExecutor)
            .build()
            .await
            .with_context(|| format!("pulsar url {}", args.url))?;
    }

    // let mut output = TaskOutput::default();
    for (i, topic) in args.topics.iter().enumerate() {
        run_topic(args, topic, i, output)
            .await
            .with_context(|| format!("topic {}", topic))?;
    }

    Ok(())
}

pub async fn run_read(args: &ReadArgs) -> Result<()> {
    let mut output = TaskOutput::default();
    {
        let task = read_task(&args, &mut output);
        tokio::pin!(task);

        let ctrl_c_fut = tokio::signal::ctrl_c();
        tokio::pin!(ctrl_c_fut);

        tokio::select! {
            r = &mut task => {
                let _r = r?;
            }

            r = &mut ctrl_c_fut => {
                r.with_context(||"failed to listen ctrl+c")?;
                info!("got ctrl+c");
            }
        }
    }

    info!("{:?}", output);

    Ok(())
}
