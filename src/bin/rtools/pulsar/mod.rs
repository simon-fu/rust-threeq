use crate::pulsar::pulsar_util::TopicConsumerBuilder;
use anyhow::{bail, Context, Result};
use bytes::{BufMut, Bytes, BytesMut};
use chrono::{DateTime, Local, TimeZone};
use clap::{Clap, ValueHint};
use futures::TryStreamExt;
use log::{debug, info};
use prost::Message;
use pulsar::{producer, DeserializeMessage, Payload, Pulsar, SerializeMessage, TokioExecutor};
use rust_threeq::{
    here,
    tq3::{hex::BinStrLine, tbytes::PacketDecoder, tt},
};
use std::{
    convert::TryFrom,
    time::{Duration, SystemTime},
};

// use async_trait::async_trait;
use self::pulsar_util::{Admin, EntryIndex, TopicParts};

mod pulsar_util;

#[derive(Clap, Debug, PartialEq, Clone)]
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

    // #[clap(
    //     long = "msgid",
    //     long_about = "message id to start read from, {ledger_id}:{entry_id}"
    // )]
    // msgid: Option<EntryIndex>,
    #[clap(
        long = "begin",
        long_about = "begin position to read from, one of formats:\n1. pulsar message id in format of {ledger_id}:{entry_id}\n2. time format, for example 2021-09-20T12:35:57"
    )]
    begin: PosBegin,

    #[clap(long = "num", long_about = "optional, max messages to read")]
    num: Option<u64>,

    #[clap(
        long = "end-time",
        long_about = "optional, end position in time format, for example 2021-09-20T12:35:57"
    )]
    end_time: Option<TimeArg>,
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

impl msg::Events {
    #[inline]
    fn encode<B>(&self, ver: u8, buf: &mut B)
    where
        B: BufMut,
    {
        let mut d32: u32 = msg::Events::Uplink as u32;
        d32 |= (ver as u32) << 24;
        buf.put_i32(d32 as i32);
    }
}

type Consumer = pulsar::Consumer<Data, TokioExecutor>;

#[derive(Debug)]
enum Data {
    ULink(u8, msg::UplinkMessage),
    DLink(u8, msg::DownlinkMessage),
    Sub(u8, msg::Subscription),
    Unsub(u8, msg::UnSubscription),
    Conn(u8, msg::Connection),
    Disconn(u8, msg::Disconnection),
    Close(u8, msg::SessionClose),
    Malformed(u8, msg::MalformedPackage),
    Unknown(i32),
}

impl Data {
    fn encode(&self) -> Result<Vec<u8>> {
        let mut buf = BytesMut::new();
        match self {
            Data::ULink(v, m) => {
                msg::Events::Uplink.encode(*v, &mut buf);
                m.encode(&mut buf)?;
            }
            Data::DLink(v, m) => {
                msg::Events::Downlink.encode(*v, &mut buf);
                m.encode(&mut buf)?;
            }
            Data::Sub(v, m) => {
                msg::Events::Subscription.encode(*v, &mut buf);
                m.encode(&mut buf)?;
            }
            Data::Unsub(v, m) => {
                msg::Events::Unsubscription.encode(*v, &mut buf);
                m.encode(&mut buf)?;
            }
            Data::Conn(v, m) => {
                msg::Events::Connection.encode(*v, &mut buf);
                m.encode(&mut buf)?;
            }
            Data::Disconn(v, m) => {
                msg::Events::Disconnection.encode(*v, &mut buf);
                m.encode(&mut buf)?;
            }
            Data::Close(v, m) => {
                msg::Events::Close.encode(*v, &mut buf);
                m.encode(&mut buf)?;
            }
            Data::Malformed(v, m) => {
                msg::Events::Malformed.encode(*v, &mut buf);
                m.encode(&mut buf)?;
            }
            Data::Unknown(m) => {
                bail!(format!("unknown type {}", m));
            }
        }
        Ok(buf.to_vec())
    }
}

impl SerializeMessage for Data {
    fn serialize_message(input: Self) -> Result<producer::Message, pulsar::Error> {
        let r = input.encode();
        match r {
            Ok(v) => Ok(producer::Message {
                payload: v,
                ..Default::default()
            }),
            Err(e) => Err(pulsar::Error::Custom(e.to_string())),
        }
    }
}

impl DeserializeMessage for Data {
    //type Output = Result<Data, serde_json::Error>;
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

        let bytes = &payload.data[4..];
        match ev {
            msg::Events::Uplink => Ok(Self::ULink(
                ver,
                msg::UplinkMessage::decode(bytes).context(here!())?,
            )),
            msg::Events::Downlink => Ok(Self::DLink(
                ver,
                msg::DownlinkMessage::decode(bytes).context(here!())?,
            )),
            msg::Events::Subscription => Ok(Self::Sub(
                ver,
                msg::Subscription::decode(bytes).context(here!())?,
            )),
            msg::Events::Unsubscription => Ok(Self::Unsub(
                ver,
                msg::UnSubscription::decode(bytes).context(here!())?,
            )),
            msg::Events::Connection => Ok(Self::Conn(
                ver,
                msg::Connection::decode(bytes).context(here!())?,
            )),
            msg::Events::Disconnection => Ok(Self::Disconn(
                ver,
                msg::Disconnection::decode(bytes).context(here!())?,
            )),
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

fn parse_msg(msg: &pulsar::consumer::Message<Data>) -> Result<()> {
    let data = msg.deserialize().context(here!())?;
    debug!("  {:?}", data);
    match data {
        Data::ULink(v, d) => {
            let ver = tt::Protocol::from_u8(v)?;
            let h = tt::check(d.packet.iter(), 65536).context(here!())?;
            let packet_type = tt::PacketType::try_from(h.get_type_byte()).context(here!())?;
            let mut bytes = Bytes::copy_from_slice(&d.packet);
            let mqtt_pkt = tt::Publish::decode(ver, &h, &mut bytes).context(here!())?;
            log::info!("  {}, {:?}", d.packet.len(), h);
            log::info!("  {}, {:?}, {:?}", v, packet_type, mqtt_pkt);
            log::info!("  content=[{:?}]", mqtt_pkt.payload.bin_str());
        }
        Data::DLink(v, d) => {
            let ver = tt::Protocol::from_u8(v)?;
            debug!("  ver {:?}, msg {:?}", ver, d);
            // debug!("  packet {}", d.packet.dump_bin());
            // let h = tt::check(d.packet.iter(), 65536).context(here!())?;
            // let packet_type = tt::PacketType::try_from(h.get_type_byte()).context(here!())?;
            // // let mut bytes = Bytes::copy_from_slice(&d.packet);
            // let  mut bytes = &d.packet[..];
            // let mqtt_pkt = tt::Publish::decode(ver, &h, &mut bytes).context(here!())?;
            // log::info!("  {}, {:?}", d.packet.len(), h);
            // log::info!("  {}, {:?}, {:?}", v, packet_type, mqtt_pkt);
            // log::info!("  content=[{:?}]", mqtt_pkt.payload.bin_str());
        }
        Data::Sub(v, d) => {
            let ver = tt::Protocol::from_u8(v)?;
            let h = tt::check(d.packet.iter(), 65536).context(here!())?;
            let packet_type = tt::PacketType::try_from(h.get_type_byte()).context(here!())?;
            let mut bytes = Bytes::copy_from_slice(&d.packet);
            let mqtt_pkt = tt::Subscribe::decode(ver, &h, &mut bytes).context(here!())?;
            log::info!("  {}, {:?}", d.packet.len(), h);
            log::info!("  {}, {:?}, {:?}", v, packet_type, mqtt_pkt);
        }
        Data::Unsub(v, d) => {
            let ver = tt::Protocol::from_u8(v)?;
            let h = tt::check(d.packet.iter(), 65536).context(here!())?;
            let packet_type = tt::PacketType::try_from(h.get_type_byte()).context(here!())?;
            let mut bytes = Bytes::copy_from_slice(&d.packet);
            let mqtt_pkt = tt::Unsubscribe::decode(ver, &h, &mut bytes).context(here!())?;
            log::info!("  {}, {:?}", d.packet.len(), h);
            log::info!("  {}, {:?}, {:?}", v, packet_type, mqtt_pkt);
        }
        Data::Conn(v, d) => {
            let h = tt::check(d.packet.iter(), 65536).context(here!())?;
            let packet_type = tt::PacketType::try_from(h.get_type_byte()).context(here!())?;
            let mut bytes = Bytes::copy_from_slice(&d.packet);
            let mqtt_pkt =
                tt::Connect::decode(tt::Protocol::V5, &h, &mut bytes).context(here!())?;
            log::info!("  {}, {:?}", d.packet.len(), h);
            log::info!("  {}, {:?}, {:?}", v, packet_type, mqtt_pkt);
        }
        Data::Disconn(v, d) => {
            let ver = tt::Protocol::from_u8(v)?;
            let h = tt::check(d.packet.iter(), 65536).context(here!())?;
            let packet_type = tt::PacketType::try_from(h.get_type_byte()).context(here!())?;
            let mut bytes = Bytes::copy_from_slice(&d.packet);
            let mqtt_pkt = tt::Disconnect::decode(ver, &h, &mut bytes).context(here!())?;
            log::info!("  {}, {:?}", d.packet.len(), h);
            log::info!("  {}, {:?}, {:?}", v, packet_type, mqtt_pkt);
        }
        Data::Close(v, d) => {
            log::info!("  {}, {:?}", v, d);
        }
        Data::Malformed(_v, _d) => {}
        Data::Unknown(_) => todo!(),
    }
    Ok(())
}

pub async fn run_read(args: &ReadArgs) -> Result<()> {
    let pulsar: Pulsar<_> = Pulsar::builder(&args.url, TokioExecutor).build().await?;
    let admin = Admin::new(args.rest.clone());

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
            PosBegin::Index(index) => builder.with_begin_entry(index.clone()),
            PosBegin::Timestamp(ts) => builder.with_begin_timestamp(ts.0),
        };

        builder.build().await?
    };

    let mut num = 0;
    loop {
        let r = consumer.try_next().await?;
        if r.is_none() {
            break;
        }
        let msg = r.unwrap();

        if args.end_time.is_some()
            && msg.metadata().publish_time > args.end_time.as_ref().unwrap().0
        {
            info!("reach end time");
            break;
        }

        // consumer.ack(&msg).await?;
        let time1 = SystemTime::UNIX_EPOCH + Duration::from_millis(msg.metadata().publish_time);
        let time1: DateTime<Local> = time1.into();
        let time_str = time1.format("%m-%d %H:%M:%S%.3f");

        debug!(
            "====== No.{}, id=[{}:{}], time=[{}]-[{}]",
            num,
            msg.message_id().ledger_id,
            msg.message_id().entry_id,
            time_str,
            msg.metadata().publish_time
        );

        parse_msg(&msg).context(here!())?;
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
    Ok(())
}
