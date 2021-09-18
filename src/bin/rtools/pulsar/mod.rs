use anyhow::{bail, Context, Result};
use bytes::{BufMut, Bytes, BytesMut};
use chrono::{DateTime, Local};
use clap::{Clap, ValueHint};
use futures::TryStreamExt;
use log::{debug, info};
use pretty_hex::{hex_write, HexConfig};
use prost::Message;
use pulsar::{
    consumer::InitialPosition, producer, proto::MessageIdData, Consumer, ConsumerOptions,
    DeserializeMessage, Payload, Pulsar, SerializeMessage, TokioExecutor,
};
use rust_threeq::tq3::tt;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::{
    convert::TryFrom,
    str::FromStr,
    time::{Duration, SystemTime},
};

macro_rules! here {
    () => {
        // src/bin/mongodb.rs:16:5
        concat!("at ", file!(), ":", line!(), ":", column!())
    };
}

#[derive(Clap, Debug, PartialEq)]
pub struct ReadArgs {
    #[clap(long = "url", long_about = "pulsar url", default_value = "pulsar://127.0.0.1:6650", value_hint = ValueHint::Url,)]
    url: String,

    #[clap(
        long = "topic",
        long_about = "pulsar topic, for example persistent://em/default/ev0"
    )]
    topic: String,

    #[clap(
        long = "msgid",
        long_about = "message id to start read from, {ledger_id}:{entry_id}"
    )]
    msgid: Option<MessageId>,
}

#[derive(Debug, PartialEq, Serialize)]
pub struct MessageId {
    pub ledger_id: u64,
    pub entry_id: u64,
}

impl std::str::FromStr for MessageId {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let v: Vec<&str> = s.split(':').collect();
        if v.len() != 2 {
            bail!("message id consist of {ledger_id}:{entry_id}");
        }
        Ok(Self {
            ledger_id: v[0].parse::<u64>()?,
            entry_id: v[1].parse::<u64>()?,
        })
    }
}

impl<'de> serde::Deserialize<'de> for MessageId {
    fn deserialize<D>(de: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(de)?;
        let r: Result<MessageId> = s.parse();
        match r {
            Ok(m) => Ok(m),
            Err(e) => Err(serde::de::Error::custom(e)),
        }
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

// fn parse_mqtt_packet(packet: &mut Vec<u8>) -> Result<()> {

//     let h = tt::check(packet.iter(), 65536).context(here!())?;
//     let packet_type = tt::PacketType::try_from(h.get_type_byte()).context(here!())?;
//     let bytes = Bytes::copy_from_slice(packet);
//     let conn_pkt = tt::Connect::read(h, bytes).context(here!())?;
//     Ok(())
// }

// pub trait PrettyHexLine: Sized {
//     fn dump_line(&self) -> HexLine;
// }

// impl PrettyHexLine for &[u8] {
//     fn dump_line(&self) -> HexLine {
//         HexLine(self)
//     }
// }

// impl PrettyHexLine for Bytes {
//     fn dump_line(&self) -> HexLine {
//         HexLine(self)
//     }
// }

// pub fn hex_line_write<W:std::fmt::Write>(f: &mut W, source: &[u8]) -> std::fmt::Result {
//     let cfg = HexConfig {title: false, width: 0, group: 0, ..HexConfig::default() };
//     let v = if source.len() <= 16 {
//         source
//     } else {
//         &source[..16]
//     };

//     write!(f, "|")?;
//     write!(f, "{}", source.len())?;
//     write!(f, "|")?;
//     hex_write(f, &v, cfg)?;
//     write!(f, "|")?;
//     if source.len() > 16 {
//         write!(f, "...")?;
//     }
//     write!(f, "|")
// }

// pub struct HexLine<'a>(&'a [u8]);

// impl<'a> std::fmt::Display for HexLine<'a> {
//     /// Formats the value by `simple_hex_write` using the given formatter.
//     fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
//         hex_line_write(f, self.0)
//     }
// }

// impl<'a> std::fmt::Debug for HexLine<'a> {
//     fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
//         hex_line_write(f, self.0)
//     }
// }

pub fn hex_line_write<W: std::fmt::Write, T: AsRef<[u8]>>(
    f: &mut W,
    source: T,
    max_len: usize,
) -> std::fmt::Result {
    let cfg = HexConfig {
        title: false,
        width: 0,
        group: 0,
        ..HexConfig::default()
    };
    let source = source.as_ref();

    let v = if source.len() <= max_len {
        source
    } else {
        &source[..max_len]
    };

    if let Ok(s) = std::str::from_utf8(source) {
        write!(f, "|str|{}|", s)?;
    } else {
        write!(f, "|")?;
        write!(f, "{}", source.len())?;
        write!(f, "|")?;
        hex_write(f, &v, cfg)?;
        write!(f, "|")?;
    }

    if source.len() > 16 {
        write!(f, "..")?;
    }
    write!(f, "|")
}

pub struct HexLine<'a, T: 'a>(&'a T, usize);

impl<'a, T: 'a + AsRef<[u8]>> std::fmt::Display for HexLine<'a, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        hex_line_write(f, self.0, self.1)
    }
}

impl<'a, T: 'a + AsRef<[u8]>> std::fmt::Debug for HexLine<'a, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        hex_line_write(f, self.0, self.1)
    }
}

pub trait BinStrLine: Sized {
    fn bin_str(&self) -> HexLine<Self>;
    fn bin_str_limit(&self, max: usize) -> HexLine<Self>;
}

impl<T> BinStrLine for T
where
    T: AsRef<[u8]>,
{
    fn bin_str(&self) -> HexLine<Self> {
        HexLine(self, 16)
    }

    fn bin_str_limit(&self, max: usize) -> HexLine<Self> {
        HexLine(self, max)
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TopicLocation {
    //{"brokerUrl":"pulsar://localhost:6650","httpUrl":"http://localhost:18080","nativeUrl":"pulsar://localhost:6650","brokerUrlSsl":""}
    pub broker_url: String,
    pub http_url: String,
    pub native_url: String,
    pub broker_url_ssl: String,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TopicInternalState {
    pub entries_added_counter: u64,
    pub number_of_entries: u64,
    pub total_size: u64,
    pub current_ledger_entries: u64,
    pub current_ledger_size: u64,
    pub waiting_cursors_count: u64,
    pub pending_add_entries_count: u64,

    #[serde(with = "my_date_format")]
    pub last_ledger_created_timestamp: DateTime<Local>, // "2021-09-18T10:43:34.14+08:00"

    pub last_confirmed_entry: MessageId, // "4549:27"
}

// {
//     "": 28,
//     "": 28,
//     "": 6710,
//     "": 28,
//     "": 6710,
//     "": "2021-09-18T10:43:34.14+08:00",
//     "": 7,
//     "": 0,
//     "": "4549:27",
//     "state": "LedgerOpened",
//     "ledgers": [
//         {
//             "ledgerId": 4549,
//             "entries": 0,
//             "size": 0,
//             "offloaded": false,
//             "underReplicated": false
//         }
//     ],
//     "cursors": {
//         "sub1": {
//             "markDeletePosition": "4549:27",
//             "readPosition": "4549:28",
//             "waitingReadOp": true,
//             "pendingReadOps": 0,
//             "messagesConsumedCounter": 28,
//             "cursorLedger": 4550,
//             "cursorLedgerLastEntry": 20,
//             "individuallyDeletedMessages": "[]",
//             "lastLedgerSwitchTimestamp": "2021-09-18T10:43:34.145+08:00",
//             "state": "Open",
//             "numberOfEntriesSinceFirstNotAckedMessage": 1,
//             "totalNonContiguousDeletedMessagesRange": 0,
//             "subscriptionHavePendingRead": true,
//             "subscriptionHavePendingReplayRead": false,
//             "properties": { }
//         }
//     },
//     "schemaLedgers": [ ],
//     "compactedLedger": {
//         "ledgerId": -1,
//         "entries": -1,
//         "size": -1,
//         "offloaded": false,
//         "underReplicated": false
//     }
// }

mod my_date_format {
    use chrono::{DateTime, Local, TimeZone};
    use serde::{self, Deserialize, Deserializer, Serializer};

    // https://docs.rs/chrono/0.4.19/chrono/format/strftime/index.html
    // 2021-09-18T10:43:34.14+08:00
    pub const FORMAT: &'static str = "%Y-%m-%dT%H:%M:%S%.f%:z";

    pub fn serialize<S>(date: &DateTime<Local>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let s = format!("{}", date.format(FORMAT));
        serializer.serialize_str(&s)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<DateTime<Local>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Local
            .datetime_from_str(&s, FORMAT)
            .map_err(serde::de::Error::custom)
    }
}

pub struct Admin {
    client: reqwest::Client,
    url: String,
}

impl Admin {
    pub fn new(url: &str) -> Self {
        Self {
            client: reqwest::Client::new(),
            url: url.into(),
        }
    }

    pub async fn list_tenants(&self) -> Result<Vec<String>> {
        // curl http://localhost:18080/admin/v2/tenants
        let path = format!("{}/admin/v2/tenants", self.url);
        Ok(self.get(&path).await?)
    }

    pub async fn get<T: DeserializeOwned>(&self, path: &str) -> Result<T> {
        let builder = self.client.get(path);

        let res = builder
            .send()
            .await
            .context(here!())
            .context(format!("fail to get path {}", path))?;

        let rsp_status = res.status();
        if !rsp_status.is_success() {
            return Err(
                anyhow::anyhow!(" get path {} response with {:?}", path, res)
                    .context(format!("status {}", rsp_status)),
            );
        }

        let rsp_body = res.text().await.context(here!())?;
        // debug!("{}", rsp_body);
        let v: T = serde_json::from_str(&rsp_body).context(here!())?;
        Ok(v)
    }

    pub async fn lookup_topic(&self, topic: &Topic) -> Result<TopicLocation> {
        // GET /lookup/v2/topic/:schema/:tenant/:namespace/:topic
        let path = format!(
            "{}/lookup/v2/topic/{}/{}/{}/{}",
            self.url, topic.schema, topic.tenant, topic.namespace, topic.topic
        );
        Ok(self
            .get(&path)
            .await
            .context("fail to lookup topic")
            .context(here!())?)
    }

    pub async fn topic_internal_state(&self, topic: &Topic) -> Result<TopicInternalState> {
        //GET /admin/v2/:schema/:tenant/:namespace/:topic/internalStats
        let path = format!(
            "{}/admin/v2/{}/{}/{}/{}/internalStats",
            self.url, topic.schema, topic.tenant, topic.namespace, topic.topic
        );
        Ok(self
            .get(&path)
            .await
            .context(here!())
            .context("fail to get topic internal state")?)
    }
}

fn parse_msg(msg: pulsar::consumer::Message<Data>) -> Result<()> {
    let data = msg.deserialize().context(here!())?;
    debug!("  {:?}", data);
    match data {
        Data::ULink(v, d) => {
            let ver = tt::Protocol::from_u8(v)?;
            let h = tt::check(d.packet.iter(), 65536).context(here!())?;
            let packet_type = tt::PacketType::try_from(h.get_type_byte()).context(here!())?;
            let bytes = Bytes::copy_from_slice(&d.packet);
            let mqtt_pkt = tt::Publish::decode(ver, h, bytes).context(here!())?;
            log::info!("  {}, {:?}", d.packet.len(), h);
            log::info!("  {}, {:?}, {:?}", v, packet_type, mqtt_pkt);
            log::info!("  content=[{:?}]", mqtt_pkt.payload.bin_str());
        }
        Data::DLink(_, _) => todo!(),
        Data::Sub(v, d) => {
            let ver = tt::Protocol::from_u8(v)?;
            let h = tt::check(d.packet.iter(), 65536).context(here!())?;
            let packet_type = tt::PacketType::try_from(h.get_type_byte()).context(here!())?;
            let bytes = Bytes::copy_from_slice(&d.packet);
            let mqtt_pkt = tt::Subscribe::decode(ver, h, bytes).context(here!())?;
            log::info!("  {}, {:?}", d.packet.len(), h);
            log::info!("  {}, {:?}, {:?}", v, packet_type, mqtt_pkt);
        }
        Data::Unsub(v, d) => {
            let ver = tt::Protocol::from_u8(v)?;
            let h = tt::check(d.packet.iter(), 65536).context(here!())?;
            let packet_type = tt::PacketType::try_from(h.get_type_byte()).context(here!())?;
            let bytes = Bytes::copy_from_slice(&d.packet);
            let mqtt_pkt = tt::Unsubscribe::decode(ver, h, bytes).context(here!())?;
            log::info!("  {}, {:?}", d.packet.len(), h);
            log::info!("  {}, {:?}, {:?}", v, packet_type, mqtt_pkt);
        }
        Data::Conn(v, d) => {
            let h = tt::check(d.packet.iter(), 65536).context(here!())?;
            let packet_type = tt::PacketType::try_from(h.get_type_byte()).context(here!())?;
            let bytes = Bytes::copy_from_slice(&d.packet);
            let mqtt_pkt = tt::Connect::read(h, bytes).context(here!())?;
            log::info!("  {}, {:?}", d.packet.len(), h);
            log::info!("  {}, {:?}, {:?}", v, packet_type, mqtt_pkt);
        }
        Data::Disconn(v, d) => {
            let ver = tt::Protocol::from_u8(v)?;
            let h = tt::check(d.packet.iter(), 65536).context(here!())?;
            let packet_type = tt::PacketType::try_from(h.get_type_byte()).context(here!())?;
            let bytes = Bytes::copy_from_slice(&d.packet);
            let mqtt_pkt = tt::Disconnect::decode(ver, h, bytes).context(here!())?;
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

#[derive(Debug)]
pub struct Topic {
    pub schema: String,
    pub tenant: String,
    pub namespace: String,
    pub topic: String,
}

impl FromStr for Topic {
    type Err = anyhow::Error;

    fn from_str(topic_str: &str) -> Result<Self, Self::Err> {
        let url = reqwest::Url::parse(topic_str).context("parse topic url fail")?;
        if url.cannot_be_a_base() || !url.has_host() {
            bail!("invalid topic [{}]", topic_str);
        }

        let tenant = url.host_str().context(here!())?;
        let mut paths = url.path().split('/');
        paths.next().context("invalid topic: path-1")?;
        let namespace = paths.next().context("invalid topic: path-2")?;
        let topic = paths.collect::<String>();

        Ok(Self {
            schema: url.scheme().into(),
            tenant: tenant.into(),
            namespace: namespace.into(),
            topic,
        })
    }
}

pub async fn run_read(args: &ReadArgs) -> Result<()> {
    let admin = Admin::new("http://localhost:18080");
    let tenants = admin.list_tenants().await?;
    debug!("tenants: {:?}", tenants);

    let topic: Topic = args.topic.parse()?;
    info!("{:?}", topic);
    info!("{:?}", admin.lookup_topic(&topic).await?);
    info!("{:?}", admin.topic_internal_state(&topic).await?);

    let builder = Pulsar::builder(&args.url, TokioExecutor);
    let pulsar: Pulsar<_> = builder.build().await?;

    let mut options = ConsumerOptions::default();
    if args.msgid.is_none() {
        options = options.with_initial_position(InitialPosition::Earliest);
    } else {
        let msgid = &args.msgid.as_ref().unwrap();
        options = options.starting_on_message(MessageIdData {
            ledger_id: msgid.ledger_id, // TODO: aaa
            entry_id: msgid.entry_id,   // TODO: aaa
            partition: None,
            batch_index: None,
            ack_set: Vec::new(), // TODO: aaa
            batch_size: None,
        });
    }
    options = options.durable(false);

    // let mut reader: Reader<Data, _> = pulsar
    //     .reader()
    //     .with_topic(&args.topic)
    //     .with_options(options)
    //     .with_consumer_name("rtools-reader")
    //     .with_subscription("subscription11")
    //     .with_subscription_type(pulsar::SubType::Exclusive)
    //     .into_reader()
    //     .await?;

    let mut reader: Consumer<Data, _> = pulsar
        .consumer()
        .with_topic(&args.topic)
        .with_options(options)
        .with_consumer_name("rtools-reader")
        .with_subscription("subscription11")
        .with_subscription_type(pulsar::SubType::Exclusive)
        .build()
        .await?;

    let mut num = 0;
    while let Some(msg) = reader.try_next().await? {
        // reader.ack(&msg).await?;
        let time = SystemTime::UNIX_EPOCH + Duration::from_millis(msg.metadata().publish_time);
        let time: DateTime<Local> = time.into();
        let time = time.format("%m-%d %H:%M:%S%.3f");

        debug!(
            "====== No.{}, id=[{}:{}], time=[{}]-[{}]",
            num,
            msg.message_id().ledger_id,
            msg.message_id().entry_id,
            time,
            msg.metadata().publish_time
        );

        parse_msg(msg).context(here!())?;
        num += 1;
    }
    Ok(())
}
