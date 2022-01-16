use anyhow::{Context, Result, bail};
use bytes::Buf;
use clap::Parser;
use log::info;

use rdkafka::{
    consumer::{BaseConsumer, Consumer},
    message::BorrowedMessage,
    util::Timeout,
    ClientConfig, Message, Offset, topic_partition_list::TopicPartitionListElem,
};
use rust_threeq::tq3::{
    tbytes::PacketDecoder,
    tt::{self, Protocol},
};
use serde::{Serialize, Deserialize};
use std::{time::Duration, collections::HashMap};
use tracing::{debug, error};

use crate::util::{MatchFlag, MatchPayloadText, MatchTopic, TimeArg};

#[derive(Parser, Debug, Clone)]
pub struct ReadArgs {
    #[clap(long = "topic", long_help = "topic to read")]
    topic: String,

    #[clap(long = "group", long_help = "kafka group", default_value = "rtools")]
    group: String,

    #[clap(
        long = "addr",
        long_help = "kafka address",
        default_value = "127.0.0.1:9092"
    )]
    addr: String,

    #[clap(
        long = "begin",
        long_help = "begin position time to read from",
        default_value = "2021-01-01T00:00:00"
    )]
    begin: TimeArg,

    #[clap(
        long = "end",
        long_help = "end time",
    )]
    end: Option<TimeArg>,

    #[clap(
        long = "num",
        long_help = "num of messages to read",
        default_value = "999999999"
    )]
    num: u64,

    #[clap(long = "match-topic", long_help = "optional, regex match topic")]
    match_topic: Option<MatchTopic>,

    #[clap(long = "match-text", long_help = "optional, regex match payload text")]
    match_text: Option<MatchPayloadText>,

    #[clap(
        long = "timeout",
        long_help = "timeout in seconds",
        default_value = "10"
    )]
    timeout_sec: u64,
    
}

fn print_metadata(
    brokers: &str,
    topic: Option<&str>,
    timeout: Duration,
    fetch_offsets: bool,
) -> Result<()> {
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .create()
        .with_context(|| "Consumer creation failed")?;

    let metadata = consumer
        .fetch_metadata(topic, timeout.clone())
        .with_context(|| "Failed to fetch metadata")?;

    let mut message_count = 0;

    info!("Cluster information:");
    info!("  Broker count: {}", metadata.brokers().len());
    info!("  Topics count: {}", metadata.topics().len());
    info!("  Metadata broker name: {}", metadata.orig_broker_name());
    info!("  Metadata broker id: {}\n", metadata.orig_broker_id());

    info!("Brokers:");
    for broker in metadata.brokers() {
        info!(
            "  Id: {}  Host: {}:{}  ",
            broker.id(),
            broker.host(),
            broker.port()
        );
    }

    info!("");
    info!("Topics:");
    for topic in metadata.topics() {
        info!("  Topic: {}  Err: {:?}", topic.name(), topic.error());
        for partition in topic.partitions() {
            info!(
                "     Partition: {}  Leader: {}  Replicas: {:?}  ISR: {:?}  Err: {:?}",
                partition.id(),
                partition.leader(),
                partition.replicas(),
                partition.isr(),
                partition.error()
            );
            if fetch_offsets {
                let (low, high) = consumer
                    .fetch_watermarks(topic.name(), partition.id(), timeout.clone())
                    .unwrap_or((-1, -1));
                info!(
                    "       Low watermark: {}  High watermark: {} (difference: {})",
                    low,
                    high,
                    high - low
                );
                message_count += high - low;
            }
        }
        if fetch_offsets {
            info!("     Total message count: {}", message_count);
        }
    }
    Ok(())
}


#[inline]
fn format_milli(milli: u64) -> String{
    const MILLI_TIME_FORMAT: &'static str = "%Y-%m-%dT%H:%M:%S%.3f";
    let t = std::time::SystemTime::UNIX_EPOCH + Duration::from_millis(milli);
    let datetime: chrono::DateTime<chrono::Local> = t.into();
    format!("{}", datetime.format(MILLI_TIME_FORMAT))
}


mod msg {
    include!(concat!(env!("OUT_DIR"), "/mqtt.data.rs"));
}

// #[derive(Debug, Clone, Default)]
// struct AppStat {
//     num_ulink_qos0: u64,
//     num_ulink_qos1: u64,
//     num_ulink_qos2: u64,
//     num_ulink_total: u64,

    
//     num_dlink_qos0: u64,
//     num_dlink_qos1: u64,
//     num_dlink_qos2: u64,
//     num_dlink_total: u64,

//     num_dlink_qos0_real: u64,
//     num_dlink_total_real: u64,

//     num_msg_total: u64,
//     num_msg_total_real: u64,
// }

// impl AppStat {
//     fn complet(&mut self) {
//         self.num_ulink_total = self.num_ulink_qos0 + self.num_ulink_qos1 + self.num_ulink_qos2;
//         self.num_dlink_total = self.num_dlink_qos0 + self.num_dlink_qos1 + self.num_dlink_qos2;
//         self.num_dlink_total_real = self.num_dlink_qos0_real + self.num_dlink_qos1 + self.num_dlink_qos2;
//         self.num_msg_total = self.num_ulink_total + self.num_dlink_total;
//         self.num_msg_total_real = self.num_ulink_total + self.num_dlink_total_real;
//     }
// }


#[derive(Debug, Default, Clone, Serialize, Deserialize)]
struct Stat {
    num_uplinks: u64,
    apps: HashMap<String, StatMsg>
}

impl Stat {
    fn add_app_delta(&mut self, delta: StatMsg) -> Result<()> {
        let app = self.apps.entry(delta.appid).or_insert_with(||StatMsg::default());
        merge_stat_delta(&mut app.dl_bytes, delta.dl_bytes);
        merge_stat_delta(&mut app.dl_bytes_theory, delta.dl_bytes_theory);
        merge_stat_delta(&mut app.dl_count, delta.dl_count);
        merge_stat_delta(&mut app.dl_count_theory, delta.dl_count_theory);
        merge_stat_delta(&mut app.ul_bytes, delta.ul_bytes);
        merge_stat_delta(&mut app.ul_count, delta.ul_count);
        Ok(())
    }
}

fn merge_stat_delta(total: &mut Option<HashMap<String, u64>>, delta: Option<HashMap<String, u64>>,) {
    if let Some(delta) = delta {
        let total = match total {
            Some(total) => total,
            None => {
                *total = Some(Default::default());
                total.as_mut().unwrap()
            },
        };
        
        for (k, v) in delta {
            let entry = total.entry(k).or_insert_with(||0);
            *entry += v;
        }
    }
    
}

/*
        {
            "ul_count":{"2":3,"1":3,"0":3},
            "ul_bytes":{"2":3162,"1":3162,"0":3156},
            "dl_count_theory":{"0":3},
            "dl_count":{"2":1,"1":3,"0":5},
            "dl_bytes_theory":{"0":3156},
            "dl_bytes":{"2":1054,"1":3162,"0":5260},
            "appid":"1PGUGY"
        }
    */
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
struct StatMsg {
    appid: String,
    #[serde(skip_serializing_if="Option::is_none")]
    dl_bytes: Option<HashMap<String, u64>>,
    #[serde(skip_serializing_if="Option::is_none")]
    dl_bytes_theory: Option<HashMap<String, u64>>,
    #[serde(skip_serializing_if="Option::is_none")]
    dl_count: Option<HashMap<String, u64>>,
    #[serde(skip_serializing_if="Option::is_none")]
    dl_count_theory: Option<HashMap<String, u64>>,
    #[serde(skip_serializing_if="Option::is_none")]
    ul_bytes: Option<HashMap<String, u64>>,
    #[serde(skip_serializing_if="Option::is_none")]
    ul_count: Option<HashMap<String, u64>>,
}



struct ParsedHeader {
    header: msg::ControlHeaderInfo,
}

impl std::fmt::Debug for ParsedHeader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let header = &self.header;
        let mut f = f.debug_struct("Header");
        let f = f.field("msgid", &format!("{:016X?}", header.msgid()));
        let f = f.field("connid", &format!("{:016X?}", header.connid));
        let f = f.field("timestamp", &format_milli(header.timestamp));
        let f = f.field("clientid", &header.clientid());
        let f = f.field("user", &header.user);
        f.finish()
    }
}

#[derive(Debug, Default)]
struct ParsedMessage {
    n: u64,
    ts: String,
    ver: u8,
    etype: msg::Events,
    header: Option<ParsedHeader>,
    packet: Option<tt::Packet>,
    code: Option<String>,
}


fn process_ulink_msg(n: u64, msg: &BorrowedMessage, args: &ReadArgs, stat: &mut Stat) -> Result<()> {
    // let msg = borrowed_message.detach();
    // info!("msg {:?}", msg);
    // let payload = msg.payload().unwrap();
    let payload = msg.payload().with_context(||format!("No.{} empty payload", n))?;
    let mut cursor = payload;

    let ver = cursor.get_u8();
    cursor.advance(2);
    let etype = cursor.get_u8();
    let etype = msg::Events::from_i32(etype as i32).with_context(||"decode event type fail")?;

    let ts = {
        let r = msg.timestamp().to_millis();
        if let Some(n) = r {
            format_milli(n as u64)
        } else {
            "None".into()
        }
    };

    let mut kmsg = ParsedMessage {
        n,
        ts,
        ver,
        etype,
        ..Default::default()
    };

    let mut flag = MatchFlag::default();

    match etype {
        msg::Events::Uplink => {
            stat.num_uplinks += 1;
            let m = <msg::UplinkMessage as prost::Message>::decode(cursor)?;
            if let Some(header) = m.header {
                kmsg.header = Some(ParsedHeader{header});
            }
            kmsg.code = Some(m.code);
            let mut cursor = &m.packet[..];
            let fixed_header = tt::check(cursor.iter(), 65536)?;
            let packet = tt::Publish::decode(Protocol::from_u8(ver)?, &fixed_header, &mut cursor)?;
            flag.match_text(&args.match_topic, &packet.topic);
            flag.match_utf8(&args.match_text, packet.payload.to_vec());
            kmsg.packet = Some(tt::Packet::Publish(packet));
        },
        msg::Events::Downlink => {
            let _m = <msg::DownlinkMessage as prost::Message>::decode(cursor)?;
        },
        msg::Events::Subscription => {},
        msg::Events::Unsubscription => {},
        msg::Events::Connection => {},
        msg::Events::Disconnection => {},
        msg::Events::Close => {},
        msg::Events::Malformed => {},
    }

    if !flag.is_empty() {
        info!("-- No.{}: [{}], ver [{}], [{:?}], {:?}", n, kmsg.ts, ver, etype, flag.flags);
        info!("{:?}", kmsg);
        info!("");
    } else {
        debug!("-- No.{}: [{}], ver [{}], [{:?}]", n, kmsg.ts, ver, etype);
        debug!("{:?}", kmsg);
        debug!("");
    }
    

    Ok(())
}

fn process_stat_msg(n: u64, msg: &BorrowedMessage, _args: &ReadArgs, stat: &mut Stat) -> Result<()> {
    let payload = msg.payload().with_context(||format!("No.{} empty payload", n))?;

    let ts = {
        let r = msg.timestamp().to_millis();
        if let Some(n) = r {
            format_milli(n as u64)
        } else {
            "None".into()
        }
    };
    let s = std::str::from_utf8(payload)?;
    debug!("-- No.{}: ts [{}], parti [{}], offset [{}]", n, ts, msg.partition(), msg.offset());
    debug!("-- data: [{}]", s);
    
    let msg: StatMsg = serde_json::from_slice(payload).with_context(||"parse stat msg fail")?;
    stat.add_app_delta(msg)?;

    Ok(())
}

// struct EndItem {
//     end_offset: i64,
//     is_reach_end: bool,
// }

// impl EndItem {
//     fn update(&mut self, offset: i64) -> bool {
//         if offset >= self.end_offset {
//             if !self.is_reach_end {
//                 self.is_reach_end = true;
//                 return true;
//             }
//         }
//         false
//     }
// }

struct EndPos {
    // partition -> (offset, is_reach_end)
    positions: HashMap<i32, (i64, bool)>, 
    num_ends: usize,
}

impl EndPos {
    fn new<'a>(list: &Vec<TopicPartitionListElem<'a>>) -> Result<Self> {
        let mut positions = HashMap::new();
        let mut num_ends = 0;
        for ele in list {
            let v = match ele.offset() {
                Offset::Offset(n) => (n, false),
                Offset::End => (i64::MAX, false),
                _ => bail!("unsupported init offset {:?}", ele.offset()),
            };
            if v.1 {
                num_ends += 1;
            }
            positions.insert(ele.partition(), v);
        }
        Ok(Self{positions, num_ends})
    }

    fn update_by_msg(&mut self, msg: &BorrowedMessage) -> Result<Option<i64>> {
        self.update(msg.partition(), msg.offset())
    }

    fn update_by_offset(&mut self, partition: i32, offset: Offset) -> Result<Option<i64>> {
        match offset {
            Offset::Offset(n) => self.update(partition, n),
            Offset::End => self.update(partition, i64::MAX),
            _ => bail!("unsupported update offset {:?}", offset),
        }
    }

    fn update(&mut self, partition: i32, offset: i64) -> Result<Option<i64>> {
        if let Some(pos) = self.positions.get_mut(&partition) {
            if offset >= pos.0 {
                if !pos.1 {
                    pos.1 = true;
                    self.num_ends += 1;
                    return Ok(Some(pos.0));
                }
            }
        }
        Ok(None)
    }

    #[inline]
    fn is_all_end(&self) -> bool {
        self.num_ends >= self.positions.len()
    }
}

pub async fn run_read(args: &ReadArgs) -> Result<()> {
    // let args = &ReadArgs {
    //     topic: "1PGUGY-JmlZla".into(),
    //     addr: "172.17.1.160:9092".into(),
    //     begin: TimeArg::from_str("2021-11-09T16:14:39")?, // 1636445679000
    //     num: 5,
    //     group: "rools".into(),
    //     match_topic: None,
    //     match_text: None,
    //     timeout_sec: 10,
    // };

    let timeout = Duration::from_secs(args.timeout_sec);

    info!("args: {:?}", args);
    info!("");

    info!("fetching meta from {}...", args.addr);
    print_metadata(&args.addr, Some(&args.topic), timeout.clone(), true)?;

    // https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
    let consumer: BaseConsumer = ClientConfig::new()
        .set("group.id", &args.group)
        .set("bootstrap.servers", &args.addr)
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "6000")
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest")
        .create()?;

    let _r = consumer.subscribe(&[&args.topic])?;
    info!("subscribed topic {}", args.topic);

    info!("subscription {:?}", consumer.subscription()?);
    for ele in consumer.subscription()?.elements() {
        info!(
            "  ele partition {}, offset {:?}",
            ele.partition(),
            ele.offset(),
        );
    }

    info!("position {:?}", consumer.position()?);

    info!("partition assignment...");
    while consumer.assignment()?.count() == 0 {
        info!("try...");
        // let r = consumer.poll(Timeout::After(timeout.clone()));
        let r = consumer.poll(Timeout::After(Duration::from_secs(1)));
        if let Some(r) = r {
            let _msg = r?;
            // print_msg(&_msg)?;
        } else {
            info!(
                "assignment timeout with count {}",
                consumer.assignment()?.count()
            );
            // bail!("assignment timeout");
        }
    }
    info!("partition assignment completed, {:?}", consumer.assignment()?);

    let mut end_pos = if let Some(end) = &args.end {
        let tpl =
        consumer.offsets_for_timestamp(end.0 as i64, Timeout::After(timeout.clone()))?;
        info!("offsets_for_timestamp {:?}, end {}-{:?}", tpl, end.0, end);
        let list = tpl.elements_for_topic(&args.topic);
        for ele in &list {
            if ele.offset().to_raw().is_some() {
                info!(
                    "  end pos: partition {}, offset {:?}",
                    ele.partition(), ele.offset(), 
                );
            }
        }
        Some(EndPos::new(&list)?)
    } else {
        None
    };

    let tpl =
        consumer.offsets_for_timestamp(args.begin.0 as i64, Timeout::After(timeout.clone()))?;
    info!("offsets_for_timestamp {:?}, begin {}-{:?}", tpl, args.begin.0, args.begin);

    for ele in &tpl.elements_for_topic(&args.topic) {
        if ele.offset().to_raw().is_some() {
            info!(
                "  seek begin, partition {}, offset {:?}",
                ele.partition(),
                ele.offset()
            );
            let _r = consumer.seek(
                &args.topic,
                ele.partition(),
                ele.offset(),
                Timeout::After(timeout.clone()),
            )?;
            if let Some(end) = &mut end_pos {
                if let Some(pos) = end.update_by_offset(ele.partition(), ele.offset())? {
                    info!("init reach end: partition {}, offset {:?}, end {}", ele.partition(), ele.offset(), pos);
                }
            }
        }
    }

    
    info!("");
    // use std::str::FromStr;
    // match &args.end {
    //     Some(end) => {
    //         if let Ok(num) = u64::from_str(end) {
    //             return read_loop(&consumer, &args, &timeout, NumEndChecker{num});
    //         }

    //         let end: TimeArg = end.parse().with_context(||"invalid end arg")?;
    //         return read_loop(&consumer, &args, &timeout, MilliEndChecker{milli: end.0});
    //     },
    //     None => return read_loop(&consumer, &args, &timeout, NumEndChecker{num: 1}),
    // }

    let mut stat = Stat::default();
    let mut n = 0;
    while n < args.num {
        let r = consumer.poll(Timeout::After(timeout.clone()));
        if let Some(r) = r {
            let borrowed_message = r?;
            
            let milli = borrowed_message.timestamp().to_millis().with_context(||"fail to parsed kafka ts")? as u64;

            if milli < args.begin.0 {
                info!("skip begin: [{} < {}]", format_milli(milli), format_milli(args.begin.0));
                continue;
            }

            if let Some(end) = &mut end_pos {
                if let Some(pos) = end.update_by_msg(&borrowed_message)? {
                    info!("update reach end: partition {}, offset {:?}, end {}", borrowed_message.partition(), borrowed_message.offset(), pos);
                }

                if end.is_all_end() {
                    info!("reach all partition end");
                    break;
                }
            }

            if let Some(end) = &args.end {
                if milli >= end.0 {
                    // info!("reach end: [{} >= {}]", format_milli(milli), format_milli(end.0));
                    // break;
                    info!("skip end: parti [{}], [{} >= {}]", borrowed_message.partition(), format_milli(milli), format_milli(end.0));
                    continue;
                }
            }

            // borrowed_message.partition();
            // borrowed_message.offset();
            // process_ulink_msg(n+1, &borrowed_message, args, &mut stat)?;
            process_stat_msg(n+1, &borrowed_message, args, &mut stat)?;
            
            n += 1;
        } else {
            error!("read timeout");
            break;
        }
    }

    let s = serde_json::to_string_pretty(&stat)?;
    info!("--- app stats ---", );
    info!("{}", s);
    Ok(())
}
