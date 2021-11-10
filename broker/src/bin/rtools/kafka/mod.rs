
use std::{time::{Duration}};
use bytes::Buf;
use clap::{Clap};
use anyhow::{Result, bail};
use log::info;
use rdkafka::{ClientConfig, Message, consumer::{BaseConsumer, Consumer}, message::BorrowedMessage, util::Timeout};
use rust_threeq::tq3::{tbytes::PacketDecoder, tt::{self, Protocol}};
use tracing::error;

use crate::util::{TimeArg, MatchTopic, MatchPayloadText, MatchFlag};

#[derive(Clap, Debug, Clone)]
pub struct ReadArgs {
    #[clap(long = "topic", long_about = "topic to read")]
    topic: String,

    #[clap(long = "group", long_about = "kafka group", default_value = "rtools")]
    group: String,

    #[clap(long = "addr", long_about = "kafka address", default_value = "127.0.0.1:9092")]
    addr: String,

    #[clap(
        long = "begin",
        long_about = "begin position time to read from",
        default_value = "2021-01-01T00:00:00"
    )]
    begin: TimeArg,

    #[clap(long = "num", long_about = "num of messages to read", default_value = "1")]
    num: u64,

    #[clap(long = "match-topic", long_about = "optional, regex match topic")]
    match_topic: Option<MatchTopic>,

    #[clap(
        long = "match-text",
        long_about = "optional, regex match payload text"
    )]
    match_text: Option<MatchPayloadText>,

    #[clap(long = "timeout", long_about = "timeout in seconds", default_value = "10")]
    timeout_sec: u64,
}


fn print_metadata(brokers: &str, topic: Option<&str>, timeout: Duration, fetch_offsets: bool) {
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .create()
        .expect("Consumer creation failed");

    let metadata = consumer
        .fetch_metadata(topic, timeout.clone())
        .expect("Failed to fetch metadata");

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
}

fn print_msg(n: u64, borrowed_message: &BorrowedMessage, args: &ReadArgs) -> Result<()>{
    let msg = borrowed_message.detach();
    // info!("msg {:?}", msg);
    let payload = msg.payload().unwrap();
    let mut cursor = payload;

    let ts = {
        let r = msg.timestamp().to_millis();
        if let Some(n) = r {
            TimeArg(n as u64).format()

        } else {
            "None".into()
        }
    };
    
    let mid = cursor.get_u64(); // MID:8/binary
    cursor.advance(10); // Expiry:10/binary
    let ver = cursor.get_u8(); // Version:1/binary
    let fixed_header = tt::check(cursor.iter(), 65536)?;
    let packet = tt::Publish::decode(Protocol::from_u8(ver)?, &fixed_header, &mut cursor)?;
    let payload = {
        let r = String::from_utf8(packet.payload.to_vec());
        match r {
            Ok(s) => s,
            Err(_) => "".into(),
        }
    };
    let mut flag = MatchFlag::default();
    flag.match_text(&args.match_topic, &packet.topic);
    flag.match_utf8(&args.match_text, packet.payload.to_vec());

    let s = format!(
        "--- No.{}: time [{}], mid: [{:#016X}], topic [{}], ver {}, len {}, payload: [{}]",
        n+1,
        ts,
        mid,
        packet.topic, ver,
        packet.payload.len(), payload
    );

    info!("{}", s);
    info!("");

    Ok(())
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

    print_metadata(&args.addr, Some(&args.topic), timeout.clone(), true);

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
        info!("  ele partition {}, offset {:?}", ele.partition(), ele.offset());
    }

    info!("position {:?}", consumer.position()?);

    info!("assignment...");
    while consumer.assignment()?.count() == 0 {
        let r = consumer.poll(Timeout::After(timeout.clone()));
        if let Some(r) = r {
            let _msg = r?;
            // print_msg(&_msg)?;
        } else {
            info!("assignment timeout with count {}", consumer.assignment()?.count());
            bail!("assignment timeout");
        }
    }
    info!("assignment completed, {:?}", consumer.assignment()?);

    let tpl = consumer.offsets_for_timestamp(args.begin.0 as i64, Timeout::After(timeout.clone()))?;
    info!("offsets_for_timestamp {:?}", tpl);

    for ele in &tpl.elements_for_topic(&args.topic) {
        if ele.offset().to_raw().is_some() {
            info!("  seek partition {}, offset {:?}", ele.partition(), ele.offset());
            let _r = consumer.seek(&args.topic, ele.partition(), ele.offset(), Timeout::After(timeout.clone()))?; 
        }
    }
    
    info!("");
    let mut n = 0;
    while n < args.num {
        let r = consumer.poll(Timeout::After(timeout.clone()));
        if let Some(r) = r {
            let borrowed_message = r?;
            print_msg(n, &borrowed_message, args)?;
            n += 1;
        } else {
            error!("read timeout");
            break;
        }
    }
    
    Ok(())
}
