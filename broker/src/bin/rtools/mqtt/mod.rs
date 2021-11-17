use anyhow::{bail, Context, Result};
use clap::Clap;
use pretty_hex::PrettyHex;
use rust_threeq::tq3::{hex::BinStrLine, tt};
use tracing::{info, warn};

#[derive(Clap, Debug, Clone)]
pub struct SubArgs {
    #[clap(
        long = "addr",
        long_about = "broker address",
        default_value = "127.0.0.1:1883"
    )]
    addr: String,

    #[clap(long = "user", long_about = "username")]
    user: Option<String>,

    #[clap(long = "password", long_about = "password")]
    password: Option<String>,

    #[clap(long = "clientid", long_about = "clientid")]
    clientid: Option<String>,

    #[clap(
        long = "topic",
        long_about = "subscribing topic, support multiple topic, for example t1/t2",
        multiple = true
    )]
    topics: Vec<String>,

    #[clap(
        long = "qos",
        long_about = "subscribing QoS, for example QoS0",
        default_value = "QoS1"
    )]
    qos: tt::QoS,

    #[clap(
        long = "ofmt",
        long_about = "output format",
        default_value = "plaintext"
    )]
    oformat: OFormat,
}

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd)]
pub enum OFormat {
    None,
    PlainText,
    Base64,
    Hex,
}

impl OFormat {
    fn get_dumper(&self) -> Box<dyn PublishDumper> {
        match self {
            OFormat::None => Box::new(()),
            OFormat::PlainText => Box::new(PlainTextDumper),
            OFormat::Base64 => Box::new(Base64Dumper),
            OFormat::Hex => Box::new(HexDumper),
        }
    }
}

impl Default for OFormat {
    fn default() -> Self {
        OFormat::PlainText
    }
}

impl std::str::FromStr for OFormat {
    type Err = String;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        match input.to_lowercase().as_str() {
            "none" => Ok(Self::None),
            "plaintext" => Ok(Self::PlainText),
            "base64" => Ok(Self::Base64),
            "hex" => Ok(Self::Hex),
            s => Err(format!("Unknown output format {}", s)),
        }
    }
}

trait PublishDumper {
    fn dump(&self, pkt: &tt::Publish);
}

impl PublishDumper for () {
    #[inline]
    fn dump(&self, _pkt: &tt::Publish) {}
}

struct PlainTextDumper;
impl PublishDumper for PlainTextDumper {
    fn dump(&self, pkt: &tt::Publish) {
        let r = String::from_utf8(pkt.payload.to_vec());
        match r {
            Ok(s) => info!("  text=[{}]", s),
            Err(e) => {
                warn!("  error={:?}", e)
            }
        }
    }
}

struct Base64Dumper;
impl PublishDumper for Base64Dumper {
    #[inline]
    fn dump(&self, pkt: &tt::Publish) {
        info!("  base64=[{}]", base64::encode(&pkt.payload));
    }
}

struct HexDumper;
impl PublishDumper for HexDumper {
    #[inline]
    fn dump(&self, pkt: &tt::Publish) {
        info!("  hex=[{}]", pkt.payload.hex_dump());
    }
}

impl SubArgs {
    fn make_conn_pkt(&self) -> tt::Connect {
        let clientid: &str = self.clientid.as_deref().unwrap_or("");
        let mut pkt = tt::Connect::new(clientid);
        if self.user.is_some() || self.password.is_some() {
            pkt.set_login(
                self.user.as_deref().unwrap_or(""),
                self.password.as_deref().unwrap_or(""),
            );
        }
        pkt.protocol = tt::Protocol::V4;
        pkt
    }

    fn make_sub_pkt(&self) -> tt::Subscribe {
        let mut filters = Vec::new();
        for topic in &self.topics {
            filters.push(tt::SubscribeFilter::new(topic.clone(), self.qos));
        }
        tt::Subscribe::new_many(filters)
    }
}

pub async fn run_sub(args: &SubArgs) -> Result<()> {
    info!("{:?}", args);
    info!("");

    let (mut sender, mut recver) = tt::client::make_connection("mqtt", &args.addr)
        .await?
        .split();

    let pkt = args.make_conn_pkt();
    let ack = sender
        .connect(pkt)
        .await
        .with_context(|| format!("fail to send connect"))?;
    if ack.code != tt::ConnectReturnCode::Success {
        bail!("{:?}", ack);
    }
    info!("login Ok");

    if args.topics.len() > 0 {
        let pkt = args.make_sub_pkt();
        let ack = sender
            .subscribe(pkt)
            .await
            .with_context(|| format!("fail to subscribe {:?}", args.topics))?;
        for reason in &ack.return_codes {
            if !reason.is_success() {
                info!("subscribe ack fail, {:?}", args.topics);
                bail!("{:?}", ack);
            }
        }
        info!("subscribe Ok, {:?}", args.topics);
    }

    info!("");
    let dumper = args.oformat.get_dumper();
    let mut n = 0_u64;
    loop {
        let r = recver.recv().await?;
        match r {
            tt::client::Event::Packet(packet) => {
                n += 1;
                match packet {
                    tt::Packet::Publish(pkt) => {
                        info!(
                            "No.{}: {:?}, {:?}, {:?}{}{}{}",
                            n,
                            pkt.topic,
                            pkt.qos,
                            pkt.pkid,
                            if pkt.retain { ", retain" } else { "" },
                            if pkt.dup { ", dup" } else { "" },
                            if pkt.properties.is_some() {
                                format!("{:?}", pkt.properties.as_ref().unwrap())
                            } else {
                                "".to_string()
                            }
                        );
                        info!("  payload={:?}", pkt.payload.dump_bin());
                        dumper.dump(&pkt);
                    }
                    _ => {
                        info!("No.{}: {:?}", n, packet);
                    }
                }
            }
            tt::client::Event::Closed(reason) => {
                warn!("got closed, reason {}", reason);
                break;
            }
        }
    }

    Ok(())
}
