use anyhow::{bail, Context, Result};
use bytes::{Buf, BytesMut};
use clap::{Clap, ValueHint};
use log::{debug, info};

use async_trait::async_trait;
use rust_threeq::tq3::{tbytes::PacketDecoder, tt};
use std::convert::TryFrom;
use std::sync::Arc;
use tokio::io::AsyncReadExt;
use tt::FixedHeader;
use tt::PacketType;

#[derive(Clap, Debug, Clone)]
pub struct ReadMqttPcapArgs {
    #[clap(short='f', long = "file", long_about = "pcap file. ", value_hint = ValueHint::FilePath,)]
    file: String,
}

#[async_trait]
trait PacketHandler {
    type Output;
    // async fn handle_packet(&mut self, pkt: T) -> Result<()>;
    async fn handle_connect(&mut self, pkt: tt::Connect) -> Result<Self::Output>;
    async fn handle_conn_ack(&mut self, pkt: tt::ConnAck) -> Result<()>;
    async fn handle_publish(&mut self, pkt: tt::Publish) -> Result<()>;
    async fn handle_pub_ack(&mut self, pkt: tt::PubAck) -> Result<()>;
    async fn handle_pub_rec(&mut self, pkt: tt::PubRec) -> Result<()>;
    async fn handle_pub_rel(&mut self, pkt: tt::PubRel) -> Result<()>;
    async fn handle_pub_comp(&mut self, pkt: tt::PubComp) -> Result<()>;
    async fn handle_subscribe(&mut self, pkt: tt::Subscribe) -> Result<()>;
    async fn handle_sub_ack(&mut self, pkt: tt::SubAck) -> Result<()>;
    async fn handle_unsubscribe(&mut self, pkt: tt::Unsubscribe) -> Result<()>;
    async fn handle_unsub_ack(&mut self, pkt: tt::UnsubAck) -> Result<()>;
    async fn handle_ping_req(&mut self, pkt: tt::PingReq) -> Result<()>;
    async fn handle_ping_resp(&mut self, pkt: tt::PingResp) -> Result<()>;
    async fn handle_disconnect(&mut self, pkt: tt::Disconnect) -> Result<()>;
}

#[derive(Debug)]
struct Parser {
    max_packet_size: usize,
    proto: Option<tt::Protocol>,
}

impl Parser {
    pub fn new(mut max_packet_size: usize, proto: Option<tt::Protocol>) -> Self {
        if max_packet_size == 0 {
            max_packet_size = 65536;
        }
        Self {
            max_packet_size,
            proto,
        }
    }

    pub async fn parse_buf<H: PacketHandler>(
        &mut self,
        buf: &mut BytesMut,
        handler: &mut H,
    ) -> Result<usize> {
        let r = tt::check(buf.iter(), self.max_packet_size);
        match r {
            Ok(h) => {
                let mut bytes = buf.split_to(h.frame_length()).freeze();
                return self.process_packet(&h, &mut bytes, handler).await;
            }
            Err(tt::Error::InsufficientBytes(_required)) => {
                return Ok(0);
            }
            Err(e) => {
                return Err(anyhow::Error::from(e));
            }
        }
    }

    // pub async fn parse_slice<H: PacketHandler>(&mut self, buf: &[u8], handler: &mut H) -> Result<usize> {
    //     let r = tt::check(buf.iter(), self.max_packet_size );
    //     match r {
    //         Ok(h) => {
    //             let mut bytes = &buf[0..h.frame_length()]; // .split_to(h.frame_length()).freeze();
    //             return self.process_packet(&h, &mut bytes, handler).await;
    //         }
    //         Err(tt::Error::InsufficientBytes(_required)) => {
    //             return Ok(0);
    //         }
    //         Err(e) => {
    //             return Err(anyhow::Error::from(e));
    //         }
    //     }
    // }

    pub async fn process_packet<B: Buf, H: PacketHandler>(
        &mut self,
        h: &FixedHeader,
        buf: &mut B,
        handler: &mut H,
    ) -> Result<usize> {
        let len = buf.remaining();

        let packet_type = PacketType::try_from(h.get_type_byte())?;
        if self.proto.is_none() {
            if packet_type != PacketType::Connect {
                bail!("expect first conn packet but {:?}", packet_type);
            }
        }

        match packet_type {
            PacketType::Connect => {
                let pkt = tt::Connect::decode(tt::Protocol::V5, h, buf)?;
                self.proto = Some(pkt.protocol);
                handler.handle_connect(pkt).await?;
            }
            PacketType::ConnAck => {
                let pkt = tt::ConnAck::decode(self.proto.unwrap(), &h, buf)?;
                handler.handle_conn_ack(pkt).await?;
            }
            PacketType::Publish => {
                let pkt = tt::Publish::decode(self.proto.unwrap(), &h, buf)?;
                handler.handle_publish(pkt).await?;
            }
            rust_threeq::tq3::tbytes::PacketType::PubAck => {
                let pkt = tt::PubAck::decode(self.proto.unwrap(), &h, buf)?;
                handler.handle_pub_ack(pkt).await?;
            }
            rust_threeq::tq3::tbytes::PacketType::PubRec => {
                let pkt = tt::PubRec::decode(self.proto.unwrap(), &h, buf)?;
                handler.handle_pub_rec(pkt).await?;
            }
            rust_threeq::tq3::tbytes::PacketType::PubRel => {
                let pkt = tt::PubRel::decode(self.proto.unwrap(), &h, buf)?;
                handler.handle_pub_rel(pkt).await?;
            }
            rust_threeq::tq3::tbytes::PacketType::PubComp => {
                let pkt = tt::PubComp::decode(self.proto.unwrap(), &h, buf)?;
                handler.handle_pub_comp(pkt).await?;
            }
            rust_threeq::tq3::tbytes::PacketType::Subscribe => {
                let pkt = tt::Subscribe::decode(self.proto.unwrap(), &h, buf)?;
                handler.handle_subscribe(pkt).await?;
            }
            rust_threeq::tq3::tbytes::PacketType::SubAck => {
                let pkt = tt::SubAck::decode(self.proto.unwrap(), &h, buf)?;
                handler.handle_sub_ack(pkt).await?;
            }
            rust_threeq::tq3::tbytes::PacketType::Unsubscribe => {
                let pkt = tt::Unsubscribe::decode(self.proto.unwrap(), &h, buf)?;
                handler.handle_unsubscribe(pkt).await?;
            }
            rust_threeq::tq3::tbytes::PacketType::UnsubAck => {
                let pkt = tt::UnsubAck::decode(self.proto.unwrap(), &h, buf)?;
                handler.handle_unsub_ack(pkt).await?;
            }
            rust_threeq::tq3::tbytes::PacketType::PingReq => {
                handler.handle_ping_req(tt::PingReq).await?;
            }
            rust_threeq::tq3::tbytes::PacketType::PingResp => {
                handler.handle_ping_resp(tt::PingResp).await?;
            }
            rust_threeq::tq3::tbytes::PacketType::Disconnect => {
                let pkt = tt::Disconnect::decode(self.proto.unwrap(), &h, buf)?;
                handler.handle_disconnect(pkt).await?;
            }
        }
        // return Ok((packet_type, h, bytes));
        return Ok(len);
    }
}

struct PacketDumper {
    num: u64,
}

impl PacketDumper {
    #[inline]
    async fn handle_pkt<T: 'static + std::fmt::Debug>(&mut self, pkt: T) -> Result<()> {
        debug!("======No.{}", self.num);
        debug!("{:?}\n", pkt);
        self.num += 1;
        Ok(())
    }
}

#[async_trait]
impl PacketHandler for PacketDumper {
    type Output = ();

    async fn handle_connect(&mut self, pkt: tt::Connect) -> Result<Self::Output> {
        self.handle_pkt(pkt).await
    }

    async fn handle_conn_ack(&mut self, pkt: tt::ConnAck) -> Result<()> {
        self.handle_pkt(pkt).await
    }

    async fn handle_publish(&mut self, pkt: tt::Publish) -> Result<()> {
        self.handle_pkt(pkt).await
    }

    async fn handle_pub_ack(&mut self, pkt: tt::PubAck) -> Result<()> {
        self.handle_pkt(pkt).await
    }

    async fn handle_pub_rec(&mut self, pkt: tt::PubRec) -> Result<()> {
        self.handle_pkt(pkt).await
    }

    async fn handle_pub_rel(&mut self, pkt: tt::PubRel) -> Result<()> {
        self.handle_pkt(pkt).await
    }

    async fn handle_pub_comp(&mut self, pkt: tt::PubComp) -> Result<()> {
        self.handle_pkt(pkt).await
    }

    async fn handle_subscribe(&mut self, pkt: tt::Subscribe) -> Result<()> {
        self.handle_pkt(pkt).await
    }

    async fn handle_sub_ack(&mut self, pkt: tt::SubAck) -> Result<()> {
        self.handle_pkt(pkt).await
    }

    async fn handle_unsubscribe(&mut self, pkt: tt::Unsubscribe) -> Result<()> {
        self.handle_pkt(pkt).await
    }

    async fn handle_unsub_ack(&mut self, pkt: tt::UnsubAck) -> Result<()> {
        self.handle_pkt(pkt).await
    }

    async fn handle_ping_req(&mut self, pkt: tt::PingReq) -> Result<()> {
        self.handle_pkt(pkt).await
    }

    async fn handle_ping_resp(&mut self, pkt: tt::PingResp) -> Result<()> {
        self.handle_pkt(pkt).await
    }

    async fn handle_disconnect(&mut self, pkt: tt::Disconnect) -> Result<()> {
        self.handle_pkt(pkt).await
    }
}

async fn run_read_mqtt_bin_file(file: &str) -> Result<()> {
    use tokio::fs::File;

    let mut packet_logger = PacketDumper { num: 0 };
    let mut parser = Parser::new(0, Some(tt::Protocol::V4));

    let mut file = File::open(file)
        .await
        .with_context(|| format!("fail to open {}", file))?;

    let mut buf = BytesMut::new();

    loop {
        let _r = parser.parse_buf(&mut buf, &mut packet_logger).await?;
        let r = file.read_buf(&mut buf).await.with_context(|| "read fail")?;
        if r == 0 {
            debug!("reach file end");
            break;
        }
    }

    Ok(())
}

// async fn run_file(file: &str) -> Result<()> {

//     use pcap::Capture;
//     use pcap::Linktype;
//     use rust_threeq::tq3::hex::BinStrLine;
//     let mut cap = Capture::from_file(file).with_context(|| format!("fail to open {}", file))?;
//     // cap.filter("tcp port 1883", true)?;
//     let link_type = cap.get_datalink();
//     debug!(
//         "link type {:?}, {}, {}",
//         link_type,
//         link_type.get_name()?,
//         link_type.get_description()?
//     );
//     let mut num = 0u64;
//     while let Ok(packet) = cap.next() {
//         let data = packet.data.to_owned();
//         let len = packet.header.len;
//         let ts: String = format!(
//             "{}.{:06}",
//             &packet.header.ts.tv_sec, &packet.header.ts.tv_usec
//         );
//         num += 1;
//         debug!("== No.{}, {}, {}, {}", num, ts, len, data.dump_bin());

//         let value = if link_type == Linktype::NULL {
//             etherparse::SlicedPacket::from_ip(&data[4..])?
//         } else {
//             etherparse::SlicedPacket::from_ethernet(&data[0..])?
//         };
//         debug!("  link: {:?}", value.link);
//         debug!("  vlan: {:?}", value.vlan);
//         debug!("  ip: {:?}", value.ip);
//         match &value.transport {
//             Some(tslice) => match tslice {
//                 etherparse::TransportSlice::Udp(_udp) => {
//                     debug!("  transport: {:?}", value.transport);
//                 }
//                 etherparse::TransportSlice::Tcp(tcp) => {
//                     let ip = match value.ip.unwrap() {
//                         etherparse::InternetSlice::Ipv4(ipv4) => {
//                             let src = ipv4.source_addr();
//                             let dst = ipv4.destination_addr();
//                             (src.to_string(), dst.to_string())
//                         }
//                         etherparse::InternetSlice::Ipv6(_ipv6, _) => todo!(),
//                     };
//                     debug!(
//                         "  transport: {}:{} => {}:{}",
//                         ip.0,
//                         tcp.source_port(),
//                         ip.1,
//                         tcp.destination_port(),
//                     );
//                     debug!("    raw {}", tcp.slice().dump_bin());
//                 }
//             },
//             None => {
//                 debug!("  transport: {:?}", value.transport);
//             }
//         }
//         debug!("  payload: {:?}", value.payload.dump_bin());

//         // match etherparse::PacketHeaders::from_ethernet_slice(&packet) {
//         //     Err(value) => println!("Err {:?}", value),
//         //     Ok(value) => {
//         //         println!("  link: {:?}", value.link);
//         //         println!("  vlan: {:?}", value.vlan);
//         //         println!("  ip: {:?}", value.ip);
//         //         println!("  transport: {:?}", value.transport);
//         //     }
//         // }

//         // let packets = packets.clone();

//         // pool.execute(move || {
//         //     let packet_parse = PacketParse::new();
//         //     let parsed_packet = packet_parse.parse_packet(data, len, ts);

//         //     packets.lock().unwrap().push(parsed_packet);
//         // });
//     }

//     std::process::exit(0);
//     // Ok(())
// }

use libpcap_analyzer::default_plugin_builder;
use libpcap_analyzer::packet_info::PacketInfo;
use libpcap_analyzer::{Plugin, PluginResult, PLUGIN_FLOW_DEL, PLUGIN_FLOW_NEW, PLUGIN_L4};
use libpcap_tools::pcap_parser::nom::HexDisplay;
use libpcap_tools::{Flow, Packet};

#[derive(Default)]
pub struct MqttDump;
default_plugin_builder!(MqttDump, MqttDumpBuilder);

impl Plugin for MqttDump {
    fn name(&self) -> &'static str {
        "MqttDump"
    }

    fn plugin_type(&self) -> u16 {
        PLUGIN_FLOW_NEW | PLUGIN_FLOW_DEL | PLUGIN_L4
    }

    fn flow_created(&mut self, flow: &Flow) {
        info!("MqttDump::flow_created: {:?}", flow);
    }

    fn flow_destroyed(&mut self, flow: &Flow) {
        info!("MqttDump::flow_destroyed: {:?}", flow);
    }

    fn handle_layer_transport<'s, 'i>(
        &'s mut self,
        _packet: &'s Packet,
        pinfo: &PacketInfo,
    ) -> PluginResult<'i> {
        let five_tuple = &pinfo.five_tuple;
        info!("MqttDump::handle_l4");
        debug!("    5-t: {}", five_tuple);
        debug!("    to_server: {}", pinfo.to_server);
        debug!("    l3_type: 0x{:x}", pinfo.l3_type);
        debug!("    l4_data_len: {}", pinfo.l4_data.len());
        debug!("    l4_type: {}", pinfo.l4_type);
        debug!(
            "    l4_payload_len: {}",
            pinfo.l4_payload.map_or(0, |d| d.len())
        );
        if let Some(flow) = pinfo.flow {
            let five_tuple = &flow.five_tuple;
            debug!(
                "    flow: [{}]:{} -> [{}]:{} [{}]",
                five_tuple.src,
                five_tuple.src_port,
                five_tuple.dst,
                five_tuple.dst_port,
                five_tuple.proto
            );
        }
        if let Some(d) = pinfo.l4_payload {
            debug!("    l4_payload:\n{}", d.to_hex(16));
        }
        PluginResult::None
    }
}

async fn run_tools(file: &str) -> Result<()> {
    // TODO: libpcap_analyzer do NOT support tcp disconnect ？

    use libpcap_analyzer::*;
    use libpcap_tools::{Config, PcapDataEngine, PcapEngine};

    let mut config = Config::default();
    config.set("do_checksums", false);

    let mut registry = PluginRegistry::new();
    let builder = Box::new(MqttDumpBuilder);
    let r = builder.build(&mut registry, &config);
    if let Err(e) = r {
        bail!("build plugin fail, {:?}", e);
    }

    debug!("Plugins loaded:");
    registry.run_plugins(
        |_| true,
        |p| {
            debug!("  {}", p.name());
        },
    );

    let mut input_reader = {
        let file = std::fs::File::open(file)?;
        Box::new(file) as Box<dyn std::io::Read>
    };

    let mut engine = {
        let analyzer = Analyzer::new(Arc::new(registry), &config);
        Box::new(PcapDataEngine::new(analyzer, &config)) as Box<dyn PcapEngine>
    };
    engine.run(&mut input_reader).expect("run analyzer");

    std::process::exit(0);
    // Ok(())
}

pub async fn run_read_mqtt_pcap_file(args: &ReadMqttPcapArgs) -> Result<()> {
    if args.file.is_empty() {
        run_read_mqtt_bin_file(&args.file).await?;
        return Ok(());
    } else {
        // run_file(&args.file).await?;
        run_tools(&args.file).await?;
    }

    use pcap_parser::traits::PcapReaderIterator;
    use pcap_parser::PcapNGReader;
    use pcap_parser::*;
    use std::fs::File;

    let file = File::open(&args.file).with_context(|| format!("fail to open {}", args.file))?;

    let mut num_legacy = 0u64;
    let mut num_legacy_header = 0;
    let mut num_ng = 0;
    let mut num_blocks = 0;

    let mut num_sections = 0u64;
    let mut num_ifdescs = 0u64;
    let mut num_epackets = 0u64;
    let mut num_ifstats = 0u64;
    let mut num_others = 0u64;

    let mut reader =
        PcapNGReader::new(65536, file).with_context(|| format!("fail parse pcap {}", args.file))?;
    loop {
        match reader.next() {
            Ok((offset, block)) => {
                debug!("== No.{}, offset {}", num_blocks, offset);
                match block {
                    PcapBlockOwned::Legacy(b) => {
                        debug!("  {:?}", &b);
                        num_legacy += 1;
                    }
                    PcapBlockOwned::LegacyHeader(b) => {
                        debug!("  {:?}", b);
                        num_legacy_header += 1;
                    }
                    PcapBlockOwned::NG(blk) => {
                        debug!("  block: {:?}", blk);
                        match blk {
                            Block::SectionHeader(_) => {
                                num_sections += 1;
                            }
                            Block::InterfaceDescription(_) => {
                                num_ifdescs += 1;
                            }
                            Block::EnhancedPacket(_blk) => {
                                num_epackets += 1;
                            }
                            Block::InterfaceStatistics(_) => {
                                num_ifstats += 1;
                            }
                            _ => {
                                num_others += 1;
                            }
                        }
                        num_ng += 1;
                    }
                }
                num_blocks += 1;
                reader.consume(offset);
            }
            Err(PcapError::Eof) => break,
            Err(PcapError::Incomplete) => {
                reader.refill().unwrap();
            }
            Err(e) => panic!("error while reading: {:?}", e),
        }
    }
    debug!("num_legacy: {}", num_legacy);
    debug!("num_legacy_header: {}", num_legacy_header);
    debug!("num_ng: {}", num_ng);
    debug!("num_blocks: {}", num_blocks);
    debug!("  num_sections: {}", num_sections);
    debug!("  num_ifdescs: {}", num_ifdescs);
    debug!("  num_ifstats: {}", num_ifstats);
    debug!("  num_epackets: {}", num_epackets);
    debug!("  num_others: {}", num_others);

    Ok(())
}
