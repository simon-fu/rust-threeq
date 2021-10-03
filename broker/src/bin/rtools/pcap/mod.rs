use anyhow::{bail, Context, Result};
use bytes::{Buf, BytesMut};
use clap::{Clap, ValueHint};
use log::debug;

use async_trait::async_trait;
use pcap::Linktype;
use rust_threeq::tq3::hex::BinStrLine;
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

// fn build_tcp_packet() -> Vec<u8>{

//     //setup the packet headers
//     let builder = etherparse::PacketBuilder::
//     ethernet2([1,2,3,4,5,6],     //source mac
//                [7,8,9,10,11,12]) //destionation mac
//     .ipv4([192,168,1,1], //source ip
//           [192,168,1,2], //desitionation ip
//           20)            //time to life
//     .tcp(21,    //source port
//          1234,  //desitnation port
//          1,     //sequence number
//          26180) //window size

//     //set additional tcp header fields
//     .ns() //set the ns flag
//     //supported flags: ns(), fin(), syn(), rst(), psh(), ece(), cwr()
//     .ack(123) //ack flag + the ack number
//     .urg(23) //urg flag + urgent pointer

//     //tcp header options
//     .options(&[
//         etherparse::TcpOptionElement::Nop,
//         etherparse::TcpOptionElement::MaximumSegmentSize(1234)
//     ]).unwrap();

//     //payload of the tcp packet
//     let payload = [1,2,3,4,5,6,7,8];

//     //get some memory to store the result
//     let mut result = Vec::<u8>::with_capacity(
//                     builder.size(payload.len()));

//     //serialize
//     //this will automatically set all length fields, checksums and identifiers (ethertype & protocol)
//     builder.write(&mut result, &payload).unwrap();
//     println!("{:?}", result);
//     result
// }

async fn run_file(file: &str) -> Result<()> {
    // let packet = build_tcp_packet();
    // debug!("build tcp packet:");
    // match etherparse::SlicedPacket::from_ethernet(&packet) {
    //     Err(value) => println!("Err {:?}", value),
    //     Ok(value) => {
    //         debug!("  link: {:?}", value.link);
    //         debug!("  vlan: {:?}", value.vlan);
    //         debug!("  ip: {:?}", value.ip);
    //         debug!("  transport: {:?}", value.transport);
    //     }
    // }

    use pcap::Capture;
    let mut cap = Capture::from_file(file).with_context(|| format!("fail to open {}", file))?;
    // cap.filter("tcp port 1883", true)?;
    let link_type = cap.get_datalink();
    debug!(
        "link type {:?}, {}, {}",
        link_type,
        link_type.get_name()?,
        link_type.get_description()?
    );
    let mut num = 0u64;
    while let Ok(packet) = cap.next() {
        let data = packet.data.to_owned();
        let len = packet.header.len;
        let ts: String = format!(
            "{}.{:06}",
            &packet.header.ts.tv_sec, &packet.header.ts.tv_usec
        );
        num += 1;
        debug!("== No.{}, {}, {}, {}", num, ts, len, data.dump_bin());

        let value = if link_type == Linktype::NULL {
            etherparse::SlicedPacket::from_ip(&data[4..])?
        } else {
            etherparse::SlicedPacket::from_ethernet(&data[0..])?
        };
        debug!("  link: {:?}", value.link);
        debug!("  vlan: {:?}", value.vlan);
        debug!("  ip: {:?}", value.ip);
        match &value.transport {
            Some(tslice) => match tslice {
                etherparse::TransportSlice::Udp(_udp) => {
                    debug!("  transport: {:?}", value.transport);
                }
                etherparse::TransportSlice::Tcp(tcp) => {
                    let ip = match value.ip.unwrap() {
                        etherparse::InternetSlice::Ipv4(ipv4) => {
                            let src = ipv4.source_addr();
                            let dst = ipv4.destination_addr();
                            (src.to_string(), dst.to_string())
                        }
                        etherparse::InternetSlice::Ipv6(_ipv6, _) => todo!(),
                    };
                    debug!(
                        "  transport: {}:{} => {}:{}",
                        ip.0,
                        tcp.source_port(),
                        ip.1,
                        tcp.destination_port(),
                    );
                    debug!("    raw {}", tcp.slice().dump_bin());
                }
            },
            None => {
                debug!("  transport: {:?}", value.transport);
            }
        }
        debug!("  payload: {:?}", value.payload.dump_bin());

        // match etherparse::PacketHeaders::from_ethernet_slice(&packet) {
        //     Err(value) => println!("Err {:?}", value),
        //     Ok(value) => {
        //         println!("  link: {:?}", value.link);
        //         println!("  vlan: {:?}", value.vlan);
        //         println!("  ip: {:?}", value.ip);
        //         println!("  transport: {:?}", value.transport);
        //     }
        // }

        // let packets = packets.clone();

        // pool.execute(move || {
        //     let packet_parse = PacketParse::new();
        //     let parsed_packet = packet_parse.parse_packet(data, len, ts);

        //     packets.lock().unwrap().push(parsed_packet);
        // });
    }

    std::process::exit(0);
    // Ok(())
}

async fn run_tools(file: &str) -> Result<()> {
    use libpcap_analyzer::*;
    use libpcap_tools::{Config, PcapDataEngine, PcapEngine};

    let factory = plugins::PluginsFactory::default();
    println!("pcap-analyzer available plugin builders:");
    factory.iter_builders(|name| println!("    {}", name));

    let config = Config::default();
    let registry = factory
        .build_plugins(&config)
        .expect("Could not build factory");
    {
        println!("pcap-analyzer instanciated plugins:");
        registry.run_plugins(
            |_| true,
            |p| {
                println!("  {}", p.name());
                let t = p.plugin_type();
                print!("    layers: ");
                if t & PLUGIN_L2 != 0 {
                    print!("  L2");
                }
                if t & PLUGIN_L3 != 0 {
                    print!("  L3");
                }
                if t & PLUGIN_L4 != 0 {
                    print!("  L4");
                }
                println!();
                print!("    events: ");
                if t & PLUGIN_FLOW_NEW != 0 {
                    print!("  FLOW_NEW");
                }
                if t & PLUGIN_FLOW_DEL != 0 {
                    print!("  FLOW_DEL");
                }
                println!();
            },
        );
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

    // use libpcap_tools::{Config, Error, Packet, ParseContext, PcapAnalyzer, PcapDataEngine, PcapEngine};
    // #[derive(Default)]
    // pub struct ExampleAnalyzer {
    //     packet_count: usize,
    // }

    //  impl PcapAnalyzer for ExampleAnalyzer {
    //      fn handle_packet(&mut self, packet: &Packet, ctx: &ParseContext) -> Result<(), Error> {
    //          debug!("{:?}, {:?}", packet, ctx.first_packet_ts);
    //          Ok(())
    //      }
    // }

    // let mut file = std::fs::File::open(file)
    // .with_context(|| format!("fail to open {}", file))?;

    // let config = Config::default();
    // let analyzer = ExampleAnalyzer::default();
    // let mut engine = PcapDataEngine::new(analyzer, &config);
    // let _res = engine.run(&mut file).with_context(||"pcap engine run fail");

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
