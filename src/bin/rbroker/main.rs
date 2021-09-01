/*
TODO:
- support QoS1, Qos2, QoS match
- support retain, and publish empty msg to clean retain
- support cluster
- support in-flight window
- support publish topic alias
- support redis
- support kafka
- support websocket over mqtt and pure websocket
- support local disk storage
*/

use std::{collections::HashSet, sync::Arc, time::Duration};

use bytes::{Bytes, BytesMut};
use clap::Clap;
use rust_threeq::tq3::{self, tt};
use std::convert::TryFrom;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{
        tcp::{ReadHalf, WriteHalf},
        TcpListener, TcpStream,
    },
    select,
    sync::broadcast,
    time::Instant,
};
use tracing::{debug, error, info};

mod discovery;
mod hub;
mod registry;
mod znodes;
mod zrpc;
mod zserver;

// refer https://doc.rust-lang.org/reference/conditional-compilation.html?highlight=target_os#target_os
// refer https://doc.rust-lang.org/rust-by-example/attribute/cfg.html
// refer https://cloud.tencent.com/developer/article/1138651
fn call_malloc_trim() -> bool {
    #[cfg(target_os = "linux")]
    {
        extern "C" {
            fn malloc_trim(pad: usize) -> i32;
        }

        let freed = unsafe { malloc_trim(128 * 1024) };
        return if freed == 0 { false } else { true };
        //debug!("malloc_trim freed {}", freed);
    }

    #[cfg(not(target_os = "linux"))]
    {
        false
    }
}

// refer https://github.com/clap-rs/clap/tree/master/clap_derive/examples
#[derive(Clap, Debug, Default)]
#[clap(name = "threeq broker", author, about, version)]
struct Config {
    #[clap(
        short = 'l',
        long = "tcp-listen",
        default_value = "0.0.0.0:1883",
        long_about = "tcp listen address."
    )]
    tcp_listen_addr: String,

    #[clap(
        short = 'g',
        long = "enable_gc",
        long_about = "enable memory garbage collection"
    )]
    enable_gc: bool,

    #[clap(long = "node", long_about = "this node id", default_value = " ")]
    node_id: String,

    #[clap(long = "seed", long_about = "seed node address", default_value = " ")]
    seed: String,

    #[clap(
        long = "cluster-listen",
        long_about = "cluster listen address",
        default_value = "127.0.0.1:50051"
    )]
    cluster_listen_addr: String,
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("I/O error: {0}")]
    IoError(#[from] std::io::Error),

    #[error("Packet parsing error: {0}")]
    TtError(#[from] tt::Error),

    #[error("Timeout:{0}")]
    ElapsedError(#[from] tokio::time::error::Elapsed),

    #[error("packet type error: {0}")]
    PackettypeError(#[from] num_enum::TryFromPrimitiveError<tt::PacketType>),

    #[error("expect connect packet but got :{0:?}")]
    ExpectConnectPacket(tt::PacketType),

    #[error("unexpect packet type: {0:?}")]
    UnexpectPacket(tt::PacketType),

    #[error("error: {0}")]
    Generic(String),
}

impl Error {
    fn broken_pipe<E>(reason: E) -> Self
    where
        E: Into<Box<dyn std::error::Error + Send + Sync>>,
    {
        Error::from(std::io::Error::new(std::io::ErrorKind::BrokenPipe, reason))
    }

    fn timeout<E>(reason: E) -> Self
    where
        E: Into<Box<dyn std::error::Error + Send + Sync>>,
    {
        Error::from(std::io::Error::new(std::io::ErrorKind::TimedOut, reason))
    }
}

type ThizResult<T> = core::result::Result<T, Error>;

struct Reader<'a> {
    rd: ReadHalf<'a>,
    ibuf: BytesMut,
}

impl<'a> Reader<'a> {
    pub fn new(rd: ReadHalf<'a>) -> Self {
        Self {
            rd,
            ibuf: BytesMut::new(),
        }
    }
    pub async fn read_packet(
        &mut self,
        max_packet_size: usize,
    ) -> ThizResult<(tt::PacketType, tt::FixedHeader, Bytes)> {
        loop {
            let r = tt::check(self.ibuf.iter(), max_packet_size);
            match r {
                Ok(h) => {
                    let bytes = self.ibuf.split_to(h.frame_length()).freeze();
                    let packet_type = tt::PacketType::try_from(h.get_type_byte())?;
                    return Ok((packet_type, h, bytes));
                }
                Err(tt::Error::InsufficientBytes(_required)) => {}
                Err(e) => {
                    return Err(Error::from(e));
                }
            }

            let n = self.rd.read_buf(&mut self.ibuf).await?;
            if n == 0 {
                return Err(Error::broken_pipe("read disconnect"));
            }
        }
    }
}

struct Writer<'a> {
    wr: WriteHalf<'a>,
    obuf: BytesMut,
}

impl<'a> Writer<'a> {
    pub fn new(wr: WriteHalf<'a>) -> Self {
        Self {
            wr,
            obuf: BytesMut::new(),
        }
    }
}

struct Session {
    info: Arc<hub::SessionInfo>,
    protocol: tt::Protocol,
    rx_bc: hub::BcRecver,
    rx_ctrl: hub::CtrlRecver,
    max_incoming_size: usize,
    keep_alive_ms: u64,
    // conn_pkt: tt::Connect,
    packet_id: tt::PacketId,
    last_active_time: Instant,
    disconnected: bool,
    topic_filters: HashSet<String>,
    // pub_topic_cache: HashMap<String, Arc<hub::Topic>>,
    last_pub_topic: Option<hub::PubTopic>,
}

const CONNECT_MAX_PACKET_SIZE: usize = 16 * 1024;
const DEFAUT_KEEP_ALIVE_SEC: u16 = 60;

fn generate_client_id() -> String {
    let duration = std::time::SystemTime::now()
        .duration_since(std::time::SystemTime::UNIX_EPOCH)
        .unwrap();
    let in_nanos = duration.as_secs() * 1_000_000 + duration.subsec_nanos() as u64;
    format!("{}@abcdef", in_nanos)
}

async fn service_connect(
    uid: u64,
    reader: &mut Reader<'_>,
    writer: &mut Writer<'_>,
) -> ThizResult<Box<Session>> {
    let (ptype, header, bytes) = reader.read_packet(CONNECT_MAX_PACKET_SIZE).await?;

    if ptype != tt::PacketType::Connect {
        return Err(Error::Generic(format!(
            "expect first connect but {:?}",
            ptype
        )));
    }

    let mut conn_pkt = tt::Connect::read(header, bytes)?;
    let mut conn_ack = tt::ConnAck::new(tt::ConnectReturnCode::Success, false);
    conn_ack.properties = Some(tt::ConnAckProperties::new());

    if conn_pkt.client_id.is_empty() {
        conn_pkt.client_id = generate_client_id();
        debug!("generate clientid {}", conn_pkt.client_id);

        conn_ack
            .properties
            .as_mut()
            .unwrap()
            .assigned_client_identifier = Some(conn_pkt.client_id.clone());
    } else {
        debug!("connect with  clientid {}", conn_pkt.client_id);
    }

    if conn_pkt.keep_alive == 0 {
        conn_pkt.keep_alive = DEFAUT_KEEP_ALIVE_SEC;
    }

    let _ = conn_ack.encode(conn_pkt.protocol, &mut writer.obuf);

    let session = Box::new(Session::new(uid, hub::get().clone(), &conn_pkt));

    if let Some(info) = session.info.hub.add_session(session.info.clone()) {
        debug!("kick same clientid {:?}", info);
        let _r = info.tx_ctrl.send(hub::CtrlData::KickByOther).await;
    }

    Ok(session)
}

async fn session_entry(uid: u64, mut socket: TcpStream) {
    debug!("connected from {:?}", socket.peer_addr().unwrap());

    let (rd, wr) = socket.split();
    let mut reader = Reader::new(rd);
    let mut writer = Writer::new(wr);

    // let r = Box::pin(service_connect(uid, &mut reader, &mut writer)).await;
    let r = service_connect(uid, &mut reader, &mut writer).await;
    let mut session = match r {
        Err(e) => {
            debug!("service connect finished with [{:?}]", e);
            return;
        }
        Ok(ss) => ss,
    };

    let r = session.run(&mut reader, &mut &mut writer).await;
    match r {
        Err(e) => {
            debug!("session finished with [{:?}]", e);
            return;
        }
        Ok(_r) => {}
    }
    session.cleanup().await;
}

impl Session {
    pub fn new(uid: u64, hub: hub::Hub, conn_pkt: &tt::Connect) -> Self {
        let (tx_bc, rx_bc) = hub::make_bc_pair();
        let (tx_ctrl, rx_ctrl) = hub::make_ctrl_pair();
        let info = hub::SessionInfo::new(hub, uid, conn_pkt.client_id.clone(), tx_bc, tx_ctrl);

        Session {
            info,
            protocol: conn_pkt.protocol,
            max_incoming_size: 64 * 1024,
            keep_alive_ms: conn_pkt.keep_alive as u64 * 1000 * 3 / 2,
            // conn_pkt: tt::Connect::new(""),
            packet_id: tt::PacketId::default(),
            last_active_time: Instant::now(),
            rx_bc,
            rx_ctrl,
            disconnected: false,
            topic_filters: HashSet::new(),
            // pub_topic_cache: HashMap::new(),
            last_pub_topic: None,
        }
    }

    pub async fn cleanup(&mut self) {
        for v in &self.topic_filters {
            // self.hub.unsubscribe(v, self.uid).await;
            self.info.hub.unsubscribe(v, self.info.uid).await;
        }
        self.topic_filters.clear();

        self.last_pub_topic.take();

        self.info.hub.remove_session(&self.info);

        // for r in &self.pub_topic_cache {
        //     hub::get().release_topic(r.0).await;
        // }
        // self.pub_topic_cache.clear();
    }

    // fn check_connect(&mut self) {
    //     if self.conn_pkt.client_id.is_empty() {
    //         let duration = std::time::SystemTime::now()
    //             .duration_since(std::time::SystemTime::UNIX_EPOCH)
    //             .unwrap();
    //         let in_nanos = duration.as_secs() * 1_000_000 + duration.subsec_nanos() as u64;
    //         self.conn_pkt.client_id = format!("{}@abcdef", in_nanos); //UNIX_EPOCH
    //     }

    //     self.keep_alive_ms = self.conn_pkt.keep_alive as u64 * 1000 * 2;
    //     if self.keep_alive_ms == 0 {
    //         self.keep_alive_ms = 30 * 1000;
    //     }
    // }

    fn check_alive(&self) -> ThizResult<u64> {
        let now = Instant::now();
        let elapsed = {
            if now > self.last_active_time {
                (now - self.last_active_time).as_millis()
            } else {
                0
            }
        } as u64;

        if self.keep_alive_ms > elapsed {
            return Ok(self.keep_alive_ms - elapsed);
        } else {
            let reason = format!("keep alive timeout {} millis", self.keep_alive_ms);
            return Err(Error::timeout(reason));
        }
    }

    pub async fn run(
        &mut self,
        reader: &mut Reader<'_>,
        writer: &mut Writer<'_>,
    ) -> ThizResult<()> {
        self.xfer_loop(reader, writer).await?;

        while !self.disconnected {
            let rd_timeout = self.check_alive()?;
            let dead_line = Instant::now() + Duration::from_millis(rd_timeout);
            let mut dump_buf = [0u8; 1];

            select! {

                //r = socket.ready(Interest::READABLE) =>{
                r = reader.rd.peek(&mut dump_buf) =>{
                    match r{
                        Ok(_) => {
                            self.xfer_loop(reader, writer).await?;
                        },
                        Err(e) => {
                            return Err(Error::from(e));
                        },
                    }
                }

                r = hub::recv_bc(&mut self.rx_bc) => {
                    match r{
                        Ok(d) => {
                            self.handle_bc_event(d, writer).await?;
                            self.xfer_loop(reader, writer).await?;
                        },
                        Err(broadcast::error::RecvError::Lagged(n)) => {
                            debug!("lagged {}", n);
                        },
                        Err(_e) => { }
                    }
                }

                r = hub::recv_ctrl(&mut self.rx_ctrl) => {
                    match r {
                        Some(d) => {
                            self.handle_ctrl_event(d, writer).await?;
                            self.xfer_loop(reader, writer).await?;
                        }
                        None => {},
                    }
                }

                _ = tokio::time::sleep_until(dead_line) => {

                }
            }
        }
        Ok(())
    }

    async fn handle_connect(
        &mut self,
        fixed_header: tt::FixedHeader,
        bytes: Bytes,
        obuf: &mut BytesMut,
    ) -> ThizResult<()> {
        // self.conn_pkt = tt::Connect::read(fixed_header, bytes)?;
        // let is_empty_clientid = self.conn_pkt.client_id.is_empty();
        // self.check_connect();

        // let mut connack = tt::ConnAck::new(tt::ConnectReturnCode::Success, false);

        // connack.properties = Some(tt::ConnAckProperties::new());
        // if is_empty_clientid {
        //     connack
        //         .properties
        //         .as_mut()
        //         .unwrap()
        //         .assigned_client_identifier = Some(self.conn_pkt.client_id.clone());
        // }

        // let _ = connack.encode(self.conn_pkt.protocol, obuf);

        let _conn_pkt = tt::Connect::read(fixed_header, bytes)?;
        let connack = tt::ConnAck::new(tt::ConnectReturnCode::UnspecifiedError, false);
        let _r = connack.encode(self.protocol, obuf)?;
        Ok(())
    }

    async fn handle_publish(
        &mut self,
        fixed_header: tt::FixedHeader,
        bytes: Bytes,
        obuf: &mut BytesMut,
    ) -> ThizResult<()> {
        let packet = tt::Publish::decode(self.protocol, fixed_header, bytes)?;
        match packet.qos {
            tt::QoS::AtMostOnce => {}
            tt::QoS::AtLeastOnce => {
                let ack = tt::PubAck::new(packet.pkid);
                let _ = ack.encode(self.protocol, obuf);
            }
            tt::QoS::ExactlyOnce => {}
        }

        // if let Some(topic) = self.pub_topic_cache.get(&packet.topic) {
        //     topic
        //         .broadcast(Arc::new(hub::BcData::PUB(packet.clone())))
        //         .await;
        //     return Ok(());
        // }

        // let topic = hub::get().acquire_topic(&packet.topic).await;
        // topic
        //     .broadcast(Arc::new(hub::BcData::PUB(packet.clone())))
        //     .await;
        // self.pub_topic_cache.insert(packet.topic, topic);
        // Ok(())

        if let Some(pub_topic) = &self.last_pub_topic {
            if packet.topic == pub_topic.name() {
                pub_topic
                    .broadcast(hub::BcData::PUB(Arc::new(packet.clone())))
                    .await;
                return Ok(());
            }
        }

        let pub_topic = hub::get().acquire_pub_topic(&packet.topic);
        pub_topic
            .broadcast(hub::BcData::PUB(Arc::new(packet.clone())))
            .await;

        self.last_pub_topic = Some(pub_topic);

        Ok(())
    }

    async fn handle_puback(
        &mut self,
        fixed_header: tt::FixedHeader,
        bytes: Bytes,
        _obuf: &mut BytesMut,
    ) -> ThizResult<()> {
        let _pkt = tt::PubAck::decode(self.protocol, fixed_header, bytes)?;
        Ok(())
    }

    // async fn handle_pubrec(&mut self, fixed_header: tt::FixedHeader, bytes: Bytes, obuf : &mut BytesMut) -> AppResult<()> {
    //     let pkt = tt::PubRec::decode(self.conn_pkt.protocol, fixed_header, bytes)?;
    //     Ok(())
    // }

    // async fn handle_pubrel(&mut self, fixed_header: tt::FixedHeader, bytes: Bytes, obuf : &mut BytesMut) -> AppResult<()> {
    //     let pkt = tt::PubRel::decode(self.conn_pkt.protocol, fixed_header, bytes)?;
    //     Ok(())
    // }

    // async fn handle_pubcomp(&mut self, fixed_header: tt::FixedHeader, bytes: Bytes, obuf : &mut BytesMut) -> AppResult<()> {
    //     let pkt = tt::PubComp::decode(self.conn_pkt.protocol, fixed_header, bytes)?;
    //     Ok(())
    // }

    async fn handle_subscribe(
        &mut self,
        fixed_header: tt::FixedHeader,
        bytes: Bytes,
        obuf: &mut BytesMut,
    ) -> ThizResult<()> {
        let packet = tt::Subscribe::decode(self.protocol, fixed_header, bytes)?;

        let mut return_codes: Vec<tt::SubscribeReasonCode> = Vec::new();
        for val in packet.filters.iter() {
            // self.hub
            //     .subscribe(&val.path, self.uid, self.tx.clone())
            //     .await;
            self.topic_filters.insert(val.path.clone());
            // return_codes.push(tt::SubscribeReasonCode::QoS0);

            hub::get()
                .subscribe(&val.path, self.info.uid, self.info.tx_bc.clone())
                .await;
            return_codes.push(tt::SubscribeReasonCode::from(val.qos));
        }

        let ack = tt::SubAck::new(packet.pkid, return_codes);
        ack.encode(self.protocol, obuf)?;

        Ok(())
    }

    async fn handle_unsubscribe(
        &mut self,
        fixed_header: tt::FixedHeader,
        bytes: Bytes,
        obuf: &mut BytesMut,
    ) -> ThizResult<()> {
        let packet = tt::Unsubscribe::decode(self.protocol, fixed_header, bytes)?;

        for filter in packet.filters.iter() {
            if self.topic_filters.remove(filter) {
                // self.hub.unsubscribe(filter, self.uid).await;
                let _r = hub::get().unsubscribe(filter, self.info.uid).await;
                // info!("unsubscribe result {}", r);
            }
        }

        let ack = tt::UnsubAck::new(packet.pkid);
        ack.encode(self.protocol, obuf)?;

        Ok(())
    }

    async fn handle_pingreq(
        &mut self,
        _fixed_header: tt::FixedHeader,
        _bytes: Bytes,
        obuf: &mut BytesMut,
    ) -> ThizResult<()> {
        let ack = tt::PingResp {};
        ack.write(obuf)?;
        Ok(())
    }

    async fn handle_disconnect(
        &mut self,
        fixed_header: tt::FixedHeader,
        bytes: Bytes,
        _obuf: &mut BytesMut,
    ) -> ThizResult<()> {
        let _pkt = tt::Disconnect::decode(self.protocol, fixed_header, bytes)?;
        debug!("disconnect by client");
        self.disconnected = true;
        Ok(())
    }

    async fn handle_unexpect(&mut self, packet_type: tt::PacketType) -> ThizResult<()> {
        Err(Error::UnexpectPacket(packet_type))
    }

    async fn handle_packet(
        &mut self,
        packet_type: tt::PacketType,
        h: tt::FixedHeader,
        bytes: Bytes,
        obuf: &mut BytesMut,
    ) -> ThizResult<()> {
        match packet_type {
            tt::PacketType::Connect => {
                self.handle_connect(h, bytes, obuf).await?;
            }
            // tt::PacketType::ConnAck     => todo!(),
            tt::PacketType::Publish => {
                self.handle_publish(h, bytes, obuf).await?;
            }
            tt::PacketType::PubAck => {
                self.handle_puback(h, bytes, obuf).await?;
            }
            // tt::PacketType::PubRec      => {self.handle_pubrec(h, bytes, obuf).await?;},
            // tt::PacketType::PubRel      => {self.handle_pubrel(h, bytes, obuf).await?;},
            // tt::PacketType::PubComp     => {self.handle_pubcomp(h, bytes, obuf).await?;},
            tt::PacketType::Subscribe => {
                self.handle_subscribe(h, bytes, obuf).await?;
            }
            // tt::PacketType::SubAck      => todo!(),
            tt::PacketType::Unsubscribe => {
                self.handle_unsubscribe(h, bytes, obuf).await?;
            }
            // tt::PacketType::UnsubAck    => todo!(),
            tt::PacketType::PingReq => {
                self.handle_pingreq(h, bytes, obuf).await?;
            }
            // tt::PacketType::PingResp    => todo!(),
            tt::PacketType::Disconnect => {
                self.handle_disconnect(h, bytes, obuf).await?;
            }
            _ => {
                self.handle_unexpect(packet_type).await?;
            }
        }
        Ok(())
    }

    // async fn handle_incoming(
    //     &mut self,
    //     ibuf: &mut BytesMut,
    //     obuf: &mut BytesMut,
    // ) -> ThizResult<()> {
    //     while !self.disconnected {
    //         let r = tt::check(ibuf.iter(), self.max_incoming_size);
    //         match r {
    //             Err(tt::Error::InsufficientBytes(_required)) => return Ok(()),
    //             Err(e) => return Err(Error::from(e)),
    //             Ok(h) => {
    //                 let bytes = ibuf.split_to(h.frame_length()).freeze();
    //                 let packet_type = tt::PacketType::try_from(h.get_type_byte())?;

    //                 self.last_active_time = Instant::now();
    //                 self.handle_packet(packet_type, h, bytes, obuf).await?;

    //             }
    //         }
    //     }
    //     Ok(())
    // }

    async fn handle_bc_event(
        &mut self,
        pubd: hub::BcData,
        writer: &mut Writer<'_>,
    ) -> ThizResult<()> {
        match pubd {
            hub::BcData::PUB(packet) => {
                packet.encode_with(
                    self.protocol,
                    self.packet_id.next().unwrap(),
                    packet.qos,
                    &mut writer.obuf,
                )?;
            }
        }
        Ok(())
    }

    async fn handle_ctrl_event(
        &mut self,
        data: hub::CtrlData,
        writer: &mut Writer<'_>,
    ) -> ThizResult<()> {
        match data {
            hub::CtrlData::KickByOther => {
                debug!("kick by other");
                let mut packet = tt::Disconnect::new();
                packet.reason_code = tt::DisconnectReasonCode::SessionTakenOver;
                packet.encode(self.protocol, &mut writer.obuf)?;
                self.disconnected = true;
            }
        }
        Ok(())
    }

    pub async fn xfer_loop(
        &mut self,
        reader: &mut Reader<'_>,
        writer: &mut Writer<'_>,
    ) -> ThizResult<()> {
        while !self.disconnected {
            let rd_timeout = self.check_alive()?;
            let rd_timeout = if writer.obuf.len() == 0 && rd_timeout > 10000 {
                10000
            } else {
                rd_timeout
            };

            let check_time = Instant::now() + Duration::from_millis(rd_timeout);

            select! {
                r = reader.read_packet(self.max_incoming_size) => {
                    let (t, h, bytes) = r?;
                    self.handle_packet(t, h, bytes, &mut writer.obuf).await?;
                }

                r = writer.wr.write_buf(&mut writer.obuf), if writer.obuf.len() > 0 => {
                    match r{
                        Ok(_) => { },
                        Err(e) => { return Err(Error::from(e)); },
                    }
                }

                r = hub::recv_bc(&mut self.rx_bc) => {
                    match r{
                        Ok(d) => {
                            self.handle_bc_event(d, writer).await?;
                        },
                        Err(broadcast::error::RecvError::Lagged(n)) => {
                            info!("lagged {}", n);
                        },
                        Err(_e) => { }
                    }
                }

                r = hub::recv_ctrl(&mut self.rx_ctrl) => {
                    match r {
                        Some(d) => {
                            self.handle_ctrl_event(d, writer).await?;
                        }
                        None => {},
                    }
                }

                _ = tokio::time::sleep_until(check_time), if rd_timeout > 0=> {
                    let _ = self.check_alive()?;
                    if reader.ibuf.len() == 0 && writer.obuf.len() == 0{
                        debug!("no data, sleep");
                        return Ok(());
                    }
                }
            }
        }
        Ok(())
    }
}

async fn run_server(cfg: &Config) -> core::result::Result<(), Box<dyn std::error::Error>> {
    info!("channel type: [{}]", hub::bc_channel_type_name());

    let r = discovery::Service::launch(&cfg.node_id, &cfg.cluster_listen_addr, &cfg.seed).await;
    if let Err(e) = r {
        return Err(Box::new(e));
    }
    let discovery = r.unwrap();

    let reg = Registry {
        discovery,
        tenants: Default::default(),
    };
    registry::set(reg);
    info!(
        "cluster service at [{}], id [{}]",
        registry::get().discovery.local_addr(),
        registry::get().discovery.id()
    );

    //launch_sub_service(cfg).await?;

    let listener = TcpListener::bind(&cfg.tcp_listen_addr).await?;
    info!("mqtt tcp service at [{:?}]", listener.local_addr().unwrap());

    SESSION_GAUGE.set(0);
    // let hub = Arc::new(hub::Hub::default());
    // let mut uid = 0;

    loop {
        tokio::select! {
            result = listener.accept() => {
                match result{
                    Ok((socket, _r)) => {
                        let uid = hub::next_uid();
                        let span = tracing::span!(tracing::Level::INFO, "", t=uid);
                        if let Err(e) = socket.set_nodelay(true) {
                            error!("socket set nodelay with {:?}", e);
                        }

                        let f = session_entry(uid, socket);
                        tokio::spawn(tracing::Instrument::instrument(f, span));
                    },
                    Err(e) => {
                        error!("listener accept error {}", e);
                        return Err(Box::new(e));
                    },
                }
            }

            _ = tokio::time::sleep(tokio::time::Duration::from_secs(5)), if cfg.enable_gc =>{
                let success = call_malloc_trim();
                info!("gc result {}", success);
            }

        };
    }
}

use actix_web::{get, HttpResponse, Responder};
use prometheus::{Encoder, TextEncoder};

use crate::registry::Registry;

// Register & measure some metrics.
lazy_static::lazy_static! {
    static ref SESSION_GAUGE: prometheus::IntGauge =
        prometheus::register_int_gauge!("sessions", "Number of sessions").unwrap();
}

#[get("/metrics")]
async fn metrics() -> impl Responder {
    SESSION_GAUGE.set(hub::get().num_sessions() as i64);
    let metric_families = prometheus::gather();

    let mut buffer = Vec::new();
    let encoder = TextEncoder::new();
    encoder.encode(&metric_families, &mut buffer).unwrap();
    let output = String::from_utf8(buffer).unwrap();
    // debug!("{}", output);
    HttpResponse::Ok().body(output)
}

async fn async_main() -> std::io::Result<()> {
    tq3::log::tracing_subscriber::init();

    let cfg = Config::parse();
    info!("cfg={:?}", cfg);

    let tokio_h = tokio::spawn(async move {
        match run_server(&cfg).await {
            Ok(_) => {}
            Err(e) => {
                error!("{}", e);
            }
        }
    });
    let _r = tokio_h.await;

    // let actix_h = actix_web::HttpServer::new(|| actix_web::App::new().service(metrics))
    //     .workers(1)
    //     .bind("127.0.0.1:8080")
    //     .expect("Couldn't bind to 127.0.0.1:8080")
    //     .run();

    // match futures::future::select(tokio_h, actix_h).await {
    //     futures::future::Either::Left(_r) => {}
    //     futures::future::Either::Right(_r) => {}
    // }

    Ok(())
}

// #[tokio::main]
// #[actix_web::main]
fn main() -> std::io::Result<()> {
    actix_web::rt::System::with_tokio_rt(|| {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            // .worker_threads(8)
            .thread_name("main-tokio")
            .build()
            .unwrap()
    })
    .block_on(async_main())?;

    Ok(())
}
