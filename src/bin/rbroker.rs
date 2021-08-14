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

use std::{
    collections::HashSet,
    sync::{Arc, Weak},
    time::Duration,
};

use bytes::{Bytes, BytesMut};
use clap::Clap;
use hub::BcSenders;
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
}

// #[derive(Debug, thiserror::Error)]
// pub enum Error {
//     #[error("Timeout:{0}")]
//     Timeout(#[from] Elapsed),

//     #[error("Packet parsing error: {0}")]
//     TtError(tt::Error),

//     #[error("I/O error: {0}")]
//     Io(#[from] std::io::Error),
// }

#[derive(Debug, thiserror::Error)]
pub enum AppError {
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
}

// impl From<std::io::Error> for AppError {
//     fn from(error: std::io::Error) -> Self {
//         AppError::IoError(error)
//     }
// }

// impl From<tt::Error> for AppError {
//     fn from(error: tt::Error) -> Self {
//         AppError::TtError(error)
//     }
// }

// impl From<tokio::time::error::Elapsed> for AppError {
//     fn from(error: tokio::time::error::Elapsed) -> Self {
//         AppError::ElapsedError(error)
//     }
// }

// impl From<num_enum::TryFromPrimitiveError<tt::PacketType>> for AppError {
//     fn from(error: num_enum::TryFromPrimitiveError<tt::PacketType>) -> Self {
//         AppError::PackettypeError(error)
//     }
// }

impl AppError {
    fn broken_pipe<E>(reason: E) -> Self
    where
        E: Into<Box<dyn std::error::Error + Send + Sync>>,
    {
        AppError::from(std::io::Error::new(std::io::ErrorKind::BrokenPipe, reason))
    }

    fn timeout<E>(reason: E) -> Self
    where
        E: Into<Box<dyn std::error::Error + Send + Sync>>,
    {
        AppError::from(std::io::Error::new(std::io::ErrorKind::TimedOut, reason))
    }
}

type AppResult<T> = core::result::Result<T, AppError>;

mod hub {
    use rust_threeq::tq3::tt;
    use std::{collections::HashMap, sync::Arc};
    use tokio::sync::RwLock;
    use tracing::debug;

    pub enum BcData {
        PUB(tt::Publish),
    }

    type BcSender = tokio::sync::broadcast::Sender<Arc<BcData>>;
    // type BcSenders = Arc<RwLock<HashMap<u64, BcSender>>>;

    #[derive(Default, Debug)]
    pub struct BcSenders {
        map: RwLock<HashMap<u64, BcSender>>,
    }

    impl BcSenders {
        #[inline(always)]
        pub async fn insert(&self, uid: u64, tx: BcSender) -> usize {
            let mut senders = self.map.write().await;
            senders.insert(uid, tx);
            senders.len()
        }

        #[inline(always)]
        pub async fn remove(&self, uid: &u64) -> usize {
            let mut senders = self.map.write().await;
            senders.remove(&uid);
            senders.len()
        }

        #[inline(always)]
        pub async fn broadcast(&self, d: Arc<BcData>) {
            let senders = self.map.read().await;
            for (_, tx) in senders.iter() {
                let _ = tx.send(d.clone());
            }
        }
    }

    #[derive(Default, Debug)]
    pub struct Hub {
        // topic_filter -> senders
        subscriptions: RwLock<HashMap<String, Arc<BcSenders>>>,
    }

    impl Hub {
        #[inline(always)]
        async fn get_or_add_senders(&self, topic_filter: &str) -> Arc<BcSenders> {
            let mut map = self.subscriptions.write().await;
            if !map.contains_key(topic_filter) {
                map.insert(topic_filter.to_string(), Default::default());
            }
            map.get_mut(topic_filter).unwrap().clone()
        }

        #[inline(always)]
        pub async fn subscribe(&self, topic_filter: &str, uid: u64, tx: BcSender) {
            let senders = self.get_or_add_senders(topic_filter).await;
            let num = senders.insert(uid, tx).await;
            debug!(
                "subscribe filter {}, uid {}, num {}",
                topic_filter, uid, num
            );
        }

        #[inline(always)]
        pub async fn unsubscribe(&self, topic_filter: &str, uid: u64) {
            let mut map = self.subscriptions.write().await;
            if let Some(senders) = map.get_mut(topic_filter) {
                let num = senders.remove(&uid).await;
                debug!(
                    "unsubscribe filter {}, uid {}, num {}",
                    topic_filter, uid, num
                );
                if num == 0 {
                    drop(senders);
                    map.remove(topic_filter);
                }
            }
        }

        #[inline(always)]
        pub async fn publish(&self, filter: &str, d: Arc<BcData>) -> Option<Arc<BcSenders>> {
            let map = self.subscriptions.read().await;
            match map.get(filter) {
                Some(senders) => {
                    senders.broadcast(d).await;
                    Some(senders.clone())
                }
                None => None,
            }
        }
    }
}

struct Session {
    hub: Arc<hub::Hub>,
    uid: u64,
    max_incoming_size: usize,
    keep_alive_ms: u64,
    conn_pkt: tt::Connect,
    packet_id: tt::PacketId,
    last_active_time: Instant,
    tx: broadcast::Sender<Arc<hub::BcData>>,
    rx: broadcast::Receiver<Arc<hub::BcData>>,
    disconnected: bool,
    topic_filters: HashSet<String>,
    last_pub_senders: Weak<BcSenders>,
    last_pub_topic: String,
}

impl Session {
    pub fn new(hub: Arc<hub::Hub>, uid: u64) -> Self {
        let (tx, rx) = broadcast::channel(16);

        Session {
            hub,
            uid,
            max_incoming_size: 64 * 1024,
            keep_alive_ms: 30 * 1000,
            conn_pkt: tt::Connect::new(""),
            packet_id: tt::PacketId::default(),
            last_active_time: Instant::now(),
            tx,
            rx,
            disconnected: false,
            topic_filters: HashSet::new(),
            last_pub_senders: Weak::new(),
            last_pub_topic: "".to_string(),
        }
    }

    pub async fn cleanup(&mut self) {
        for v in &self.topic_filters {
            self.hub.unsubscribe(v, self.uid).await;
        }
        self.topic_filters.clear();
    }

    fn check_connect(&mut self) {
        if self.conn_pkt.client_id.is_empty() {
            let duration = std::time::SystemTime::now()
                .duration_since(std::time::SystemTime::UNIX_EPOCH)
                .unwrap();
            let in_nanos = duration.as_secs() * 1_000_000 + duration.subsec_nanos() as u64;
            self.conn_pkt.client_id = format!("{}@abcdef", in_nanos); //UNIX_EPOCH
        }

        self.keep_alive_ms = self.conn_pkt.keep_alive as u64 * 1000 * 2;
        if self.keep_alive_ms == 0 {
            self.keep_alive_ms = 30 * 1000;
        }
    }

    fn check_alive(&self) -> AppResult<u64> {
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
            return Err(AppError::timeout(reason));
        }
    }

    pub async fn run(&mut self, mut socket: TcpStream) -> AppResult<()> {
        //socket.set_nodelay(true)?; // TODO:

        let (mut rd, mut wr) = socket.split();

        while !self.disconnected {
            let rd_timeout = self.check_alive()?;
            let dead_line = Instant::now() + Duration::from_millis(rd_timeout);
            let mut dump_buf = [0u8; 1];

            select! {

                //r = socket.ready(Interest::READABLE) =>{
                r = rd.peek(&mut dump_buf) =>{
                    match r{
                        Ok(_) => {
                            self.xfer_loop(&mut rd, &mut wr, None).await?;
                        },
                        Err(e) => {
                            return Err(AppError::from(e));
                        },
                    }
                }

                r = self.rx.recv() =>{
                    match r{
                        Ok(d) => {
                            self.xfer_loop(&mut rd, &mut wr, Some(d)).await?;
                        },
                        Err(broadcast::error::RecvError::Lagged(n)) => {
                            info!("lagged {}", n);
                            //while let Ok(d) = self.rx.try_recv() { }
                        },
                        Err(_) => {

                        }
                    }
                }

                _ = tokio::time::sleep_until(dead_line) => {

                }
            }
        }
        Ok(())
    }

    fn is_got_connect(&self) -> bool {
        !self.conn_pkt.client_id.is_empty()
    }

    async fn handle_connect(
        &mut self,
        fixed_header: tt::FixedHeader,
        bytes: Bytes,
        obuf: &mut BytesMut,
    ) -> AppResult<()> {
        self.conn_pkt = tt::Connect::read(fixed_header, bytes)?;
        let is_empty_clientid = self.conn_pkt.client_id.is_empty();
        self.check_connect();

        let mut connack = tt::ConnAck::new(tt::ConnectReturnCode::Success, false);

        connack.properties = Some(tt::ConnAckProperties::new());
        if is_empty_clientid {
            connack
                .properties
                .as_mut()
                .unwrap()
                .assigned_client_identifier = Some(self.conn_pkt.client_id.clone());
        }

        let _ = connack.encode(self.conn_pkt.protocol, obuf);
        Ok(())
    }

    async fn handle_publish(
        &mut self,
        fixed_header: tt::FixedHeader,
        bytes: Bytes,
        obuf: &mut BytesMut,
    ) -> AppResult<()> {
        let packet = tt::Publish::decode(self.conn_pkt.protocol, fixed_header, bytes)?;
        match packet.qos {
            tt::QoS::AtMostOnce => {}
            tt::QoS::AtLeastOnce => {
                let ack = tt::PubAck::new(packet.pkid);
                let _ = ack.encode(self.conn_pkt.protocol, obuf);
            }
            tt::QoS::ExactlyOnce => {}
        }

        if packet.topic == self.last_pub_topic {
            if let Some(senders) = self.last_pub_senders.upgrade() {
                senders
                    .broadcast(Arc::new(hub::BcData::PUB(packet.clone())))
                    .await;
                return Ok(());
            }
        }

        let senders = self
            .hub
            .publish(&packet.topic, Arc::new(hub::BcData::PUB(packet.clone())))
            .await;

        if let Some(senders) = senders {
            self.last_pub_senders = Arc::downgrade(&senders);
            self.last_pub_topic = packet.topic.clone();
        }

        Ok(())
    }

    async fn handle_puback(
        &mut self,
        fixed_header: tt::FixedHeader,
        bytes: Bytes,
        _obuf: &mut BytesMut,
    ) -> AppResult<()> {
        let _pkt = tt::PubAck::decode(self.conn_pkt.protocol, fixed_header, bytes)?;
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
    ) -> AppResult<()> {
        let packet = tt::Subscribe::decode(self.conn_pkt.protocol, fixed_header, bytes)?;

        let mut return_codes: Vec<tt::SubscribeReasonCode> = Vec::new();
        for val in packet.filters.iter() {
            self.hub
                .subscribe(&val.path, self.uid, self.tx.clone())
                .await;
            self.topic_filters.insert(val.path.clone());
            return_codes.push(tt::SubscribeReasonCode::QoS0);
        }

        let ack = tt::SubAck::new(packet.pkid, return_codes);
        ack.encode(self.conn_pkt.protocol, obuf)?;

        Ok(())
    }

    async fn handle_unsubscribe(
        &mut self,
        fixed_header: tt::FixedHeader,
        bytes: Bytes,
        obuf: &mut BytesMut,
    ) -> AppResult<()> {
        let packet = tt::Unsubscribe::decode(self.conn_pkt.protocol, fixed_header, bytes)?;

        for topic in packet.filters.iter() {
            if self.topic_filters.remove(topic) {
                self.hub.unsubscribe(topic, self.uid).await;
            }
        }

        let ack = tt::UnsubAck::new(packet.pkid);
        ack.encode(self.conn_pkt.protocol, obuf)?;

        Ok(())
    }

    async fn handle_pingreq(
        &mut self,
        _fixed_header: tt::FixedHeader,
        _bytes: Bytes,
        obuf: &mut BytesMut,
    ) -> AppResult<()> {
        let ack = tt::PingResp {};
        ack.write(obuf)?;
        Ok(())
    }

    async fn handle_disconnect(
        &mut self,
        fixed_header: tt::FixedHeader,
        bytes: Bytes,
        _obuf: &mut BytesMut,
    ) -> AppResult<()> {
        let _pkt = tt::Disconnect::decode(self.conn_pkt.protocol, fixed_header, bytes)?;
        debug!("disconnect by client");
        self.disconnected = true;
        Ok(())
    }

    async fn handle_unexpect(&mut self, packet_type: tt::PacketType) -> AppResult<()> {
        Err(AppError::UnexpectPacket(packet_type))
    }

    async fn handle_incoming(&mut self, ibuf: &mut BytesMut, obuf: &mut BytesMut) -> AppResult<()> {
        while !self.disconnected {
            let r = tt::check(ibuf.iter(), self.max_incoming_size);
            match r {
                Err(tt::Error::InsufficientBytes(_required)) => return Ok(()),
                Err(e) => return Err(AppError::from(e)),
                Ok(h) => {
                    let bytes = ibuf.split_to(h.frame_length()).freeze();
                    let packet_type = tt::PacketType::try_from(h.get_type_byte())?;

                    if !self.is_got_connect() && !matches!(packet_type, tt::PacketType::Connect) {
                        return Err(AppError::ExpectConnectPacket(packet_type));
                    }

                    self.last_active_time = Instant::now();

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
                }
            }
        }
        Ok(())
    }

    async fn xfer_loop<'a>(
        &mut self,
        rd: &mut ReadHalf<'a>,
        wr: &mut WriteHalf<'a>,
        pubd: Option<Arc<hub::BcData>>,
    ) -> AppResult<()> {
        let mut ibuf = BytesMut::with_capacity(1);
        let mut obuf = BytesMut::new();

        if !pubd.is_none() {
            match &*pubd.unwrap() {
                hub::BcData::PUB(packet) => {
                    packet.encode_with(
                        self.conn_pkt.protocol,
                        self.packet_id.next().unwrap(),
                        packet.qos,
                        &mut obuf,
                    )?;
                }
            }
        }

        while !self.disconnected {
            let rd_timeout = self.check_alive()?;
            let rd_timeout = if obuf.len() == 0 && rd_timeout > 10000 {
                10000
            } else {
                rd_timeout
            };

            let check_time = Instant::now() + Duration::from_millis(rd_timeout);

            select! {
                r = rd.read_buf(&mut ibuf) =>{
                    let n = r?;
                    if n == 0 {
                        return Err(AppError::broken_pipe("read disconnect"));
                    }

                    self.handle_incoming(&mut ibuf, &mut obuf).await?
                }

                r = wr.write_buf(&mut obuf), if obuf.len() > 0 => {
                    match r{
                        Ok(_) => { },
                        Err(e) => { return Err(AppError::from(e)); },
                    }
                }

                r = self.rx.recv() =>{
                    match r{
                        Ok(d) => {
                            match &*d{
                                hub::BcData::PUB(packet) => {
                                    packet.encode_with( self.conn_pkt.protocol, self.packet_id.next().unwrap(), packet.qos, &mut obuf)?;
                                },
                            }
                        },
                        Err(broadcast::error::RecvError::Lagged(n)) => {
                            info!("lagged {}", n);
                            //while let Ok(d) = self.rx.try_recv() { }
                        },
                        Err(_) => {

                        }
                    }
                }

                _ = tokio::time::sleep_until(check_time), if rd_timeout > 0=> {
                    let _ = self.check_alive()?;
                    if ibuf.len() == 0 && obuf.len() == 0{
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
    let listener = TcpListener::bind(&cfg.tcp_listen_addr).await?;
    info!("mqtt tcp broker listening on {}", cfg.tcp_listen_addr);

    SESSION_COUNTER.set(0);
    let hub = Arc::new(hub::Hub::default());
    let mut uid = 0;

    loop {
        tokio::select! {
            result = listener.accept() => {
                match result{
                    Ok((socket, _)) => {
                        uid += 1;
                        let span = tracing::span!(tracing::Level::INFO, "", t=uid);

                        let hub0 = hub.clone();
                        let f = async move {
                            debug!("connected from {:?}", socket.peer_addr().unwrap());
                            let mut session = Session::new(hub0, uid);
                            SESSION_COUNTER.inc();
                            if let Err(e) = session.run(socket).await {
                                debug!("session finished error [{:?}]", e);
                            }
                            SESSION_COUNTER.dec();
                            session.cleanup().await;
                        };
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

use actix_web::{get, App, HttpResponse, HttpServer, Responder};
use prometheus::{Encoder, TextEncoder};

// Register & measure some metrics.
lazy_static::lazy_static! {
    static ref SESSION_COUNTER: prometheus::IntGauge =
        prometheus::register_int_gauge!("sessions", "Number of sessions").unwrap();
}

#[get("/metrics")]
async fn metrics() -> impl Responder {
    let mut buffer = Vec::new();
    let encoder = TextEncoder::new();
    let metric_families = prometheus::gather();
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

    let actix_h = HttpServer::new(|| App::new().service(metrics))
        // .workers(8)
        .bind("127.0.0.1:8080")
        .expect("Couldn't bind to 127.0.0.1:8080")
        .run();

    match futures::future::select(tokio_h, actix_h).await {
        futures::future::Either::Left(_r) => {}
        futures::future::Either::Right(_r) => {}
    }

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
