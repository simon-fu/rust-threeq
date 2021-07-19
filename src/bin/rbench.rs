use std::{
    fmt::Debug,
    sync::Arc,
    time::{Duration, Instant},
};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use clap::Clap;
use histogram::Histogram;
use rust_threeq::tq3::{self, tt, TS};
use tokio::sync::{mpsc, watch};
use tracing::{debug, error, info, trace, warn};
use tt::config::*;

// refer https://github.com/clap-rs/clap/tree/master/clap_derive/examples
#[derive(Clap, Debug, Default)]
#[clap(name = "threeq bench", author, about, version)]
struct CmdArgs {
    #[clap(short = 'c', long = "config", long_about = "config file.")]
    config: String,
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("{0}")]
    ClientError(#[from] tt::client::Error),

    #[error("{0}")]
    WatchError(tokio::sync::watch::error::RecvError),

    #[error("error: {0}")]
    Generic(String),
}

#[derive(Default, Debug)]
struct SubStati {
    // latencyh: histogram::Histogram,
    recv_packets: u64,
    lost_packets: u64,
}

impl SubStati {
    fn merge(&mut self, other: &SubStati) {
        //self.latencyh.merge(&other.latencyh);
        self.recv_packets += other.recv_packets;
        self.lost_packets += other.lost_packets;
    }
}

#[derive(Default, Debug)]
struct PubStati {
    qps: u64,
}

#[derive(Debug)]
enum TaskEvent {
    Error(Error),
    SubConnected(Instant),
    PubConnected(Instant),
    Subscribed(Instant),
    PubStart(Instant),
    SendPacket(Instant, usize),
    RecvPacket(Instant, usize, u64),
    SubResult(Instant, SubStati),
    PubResult(Instant, PubStati),
    SubFinished(u64),
    PubFinished(u64),
}

#[derive(Debug, Clone, Copy)]
enum TaskReq {
    Ready,
    KickXfer(Instant),
    Stop,
}

#[derive(Debug, Clone, Copy)]
struct Header {
    pubid: usize,
    ts: i64,
    seq: u64,
    max_seq: u64,
}

impl Header {
    fn new(pubid: usize) -> Self {
        Self {
            pubid,
            ts: 0,
            seq: 0,
            max_seq: 0,
        }
    }

    fn decode(buf: &mut Bytes) -> Self {
        Self {
            pubid: buf.get_u32() as usize,
            ts: buf.get_i64(),
            seq: buf.get_u64(),
            max_seq: buf.get_u64(),
        }
    }
}

fn encode_msg(header: &Header, content: &[u8], padding_to_size: usize, buf: &mut BytesMut) {
    let len = 24 + content.len();

    buf.reserve(len);

    buf.put_u32(header.pubid as u32);
    buf.put_i64(header.ts);
    buf.put_u64(header.seq);
    buf.put_u64(header.max_seq);
    buf.put_u32(content.len() as u32);
    buf.put(content);

    if len < padding_to_size {
        let remaining = padding_to_size - len;
        buf.reserve(remaining);
        unsafe {
            buf.advance_mut(remaining);
        }
    }
}

type EVSender = mpsc::Sender<TaskEvent>;
type EVRecver = mpsc::Receiver<TaskEvent>;
type ReqRecver = watch::Receiver<TaskReq>;

#[derive(Debug, Default, Clone, Copy)]
struct Puber {
    next_seq: u64,
    max_seq: u64,
}

fn is_recv_done(pubers: &Vec<Puber>) -> bool {
    let mut done = true;
    for v in pubers {
        if v.next_seq < v.max_seq {
            done = false;
            break;
        }
    }
    return done;
}

async fn wait_for_req(rx: &mut ReqRecver) -> Result<TaskReq, Error> {
    if let Err(e) = rx.changed().await {
        return Err(Error::WatchError(e));
    }
    return Ok(*rx.borrow());
}

const MAX_PUBS: usize = 10000;

async fn sub_task(
    subid: u64,
    cfgw: Arc<tt::config::Config>,
    acc: Account,
    tx: &EVSender,
    mut rx: ReqRecver,
) -> Result<(), Error> {
    let cfg = cfgw.raw();
    let (mut sender, mut receiver) =
        tt::client::make_connection(&format!("sub{}", subid), &cfgw.env().address)
            .await?
            .split();

    let mut pkt = init_conn_pkt(&acc, tt::Protocol::V4);
    pkt.keep_alive = cfgw.raw().subs.keep_alive_secs as u16;
    let ack = sender.connect(pkt).await?;
    if ack.code != tt::ConnectReturnCode::Success {
        return Err(Error::Generic(format!("{:?}", ack)));
    }
    let _r = tx.send(TaskEvent::SubConnected(Instant::now())).await;

    let ack = sender
        .subscribe(tt::Subscribe::new(&cfgw.sub_topic(), cfg.subs.qos))
        .await?;
    for reason in &ack.return_codes {
        if !reason.is_success() {
            return Err(Error::Generic(format!("{:?}", ack)));
        }
    }

    let _r = tx.send(TaskEvent::Subscribed(Instant::now())).await;

    let mut pubers = vec![Puber::default(); 1];

    let mut stati = SubStati::default();

    loop {
        let ev = tokio::select! {
            r = receiver.recv() => {
                r?
            }

            r = wait_for_req(&mut rx) => {
                let req = r?;
                match req {
                    TaskReq::Stop => { break; },
                    _ => continue,
                }
            }
        };

        let mut rpkt = match ev {
            tt::client::Event::Packet(pkt) => match pkt {
                tt::Packet::Publish(rpkt) => rpkt,
                _ => return Err(Error::Generic(format!("unexpect packet {:?}", pkt))),
            },
            tt::client::Event::Closed(s) => {
                debug!("got closed [{}]", s);
                break;
            }
        };

        let payload_size = rpkt.payload.len();
        let header = Header::decode(&mut rpkt.payload);

        if header.pubid >= pubers.len() {
            if header.pubid >= MAX_PUBS {
                error!(
                    "pubid exceed limit, expect {} but {}",
                    MAX_PUBS, header.pubid
                );
                break;
            }
            pubers.resize(header.pubid, Puber::default());
        }

        let puber = &mut pubers[header.pubid];
        puber.max_seq = header.max_seq;

        {
            let next_seq = &puber.next_seq;

            if header.seq == 0 && *next_seq > 0 {
                // restart
                debug!("restart, n {}, npkt {}", header.seq, next_seq);
                return Err(Error::Generic(format!(
                    "restart, n {}, npkt {}",
                    header.seq, next_seq
                )));
            } else if header.seq < *next_seq {
                return Err(Error::Generic(format!(
                    "expect seq {}, but {}",
                    *next_seq, header.seq
                )));
            } else if header.seq > *next_seq {
                stati.lost_packets += header.seq - *next_seq;
            }
        }

        let latency = TS::now_ms() - header.ts;
        let latency = if latency >= 0 { latency as u64 } else { 0 };
        let _r = tx
            .send(TaskEvent::RecvPacket(Instant::now(), payload_size, latency))
            .await;
        stati.recv_packets += 1;

        puber.next_seq = header.seq + 1;

        if puber.next_seq > cfg.pubs.packets {
            error!(
                "seq exceed limit {}, pubid {}",
                puber.next_seq, header.pubid
            );
            break;
        } else if puber.next_seq == cfg.pubs.packets {
            if is_recv_done(&pubers) {
                break;
            }
        }
    }
    let result_time = Instant::now();
    // check lost
    for v in &pubers {
        if v.next_seq < v.max_seq {
            stati.lost_packets += v.max_seq - v.next_seq;
        }
    }
    let _r = tx.send(TaskEvent::SubResult(result_time, stati)).await;

    sender.disconnect(tt::Disconnect::new()).await?;
    // debug!("finished");
    let _r = tx.send(TaskEvent::SubFinished(subid)).await;

    Ok(())
}

async fn pub_task(
    pubid: u64,
    cfgw: Arc<tt::config::Config>,
    acc: Account,
    tx: &EVSender,
    mut rx: ReqRecver,
) -> Result<(), Error> {
    let cfg = cfgw.raw();
    let (mut sender, _recver) =
        tt::client::make_connection(&format!("pub{}", pubid), &cfgw.env().address)
            .await?
            .split();

    let mut pkt = init_conn_pkt(&acc, tt::Protocol::V4);
    pkt.keep_alive = cfgw.raw().pubs.keep_alive_secs as u16;
    sender.connect(pkt).await?;
    drop(acc);
    let _r = tx.send(TaskEvent::PubConnected(Instant::now())).await;

    let req = wait_for_req(&mut rx).await?;

    let mut header = Header::new(pubid as usize);
    header.max_seq = cfg.pubs.packets;

    let mut pacer = tq3::limit::Pacer::new(cfg.pubs.qps);

    if let TaskReq::KickXfer(t) = req {
        let mut buf = BytesMut::new();
        let pkt = tt::Publish::new(&cfgw.pub_topic(), cfg.pubs.qos, []);
        pacer = pacer.with_time(t);
        let start_time = Instant::now();

        while header.seq < cfg.pubs.packets {
            trace!("send No.{} packet", header.seq);

            if let Some(d) = pacer.get_sleep_duration(header.seq) {
                tokio::time::sleep(d).await;
            }

            header.ts = TS::now_ms();
            encode_msg(
                &header,
                cfg.pubs.content.as_bytes(),
                cfg.pubs.padding_to_size,
                &mut buf,
            );

            let mut pkt0 = pkt.clone();
            pkt0.payload = buf.split().freeze();

            let _r = tx
                .send(TaskEvent::SendPacket(Instant::now(), pkt0.payload.len()))
                .await;

            let _r = sender.publish(pkt0).await?;
            header.seq += 1;
        }
        let _r = tx.send(TaskEvent::PubStart(start_time)).await;
    }
    let t = Instant::now();
    let elapsed_ms = pacer.kick_time().elapsed().as_millis() as u64;
    // debug!("elapsed_ms {}", elapsed_ms);

    let mut stati = PubStati::default();
    if elapsed_ms > 1000 {
        stati.qps = cfg.pubs.packets * 1000 / elapsed_ms;
    } else {
        stati.qps = header.seq;
    }
    let _r = tx.send(TaskEvent::PubResult(t, stati)).await;

    sender.disconnect(tt::Disconnect::new()).await?;

    // debug!("finished");
    let _r = tx.send(TaskEvent::PubFinished(pubid)).await;

    Ok(())
}

fn print_histogram_summary(name: &str, unit: &str, h: &Histogram) {
    if h.entries() == 0 {
        info!("{} Summary: (empty)", name);
        return;
    }

    info!("{} Summary:", name);
    info!("     Min: {} {}", h.minimum().unwrap(), unit);
    info!("     Avg: {} {}", h.mean().unwrap(), unit);
    info!("     Max: {} {}", h.maximum().unwrap(), unit);
    info!("  StdDev: {} {}", h.stddev().unwrap(), unit);
}

fn print_histogram_percent(name: &str, unit: &str, h: &Histogram) {
    if h.entries() == 0 {
        info!("{} Percentiles: (empty)", name);
        return;
    }

    info!("{} Percentiles:", name);
    info!(
        "   P50: {} {} ({}/{})",
        h.percentile(50.0).unwrap(),
        unit,
        h.entries() * 50 / 100,
        h.entries()
    );
    info!(
        "   P90: {} {} ({}/{})",
        h.percentile(90.0).unwrap(),
        unit,
        h.entries() * 90 / 100,
        h.entries()
    );
    info!(
        "   P99: {} {} ({}/{})",
        h.percentile(99.0).unwrap(),
        unit,
        h.entries() * 99 / 100,
        h.entries()
    );
    info!(
        "  P999: {} {} ({}/{})",
        h.percentile(99.9).unwrap(),
        unit,
        h.entries() * 999 / 1000,
        h.entries()
    );
}

#[derive(Debug, Default)]
struct InstantRange {
    first: Option<Instant>,
    last: Option<Instant>,
}

impl InstantRange {
    fn update(&mut self, t: Instant) -> bool {
        if self.first.is_none() {
            self.first = Some(t);
            self.last = Some(t);
            return true;
        } else {
            if t < *self.first.as_ref().unwrap() {
                self.first = Some(t);
            }

            if t > *self.last.as_ref().unwrap() {
                self.last = Some(t);
            }
            return false;
        }
    }

    fn delta_time_range(&self, t: &Instant) -> (Duration, Duration) {
        (
            if self.first.is_none() {
                Duration::from_millis(0)
            } else {
                *self.first.as_ref().unwrap() - *t
            },
            if self.last.is_none() {
                Duration::from_millis(0)
            } else {
                *self.last.as_ref().unwrap() - *t
            },
        )
    }
}

const INTERVAL: Duration = Duration::from_millis(1000);

#[derive(Debug)]
struct PacketSpeedEst {
    last_time: Instant,
    next_time: Instant,
    pub_packets: usize,
    pub_bytes: usize,
    sub_packets: usize,
    sub_bytes: usize,
}

impl Default for PacketSpeedEst {
    fn default() -> Self {
        Self {
            last_time: Instant::now(),
            next_time: Instant::now() + INTERVAL,
            pub_packets: 0,
            pub_bytes: 0,
            sub_packets: 0,
            sub_bytes: 0,
        }
    }
}

impl PacketSpeedEst {
    fn reset(&mut self, now: Instant) {
        self.last_time = now;
        self.next_time = now + INTERVAL;
        self.pub_packets = 0;
        self.pub_bytes = 0;
        self.sub_packets = 0;
        self.sub_bytes = 0;
    }

    fn inc_pub(&mut self, now: Instant, packets: usize, bytes: usize) {
        self.pub_packets += packets;
        self.pub_bytes += bytes;
        self.check(now);
    }

    fn inc_sub(&mut self, now: Instant, packets: usize, bytes: usize) {
        self.sub_packets += packets;
        self.sub_bytes += bytes;
        self.check(now);
    }

    fn check(&mut self, now: Instant) {
        if now >= self.next_time {
            let d = (now - self.last_time).as_millis() as usize;
            debug!(
                "pub [{} q/s, {} KB/s], sub [{} q/s, {} KB/s]",
                self.pub_packets * 1000 / d,
                self.pub_bytes * 1000 / d / 1000,
                self.sub_packets * 1000 / d,
                self.sub_bytes * 1000 / d / 1000,
            );
            self.reset(now);
        }
    }
}

#[derive(Debug, Default)]
struct BenchLatency {
    sub_conns: u64,
    pub_conns: u64,
    subscribes: u64,
    sub_results: u64,
    pub_results: u64,
    sub_finished: u64,
    pub_finished: u64,

    sub_stati: SubStati,
    latencyh: histogram::Histogram,
    pub_qos_h: Histogram,

    pub_start_range: InstantRange,
    pub_result_range: InstantRange,
    sub_result_range: InstantRange,

    packet_speed: PacketSpeedEst,
}

impl BenchLatency {
    async fn recv_event(&mut self, ev_rx: &mut EVRecver) -> Result<(), Error> {
        let ev = ev_rx.recv().await.unwrap();
        // debug!("recv {:?}", ev);
        match ev {
            TaskEvent::Error(e) => {
                return Err(e);
            }
            TaskEvent::SubConnected(_) => {
                self.sub_conns += 1;
            }
            TaskEvent::PubConnected(_) => {
                self.pub_conns += 1;
            }
            TaskEvent::Subscribed(_) => {
                self.subscribes += 1;
            }
            TaskEvent::PubStart(t) => {
                self.pub_start_range.update(t);
            }
            TaskEvent::SendPacket(_t, size) => {
                self.packet_speed.inc_pub(Instant::now(), 1, size);
            }
            TaskEvent::RecvPacket(_t, size, d) => {
                let _r = self.latencyh.increment(d);
                self.packet_speed.inc_sub(Instant::now(), 1, size);
            }
            TaskEvent::SubResult(t, s) => {
                self.sub_results += 1;
                self.sub_stati.merge(&s);
                if self.sub_result_range.update(t) {
                    debug!("first sub result");
                }
            }
            TaskEvent::PubResult(t, s) => {
                self.pub_results += 1;
                let _r = self.pub_qos_h.increment(s.qps);
                self.pub_result_range.update(t);
            }
            TaskEvent::SubFinished(_n) => {
                self.sub_finished += 1;
            }
            TaskEvent::PubFinished(_n) => {
                self.pub_finished += 1;
            }
        }
        Ok(())
    }

    fn print(&self, cfg: &Arc<tt::config::Config>) {
        info!("");
        info!("Pub connections: {}", cfg.raw().pubs.connections);
        info!("Pub packets: {} packets/connection", cfg.raw().pubs.packets);
        print_histogram_summary("Pub QPS", "qps/connection", &self.pub_qos_h);

        info!("");
        info!("Sub connections: {}", cfg.raw().subs.connections);
        info!("Sub recv packets: {}", self.sub_stati.recv_packets);
        info!("Sub lost packets: {}", self.sub_stati.lost_packets);
        print_histogram_summary("Sub Latency", "ms", &self.latencyh);
        print_histogram_percent("Sub Latency", "ms", &self.latencyh);
    }

    pub async fn bench_priv(
        &mut self,
        cfgw: Arc<tt::config::Config>,
        req_tx: &mut watch::Sender<TaskReq>,
        req_rx: &mut watch::Receiver<TaskReq>,
    ) -> Result<(), Error> {
        let cfg = cfgw.raw();
        info!("");
        info!("env: [{}]", cfg.env);
        info!("address: [{}]", cfgw.env().address);
        info!("");

        let mut accounts = AccountIter::new(&cfgw.env().accounts);
        let (ev_tx, mut ev_rx) = mpsc::channel(10240);
        // let (req_tx, req_rx) = watch::channel(TaskReq::Ready);

        let pacer = tq3::limit::Pacer::new(cfg.subs.conn_per_sec);
        let mut interval = tq3::limit::Interval::new(1000);
        let mut n = 0;
        while n < cfg.subs.connections {
            // pacer.check(n, |d|{
            //     futures::executor::block_on(tokio::time::sleep(d));
            // });

            if let Some(d) = pacer.get_sleep_duration(n) {
                if self.sub_conns < n {
                    self.recv_event(&mut ev_rx).await?;
                }
                tokio::time::sleep(d).await;
            }

            // pacer.check_and_wait(n).await;
            if interval.check() {
                debug!("spawned sub tasks {}", n);
            }

            let acc = accounts.next().unwrap();
            let cfg0 = cfgw.clone();
            let tx0 = ev_tx.clone();
            let rx0 = req_rx.clone();
            let f = async move {
                let r = sub_task(n, cfg0, acc, &tx0, rx0).await;
                if let Err(e) = r {
                    debug!("sub task finished error [{:?}]", e);
                    let _r = tx0.send(TaskEvent::Error(e)).await;
                }
            };
            let span = tracing::span!(tracing::Level::INFO, "", s = n);
            tokio::spawn(tracing::Instrument::instrument(f, span));
            n += 1;
        }
        debug!("spawned sub tasks {}", n);

        if cfg.subs.connections > 0 {
            // wait for sub connections and subscriptions
            while self.sub_conns < cfg.subs.connections && self.subscribes < cfg.subs.connections {
                if interval.check() {
                    debug!("setup sub connections {}", self.sub_conns);
                }
                self.recv_event(&mut ev_rx).await?;
            }
            info!("setup sub connections {}", self.sub_conns);
        }

        let pacer = tq3::limit::Pacer::new(cfg.pubs.conn_per_sec);
        let mut interval = tq3::limit::Interval::new(1000);
        let mut n = 0;

        while n < cfg.pubs.connections {
            if let Some(d) = pacer.get_sleep_duration(n) {
                if self.pub_conns < n {
                    self.recv_event(&mut ev_rx).await?;
                }
                tokio::time::sleep(d).await;
            }

            if interval.check() {
                debug!("spawned pub tasks {}", n);
            }

            let acc = accounts.next().unwrap();
            let cfg0 = cfgw.clone();
            let tx0 = ev_tx.clone();
            let rx0 = req_rx.clone();
            let f = async move {
                let r = pub_task(n, cfg0, acc, &tx0, rx0).await;
                if let Err(e) = r {
                    debug!("pub task finished error [{:?}]", e);
                    let _r = tx0.send(TaskEvent::Error(e)).await;
                }
            };
            let span = tracing::span!(tracing::Level::INFO, "", p = n);
            tokio::spawn(tracing::Instrument::instrument(f, span));
            n += 1;
        }

        if cfg.pubs.connections > 0 {
            // wait for pub connections
            while self.pub_conns < cfg.pubs.connections {
                self.recv_event(&mut ev_rx).await?;
            }
            info!("setup pub connections {}", cfg.pubs.connections);
        }

        let kick_time = Instant::now();
        if cfg.pubs.connections > 0 && cfg.pubs.packets > 0 {
            // kick publish
            debug!("-> kick publish");
            let _r = req_tx.send(TaskReq::KickXfer(kick_time));

            debug!("waiting for pub result...");
            while self.pub_results < cfg.pubs.connections {
                self.recv_event(&mut ev_rx).await?;
            }
            debug!("recv all pub result");

            debug!("waiting for sub result...");
            let r = tokio::time::timeout(Duration::from_millis(cfg.recv_timeout_ms), async {
                while self.sub_results < cfg.subs.connections {
                    self.recv_event(&mut ev_rx).await?;
                }
                Ok::<(), Error>(())
            })
            .await;

            if let Err(_) = r {
                // force stop
                warn!("waiting for sub result timeout, send stop");
                let _r = req_tx.send(TaskReq::Stop);

                // wait for sub result again
                while self.sub_results < cfg.subs.connections {
                    self.recv_event(&mut ev_rx).await?;
                }
            }
            debug!("<- recv all sub result");
        } else {
            debug!("skip publish, waiting for connections down");

            while self.pub_results < cfg.pubs.connections {
                self.recv_event(&mut ev_rx).await?;
            }

            while self.sub_results < cfg.subs.connections {
                self.recv_event(&mut ev_rx).await?;
            }
        }

        let elapsed = kick_time.elapsed();

        info!("");
        info!(
            "Pub start time  : {:?}",
            self.pub_start_range.delta_time_range(&kick_time)
        );
        info!(
            "Pub result time: {:?}",
            self.pub_result_range.delta_time_range(&kick_time)
        );
        info!(
            "Sub result time: {:?}",
            self.sub_result_range.delta_time_range(&kick_time)
        );
        debug!("Elapsed: {:?}", elapsed);

        self.print(&cfgw);

        Ok(())
    }

    pub async fn bench(&mut self, cfg: Arc<tt::config::Config>) -> Result<(), Error> {
        let (mut req_tx, mut req_rx) = watch::channel(TaskReq::Ready);
        let r = self.bench_priv(cfg, &mut req_tx, &mut req_rx).await;
        // let _r = req_tx.send(TaskReq::Stop);
        return r;
    }
}

// fn test() {
//     let now = TS::now_ms();
//     let mono = TS::mono_ms();
//     info!("now : {}", now);
//     info!("mono: {}", mono);
//     info!("diff: {}", now - mono);
//     std::process::exit(0);
// }

#[tokio::main]
async fn main() {
    tq3::log::tracing_subscriber::init();

    // test();

    let args = CmdArgs::parse();

    let cfg = tt::config::Config::load_from_file(&args.config);
    debug!("cfg=[{:#?}]", cfg.raw());

    {
        let cfg = Arc::new(cfg);
        let mut bencher = BenchLatency::default();

        match bencher.bench(cfg).await {
            Ok(_) => {
                // info!("bench result ok");
            }
            Err(e) => {
                error!("bench result error [{}]", e);
            }
        }
    }
}
