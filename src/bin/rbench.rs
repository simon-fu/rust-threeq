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

#[derive(Default, Debug)]
struct TaskStati {
    packets: u64,
    lost: u64,
    qps: u64,
}

impl TaskStati {
    fn merge(&mut self, other: &TaskStati) {
        //self.latencyh.merge(&other.latencyh);
        self.packets += other.packets;
        self.lost += other.lost;
    }
}

#[derive(Debug)]
enum TaskEvent {
    Error(Error),
    Connected(Instant),
    Ready(Instant),
    Work(Instant),
    Packet(Instant, usize, u64),
    Result(Instant, TaskStati),
    Finished(u64),
}

type EVSender = mpsc::Sender<TaskEvent>;
type EVRecver = mpsc::Receiver<TaskEvent>;
type ReqRecver = watch::Receiver<TaskReq>;

const MAX_PUBS: usize = 10000;

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

    let mut pkt = init_conn_pkt(&acc, cfgw.raw().subs.protocol);
    pkt.clean_session = cfgw.raw().subs.clean_session;
    pkt.keep_alive = cfgw.raw().subs.keep_alive_secs as u16;
    let ack = sender.connect(pkt).await?;
    if ack.code != tt::ConnectReturnCode::Success {
        return Err(Error::Generic(format!("{:?}", ack)));
    }
    let _r = tx.send(TaskEvent::Connected(Instant::now())).await;

    let ack = sender
        .subscribe(tt::Subscribe::new(&cfgw.sub_topic(), cfg.subs.qos))
        .await?;
    for reason in &ack.return_codes {
        if !reason.is_success() {
            return Err(Error::Generic(format!("{:?}", ack)));
        }
    }

    let _r = tx.send(TaskEvent::Ready(Instant::now())).await;

    let mut pubers = vec![Puber::default(); 1];

    let mut stati = TaskStati::default();

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
                stati.lost += header.seq - *next_seq;
            }
        }

        let latency = TS::now_ms() - header.ts;
        let latency = if latency >= 0 { latency as u64 } else { 0 };
        let _r = tx
            .send(TaskEvent::Packet(Instant::now(), payload_size, latency))
            .await;
        stati.packets += 1;

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
            stati.lost += v.max_seq - v.next_seq;
        }
    }
    let _r = tx.send(TaskEvent::Result(result_time, stati)).await;

    sender.disconnect(tt::Disconnect::new()).await?;
    // debug!("finished");
    let _r = tx.send(TaskEvent::Finished(subid)).await;

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
    let (mut sender, mut recver) =
        tt::client::make_connection(&format!("pub{}", pubid), &cfgw.env().address)
            .await?
            .split();

    let mut pkt = init_conn_pkt(&acc, cfgw.raw().pubs.protocol);
    pkt.clean_session = cfgw.raw().pubs.clean_session;
    pkt.keep_alive = cfgw.raw().pubs.keep_alive_secs as u16;
    sender.connect(pkt).await?;
    drop(acc);
    let _r = tx.send(TaskEvent::Connected(Instant::now())).await;
    let _r = tx.send(TaskEvent::Ready(Instant::now())).await;
    let req = wait_for_req(&mut rx).await?;

    let mut header = Header::new(pubid as usize);
    header.max_seq = cfg.pubs.packets;

    let mut pacer = tq3::limit::Pacer::new(cfg.pubs.qps);

    if let TaskReq::KickXfer(t) = req {
        if cfg.pubs.packets > 0 {
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
                    .send(TaskEvent::Packet(Instant::now(), pkt0.payload.len(), 0))
                    .await;

                let _r = sender.publish(pkt0).await?;
                header.seq += 1;
            }
            let _r = tx.send(TaskEvent::Work(start_time)).await;
        } else {
            loop {
                let ev = recver.recv().await?;
                if let tt::client::Event::Closed(_reason) = ev {
                    break;
                }
            }
        }
    }
    let t = Instant::now();
    let elapsed_ms = pacer.kick_time().elapsed().as_millis() as u64;
    // debug!("elapsed_ms {}", elapsed_ms);

    let mut stati = TaskStati::default();
    if elapsed_ms > 1000 {
        stati.qps = cfg.pubs.packets * 1000 / elapsed_ms;
    } else {
        stati.qps = header.seq;
    }
    let _r = tx.send(TaskEvent::Result(t, stati)).await;

    sender.disconnect(tt::Disconnect::new()).await?;

    // debug!("finished");
    let _r = tx.send(TaskEvent::Finished(pubid)).await;

    Ok(())
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

#[derive(Debug, Default, Clone)]
struct Traffic {
    pub packets: u64,
    pub bytes: u64,
}

impl Traffic {
    fn inc(&mut self, bytes: u64) {
        self.packets += 1;
        self.bytes += bytes;
    }
}

#[derive(Debug)]
struct TrafficSpeed {
    last_time: Instant,
    next_time: Instant,
    traffic: Traffic,
}

impl Default for TrafficSpeed {
    fn default() -> Self {
        Self {
            last_time: Instant::now(),
            next_time: Instant::now() + INTERVAL,
            traffic: Traffic::default(),
        }
    }
}

impl TrafficSpeed {
    fn reset(&mut self, now: Instant) {
        self.last_time = now;
        self.next_time = now + INTERVAL;
    }

    fn check(&mut self, now: Instant, t: &Traffic) -> Option<(u64, u64)> {
        if now < self.next_time {
            return None;
        }
        let d = now - self.last_time;
        let d = d.as_millis() as u64;
        if d == 0 {
            return None;
        }
        let r = (
            (t.packets - self.traffic.packets) * 1000 / d,
            (t.bytes - self.traffic.bytes) * 1000 / d / 1000,
        );

        self.traffic.packets = t.packets;
        self.traffic.bytes = t.bytes;
        self.reset(now);

        return Some(r);
    }
}

#[derive(Debug, Default)]
struct Sessions {
    name: String,
    num_conns: u64,
    num_readys: u64,
    num_results: u64,
    num_finisheds: u64,
    traffic: Traffic,
    stati: TaskStati,
    latencyh: Histogram,
    qps_h: Histogram,
    work_range: InstantRange,
    result_range: InstantRange,
    speed: TrafficSpeed,
}

impl Sessions {
    fn customize(&mut self, name: &str, conns: u64) {
        self.name = name.to_string();
        self.num_conns = conns;
    }

    async fn recv_event(&mut self, ev_rx: &mut EVRecver) -> Result<(), Error> {
        let ev = ev_rx.recv().await.unwrap();
        match ev {
            TaskEvent::Error(e) => {
                return Err(e);
            }
            TaskEvent::Connected(_) => {}
            TaskEvent::Ready(_) => {
                self.num_readys += 1;
            }
            TaskEvent::Work(t) => {
                self.work_range.update(t);
            }
            TaskEvent::Packet(_t, size, d) => {
                //self.packet_speed.inc_pub(Instant::now(), 1, size);
                self.traffic.inc(size as u64);
                if let Some(r) = self.speed.check(Instant::now(), &self.traffic) {
                    debug!("{}: [{} q/s, {} KB/s]", self.name, r.0, r.1,);
                }
                let _r = self.latencyh.increment(d * 1000_000);
            }
            TaskEvent::Result(t, s) => {
                self.num_results += 1;
                self.stati.merge(&s);
                let _r = self.qps_h.increment(s.qps);
                if self.result_range.update(t) {
                    debug!("{}: first result", self.name); // aaa
                }
            }
            TaskEvent::Finished(_n) => {
                self.num_finisheds += 1;
            }
        }
        Ok(())
    }

    async fn wait_for_ready(
        &mut self,
        ev_rx: &mut EVRecver,
        interval: &mut tq3::limit::Interval,
    ) -> Result<(), Error> {
        if self.num_conns > 0 {
            while self.num_readys < self.num_conns {
                if interval.check() {
                    debug!("{}: setup connections {}", self.name, self.num_readys);
                }
                self.recv_event(ev_rx).await?;
            }
            info!("{}: setup connections {}", self.name, self.num_readys);
        }
        Ok(())
    }

    async fn wait_for_result(&mut self, ev_rx: &mut EVRecver) -> Result<(), Error> {
        if self.num_conns > 0 {
            while !self.is_recv_results() {
                self.recv_event(ev_rx).await?;
            }
            debug!("{}: recv all result", self.name);
        }
        Ok(())
    }

    fn is_recv_results(&self) -> bool {
        self.num_results >= self.num_conns
    }
}

#[derive(Debug, Default)]
struct BenchLatency {
    pub_sessions: Sessions,
    sub_sessions: Sessions,
}

impl BenchLatency {
    fn print(&self, cfg: &Arc<tt::config::Config>) {
        let sessions = &self.pub_sessions;
        info!("");
        info!("Pub connections: {}", cfg.raw().pubs.connections);
        info!("Pub packets: {} packets/connection", cfg.raw().pubs.packets);
        tq3::histogram::print_summary("Pub QPS", "qps/connection", &sessions.qps_h);

        let sessions = &self.sub_sessions;
        info!("");
        info!("Sub connections: {}", cfg.raw().subs.connections);
        info!("Sub recv packets: {}", sessions.stati.packets);
        info!("Sub lost packets: {}", sessions.stati.lost);
        tq3::histogram::print_duration("Sub Latency", &sessions.latencyh);
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

        self.pub_sessions.customize("pub", cfg.pubs.connections);
        self.sub_sessions.customize("sub", cfg.subs.connections);

        let mut accounts = AccountIter::new(&cfgw.env().accounts);
        let (pub_tx, mut pub_rx) = mpsc::channel(10240);
        let (sub_tx, mut sub_rx) = mpsc::channel(10240);

        if cfg.subs.connections > 0 {
            let pacer = tq3::limit::Pacer::new(cfg.subs.conn_per_sec);
            let mut interval = tq3::limit::Interval::new(1000);
            let mut n = 0;
            while n < cfg.subs.connections {
                if let Some(d) = pacer.get_sleep_duration(n) {
                    tokio::time::sleep(d).await;
                }

                // pacer.check_and_wait(n).await;
                if interval.check() {
                    debug!(
                        "spawned sub tasks {}, connections {}",
                        n, self.sub_sessions.num_readys
                    );
                }

                let acc = accounts.next().unwrap();
                let cfg0 = cfgw.clone();
                let tx0 = sub_tx.clone();
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

            debug!("spawned sub tasks {}", cfg.subs.connections);
            self.sub_sessions
                .wait_for_ready(&mut sub_rx, &mut interval)
                .await?;
        }

        if cfg.pubs.connections > 0 {
            debug!("pub: try spawn tasks {}", cfg.pubs.connections);
            let pacer = tq3::limit::Pacer::new(cfg.pubs.conn_per_sec);
            let mut interval = tq3::limit::Interval::new(1000);
            let mut n = 0;

            while n < cfg.pubs.connections {
                if let Some(d) = pacer.get_sleep_duration(n) {
                    tokio::time::sleep(d).await;
                }

                if interval.check() {
                    debug!(
                        "pub: spawned tasks {}, connections {}",
                        n, self.pub_sessions.num_readys
                    );
                }

                let acc = accounts.next().unwrap();
                let cfg0 = cfgw.clone();
                let tx0 = pub_tx.clone();
                let rx0 = req_rx.clone();
                let f = async move {
                    let r = pub_task(n, cfg0, acc, &tx0, rx0).await;
                    if let Err(e) = r {
                        debug!("pub: task finished error [{:?}]", e);
                        let _r = tx0.send(TaskEvent::Error(e)).await;
                    }
                };
                let span = tracing::span!(tracing::Level::INFO, "", p = n);
                tokio::spawn(tracing::Instrument::instrument(f, span));
                n += 1;
            }

            debug!("pub: spawned tasks {}", cfg.pubs.connections);
            self.pub_sessions
                .wait_for_ready(&mut pub_rx, &mut interval)
                .await?;
        }

        let is_pub_packets = cfg.pubs.connections > 0 && cfg.pubs.packets > 0;
        if is_pub_packets {
            debug!("-> kick start");
        } else {
            debug!("wait for connections down...");
        }
        let kick_time = Instant::now();
        let _r = req_tx.send(TaskReq::KickXfer(kick_time));

        {
            let pub_fut = self.pub_sessions.wait_for_result(&mut pub_rx);
            let sub_fut = self.sub_sessions.wait_for_result(&mut sub_rx);

            tokio::pin!(pub_fut);
            tokio::pin!(sub_fut);

            let mut dead_line =
                tokio::time::Instant::now() + Duration::from_millis(cfg.recv_timeout_ms);
            let for_ever = tokio::time::Instant::now() + Duration::from_secs(9999999);

            let mut pub_done = false;
            let mut sub_done = false;

            while !pub_done || !sub_done {
                tokio::select! {
                    r = &mut pub_fut, if !pub_done => {
                        let _r = r?;
                        pub_done = true;
                        if is_pub_packets {
                            dead_line = tokio::time::Instant::now() + Duration::from_millis(cfg.recv_timeout_ms);
                        } else {
                            dead_line = for_ever;
                        }
                    }

                    r = &mut sub_fut, if !sub_done => {
                        let _r = r?;
                        sub_done = true;
                    }

                    _ = tokio::time::sleep_until(dead_line), if pub_done && !sub_done => {
                        // force stop
                        warn!("waiting for sub result timeout, send stop");
                        let _r = req_tx.send(TaskReq::Stop);
                        dead_line = for_ever;
                    }
                }
            }
        }

        debug!("<- all done");

        let elapsed = kick_time.elapsed();

        info!("");
        info!(
            "Pub start time  : {:?}",
            self.pub_sessions.work_range.delta_time_range(&kick_time)
        );
        info!(
            "Pub result time: {:?}",
            self.pub_sessions.result_range.delta_time_range(&kick_time)
        );
        info!(
            "Sub result time: {:?}",
            self.sub_sessions.result_range.delta_time_range(&kick_time)
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

#[tokio::main]
async fn main() {
    tq3::log::tracing_subscriber::init();

    let args = CmdArgs::parse();
    let cfg = tt::config::Config::load_from_file(&args.config);
    trace!("cfg=[{:#?}]", cfg.raw());

    // use std::num::Wrapping;
    // info!("0 - 1: {}", (Wrapping(0u16) - Wrapping(1u16)).0 );
    // info!("65535 + 1: {}", (Wrapping(65535u16) + Wrapping(1)).0 );

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
