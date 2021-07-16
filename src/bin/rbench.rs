use std::{
    fmt::Debug,
    sync::Arc,
    time::{Duration, Instant},
};

use bytes::{Buf, BufMut, BytesMut};
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
    latencyh: histogram::Histogram,
    lost: u64,
}

impl SubStati {
    fn merge(&mut self, other: &SubStati) {
        self.latencyh.merge(&other.latencyh);
        self.lost += other.lost;
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
    SubFinished(SubStati),
    PubFinished(PubStati),
}

#[derive(Debug, Clone, Copy)]
enum TaskReq {
    Ready,
    KickXfer(Instant),
    Stop,
}

type EVSender = mpsc::Sender<TaskEvent>;
type EVRecver = mpsc::Receiver<TaskEvent>;
type ReqRecver = watch::Receiver<TaskReq>;

fn is_recv_done(seqs: &Vec<u64>, packets: u64) -> bool {
    let mut done = true;
    for v in seqs {
        if *v < packets {
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
    n: u64,
    cfg: Arc<tt::config::Config>,
    acc: Account,
    tx: &EVSender,
    mut rx: ReqRecver,
) -> Result<(), Error> {
    let (mut sender, mut receiver) =
        tt::client::make_connection(&format!("sub{}", n), &cfg.env.address)
            .await?
            .split();

    let pkt = init_conn_pkt(&acc, tt::Protocol::V4);
    let ack = sender.connect(pkt).await?;
    if ack.code != tt::ConnectReturnCode::Success {
        return Err(Error::Generic(format!("{:?}", ack)));
    }
    let _r = tx.send(TaskEvent::SubConnected(Instant::now())).await;

    let ack = sender
        .subscribe(tt::Subscribe::new(&cfg.subs.topic, cfg.subs.qos))
        .await?;
    for reason in &ack.return_codes {
        if !reason.is_success() {
            return Err(Error::Generic(format!("{:?}", ack)));
        }
    }

    let _r = tx.send(TaskEvent::Subscribed(Instant::now())).await;

    let mut seqs = vec![0u64; cfg.pubs.connections as usize];

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

        let puber = rpkt.payload.get_u64();
        let seq = rpkt.payload.get_u64();
        let ts = rpkt.payload.get_i64();

        if puber >= seqs.len() as u64 {
            error!("puber exceed limit, expect {} but {}", seqs.len(), puber);
            break;
        }

        let next_seq = &mut seqs[puber as usize];

        if seq == 0 && *next_seq > 0 {
            // restart
            debug!("restart, n {}, npkt {}", seq, next_seq);
            break;
        } else if seq < *next_seq {
            return Err(Error::Generic(format!(
                "expect seq {}, but {}",
                *next_seq, seq
            )));
        } else if seq > *next_seq {
            stati.lost += seq - *next_seq;
        }

        let latency = TS::now_ms() - ts;
        let _r = stati.latencyh.increment(latency as u64);
        *next_seq = seq + 1;

        if *next_seq > cfg.pubs.packets {
            error!("seq exceed limit {}, puber {}", next_seq, puber);
            break;
        } else if *next_seq == cfg.pubs.packets {
            if is_recv_done(&seqs, cfg.pubs.packets) {
                break;
            }
        }
    }

    // check lost
    for v in &seqs {
        if *v < cfg.pubs.packets {
            stati.lost += cfg.pubs.packets - *v;
        }
    }

    let _r = tx.send(TaskEvent::SubFinished(stati)).await;

    Ok(())
}

async fn pub_task(
    n: u64,
    cfg: Arc<tt::config::Config>,
    acc: Account,
    tx: &EVSender,
    mut rx: ReqRecver,
) -> Result<(), Error> {
    let (mut sender, _recver) = tt::client::make_connection(&format!("pub{}", n), &cfg.env.address)
        .await?
        .split();
    drop(n);

    let pkt = init_conn_pkt(&acc, tt::Protocol::V4);
    sender.connect(pkt).await?;
    drop(acc);
    let _r = tx.send(TaskEvent::PubConnected(Instant::now())).await;

    let req = wait_for_req(&mut rx).await?;
    let mut seq = 0;
    let mut pacer = tq3::limit::Pacer::new(cfg.pubs.qps);
    if let TaskReq::KickXfer(t) = req {
        pacer = pacer.with_time(t);
        let mut buf = BytesMut::with_capacity(cfg.pubs.size);
        let pkt = tt::Publish::new(&cfg.pubs.topic, cfg.pubs.qos, []);

        while seq < cfg.pubs.packets {
            trace!("send No.{} packet", seq);

            if let Some(d) = pacer.get_sleep_duration(n) {
                tokio::time::sleep(d).await;
            }

            buf.reserve(cfg.pubs.size);

            let ts = TS::now_ms();
            buf.put_u64(n);
            buf.put_u64(seq);
            buf.put_i64(ts);
            let content = cfg.pubs.payload.as_bytes();
            buf.put_u64(content.len() as u64);
            buf.put(content);
            let remaining = cfg.pubs.size - buf.len();
            unsafe {
                buf.advance_mut(remaining);
            }

            let mut pkt0 = pkt.clone();
            pkt0.payload = buf.split().freeze();
            let _r = sender.publish(pkt0).await?;
            seq += 1;
        }
    }

    sender.disconnect(tt::Disconnect::new()).await?;

    let mut stati = PubStati::default();
    let elapsed_ms = pacer.kick_time().elapsed().as_millis() as u64;
    if elapsed_ms > 0 {
        stati.qps = cfg.pubs.packets * 1000 / elapsed_ms;
    } else {
        stati.qps = seq;
    }
    let _r = tx.send(TaskEvent::PubFinished(stati)).await;

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

#[derive(Default, Debug)]
struct BenchLatency {
    sub_conns: u64,
    pub_conns: u64,
    subscribes: u64,
    sub_finished: u64,
    pub_finished: u64,
    sub_stati: SubStati,
    pub_qos_h: Histogram,
}

impl BenchLatency {
    async fn recv_event(&mut self, ev_rx: &mut EVRecver) -> Result<(), Error> {
        let ev = ev_rx.recv().await.unwrap();
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
            TaskEvent::SubFinished(s) => {
                self.sub_finished += 1;
                self.sub_stati.merge(&s);
            }
            TaskEvent::PubFinished(s) => {
                self.pub_finished += 1;
                let _r = self.pub_qos_h.increment(s.qps);
            }
        }
        Ok(())
    }

    fn print(&self, cfg: &Arc<tt::config::Config>) {
        info!("");
        info!("Pub connections: {}", cfg.pubs.connections);
        info!("Pub packets: {} packets/connection", cfg.pubs.packets);
        print_histogram_summary("Pub QPS", "qps/connection", &self.pub_qos_h);

        info!("");
        info!("Sub connections: {}", cfg.subs.connections);
        info!("Sub recv packets: {}", self.sub_stati.latencyh.entries());
        info!("Sub lost packets: {}", self.sub_stati.lost);
        print_histogram_summary("Sub Latency", "ms", &self.sub_stati.latencyh);
        print_histogram_percent("Sub Latency", "ms", &self.sub_stati.latencyh);
    }

    pub async fn bench_priv(
        &mut self,
        cfg: Arc<tt::config::Config>,
        req_tx: &mut watch::Sender<TaskReq>,
        req_rx: &mut watch::Receiver<TaskReq>,
    ) -> Result<(), Error> {
        let mut accounts = AccountIter::new(&cfg.env.accounts);
        let (ev_tx, mut ev_rx) = mpsc::channel(1024);
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
            let cfg0 = cfg.clone();
            let tx0 = ev_tx.clone();
            let rx0 = req_rx.clone();
            let f = async move {
                let r = sub_task(n, cfg0, acc, &tx0, rx0).await;
                if let Err(e) = r {
                    debug!("sub task finished error [{:?}]", e);
                    let _r = tx0.send(TaskEvent::Error(e)).await;
                }
            };
            let span = tracing::span!(tracing::Level::INFO, "", p = n);
            tokio::spawn(tracing::Instrument::instrument(f, span));
            n += 1;
        }

        // wati for sub connections and subscriptions
        while self.sub_conns < cfg.subs.connections && self.subscribes < cfg.subs.connections {
            self.recv_event(&mut ev_rx).await?;
        }
        debug!("setup sub connections {} done", cfg.subs.connections);

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
            let cfg0 = cfg.clone();
            let tx0 = ev_tx.clone();
            let rx0 = req_rx.clone();
            let f = async move {
                let r = pub_task(n, cfg0, acc, &tx0, rx0).await;
                if let Err(e) = r {
                    debug!("pub task finished error [{:?}]", e);
                    let _r = tx0.send(TaskEvent::Error(e)).await;
                }
            };
            let span = tracing::span!(tracing::Level::INFO, "", s = n);
            tokio::spawn(tracing::Instrument::instrument(f, span));
            n += 1;
        }

        // wait for pub connections
        while self.pub_conns < cfg.pubs.connections {
            self.recv_event(&mut ev_rx).await?;
        }
        debug!("setup pub connections {} done", cfg.pubs.connections);

        // kick publish
        let kick_time = Instant::now();
        let _r = req_tx.send(TaskReq::KickXfer(Instant::now()));

        debug!("waiting for pub finished...");
        while self.pub_finished < cfg.pubs.connections {
            self.recv_event(&mut ev_rx).await?;
        }
        debug!("all pub finished");

        debug!("waiting for sub finished...");
        if let Err(_) = tokio::time::timeout(Duration::from_millis(cfg.recv_timeout_ms), async {
            while self.sub_finished < cfg.subs.connections {
                self.recv_event(&mut ev_rx).await?;
            }
            Ok::<(), Error>(())
        })
        .await
        {
            // force stop
            warn!("waiting for sub finished timeout, send stop");
            let _r = req_tx.send(TaskReq::Stop);
        }

        // wait for sub finished again
        while self.sub_finished < cfg.subs.connections {
            self.recv_event(&mut ev_rx).await?;
        }
        let duration = kick_time.elapsed();
        debug!("all sub finished");

        self.print(&cfg);
        info!("");
        info!("Duration: {:?}", duration);

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
    TS::init();
    // test();
    let args = CmdArgs::parse();

    // let mut cfg = tt::config::Config::default();
    // if let Some(fname) = &args.config {
    let fname = &args.config;
    debug!("loading config file [{}]...", fname);
    let mut c = config::Config::default();
    c.merge(config::File::with_name(fname)).unwrap();
    let cfg = c.try_into().unwrap();
    debug!("loaded config file [{}]", fname);
    // }

    debug!("cfg=[{:?}]", cfg);

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
        // std::process::exit(0);
    }
}
