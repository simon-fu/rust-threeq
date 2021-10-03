pub mod body;
pub mod config;

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{bail, Result};
use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use futures::Future;
use histogram::Histogram;
use rust_threeq::tq3::{self, limit::Rate, TryRecv, TryRecvResult, TS};
use tokio::{
    sync::{mpsc, watch},
    task::JoinHandle,
    time::timeout,
};
use tracing::{debug, info, trace, warn};

// #[derive(Debug, thiserror::Error)]
// pub enum Error {
//     #[error("error: {0}")]
//     Generic(String),

//     #[error("{0}")]
//     WatchError(tokio::sync::watch::error::RecvError),

//     #[error("{0}")]
//     JoinError(#[from] JoinError),

//     #[error("{0}")]
//     Timeout(#[from] Elapsed),
// }

#[async_trait]
pub trait Suber {
    async fn connect(&mut self) -> Result<()>;
    async fn disconnect(&mut self) -> Result<()>;
    async fn recv(&mut self) -> Result<Bytes>;
}

#[async_trait]
pub trait Puber {
    async fn connect(&mut self) -> Result<()>;
    async fn disconnect(&mut self) -> Result<()>;
    async fn send(&mut self, data: Bytes) -> Result<()>;
    async fn flush(&mut self) -> Result<()>;
    async fn idle(&mut self) -> Result<()>;
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

    fn check_float(&mut self, now: Instant, t: &Traffic) -> Option<(f64, f64)> {
        if now < self.next_time {
            return None;
        }
        let d = now - self.last_time;
        let d = d.as_millis() as u64;
        if d == 0 {
            return None;
        }
        let r = (
            (t.packets - self.traffic.packets) as f64 * 1000.0 / d as f64,
            (t.bytes - self.traffic.bytes) as f64 * 1000.0 / d as f64 / 1000.0,
        );

        self.traffic.packets = t.packets;
        self.traffic.bytes = t.bytes;
        self.reset(now);

        return Some(r);
    }
}

#[derive(Debug, Clone, Copy)]
enum TaskReq {
    Ready,
    KickXfer(Instant),
    Stop,
}

#[derive(Default, Debug, Clone)]
struct TaskStati {
    packets: u64,
    lost: u64,
    qps: u64,
}

impl TaskStati {
    fn merge(&mut self, other: &TaskStati) {
        self.packets += other.packets;
        self.lost += other.lost;
    }
}

#[derive(Debug)]
enum TaskEvent {
    Error(anyhow::Error),
    // Connected(Instant),
    Ready(Instant),
    Work(Instant),
    Packet(Instant, usize, u64),
    Result(Instant, TaskStati),
    Finished(u64),
}

type EVSender = mpsc::Sender<TaskEvent>;
type EVRecver = mpsc::Receiver<TaskEvent>;
type ReqSender = watch::Sender<TaskReq>;
type ReqRecver = watch::Receiver<TaskReq>;

const MAX_PUBS: usize = 1_000_000_000;

#[derive(Debug, Default, Clone, Copy)]
struct PuberItem {
    next_seq: u64,
    max_seq: u64,
}

#[derive(Debug, Default, Clone)]
struct Checker {
    items: Vec<PuberItem>,
    stati: TaskStati,
    done: bool,
}

impl Checker {
    pub fn input_and_check(&mut self, now_ms: i64, header: &body::Header) -> Result<u64> {
        if header.pubid >= self.items.len() {
            if header.pubid >= MAX_PUBS {
                bail!(format!(
                    "pubid exceed limit, expect {} but {}",
                    MAX_PUBS, header.pubid
                ));
            }
            self.items.resize(header.pubid + 1, PuberItem::default());
        }

        let item = &mut self.items[header.pubid];
        item.max_seq = header.max_seq;

        let next_seq = &item.next_seq;

        if header.seq == 0 && *next_seq > 0 {
            // restart
            bail!(format!("restart, n {}, npkt {}", header.seq, next_seq));
        } else if header.seq < *next_seq {
            bail!(format!("expect seq {}, but {}", *next_seq, header.seq));
        } else if header.seq > *next_seq {
            debug!("detect lost, expect seq {}, but {:?}", *next_seq, header);
            self.stati.lost += header.seq - *next_seq;
        }

        let latency = now_ms - header.ts;
        let latency = if latency >= 0 { latency as u64 } else { 0 };

        self.stati.packets += 1;

        item.next_seq = header.seq + 1;

        if item.next_seq > item.max_seq {
            bail!(format!(
                "seq exceed limit {}, pubid {}",
                item.next_seq, header.pubid
            ));
        } else if item.next_seq == item.max_seq {
            if self.check_done() {
                self.done = true;
            }
        }

        Ok(latency)
    }

    pub fn check_lost(&mut self) {
        let mut num_nothing = 0usize;
        for (idx, v) in self.items.iter().enumerate() {
            // debug!("check_lost: puber[{}]: {:?}", idx, v);
            if v.next_seq < v.max_seq {
                let lost = v.max_seq - v.next_seq;
                self.stati.lost += lost;
                debug!(
                    "check_lost: puber[{}] next {}, max {}, lost {}",
                    idx, v.next_seq, v.max_seq, lost
                );
            } else if v.next_seq == 0 && v.max_seq == 0 {
                num_nothing += 1;
            }
        }
        if num_nothing > 0 {
            debug!(
                "check_lost: recv nothings of pubs {}/{}",
                num_nothing,
                self.items.len()
            );
        }

        if self.stati.lost > 0 {
            debug!("check_lost: lost {}", self.stati.lost);
        }

        if self.items.len() == 0 {
            warn!("check_lost: no pubs");
        }
    }

    pub fn is_recv_done(&self) -> bool {
        self.done
    }

    pub fn pull_stati(self) -> TaskStati {
        self.stati
    }

    fn check_done(&self) -> bool {
        let mut done = true;
        for v in &self.items {
            if v.next_seq < v.max_seq {
                done = false;
                break;
            }
        }
        return done;
    }
}

async fn wait_for_req(rx: &mut ReqRecver) -> Result<TaskReq> {
    if let Err(e) = rx.changed().await {
        bail!(e);
    }
    return Ok(*rx.borrow());
}

pub struct SubSession<T: Suber + Send> {
    client: T,
    id: u64,
    tx: EVSender,
    rx: ReqRecver,
}

impl<T: Suber + Send> SubSession<T> {
    fn new(client: T, id: u64, tx: EVSender, rx: ReqRecver) -> Self {
        Self { client, id, tx, rx }
    }

    async fn task_entry(&mut self) -> Result<()> {
        self.client.connect().await?;

        let _r = self.tx.send(TaskEvent::Ready(Instant::now())).await;

        let mut checker: Checker = Default::default();

        while !checker.is_recv_done() {
            let mut data = tokio::select! {
                r = self.client.recv() => {
                    r?
                }

                r = wait_for_req(&mut self.rx) => {
                    let req = r?;
                    match req {
                        TaskReq::Stop => { break; },
                        _ => {
                            continue;
                        }
                    }
                }
            };

            let header = body::Header::decode(&mut data);
            let latency = checker.input_and_check(TS::now_ms(), &header)?;
            trace!("recv packet {:?}", header);

            let _r = self
                .tx
                .send(TaskEvent::Packet(Instant::now(), data.len(), latency))
                .await;
        }
        let result_time = Instant::now();

        checker.check_lost();

        let _r = self
            .tx
            .send(TaskEvent::Result(result_time, checker.pull_stati()))
            .await;

        self.client.disconnect().await?;

        let _r = self.tx.send(TaskEvent::Finished(self.id)).await;

        Ok(())
    }
}

pub struct PubArgs {
    pub qps: Rate,
    pub packets: u64,
    pub padding_to_size: usize,
    pub content: Bytes,
}

pub struct PubSession<T: Puber + Send> {
    client: T,
    id: u64,
    index: u64,
    tx: EVSender,
    rx: ReqRecver,
    args: Arc<PubArgs>,
}

impl<T: Puber + Send> PubSession<T> {
    fn new(
        client: T,
        id: u64,
        index: u64,
        tx: EVSender,
        rx: ReqRecver,
        args: Arc<PubArgs>,
    ) -> Self {
        Self {
            client,
            id,
            index,
            tx,
            rx,
            args,
        }
    }

    async fn task_entry(&mut self) -> Result<()> {
        self.client.connect().await?;

        let _r = self.tx.send(TaskEvent::Ready(Instant::now())).await;
        let req = wait_for_req(&mut self.rx).await?;

        let mut header = body::Header::new(self.index as usize);
        header.max_seq = self.args.packets;

        let mut pacer = tq3::limit::Pacer::new(self.args.qps);

        if let TaskReq::KickXfer(t) = req {
            if self.args.packets > 0 {
                let mut buf = BytesMut::new();
                // let pkt = tt::Publish::new(&cfgw.pub_topic(), cfg.pubs.qos, []);
                pacer = pacer.with_time(t);
                let start_time = Instant::now();

                while header.seq < self.args.packets {
                    if let Some(d) = pacer.get_sleep_duration(header.seq) {
                        tokio::time::sleep(d).await;
                    }

                    header.ts = TS::now_ms();

                    trace!("send packet {:?}", header);

                    body::encode_binary(
                        &header,
                        &self.args.content,
                        self.args.padding_to_size,
                        &mut buf,
                    );
                    let data = buf.split().freeze();

                    let _r = self
                        .tx
                        .send(TaskEvent::Packet(Instant::now(), data.len(), 0))
                        .await;

                    self.client.send(data).await?;

                    header.seq += 1;
                }
                let _r = self.tx.send(TaskEvent::Work(start_time)).await;
            } else {
                self.client.idle().await?;
            }
        }
        self.client.flush().await?;

        let t = Instant::now();
        let elapsed_ms = pacer.kick_time().elapsed().as_millis() as u64;
        // debug!("elapsed_ms {}", elapsed_ms);

        let mut stati = TaskStati::default();
        if elapsed_ms > 1000 {
            stati.qps = self.args.packets * 1000 / elapsed_ms;
        } else {
            stati.qps = header.seq;
        }
        let _r = self.tx.send(TaskEvent::Result(t, stati)).await;

        self.client.disconnect().await?;

        let _r = self.tx.send(TaskEvent::Finished(self.id)).await;

        Ok(())
    }
}

#[derive(Debug, Default)]
struct SessionsResult {
    name: String,
    num_tasks: u64,
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
    next_ready_ms: i64,
}

impl SessionsResult {
    fn customize(&mut self, name: &str, conns: u64) {
        self.name = name.to_string();
        self.num_conns = conns;
    }

    fn print_readys(&self) {
        debug!(
            "{}: connections progress {}/{}",
            self.name, self.num_readys, self.num_tasks,
        );
    }

    async fn handle_event(&mut self, ev: TaskEvent) -> Result<()> {
        match ev {
            TaskEvent::Error(e) => {
                return Err(e);
            }
            // TaskEvent::Connected(_) => {}
            TaskEvent::Ready(_) => {
                self.num_readys += 1;
                if TS::mono_ms() >= self.next_ready_ms {
                    self.print_readys();
                    self.next_ready_ms = TS::mono_ms() + 1000;
                }
            }
            TaskEvent::Work(t) => {
                self.work_range.update(t);
            }
            TaskEvent::Packet(_t, size, d) => {
                //self.packet_speed.inc_pub(Instant::now(), 1, size);
                self.traffic.inc(size as u64);
                if let Some(r) = self.speed.check_float(Instant::now(), &self.traffic) {
                    debug!("{}: [{:.2} q/s, {:.2} KB/s]", self.name, r.0, r.1,);
                }
                let _r = self.latencyh.increment(d * 1000_000);
            }
            TaskEvent::Result(t, s) => {
                self.num_results += 1;
                self.stati.merge(&s);
                let _r = self.qps_h.increment(s.qps);
                self.result_range.update(t);

                if self.num_results == 1 {
                    debug!("{}: recv first result", self.name);
                }
                if self.num_results >= self.num_conns {
                    debug!("{}: recv all result", self.name);
                }
            }
            TaskEvent::Finished(_n) => {
                self.num_finisheds += 1;
            }
        }

        Ok(())
    }

    async fn recv_event(&mut self, ev_rx: &mut EVRecver) -> Result<bool> {
        let o = ev_rx.recv().await;
        if o.is_none() {
            return Ok(false);
        }

        self.handle_event(o.unwrap()).await?;

        Ok(true)
    }

    async fn try_recv_event(&mut self, ev_rx: &mut EVRecver) -> Result<bool> {
        let r = TryRecv::new(ev_rx).await;
        match r {
            TryRecvResult::Value(ev) => {
                self.handle_event(ev).await?;
                return Ok(true);
            }
            TryRecvResult::Empty => {}
            TryRecvResult::NoSender => {}
        }
        Ok(false)
    }

    async fn wait_for_ready(&mut self, ev_rx: &mut EVRecver) -> Result<()> {
        if self.num_readys < self.num_tasks {
            debug!("{}: waiting for connections ready...", self.name);
            while self.num_readys < self.num_tasks {
                self.recv_event(ev_rx).await?;
            }
            self.print_readys();
            debug!("{}: setup connections {}", self.name, self.num_readys);
        }
        Ok(())
    }

    pub async fn launch(
        mut self: Box<Self>,
        mut ev_rx: EVRecver,
    ) -> Result<JoinHandle<Result<Box<Self>>>> {
        let h = tokio::spawn(async move {
            loop {
                if !self.recv_event(&mut ev_rx).await? {
                    break;
                }
            }
            Ok::<Box<Self>, anyhow::Error>(self)
        });
        Ok(h)
    }
}

#[derive(Debug, Default)]
pub struct Sessions {
    name: String,
    results: Option<Box<SessionsResult>>,
    merge_task: Option<JoinHandle<Result<Box<SessionsResult>>>>,
}

impl Sessions {
    pub fn new(name: String) -> Self {
        Self {
            name,
            results: None,
            merge_task: None,
        }
    }

    async fn launch<F, R: 'static>(
        &mut self,
        req_rx: &ReqRecver,
        session_id: &mut u64,
        num_sessions: u64,
        sessions_per_sec: u64,
        mut factory: F,
    ) -> Result<()>
    where
        F: FnMut(u64, EVSender, ReqRecver) -> R,
        R: Future<Output = ()> + Send,
    {
        let mut results = Box::new(SessionsResult::default());
        results.customize(&self.name, num_sessions);

        if num_sessions == 0 {
            self.results = Some(results);
            return Ok(());
        }

        let (ev_tx, mut ev_rx) = mpsc::channel(10240);
        let mut pacer = tq3::limit::Pacer::new(Rate::new(sessions_per_sec, 1));
        while results.num_tasks < num_sessions {
            {
                let remains = results.num_tasks - results.num_readys;
                if remains >= sessions_per_sec {
                    debug!(
                        "too slow, remains({}/{}, {}) >= rate({}) ",
                        results.num_readys, results.num_tasks, remains, sessions_per_sec
                    );
                    while results.num_readys < results.num_tasks {
                        results.recv_event(&mut ev_rx).await?;
                    }
                    pacer.kick(); // reset timer
                }
            }

            if let Some(d) = pacer.get_sleep_duration(results.num_tasks) {
                if results.num_readys < results.num_tasks {
                    if results.try_recv_event(&mut ev_rx).await? {
                        continue;
                    }
                }
                tokio::time::sleep(d).await;
            }

            let f = factory(*session_id, ev_tx.clone(), req_rx.clone());
            // debug!("recver future size {}", std::mem::size_of_val(&f));
            let name0 = format!("{}-{}", self.name, *session_id);
            let span = tracing::span!(tracing::Level::INFO, "", s = &name0[..]);
            tokio::spawn(tracing::Instrument::instrument(f, span));
            results.num_tasks += 1;
            *session_id += 1;
        }

        results.wait_for_ready(&mut ev_rx).await?;

        self.merge_task = Some(results.launch(ev_rx).await?);

        Ok(())
    }

    pub async fn wait_for_finished(&mut self) -> Result<()> {
        if self.merge_task.is_some() {
            let results = self.merge_task.as_mut().unwrap().await??;
            self.merge_task.take();
            self.results = Some(results);
        }
        Ok(())
    }
}

#[derive(Debug, Default)]
struct SessionGroups {
    groups: Vec<Sessions>,
}

impl SessionGroups {
    async fn wait_for_finished(&mut self) -> Result<()> {
        for sss in &mut self.groups {
            sss.wait_for_finished().await?;
        }
        Ok(())
    }
}

#[derive(Debug)]
pub struct PubsubBencher {
    req_tx: ReqSender,
    req_rx: ReqRecver,
    pubs: SessionGroups,
    subs: SessionGroups,
    is_pub_packets: bool,
}

impl PubsubBencher {
    pub fn new() -> Self {
        let (req_tx, req_rx) = watch::channel(TaskReq::Ready);
        Self {
            req_tx,
            req_rx,
            pubs: Default::default(),
            subs: Default::default(),
            is_pub_packets: false,
        }
    }

    pub async fn launch_sub_sessions<F, T: 'static + Suber + Send>(
        &mut self,
        name: String,
        session_id: &mut u64,
        num_sessions: u64,
        sessions_per_sec: u64,
        mut factory: F,
    ) -> Result<()>
    where
        F: FnMut(u64) -> T,
    {
        let mut sss = Sessions::new(name);
        sss.launch(
            &self.req_rx,
            session_id,
            num_sessions,
            sessions_per_sec,
            |id, tx, rx| {
                let client = factory(id);
                let mut s0 = SubSession::new(client, id, tx, rx);
                let f = async move {
                    let r = s0.task_entry().await;
                    if let Err(e) = r {
                        debug!("task finished error [{:?}]", e);
                        let _r = s0.tx.send(TaskEvent::Error(e)).await;
                    }
                };
                f
            },
        )
        .await?;
        self.subs.groups.push(sss);
        Ok(())
    }

    pub async fn launch_pub_sessions<F, T: 'static + Puber + Send>(
        &mut self,
        name: String,
        session_id: &mut u64,
        num_sessions: u64,
        sessions_per_sec: u64,
        args: Arc<PubArgs>,
        mut factory: F,
    ) -> Result<()>
    where
        F: FnMut(u64) -> (u64, T),
    {
        let mut sss = Sessions::new(name);
        sss.launch(
            &self.req_rx,
            session_id,
            num_sessions,
            sessions_per_sec,
            |id, tx, rx| {
                let (index, client) = factory(id);
                let mut s0 = PubSession::new(client, id, index, tx, rx, args.clone());
                let f = async move {
                    let r = s0.task_entry().await;
                    if let Err(e) = r {
                        debug!("task finished error [{:?}]", e);
                        let _r = s0.tx.send(TaskEvent::Error(e)).await;
                    }
                };
                f
            },
        )
        .await?;

        self.pubs.groups.push(sss);

        if num_sessions > 0 && args.packets > 0 {
            self.is_pub_packets = true;
        }

        Ok(())
    }

    pub async fn kick_and_wait(&mut self, timeout_ms: u64) -> Result<()> {
        let kick_time = Instant::now();
        if self.is_pub_packets {
            debug!("-> kick start");

            let _r = self.req_tx.send(TaskReq::KickXfer(kick_time));
            self.pubs.wait_for_finished().await?;

            let r = timeout(Duration::from_millis(timeout_ms), async {
                self.subs.wait_for_finished().await?;
                Ok::<(), anyhow::Error>(())
            })
            .await;

            match r {
                Ok(r0) => {
                    r0?;
                }
                Err(_) => {
                    warn!("waiting for sub result timeout, send stop");
                    let _r = self.req_tx.send(TaskReq::Stop);
                    self.subs.wait_for_finished().await?;
                }
            }
        } else {
            debug!("wait for connections down...");
            self.pubs.wait_for_finished().await?;
            self.subs.wait_for_finished().await?;
        }

        debug!("<- all done");
        let elapsed = kick_time.elapsed();

        info!("");
        debug!("Elapsed: {:?}", elapsed);

        self.print(&kick_time);

        Ok(())
    }

    fn print(&self, kick_time: &Instant) {
        for sss in &self.pubs.groups {
            if let Some(results) = &sss.results {
                info!(
                    "{}: start time  : {:?}",
                    results.name,
                    results.work_range.delta_time_range(&kick_time)
                );
            }
        }

        for sss in &self.pubs.groups {
            if let Some(results) = &sss.results {
                info!(
                    "{}: result time: {:?}",
                    results.name,
                    results.result_range.delta_time_range(&kick_time)
                );
            }
        }

        for sss in &self.subs.groups {
            if let Some(results) = &sss.results {
                info!(
                    "{}: result time: {:?}",
                    results.name,
                    results.result_range.delta_time_range(&kick_time)
                );
            }
        }

        info!("");

        for sss in &self.pubs.groups {
            if let Some(results) = &sss.results {
                tq3::histogram::print_summary(
                    &format!("{} QPS", results.name),
                    "qps/connection",
                    &results.qps_h,
                );
            }
        }

        for sss in &self.subs.groups {
            if let Some(results) = &sss.results {
                info!("{}: recv packets {}", results.name, results.stati.packets);
                if results.stati.lost == 0 {
                    info!("{}: lost packets {}", results.name, results.stati.lost);
                } else {
                    warn!("{}: lost packets {}", results.name, results.stati.lost);
                }

                tq3::histogram::print_duration(
                    &format!("{} Latency", results.name),
                    &results.latencyh,
                );
            }
        }
    }
}
