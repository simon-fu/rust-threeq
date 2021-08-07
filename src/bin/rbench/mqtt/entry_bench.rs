// reqwest=info,hyper=info

// use std::time::{Duration, Instant};
// use bytes::BytesMut;
// use histogram::Histogram;
// use tokio::task::JoinHandle;
// use tokio::{sync::{mpsc, watch}, time::timeout,};
// use tracing::{debug, info, warn};
// use super::super::common::body;
// use rust_threeq::tq3::{self, TryRecv, TryRecvResult, TS};

use super::super::common;
use super::config as app;
use async_trait::async_trait;
use bytes::Bytes;
use rust_threeq::tq3::tt;
use std::sync::Arc;
use tokio::{task::JoinError, time::error::Elapsed};
use tracing::{error, trace};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    // #[error("error: {0}")]
    // Generic(String),
    #[error("{0}")]
    ClientError(#[from] tt::client::Error),

    // #[error("{0}")]
    // WatchError(tokio::sync::watch::error::RecvError),
    #[error("{0}")]
    ReqwestError(#[from] reqwest::Error),

    #[error("{0}")]
    JoinError(#[from] JoinError),

    #[error("{0}")]
    Timeout(#[from] Elapsed),

    #[error("{0}")]
    RunError(#[from] common::Error),
}

// #[derive(Debug, Clone, Copy)]
// enum TaskReq {
//     Ready,
//     KickXfer(Instant),
//     Stop,
// }

// #[derive(Default, Debug)]
// struct TaskStati {
//     packets: u64,
//     lost: u64,
//     qps: u64,
// }

// impl TaskStati {
//     fn merge(&mut self, other: &TaskStati) {
//         //self.latencyh.merge(&other.latencyh);
//         self.packets += other.packets;
//         self.lost += other.lost;
//     }
// }

// #[derive(Debug)]
// enum TaskEvent {
//     Error(Error),
//     Connected(Instant),
//     Ready(Instant),
//     Work(Instant),
//     Packet(Instant, usize, u64),
//     Result(Instant, TaskStati),
//     Finished(u64),
// }

// type EVSender = mpsc::Sender<TaskEvent>;
// type EVRecver = mpsc::Receiver<TaskEvent>;
// type ReqRecver = watch::Receiver<TaskReq>;

// const MAX_PUBS: usize = 10000;

// #[derive(Debug, Default, Clone, Copy)]
// struct Puber {
//     next_seq: u64,
//     max_seq: u64,
// }

// fn is_recv_done(pubers: &Vec<Puber>) -> bool {
//     let mut done = true;
//     for v in pubers {
//         if v.next_seq < v.max_seq {
//             done = false;
//             break;
//         }
//     }
//     return done;
// }

// async fn wait_for_req(rx: &mut ReqRecver) -> Result<TaskReq, Error> {
//     if let Err(e) = rx.changed().await {
//         return Err(Error::WatchError(e));
//     }
//     return Ok(*rx.borrow());
// }

// async fn sub_task(
//     subid: u64,
//     cfgw: Arc<app::Config>,
//     acc: app::Account,
//     tx: &EVSender,
//     mut rx: ReqRecver,
// ) -> Result<(), Error> {
//     let cfg = cfgw.raw();
//     let (mut sender, mut receiver) =
//         tt::client::make_connection(&format!("sub{}", subid), &cfgw.env().address)
//             .await?
//             .split();

//     let mut pkt = app::init_conn_pkt(&acc, cfgw.raw().subs.protocol);
//     pkt.clean_session = cfgw.raw().subs.clean_session;
//     pkt.keep_alive = cfgw.raw().subs.keep_alive_secs as u16;
//     let ack = sender.connect(pkt).await?;
//     if ack.code != tt::ConnectReturnCode::Success {
//         return Err(Error::Generic(format!("{:?}", ack)));
//     }
//     let _r = tx.send(TaskEvent::Connected(Instant::now())).await;

//     let ack = sender
//         .subscribe(tt::Subscribe::new(&cfgw.sub_topic(), cfg.subs.qos))
//         .await?;
//     for reason in &ack.return_codes {
//         if !reason.is_success() {
//             return Err(Error::Generic(format!("{:?}", ack)));
//         }
//     }

//     let _r = tx.send(TaskEvent::Ready(Instant::now())).await;

//     let mut pubers = vec![Puber::default(); 1];

//     let mut stati = TaskStati::default();

//     loop {
//         let ev = tokio::select! {
//             r = receiver.recv() => {
//                 r?
//             }

//             r = wait_for_req(&mut rx) => {
//                 let req = r?;
//                 match req {
//                     TaskReq::Stop => { break; },
//                     _ => continue,
//                 }
//             }
//         };

//         let mut rpkt = match ev {
//             tt::client::Event::Packet(pkt) => match pkt {
//                 tt::Packet::Publish(rpkt) => rpkt,
//                 _ => return Err(Error::Generic(format!("unexpect packet {:?}", pkt))),
//             },
//             tt::client::Event::Closed(s) => {
//                 debug!("got closed [{}]", s);
//                 break;
//             }
//         };

//         let payload_size = rpkt.payload.len();
//         let header = body::Header::decode(&mut rpkt.payload);

//         if header.pubid >= pubers.len() {
//             if header.pubid >= MAX_PUBS {
//                 error!(
//                     "pubid exceed limit, expect {} but {}",
//                     MAX_PUBS, header.pubid
//                 );
//                 break;
//             }
//             pubers.resize(header.pubid + 1, Puber::default());
//         }

//         let puber = &mut pubers[header.pubid];
//         puber.max_seq = header.max_seq;

//         {
//             let next_seq = &puber.next_seq;

//             if header.seq == 0 && *next_seq > 0 {
//                 // restart
//                 debug!("restart, n {}, npkt {}", header.seq, next_seq);
//                 return Err(Error::Generic(format!(
//                     "restart, n {}, npkt {}",
//                     header.seq, next_seq
//                 )));
//             } else if header.seq < *next_seq {
//                 return Err(Error::Generic(format!(
//                     "expect seq {}, but {}",
//                     *next_seq, header.seq
//                 )));
//             } else if header.seq > *next_seq {
//                 stati.lost += header.seq - *next_seq;
//             }
//         }

//         let latency = TS::now_ms() - header.ts;
//         let latency = if latency >= 0 { latency as u64 } else { 0 };
//         let _r = tx
//             .send(TaskEvent::Packet(Instant::now(), payload_size, latency))
//             .await;
//         stati.packets += 1;

//         puber.next_seq = header.seq + 1;

//         if puber.next_seq > cfg.pubs.packets {
//             error!(
//                 "seq exceed limit {}, pubid {}",
//                 puber.next_seq, header.pubid
//             );
//             break;
//         } else if puber.next_seq == cfg.pubs.packets {
//             if is_recv_done(&pubers) {
//                 break;
//             }
//         }
//     }
//     let result_time = Instant::now();
//     // check lost
//     for v in &pubers {
//         if v.next_seq < v.max_seq {
//             stati.lost += v.max_seq - v.next_seq;
//         }
//     }
//     let _r = tx.send(TaskEvent::Result(result_time, stati)).await;

//     sender.disconnect(tt::Disconnect::new()).await?;
//     // debug!("finished");
//     let _r = tx.send(TaskEvent::Finished(subid)).await;

//     Ok(())
// }

// async fn pub_task(
//     pubid: u64,
//     cfgw: Arc<app::Config>,
//     acc: app::Account,
//     tx: &EVSender,
//     mut rx: ReqRecver,
// ) -> Result<(), Error> {
//     let cfg = cfgw.raw();
//     let (mut sender, mut recver) =
//         tt::client::make_connection(&format!("pub{}", pubid), &cfgw.env().address)
//             .await?
//             .split();

//     let mut pkt = app::init_conn_pkt(&acc, cfgw.raw().pubs.protocol);
//     pkt.clean_session = cfgw.raw().pubs.clean_session;
//     pkt.keep_alive = cfgw.raw().pubs.keep_alive_secs as u16;
//     sender.connect(pkt).await?;
//     drop(acc);
//     let _r = tx.send(TaskEvent::Connected(Instant::now())).await;
//     let _r = tx.send(TaskEvent::Ready(Instant::now())).await;
//     let req = wait_for_req(&mut rx).await?;

//     let mut header = body::Header::new(pubid as usize);
//     header.max_seq = cfg.pubs.packets;

//     let mut pacer = tq3::limit::Pacer::new(cfg.pubs.qps);

//     if let TaskReq::KickXfer(t) = req {
//         if cfg.pubs.packets > 0 {
//             let mut buf = BytesMut::new();
//             let pkt = tt::Publish::new(&cfgw.pub_topic(), cfg.pubs.qos, []);
//             pacer = pacer.with_time(t);
//             let start_time = Instant::now();

//             while header.seq < cfg.pubs.packets {
//                 trace!("send No.{} packet", header.seq);

//                 if let Some(d) = pacer.get_sleep_duration(header.seq) {
//                     tokio::time::sleep(d).await;
//                 }

//                 header.ts = TS::now_ms();
//                 body::encode_binary(
//                     &header,
//                     cfg.pubs.content.as_bytes(),
//                     cfg.pubs.padding_to_size,
//                     &mut buf,
//                 );

//                 let mut pkt0 = pkt.clone();
//                 pkt0.payload = buf.split().freeze();

//                 let _r = tx
//                     .send(TaskEvent::Packet(Instant::now(), pkt0.payload.len(), 0))
//                     .await;

//                 let _r = sender.publish(pkt0).await?;
//                 header.seq += 1;
//             }
//             let _r = tx.send(TaskEvent::Work(start_time)).await;
//         } else {
//             loop {
//                 let ev = recver.recv().await?;
//                 if let tt::client::Event::Closed(_reason) = ev {
//                     break;
//                 }
//             }
//         }
//     }
//     let t = Instant::now();
//     let elapsed_ms = pacer.kick_time().elapsed().as_millis() as u64;
//     // debug!("elapsed_ms {}", elapsed_ms);

//     let mut stati = TaskStati::default();
//     if elapsed_ms > 1000 {
//         stati.qps = cfg.pubs.packets * 1000 / elapsed_ms;
//     } else {
//         stati.qps = header.seq;
//     }
//     let _r = tx.send(TaskEvent::Result(t, stati)).await;

//     sender.disconnect(tt::Disconnect::new()).await?;

//     // debug!("finished");
//     let _r = tx.send(TaskEvent::Finished(pubid)).await;

//     Ok(())
// }

// #[derive(Debug, Default)]
// struct InstantRange {
//     first: Option<Instant>,
//     last: Option<Instant>,
// }

// impl InstantRange {
//     fn update(&mut self, t: Instant) -> bool {
//         if self.first.is_none() {
//             self.first = Some(t);
//             self.last = Some(t);
//             return true;
//         } else {
//             if t < *self.first.as_ref().unwrap() {
//                 self.first = Some(t);
//             }

//             if t > *self.last.as_ref().unwrap() {
//                 self.last = Some(t);
//             }
//             return false;
//         }
//     }

//     fn delta_time_range(&self, t: &Instant) -> (Duration, Duration) {
//         (
//             if self.first.is_none() {
//                 Duration::from_millis(0)
//             } else {
//                 *self.first.as_ref().unwrap() - *t
//             },
//             if self.last.is_none() {
//                 Duration::from_millis(0)
//             } else {
//                 *self.last.as_ref().unwrap() - *t
//             },
//         )
//     }
// }

// const INTERVAL: Duration = Duration::from_millis(1000);

// #[derive(Debug, Default, Clone)]
// struct Traffic {
//     pub packets: u64,
//     pub bytes: u64,
// }

// impl Traffic {
//     fn inc(&mut self, bytes: u64) {
//         self.packets += 1;
//         self.bytes += bytes;
//     }
// }

// #[derive(Debug)]
// struct TrafficSpeed {
//     last_time: Instant,
//     next_time: Instant,
//     traffic: Traffic,
// }

// impl Default for TrafficSpeed {
//     fn default() -> Self {
//         Self {
//             last_time: Instant::now(),
//             next_time: Instant::now() + INTERVAL,
//             traffic: Traffic::default(),
//         }
//     }
// }

// impl TrafficSpeed {
//     fn reset(&mut self, now: Instant) {
//         self.last_time = now;
//         self.next_time = now + INTERVAL;
//     }

//     fn check(&mut self, now: Instant, t: &Traffic) -> Option<(u64, u64)> {
//         if now < self.next_time {
//             return None;
//         }
//         let d = now - self.last_time;
//         let d = d.as_millis() as u64;
//         if d == 0 {
//             return None;
//         }
//         let r = (
//             (t.packets - self.traffic.packets) * 1000 / d,
//             (t.bytes - self.traffic.bytes) * 1000 / d / 1000,
//         );

//         self.traffic.packets = t.packets;
//         self.traffic.bytes = t.bytes;
//         self.reset(now);

//         return Some(r);
//     }
// }

// #[derive(Debug, Default)]
// struct Sessions {
//     name: String,
//     num_tasks: u64,
//     num_conns: u64,
//     num_readys: u64,
//     num_results: u64,
//     num_finisheds: u64,
//     traffic: Traffic,
//     stati: TaskStati,
//     latencyh: Histogram,
//     qps_h: Histogram,
//     work_range: InstantRange,
//     result_range: InstantRange,
//     speed: TrafficSpeed,
//     next_ready_ms: i64,
// }

// impl Sessions {
//     fn customize(&mut self, name: &str, conns: u64) {
//         self.name = name.to_string();
//         self.num_conns = conns;
//     }

//     fn print_readys(&self) {
//         debug!(
//             "{}: spawned tasks {}, connections {}",
//             self.name, self.num_tasks, self.num_readys
//         );
//     }

//     async fn handle_event(&mut self, ev: TaskEvent) -> Result<(), Error> {
//         match ev {
//             TaskEvent::Error(e) => {
//                 return Err(e);
//             }
//             TaskEvent::Connected(_) => {}
//             TaskEvent::Ready(_) => {
//                 self.num_readys += 1;
//                 if TS::mono_ms() >= self.next_ready_ms {
//                     self.print_readys();
//                     self.next_ready_ms = TS::mono_ms() + 1000;
//                 }
//             }
//             TaskEvent::Work(t) => {
//                 self.work_range.update(t);
//             }
//             TaskEvent::Packet(_t, size, d) => {
//                 //self.packet_speed.inc_pub(Instant::now(), 1, size);
//                 self.traffic.inc(size as u64);
//                 if let Some(r) = self.speed.check(Instant::now(), &self.traffic) {
//                     debug!("{}: [{} q/s, {} KB/s]", self.name, r.0, r.1,);
//                 }
//                 let _r = self.latencyh.increment(d * 1000_000);
//             }
//             TaskEvent::Result(t, s) => {
//                 self.num_results += 1;
//                 self.stati.merge(&s);
//                 let _r = self.qps_h.increment(s.qps);
//                 self.result_range.update(t);

//                 if self.num_results == 1 {
//                     debug!("{}: recv first result", self.name);
//                 }
//                 if self.num_results >= self.num_conns {
//                     debug!("{}: recv all result", self.name);
//                 }
//             }
//             TaskEvent::Finished(_n) => {
//                 self.num_finisheds += 1;
//             }
//         }

//         Ok(())
//     }

//     async fn recv_event(&mut self, ev_rx: &mut EVRecver) -> Result<bool, Error> {
//         let o = ev_rx.recv().await;
//         if o.is_none() {
//             return Ok(false);
//         }

//         self.handle_event(o.unwrap()).await?;

//         Ok(true)
//     }

//     async fn try_recv_event(&mut self, ev_rx: &mut EVRecver) -> Result<bool, Error> {
//         let r = TryRecv::new(ev_rx).await;
//         match r {
//             TryRecvResult::Value(ev) => {
//                 self.handle_event(ev).await?;
//                 return Ok(true);
//             }
//             TryRecvResult::Empty => {}
//             TryRecvResult::NoSender => {}
//         }
//         Ok(false)
//     }

//     async fn wait_for_ready(&mut self, ev_rx: &mut EVRecver) -> Result<(), Error> {
//         if self.num_readys < self.num_tasks {
//             debug!("waiting for connections ready...");
//             while self.num_readys < self.num_tasks {
//                 self.recv_event(ev_rx).await?;
//             }
//             self.print_readys();
//             debug!("{}: all connections ready", self.name);
//         }
//         Ok(())
//     }

//     pub async fn launch(
//         mut self: Box<Self>,
//         mut ev_rx: EVRecver,
//     ) -> Result<JoinHandle<Result<Box<Self>, Error>>, Error> {
//         let h = tokio::spawn(async move {
//             loop {
//                 if !self.recv_event(&mut ev_rx).await? {
//                     break;
//                 }
//             }
//             Ok::<Box<Self>, Error>(self)
//         });
//         Ok(h)
//     }
// }

// #[derive(Debug)]
// struct RestSession {
//     cfg: Arc<app::Config>,
//     tx: EVSender,
//     rx: ReqRecver,
// }

// impl RestSession {
//     pub fn new(cfg: Arc<app::Config>, tx: EVSender, rx: ReqRecver) -> Self {
//         Self { cfg, tx, rx }
//     }

//     pub async fn call_rest(cfg: &app::RestApiArg, s: String) -> Result<(), reqwest::Error> {
//         let req_body = cfg.make_body(&mut cfg.body.clone(), s);

//         {
//             let client = reqwest::Client::new();

//             let mut builder = client.post(&cfg.url);
//             for (k, v) in &cfg.headers {
//                 builder = builder.header(k, v);
//             }

//             trace!("request url ={:?}", cfg.url);
//             trace!("request body={:?}", req_body);

//             let res = builder.body(req_body).send().await?;
//             let rsp_status = res.status();
//             let rsp_body = res.text().await?;

//             trace!("response status: {}", rsp_status);
//             trace!("response body  : {}", rsp_body);
//         }
//         Ok(())
//     }

//     async fn task_entry(&mut self, pubid: u64) -> Result<(), Error> {
//         let _r = self.tx.send(TaskEvent::Ready(Instant::now())).await;
//         let req = wait_for_req(&mut self.rx).await?;

//         let mut header = body::Header::new(pubid as usize);
//         header.max_seq = self.cfg.raw().rest_pubs.packets;

//         let mut pacer = tq3::limit::Pacer::new(self.cfg.raw().rest_pubs.qps);

//         if let TaskReq::KickXfer(t) = req {
//             let mut buf = BytesMut::new();
//             pacer = pacer.with_time(t);
//             let start_time = Instant::now();

//             while header.seq < header.max_seq {
//                 trace!("send No.{} packet", header.seq);

//                 if let Some(d) = pacer.get_sleep_duration(header.seq) {
//                     tokio::time::sleep(d).await;
//                 }

//                 header.ts = TS::now_ms();
//                 body::encode_binary(
//                     &header,
//                     &[],
//                     self.cfg.raw().rest_pubs.padding_to_size,
//                     &mut buf,
//                 );

//                 let payload = buf.split().freeze();
//                 let payload = base64::encode(payload);
//                 let _r = self
//                     .tx
//                     .send(TaskEvent::Packet(Instant::now(), payload.len(), 0))
//                     .await;

//                 Self::call_rest(&self.cfg.env().rest_api, payload).await?;

//                 header.seq += 1;
//             }
//             let _r = self.tx.send(TaskEvent::Work(start_time)).await;
//         }
//         let t = Instant::now();
//         let elapsed_ms = pacer.kick_time().elapsed().as_millis() as u64;
//         // debug!("elapsed_ms {}", elapsed_ms);

//         let mut stati = TaskStati::default();
//         if elapsed_ms > 1000 {
//             stati.qps = header.max_seq * 1000 / elapsed_ms;
//         } else {
//             stati.qps = header.seq;
//         }
//         let _r = self.tx.send(TaskEvent::Result(t, stati)).await;
//         let _r = self.tx.send(TaskEvent::Finished(pubid)).await;

//         Ok(())
//     }
// }

// #[derive(Debug)]
// struct RestSessions {
//     name: String,
//     sessions: Option<Box<Sessions>>,
//     merge_task: Option<JoinHandle<Result<Box<Sessions>, Error>>>,
// }

// impl RestSessions {
//     pub fn new(name: String) -> Self {
//         Self {
//             name,
//             sessions: None,
//             merge_task: None,
//         }
//     }

//     pub async fn launch(
//         &mut self,
//         cfgw: Arc<app::Config>,
//         req_rx: &ReqRecver,
//         pubid: &mut u64,
//     ) -> Result<(), Error> {
//         let cfg = &cfgw.raw().rest_pubs;

//         let connections = 1u64;
//         let mut ss = Box::new(Sessions::default());
//         ss.customize(&self.name, connections);

//         if cfg.packets == 0 || cfgw.env().rest_api.url.is_empty() {
//             self.sessions = Some(ss);
//             return Ok(());
//         }

//         let (ev_tx, mut ev_rx) = mpsc::channel(10240);
//         let pacer = tq3::limit::Pacer::new(connections);
//         while ss.num_tasks < connections {
//             if let Some(d) = pacer.get_sleep_duration(ss.num_tasks) {
//                 if ss.num_readys < ss.num_tasks {
//                     if ss.try_recv_event(&mut ev_rx).await? {
//                         continue;
//                     }
//                 }
//                 tokio::time::sleep(d).await;
//             }

//             // let acc = accounts.next().unwrap();
//             let cfg0 = cfgw.clone();
//             let tx0 = ev_tx.clone();
//             let rx0 = req_rx.clone();
//             let n = *pubid;
//             let mut session = RestSession::new(cfg0, tx0, rx0);
//             let f = async move {
//                 let r = session.task_entry(n).await;
//                 if let Err(e) = r {
//                     debug!("rest: task finished error [{:?}]", e);
//                     let _r = session.tx.send(TaskEvent::Error(e)).await;
//                 }
//             };
//             // debug!("rest future size {}", std::mem::size_of_val(&f));
//             let span = tracing::span!(tracing::Level::INFO, "", s = n);
//             tokio::spawn(tracing::Instrument::instrument(f, span));
//             *pubid += 1;
//             ss.num_tasks += 1;
//         }

//         ss.wait_for_ready(&mut ev_rx).await?;

//         self.merge_task = Some(ss.launch(ev_rx).await?);

//         Ok(())
//     }

//     pub async fn wait_for_finished(&mut self) -> Result<(), Error> {
//         if self.merge_task.is_some() {
//             let ss = self.merge_task.as_mut().unwrap().await??;
//             self.merge_task.take();
//             self.sessions = Some(ss);
//         }
//         Ok(())
//     }
// }

// #[derive(Debug)]
// struct SubSessions {
//     name: String,
//     sessions: Option<Box<Sessions>>,
//     merge_task: Option<JoinHandle<Result<Box<Sessions>, Error>>>,
// }

// impl SubSessions {
//     pub fn new(name: String) -> Self {
//         Self {
//             name,
//             sessions: None,
//             merge_task: None,
//         }
//     }

//     pub async fn launch(
//         &mut self,
//         cfgw: Arc<app::Config>,
//         req_rx: &ReqRecver,
//         accounts: &mut app::AccountIter<'_>,
//     ) -> Result<(), Error> {
//         let cfg = &cfgw.raw().subs;

//         let mut ss = Box::new(Sessions::default());
//         ss.customize(&self.name, cfg.connections);

//         if cfg.connections == 0 {
//             self.sessions = Some(ss);
//             return Ok(());
//         }

//         let (ev_tx, mut ev_rx) = mpsc::channel(10240);
//         let pacer = tq3::limit::Pacer::new(cfg.conn_per_sec);
//         while ss.num_tasks < cfg.connections {
//             if let Some(d) = pacer.get_sleep_duration(ss.num_tasks) {
//                 if ss.num_readys < ss.num_tasks {
//                     if ss.try_recv_event(&mut ev_rx).await? {
//                         continue;
//                     }
//                 }
//                 tokio::time::sleep(d).await;
//             }

//             let acc = accounts.next().unwrap();
//             let cfg0 = cfgw.clone();
//             let tx0 = ev_tx.clone();
//             let rx0 = req_rx.clone();
//             let n = ss.num_tasks;
//             let f = async move {
//                 let r = sub_task(n, cfg0, acc, &tx0, rx0).await;
//                 if let Err(e) = r {
//                     debug!("sub task finished error [{:?}]", e);
//                     let _r = tx0.send(TaskEvent::Error(e)).await;
//                 }
//             };
//             // debug!("sub future size {}", std::mem::size_of_val(&f));
//             let span = tracing::span!(tracing::Level::INFO, "", s = n);
//             tokio::spawn(tracing::Instrument::instrument(f, span));
//             ss.num_tasks += 1;
//         }

//         ss.wait_for_ready(&mut ev_rx).await?;

//         self.merge_task = Some(ss.launch(ev_rx).await?);

//         Ok(())
//     }

//     pub async fn wait_for_finished(&mut self) -> Result<(), Error> {
//         if self.merge_task.is_some() {
//             let ss = self.merge_task.as_mut().unwrap().await??;
//             self.merge_task.take();
//             self.sessions = Some(ss);
//         }
//         Ok(())
//     }
// }

// #[derive(Debug)]
// struct PubSessions {
//     name: String,
//     sessions: Option<Box<Sessions>>,
//     merge_task: Option<JoinHandle<Result<Box<Sessions>, Error>>>,
// }

// impl PubSessions {
//     pub fn new(name: String) -> Self {
//         Self {
//             name,
//             sessions: None,
//             merge_task: None,
//         }
//     }

//     pub async fn launch(
//         &mut self,
//         cfgw: Arc<app::Config>,
//         req_rx: &ReqRecver,
//         accounts: &mut app::AccountIter<'_>,
//         pubid: &mut u64,
//     ) -> Result<(), Error> {
//         let cfg = &cfgw.raw().pubs;

//         let mut ss = Box::new(Sessions::default());
//         ss.customize(&self.name, cfg.connections);

//         if cfg.connections == 0 {
//             self.sessions = Some(ss);
//             return Ok(());
//         }

//         let (ev_tx, mut ev_rx) = mpsc::channel(10240);
//         let pacer = tq3::limit::Pacer::new(cfg.conn_per_sec);
//         while ss.num_tasks < cfg.connections {
//             if let Some(d) = pacer.get_sleep_duration(ss.num_tasks) {
//                 if ss.num_readys < ss.num_tasks {
//                     if ss.try_recv_event(&mut ev_rx).await? {
//                         continue;
//                     }
//                 }
//                 tokio::time::sleep(d).await;
//             }

//             let acc = accounts.next().unwrap();
//             let cfg0 = cfgw.clone();
//             let tx0 = ev_tx.clone();
//             let rx0 = req_rx.clone();
//             let n = *pubid;
//             let f = async move {
//                 let r = pub_task(n, cfg0, acc, &tx0, rx0).await;
//                 if let Err(e) = r {
//                     debug!("pub task finished error [{:?}]", e);
//                     let _r = tx0.send(TaskEvent::Error(e)).await;
//                 }
//             };
//             // debug!("pub future size {}", std::mem::size_of_val(&f));
//             let span = tracing::span!(tracing::Level::INFO, "", s = n);
//             tokio::spawn(tracing::Instrument::instrument(f, span));
//             *pubid += 1;
//             ss.num_tasks += 1;
//         }

//         ss.wait_for_ready(&mut ev_rx).await?;

//         self.merge_task = Some(ss.launch(ev_rx).await?);

//         Ok(())
//     }

//     pub async fn wait_for_finished(&mut self) -> Result<(), Error> {
//         if self.merge_task.is_some() {
//             let ss = self.merge_task.as_mut().unwrap().await??;
//             self.merge_task.take();
//             self.sessions = Some(ss);
//         }
//         Ok(())
//     }
// }

// #[derive(Debug)]
// struct BenchLatency {
//     pub_sessions: PubSessions,
//     sub_sessions: SubSessions,
//     rest_sessions: RestSessions,
// }

// impl BenchLatency {
//     fn new() -> Self {
//         Self {
//             pub_sessions: PubSessions::new("pubs".to_string()),
//             sub_sessions: SubSessions::new("subs".to_string()),
//             rest_sessions: RestSessions::new("rest".to_string()),
//         }
//     }
//     fn print(&self, cfg: &Arc<app::Config>, kick_time: &Instant) {
//         let pub_sessions = self.pub_sessions.sessions.as_ref().unwrap();
//         let sub_sessions = self.sub_sessions.sessions.as_ref().unwrap();

//         info!(
//             "Pub start time  : {:?}",
//             pub_sessions.work_range.delta_time_range(&kick_time)
//         );
//         info!(
//             "Pub result time: {:?}",
//             pub_sessions.result_range.delta_time_range(&kick_time)
//         );
//         info!(
//             "Sub result time: {:?}",
//             sub_sessions.result_range.delta_time_range(&kick_time)
//         );

//         info!("");
//         info!("Pub connections: {}", cfg.raw().pubs.connections);
//         info!("Pub packets: {} packets/connection", cfg.raw().pubs.packets);
//         tq3::histogram::print_summary("Pub QPS", "qps/connection", &pub_sessions.qps_h);

//         info!("");
//         info!("Sub connections: {}", cfg.raw().subs.connections);
//         info!("Sub recv packets: {}", sub_sessions.stati.packets);
//         info!("Sub lost packets: {}", sub_sessions.stati.lost);
//         tq3::histogram::print_duration("Sub Latency", &sub_sessions.latencyh);
//     }

//     pub async fn bench_priv(
//         &mut self,
//         cfgw: Arc<app::Config>,
//         req_tx: &mut watch::Sender<TaskReq>,
//         req_rx: &mut watch::Receiver<TaskReq>,
//     ) -> Result<(), Error> {
//         let cfg = cfgw.raw();
//         info!("");
//         info!("env: [{}]", cfg.env);
//         info!("address: [{}]", cfgw.env().address);
//         info!("");

//         let mut accounts = app::AccountIter::new(&cfgw.env().accounts);
//         let mut pubid = 0u64;

//         self.sub_sessions
//             .launch(cfgw.clone(), req_rx, &mut accounts)
//             .await?;
//         self.pub_sessions
//             .launch(cfgw.clone(), req_rx, &mut accounts, &mut pubid)
//             .await?;
//         self.rest_sessions
//             .launch(cfgw.clone(), req_rx, &mut pubid)
//             .await?;

//         let kick_time = Instant::now();
//         let is_pub_packets =
//             (cfg.pubs.connections > 0 && cfg.pubs.packets > 0) || cfg.rest_pubs.packets > 0;
//         if is_pub_packets {
//             debug!("-> kick start");

//             let _r = req_tx.send(TaskReq::KickXfer(kick_time));
//             self.rest_sessions.wait_for_finished().await?;

//             self.pub_sessions.wait_for_finished().await?;

//             let r = timeout(Duration::from_millis(cfg.recv_timeout_ms), async {
//                 self.sub_sessions.wait_for_finished().await
//             })
//             .await;

//             match r {
//                 Ok(r0) => {
//                     r0?;
//                 }
//                 Err(_) => {
//                     warn!("waiting for sub result timeout, send stop");
//                     let _r = req_tx.send(TaskReq::Stop);
//                     self.sub_sessions.wait_for_finished().await?;
//                 }
//             }
//         } else {
//             debug!("wait for connections down...");
//             self.rest_sessions.wait_for_finished().await?;
//             self.pub_sessions.wait_for_finished().await?;
//             self.sub_sessions.wait_for_finished().await?;
//         }

//         debug!("<- all done");

//         let elapsed = kick_time.elapsed();

//         info!("");
//         debug!("Elapsed: {:?}", elapsed);

//         self.print(&cfgw, &kick_time);

//         Ok(())
//     }

//     pub async fn bench(&mut self, cfg: Arc<app::Config>) -> Result<(), Error> {
//         let (mut req_tx, mut req_rx) = watch::channel(TaskReq::Ready);
//         let r = self.bench_priv(cfg, &mut req_tx, &mut req_rx).await;
//         // let _r = req_tx.send(TaskReq::Stop);
//         return r;
//     }
// }

impl From<tt::client::Error> for common::Error {
    fn from(error: tt::client::Error) -> Self {
        common::Error::Generic(error.to_string())
    }
}

impl From<reqwest::Error> for common::Error {
    fn from(error: reqwest::Error) -> Self {
        common::Error::Generic(error.to_string())
    }
}

struct Suber {
    cfg: Arc<app::Config>,
    acc: app::Account,
    sender: Option<tt::client::Sender>,
    recver: Option<tt::client::Receiver>,
}

#[async_trait]
impl common::Suber for Suber {
    async fn connect(&mut self) -> Result<(), common::Error> {
        let cfgw = &self.cfg;
        let cfg = cfgw.raw();
        let (mut sender, recver) = tt::client::make_connection("mqtt", &cfgw.env().address)
            .await?
            .split();

        let mut pkt = app::init_conn_pkt(&self.acc, cfgw.raw().subs.protocol);
        pkt.clean_session = cfgw.raw().subs.clean_session;
        pkt.keep_alive = cfgw.raw().subs.keep_alive_secs as u16;
        let ack = sender.connect(pkt).await?;
        if ack.code != tt::ConnectReturnCode::Success {
            return Err(common::Error::Generic(format!("{:?}", ack)));
        }

        let ack = sender
            .subscribe(tt::Subscribe::new(&cfgw.sub_topic(), cfg.subs.qos))
            .await?;
        for reason in &ack.return_codes {
            if !reason.is_success() {
                return Err(common::Error::Generic(format!("{:?}", ack)));
            }
        }

        self.sender = Some(sender);
        self.recver = Some(recver);

        Ok(())
    }

    async fn disconnect(&mut self) -> Result<(), common::Error> {
        if self.sender.is_some() {
            self.sender
                .as_mut()
                .unwrap()
                .disconnect(tt::Disconnect::new())
                .await?;
        }
        Ok(())
    }

    async fn recv(&mut self) -> Result<Bytes, common::Error> {
        let ev = self.recver.as_mut().unwrap().recv().await?;
        let rpkt = match ev {
            tt::client::Event::Packet(pkt) => match pkt {
                tt::Packet::Publish(rpkt) => rpkt,
                _ => return Err(common::Error::Generic(format!("unexpect packet {:?}", pkt))),
            },
            tt::client::Event::Closed(s) => {
                return Err(common::Error::Generic(format!("got closed [{}]", s)));
            }
        };
        Ok(rpkt.payload)
    }
}

struct Puber0 {
    cfg: Arc<app::Config>,
    acc: app::Account,
    sender: Option<tt::client::Sender>,
    recver: Option<tt::client::Receiver>,
}

#[async_trait]
impl common::Puber for Puber0 {
    async fn connect(&mut self) -> Result<(), common::Error> {
        let cfgw = &self.cfg;
        let (mut sender, recver) = tt::client::make_connection("mqtt", &cfgw.env().address)
            .await?
            .split();

        let mut pkt = app::init_conn_pkt(&self.acc, cfgw.raw().pubs.protocol);
        pkt.clean_session = cfgw.raw().pubs.clean_session;
        pkt.keep_alive = cfgw.raw().pubs.keep_alive_secs as u16;
        sender.connect(pkt).await?;

        self.sender = Some(sender);
        self.recver = Some(recver);

        Ok(())
    }

    async fn disconnect(&mut self) -> Result<(), common::Error> {
        if self.sender.is_some() {
            self.sender
                .as_mut()
                .unwrap()
                .disconnect(tt::Disconnect::new())
                .await?;
        }
        Ok(())
    }

    async fn send(&mut self, data: Bytes) -> Result<(), common::Error> {
        let mut pkt = tt::Publish::new(self.cfg.pub_topic(), self.cfg.raw().pubs.qos, []);
        pkt.payload = data;
        let _r = self.sender.as_mut().unwrap().publish(pkt).await?;
        Ok(())
    }

    async fn idle(&mut self) -> Result<(), common::Error> {
        loop {
            let ev = self.recver.as_mut().unwrap().recv().await?;
            if let tt::client::Event::Closed(s) = ev {
                return Err(common::Error::Generic(format!("got closed [{}]", s)));
            }
        }
    }
}

struct RestPuber {
    cfg: Arc<app::Config>,
}

impl RestPuber {
    pub async fn call_rest(cfg: &app::RestApiArg, s: String) -> Result<(), reqwest::Error> {
        let req_body = cfg.make_body(&mut cfg.body.clone(), s);

        {
            let client = reqwest::Client::new();

            let mut builder = client.post(&cfg.url);
            for (k, v) in &cfg.headers {
                builder = builder.header(k, v);
            }

            trace!("request url ={:?}", cfg.url);
            trace!("request body={:?}", req_body);

            let res = builder.body(req_body).send().await?;
            let rsp_status = res.status();
            let rsp_body = res.text().await?;

            trace!("response status: {}", rsp_status);
            trace!("response body  : {}", rsp_body);
        }
        Ok(())
    }
}

#[async_trait]
impl common::Puber for RestPuber {
    async fn connect(&mut self) -> Result<(), common::Error> {
        Ok(())
    }

    async fn disconnect(&mut self) -> Result<(), common::Error> {
        Ok(())
    }

    async fn send(&mut self, data: Bytes) -> Result<(), common::Error> {
        Self::call_rest(&self.cfg.env().rest_api, base64::encode(data)).await?;
        Ok(())
    }

    async fn idle(&mut self) -> Result<(), common::Error> {
        Ok(())
    }
}

pub async fn bench_all(cfgw: Arc<app::Config>) -> Result<(), Error> {
    let mut accounts = app::AccountIter::new(&cfgw.env().accounts);

    let mut launcher = common::Launcher::new();
    let mut sub_id = 10u64;
    let mut pub_id = 11u64;

    if cfgw.raw().subs.connections > 0 {
        let _r = launcher
            .launch_sub_sessions(
                "subs".to_string(),
                &mut sub_id,
                cfgw.raw().subs.connections,
                cfgw.raw().subs.conn_per_sec,
                |_n| {
                    let acc = accounts.next().unwrap();
                    Suber {
                        cfg: cfgw.clone(),
                        acc,
                        sender: None,
                        recver: None,
                    }
                },
            )
            .await?;
    }

    if cfgw.raw().pubs.connections > 0 {
        let args = Arc::new(common::PubArgs {
            qps: cfgw.raw().pubs.qps,
            packets: cfgw.raw().pubs.packets,
            padding_to_size: cfgw.raw().pubs.padding_to_size,
            content: Bytes::copy_from_slice(cfgw.raw().pubs.content.as_bytes()),
        });

        let _r = launcher
            .launch_pub_sessions(
                "pubs".to_string(),
                &mut pub_id,
                cfgw.raw().pubs.connections,
                cfgw.raw().pubs.conn_per_sec,
                args,
                |_n| {
                    let acc = accounts.next().unwrap();
                    Puber0 {
                        cfg: cfgw.clone(),
                        acc,
                        sender: None,
                        recver: None,
                    }
                },
            )
            .await?;
    }

    if cfgw.raw().rest_pubs.packets > 0 {
        let args = Arc::new(common::PubArgs {
            qps: cfgw.raw().rest_pubs.qps,
            packets: cfgw.raw().rest_pubs.packets,
            padding_to_size: cfgw.raw().rest_pubs.padding_to_size,
            content: Bytes::new(),
        });

        let _r = launcher
            .launch_pub_sessions("rests".to_string(), &mut pub_id, 1, 1, args, |_n| {
                RestPuber { cfg: cfgw.clone() }
            })
            .await?;
    }

    let cfg = cfgw.raw();
    launcher.kick_and_wait(cfg.recv_timeout_ms).await?;

    Ok(())
}

pub async fn run(config_file: &str) {
    let cfg = app::Config::load_from_file(&config_file);
    trace!("cfg=[{:#?}]", cfg.raw());
    let cfg = Arc::new(cfg);

    match bench_all(cfg.clone()).await {
        Ok(_) => {}
        Err(e) => {
            error!("bench result error [{}]", e);
        }
    }

    // let mut bencher = BenchLatency::new();
    // match bencher.bench(cfg).await {
    //     Ok(_) => {
    //         // info!("bench result ok");
    //     }
    //     Err(e) => {
    //         error!("bench result error [{}]", e);
    //     }
    // }
}
