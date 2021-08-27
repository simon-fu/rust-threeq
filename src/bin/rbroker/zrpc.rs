// TODO:
// - rpc timeout
// - client.call type safe, only allow the message belone to the specifc service
// - client.call return future
// - client state machine, retry
// done - client request close request to server
//

// #[macro_use]

use super::zserver;
use async_trait::async_trait;
use bytes::Buf;
use bytes::BufMut;
use bytes::Bytes;
use bytes::BytesMut;
use paste::paste;
use prost::Message;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::RwLock;
use std::time::Duration;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::net;
use tokio::net::tcp::WriteHalf;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::time::Instant;
use tracing::debug;
use tracing::error;

mod rpc {
    include!(concat!(env!("OUT_DIR"), "/zrpc.rs"));
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("{0}")]
    DecodeError(#[from] prost::DecodeError),

    #[error("{0}")]
    EncodeError(#[from] prost::EncodeError),

    #[error("{0}")]
    RecvError(#[from] oneshot::error::RecvError),

    #[error("{0}")]
    IoError(#[from] std::io::Error),

    #[error("error: {0}")]
    Generic(String),
}

const MAGIC: &str = "zrpc";

pub const MESSAGE_ID_BASE: i32 = 100;

pub trait Id32 {
    fn id(&self) -> i32;
}

#[macro_export]
macro_rules! define_msgs_ {
        ($val:expr,) => {

        };
        ($val:expr, $id:ident, $($ids:ident),*$(,)?) => {
            paste! {
                const [<$id:upper _ID>]: i32 = $val;

                impl Id32 for $id {
                    fn id(&self) -> i32 {
                        [<$id:upper _ID>]
                    }
                }
            }

            define_msgs_!($val + 1, $($ids,)*);
        };
    }
// define_msgs_!(MESSAGE_ID_BASE, NodeSyncRequest, NodeSyncReply);

#[macro_export]
macro_rules! define_msgs {
        ($($ids:ident),*$(,)?) => {
            define_msgs_!(MESSAGE_ID_BASE, $($ids,)*);
        };
    }

use rpc::ByeReply;
use rpc::ByeRequest;
use rpc::HelloReply;
use rpc::HelloRequest;
use rpc::PingRequest;
use rpc::PongReply;
define_msgs_!(
    1,
    HelloRequest,
    HelloReply,
    PingRequest,
    PongReply,
    ByeRequest,
    ByeReply
);

#[inline]
fn is_internal_packet(ptype: i32) -> bool {
    ptype >= 0 && ptype < MESSAGE_ID_BASE
}

pub fn decode<M, B>(buf: &mut B) -> Result<M, String>
where
    M: Message + Default,
    B: Buf,
{
    match M::decode(buf) {
        Ok(m) => Ok(m),
        Err(e) => Err(e.to_string()),
    }
}

pub fn reply_result<M>(ptype: i32, msg: M) -> Result<(i32, Bytes), String>
where
    M: Message,
{
    let mut obuf = BytesMut::new();
    msg.encode(&mut obuf).unwrap();
    return Ok((ptype, obuf.freeze()));
}

#[inline]
fn decode_seq<B>(buf: &mut B) -> Result<u16, Error>
where
    B: Buf,
{
    if buf.remaining() < 2 {
        return Err(Error::Generic("decode seq: too short".to_string()));
    }
    let seq = buf.get_u16();
    Ok(seq)
}

// fn decode_msg<M, B>(buf: &mut B) -> Result<(u16, M), Error>
// where
// M: Message + Default,
// B: Buf,
// {
//     let seq = decode_seq(buf)?;
//     let msg = M::decode(buf)?;
//     Ok( (seq, msg) )
// }

fn encode_msg<M, B>(seq: u16, msg: &M, obuf: &mut B) -> Result<(), Error>
where
    M: Message + Id32,
    B: BufMut,
{
    zserver::encode_header(msg.id(), msg.encoded_len() + 2, obuf);
    obuf.put_u16(seq);
    msg.encode(obuf)?;
    Ok(())
}

fn encode_buf<M, B>(ptype: i32, seq: u16, msg: &mut M, obuf: &mut B)
where
    M: Buf,
    B: BufMut,
{
    zserver::encode_header(ptype, msg.remaining() + 2, obuf);
    obuf.put_u16(seq);
    obuf.put(msg);
}

pub struct Session {
    services: Services,
    service: Option<Arc<dyn Service>>,
}

impl Session {
    async fn process_packet(
        &mut self,
        ptype: i32,
        mut bytes: bytes::Bytes,
        obuf: &mut bytes::BytesMut,
    ) -> Result<zserver::Action, Error> {
        let seq = decode_seq(&mut bytes)?;
        if !is_internal_packet(ptype) && self.service.is_some() {
            let r = self
                .service
                .as_ref()
                .unwrap()
                .handle_request(ptype, bytes)
                .await;
            match r {
                Ok((ptype, mut bytes)) => {
                    encode_buf(ptype, seq, &mut bytes, obuf);
                    debug!("server: => seq {}, ptype {:?}", seq, ptype);
                    return Ok(zserver::Action::None);
                }
                Err(e) => {
                    return Err(Error::Generic(e));
                }
            }
        }

        // let packet_type = rpc::MessageType::from_i32(ptype).unwrap();
        match ptype {
            HELLOREQUEST_ID => {
                //rpc::MessageType::HelloReq => {
                let pkt = rpc::HelloRequest::decode(&mut bytes)?;
                debug!("server: <= seq {}, {:?}", seq, pkt);
                if pkt.magic != MAGIC {
                    return Err(Error::Generic(format!("unexpect magic {}", pkt.magic)));
                }

                let mut reply = rpc::HelloReply::default();
                reply.magic = MAGIC.to_string();
                {
                    let services = self.services.read().unwrap();
                    if let Some(s) = services.get(&pkt.service_type_name) {
                        self.service = Some(s.clone());
                        reply.code = 0;
                    } else {
                        reply.code = rpc::ErrorType::NotFoundService as i32;
                        reply.msg = format!("Not found service {}", pkt.service_type_name);
                        debug!("{}", reply.msg);
                    }
                }

                encode_msg(seq, &reply, obuf)?;
                debug!("server: => seq {}, {:?}", seq, reply);
            }
            PINGREQUEST_ID => {
                // rpc::MessageType::Ping => {
                let pkt = rpc::PingRequest::decode(&mut bytes)?;
                debug!("server: <= seq {}, {:?}", seq, pkt);
                let reply = rpc::PongReply::default();
                encode_msg(seq, &reply, obuf)?;
                debug!("server: => seq {}, {:?}", seq, reply);
            }
            BYEREQUEST_ID => {
                // rpc::MessageType::ByeReq => {
                let pkt = rpc::ByeRequest::decode(&mut bytes)?;
                debug!("server: <= seq {}, {:?}", seq, pkt);
                if self.service.is_some() {
                    self.service
                        .as_ref()
                        .unwrap()
                        .handle_bye(pkt.code, &pkt.msg)
                        .await;
                }

                if seq > 0 {
                    let reply = rpc::ByeReply::default();
                    encode_msg(seq, &reply, obuf)?;
                    debug!("server: => seq {}, {:?}", seq, reply);
                } else {
                    return Ok(zserver::Action::Close);
                }
            }
            _ => {
                debug!("server: <= ptype {:?}", ptype);
                return Err(Error::Generic(format!("unexpect packet {:?}", ptype)));
            }
        }
        Ok(zserver::Action::None)
    }
}

#[async_trait]
impl zserver::Session for Session {
    async fn handle_packet(
        &mut self,
        ptype: i32,
        bytes: bytes::Bytes,
        obuf: &mut bytes::BytesMut,
    ) -> Result<zserver::Action, String> {
        match self.process_packet(ptype, bytes, obuf).await {
            Ok(a) => {
                return Ok(a);
            }
            Err(e) => {
                return Err(e.to_string());
            }
        }
    }

    fn handle_final_error(&self, e: std::io::Error) {
        if self.service.is_some() {
            self.service.as_ref().unwrap().session_finish_with_error(e);
        } else {
            debug!("session error with {:?}", e);
        }
    }
}

type Services = Arc<RwLock<HashMap<String, Arc<dyn Service>>>>;

pub struct Server {
    services: Services,
    server: zserver::Server<Session, Factory>,
}

impl Server {
    fn new() -> Self {
        Self {
            services: Services::default(),
            server: zserver::Server::builder(),
        }
    }

    pub fn builder() -> Self {
        Self::new()
    }

    pub fn add_service(self, svc: Arc<dyn Service>) -> Self {
        {
            let mut services = self.services.write().unwrap();
            services.insert(svc.type_name().to_string(), svc);
        }
        self
    }

    pub fn build(self) -> zserver::Server<Session, Factory> {
        let f = Factory::new(self.services.clone());
        self.server.factory(f).build()
    }
}

pub struct Factory {
    services: Services,
}

impl zserver::SessionFactory<Session> for Factory {
    fn make_session(
        &self,
        _socket: &tokio::net::TcpStream,
        _addr: &std::net::SocketAddr,
    ) -> Session {
        Session {
            service: None,
            services: self.services.clone(),
        }
    }
}

#[async_trait]
pub trait Service: Send + Sync {
    fn type_name(&self) -> &'static str;

    async fn handle_request(&self, ptype: i32, bytes: Bytes) -> Result<(i32, Bytes), String>;

    fn session_finish_with_error(&self, e: std::io::Error) {
        debug!("session finish with {:?}", e);
    }

    async fn handle_bye(&self, code: i32, msg: &str) {
        debug!("session bye with [{:?}]-[{}]", code, msg);
    }
}

impl Factory {
    fn new(services: Services) -> Self {
        Self { services }
    }
}

// use rust_fsm::*;
// state_machine! {
//     derive(Debug)
//     ClientState(Ready)

//     Ready => {
//         ConnectReq => Connecting ,
//     },
//     Connecting => {
//         ConnReplyOk => Working,
//         ConnReplyFail => Closed,
//         ConnDown => Wait4Retry [SetupRetryTimer],
//         CloseReq => Closed ,
//     },
//     Working => {
//         ConnDown => Wait4Retry [SetupRetryTimer],
//         CloseReq => Closing [SayBye],
//     },
//     Wait4Retry => {
//         RetryTimerTriggered => Connecting [MakeConnect],
//         CloseReq => Closed,
//     },
//     Closing => {
//         ByeReplyOk => Closed,
//         ConnDown => Closed,
//     },
//     Closed => {
//         CloseReq => Closed
//     },
// }

enum Response {
    Reply((u16, Bytes)),
}
type ResponseTX = oneshot::Sender<Response>;
// type ResponseRX = oneshot::Receiver<Response>;

struct Request {
    ptype: i32,
    bytes: Bytes,
    tx: Option<ResponseTX>,
}

struct Pending {
    tx: Option<ResponseTX>,
}

enum Command {
    Call(Request),
    Shutdown(oneshot::Sender<()>),
    Close(oneshot::Sender<()>),
}

struct ClientWork {
    cfg: Config,
    rx: mpsc::Receiver<Command>,
    watcher: Option<Box<dyn ClientWatcher>>,
    ibuf: BytesMut,
    obuf: BytesMut,
    inflight: HashMap<u16, Pending>,
    seq: u16,
    next_ping_time: Instant,
    pingable: bool,
    finished: bool,
}

impl ClientWork {
    fn new(rx: mpsc::Receiver<Command>) -> Self {
        Self {
            cfg: Config::default(),
            rx,
            watcher: None,
            ibuf: BytesMut::new(),
            obuf: BytesMut::new(),
            inflight: HashMap::new(),
            seq: 0,
            next_ping_time: Instant::now(),
            pingable: false,
            finished: false,
        }
    }

    fn next_seq(&mut self) -> u16 {
        if self.seq == u16::MAX {
            self.seq = 1;
        } else {
            self.seq += 1;
        }
        self.seq
    }

    fn check_ping(&mut self, now: Instant) -> Result<bool, Error> {
        if self.pingable && now >= self.next_ping_time {
            let pkt = rpc::PingRequest::default();
            let seq = self.next_seq();
            encode_msg(seq, &pkt, &mut self.obuf)?;
            debug!("client: => seq {}, {:?}", seq, pkt);
            self.pingable = false;
            return Ok(true);
        } else {
            return Ok(false);
        }
    }

    fn calc_next_ping_time(&mut self) {
        self.pingable = true;
        self.next_ping_time = Instant::now() + Duration::from_secs(self.cfg.keep_alive as u64);
    }

    fn next_round(&mut self, now: Instant) -> Result<(bool, bool, Instant), Error> {
        // let hold_req = self.state == State::Disconnecting || obuf.len() >= self.max_osize;
        let hold_req = false;
        let hold_read = self.obuf.len() >= self.cfg.max_osize;

        let mut next_check_time = now + Duration::from_secs(999999);
        if !hold_read && self.pingable && next_check_time > self.next_ping_time {
            next_check_time = self.next_ping_time;
        }

        Ok((hold_req, hold_read, next_check_time))
    }

    async fn handle_packet(&mut self, ptype: i32, mut bytes: Bytes) -> Result<(), Error> {
        let seq = decode_seq(&mut bytes)?;

        let req = self.inflight.remove(&seq);
        if let Some(req) = req {
            if let Some(tx) = req.tx {
                let _r = tx.send(Response::Reply((seq, bytes)));
            }

            if seq == 1 {
                self.calc_next_ping_time();
            }
        } else {
            if is_internal_packet(ptype) {
                // let packet_type = rpc::MessageType::from_i32(ptype).unwrap();
                match ptype {
                    PONGREPLY_ID => {
                        // rpc::MessageType::Pong => {
                        self.calc_next_ping_time();
                        let pkt = rpc::PongReply::decode(&mut bytes)?;
                        debug!("client: <= seq {}, {:?}", seq, pkt);
                    }
                    BYEREPLY_ID => {
                        // rpc::MessageType::ByeRly => {
                        let pkt = rpc::ByeReply::decode(&mut bytes)?;
                        debug!("client: <= seq {}, {:?}", seq, pkt);
                    }
                    _ => {
                        debug!("client: <= seq {}, ptype {}", seq, ptype);
                        return Err(Error::Generic(format!("unexpect packet {:?}", ptype)));
                    }
                }
            }
        }
        Ok(())
    }

    async fn handle_command(&mut self, cmd: Command, wr: &mut WriteHalf<'_>) -> Result<(), Error> {
        match cmd {
            Command::Call(mut r) => {
                let seq = self.next_seq();
                encode_buf(r.ptype, seq, &mut r.bytes, &mut self.obuf);
                let pending = Pending { tx: r.tx };
                debug!("client: => seq {}, ptype {}", seq, r.ptype);
                self.inflight.insert(seq, pending);
            }
            Command::Shutdown(tx) => {
                self.finished = true;
                let _r = tx.send(());
            }
            Command::Close(tx) => {
                let pkt = rpc::ByeRequest::default();
                encode_msg(0, &pkt, &mut self.obuf)?;
                wr.write_all_buf(&mut self.obuf).await?;
                let _r = tx.send(());
            }
        }
        Ok(())
    }

    async fn decode_packets(&mut self) -> Result<(), Error> {
        loop {
            let r = zserver::decode_packet(&mut self.ibuf, usize::MAX)?;
            if let Some((ptype, bytes)) = r {
                self.handle_packet(ptype, bytes).await?;
            } else {
                return Ok(());
            }
        }
    }

    async fn fire_broken(&mut self, e: &Error) {
        if let Some(w) = &mut self.watcher {
            w.on_disconnect(Reason::Broken, e).await;
        }
    }

    async fn run(&mut self, mut socket: TcpStream) -> Result<(), Error> {
        self.cfg.check();

        let (mut rd, mut wr) = socket.split();
        while !self.finished {
            let now = Instant::now();
            let (hold_req, hold_read, next_check_time) = self.next_round(now)?;

            if self.check_ping(now)? {
                continue;
            }

            tokio::select! {
                r = wr.write_buf(&mut self.obuf), if !self.obuf.is_empty() => {
                    r?;
                }

                r = rd.read_buf(&mut self.ibuf), if !hold_read=> {
                    let (disconnect, r) = match r {
                        Ok(n) => {
                            if n == 0 {
                                let e = std::io::Error::new(std::io::ErrorKind::BrokenPipe, "detect disconnect by server");
                                (true, Err(Error::IoError(e)))
                            } else {
                                ( false, Ok(()) )
                            }

                        },
                        Err(e) => {
                            (true, Err(Error::IoError(e)))
                        },
                    };
                    if disconnect {
                        self.fire_broken(r.as_ref().unwrap_err()).await;
                        return r;
                    }

                    self.decode_packets().await?;
                }


                // r = rd.read_buf(&mut ibuf), if !hold_read=> {
                //     let len = match r {
                //         Ok(n) => n,
                //         Err(e) => {
                //             debug!("read socket fail, {:?}", e);
                //             return Err(Error::Io(e))
                //         },
                //     };
                //     if len == 0 {
                //         if session.state == State::Disconnecting {
                //             // gracefully disconnect
                //             session.response_disconnect("active disconnect").await?;
                //             break;
                //         } else {
                //             return Err(Error::Broken("read disconnected".to_string()));
                //         }
                //     }
                //     session.handle_incoming(&mut ibuf, &mut obuf).await?
                // }

                r = self.rx.recv(), if !hold_req => {
                    match r{
                        Some(cmd)=>{
                            self.handle_command(cmd, &mut wr).await?
                        }
                        None => {
                            //debug!("no sender, closed");
                            break;
                        },
                    }
                }

                _ = tokio::time::sleep_until(next_check_time) => {

                }
            }
        }
        Ok(())
    }
}

#[derive(Default, Clone, Debug)]
struct Config {
    service_type: String,
    keep_alive: u32,
    max_osize: usize,
}

impl Config {
    fn check(&mut self) {
        if self.keep_alive == 0 {
            self.keep_alive = 30;
        }

        if self.max_osize == 0 {
            self.max_osize = 64 * 1024;
        }
    }
}

#[derive(Debug)]
pub enum Reason {
    Broken,
}

#[async_trait]
pub trait ClientWatcher: Send {
    async fn on_disconnect(&mut self, reason: Reason, detail: &Error);
}

pub struct Client {
    work: Option<Box<ClientWork>>,
    tx: mpsc::Sender<Command>,
}

impl Client {
    pub fn builder() -> Self {
        let (tx, rx) = mpsc::channel(32);
        Self {
            work: Some(Box::new(ClientWork::new(rx))),
            tx: tx,
        }
    }

    pub fn service_type(mut self, stype: &str) -> Self {
        self.work.as_mut().unwrap().cfg.service_type = stype.to_string();
        self
    }

    pub fn keep_alive(mut self, keep_alive: u32) -> Self {
        self.work.as_mut().unwrap().cfg.keep_alive = keep_alive;
        self
    }

    pub fn watcher(mut self, watcher: Box<dyn ClientWatcher>) -> Self {
        self.work.as_mut().unwrap().watcher = Some(watcher);
        self
    }

    pub fn build(self) -> Self {
        self
    }

    pub async fn connect<A: net::ToSocketAddrs>(&mut self, addr: A) -> Result<(), Error> {
        let socket = TcpStream::connect(&addr).await?;

        let mut work = self.work.take().unwrap();

        let mut hello = rpc::HelloRequest::default();
        hello.magic = MAGIC.to_string();
        hello.service_type_name = work.cfg.service_type.clone();
        hello.keep_alive = work.cfg.keep_alive;

        tokio::spawn(async move {
            let r = work.run(socket).await;
            match r {
                Ok(_r) => {
                    debug!("work finished");
                }
                Err(e) => {
                    debug!("work finished with {:?}", e);
                }
            }
        });

        let reply: rpc::HelloReply = self.call(hello).await?;
        if reply.magic != MAGIC {
            return Err(Error::Generic(format!("unexpect magic {}", reply.magic)));
        }

        if reply.code != 0 {
            self.shutdown().await?;
            return Err(Error::Generic(format!(
                "reply code [{}], error [{}]",
                reply.code, reply.msg
            )));
        }

        Ok(())
    }

    pub async fn shutdown(&mut self) -> Result<(), Error> {
        let (tx, rx) = oneshot::channel();
        let r = self.tx.send(Command::Shutdown(tx)).await;
        if r.is_ok() {
            rx.await?;
        }
        Ok(())
    }

    pub async fn close(&mut self) {
        let (tx, rx) = oneshot::channel();
        let r = self.tx.send(Command::Close(tx)).await;
        if r.is_ok() {
            let _r = rx.await;
        }
    }

    pub async fn call<M1, M2>(&mut self, msg: M1) -> Result<M2, Error>
    where
        M1: Id32 + Message,
        M2: Id32 + Message + Default,
    {
        let mut obuf = BytesMut::new();
        msg.encode(&mut obuf)?;
        let (tx, rx) = oneshot::channel();
        let req = Request {
            ptype: msg.id(),
            bytes: obuf.freeze(),
            tx: Some(tx),
        };

        debug!("client: => seq {}, ptype {}, {:?}", 0, msg.id(), msg);
        let _r = self.tx.send(Command::Call(req)).await;

        let rsp = rx.await?;
        match rsp {
            Response::Reply(mut r) => {
                let reply = M2::decode(&mut r.1)?;
                debug!("client: <= seq {}, {:?}", r.0, reply);
                Ok(reply)
            }
        }
    }
}
