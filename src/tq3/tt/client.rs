/*
TODO:
√ connect
√ gracefully disconnect
√ subscribe
√ send publish
√ recv publish
√ auto ping
- in-flight window / max pending out buf size
- auto reconnect

Scenario:
- send connect sync: response connack
- send connect async: notify connack
- send publish async:  auto send pubrel, notify puback/pubcomp
- send subscribe sync: response suback
- send subscribe async: notify suback
- send unsubscribe sync: response unsuback
- send unsubscribe async： notify unsuback
- send disconnect async: notify closed
- send disconnect sync: response closed
- recv publish : notfiy publish, auto send puback/pubrec/pubcomp

Request:
- packet: connect/publish/subscribe/unsubscribe/disconnect

Response:
- packet: connack/suback/unsuback
- gracefully closed:

Events:
- packet: connack/publish/puback/pubrec/pubcomp/suback/unsuback/disconnect
- gracefully closed:
- error:

*/

use crate::tq3::tt;
use bytes::{Bytes, BytesMut};
use core::panic;
use std::{
    collections::HashMap,
    convert::TryFrom,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::{mpsc, oneshot},
    time::{error::Elapsed, Instant},
};
use tracing::{debug, error, trace, Instrument};

macro_rules! trace_input {
    ($a:expr) => {{
        trace!("recv <-: {:?}", $a)
    }};
}

macro_rules! trace_output {
    ($a:expr) => {{
        trace!("send ->: {:?}", $a)
    }};
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Timeout:{0}")]
    Timeout(#[from] Elapsed),

    #[error("Packet parsing error: {0:?}")]
    Tt(#[from] tt::Error),

    #[error("packet type error: {0}")]
    PackettypeError(#[from] num_enum::TryFromPrimitiveError<tt::PacketType>),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("No receiver")]
    NoReceiver,

    #[error("Broken: {0}")]
    Broken(String),

    #[error("client already finished")]
    Finished,

    #[error("State error: {0}")]
    State(String),

    #[error("error: {0}")]
    Generic(String),
}

#[derive(Debug)]
pub enum Event {
    Packet(tt::Packet),
    Closed(String),
}

#[derive(Debug)]
enum Response {
    Connected,
    Event(Event),
    Error(Error),
}
type ResponseTX = oneshot::Sender<Response>;
type ResponseRX = oneshot::Receiver<Response>;

async fn recv_rsp(rx: &mut ResponseRX) -> Result<Response, Error> {
    let r = rx.await;
    match r {
        Ok(rsp) => Ok(rsp),
        Err(_) => Err(Error::Finished),
    }
}

async fn recv_ev(rx: &mut ResponseRX) -> Result<Event, Error> {
    match recv_rsp(rx).await? {
        Response::Event(ev) => Ok(ev),
        Response::Error(e) => Err(e),
        Response::Connected => panic!("never reach heare"),
    }
}

async fn recv_pkt(rx: &mut ResponseRX) -> Result<tt::Packet, Error> {
    match recv_ev(rx).await? {
        Event::Packet(pkt) => Ok(pkt),
        Event::Closed(_) => Err(Error::Generic("expect packet but closed".to_string())),
    }
}

async fn recv_closed(rx: &mut ResponseRX) -> Result<(), Error> {
    match recv_ev(rx).await? {
        Event::Packet(_) => Err(Error::Generic("expect closed but packet".to_string())),
        Event::Closed(_) => Ok(()),
    }
}

#[derive(Debug)]
enum ReqItem {
    Packet(tt::Packet),
    Shutdown,
}

impl ReqItem {
    fn ref_pkt(&self) -> Result<&tt::Packet, Error> {
        match self {
            ReqItem::Packet(pkt) => Ok(&pkt),
            ReqItem::Shutdown => Err(Error::Generic("Not packet req".to_string())),
        }
    }

    fn unwrap_pkt(self) -> Result<tt::Packet, Error> {
        match self {
            ReqItem::Packet(pkt) => Ok(pkt),
            ReqItem::Shutdown => Err(Error::Generic("Not packet req".to_string())),
        }
    }
}

#[derive(Debug)]
struct Request {
    req: ReqItem,
    tx: Option<ResponseTX>,
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
enum State {
    Ready,
    Connecting,
    Working,
    Disconnecting,
    Closed,
}

impl Default for State {
    fn default() -> Self {
        State::Ready
    }
}

#[derive(Debug)]
struct Session {
    max_osize: usize,
    tx: mpsc::Sender<Response>,
    state: State,
    pktid: tt::PacketId,
    max_incoming_size: usize,
    conn_pkt: Option<tt::Connect>,
    inflight: HashMap<u16, Request>,
    qos2_rsp: HashMap<u16, tt::Packet>,
    last_active_time: Instant,
}

impl Session {
    fn new(tx: mpsc::Sender<Response>, now: Instant) -> Self {
        Session {
            max_osize: 16 * 1024,
            tx,
            state: State::Ready,
            pktid: tt::PacketId::default(),
            max_incoming_size: 64 * 1024,
            conn_pkt: None,
            inflight: Default::default(),
            qos2_rsp: Default::default(),
            last_active_time: now,
        }
    }

    fn get_protocol(&self) -> tt::Protocol {
        self.conn_pkt.as_ref().unwrap().protocol
    }

    fn check_state<S: Into<String>>(&self, expect: State, origin: S) -> Result<(), Error> {
        if expect == self.state {
            Ok(())
        } else {
            Err(Error::State(format!(
                "expect {:?} but {:?}, origin {}",
                expect,
                self.state,
                origin.into()
            )))
        }
    }

    fn check_working_state<S: Into<String>>(&self, origin: S) -> Result<(), Error> {
        if State::Working == self.state || State::Disconnecting == self.state {
            Ok(())
        } else {
            Err(Error::State(format!(
                "expect working but {:?}, origin {}",
                self.state,
                origin.into()
            )))
        }
    }

    async fn rsp_packet(&mut self, tx: Option<ResponseTX>, pkt: tt::Packet) -> Result<(), Error> {
        self.rsp_event(tx, Event::Packet(pkt)).await
    }

    async fn rsp_error(&mut self, tx: Option<ResponseTX>, e: Error) -> Result<(), Error> {
        self.rsp_rsp(tx, Response::Error(e)).await
    }

    async fn rsp_event(&mut self, tx: Option<ResponseTX>, ev: Event) -> Result<(), Error> {
        self.rsp_rsp(tx, Response::Event(ev)).await
    }

    async fn rsp_rsp(&mut self, tx: Option<ResponseTX>, rsp: Response) -> Result<(), Error> {
        if let Some(tx0) = tx {
            let _ = tx0.send(rsp);
            return Ok(());
        }

        if let Err(_) = self.tx.send(rsp).await {
            Err(Error::NoReceiver)
        } else {
            Ok(())
        }
    }

    async fn handle_connack(
        &mut self,
        fixed_header: tt::FixedHeader,
        bytes: Bytes,
        _obuf: &mut BytesMut,
    ) -> Result<(), Error> {
        self.check_state(State::Connecting, "handle_connack")?;

        let pkt = tt::ConnAck::decode(self.get_protocol(), fixed_header, bytes)?;
        trace_input!(pkt);

        self.state = State::Working;

        let req = self
            .inflight
            .remove(&0)
            .ok_or_else(|| Error::Generic("inflight unexpect connack".to_string()))?;

        if let tt::Packet::Connect(_) = req.req.ref_pkt()? {
            if pkt.properties.is_some() {
                if pkt.properties.as_ref().unwrap().server_keep_alive.is_some() {
                    let conn = self.conn_pkt.as_mut().unwrap();
                    conn.keep_alive = *pkt
                        .properties
                        .as_ref()
                        .unwrap()
                        .server_keep_alive
                        .as_ref()
                        .unwrap();
                }
            }
            self.rsp_packet(req.tx, tt::Packet::ConnAck(pkt)).await
        } else {
            Err(Error::Generic(format!("{:?} unexpect connack", pkt)))
        }
    }

    async fn handle_publish(
        &mut self,
        fixed_header: tt::FixedHeader,
        bytes: Bytes,
        obuf: &mut BytesMut,
    ) -> Result<(), Error> {
        self.check_working_state("handle_publish")?;

        let pkt = tt::Publish::decode(self.get_protocol(), fixed_header, bytes)?;
        trace_input!(pkt);

        let req = self.qos2_rsp.get(&pkt.pkid);
        if req.is_some() {
            return Err(Error::Generic(format!("{:?} unexpect publish", req)));
        }

        match pkt.qos {
            tt::QoS::AtMostOnce => {}

            tt::QoS::AtLeastOnce => {
                let ack = tt::PubAck::new(pkt.pkid);
                ack.encode(self.get_protocol(), obuf)?;
                trace_output!(ack);
            }

            tt::QoS::ExactlyOnce => {
                let ack = tt::PubRec::new(pkt.pkid);
                ack.encode(self.get_protocol(), obuf)?;
                trace_output!(ack);
                self.qos2_rsp.insert(pkt.pkid, tt::Packet::PubRec(ack));
            }
        }

        return self.rsp_packet(None, tt::Packet::Publish(pkt)).await;
    }

    async fn handle_suback(
        &mut self,
        fixed_header: tt::FixedHeader,
        bytes: Bytes,
        _obuf: &mut BytesMut,
    ) -> Result<(), Error> {
        self.check_working_state("handle_suback")?;

        let pkt = tt::SubAck::decode(self.get_protocol(), fixed_header, bytes)?;
        trace_input!(pkt);

        let req = self
            .inflight
            .remove(&pkt.pkid)
            .ok_or_else(|| Error::Generic("inflight unexpect subnack".to_string()))?;

        if let tt::Packet::Subscribe(_) = req.req.ref_pkt()? {
            self.rsp_packet(req.tx, tt::Packet::SubAck(pkt)).await
        } else {
            Err(Error::Generic(format!("{:?} unexpect suback", pkt)))
        }
    }

    async fn handle_unsuback(
        &mut self,
        fixed_header: tt::FixedHeader,
        bytes: Bytes,
        _obuf: &mut BytesMut,
    ) -> Result<(), Error> {
        self.check_working_state("handle_unsuback")?;

        let pkt = tt::UnsubAck::decode(self.get_protocol(), fixed_header, bytes)?;
        trace_input!(pkt);

        let req = self
            .inflight
            .remove(&pkt.pkid)
            .ok_or_else(|| Error::Generic("inflight unexpect unsubnack".to_string()))?;

        if let tt::Packet::Unsubscribe(_) = req.req.ref_pkt()? {
            self.rsp_packet(req.tx, tt::Packet::UnsubAck(pkt)).await
        } else {
            Err(Error::Generic(format!("{:?} unexpect unsubnack", pkt)))
        }
    }

    async fn handle_puback(
        &mut self,
        fixed_header: tt::FixedHeader,
        bytes: Bytes,
        _obuf: &mut BytesMut,
    ) -> Result<(), Error> {
        self.check_working_state("handle_puback")?;

        let pkt = tt::PubAck::decode(self.get_protocol(), fixed_header, bytes)?;
        trace_input!(pkt);

        let req = self
            .inflight
            .remove(&pkt.pkid)
            .ok_or_else(|| Error::Generic("inflight unexpect puback".to_string()))?;

        if let tt::Packet::Publish(rpkt) = req.req.ref_pkt()? {
            if rpkt.qos == tt::QoS::AtLeastOnce {
                return self.rsp_packet(req.tx, tt::Packet::PubAck(pkt)).await;
            }
        }
        Err(Error::Generic(format!(
            "req {:?} but got puback {:?}",
            req.req, pkt
        )))
    }

    async fn handle_pubrec(
        &mut self,
        fixed_header: tt::FixedHeader,
        bytes: Bytes,
        obuf: &mut BytesMut,
    ) -> Result<(), Error> {
        self.check_working_state("handle_pubrec")?;

        let pkt = tt::PubRec::decode(self.get_protocol(), fixed_header, bytes)?;
        trace_input!(pkt);

        let req = self
            .inflight
            .remove(&pkt.pkid)
            .ok_or_else(|| Error::Generic("inflight unexpect pubrec".to_string()))?;

        match req.req.unwrap_pkt()? {
            tt::Packet::Publish(rpkt) => {
                if rpkt.qos == tt::QoS::ExactlyOnce {
                    let rel = tt::PubRel::new(pkt.pkid);
                    rel.encode(self.get_protocol(), obuf)?;
                    trace_output!(rel);
                    let req = Request {
                        req: ReqItem::Packet(tt::Packet::PubRel(rel)),
                        tx: req.tx,
                    };
                    self.inflight.insert(pkt.pkid, req);
                    return Ok(());
                }
            }
            tt::Packet::PubRel(rel) => {
                rel.encode(self.get_protocol(), obuf)?;
                trace_input!(rel);
                let req = Request {
                    req: ReqItem::Packet(tt::Packet::PubRel(rel)),
                    tx: req.tx,
                };
                self.inflight.insert(pkt.pkid, req);
                return Ok(());
            }
            _ => {}
        }

        Err(Error::Generic(format!("{:?} unexpect pubrec", pkt)))
    }

    async fn handle_pubrel(
        &mut self,
        fixed_header: tt::FixedHeader,
        bytes: Bytes,
        obuf: &mut BytesMut,
    ) -> Result<(), Error> {
        self.check_working_state("handle_pubrec")?;

        let pkt = tt::PubRel::decode(self.get_protocol(), fixed_header, bytes)?;
        trace_input!(pkt);

        let req = self
            .qos2_rsp
            .remove(&pkt.pkid)
            .ok_or_else(|| Error::Generic("qos2 unexpect pubrel".to_string()))?;

        if let tt::Packet::PubRec(_) = req {
            let comp = tt::PubComp::new(pkt.pkid);
            comp.encode(self.get_protocol(), obuf)?;
            trace_output!(comp);
            Ok(())
        } else {
            Err(Error::Generic(format!("{:?} unexpect pubrel", pkt)))
        }
    }

    async fn handle_pubcomp(
        &mut self,
        fixed_header: tt::FixedHeader,
        bytes: Bytes,
        _obuf: &mut BytesMut,
    ) -> Result<(), Error> {
        self.check_working_state("handle_pubcomp")?;

        let pkt = tt::PubComp::decode(self.get_protocol(), fixed_header, bytes)?;
        trace_input!(pkt);

        let req = self
            .inflight
            .remove(&pkt.pkid)
            .ok_or_else(|| Error::Generic("inflight unexpect pubcomp".to_string()))?;

        if let tt::Packet::PubRel(_) = req.req.ref_pkt()? {
            return self.rsp_packet(req.tx, tt::Packet::PubComp(pkt)).await;
        }
        Err(Error::Generic(format!("{:?} unexpect pubcomp", pkt)))
    }

    async fn handle_pingresp(
        &mut self,
        _fixed_header: tt::FixedHeader,
        _bytes: Bytes,
        _obuf: &mut BytesMut,
    ) -> Result<(), Error> {
        self.check_working_state("handle_pingresp")?;

        trace_input!(tt::Packet::PingResp);

        Ok(())
    }

    async fn handle_disconnect(
        &mut self,
        fixed_header: tt::FixedHeader,
        bytes: Bytes,
        _obuf: &mut BytesMut,
    ) -> Result<(), Error> {
        self.check_working_state("handle_disconnect")?;

        let pkt = tt::Disconnect::decode(self.get_protocol(), fixed_header, bytes)?;
        trace_input!(pkt);
        if self.get_protocol() == tt::Protocol::V4 {
            return Err(Error::Generic("V4 got disconnect".to_string()));
        }

        return self.rsp_packet(None, tt::Packet::Disconnect(pkt)).await;
    }

    async fn handle_incoming(
        &mut self,
        ibuf: &mut BytesMut,
        obuf: &mut BytesMut,
    ) -> Result<(), Error> {
        while self.state != State::Closed {
            let r = tt::check(ibuf.iter(), self.max_incoming_size);
            match r {
                Err(tt::Error::InsufficientBytes(_required)) => return Ok(()),
                Err(e) => return Err(Error::Tt(e)),
                Ok(h) => {
                    let bytes = ibuf.split_to(h.frame_length()).freeze();
                    let packet_type = tt::PacketType::try_from(h.get_type_byte())?;

                    // if packet_type != tt::PacketType::ConnAck {
                    //     self.check_state(State::Working, format!("handle_incoming type {:?}", packet_type))?;
                    // }

                    match packet_type {
                        tt::PacketType::ConnAck => {
                            self.handle_connack(h, bytes, obuf).await?;
                        }

                        tt::PacketType::Publish => {
                            self.handle_publish(h, bytes, obuf).await?;
                        }

                        tt::PacketType::PubAck => {
                            self.handle_puback(h, bytes, obuf).await?;
                        }

                        tt::PacketType::PubRec => {
                            self.handle_pubrec(h, bytes, obuf).await?;
                        }

                        tt::PacketType::PubRel => {
                            self.handle_pubrel(h, bytes, obuf).await?;
                        }

                        tt::PacketType::PubComp => {
                            self.handle_pubcomp(h, bytes, obuf).await?;
                        }

                        tt::PacketType::SubAck => {
                            self.handle_suback(h, bytes, obuf).await?;
                        }

                        tt::PacketType::UnsubAck => {
                            self.handle_unsuback(h, bytes, obuf).await?;
                        }

                        tt::PacketType::PingResp => {
                            self.handle_pingresp(h, bytes, obuf).await?;
                        }

                        tt::PacketType::Disconnect => {
                            self.handle_disconnect(h, bytes, obuf).await?;
                        }

                        _ => {
                            return Err(Error::Generic(format!(
                                "incoming unexpect {:?}",
                                packet_type
                            )));
                        }
                    }
                }
            }
        }
        Ok(())
    }

    async fn response_disconnect(&mut self, reason: &str) -> Result<(), Error> {
        match self.inflight.remove(&self.pktid.get()) {
            Some(req) => self.rsp_event(req.tx, Event::Closed(reason.into())).await?,
            None => {}
        };

        self.rsp_event(None, Event::Closed(reason.into())).await
    }

    async fn response_inflight_error(&mut self, reason: &str) -> Result<(), Error> {
        let mut txx: Vec<Option<ResponseTX>> = Default::default();
        for (_, v) in self.inflight.drain() {
            txx.push(v.tx);
        }

        for tx in txx {
            let e = Error::Generic(reason.into());
            let _ = self.rsp_error(tx, e).await;
        }

        Ok(())
    }

    async fn exec_connect(
        &mut self,
        pkt: &mut tt::Connect,
        obuf: &mut BytesMut,
    ) -> Result<(u16, bool), Error> {
        self.check_state(State::Ready, "exec_connect")?;
        pkt.write(obuf)?;
        trace_output!(pkt);
        self.conn_pkt = Some(pkt.clone());
        self.state = State::Connecting;
        Ok((0, true))
    }

    async fn exec_subscribe(
        &mut self,
        pkt: &mut tt::Subscribe,
        obuf: &mut BytesMut,
    ) -> Result<(u16, bool), Error> {
        self.check_state(State::Working, "exec_subscribe")?;

        if pkt.pkid == 0 {
            pkt.pkid = self.pktid.next().unwrap();
        }

        pkt.encode(self.get_protocol(), obuf)?;
        trace_output!(pkt);

        Ok((pkt.pkid, true))
    }

    async fn exec_unsubscribe(
        &mut self,
        pkt: &mut tt::Unsubscribe,
        obuf: &mut BytesMut,
    ) -> Result<(u16, bool), Error> {
        self.check_state(State::Working, "exec_unsubscribe")?;

        if pkt.pkid == 0 {
            pkt.pkid = self.pktid.next().unwrap();
        }

        pkt.encode(self.get_protocol(), obuf)?;
        trace_output!(pkt);

        Ok((pkt.pkid, true))
    }

    async fn exec_publish(
        &mut self,
        pkt: &mut tt::Publish,
        obuf: &mut BytesMut,
    ) -> Result<(u16, bool), Error> {
        self.check_state(State::Working, "exec_publish")?;

        if pkt.pkid == 0 && pkt.qos != tt::QoS::AtMostOnce {
            pkt.pkid = self.pktid.next().unwrap();
        }

        pkt.encode(self.get_protocol(), obuf)?;
        trace_output!(pkt);

        Ok((pkt.pkid, pkt.qos != tt::QoS::AtMostOnce))
    }

    async fn exec_disconnect(
        &mut self,
        pkt: &mut tt::Disconnect,
        obuf: &mut BytesMut,
    ) -> Result<(u16, bool), Error> {
        self.check_state(State::Working, "exec_disconnect")?;

        pkt.encode(self.get_protocol(), obuf)?;
        trace_output!(pkt);

        self.state = State::Disconnecting;

        Ok((self.pktid.next().unwrap(), true))
    }

    async fn exec_req(
        &mut self,
        now: Instant,
        mut req: Request,
        obuf: &mut BytesMut,
    ) -> Result<bool, Error> {
        let r = match &mut req.req {
            ReqItem::Packet(pkt0) => {
                self.last_active_time = now;
                match pkt0 {
                    tt::Packet::Connect(pkt) => self.exec_connect(pkt, obuf).await,
                    tt::Packet::Subscribe(pkt) => self.exec_subscribe(pkt, obuf).await,
                    tt::Packet::Unsubscribe(pkt) => self.exec_unsubscribe(pkt, obuf).await,
                    tt::Packet::Publish(pkt) => self.exec_publish(pkt, obuf).await,
                    tt::Packet::Disconnect(pkt) => self.exec_disconnect(pkt, obuf).await,
                    _ => panic!("exec_req unexpect {:?}", pkt0),
                }
            }
            ReqItem::Shutdown => {
                return Ok(true);
            }
        };

        match r {
            Ok((n, is_inflight)) => {
                if is_inflight {
                    self.inflight.insert(n, req);
                }
                Ok(false)
            }
            Err(e) => Err(e),
        }
    }

    fn next(&mut self, now: Instant, obuf: &mut BytesMut) -> Result<(bool, bool, Instant), Error> {
        let hold_req = self.state == State::Disconnecting || obuf.len() >= self.max_osize;
        let hold_read = obuf.len() >= self.max_osize;
        let next_time = if let Some(pkt) = &self.conn_pkt {
            self.last_active_time + Duration::from_secs(pkt.keep_alive as u64)
        } else {
            self.last_active_time + Duration::from_secs(999999999)
        };

        //trace!("hold_req {}, hold_read {}", hold_req, hold_read);

        if now >= next_time {
            if !hold_read {
                tt::PingReq {}.write(obuf)?;
                trace_output!(tt::Packet::PingReq);
                self.last_active_time = now.clone();
                return self.next(now, obuf);
            }
        }
        Ok((hold_req, hold_read, next_time))
    }
}

async fn task_entry(
    mut socket: TcpStream,
    mut req_rx: mpsc::Receiver<Request>,
    session: &mut Session,
) -> Result<(), Error> {
    let (mut rd, mut wr) = socket.split();
    let mut ibuf = bytes::BytesMut::with_capacity(64);
    let mut obuf = bytes::BytesMut::with_capacity(64);

    loop {
        let (hold_req, hold_read, next_time) = session.next(Instant::now(), &mut obuf)?;

        tokio::select! {
            r = wr.write_buf(&mut obuf), if !obuf.is_empty() => {
                r?;
            }

            r = rd.read_buf(&mut ibuf), if !hold_read=> {
                let len = match r {
                    Ok(n) => n,
                    Err(e) => {
                        debug!("read socket fail, {:?}", e);
                        return Err(Error::Io(e))
                    },
                };
                if len == 0 {
                    if session.state == State::Disconnecting {
                        // gracefully disconnect
                        session.response_disconnect("active disconnect").await?;
                        break;
                    } else {
                        return Err(Error::Broken("read disconnected".to_string()));
                    }
                }
                session.handle_incoming(&mut ibuf, &mut obuf).await?
            }

            r = req_rx.recv(), if !hold_req => {
                match r{
                    Some(req)=>{
                        let shutdown = session.exec_req(Instant::now(), req, &mut obuf).await?;
                        if shutdown {
                            debug!("shutdown");
                            wr.shutdown().await?;
                            break;
                        }
                    }
                    None => {
                        //debug!("no sender, closed");
                        break;
                    },
                }
            }

            _ = tokio::time::sleep_until(next_time), if !hold_read => {

            }
        }
    }

    Ok(())
}

pub async fn make_connection(name: &str, addr: &str) -> Result<Client, Error> {
    // trace!("connecting to [{}]...", addr);
    // let socket = match TcpStream::connect(addr).await{
    //     Ok(h) => h,
    //     Err(e) => {
    //         let e = Error::Generic(format!("unable to connect {}, {}", addr, e.to_string()));
    //         debug!("{}", e.to_string());
    //         return Err(e);
    //     },
    // };
    // trace!("connected [{}]", addr);

    let (req_tx, req_rx) = mpsc::channel(64);
    let (ev_tx, mut ev_rx) = mpsc::channel(64);
    let addr = addr.to_string();
    let fut = async move {
        trace!("connecting to [{}]...", addr);
        let socket = match TcpStream::connect(&addr).await {
            Ok(h) => h,
            Err(e) => {
                let e = Error::Generic(format!("unable to connect {}, {}", &addr, e.to_string()));
                debug!("{}", e.to_string());
                let _r = ev_tx.send(Response::Error(e)).await;
                return;
                //return Err(e);
            }
        };
        trace!("connected [{}]", addr);
        if let Err(e) = socket.set_nodelay(true) {
            error!("socket set nodelay with {:?}", e);
        }
        let _r = ev_tx.send(Response::Connected).await;

        let mut session = Session::new(ev_tx, Instant::now());
        if let Err(e) = task_entry(socket, req_rx, &mut session).await {
            debug!("<{:p}> task finished with error [{:?}]", &session, e);
            let _ = session.response_inflight_error(&format!("{:?}", e)).await;
            let _ = session.rsp_error(None, e).await;
        }
    };

    if !name.is_empty() {
        let span = tracing::span!(tracing::Level::DEBUG, "", c = name);
        tokio::spawn(tracing::Instrument::instrument(fut, span));
    } else {
        tokio::spawn(fut);
    }

    let rsp = ev_rx.recv().await.ok_or_else(|| Error::Finished)?;
    match rsp {
        Response::Connected => {}
        Response::Event(_) => {
            panic!("never reach heare");
        }
        Response::Error(e) => {
            return Err(e);
        }
    };

    Ok(Client {
        sender: Sender { tx: req_tx },
        receiver: Receiver { rx: ev_rx },
    })
}

#[derive(Debug)]
pub struct Client {
    pub sender: Sender,
    pub receiver: Receiver,
}

impl Client {
    pub fn split(self) -> (Sender, Receiver) {
        (self.sender, self.receiver)
    }
}

#[derive(Debug)]
pub struct Receiver {
    rx: mpsc::Receiver<Response>,
}

impl Receiver {
    pub async fn recv(&mut self) -> Result<Event, Error> {
        let rsp = self.rx.recv().await.ok_or_else(|| Error::Finished)?;
        match rsp {
            Response::Event(ev) => Ok(ev),
            Response::Error(e) => Err(e),
            Response::Connected => panic!("never reach heare"),
        }
    }
}

pub struct PublishFuture {
    rx: ResponseRX,
    qos: tt::QoS,
}

impl PublishFuture {
    fn new(rx: ResponseRX, qos: tt::QoS) -> Self {
        Self { rx, qos }
    }
}

impl futures::Future for PublishFuture {
    type Output = Result<(), Error>;
    // type Output = Result<M, Error>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.qos == tt::QoS::AtMostOnce {
            return Poll::Ready(Ok(()));
        }

        match Pin::new(&mut self.rx).poll(cx) {
            Poll::Ready(Ok(r)) => {
                let event = match r {
                    Response::Event(e) => e,
                    Response::Error(e) => {
                        return Poll::Ready(Err(e));
                    }
                    Response::Connected => panic!("never reach here"),
                };

                let pkt = match event {
                    Event::Packet(pkt) => pkt,
                    Event::Closed(r) => {
                        let e = Error::Generic(format!("expect packet but closed with [{}]", r));
                        return Poll::Ready(Err(e));
                    }
                };

                match self.qos {
                    tt::QoS::AtMostOnce => Poll::Ready(Ok(())),
                    tt::QoS::AtLeastOnce => {
                        if let tt::Packet::PubAck(_r) = pkt {
                            Poll::Ready(Ok(()))
                        } else {
                            let e = Error::Generic(format!("response expect PubAck but {:?}", pkt));
                            Poll::Ready(Err(e))
                        }
                    }
                    tt::QoS::ExactlyOnce => {
                        if let tt::Packet::PubComp(_) = pkt {
                            Poll::Ready(Ok(()))
                        } else {
                            let e =
                                Error::Generic(format!("response expect PubComp but {:?}", pkt));
                            Poll::Ready(Err(e))
                        }
                    }
                }
            }

            Poll::Ready(Err(_e)) => Poll::Ready(Err(Error::Generic(
                "worker unexpectedly disconnected".into(),
            )
            .into())),
            Poll::Pending => Poll::Pending,
        }
    }
}

#[derive(Debug)]
pub struct Sender {
    tx: mpsc::Sender<Request>,
}

impl Sender {
    pub async fn connect(&mut self, pkt: tt::Connect) -> Result<tt::ConnAck, Error> {
        let (tx, mut rx) = oneshot::channel();

        if let Err(_) = self
            .tx
            .send(Request {
                req: ReqItem::Packet(tt::Packet::Connect(pkt)),
                tx: Some(tx),
            })
            .await
        {
            return Err(Error::Finished);
        }

        let pkt = recv_pkt(&mut rx).await?;

        if let tt::Packet::ConnAck(ack) = pkt {
            Ok(ack)
        } else {
            Err(Error::Generic("response expect ConnAck".to_string()))
        }
    }

    pub async fn disconnect(&mut self, pkt: tt::Disconnect) -> Result<(), Error> {
        let (tx, mut rx) = oneshot::channel();

        if let Err(_) = self
            .tx
            .send(Request {
                req: ReqItem::Packet(tt::Packet::Disconnect(pkt)),
                tx: Some(tx),
            })
            .await
        {
            return Err(Error::Finished);
        }

        return recv_closed(&mut rx).await;
    }

    pub async fn shutdown(&mut self) -> Result<(), Error> {
        if let Err(_) = self
            .tx
            .send(Request {
                req: ReqItem::Shutdown,
                tx: None,
            })
            .await
        {
            return Err(Error::Finished);
        }

        return Ok(());
    }

    pub async fn subscribe(&mut self, pkt: tt::Subscribe) -> Result<tt::SubAck, Error> {
        let (tx, mut rx) = oneshot::channel();

        if let Err(_) = self
            .tx
            .send(Request {
                req: ReqItem::Packet(tt::Packet::Subscribe(pkt)),
                tx: Some(tx),
            })
            .await
        {
            return Err(Error::Finished);
        }

        let pkt = recv_pkt(&mut rx).await?;

        if let tt::Packet::SubAck(ack) = pkt {
            Ok(ack)
        } else {
            Err(Error::Generic("response expect SubAck".to_string()))
        }
    }

    pub async fn unsubscribe(&mut self, pkt: tt::Unsubscribe) -> Result<tt::UnsubAck, Error> {
        let (tx, mut rx) = oneshot::channel();

        if let Err(_) = self
            .tx
            .send(Request {
                req: ReqItem::Packet(tt::Packet::Unsubscribe(pkt)),
                tx: Some(tx),
            })
            .await
        {
            return Err(Error::Finished);
        }

        let pkt = recv_pkt(&mut rx).await?;

        if let tt::Packet::UnsubAck(ack) = pkt {
            Ok(ack)
        } else {
            Err(Error::Generic("response expect UnsubAck".to_string()))
        }
    }

    // publish QoS0~2
    pub async fn publish(&mut self, pkt: tt::Publish) -> Result<(), Error> {
        // QoS0, send direcly and return
        if pkt.qos == tt::QoS::AtMostOnce {
            if let Err(_) = self
                .tx
                .send(Request {
                    req: ReqItem::Packet(tt::Packet::Publish(pkt)),
                    tx: None,
                })
                .await
            {
                return Err(Error::Finished);
            }
            return Ok(());
        }

        let qos = pkt.qos;

        let (tx, mut rx) = oneshot::channel();

        if let Err(_) = self
            .tx
            .send(Request {
                req: ReqItem::Packet(tt::Packet::Publish(pkt)),
                tx: Some(tx),
            })
            .await
        {
            return Err(Error::Finished);
        }

        let pkt = recv_pkt(&mut rx).await?;
        match qos {
            tt::QoS::AtMostOnce => Ok(()),
            tt::QoS::AtLeastOnce => {
                if let tt::Packet::PubAck(_) = pkt {
                    Ok(())
                } else {
                    Err(Error::Generic("response expect PubAck".to_string()))
                }
            }
            tt::QoS::ExactlyOnce => {
                if let tt::Packet::PubComp(_) = pkt {
                    Ok(())
                } else {
                    Err(Error::Generic("response expect PubAck".to_string()))
                }
            }
        }
    }

    pub async fn publish_result(&mut self, pkt: tt::Publish) -> Result<PublishFuture, Error> {
        let qos = pkt.qos;

        let (tx, rx) = oneshot::channel();
        let tx = if pkt.qos == tt::QoS::AtMostOnce {
            let _r = tx.send(Response::Connected); // never use Response::Connected
            None
        } else {
            Some(tx)
        };

        if let Err(_) = self
            .tx
            .send(Request {
                req: ReqItem::Packet(tt::Packet::Publish(pkt)),
                tx,
            })
            .await
        {
            return Err(Error::Finished);
        }

        Ok(PublishFuture::new(rx, qos))
    }
}

#[derive(Debug)]
pub struct SyncClient {
    name: String,
    protocol: tt::Protocol,
    socket: Option<TcpStream>,
    ibuf: BytesMut,
    pktid: tt::PacketId,
}

impl SyncClient {
    pub fn new(name: String) -> Self {
        Self {
            name,
            protocol: tt::Protocol::V4,
            socket: None,
            ibuf: BytesMut::new(),
            pktid: tt::PacketId::default(),
        }
    }

    pub fn name(&self) -> &String {
        &self.name
    }

    pub async fn recv_packet(&mut self) -> Result<(tt::PacketType, tt::FixedHeader, Bytes), Error> {
        loop {
            let r = tt::check(self.ibuf.iter(), 64 * 1024);
            match r {
                Err(tt::Error::InsufficientBytes(_required)) => {}
                Err(e) => return Err(Error::Tt(e)),
                Ok(h) => {
                    let bytes = self.ibuf.split_to(h.frame_length()).freeze();
                    let packet_type = tt::PacketType::try_from(h.get_type_byte())?;
                    return Ok((packet_type, h, bytes));
                }
            }
            self.socket
                .as_mut()
                .unwrap()
                .read_buf(&mut self.ibuf)
                .await?;
        }
    }

    async fn recv_specific_packet(
        &mut self,
        expect_type: tt::PacketType,
    ) -> Result<(tt::FixedHeader, Bytes), Error> {
        let (ptype, h, bytes) = self.recv_packet().await?;
        if ptype != expect_type {
            return Err(Error::Generic(format!(
                "expect packet {:?} but {:?}",
                expect_type, ptype
            )));
        } else {
            return Ok((h, bytes));
        }
    }

    pub async fn connect(&mut self, addr: &str, pkt: &tt::Connect) -> Result<tt::ConnAck, Error> {
        let span = tracing::span!(tracing::Level::DEBUG, "", c = &self.name[..]);
        async move {
            trace!("connecting to [{}]...", addr);

            self.protocol = pkt.protocol;

            let mut socket = TcpStream::connect(&addr).await?;

            let mut obuf = BytesMut::new();
            pkt.write(&mut obuf)?;
            trace_output!(pkt);
            socket.write_all_buf(&mut obuf).await?;
            self.socket = Some(socket);

            let (h, bytes) = self.recv_specific_packet(tt::PacketType::ConnAck).await?;
            let ack = tt::ConnAck::decode(self.protocol, h, bytes)?;
            trace_input!(ack);
            Ok(ack)
        }
        .instrument(span)
        .await
    }

    pub async fn subscribe(&mut self, pkt: &tt::Subscribe) -> Result<tt::SubAck, Error> {
        let span = tracing::span!(tracing::Level::DEBUG, "", c = &self.name[..]);
        async move {
            let pktid = self.pktid.next().unwrap();
            let socket = self.socket.as_mut().unwrap();
            let mut obuf = BytesMut::new();
            pkt.encode_with_pktid(self.protocol, pktid, &mut obuf)?;
            trace_output!(pkt);
            socket.write_all_buf(&mut obuf).await?;
            let (h, bytes) = self.recv_specific_packet(tt::PacketType::SubAck).await?;
            let ack = tt::SubAck::decode(self.protocol, h, bytes)?;
            trace_input!(ack);
            Ok(ack)
        }
        .instrument(span)
        .await
    }

    pub async fn unsubscribe(&mut self, pkt: &tt::Unsubscribe) -> Result<tt::UnsubAck, Error> {
        let span = tracing::span!(tracing::Level::DEBUG, "", c = &self.name[..]);
        async move {
            let pktid = self.pktid.next().unwrap();
            let socket = self.socket.as_mut().unwrap();
            let mut obuf = BytesMut::new();
            pkt.encode_with_pktid(self.protocol, pktid, &mut obuf)?;
            trace_output!(pkt);
            socket.write_all_buf(&mut obuf).await?;
            let (h, bytes) = self.recv_specific_packet(tt::PacketType::UnsubAck).await?;
            let ack = tt::UnsubAck::decode(self.protocol, h, bytes)?;
            trace_input!(ack);
            Ok(ack)
        }
        .instrument(span)
        .await
    }

    pub async fn publish(&mut self, pkt: &tt::Publish) -> Result<(), Error> {
        let span = tracing::span!(tracing::Level::DEBUG, "", c = &self.name[..]);
        async move {
            let pktid = self.pktid.next().unwrap();
            {
                let socket = self.socket.as_mut().unwrap();
                let mut obuf = BytesMut::new();
                pkt.encode_with(self.protocol, pktid, pkt.qos, &mut obuf)?;
                trace_output!(pkt);
                socket.write_all_buf(&mut obuf).await?;
            }

            match pkt.qos {
                tt::QoS::AtMostOnce => {
                    return Ok(());
                }
                tt::QoS::AtLeastOnce => {
                    let (h, bytes) = self.recv_specific_packet(tt::PacketType::PubAck).await?;
                    let ack = tt::PubAck::decode(self.protocol, h, bytes)?;
                    trace_input!(ack);
                    if ack.reason != tt::PubAckReason::Success {
                        return Err(Error::Generic(format!("puback reason {:?}", ack.reason)));
                    } else {
                        return Ok(());
                    }
                }
                tt::QoS::ExactlyOnce => {
                    let (h, bytes) = self.recv_specific_packet(tt::PacketType::PubRec).await?;
                    let rec = tt::PubRec::decode(self.protocol, h, bytes)?;
                    trace_input!(rec);
                    if rec.reason != tt::PubRecReason::Success {
                        return Err(Error::Generic(format!("pubrec reason {:?}", rec.reason)));
                    }

                    let socket = self.socket.as_mut().unwrap();
                    let mut obuf = BytesMut::new();

                    let rel = tt::PubRel::new(rec.pkid);
                    rel.encode(self.protocol, &mut &mut obuf)?;
                    trace_output!(rel);
                    socket.write_all_buf(&mut obuf).await?;

                    let (h, bytes) = self.recv_specific_packet(tt::PacketType::PubComp).await?;
                    let comp = tt::PubComp::decode(self.protocol, h, bytes)?;
                    trace_input!(comp);
                    if comp.reason != tt::PubCompReason::Success {
                        return Err(Error::Generic(format!("pubcomp reason {:?}", rec.reason)));
                    }

                    return Ok(());
                }
            }
        }
        .instrument(span)
        .await
    }

    pub async fn recv_publish(&mut self) -> Result<tt::Publish, Error> {
        let span = tracing::span!(tracing::Level::DEBUG, "", c = &self.name[..]);
        async move {
            let (h, bytes) = self.recv_specific_packet(tt::PacketType::Publish).await?;
            let pkt = tt::Publish::decode(self.protocol, h, bytes)?;
            trace_input!(pkt);
            match pkt.qos {
                tt::QoS::AtMostOnce => {}
                tt::QoS::AtLeastOnce => {
                    let socket = self.socket.as_mut().unwrap();
                    let ack = tt::PubAck::new(pkt.pkid);
                    let mut obuf = BytesMut::new();
                    ack.encode(self.protocol, &mut obuf)?;
                    trace_output!(ack);
                    socket.write_all_buf(&mut obuf).await?;
                }
                tt::QoS::ExactlyOnce => {
                    let mut obuf = BytesMut::new();
                    {
                        let rec = tt::PubRec::new(pkt.pkid);
                        rec.encode(self.protocol, &mut obuf)?;
                        trace_output!(rec);
                    }

                    {
                        let (h, bytes) = self.recv_specific_packet(tt::PacketType::PubRel).await?;
                        let rel = tt::PubRel::decode(self.protocol, h, bytes)?;
                        if rel.pkid != pkt.pkid {
                            return Err(Error::Generic(format!(
                                "diff pkid {}, {}",
                                rel.pkid, pkt.pkid
                            )));
                        }
                    }

                    {
                        let comp = tt::PubComp::new(pkt.pkid);
                        comp.encode(self.protocol, &mut obuf)?;
                        trace_output!(comp);
                    }
                    let socket = self.socket.as_mut().unwrap();
                    socket.write_all_buf(&mut obuf).await?;
                }
            }
            Ok(pkt)
        }
        .instrument(span)
        .await
    }

    pub async fn disconnect(&mut self, pkt: &tt::Disconnect) -> Result<(), Error> {
        let span = tracing::span!(tracing::Level::DEBUG, "", c = &self.name[..]);
        async move {
            let socket = self.socket.as_mut().unwrap();
            let mut obuf = BytesMut::new();
            pkt.encode(self.protocol, &mut obuf)?;
            trace_output!(pkt);
            socket.write_all_buf(&mut obuf).await?;
            Ok(())
        }
        .instrument(span)
        .await
    }

    pub async fn shutdown(&mut self) -> Result<(), Error> {
        let span = tracing::span!(tracing::Level::DEBUG, "", c = &self.name[..]);
        async move {
            let socket = self.socket.as_mut().unwrap();
            socket.shutdown().await?;
            self.socket = None;
            Ok(())
        }
        .instrument(span)
        .await
    }
}
