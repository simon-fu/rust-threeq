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
use std::{collections::HashMap, convert::TryFrom, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::{mpsc, oneshot},
    time::{error::Elapsed, Instant},
};
use tracing::{debug, trace};

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

    #[error("Generic error: {0}")]
    Generic(String),
}

#[derive(Debug)]
pub enum Event {
    Packet(tt::Packet),
    Closed(String),
}

#[derive(Debug)]
enum Response {
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
}

impl ReqItem {
    fn unwrap_pkt(self) -> Result<tt::Packet, Error> {
        match self {
            ReqItem::Packet(pkt) => Ok(pkt),
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
    pktid: u16,
    max_incoming_size: usize,
    conn_pkt: Option<tt::Connect>,
    inflight: HashMap<u16, Request>,
    qos2_rsp: HashMap<u16, tt::Packet>,
    last_active_time: Instant,
}

impl Session {
    fn new(tx: mpsc::Sender<Response>, now:Instant) -> Self {
        Session {
            max_osize : 16*1024,
            tx,
            state: State::Ready,
            pktid: 0,
            max_incoming_size: 64 * 1024,
            conn_pkt: None,
            inflight: Default::default(),
            qos2_rsp: Default::default(),
            last_active_time: now,
        }
    }

    fn next_pktid(&mut self) -> u16 {
        self.pktid += 1;
        if self.pktid == 0 {
            self.pktid = 1;
        }
        self.pktid
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

        if let tt::Packet::Connect(_) = req.req.unwrap_pkt()? {
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
        self.check_state(State::Working, "handle_publish")?;

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
        self.check_state(State::Working, "handle_suback")?;

        let pkt = tt::SubAck::decode(self.get_protocol(), fixed_header, bytes)?;
        trace_input!(pkt);

        let req = self
            .inflight
            .remove(&pkt.pkid)
            .ok_or_else(|| Error::Generic("inflight unexpect subnack".to_string()))?;

        if let tt::Packet::Subscribe(_) = req.req.unwrap_pkt()? {
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
        self.check_state(State::Working, "handle_unsuback")?;

        let pkt = tt::UnsubAck::decode(self.get_protocol(), fixed_header, bytes)?;
        trace_input!(pkt);

        let req = self
            .inflight
            .remove(&pkt.pkid)
            .ok_or_else(|| Error::Generic("inflight unexpect unsubnack".to_string()))?;

        if let tt::Packet::Unsubscribe(_) = req.req.unwrap_pkt()? {
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
        self.check_state(State::Working, "handle_puback")?;

        let pkt = tt::PubAck::decode(self.get_protocol(), fixed_header, bytes)?;
        trace_input!(pkt);

        let req = self
            .inflight
            .remove(&pkt.pkid)
            .ok_or_else(|| Error::Generic("inflight unexpect puback".to_string()))?;

        if let tt::Packet::Publish(rpkt) = req.req.unwrap_pkt()? {
            if rpkt.qos == tt::QoS::AtLeastOnce {
                return self.rsp_packet(req.tx, tt::Packet::PubAck(pkt)).await;
            }
        }
        Err(Error::Generic(format!("{:?} unexpect puback", pkt)))
    }

    async fn handle_pubrec(
        &mut self,
        fixed_header: tt::FixedHeader,
        bytes: Bytes,
        obuf: &mut BytesMut,
    ) -> Result<(), Error> {
        self.check_state(State::Working, "handle_pubrec")?;

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
        self.check_state(State::Working, "handle_pubrec")?;

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
        self.check_state(State::Working, "handle_pubcomp")?;

        let pkt = tt::PubComp::decode(self.get_protocol(), fixed_header, bytes)?;
        trace_input!(pkt);

        let req = self
            .inflight
            .remove(&pkt.pkid)
            .ok_or_else(|| Error::Generic("inflight unexpect pubcomp".to_string()))?;

        if let tt::Packet::PubRel(_) = req.req.unwrap_pkt()? {
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
        self.check_state(State::Working, "handle_pingresp")?;

        trace_input!(tt::Packet::PingResp);

        Ok(())
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

                    if packet_type != tt::PacketType::ConnAck {
                        self.check_state(State::Working, "handle_incoming")?;
                    }

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
        let ev = Event::Closed(reason.into());

        let tx = match self.inflight.remove(&self.pktid) {
            Some(req) => req.tx,
            None => None,
        };

        self.rsp_event(tx, ev).await
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
            pkt.pkid = self.next_pktid();
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
            pkt.pkid = self.next_pktid();
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
            pkt.pkid = self.next_pktid();
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

        Ok((self.next_pktid(), true))
    }

    async fn exec_req(&mut self, now:Instant, mut req: Request, obuf: &mut BytesMut) -> Result<(), Error> {
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
        };

        match r {
            Ok((n, is_inflight)) => {
                if is_inflight {
                    self.inflight.insert(n, req);
                }
                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    // fn get_next_ping_time(&self) -> Instant {
    //     if let Some(pkt) = &self.conn_pkt {
    //         self.last_active_time + Duration::from_secs(pkt.keep_alive as u64)
    //     } else {
    //         self.last_active_time + Duration::from_secs(999999999)
    //     }
        
    // }

    fn next(&mut self, now:Instant, obuf: &mut BytesMut) -> Result<(bool, bool, Instant), Error>{
        let hold_req = self.state == State::Disconnecting || obuf.len() >= self.max_osize;
        let hold_read = obuf.len() >= self.max_osize;
        let next_time =  if let Some(pkt) = &self.conn_pkt {
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
                let len = r?;
                if len == 0 {
                    if session.state == State::Disconnecting {
                        // gracefully disconnect
                        session.response_disconnect("active disconnect").await?;
                        break;
                    } else {
                        let _ = session.rsp_error(None, Error::Broken("read disconnected".to_string()) ).await;
                        return Err(Error::Broken("read disconnected".to_string()));
                    }
                }
                session.handle_incoming(&mut ibuf, &mut obuf).await?
            }

            r = req_rx.recv(), if !hold_req => {
                match r{
                    Some(req)=>{
                        session.exec_req(Instant::now(), req, &mut obuf).await?;
                    }
                    None => {
                        debug!("no sender, closed");
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

pub async fn make_connection(addr: &str) -> Result<(Sender, Receiver), Error> {
    let socket = TcpStream::connect(addr).await?;
    debug!("connected to {}", addr);

    let (req_tx, req_rx) = mpsc::channel(64);
    let (ev_tx, ev_rx) = mpsc::channel(64);

    tokio::spawn(async move {
        let mut session = Session::new(ev_tx, Instant::now());
        if let Err(e) = task_entry(socket, req_rx, &mut session).await {
            debug!("task finished with error [{:?}]", e);
            let _ = session.rsp_error(None, e).await;
        }
    });

    Ok((Sender { tx: req_tx }, Receiver { rx: ev_rx }))
}

pub struct Receiver {
    rx: mpsc::Receiver<Response>,
}

impl Receiver {
    pub async fn recv(&mut self) -> Result<Event, Error> {
        let rsp = self.rx.recv().await.ok_or_else(|| Error::Finished)?;
        match rsp {
            Response::Event(ev) => Ok(ev),
            Response::Error(e) => Err(e),
        }
    }
}

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
}
