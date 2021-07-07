/*
TODO:
√ connect
√ gracefully disconnect
- subscribe
- send publish
- recv publish
- auto ping
- in-flight window / max pending out buf size

Scenario:
- send publish : async send publish, auto send pubrel, notify puback/pubrec/pubcomp
- send subscribe sync: reponse 
- send subscribe async: notify suback
- send unsubscribe sync: reponse
- send unsubscribe async： notify unsuback
- recv publish : notfiy publish, auto send puback, pubrec/pubcomp

*/

use bytes::{Bytes, BytesMut};
use tokio::{io::{AsyncReadExt, AsyncWriteExt}, net::TcpStream, sync::{mpsc, oneshot}, time::error::Elapsed};
use tracing::{debug, trace};
use crate::tq3::tt;
use core::panic;
use std::{collections::HashMap, convert::TryFrom};

macro_rules! trace_input{
    ($a:expr)=>{
        {
            trace!("recv <-: {:?}", $a)
        }
    }
}

macro_rules! trace_output{
    ($a:expr)=>{
        {
            trace!("send ->: {:?}", $a)
        }
    }
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


#[derive(Debug, PartialEq, Eq)]
enum PubType{
    Async,
    SyncDone,
    SyncAck,
    SyncRec,
}

#[derive(Debug)]
enum ReqPacket{
    Connect(tt::Connect),
    Subscribe(tt::Subscribe),
    Publish(tt::Publish, PubType),
    PubRel(tt::PubRel),
    Disconnect(tt::Disconnect),
    InnerPubRel(tt::PubRel),
}

#[derive(Debug)]
pub enum RspPacket{
    ConnAck(tt::ConnAck),
    SubAck(tt::SubAck),
    PubAck(tt::PubAck),
    PubRec(tt::PubRec),
    PubComp(tt::PubComp),
    PubDone,
    DisconnectDone,
}

#[derive(Debug)]
pub enum Event{
    Error(Error),
    ConnAck(tt::ConnAck),
    Publish0(tt::Publish),
    Publish1(tt::Publish, oneshot::Sender<tt::PubAck>),
    Publish2(tt::Publish, oneshot::Sender<tt::PubRec>),
    PubRel(tt::PubRel, oneshot::Sender<tt::PubComp>),
    PubAck(tt::PubAck),
    PubRec(tt::PubRec),
    PubComp(tt::PubComp),
    SubAck(tt::SubAck),
    UnsubAck(tt::UnsubAck),
    Disconnect(tt::Disconnect),
    Closed,
}


type Response = Result<RspPacket, Error>;
type ResponseTX = oneshot::Sender<Response>;

#[derive(Debug)]
struct Request{
    pkt : ReqPacket,
    tx : Option<ResponseTX>,
}


#[derive(Debug, PartialEq, Eq, Clone, Copy)]
enum State{
    Ready,
    Connecting,
    Working,
    Disconnecting,
    Closed,
}

impl Default for State {
    fn default() -> Self { State::Ready }
}


#[derive(Debug)]
struct Session{
    tx : mpsc::Sender<Event>,
    state : State,
    pktid : u16,
    max_incoming_size: usize,
    conn_pkt : Option<tt::Connect>,
    pend_reqs : HashMap<u16, Request>,
}

impl Session{
    fn new(tx : mpsc::Sender<Event>) -> Self{
        Session{
            tx,
            state : State::Ready,
            pktid : 0,
            max_incoming_size : 64*1024,
            conn_pkt : None,
            pend_reqs : Default::default(), 
        }
    }

    fn next_pktid(&mut self) -> u16{
        self.pktid += 1;
        if self.pktid == 0 {
            self.pktid = 1;
        }
        self.pktid
    }

    fn get_protocol(&self) -> tt::Protocol{
        self.conn_pkt.as_ref().unwrap().protocol
    }

    fn check_state<S: Into<String>,>(&self, expect : State, origin : S ) -> Result<(), Error>{
        if expect == self.state {
            Ok(())
        } else {
            Err(Error::State(format!("expect {:?} but {:?}, origin {}", expect, self.state, origin.into())))
        }
    }


    async fn send_event(&mut self, ev:Event) -> Result<(), Error>{
        if let Err(_) = self.tx.send(ev).await {
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

        match self.pend_reqs.remove(&0) {
            Some(req) => {
                if let Some(tx) = req.tx {
                    let _ = tx.send(Ok(RspPacket::ConnAck(pkt)));
                }
            },
            None => {
                self.send_event(Event::ConnAck(pkt)).await?;
            },
        }

        Ok(())
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

        match pkt.qos {
            tt::QoS::AtMostOnce => {
                self.send_event(Event::Publish0(pkt)).await?;
            },

            tt::QoS::AtLeastOnce => {
                let (tx, rx) = oneshot::channel();
                let pkid = pkt.pkid;
                self.send_event(Event::Publish1(pkt, tx)).await?;
                let ack = match rx.await{
                    Ok(ack) => { ack },
                    Err(_) => { tt::PubAck::new(pkid) },
                };
                ack.encode(self.get_protocol(), obuf)?;
                trace_output!(ack);
            },

            tt::QoS::ExactlyOnce => {
                let (tx, rx) = oneshot::channel();
                let pkid = pkt.pkid;
                self.send_event(Event::Publish2(pkt, tx)).await?;
                let ack = match rx.await{
                    Ok(ack) => { ack },
                    Err(_) => { tt::PubRec::new(pkid) },
                };
                ack.encode(self.get_protocol(), obuf)?;
                trace_output!(ack);
            },
        }
        
        Ok(())
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

        match self.pend_reqs.remove(&pkt.pkid) {
            Some(req) => {
                if let Some(tx) = req.tx {
                    let _ = tx.send(Ok(RspPacket::SubAck(pkt)));
                }
            },
            None => {
                self.send_event(Event::SubAck(pkt)).await?;
            },
        }
        
        Ok(())
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

        let opt = self.pend_reqs.remove(&pkt.pkid);
        if opt.is_none(){
            return self.send_event(Event::PubAck(pkt)).await;
        }

        let req = opt.unwrap();
        match req.pkt {
            ReqPacket::Publish(rpkt, ptype) => { 
                if rpkt.qos != tt::QoS::AtLeastOnce {
                    let e = Error::Generic("Not qos1 but got puback".to_string());
                    return Err(e);
                }

                match ptype{
                    PubType::Async => {},
                    PubType::SyncDone => {let _ = req.tx.unwrap().send(Ok(RspPacket::PubDone));},
                    PubType::SyncAck => {let _ = req.tx.unwrap().send(Ok(RspPacket::PubAck(pkt)));},
                    PubType::SyncRec => {return Err(Error::Generic("expect pubrec but got puback".to_string()));},
                }
            },

            _ => {
                let msg = format!("req [{:?}] but got puback", req.pkt);
                return Err(Error::Generic(msg));
            }
        }
        
        Ok(())
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

        let opt = self.pend_reqs.remove(&pkt.pkid);
        if opt.is_none(){
            return self.send_event(Event::PubRec(pkt)).await;
        }

        let req = opt.unwrap();
        match req.pkt {
            ReqPacket::Publish(rpkt, ptype) => { 
                if rpkt.qos != tt::QoS::ExactlyOnce {
                    let e = Error::Generic("Not qos2 but got pubrec".to_string());
                    return Err(e);
                }

                match ptype{
                    PubType::Async => {},
                    PubType::SyncDone => {
                        let rel = tt::PubRel::new(pkt.pkid);
                        rel.encode(self.get_protocol(), obuf)?;
                        trace_output!(rel);
                        self.pend_reqs.insert(rpkt.pkid, Request{pkt:ReqPacket::InnerPubRel(rel), tx:req.tx} );

                    },
                    PubType::SyncAck => {return Err(Error::Generic("expect puback but got pubrec".to_string()));},
                    PubType::SyncRec => {let _ = req.tx.unwrap().send(Ok(RspPacket::PubRec(pkt)));},   
                }
            },

            _ => {
                let msg = format!("req [{:?}] but got pubrec", req.pkt);
                return Err(Error::Generic(msg));
            }
        }
        
        Ok(())
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

        let (tx, rx) = oneshot::channel();
        let pkid = pkt.pkid;
        self.send_event(Event::PubRel(pkt, tx)).await?;
        let comp = match rx.await{
            Ok(ack) => { ack },
            Err(_) => { tt::PubComp::new(pkid) },
        };
        comp.encode(self.get_protocol(), obuf)?;
        trace_output!(comp);
        
        
        Ok(())
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

        let opt = self.pend_reqs.remove(&pkt.pkid);
        if opt.is_none(){
            return self.send_event(Event::PubComp(pkt)).await;
        }

        let req = opt.unwrap();
        match req.pkt {
            ReqPacket::InnerPubRel(_) => { 
                let _ = req.tx.unwrap().send(Ok(RspPacket::PubDone));
            },

            _ => {
                let msg = format!("req [{:?}] but got puback", req.pkt);
                return Err(Error::Generic(msg));
            }
        }
        
        Ok(())
    }

    async fn handle_incoming(&mut self, ibuf: &mut BytesMut, obuf: &mut BytesMut) -> Result<(), Error> {
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
                        tt::PacketType::ConnAck => { self.handle_connack(h, bytes, obuf).await?; }
                        
                        tt::PacketType::Publish => { self.handle_publish(h, bytes, obuf).await?; }

                        tt::PacketType::PubAck      => {self.handle_puback(h, bytes, obuf).await?;},

                        tt::PacketType::PubRec      => {self.handle_pubrec(h, bytes, obuf).await?;},

                        tt::PacketType::PubRel      => {self.handle_pubrel(h, bytes, obuf).await?;},
                        
                        tt::PacketType::PubComp     => {self.handle_pubcomp(h, bytes, obuf).await?;},

                        // tt::PacketType::Subscribe => {
                        //     self.handle_subscribe(h, bytes, obuf).await?;
                        // }

                        tt::PacketType::SubAck      => {
                            self.handle_suback(h, bytes, obuf).await?;
                        },
                        // tt::PacketType::Unsubscribe => {
                        //     self.handle_unsubscribe(h, bytes, obuf).await?;
                        // }
                        // // tt::PacketType::UnsubAck    => todo!(),
                        // tt::PacketType::PingReq => {
                        //     self.handle_pingreq(h, bytes, obuf).await?;
                        // }
                        // // tt::PacketType::PingResp    => todo!(),
                        // tt::PacketType::Disconnect => {
                        //     self.handle_disconnect(h, bytes, obuf).await?;
                        // }
                        _ => {
                            //self.handle_unexpect(packet_type).await?;
                        }
                    }
                }
            }
        }
        Ok(())
    }

    async fn response_disconnect(&mut self)-> Result<(), Error>{
        match self.pend_reqs.remove(&self.pktid) {
            Some(req) => {
                if let Some(tx) = req.tx {
                    let _ = tx.send(Ok(RspPacket::DisconnectDone));
                }
            },
            None => {
                self.send_event(Event::Closed).await?;
            },
        }
        Ok(())
    }

    async fn exec_connect(&mut self, pkt : &mut tt::Connect, obuf : &mut BytesMut) -> Result<u16, Error> {
        self.check_state(State::Ready, "exec_connect")?;
        pkt.write( obuf)?;
        trace_output!(pkt);
        self.conn_pkt = Some(pkt.clone());
        self.state = State::Connecting;
        Ok(0)
    }

    async fn exec_subscribe(&mut self, pkt : &mut tt::Subscribe, obuf : &mut BytesMut) -> Result<u16, Error> {
        self.check_state(State::Working, "exec_subscribe")?;

        if pkt.pkid == 0 {
            pkt.pkid = self.next_pktid();
        }

        pkt.encode(self.get_protocol(), obuf)?;
        trace_output!(pkt);

        Ok(pkt.pkid)
    }

    async fn exec_publish(&mut self, pkt : &mut tt::Publish, obuf : &mut BytesMut) -> Result<u16, Error> {

        self.check_state(State::Working, "exec_publish")?;

        if pkt.pkid == 0 && pkt.qos != tt::QoS::AtMostOnce {
            pkt.pkid = self.next_pktid();
        }

        pkt.encode(self.get_protocol(), obuf)?;
        trace_output!(pkt);
        
        Ok(pkt.pkid)
    }

    async fn exec_pubrel(&mut self, pkt : &mut tt::PubRel, obuf : &mut BytesMut) -> Result<u16, Error> {

        self.check_state(State::Working, "exec_pubrel")?;

        if pkt.pkid == 0 {
            pkt.pkid = self.next_pktid();
        }

        pkt.encode(self.get_protocol(), obuf)?;
        trace_output!(pkt);

        Ok(pkt.pkid)
    }

    async fn exec_disconnect(&mut self, pkt : &mut tt::Disconnect, obuf : &mut BytesMut) -> Result<u16, Error> {

        self.check_state(State::Working, "exec_disconnect")?;

        pkt.encode(self.get_protocol(), obuf)?;
        trace_output!(pkt);

        self.state = State::Disconnecting;

        Ok(self.next_pktid())
    }


    async fn exec_req(&mut self, mut req : Request, obuf : &mut BytesMut) -> Result<(), Error>{
        let r = match &mut req.pkt{
            ReqPacket::Connect(pkt) => self.exec_connect(pkt, obuf).await,
            ReqPacket::Subscribe(pkt) => self.exec_subscribe(pkt, obuf).await,
            ReqPacket::Publish(pkt, _) => self.exec_publish(pkt, obuf).await,
            ReqPacket::PubRel(pkt) => self.exec_pubrel(pkt, obuf).await,
            ReqPacket::Disconnect(pkt) => self.exec_disconnect(pkt, obuf).await,
            ReqPacket::InnerPubRel(_) => panic!("exec_req unexpect InnerPubRel"),
        };
        
        match r {
            Ok(n) => {
                self.pend_reqs.insert(n, req);
                Ok(())
            }
            Err(e) => {
                if req.tx.is_some() {
                    let msg = format!("{:?}", e);
                    let _ = req.tx.unwrap().send(Err(Error::Generic(msg)));
                }
                Err(e)
            }
        }
    }
}


async fn task_entry(
    mut socket : TcpStream, 
    mut req_rx : mpsc::Receiver<Request>,
    ev_tx : mpsc::Sender<Event>,
) -> Result<() , Error>{

    let max_osize = 16*1024;

    let (mut rd, mut wr) = socket.split();
    let mut ibuf = bytes::BytesMut::with_capacity(64);
    let mut obuf = bytes::BytesMut::with_capacity(64);
    let mut session = Session::new(ev_tx);

    loop{
        let hold_req = session.state == State::Disconnecting || obuf.len() >= max_osize;
        let hold_read = obuf.len() >= max_osize;
        
        //trace!("hold_req {}, hold_read {}", hold_req, hold_read);

        tokio::select! {
            r = wr.write_buf(&mut obuf), if !obuf.is_empty() => {
                r?;
            }

            r = rd.read_buf(&mut ibuf), if !hold_read=> {
                let len = r?;
                if len == 0 {
                    if session.state == State::Disconnecting {
                        // gracefully disconnect
                        session.response_disconnect().await?;
                        break;
                    } else {
                        let _ = session.send_event(Event::Error(Error::Broken("read disconnected".to_string()))).await;
                        return Err(Error::Broken("read disconnected".to_string()));
                    }
                }
                session.handle_incoming(&mut ibuf, &mut obuf).await?
            }

            r = req_rx.recv(), if !hold_req => {
                match r{
                    Some(req)=>{
                        session.exec_req(req, &mut obuf).await?;
                    }
                    None => {
                        debug!("no sender, closed");
                        break;
                    }, 
                }
            }
        }
    }

    Ok(())
}


pub async fn make_connection(addr : &str) -> Result<(SyncSender, Receiver) , Error>{
    debug!("make_connection");
    let socket = TcpStream::connect(addr).await?;

    let (req_tx, req_rx) = mpsc::channel(64);
    let (ev_tx, ev_rx) = mpsc::channel(64);

    tokio::spawn(async move {
        if let Err(e) = task_entry(socket, req_rx, ev_tx).await {
            debug!("task finished with error [{:?}]", e);
        }
    });

    Ok((SyncSender{tx:req_tx}, Receiver{rx:ev_rx}))
}

pub struct Receiver{
    rx : mpsc::Receiver<Event>, 
}

impl Receiver{
    pub async fn recv(&mut self) -> Result<Event, Error>{
        Ok(self.rx.recv().await.ok_or_else(|| Error::Finished)?)
    }
}

pub struct SyncSender{
    tx : mpsc::Sender<Request>, 
}

impl SyncSender{
    
    pub fn clone_async_sender(&mut self) -> AsyncSender {
        AsyncSender{tx:self.tx.clone()}
    }

    pub async fn connect(&mut self, pkt : tt::Connect) -> Result<tt::ConnAck, Error> {
        let (tx, rx) = oneshot::channel();

        if let Err(_) = self.tx.send( Request{pkt: ReqPacket::Connect(pkt), tx:Some(tx) }).await {
            return Err(Error::Finished);
        }
    
        match rx.await{
            Ok(r) => {
                let rsp = r?;
                if let RspPacket::ConnAck(pkt) = rsp { 
                    Ok(pkt)
                } else {
                    Err(Error::Generic("Not ConnAck".to_string()))
                }   
                
            },
            Err(_) => {
                Err(Error::Finished)
            },
        }
    }

    pub async fn disconnect(&mut self, pkt : tt::Disconnect) -> Result<(), Error> {
        let (tx, rx) = oneshot::channel();

        if let Err(_) = self.tx.send( Request{pkt: ReqPacket::Disconnect(pkt), tx:Some(tx) }).await {
            return Err(Error::Finished);
        }
    
        match rx.await{
            Ok(r) => {
                let rsp = r?;
                if let RspPacket::DisconnectDone = rsp { 
                    Ok(())
                } else {
                    Err(Error::Generic("Not DisconnectDone".to_string()))
                }   
                
            },
            Err(_) => {
                Err(Error::Finished)
            },
        }
    }

    pub async fn subscribe(&mut self, pkt : tt::Subscribe) -> Result<tt::SubAck, Error>{
        let (tx, rx) = oneshot::channel();

        if let Err(_) = self.tx.send( Request{pkt: ReqPacket::Subscribe(pkt), tx:Some(tx) }).await {
            return Err(Error::Finished);
        }
    
        match rx.await{
            Ok(r) => {
                let rsp = r?;
                if let RspPacket::SubAck(pkt) = rsp { 
                    Ok(pkt)
                } else {
                    Err(Error::Generic("Not ConnAck".to_string()))
                }   
                
            },
            Err(_) => {
                Err(Error::Finished)
            },
        }
        
    }

    // publish QoS0~2
    pub async fn publish(&mut self, pkt : tt::Publish)->Result<(), Error>{

        if pkt.qos == tt::QoS::AtMostOnce {
            if let Err(_) = self.tx.send( Request{pkt: ReqPacket::Publish(pkt, PubType::Async), tx:None }).await {
                return Err(Error::Finished);
            } else {
                return Ok(());
            }
        }
        
        let (tx, rx) = oneshot::channel();

        if let Err(_) = self.tx.send( Request{pkt: ReqPacket::Publish(pkt, PubType::SyncDone), tx:Some(tx) }).await {
            return Err(Error::Finished);
        } 

        match rx.await{
            Ok(r) => {
                let rsp = r?;
                if let RspPacket::PubDone = rsp { 
                    Ok(())
                } else {
                    Err(Error::Generic("Not PubDone".to_string()))
                }  
            },
            Err(_) => {
                Err(Error::Finished)
            },
        }
    }


    // publish QoS1
    pub async fn publish1(&mut self, pkt : tt::Publish)->Result<tt::PubAck, Error>{
        let (tx, rx) = oneshot::channel();

        if let Err(_) = self.tx.send( Request{pkt: ReqPacket::Publish(pkt, PubType::SyncAck), tx:Some(tx) }).await {
            return Err(Error::Finished);
        } 

        match rx.await{
            Ok(r) => {
                let rsp = r?;
                if let RspPacket::PubAck(pkt) = rsp { 
                    Ok(pkt)
                } else {
                    Err(Error::Generic("Not PubAck".to_string()))
                }  
            },
            Err(_) => {
                Err(Error::Finished)
            },
        }
    }

    // publish QoS2
    pub async fn publish2(&mut self, pkt : tt::Publish)->Result<tt::PubRec, Error>{
        let (tx, rx) = oneshot::channel();

        if let Err(_) = self.tx.send( Request{pkt: ReqPacket::Publish(pkt, PubType::SyncRec), tx:Some(tx) }).await {
            return Err(Error::Finished);
        } 

        match rx.await{
            Ok(r) => {
                let rsp = r?;
                if let RspPacket::PubRec(pkt) = rsp { 
                    Ok(pkt)
                } else {
                    Err(Error::Generic("Not PubRec".to_string()))
                }  
            },
            Err(_) => {
                Err(Error::Finished)
            },
        }
    }

    // pubrel
    pub async fn pubrel(&mut self, pkt : tt::PubRel)->Result<tt::PubComp, Error>{
        let (tx, rx) = oneshot::channel();

        if let Err(_) = self.tx.send( Request{pkt: ReqPacket::PubRel(pkt), tx:Some(tx) }).await {
            return Err(Error::Finished);
        } 

        match rx.await{
            Ok(r) => {
                let rsp = r?;
                if let RspPacket::PubComp(pkt) = rsp { 
                    Ok(pkt)
                } else {
                    Err(Error::Generic("Not PubComp".to_string()))
                }  
            },
            Err(_) => {
                Err(Error::Finished)
            },
        }
    }

}

pub struct AsyncSender{
    tx : mpsc::Sender<Request>, 
}

impl AsyncSender{
    
    pub async fn connect(&mut self, pkt : tt::Connect) -> Result<(), Error> {
        if let Err(_) = self.tx.send( Request{pkt: ReqPacket::Connect(pkt), tx:None }).await {
            return Err(Error::Finished);
        } else {
            return Ok(());
        }
    }

    pub async fn pusblish(&mut self, pkt : tt::Publish)->Result<(), Error>{
        if let Err(_) = self.tx.send( Request{pkt: ReqPacket::Publish(pkt, PubType::Async), tx:None }).await {
            return Err(Error::Finished);
        } else {
            return Ok(());
        }
    }
}
