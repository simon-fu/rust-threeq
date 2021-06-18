
use std::{sync::Arc, time::Duration};

use bytes::{BytesMut};
use tokio::{io::{AsyncWriteExt}, net::{TcpListener, TcpStream, tcp::{ReadHalf, WriteHalf}}, select, sync::broadcast, time::Instant};
use tracing::{debug, error, info, warn};




fn from_v4_lastwill(other : &mqttbytes::v4::LastWill) -> mqttbytes::v5::LastWill{
    mqttbytes::v5::LastWill{
        topic: other.topic.clone(),
        message: other.message.clone(),
        qos: other.qos,
        retain: other.retain,
        properties: None,
    }
}



mod util{
    use std::time::Duration;
    use bytes::BytesMut;
    use tokio::{io::AsyncReadExt, net::tcp::ReadHalf, time::Instant};

    pub async fn wait_for_packet<'a>(rd : &mut ReadHalf<'a>, buf : &mut BytesMut, max_packet_size: usize, timeout_millis:u64) -> std::io::Result<usize> {
        let dead_line = Instant::now() + Duration::from_millis(timeout_millis);

        loop{
            if buf.len() >= 2 {
                let r = mqttbytes::check(buf.iter(), max_packet_size);
                match r{
                    Ok(h) => return Ok(h.frame_length()),
                    Err(mqttbytes::Error::InsufficientBytes(_required)) => {},
                    Err(e) => return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string())),
                }
            }

            if Instant::now() >= dead_line {
                return Ok(0);
            }

            tokio::select! {
                _ = tokio::time::sleep_until(dead_line) =>{ }
                r = rd.read_buf(buf) => {
                    match r{
                        Ok(n) => {
                            if n == 0 {
                                return Err(std::io::Error::new(std::io::ErrorKind::BrokenPipe, "read disconnect"));
                            }
                        },
                        Err(e) => {
                            return Err(e);
                        },
                    }
                }
            }
        }
    }

    pub fn parse_protocol_level(buf : & BytesMut) -> std::io::Result<mqttbytes::Protocol>{
        // Connect Packet
        //      byte 0: packet-type:4bit, reserved:4bit
        //      byte 1: Remaining Length
        //      byte 2~3: 4
        //      byte 4~7: 'MQTT'
        //      byte 8: level
        if buf.len() < 9 {
            let error = format!("parsing protocol level: too short {}", buf.len());
                return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, error));
        }

        let protocol_level = buf[8];
        match protocol_level {
            4 => return Ok(mqttbytes::Protocol::V4),
            5 => return Ok(mqttbytes::Protocol::V5),
            num => {
                let error = format!("unknown mqtt protocol level {}", num);
                return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, error));
            },
        };
    }
    
}

mod hub{
    use std::{collections::HashMap, sync::Arc};
    use tokio::sync::RwLock;


    pub enum PubData {
        V4(mqttbytes::v4::Publish),
        V5(mqttbytes::v5::Publish),
    }

    // impl PubData{
    //     pub fn from_v4(&mut self, other : &mqttbytes::v4::Publish){
    //         self.packet.dup = other.dup;
    //         self.packet.qos = other.qos;
    //         self.packet.retain = other.retain;
    //         self.packet.topic = other.topic.clone();
    //         self.packet.pkid = other.pkid;
    //         self.packet.payload = other.payload.clone();
    //     }
    // }


    type PubSender = tokio::sync::broadcast::Sender<Arc<PubData>>;

    #[derive(Default,Debug)]
    pub struct Hub{
        // topic_filter -> senders
        subscriptions : RwLock<HashMap<String, HashMap<u64, PubSender>>>,
    }

    impl Hub {
        pub async fn subscribe(&self, topic_filter : &str, uid : u64, tx : PubSender){
            let mut map = self.subscriptions.write().await;
            if !map.contains_key(topic_filter){
                map.insert(topic_filter.to_string(), HashMap::new());
            }
            map.get_mut(topic_filter).unwrap().insert(uid, tx);
        }

        pub async fn unsubscribe(&self, topic_filter : &str, uid : u64){
            let mut map = self.subscriptions.write().await;
            if let Some(senders) = map.get_mut(topic_filter) {
                senders.remove(&uid);
            }
        }

        pub async fn publish(&self, filter:&str, d : Arc<PubData>){
            let map = self.subscriptions.read().await;
            match map.get(filter) {
                Some(senders) => {
                    for (_, tx) in senders.iter(){
                        let _ = tx.send(d.clone());
                    }
                },
                None => {}
            }
        }
    }

}

struct Session{
    hub : Arc<hub::Hub>,
    uid : u64,
    max_incoming_size : usize,
    protocol : mqttbytes::Protocol,
    keep_alive_ms : u64,
    conn_pkt : mqttbytes::v5::Connect,
    packet_id: u16,
    last_active_time : Instant,
    tx : broadcast::Sender<Arc<hub::PubData>>, 
    rx : broadcast::Receiver<Arc<hub::PubData>>,
}

impl Session {
    pub fn new(hub : Arc<hub::Hub>, uid : u64) -> Self{
        let (tx, rx) = broadcast::channel(16);

        Session{
            hub,
            uid,
            max_incoming_size : 64*1024,
            protocol : mqttbytes::Protocol::V5,
            keep_alive_ms : 30 * 1000,
            conn_pkt : mqttbytes::v5::Connect::new(""),
            packet_id : 0,
            last_active_time : Instant::now(),
            tx,
            rx, 
        }
    }

    pub fn assign_conn_pkt_v4(&mut self, pkt : &mqttbytes::v4::Connect){
        //self.conn_pkt.protocol = pkt.protocol;
        self.conn_pkt.keep_alive = pkt.keep_alive;
        self.conn_pkt.client_id = pkt.client_id.clone();
        self.conn_pkt.clean_session = pkt.clean_session;
        self.conn_pkt.last_will = if !pkt.last_will.is_none() {Some(from_v4_lastwill(pkt.last_will.as_ref().unwrap()))} else {None};
        self.conn_pkt.login = None; // TODO
        //self.conn_pkt.properties = None;

        self.protocol = mqttbytes::Protocol::V4;
        self.check_connect();
    }

    pub fn assign_conn_pkt_v5(&mut self, pkt : &mqttbytes::v5::Connect){
        self.conn_pkt = pkt.clone();
        self.protocol = mqttbytes::Protocol::V5;
        self.check_connect();
    }

    fn check_connect(&mut self){
        if self.conn_pkt.client_id.is_empty(){
            let duration = std::time::SystemTime::now().duration_since(std::time::SystemTime::UNIX_EPOCH).unwrap();
            let in_nanos = duration.as_secs() * 1_000_000 + duration.subsec_nanos() as u64;
            self.conn_pkt.client_id = format!("{}@abcdef", in_nanos); //UNIX_EPOCH
        }

        self.keep_alive_ms = self.conn_pkt.keep_alive as u64 * 1000 * 2;
        if self.keep_alive_ms == 0 {
            self.keep_alive_ms = 30*1000;
        }
    }

    fn next_packet_id(&mut self) -> u16 {
        self.packet_id += 1;
        if self.packet_id == 0 {
            self.packet_id = 1;
        }
        return self.packet_id;
    }



    // /// Reads more than 'required' bytes to frame a packet into self.read buffer
    // async fn read_bytes(&mut self, required: usize, buf : &mut BytesMut) -> std::io::Result<usize> {
    //     let mut total_read = 0;
    //     loop {
    //         let read = self.socket.read_buf(buf).await?;
    //         if 0 == read {
    //             return if buf.is_empty() {
    //                 Err(std::io::Error::new(
    //                     std::io::ErrorKind::ConnectionAborted,
    //                     "connection closed by peer",
    //                 ))
    //             } else {
    //                 Err(std::io::Error::new(
    //                     std::io::ErrorKind::ConnectionReset,
    //                     "connection reset by peer",
    //                 ))
    //             };
    //         }

    //         total_read += read;
    //         if total_read >= required {
    //             return Ok(total_read);
    //         }
    //     }
    // }

    pub async fn read_v4(&mut self, buf : &mut BytesMut) -> Result<mqttbytes::v4::Packet, std::io::Error> {
        // loop {
        //     let required = match mqttbytes::v4::read(buf, self.max_incoming_size) {
        //         Ok(packet) => return Ok(packet),
        //         Err(mqttbytes::Error::InsufficientBytes(required)) => required,
        //         Err(e) => return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string())),
        //     };

        //     // read more packets until a frame can be created. This function
        //     // blocks until a frame can be created. Use this in a select! branch
        //     self.read_bytes(required, buf).await?;
        // }
        match mqttbytes::v4::read(buf, self.max_incoming_size) {
            Ok(packet) => return Ok(packet),
            Err(e) => return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string())),
        };
    }

    pub async fn read_v5(&mut self, buf : &mut BytesMut) -> Result<mqttbytes::v5::Packet, std::io::Error> {
        // loop {
        //     let required = match mqttbytes::v5::read(buf, self.max_incoming_size) {
        //         Ok(packet) => return Ok(packet),
        //         Err(mqttbytes::Error::InsufficientBytes(required)) => required,
        //         Err(e) => return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string())),
        //     };

        //     // read more packets until a frame can be created. This function
        //     // blocks until a frame can be created. Use this in a select! branch
        //     self.read_bytes(required, buf).await?;
        // }

        match mqttbytes::v5::read(buf, self.max_incoming_size) {
            Ok(packet) => return Ok(packet),
            Err(e) => return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string())),
        };
    }

    // pub async fn read_connect_v4(&mut self, buf : &mut BytesMut) -> std::io::Result<mqttbytes::v4::Connect> {
    //     let packet = self.read_v4(buf).await?;

    //     match packet {
    //         mqttbytes::v4::Packet::Connect(connect) => Ok(connect),
    //         packet => {
    //             let error = format!("Expecting connect. Received = {:?}", packet);
    //             Err(std::io::Error::new(std::io::ErrorKind::InvalidData, error))
    //         }
    //     }
    // }

    // pub async fn read_connect_v5(&mut self, buf : &mut BytesMut) -> std::io::Result<mqttbytes::v5::Connect> {
    //     let packet = self.read_v5(buf).await?;

    //     match packet {
    //         mqttbytes::v5::Packet::Connect(connect) => Ok(connect),
    //         packet => {
    //             let error = format!("Expecting connect. Received = {:?}", packet);
    //             Err(std::io::Error::new(std::io::ErrorKind::InvalidData, error))
    //         }
    //     }
    // }

    // pub async fn read_protocol_level(&mut self, buf : &mut BytesMut) -> std::io::Result<mqttbytes::Protocol>{
    //     // Connect Packet
    //     //      byte 0: packet-type:4bit, reserved:4bit
    //     //      byte 1: Remaining Length
    //     //      byte 2~3: 4
    //     //      byte 4~7: 'MQTT'
    //     //      byte 8: level
    //     let _ = self.read_bytes(9, buf).await?;
    //     let protocol_level = buf[8];
    //     match protocol_level {
    //         4 => return Ok(mqttbytes::Protocol::V4),
    //         5 => return Ok(mqttbytes::Protocol::V5),
    //         num => {
    //             let error = format!("unknown mqtt protocol level {}", num);
    //             return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, error));
    //             //return Err(mqttbytes::Error::InvalidProtocolLevel(num));
    //         },
    //     };
    // }

    // pub async fn connack_v4(&mut self, connack: mqttbytes::v4::ConnAck) -> Result<usize, std::io::Error> {
    //     let mut write = BytesMut::new();
    //     let len = match connack.write(&mut write) {
    //         Ok(size) => size,
    //         Err(e) => return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string())),
    //     };

    //     self.socket.write_all(&write[..]).await?;
    //     Ok(len)
    // }

    // pub async fn connack_v5(&mut self, connack: mqttbytes::v5::ConnAck) -> Result<usize, std::io::Error> {
    //     let mut write = BytesMut::new();
    //     let len = match connack.write(&mut write) {
    //         Ok(size) => size,
    //         Err(e) => return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string())),
    //     };

    //     self.socket.write_all(&write[..]).await?;
    //     Ok(len)
    // }


    // pub async fn pingresp_v4(&mut self, pkt: mqttbytes::v4::PingResp) -> Result<usize, std::io::Error> {
    //     let mut write = BytesMut::new();
    //     let len = match pkt.write(&mut write) {
    //         Ok(size) => size,
    //         Err(e) => return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string())),
    //     };

    //     self.socket.write_all(&write[..]).await?;
    //     Ok(len)
    // }

    // pub async fn pingresp_v5(&mut self, pkt: mqttbytes::v5::PingResp) -> Result<usize, std::io::Error> {
    //     let mut write = BytesMut::new();
    //     let len = match pkt.write(&mut write) {
    //         Ok(size) => size,
    //         Err(e) => return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string())),
    //     };

    //     self.socket.write_all(&write[..]).await?;
    //     Ok(len)
    // }
    
    fn check_alive(& self) -> std::io::Result<u64> {
        let now = Instant::now();
        let elapsed = {
            if now > self.last_active_time {
                (now - self.last_active_time).as_millis()
            } else{
                0
            }
         } as u64;

        if self.keep_alive_ms > elapsed { 
            return Ok(self.keep_alive_ms - elapsed); 
        } else { 
            let error  = format!("keep alive timeout {} millis", self.keep_alive_ms);
            return Err(std::io::Error::new(std::io::ErrorKind::TimedOut, error));
        } 
    }

    pub async fn run(&mut self, mut socket : TcpStream) -> std::io::Result<()> {
        //socket.set_nodelay(true)?; // TODO:

        let (mut rd, mut wr) = socket.split();

        // recv first connect packet
        {
            let mut ibuf = BytesMut::new();
            let mut obuf = BytesMut::new();

            self.protocol = {
                let _ = util::wait_for_packet(&mut rd, &mut ibuf, self.max_incoming_size, self.keep_alive_ms).await?;
                util::parse_protocol_level(&ibuf)?
            };
            self.last_active_time = Instant::now();
            // TODO: auth

            match self.protocol {
                mqttbytes::Protocol::V4 => { 
                    let pkt = self.read_v4(&mut ibuf).await?;
                    self.process_v4(pkt, &mut obuf).await?;
                 },
    
                mqttbytes::Protocol::V5 => { 
                    let pkt = self.read_v5(&mut ibuf).await?;
                    self.process_v5(pkt, &mut obuf).await?;
                },
            }
            wr.write_all(&mut obuf).await?;
            self.last_active_time = Instant::now();
        }
        


        // let _ = tokio::time::timeout(Duration::from_millis(self.keep_alive_ms), async {
        //     let mut obuf = BytesMut::new();
        //     match self.protocol {
        //         mqttbytes::Protocol::V4 => { 
        //             let connack = mqttbytes::v4::ConnAck::new(mqttbytes::v4::ConnectReturnCode::Success, false);
        //             let _ = connack.write(&mut obuf);
        //          },

        //         mqttbytes::Protocol::V5 => { 
        //             let mut connack = mqttbytes::v5::ConnAck::new(mqttbytes::v5::ConnectReturnCode::Success, false);
        //             connack.properties = Some(mqttbytes::v5::ConnAckProperties::new());
        //             let _ = connack.write(&mut obuf);
        //         },
        //     }
        //     wr.write_all(&obuf).await?;
        //     return Ok::<(), std::io::Error>(())
        // }).await??;
        // self.last_active_time = Instant::now();

        loop{
            let rd_timeout = self.check_alive()?;
            let dead_line = Instant::now() + Duration::from_millis(rd_timeout);
            let mut dump_buf = [0u8;1];

            select! {
                
                //r = socket.ready(Interest::READABLE) =>{
                r = rd.peek(&mut dump_buf) =>{
                    match r{
                        Ok(_) => {
                            match self.protocol {
                                mqttbytes::Protocol::V4 => { self.run_v4(&mut rd, &mut wr).await?; },
                                mqttbytes::Protocol::V5 => { self.run_v5(&mut rd, &mut wr).await?; },
                            }
                        },
                        Err(e) => {
                            return Err(e);
                        },
                    }
                    
                }

                _ = tokio::time::sleep_until(dead_line) => {

                }
            }
        }
    }

    // pub async fn read_connect(&mut self, rd : &mut ReadHalf<'a>) -> std::io::Result<()> {

    //     let mut ibuf = BytesMut::new();
    //     let _ = util::wait_for_packet(&mut rd, &mut ibuf, self.max_incoming_size, self.keep_alive_ms).await?;
    //     let protocol = util::parse_protocol_level(&ibuf)?;
        
    //     //let protocol = self.read_protocol_level(&mut ibuf).await?;

    //     match protocol {

    //         mqttbytes::Protocol::V4 => {
    //             let pkt = self.read_connect_v4(&mut ibuf).await?;
    //             debug!("got v4 first {:?}", pkt);
    //             self.assign_conn_pkt(&pkt);
    //         },

    //         mqttbytes::Protocol::V5 => {
    //             self.conn_pkt = self.read_connect_v5(&mut ibuf).await?;
    //             debug!("got v5 first {:?}", self.conn_pkt);
    //         },
    //     }
    //     return Ok(());
    // }

    pub async fn run_v4<'a>(&mut self, rd : &mut ReadHalf<'a>, wr : &mut WriteHalf<'a>) -> std::io::Result<()> {
        let mut ibuf = BytesMut::with_capacity(1);
        let mut obuf = BytesMut::new();
        
        loop{
            let rd_timeout = self.check_alive()?;
            let rd_timeout = if rd_timeout < 5000 {rd_timeout} else { 5000 };
            let mut pkt_len  = usize::MAX;

            {
                let wait_action = util::wait_for_packet(rd, &mut ibuf, self.max_incoming_size, rd_timeout);
                tokio::pin!(wait_action);
                
                select!{
                    r = &mut wait_action, if rd_timeout > 0 =>{
                        match r{
                            Ok(n) => { 
                                pkt_len = n;
                            },
                            Err(e) => { return Err(e); },
                        }
                    }
    
                    r = wr.write_buf(&mut obuf), if obuf.len() > 0 => {
                        match r{
                            Ok(_) => { },
                            Err(e) => { return Err(e); },
                        }
                    }

                    r = self.rx.recv() =>{
                        match r{
                            Ok(d) => {
                                let mut pub_pkt = match &*d{
                                    hub::PubData::V4(pkt) => {
                                        mqttbytes::v4::Publish::from_bytes(&pkt.topic, mqttbytes::QoS::AtMostOnce, pkt.payload.clone())
                                    },
                                    hub::PubData::V5(pkt) => {
                                        mqttbytes::v4::Publish::from_bytes(&pkt.topic, mqttbytes::QoS::AtMostOnce, pkt.payload.clone())
                                    },
                                };
                                pub_pkt.pkid = self.next_packet_id();
                                let _ = pub_pkt.write(&mut obuf);
                                
                                self.last_active_time = Instant::now();
                            },
                            Err(broadcast::error::RecvError::Lagged(n)) => {
                                info!("lagged {}", n); 
                                //while let Ok(d) = self.rx.try_recv() { }
                            },
                            Err(_) => {

                            }
                        }
                    }
                }

            }
            
            if pkt_len == 0 {
                if ibuf.len() == 0 && obuf.len() == 0{
                    debug!("v4 no data, sleep");
                    return Ok(());
                } else {
                    continue;
                }
            } else if pkt_len > 0 && pkt_len < usize::MAX{
                self.last_active_time = Instant::now();
                let pkt = self.read_v4(&mut ibuf).await?;
                self.process_v4(pkt, &mut obuf).await?;
            }
        }
    }

    async fn process_v4(&mut self, pkt : mqttbytes::v4::Packet, obuf : &mut BytesMut) -> std::io::Result<()>{
        debug!("process v4 packet {:?}", pkt);

        match pkt{
            mqttbytes::v4::Packet::Connect(packet) => {
                if self.conn_pkt.client_id.is_empty(){
                    debug!("got v4 first {:?}", packet);
                    self.assign_conn_pkt_v4(&packet);
                } else {
                    warn!("error v4 {:?}", packet);
                }

                let connack = mqttbytes::v4::ConnAck::new(mqttbytes::v4::ConnectReturnCode::Success, false);
                let _ = connack.write(obuf);
            }

            mqttbytes::v4::Packet::ConnAck(_ack) => {
                todo!();
            }

            mqttbytes::v4::Packet::Publish(packet) => {
                match packet.qos {
                    mqttbytes::QoS::AtMostOnce => {

                    },
                    mqttbytes::QoS::AtLeastOnce => {
                        let ack = mqttbytes::v4::PubAck::new(packet.pkid);
                        let _ = ack.write(obuf);
                    },
                    mqttbytes::QoS::ExactlyOnce => {
                        
                    },
                }
                self.hub.publish(&packet.topic, Arc::new(hub::PubData::V4(packet.clone()))).await;
            },

            mqttbytes::v4::Packet::PubAck(_ack) => {
                todo!();
            }

            mqttbytes::v4::Packet::PubRec(_rec) => {
                todo!();
            }

            mqttbytes::v4::Packet::PubRel(_rel) => {
                todo!();
            }

            mqttbytes::v4::Packet::PubComp(_comp) => {
                todo!();
            }

            mqttbytes::v4::Packet::Subscribe(subsr) => {
                let mut return_codes:Vec<mqttbytes::v4::SubscribeReasonCode> = Vec::new();
                for val in subsr.filters.iter() {
                    self.hub.subscribe(&val.path, self.uid, self.tx.clone()).await;
                    return_codes.push(mqttbytes::v4::SubscribeReasonCode::Success(val.qos));
                }
                let pkt = mqttbytes::v4::SubAck::new(subsr.pkid, return_codes);
                if let Err(e) = pkt.write(obuf) {
                    return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()));
                }
            },

            mqttbytes::v4::Packet::Unsubscribe(unsubsr) => {
                for topic in unsubsr.topics.iter(){
                    self.hub.unsubscribe(topic, self.uid).await;
                }

                let pkt = mqttbytes::v4::UnsubAck::new(unsubsr.pkid);
                if let Err(e) = pkt.write(obuf) {
                    return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()));
                }
            },

            mqttbytes::v4::Packet::PingReq => {
                let pkt = mqttbytes::v4::PingResp{};
                if let Err(e) = pkt.write(obuf) {
                    return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()));
                }
            },

            mqttbytes::v4::Packet::Disconnect => {
                return Err(std::io::Error::new(std::io::ErrorKind::Other, "client disconnect"));
            },

            pkt => {
                let error = format!("recv unexpect packet [{:?}]", pkt);
                return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, error));
            }
        }


        return Ok(());
    }

    pub async fn run_v5<'a>( &mut self, rd : &mut ReadHalf<'a>, wr : &mut WriteHalf<'a> ) -> std::io::Result<()> {
        let mut ibuf = BytesMut::with_capacity(1);
        let mut obuf = BytesMut::new();
        
        loop{
            let rd_timeout = self.check_alive()?;
            let rd_timeout = if rd_timeout < 5000 {rd_timeout} else { 5000 };
            let mut pkt_len  = usize::MAX;

            {
                let wait_action = util::wait_for_packet(rd, &mut ibuf, self.max_incoming_size, rd_timeout);
                tokio::pin!(wait_action);
                
                select!{
                    r = &mut wait_action, if rd_timeout > 0 =>{
                        match r{
                            Ok(n) => { 
                                pkt_len = n;
                            },
                            Err(e) => { return Err(e); },
                        }
                    }
    
                    r = wr.write_buf(&mut obuf), if obuf.len() > 0 => {
                        match r{
                            Ok(_) => { },
                            Err(e) => { return Err(e); },
                        }
                    }

                    r = self.rx.recv() =>{
                        match r{
                            Ok(d) => {
                                let mut pub_pkt = match &*d{
                                    hub::PubData::V4(pkt) => {
                                        mqttbytes::v5::Publish::from_bytes(&pkt.topic, mqttbytes::QoS::AtMostOnce, pkt.payload.clone())
                                    },
                                    hub::PubData::V5(pkt) => {
                                        mqttbytes::v5::Publish::from_bytes(&pkt.topic, mqttbytes::QoS::AtMostOnce, pkt.payload.clone())
                                    },
                                };
                                pub_pkt.pkid = self.next_packet_id();
                                let _ = pub_pkt.write(&mut obuf);
                            },
                            Err(broadcast::error::RecvError::Lagged(n)) => {
                                info!("lagged {}", n); 
                                //while let Ok(d) = self.rx.try_recv() { }
                            },
                            Err(_) => {

                            }
                        }
                    }
                }

            }

            if pkt_len == 0 {
                if ibuf.len() == 0 && obuf.len() == 0{
                    debug!("v5 no data, sleep");
                    return Ok(());
                } else {
                    continue;
                }
            } else if pkt_len > 0 && pkt_len < usize::MAX{
                self.last_active_time = Instant::now();
                let pkt = self.read_v5(&mut ibuf).await?;
                self.process_v5(pkt, &mut obuf).await?;
            }
        }
        // return Ok(());
    }

    async fn process_v5(&mut self, pkt : mqttbytes::v5::Packet, obuf : &mut BytesMut) -> std::io::Result<()>{
        debug!("process v5 packet {:?}", pkt);

        match pkt{
            mqttbytes::v5::Packet::Connect(packet) => {
                if self.conn_pkt.client_id.is_empty(){
                    debug!("got v5 first {:?}", packet);
                    self.assign_conn_pkt_v5(&packet);
                } else {
                    warn!("error v5 {:?}", packet);
                }

                let mut connack = mqttbytes::v5::ConnAck::new(mqttbytes::v5::ConnectReturnCode::Success, false);
                connack.properties = Some(mqttbytes::v5::ConnAckProperties::new());
                if packet.client_id.is_empty() {
                    connack.properties.as_mut().unwrap().assigned_client_identifier = Some(self.conn_pkt.client_id.clone());
                }
                let _ = connack.write(obuf);
            }

            mqttbytes::v5::Packet::ConnAck(_ack) => {
                todo!();
            }

            mqttbytes::v5::Packet::Publish(packet) => {
                match packet.qos {
                    mqttbytes::QoS::AtMostOnce => {

                    },
                    mqttbytes::QoS::AtLeastOnce => {
                        let ack = mqttbytes::v5::PubAck::new(packet.pkid);
                        let _ = ack.write(obuf);
                    },
                    mqttbytes::QoS::ExactlyOnce => {
                        
                    },
                }
                self.hub.publish(&packet.topic, Arc::new(hub::PubData::V5(packet.clone()))).await;
            },

            mqttbytes::v5::Packet::PubAck(_ack) => {
                todo!();
            }

            mqttbytes::v5::Packet::PubRec(_rec) => {
                todo!();
            }

            mqttbytes::v5::Packet::PubRel(_rel) => {
                todo!();
            }

            mqttbytes::v5::Packet::PubComp(_comp) => {
                todo!();
            }

            mqttbytes::v5::Packet::Subscribe(subsr) => {
                let mut return_codes:Vec<mqttbytes::v5::SubscribeReasonCode> = Vec::new();
                for val in subsr.filters.iter() {
                    self.hub.subscribe(&val.path, self.uid, self.tx.clone()).await;
                    return_codes.push(mqttbytes::v5::SubscribeReasonCode::QoS0);
                }
                let pkt = mqttbytes::v5::SubAck::new(subsr.pkid, return_codes);
                if let Err(e) = pkt.write(obuf) {
                    return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()));
                }
            },

            mqttbytes::v5::Packet::Unsubscribe(unsubsr) => {
                for topic in unsubsr.filters.iter(){
                    self.hub.unsubscribe(topic, self.uid).await;
                }

                let pkt = mqttbytes::v5::UnsubAck::new(unsubsr.pkid);
                if let Err(e) = pkt.write(obuf) {
                    return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()));
                }
            },

            mqttbytes::v5::Packet::PingReq => {
                let pkt = mqttbytes::v5::PingResp{};
                if let Err(e) = pkt.write(obuf) {
                    return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()));
                }
            },

            mqttbytes::v5::Packet::Disconnect(packet) => {
                return Err(std::io::Error::new(std::io::ErrorKind::Other, format!("client disconnect, {:?}", packet)));
            },

            pkt => {
                let error = format!("recv unexpect packet [{:?}]", pkt);
                return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, error));
            }
        }


        return Ok(());
    }

}


async fn run_server() -> core::result::Result<(), Box<dyn std::error::Error>>{
    let address = "0.0.0.0:1883";
    let listener = TcpListener::bind(address).await?;
    info!("mqtt tcp server listening on {}", address);

    let hub = Arc::new(hub::Hub::default());
    let mut uid = 0;

    loop {

        tokio::select! {
            result = listener.accept() => {
                match result{
                    Ok((socket, _)) => {
                        debug!("connected from {:?}", socket.peer_addr().unwrap());
                        let hub0 = hub.clone();
                        uid += 1;
                        tokio::spawn(async move {
                            let mut session = Session::new(hub0, uid);
                            if let Err(e) = session.run(socket).await {
                                debug!("session finished error [{}]", e);
                            }
                        });
                    },
                    Err(e) => {
                        error!("listener accept error {}", e);
                        return Err(Box::new(e));
                    }, 
                } 
            }

        };
    }
}


// #[ntex::main]
#[tokio::main]
async fn main() {

    use tracing_subscriber::EnvFilter;
    
    let env_filter = if std::env::var(EnvFilter::DEFAULT_ENV).is_ok() {
        EnvFilter::from_default_env()
    } else {
        EnvFilter::new("info")
    };

    tracing_subscriber::fmt()
    .with_target(false)
    .with_env_filter(env_filter)
    .init();

    let _ = run_server().await;

}

