
/*
√ basic
  - connect
  - subscribe
  - publish m1, and receive m1 
  - unsubscribe, and receive timeout

√ two concurrent client with the same client id
  - user 1: 
    - connect with client id c1
    - subscribe
  - user 2
    - connect with client id c1
  - user 1 got broken(V4) or disconnect(V5)

- ping interval

- clean session basic
  - user 1
    - connect with clean-session=0
    - subscribe 
    - publish, and receive 
    - disconnect
  - user 2 (op1)
    - connect with clean-session=1
    - publish m1
    - disconnect
  - user 1
    - connect with clean-session=0
    - receive m1
    - disconnect
  - user 2 (same as op1)
  - user 1
    - connect with clean-session=1
    - receive m1 timeout
    - disconnect

- offline message limit

- will message
  - user 1: connect and subscribe
  - user 2: connect with will m1 and shutdown socket
  - user 1: receive m1
  - user 3: connect and subscribe, recevie timeout

- retain message
  - user 1: connect and publish m1 with retain
  - user 2: connect and subscribe and recevie m1 with retain
  - user 1: publish m2 without retain
  - user 2: receive m2 without retain
  - user 1: publish m3 with retain
  - user 2: receive m3 with retain
  - user 2: disconnect
  - user 1: publish m4, m5, m6, all with retain
  - user 2: connect and recevie m6, with retain
  - user 2: disconnect
  - user 1: publish m7(without retain), m8(with retain), m9(without retain)
  - user 2: connect and recevie m8, with retain
 */


use std::time::Duration;

use clap::Clap;
use rand::{distributions::Alphanumeric, Rng};
use rust_threeq::tq3;
use rust_threeq::tq3::tt;
use tokio::time::timeout;
use tracing::{Instrument, Span, debug, error, info, instrument};

#[macro_use]
extern crate serde_derive;

#[derive(Clap, Debug, Default)]
#[clap(name = "threeq verify", author, about, version)]
struct CmdArgs {
    // #[clap(
    //     short = 'a',
    //     long = "address",
    //     default_value = "127.0.0.1:1883",
    //     long_about = "broker address, ip:port."
    // )]
    // address: String,

    #[clap(
        short = 'c',
        long = "config",
        long_about = "config file."
    )]
    config: Option<String>,
}


#[derive(Debug, Deserialize, Serialize, Clone, Default)]
struct Account {
    user: Option<String>,
    password: Option<String>,
    client_id: Option<String>,
}

#[derive(Debug, Default, Deserialize, Serialize)]
struct Config {
    address: Option<String>,
    accounts: Vec<Account>,
    timeout_seconds: u64,
}


#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("{0}")]
    ClientError(#[from] tt::client::Error),

    #[error("error: {0}")]
    Generic(String),
}

const STAR_VAR: &str = "${*}";


struct AccountIter<'a>{
    iter : std::slice::Iter<'a, Account>,
    star : Option<&'a Account>,
}

impl AccountIter<'_> {
    fn new(accounts: &Vec<Account>) -> AccountIter{
        let mut o = AccountIter{
            iter: accounts.iter(),
            star: None,
        };

        for v in accounts {
            if let Some(s) = &v.client_id {
                if s.contains(STAR_VAR){
                    o.star = Some(v);
                    break;
                }
            }
        };

        o
    }
}

impl<'a> Iterator for AccountIter<'a> {
    type Item = Account;
    fn next(&mut self) -> Option<Account> {
        while let Some(a) = self.iter.next() {
            if let Some(b) = self.star {
                if a as *const Account != b as *const Account {
                    return Some(a.clone());
                } else {
                    break;
                }
            }
        }

        if let Some(star) = self.star {
            let mut account = star.clone();
            if let Some(str) = account.client_id.as_mut() {
                *str = str.replace(STAR_VAR, &rand_client_id());
            }
            return Some(account);
        }

        None
    }
}



fn rand_client_id() -> String{    
    rand::thread_rng()
            .sample_iter(&Alphanumeric)
            .take(8)
            .map(char::from)
            .collect()
}


fn init_conn_pkt(account: &Account, protocol: tt::Protocol) -> tt::Connect{
    let mut pkt = tt::Connect::new(account.client_id.as_deref().unwrap_or(&rand_client_id()));
    if account.user.is_some() || account.password.is_some() {
        pkt.set_login(
            account.user.as_deref().unwrap_or(""), 
            account.password.as_deref().unwrap_or("")
        );
    }
    pkt.protocol = protocol;
    pkt
}

fn check_publish(rpkt : &tt::Publish, pkt : &tt::Publish) -> Result<(), String>{
    if pkt.qos != rpkt.qos {
        Err("diff qos".to_string())
    } else if pkt.topic != rpkt.topic {
        Err("diff topic".to_string())
    } else if pkt.payload != rpkt.payload {
        Err("diff payload".to_string())
    } else if pkt.retain != rpkt.retain {
        Err("diff retain".to_string())
    } else if pkt.dup != rpkt.dup {
        Err("diff dup".to_string())
    } else if pkt.properties != rpkt.properties {
        Err("diff properties".to_string())
    } else {
        Ok(())
    }
}


fn new_span(s: &str) -> Span {
    let span = tracing::span!(tracing::Level::DEBUG, "", s=s);
    return span;
}

struct Connector<'a>{
    args: &'a VArgs, 
    pkt: tt::Connect, 
    client: Option<tt::client::Client>,
}

impl<'a> Connector<'a> {
    fn new(args: &'a VArgs, account:&'a Account,) -> Self {
        Self{
            args,
            pkt: init_conn_pkt(account, args.protocol),
            client: None,
        }
    }

    fn with_clean_session(mut self, b:bool) -> Self{
        self.pkt.clean_session = b;
        self
    }

    fn with_session_expire(mut self, seconds:u32) -> Self{
        if seconds > 0 {
            if self.pkt.properties.is_none() {
                self.pkt.properties = Some(tt::ConnectProperties::new());
            }
            self.pkt.properties.as_mut().unwrap().session_expiry_interval = Some(seconds);
            // self.pkt.clean_session = false;
        } else {
            if self.pkt.properties.is_some() {
                self.pkt.properties.as_mut().unwrap().session_expiry_interval = None;
            }
            // self.pkt.clean_session = true;
        }
        
        self
    }

    fn with_will(mut self, retain:bool) -> Self{
        self.pkt.last_will = Some(tt::LastWill::new(&self.args.topic, &*self.args.payload, self.args.qos, retain));
        self
    }


    async fn connect(&mut self) -> Result<(), Error> {
        let mut client = tt::client::make_connection(&self.args.addr).await?;
        let ack = client.sender.connect(self.pkt.clone()).await?;
        if ack.code != tt::ConnectReturnCode::Success {
            return Err(Error::Generic(format!("{:?}", ack)));
        }
        self.client = Some(client);
        return Ok(());
    }

    async fn subscribe(&mut self) -> Result<(), Error> {
        let client = self.client.as_mut().unwrap();
        let ack = client.sender.subscribe(tt::Subscribe::new(&self.args.topic, self.args.qos)).await?;
        for reason in &ack.return_codes {
            if !reason.is_success() {
                return Err(Error::Generic(format!("{:?}", ack)));
            }
        }
        return Ok(());
    }

    async fn unsubscribe(&mut self) -> Result<(), Error> {
        let client = self.client.as_mut().unwrap();
        let ack = client.sender.unsubscribe(tt::Unsubscribe::new(self.args.topic.clone())).await?;
        for reason in &ack.reasons {
            if *reason != tt::UnsubAckReason::Success{
                let str = format!("unsubscribe fail, {:?}", ack);
                error!("{}", str);
                return Err(Error::Generic(str));
            }
        }
        return Ok(());
    }

    async fn publish(&mut self) -> Result<(), Error>{
        let client = self.client.as_mut().unwrap();
        let _r = client.sender.publish(tt::Publish::new(&self.args.topic, self.args.qos, self.args.payload.clone())).await?;
        Ok(())
    }

    async fn publish1(&mut self, pkt: tt::Publish) -> Result<(), Error>{
        let client = self.client.as_mut().unwrap();
        let _r = client.sender.publish(pkt).await?;
        Ok(())
    }

    async fn recv(&mut self) -> Result<tt::client::Event, tt::client::Error> {
        let client = self.client.as_mut().unwrap();
        let r = timeout(self.args.timeout, client.receiver.recv()).await;
        let ev = match r{
            Ok(r) => {
                Ok(r)
            },
            Err(e) => {
                error!("recv timeout");
                Err(tt::client::Error::Timeout(e))
            },
        }?;
        return ev;
    }

    async fn recv_timeout(&mut self) -> Result<(), Error> {
        let client = self.client.as_mut().unwrap();
        return match timeout(self.args.timeout, client.receiver.recv()).await {
            Ok(_r) => {
                error!("recv timeout");
                Err(Error::Generic(format!("recv timeout")))
            },
            Err(_) => {
                Ok(())
            },
        };
    }

    async fn recv_publish0(&mut self) -> Result<(), Error> {
        let rpkt = tt::Publish::new(self.args.topic.clone(), self.args.qos, self.args.payload.clone());
        return self.recv_publish1(&rpkt).await;
    }

    async fn recv_publish1 (&mut self, rpkt : &tt::Publish) -> Result<(), Error> {
        let ev = self.recv().await?;
    
        match &ev {
            tt::client::Event::Packet(pkt) => {
                match pkt {
                    tt::Packet::Publish(pkt) => {
                        let r = check_publish(rpkt, pkt);
                        return match r{
                            Ok(_) => Ok(()),
                            Err(s) => {
                                let str = format!("expect recv {:?}, but {:?}, reason {}", rpkt, pkt, s);
                                error!("{}", str);
                                Err(Error::Generic(str))
                            },
                        };
                    },
                    _ => {},
                }
            },
            tt::client::Event::Closed(_) => {
    
            },
        }
        return Err(Error::Generic(format!("expect publish but got {:?}", ev)));
    }

    async fn disconnect(&mut self) -> Result<(), Error> {
        let client = self.client.as_mut().unwrap();
        let _r = client.sender.disconnect(tt::Disconnect::new()).await?;
        Ok(())
    }

    async fn shutdown(&mut self) -> Result<(), Error> {
        let client = self.client.as_mut().unwrap();
        let _r = client.sender.shutdown().await?;
        Ok(())
    }
}

struct Message{
    pkt: tt::Publish,   
}

impl Message {
    fn new(args: &VArgs) -> Self {
        Self{
            pkt: tt::Publish::new(args.topic.clone(), args.qos, args.payload.clone())
        }
    }

    fn with_payload(mut self, payload: Vec<u8>) -> Self{
        self.pkt.payload = payload.into();
        self
    }

    fn with_retain(mut self) -> Self{
        self.pkt.retain = true;
        self
    }
}

struct VArgs{
    addr: String,
    protocol: tt::Protocol,
    topic: String,
    qos: tt::QoS,
    timeout: Duration,
    payload: Vec<u8>,
}

#[instrument(skip(args, accounts), level = "debug")]
async fn clean_up(args: &VArgs, mut accounts: AccountIter<'_>) -> Result<(), Error> {

    let account = accounts.next().unwrap();
    
    let mut client=  Connector::new(args, &account);

    client.connect().await?;

    let m1 = Message::new(args).with_payload(vec![]).with_retain();
    client.publish1(m1.pkt).await?;

    client.disconnect().await?;

    client.recv().await?;

    Ok(())
}

#[instrument(skip(args, accounts), level = "debug")]
async fn verify_basic(args: &VArgs, mut accounts: AccountIter<'_>) -> Result<(), Error> {

    let account = accounts.next().unwrap();
    
    let mut client=  Connector::new(args, &account);

    client.connect().await?;
    
    client.subscribe().await?;

    client.publish().await?;

    client.recv_publish0().await?;

    client.unsubscribe().await?;

    client.publish().await?;

    client.recv_timeout().instrument(new_span("recv no message after unsubscribe")).await?;

    client.disconnect().await?;

    client.recv().await?;

    Ok(())
}

#[instrument(skip(args, accounts), level = "debug")]
async fn verify_same_client_id(args: &VArgs, mut accounts: AccountIter<'_>) -> Result<(), Error>{
    let account = accounts.next().unwrap();
    
    let mut client1=  Connector::new(args, &account);
    
    client1.connect().await?;

    client1.subscribe().await?;


    let mut client2 = Connector::new(args, &account);

    client2.connect().await?;

    let r = client1.recv().instrument(new_span("client1 waiting for disconnect")).await;

    client2.disconnect().await?;

    match args.protocol {
        tt::Protocol::V4 => {
            if r.is_err() {
                if let tt::client::Error::Broken(_) = r.as_ref().unwrap_err() {
                    return Ok(());
                }
            }
            return Err(Error::Generic(format!("expect broken but got {:?}", r)));
        },
        tt::Protocol::V5 => {
            if let Ok(ev) = &r {
                if let tt::client::Event::Packet(pkt) = ev {
                    if let tt::Packet::Disconnect(_) = pkt {
                        return Ok(());
                    }
                }
            } 
            return Err(Error::Generic(format!("expect disconnect but got {:?}", r)));
        },
    }
}

#[instrument(skip(args, accounts), level = "debug")]
async fn verify_clean_session(args: &VArgs, mut accounts: AccountIter<'_>) -> Result<(), Error>{
    /* 
    - clean session basic
    - user 1
        - connect with clean-session=0
        - subscribe 
        - publish, and receive 
        - disconnect
    - user 2 (op1)
        - connect with clean-session=1
        - publish m1
        - disconnect
    - user 1
        - connect with clean-session=0
        - receive m1
        - disconnect
    - user 2 (same as op1)
    - user 1
        - connect with clean-session=1
        - receive m1 timeout
        - disconnect
    */

    let user1 = accounts.next().unwrap();
    let user2 = accounts.next().unwrap();
    let expired_seconds = 300u32;

    // user 1 connect with clean-session 0, subscribe and disconnect
    {
        let mut client1=  Connector::new( 
            args, &user1)
            .with_clean_session(false)
            .with_session_expire(expired_seconds);

        client1.connect().await?;
    
        client1.subscribe().await?;

        client1.publish().await?;

        client1.recv_publish0().instrument(new_span("recving online")).await?;

        client1.disconnect().await?;
    }
    
    // user2 connect and send message
    let mut client2=  Connector::new(args, &user2);

    client2.connect().await?;

    client2.publish().await?;

    // user1 connect again and got offline m1
    {
        let mut client1=  Connector::new( 
            args, &user1)
            .with_clean_session(false)
            .with_session_expire(expired_seconds);

        client1.connect().await?;
    
        client1.recv_publish0().instrument(new_span("recving offline")).await?;

        client1.disconnect().await?;
    }

    // user2 send message

    client2.publish().await?;

    // user1 connect with clean-sesison 1, got nothing
    {
        let mut client1=  Connector::new( args, &user1);

        client1.connect().await?;
    
        client1.recv_timeout().instrument(new_span("recv nothing")).await?;

        client1.disconnect().await?;
    }

    client2.disconnect().await?;

    Ok(())
}

#[instrument(skip(args, accounts), level = "debug")]
async fn verify_retain(args: &VArgs, mut accounts: AccountIter<'_>) -> Result<(), Error>{
    /*
        - retain message
            - user 1: connect and publish m1 with retain, m2 without retain, disconnect
            - user 2: connect and subscribe and recevie m1 with retain
            - user 3: connect and subscribe and recevie m1 with retain

            - user 1: connect and publish m3 with retain
            - user 2: receive m3 with retain
            - user 3: receive m3 with retain
            - user 3: disconnect

            - user 1: publish m4, m5, m6, all with retain
            - user 2: receive m4, m5, m6, with retain
            - user 3: connect and recevie m6, with retain
            - user 3: disconnect

            - user 1: publish m7(without retain), m8(with retain), m9(without retain)
            - user 2: receive m7(without retain), m8(with retain), m9(without retain)
            - user 3: connect and recevie m8, with retain
    */

    let user1 = accounts.next().unwrap();
    let user2 = accounts.next().unwrap();
    let user3 = accounts.next().unwrap();
    
    let m1 = Message::new(args).with_payload(vec![0x01u8; 1]).with_retain();
    let m2 = Message::new(args).with_payload(vec![0x02u8; 2]);
    let m3 = Message::new(args).with_payload(vec![0x03u8; 3]).with_retain();
    let m4 = Message::new(args).with_payload(vec![0x04u8; 4]).with_retain();
    let m5 = Message::new(args).with_payload(vec![0x05u8; 5]).with_retain();
    let m6 = Message::new(args).with_payload(vec![0x06u8; 6]).with_retain();
    let m7 = Message::new(args).with_payload(vec![0x07u8; 7]);
    let m8 = Message::new(args).with_payload(vec![0x08u8; 8]).with_retain();
    let m9 = Message::new(args).with_payload(vec![0x09u8; 9]);
    let m0 = Message::new(args).with_payload(vec![]).with_retain();
    
    // {
    //     let mut client2=  Connector::new( args, &user2);
        
    //     client2.connect().await?;

    //     client2.subscribe().await?;

    //     let mut client1=  Connector::new( args, &user1);
        
    //     client1.connect().await?;

    //     client1.publish1(m3.pkt.clone()).await?;

    //     client1.disconnect().await?;

    //     client2.recv_publish1(&m3.pkt).await?;

    //     client2.disconnect().await?;

    //     if !client2.args.addr.is_empty(){
    //         return Ok(());
    //     }
    // }


    {
        let mut client1=  Connector::new( args, &user1);
        
        client1.connect().await?;

        client1.publish1(m1.pkt.clone()).await?;

        client1.publish1(m2.pkt.clone()).await?;

        client1.disconnect().await?;
    }

    // // - user 2: connect and subscribe and recevie m1 with retain
    let mut client2=  Connector::new( args, &user2);
    client2.connect().await?;
    client2.subscribe().await?;
    client2.recv_publish1(&m1.pkt).instrument(new_span("user2 recving retain msg1")).await?;
    

    //  - user 3: connect and subscribe and recevie m1 with retain
    let mut client3=  Connector::new( args, &user3);
    client3.connect().await?;
    client3.subscribe().await?;
    client3.recv_publish1(&m1.pkt).instrument(new_span("user3 recving retain msg1")).await?;


    // - user 1: connect and publish m3 with retain
    let mut client1=  Connector::new( args, &user1);
    client1.connect().await?;
    client1.publish1(m3.pkt.clone()).await?;
    client1.disconnect().await?;

    // - user 2: receive m3 with retain
    // - user 3: receive m3 with retain
    // - user 3: disconnect
    client2.recv_publish1(&m3.pkt).instrument(new_span("user2 recving retain msg3")).await?;
    client3.recv_publish1(&m3.pkt).instrument(new_span("user3 recving retain msg3")).await?;
    client3.disconnect().await?;
    drop(client3);


    // - user 1: publish m4, m5, m6, all with retain
    client1.publish1(m4.pkt.clone()).await?;
    client1.publish1(m5.pkt.clone()).await?;
    client1.publish1(m6.pkt.clone()).await?;

    // - user 2: receive m4, m5, m6, with retain
    client2.recv_publish1(&m4.pkt).instrument(new_span("user2 recving msg4")).await?;
    client2.recv_publish1(&m5.pkt).instrument(new_span("user2 recving msg5")).await?;
    client2.recv_publish1(&m6.pkt).instrument(new_span("user2 recving msg6")).await?;

    // - user 3: connect and recevie m6, with retain
    // - user 3: disconnect
    let mut client3=  Connector::new( args, &user3);
    client3.connect().await?;
    client3.recv_publish1(&m6.pkt).instrument(new_span("user3 recving retain msg6")).await?;
    client3.disconnect().await?;
    
    // - user 1: publish m7(without retain), m8(with retain), m9(without retain)
    client1.publish1(m7.pkt.clone()).await?;
    client1.publish1(m8.pkt.clone()).await?;
    client1.publish1(m9.pkt.clone()).await?;

    // - user 2: receive m7(without retain), m8(with retain), m9(without retain)
    client2.recv_publish1(&m7.pkt).instrument(new_span("user2 recving msg7")).await?;
    client2.recv_publish1(&m8.pkt).instrument(new_span("user2 recving msg8")).await?;
    client2.recv_publish1(&m9.pkt).instrument(new_span("user2 recving msg9")).await?;

    // - user 3: connect and recevie m8, with retain
    let mut client3=  Connector::new( args, &user3);
    client3.connect().await?;
    client3.recv_publish1(&m8.pkt).instrument(new_span("user3 recving msg8")).await?;

    // clean up
    client1.publish1(m0.pkt.clone()).await?;
    client1.disconnect().await?;
    client2.disconnect().await?;
    client3.disconnect().await?;

    Ok(())
}

#[instrument(skip(args, accounts), level = "debug")]
async fn verify_will(args: &VArgs, mut accounts: AccountIter<'_>) -> Result<(), Error>{
    // - will message
    // - user 1: connect and subscribe
    // - user 2: connect with will m1 and shutdown socket
    // - user 1: receive m1
    // - user 3: connect and subscribe, recevie timeout
  
    let user1 = accounts.next().unwrap();
    let user2 = accounts.next().unwrap();
    let user3 = accounts.next().unwrap();

    let mut client1=  Connector::new( args, &user1);
    client1.connect().await?;
    client1.subscribe().await?;

    let mut client2=  Connector::new( args, &user2 ).with_will(false);
    client2.connect().await?;
    client2.shutdown().await?;

    client1.recv_publish0().await?;

    let mut client3=  Connector::new( args, &user3);
    client3.connect().await?;
    client3.subscribe().await?;
    client3.recv_timeout().await?;
    client3.disconnect().await?;

    client1.disconnect().await?;

    Ok(())
}

#[instrument(skip(cfg), level = "debug")]
async fn verfiy(cfg: &Config, ver: tt::Protocol) -> Result<(), Error> {
    let addr = cfg.address.as_ref().unwrap().to_string();
    let args = VArgs{
        addr,
        protocol: ver,
        topic: "t1/t2".to_string(),
        qos: tt::QoS::AtLeastOnce,
        timeout: Duration::from_secs(cfg.timeout_seconds),
        payload: vec![0x11u8, 0x22],
    };
    
    clean_up(&args, AccountIter::new(&cfg.accounts)).await?;

    verify_basic(&args, AccountIter::new(&cfg.accounts)).await?;
    
    verify_same_client_id(&args, AccountIter::new(&cfg.accounts)).await?;

    verify_clean_session(&args, AccountIter::new(&cfg.accounts)).await?;

    verify_retain(&args, AccountIter::new(&cfg.accounts)).await?;

    verify_will(&args, AccountIter::new(&cfg.accounts)).await?;

    Ok(())
}

async fn run(cfg: Config) -> Result<(), Error> {
    verfiy(&cfg, tt::Protocol::V4).await?;
    verfiy(&cfg, tt::Protocol::V5).await?;
    Ok(())
}


#[tokio::main]
async fn main() {
    tq3::log::tracing_subscriber::init_with_span_events(tracing_subscriber::fmt::format::FmtSpan::NEW);

    let args = CmdArgs::parse();

    let mut cfg = Config::default();
    if let Some(fname) = &args.config{
        debug!("loading config file [{}]...", fname);
        let mut c = config::Config::default();
        c.merge(config::File::with_name(fname)).unwrap();
        cfg = c.try_into().unwrap();
        debug!("loaded config file [{}]", fname);
    }

    // if cfg.address.is_none() {
    //     cfg.address = Some(args.address);
    // }

    debug!("cfg=[{:?}]", cfg);

    match run(cfg).await {
        Ok(_) => {
            info!("final ok");
        }
        Err(e) => {
            error!("final error [{}]", e);
        }
    }
}