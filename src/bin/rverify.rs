/*
√ basic
  - connect
  - subscribe
  - publish m1, and receive m1
  - unsubscribe, and receive timeout

√ two concurrent client with the same client id

√ will message

√ retain message

√ clean session basic

- ping interval

- offline message limit
*/

use std::time::Duration;

use clap::Clap;
use rust_threeq::tq3;
use rust_threeq::tq3::tt;
use tokio::time::timeout;
use tracing::{debug, error, info, instrument, Instrument, Span};
use tt::config::*;

// #[macro_use]
// extern crate serde_derive;

#[derive(Clap, Debug, Default)]
#[clap(name = "threeq verify", author, about, version)]
struct CmdArgs {
    #[clap(short = 'c', long = "config", long_about = "config file.")]
    config: Option<String>,
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("{0}")]
    ClientError(#[from] tt::client::Error),

    #[error("error: {0}")]
    Generic(String),
}

fn check_publish(rpkt: &tt::Publish, pkt: &tt::Publish, retain: bool) -> Result<(), String> {
    if pkt.qos != rpkt.qos {
        Err("diff qos".to_string())
    } else if pkt.topic != rpkt.topic {
        Err("diff topic".to_string())
    } else if pkt.payload != rpkt.payload {
        Err("diff payload".to_string())
    } else if pkt.retain != retain {
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
    let span = tracing::span!(tracing::Level::DEBUG, "", s = s);
    return span;
}

#[derive(Debug)]
struct Connector<'a> {
    args: &'a VArgs,
    pkt: tt::Connect,
    client: Option<tt::client::Client>,
}

impl<'a> Connector<'a> {
    fn new(args: &'a VArgs, account: &'a Account) -> Self {
        Self {
            args,
            pkt: init_conn_pkt(account, args.protocol),
            client: None,
        }
    }

    fn with_clean_session(mut self, b: bool) -> Self {
        self.pkt.clean_session = b;
        self
    }

    fn with_session_expire(mut self, seconds: u32) -> Self {
        if seconds > 0 {
            if self.pkt.properties.is_none() {
                self.pkt.properties = Some(tt::ConnectProperties::new());
            }
            self.pkt
                .properties
                .as_mut()
                .unwrap()
                .session_expiry_interval = Some(seconds);
            // self.pkt.clean_session = false;
        } else {
            if self.pkt.properties.is_some() {
                self.pkt
                    .properties
                    .as_mut()
                    .unwrap()
                    .session_expiry_interval = None;
            }
            // self.pkt.clean_session = true;
        }

        self
    }

    fn with_will(mut self, retain: bool) -> Self {
        self.pkt.last_will = Some(tt::LastWill::new(
            &self.args.topic,
            &*self.args.payload,
            self.args.qos,
            retain,
        ));
        self
    }

    async fn connect(&mut self, name: &str) -> Result<(), Error> {
        // let name = format!("{:p}", &self);
        let mut client = tt::client::make_connection(name, &self.args.addr).await?;
        let ack = client.sender.connect(self.pkt.clone()).await?;
        if ack.code != tt::ConnectReturnCode::Success {
            return Err(Error::Generic(format!("{:?}", ack)));
        }
        self.client = Some(client);
        return Ok(());
    }

    async fn subscribe(&mut self) -> Result<(), Error> {
        let client = self.client.as_mut().unwrap();
        let ack = client
            .sender
            .subscribe(tt::Subscribe::new(&self.args.topic, self.args.qos))
            .await?;
        for reason in &ack.return_codes {
            if !reason.is_success() {
                return Err(Error::Generic(format!("{:?}", ack)));
            }
        }
        return Ok(());
    }

    // async fn subscribe1(&mut self, filter: &str, qos: tt::QoS) -> Result<(), Error> {
    //     let client = self.client.as_mut().unwrap();
    //     let ack = client
    //         .sender
    //         .subscribe(tt::Subscribe::new(filter, qos))
    //         .await?;
    //     for reason in &ack.return_codes {
    //         if !reason.is_success() {
    //             return Err(Error::Generic(format!("{:?}", ack)));
    //         }
    //     }
    //     return Ok(());
    // }

    async fn unsubscribe(&mut self) -> Result<(), Error> {
        let client = self.client.as_mut().unwrap();
        let ack = client
            .sender
            .unsubscribe(tt::Unsubscribe::new(self.args.topic.clone()))
            .await?;
        for reason in &ack.reasons {
            if *reason != tt::UnsubAckReason::Success {
                let str = format!("unsubscribe fail, {:?}", ack);
                error!("{}", str);
                return Err(Error::Generic(str));
            }
        }
        return Ok(());
    }

    async fn publish(&mut self) -> Result<(), Error> {
        let client = self.client.as_mut().unwrap();
        let _r = client
            .sender
            .publish(tt::Publish::new(
                &self.args.topic,
                self.args.qos,
                self.args.payload.clone(),
            ))
            .await?;
        Ok(())
    }

    async fn publish1(&mut self, pkt: tt::Publish) -> Result<(), Error> {
        let client = self.client.as_mut().unwrap();
        let _r = client.sender.publish(pkt).await?;
        Ok(())
    }

    async fn recv(&mut self) -> Result<tt::client::Event, tt::client::Error> {
        let client = self.client.as_mut().unwrap();
        let r = timeout(self.args.timeout, client.receiver.recv()).await;
        let ev = match r {
            Ok(r) => Ok(r),
            Err(e) => {
                error!("recv timeout");
                Err(tt::client::Error::Timeout(e))
            }
        }?;
        return ev;
    }

    async fn recv_timeout(&mut self) -> Result<(), Error> {
        let client = self.client.as_mut().unwrap();
        return match timeout(self.args.timeout, client.receiver.recv()).await {
            Ok(_r) => {
                error!("recv timeout");
                Err(Error::Generic(format!("recv timeout")))
            }
            Err(_) => Ok(()),
        };
    }

    async fn recv_publish0(&mut self) -> Result<(), Error> {
        let rpkt = tt::Publish::new(
            self.args.topic.clone(),
            self.args.qos,
            self.args.payload.clone(),
        );
        return self.recv_publish1(&rpkt).await;
    }

    async fn recv_publish1(&mut self, rpkt: &tt::Publish) -> Result<(), Error> {
        return self.recv_publish2(&rpkt, rpkt.retain).await;
    }

    async fn recv_publish2(&mut self, rpkt: &tt::Publish, retain: bool) -> Result<(), Error> {
        let ev = self.recv().await?;

        match &ev {
            tt::client::Event::Packet(pkt) => match pkt {
                tt::Packet::Publish(pkt) => {
                    let r = check_publish(rpkt, pkt, retain);
                    return match r {
                        Ok(_) => Ok(()),
                        Err(s) => {
                            let str =
                                format!("expect recv {:?}, but {:?}, reason {}", rpkt, pkt, s);
                            error!("{}", str);
                            Err(Error::Generic(str))
                        }
                    };
                }
                _ => {}
            },
            tt::client::Event::Closed(_) => {}
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

#[derive(Debug)]
struct SyncConnector<'a> {
    args: &'a VArgs,
    pkt: tt::Connect,
    client: Option<tt::client::SyncClient>,
}

impl<'a> SyncConnector<'a> {
    fn new(args: &'a VArgs, account: &'a Account) -> Self {
        Self {
            args,
            pkt: init_conn_pkt(account, args.protocol),
            client: None,
        }
    }

    // fn with_clean_session(mut self, b: bool) -> Self {
    //     self.pkt.clean_session = b;
    //     self
    // }

    // fn with_session_expire(mut self, seconds: u32) -> Self {
    //     if seconds > 0 {
    //         if self.pkt.properties.is_none() {
    //             self.pkt.properties = Some(tt::ConnectProperties::new());
    //         }
    //         self.pkt
    //             .properties
    //             .as_mut()
    //             .unwrap()
    //             .session_expiry_interval = Some(seconds);
    //         // self.pkt.clean_session = false;
    //     } else {
    //         if self.pkt.properties.is_some() {
    //             self.pkt
    //                 .properties
    //                 .as_mut()
    //                 .unwrap()
    //                 .session_expiry_interval = None;
    //         }
    //         // self.pkt.clean_session = true;
    //     }

    //     self
    // }

    // fn with_will(mut self, retain: bool) -> Self {
    //     self.pkt.last_will = Some(tt::LastWill::new(
    //         &self.args.topic,
    //         &*self.args.payload,
    //         self.args.qos,
    //         retain,
    //     ));
    //     self
    // }

    async fn connect(&mut self, name: &str) -> Result<(), Error> {
        let mut client = tt::client::SyncClient::new(name.to_string());
        let ack = client.connect(&self.args.addr, &self.pkt).await?;
        if ack.code != tt::ConnectReturnCode::Success {
            return Err(Error::Generic(format!("{:?}", ack)));
        }
        self.client = Some(client);
        return Ok(());
    }

    // async fn subscribe(&mut self) -> Result<(), Error> {
    //     let client = self.client.as_mut().unwrap();

    //     let ack = client
    //         .subscribe(&tt::Subscribe::new(&self.args.topic, self.args.qos))
    //         .await?;
    //     for reason in &ack.return_codes {
    //         if !reason.is_success() {
    //             return Err(Error::Generic(format!("{:?}", ack)));
    //         }
    //     }
    //     return Ok(());
    // }

    async fn subscribe1(&mut self, filter: &str, qos: tt::QoS) -> Result<(), Error> {
        let client = self.client.as_mut().unwrap();
        let ack = client.subscribe(&tt::Subscribe::new(filter, qos)).await?;
        for reason in &ack.return_codes {
            if !reason.is_success() {
                return Err(Error::Generic(format!("{:?}", ack)));
            }
        }
        return Ok(());
    }

    // async fn unsubscribe(&mut self) -> Result<(), Error> {
    //     let client = self.client.as_mut().unwrap();
    //     let ack = client
    //         .unsubscribe(&tt::Unsubscribe::new(self.args.topic.clone()))
    //         .await?;
    //     for reason in &ack.reasons {
    //         if *reason != tt::UnsubAckReason::Success {
    //             let str = format!("unsubscribe fail, {:?}", ack);
    //             error!("{}", str);
    //             return Err(Error::Generic(str));
    //         }
    //     }
    //     return Ok(());
    // }

    // async fn publish<P: Into<Vec<u8>>>(&mut self, paylaod: P) -> Result<(), Error> {
    //     let client = self.client.as_mut().unwrap();
    //     let _r = client
    //         .publish(&tt::Publish::new(
    //             &self.args.topic,
    //             self.args.qos,
    //             paylaod,
    //         )).await?;
    //     Ok(())
    // }

    async fn publish_str(&mut self, s: &str) -> Result<(), Error> {
        let client = self.client.as_mut().unwrap();
        let _r = client
            .publish(&tt::Publish::new(&self.args.topic, self.args.qos, s))
            .await?;
        Ok(())
    }

    async fn recv_publish(&mut self) -> Result<tt::Publish, Error> {
        let client = self.client.as_mut().unwrap();
        let pkt = client.recv_publish().await?;
        Ok(pkt)
    }

    // async fn recv_packet(&mut self) -> Result<(tt::PacketType, tt::FixedHeader, Bytes), Error>{
    //     let client = self.client.as_mut().unwrap();
    //     let r = client.recv_packet().await?;
    //     Ok(r)
    // }

    // async fn publish(&mut self) -> Result<(), Error> {
    //     let client = self.client.as_mut().unwrap();
    //     let _r = client
    //         .publish(&tt::Publish::new(
    //             &self.args.topic,
    //             self.args.qos,
    //             self.args.payload.clone(),
    //         ))
    //         .await?;
    //     Ok(())
    // }

    // async fn publish1(&mut self, pkt: tt::Publish) -> Result<(), Error> {
    //     let client = self.client.as_mut().unwrap();
    //     let _r = client.publish(&pkt).await?;
    //     Ok(())
    // }

    // async fn recv_publish0(&mut self) -> Result<(), Error> {
    //     let rpkt = tt::Publish::new(
    //         self.args.topic.clone(),
    //         self.args.qos,
    //         self.args.payload.clone(),
    //     );
    //     return self.recv_publish1(&rpkt).await;
    // }

    // async fn recv_publish1(&mut self, rpkt: &tt::Publish) -> Result<(), Error> {
    //     return self.recv_publish2(&rpkt, rpkt.retain).await;
    // }

    // async fn recv_publish2(&mut self, rpkt: &tt::Publish, retain: bool) -> Result<(), Error> {
    //     let client = self.client.as_mut().unwrap();
    //     let pkt = client.recv_publish().await?;
    //     let r = check_publish(rpkt, &pkt, retain);
    //     return match r {
    //         Ok(_) => Ok(()),
    //         Err(s) => {
    //             let str =
    //                 format!("expect recv {:?}, but {:?}, reason {}", rpkt, pkt, s);
    //             error!("{}", str);
    //             Err(Error::Generic(str))
    //         }
    //     };
    // }

    async fn disconnect(&mut self) -> Result<(), Error> {
        let client = self.client.as_mut().unwrap();
        let _ack = client.disconnect(&tt::Disconnect::new()).await?;
        Ok(())
    }

    // async fn shutdown(&mut self) -> Result<(), Error> {
    //     let client = self.client.as_mut().unwrap();
    //     let _r = client.shutdown().await?;
    //     Ok(())
    // }
}

struct Message {
    pkt: tt::Publish,
}

impl Message {
    fn new(args: &VArgs) -> Self {
        Self {
            pkt: tt::Publish::new(args.topic.clone(), args.qos, args.payload.clone()),
        }
    }

    fn with_payload(mut self, payload: Vec<u8>) -> Self {
        self.pkt.payload = payload.into();
        self
    }

    fn with_retain(mut self) -> Self {
        self.pkt.retain = true;
        self
    }
}

#[derive(Debug, Clone)]
struct VArgs {
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

    let mut client = Connector::new(args, &account);

    client.connect("clean_up").await?;

    let m1 = Message::new(args).with_payload(vec![]).with_retain();
    client.publish1(m1.pkt).await?;

    client.disconnect().await?;

    client.recv().await?;

    Ok(())
}

#[instrument(skip(args, accounts), name = "basic", level = "debug")]
async fn verify_basic(args: &VArgs, mut accounts: AccountIter<'_>) -> Result<(), Error> {
    let account = accounts.next().unwrap();

    let mut client = Connector::new(args, &account);

    client.connect("client").await?;

    client.subscribe().await?;

    client.publish().await?;

    client.recv_publish0().await?;

    client.unsubscribe().await?;

    client.publish().await?;

    client
        .recv_timeout()
        .instrument(new_span("recv no message after unsubscribe"))
        .await?;

    client.disconnect().await?;

    client.recv().await?;

    Ok(())
}

#[instrument(skip(args, accounts), name = "same_client_id", level = "debug")]
async fn verify_same_client_id(args: &VArgs, mut accounts: AccountIter<'_>) -> Result<(), Error> {
    let account = accounts.next().unwrap();

    let mut client1 = Connector::new(args, &account);

    client1.connect("client1").await?;

    client1.subscribe().await?;

    let mut client2 = Connector::new(args, &account);

    client2.connect("client2").await?;

    let r = client1
        .recv()
        .instrument(new_span("client1 waiting for disconnect"))
        .await;

    client2.disconnect().await?;

    match args.protocol {
        tt::Protocol::V4 => {
            if r.is_err() {
                if let tt::client::Error::Broken(_) = r.as_ref().unwrap_err() {
                    return Ok(());
                }
            }
            return Err(Error::Generic(format!("expect broken but got {:?}", r)));
        }
        tt::Protocol::V5 => {
            if let Ok(ev) = &r {
                if let tt::client::Event::Packet(pkt) = ev {
                    if let tt::Packet::Disconnect(_) = pkt {
                        return Ok(());
                    }
                }
            }
            return Err(Error::Generic(format!("expect disconnect but got {:?}", r)));
        }
    }
}

#[instrument(skip(args, accounts), name = "clean_session", level = "debug")]
async fn verify_clean_session(args: &VArgs, mut accounts: AccountIter<'_>) -> Result<(), Error> {
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
        let mut client1 = Connector::new(args, &user1)
            .with_clean_session(false)
            .with_session_expire(expired_seconds);

        client1.connect("client1").await?;

        client1.subscribe().await?;

        client1.publish().await?;

        client1
            .recv_publish0()
            .instrument(new_span("recving online"))
            .await?;

        client1.disconnect().await?;
    }

    // user2 connect and send message
    let mut client2 = Connector::new(args, &user2);

    client2.connect("client2").await?;

    client2.publish().await?;

    // user1 connect again and got offline m1
    {
        let mut client1 = Connector::new(args, &user1)
            .with_clean_session(false)
            .with_session_expire(expired_seconds);

        client1.connect("client1").await?;

        client1
            .recv_publish0()
            .instrument(new_span("recving offline"))
            .await?;

        client1.disconnect().await?;
    }

    // user2 send message

    client2.publish().await?;

    // user1 connect with clean-sesison 1, got nothing
    {
        let mut client1 = Connector::new(args, &user1);

        client1.connect("client1").await?;

        client1
            .recv_timeout()
            .instrument(new_span("recv nothing"))
            .await?;

        client1.disconnect().await?;
    }

    client2.disconnect().await?;

    Ok(())
}

#[instrument(skip(args, accounts), name = "retain", level = "debug")]
async fn verify_retain(args: &VArgs, mut accounts: AccountIter<'_>) -> Result<(), Error> {
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

    let m1 = Message::new(args)
        .with_payload(vec![0x01u8; 1])
        .with_retain();
    let m2 = Message::new(args).with_payload(vec![0x02u8; 2]);
    let m3 = Message::new(args)
        .with_payload(vec![0x03u8; 3])
        .with_retain();
    let m4 = Message::new(args)
        .with_payload(vec![0x04u8; 4])
        .with_retain();
    let m5 = Message::new(args)
        .with_payload(vec![0x05u8; 5])
        .with_retain();
    let m6 = Message::new(args)
        .with_payload(vec![0x06u8; 6])
        .with_retain();
    let m7 = Message::new(args).with_payload(vec![0x07u8; 7]);
    let m8 = Message::new(args)
        .with_payload(vec![0x08u8; 8])
        .with_retain();
    let m9 = Message::new(args).with_payload(vec![0x09u8; 9]);
    let m0 = Message::new(args).with_payload(vec![]).with_retain();

    {
        let mut client1 = Connector::new(args, &user1);

        client1.connect("client1").await?;

        client1.publish1(m1.pkt.clone()).await?;

        client1.publish1(m2.pkt.clone()).await?;

        client1.disconnect().await?;
    }

    // // - user 2: connect and subscribe and recevie m1 with retain
    let mut client2 = Connector::new(args, &user2);
    client2.connect("client2").await?;
    client2.subscribe().await?;
    client2
        .recv_publish1(&m1.pkt)
        .instrument(new_span("user2 recving retain msg1"))
        .await?;

    //  - user 3: connect and subscribe and recevie m1 with retain
    let mut client3 = Connector::new(args, &user3);
    client3.connect("client3").await?;
    client3.subscribe().await?;
    client3
        .recv_publish1(&m1.pkt)
        .instrument(new_span("user3 recving retain msg1"))
        .await?;
    client3.disconnect().await?;

    // - user 1: connect and publish m3 with retain
    let mut client1 = Connector::new(args, &user1);
    client1.connect("client1").await?;
    client1.publish1(m3.pkt.clone()).await?;
    client1.disconnect().await?;

    // - user 2: receive m3 without retain
    // - user 3: receive m3 with retain
    // - user 3: disconnect
    client2
        .recv_publish2(&m3.pkt, false)
        .instrument(new_span("user2 recving msg3"))
        .await?;

    let mut client3 = Connector::new(args, &user3);
    client3.connect("client3").await?;
    client3.subscribe().await?;
    client3
        .recv_publish1(&m3.pkt)
        .instrument(new_span("user3 recving retain msg3"))
        .await?;
    client3.disconnect().await?;
    drop(client3);

    // - user 1: publish m4, m5, m6, all with retain
    let mut client1 = Connector::new(args, &user1);
    client1.connect("client1").await?;
    client1.publish1(m4.pkt.clone()).await?;
    client1.publish1(m5.pkt.clone()).await?;
    client1.publish1(m6.pkt.clone()).await?;

    // - user 2: receive m4, m5, m6, without retain
    client2
        .recv_publish2(&m4.pkt, false)
        .instrument(new_span("user2 recving msg4"))
        .await?;
    client2
        .recv_publish2(&m5.pkt, false)
        .instrument(new_span("user2 recving msg5"))
        .await?;
    client2
        .recv_publish2(&m6.pkt, false)
        .instrument(new_span("user2 recving msg6"))
        .await?;

    // - user 3: connect and subscribe, recevie m6, with retain
    // - user 3: disconnect
    let mut client3 = Connector::new(args, &user3);
    client3.connect("client3").await?;
    client3.subscribe().await?;
    client3
        .recv_publish1(&m6.pkt)
        .instrument(new_span("user3 recving retain msg6"))
        .await?;
    client3.disconnect().await?;

    // - user 1: publish m7(without retain), m8(with retain), m9(without retain)
    client1.publish1(m7.pkt.clone()).await?;
    client1.publish1(m8.pkt.clone()).await?;
    client1.publish1(m9.pkt.clone()).await?;

    // - user 2: receive m7, m8, m9, all without retain
    client2
        .recv_publish1(&m7.pkt)
        .instrument(new_span("user2 recving msg7"))
        .await?;
    client2
        .recv_publish2(&m8.pkt, false)
        .instrument(new_span("user2 recving msg8"))
        .await?;
    client2
        .recv_publish1(&m9.pkt)
        .instrument(new_span("user2 recving msg9"))
        .await?;

    // - user 3: connect and recevie m8, with retain
    let mut client3 = Connector::new(args, &user3);
    client3.connect("client3").await?;
    client3.subscribe().await?;
    client3
        .recv_publish1(&m8.pkt)
        .instrument(new_span("user3 recving retain msg8"))
        .await?;

    // clean up
    client1.publish1(m0.pkt.clone()).await?;
    client1.disconnect().await?;
    client2.disconnect().await?;
    client3.disconnect().await?;

    Ok(())
}

#[instrument(skip(args, accounts), name = "will", level = "debug")]
async fn verify_will(args: &VArgs, mut accounts: AccountIter<'_>) -> Result<(), Error> {
    // - will message
    // - user 1: connect and subscribe
    // - user 2: connect with will m1 and shutdown socket
    // - user 1: receive m1
    // - user 3: connect and subscribe, recevie timeout

    let user1 = accounts.next().unwrap();
    let user2 = accounts.next().unwrap();
    let user3 = accounts.next().unwrap();

    let mut client1 = Connector::new(args, &user1);
    client1.connect("client1").await?;
    client1.subscribe().await?;

    let mut client2 = Connector::new(args, &user2).with_will(false);
    client2.connect("client2").await?;
    client2.shutdown().await?;

    client1.recv_publish0().await?;

    let mut client3 = Connector::new(args, &user3);
    client3.connect("client3").await?;
    client3.subscribe().await?;
    client3.recv_timeout().await?;
    client3.disconnect().await?;

    client1.disconnect().await?;

    Ok(())
}

#[instrument(skip(args, accounts), name = "shared", level = "debug")]
async fn verify_shared(args: &VArgs, mut accounts: AccountIter<'_>) -> Result<(), Error> {
    // - will message
    // - user 1: connect and subscribe
    // - user 2: connect with will m1 and shutdown socket
    // - user 1: receive m1
    // - user 3: connect and subscribe, recevie timeout
    let shared_filter = format!("$share/group1/{}", args.topic);

    let user1 = accounts.next().unwrap();
    let user2 = accounts.next().unwrap();
    let user3 = accounts.next().unwrap();
    let nmsgs = 10 as usize;

    let mut client1 = SyncConnector::new(args, &user1);
    client1.connect("client1").await?;
    client1.subscribe1(&shared_filter, args.qos).await?;

    let mut client2 = SyncConnector::new(&args, &user2);
    client2.connect("client2").await?;
    client2.subscribe1(&shared_filter, args.qos).await?;

    let args0 = args.clone();
    let send_task = async move {
        let mut client3 = SyncConnector::new(&args0, &user3);
        client3.connect("client3").await?;
        for n in 0..nmsgs {
            client3.publish_str(&format!(r#"{{"k":{}}}"#, n)).await?;
            debug!("client3: published message {}", n);
        }
        client3.disconnect().await?;
        Ok::<(), Error>(())
    };

    let h = tokio::spawn(async move {
        let r = send_task.await;
        match r {
            Ok(_o) => {}
            Err(e) => {
                error!("send task error [{}]", e);
            }
        }
    });

    {
        let mut n = 0usize;
        while n < 2 {
            let pkt = client2.recv_publish().await?;
            let s = std::str::from_utf8(&pkt.payload).unwrap();
            info!("client2: got message {}: {}", n, s);
            n += 1;
        }
        client1.disconnect().await?;
        info!("client1: disconnected");

        while n < nmsgs {
            let pkt = client2.recv_publish().await?;
            let s = std::str::from_utf8(&pkt.payload).unwrap();
            info!("client2: got message {}: {}", n, s);
            n += 1;
        }
    }

    let _r = h.await;
    client2.disconnect().await?;

    Ok(())
}

#[instrument(skip(cfg), level = "debug")]
async fn verfiy(cfg: &Config, ver: tt::Protocol) -> Result<(), Error> {
    let verification = cfg.verification();
    let args = VArgs {
        addr: cfg.env().address.clone(),
        protocol: ver,
        topic: "t1/t2".to_string(),
        qos: tt::QoS::AtLeastOnce,
        timeout: Duration::from_millis(cfg.raw().recv_timeout_ms),
        payload: "{\"k\":111}".as_bytes().into(), //vec![0x11u8, 0x22],
    };

    // info!("payload len {}", args.payload.len());

    if verification.clean_up {
        clean_up(&args, AccountIter::new(&cfg.env().accounts)).await?;
    }

    if verification.verify_basic {
        verify_basic(&args, AccountIter::new(&cfg.env().accounts)).await?;
    }

    if verification.verify_same_client_id {
        verify_same_client_id(&args, AccountIter::new(&cfg.env().accounts)).await?;
    }

    if verification.verify_clean_session {
        verify_clean_session(&args, AccountIter::new(&cfg.env().accounts)).await?;
    }

    if verification.verify_retain {
        verify_retain(&args, AccountIter::new(&cfg.env().accounts)).await?;
    }

    if verification.verify_will {
        verify_will(&args, AccountIter::new(&cfg.env().accounts)).await?;
    }

    if verification.verify_shared {
        verify_shared(&args, AccountIter::new(&cfg.env().accounts)).await?;
    }

    Ok(())
}

async fn run(cfg: Config) -> Result<(), Error> {
    if cfg.verification().verify_v4 {
        verfiy(&cfg, tt::Protocol::V4).await?;
    }

    if cfg.verification().verify_v5 {
        verfiy(&cfg, tt::Protocol::V5).await?;
    }

    Ok(())
}

#[tokio::main]
async fn main() {
    tq3::log::tracing_subscriber::init_with_span_events(
        tracing_subscriber::fmt::format::FmtSpan::NONE,
    );

    let args = CmdArgs::parse();

    // let mut cfg = Config::default();
    // if let Some(fname) = &args.config {
    //     debug!("loading config file [{}]...", fname);
    //     let mut c = config::Config::default();
    //     c.merge(config::File::with_name(fname)).unwrap();
    //     cfg = c.try_into().unwrap();
    //     debug!("loaded config file [{}]", fname);
    // }

    let cfg = if let Some(fname) = &args.config {
        tt::config::Config::load_from_file(fname)
    } else {
        tt::config::Config::default()
    };

    debug!("cfg=[{:?}]", cfg);
    info!("-");
    info!("env=[{}]", cfg.raw().env);
    info!("-");

    match run(cfg).await {
        Ok(_) => {
            info!("final ok");
        }
        Err(e) => {
            error!("final error [{}]", e);
        }
    }
}
