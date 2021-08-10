// reqwest=info,hyper=info

use super::super::common;
use super::config as app;
use async_trait::async_trait;
use bytes::Bytes;
use rust_threeq::tq3::tt;
use std::sync::Arc;
use tracing::{error, trace};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("{0}")]
    RunError(#[from] common::Error),
}

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

struct Puber {
    cfg: Arc<app::Config>,
    acc: app::Account,
    sender: Option<tt::client::Sender>,
    recver: Option<tt::client::Receiver>,
}

#[async_trait]
impl common::Puber for Puber {
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

    let mut bencher = common::PubsubBencher::new();
    let mut sub_id = 0u64;
    let mut pub_id = 0u64;

    if cfgw.raw().subs.connections > 0 {
        let _r = bencher
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

        let _r = bencher
            .launch_pub_sessions(
                "pubs".to_string(),
                &mut pub_id,
                cfgw.raw().pubs.connections,
                cfgw.raw().pubs.conn_per_sec,
                args,
                |_n| {
                    let acc = accounts.next().unwrap();
                    Puber {
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

        let _r = bencher
            .launch_pub_sessions("rests".to_string(), &mut pub_id, 1, 1, args, |_n| {
                RestPuber { cfg: cfgw.clone() }
            })
            .await?;
    }

    let cfg = cfgw.raw();
    bencher.kick_and_wait(cfg.recv_timeout_ms).await?;

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
}
