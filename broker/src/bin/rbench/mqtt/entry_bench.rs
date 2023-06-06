// reqwest=info,hyper=info

use crate::common::config::make_pubsub_topics;

use super::super::common;
use super::config as app;
use anyhow::{bail, Result};
use async_trait::async_trait;
use bytes::Bytes;
use rust_threeq::tq3::{
    try_poll,
    tt::{self, client::PublishFuture},
    Flight, Inflights,
};
use std::sync::Arc;
use tracing::{error, info, trace};

struct Suber {
    cfg: Arc<app::Config>,
    acc: app::Account,
    topic: String,
    sender: Option<tt::client::Sender>,
    recver: Option<tt::client::Receiver>,
}

#[async_trait]
impl common::Suber for Suber {
    async fn connect(&mut self) -> Result<()> {
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
            bail!(format!("{:?}", ack));
        }

        let ack = sender
            .subscribe(tt::Subscribe::new(&self.topic, cfg.subs.qos))
            .await?;
        for reason in &ack.return_codes {
            if !reason.is_success() {
                bail!(format!(
                    "subscribe [{}], [{:?}] fail, {:?}",
                    self.topic, cfg.subs.qos, ack
                ));
            }
        }

        self.sender = Some(sender);
        self.recver = Some(recver);

        Ok(())
    }

    async fn disconnect(&mut self) -> Result<()> {
        if self.sender.is_some() {
            self.sender
                .as_mut()
                .unwrap()
                .disconnect(tt::Disconnect::new())
                .await?;
        }
        Ok(())
    }

    async fn recv(&mut self) -> Result<Bytes> {
        let ev = self.recver.as_mut().unwrap().recv().await?;
        let rpkt = match ev {
            tt::client::Event::Packet(pkt) => match pkt {
                tt::Packet::Publish(rpkt) => rpkt,
                _ => bail!(format!("unexpect packet {:?}", pkt)),
            },
            tt::client::Event::Closed(s) => {
                bail!(format!("got closed [{}]", s));
            }
        };
        Ok(rpkt.payload)
    }
}

struct PubFlight {
    future: PublishFuture,
}

#[async_trait]
impl Flight for PubFlight {
    type Output = ();

    async fn try_recv_ack(&mut self) -> Result<Option<Self::Output>> {
        let r = try_poll(&mut self.future).await;
        if r.is_none() {
            return Ok(None);
        }
        Ok(Some(r.unwrap()?))
    }

    async fn recv_ack(self) -> Result<Self::Output> {
        Ok(self.future.await?)
    }
}

struct Puber {
    cfg: Arc<app::Config>,
    acc: app::Account,
    topic: String,
    sender: Option<tt::client::Sender>,
    recver: Option<tt::client::Receiver>,
    inflights: Inflights<PubFlight>,
}

#[async_trait]
impl common::Puber for Puber {
    async fn connect(&mut self) -> Result<()> {
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

    async fn disconnect(&mut self) -> Result<()> {
        if self.sender.is_some() {
            self.sender
                .as_mut()
                .unwrap()
                .disconnect(tt::Disconnect::new())
                .await?;
        }
        Ok(())
    }

    async fn send(&mut self, data: Bytes) -> Result<()> {
        let mut pkt = tt::Publish::new(&self.topic, self.cfg.raw().pubs.qos, []);
        pkt.payload = data;
        let r = self.sender.as_mut().unwrap().publish_result(pkt).await?;
        // let _r = r.await?;
        self.inflights
            .add_and_check(PubFlight { future: r }, self.cfg.raw().pubs.inflights)
            .await?;
        Ok(())
    }

    async fn flush(&mut self) -> Result<()> {
        let _r = self.inflights.wait_for_newest().await?;
        Ok(())
    }

    async fn idle(&mut self) -> Result<()> {
        loop {
            let ev = self.recver.as_mut().unwrap().recv().await?;
            if let tt::client::Event::Closed(s) = ev {
                bail!(format!("got closed [{}]", s));
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
    async fn connect(&mut self) -> Result<()> {
        Ok(())
    }

    async fn disconnect(&mut self) -> Result<()> {
        Ok(())
    }

    async fn send(&mut self, data: Bytes) -> Result<()> {
        Self::call_rest(&self.cfg.env().rest_api, base64::encode(data)).await?;
        Ok(())
    }

    async fn flush(&mut self) -> Result<()> {
        Ok(())
    }

    async fn idle(&mut self) -> Result<()> {
        Ok(())
    }
}

pub async fn bench_all(cfgw: Arc<app::Config>) -> Result<()> {
    let mut accounts = app::AccountIter::new(&cfgw.env().accounts);

    let mut bencher = common::PubsubBencher::new();
    let mut sub_id = 0u64;
    let mut pub_id = 0u64;

    let (mut pub_topics, mut sub_topics, desc) = make_pubsub_topics(
        cfgw.raw().pubs.connections,
        &cfgw.raw().pubs.topic,
        cfgw.raw().subs.connections,
        &cfgw.raw().subs.topic,
        &cfgw.raw().random_seed,
    );

    info!("topic rule: {}", desc);
    info!("pub inflights: {}", cfgw.raw().pubs.inflights);

    // for t in pub_topics.iter() {
    //     tracing::debug!("pub topic: [{:?}]", t);
    // }

    // for t in sub_topics.iter() {
    //     tracing::debug!("sub topic: [{}]", t);
    // }

    if sub_topics.len() > 0 {
        let _r = bencher
            .launch_sub_sessions(
                "subs".to_string(),
                &mut sub_id,
                sub_topics.len() as u64,
                cfgw.raw().subs.conn_per_sec,
                |_n| {
                    let acc = accounts.next().unwrap();
                    // tracing::debug!("sub client_id: [{:?}]", acc.client_id);

                    Suber {
                        cfg: cfgw.clone(),
                        acc,
                        topic: sub_topics.pop().unwrap(),
                        sender: None,
                        recver: None,
                    }
                },
            )
            .await?;
    }

    if pub_topics.len() > 0 {
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
                pub_topics.len() as u64,
                cfgw.raw().pubs.conn_per_sec,
                args,
                |_n| {
                    let acc = accounts.next().unwrap();
                    // tracing::debug!("pub client_id: [{:?}]", acc.client_id);

                    let item = pub_topics.pop().unwrap();
                    let o = Puber {
                        cfg: cfgw.clone(),
                        acc,
                        topic: item.1,
                        sender: None,
                        recver: None,
                        inflights: Inflights::new(),
                    };
                    (item.0, o)
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
            .launch_pub_sessions("rests".to_string(), &mut pub_id, 1, 1, args, |n| {
                let o = RestPuber { cfg: cfgw.clone() };
                (n, o)
            })
            .await?;
    }

    if pub_id > 0 {
        info!("press Enter to continue...");
        let _ = std::io::Read::read(&mut std::io::stdin(), &mut [0u8]).unwrap();
    }

    bencher.kick_and_wait(cfgw.raw().recv_timeout_ms).await?;

    Ok(())
}

pub async fn run(args: &super::Args) {
    let cfg = app::Config::load_from_file(&args.config_file);
    trace!("cfg=[{:#?}]", cfg.raw());
    let cfg = Arc::new(cfg);

    match bench_all(cfg.clone()).await {
        Ok(_) => {}
        Err(e) => {
            error!("bench result error [{}]", e);
        }
    }
}
