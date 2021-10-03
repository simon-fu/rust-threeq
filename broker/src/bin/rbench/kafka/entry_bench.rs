use super::config::Config;
use crate::common;
use crate::common::config::make_pubsub_topics;
use anyhow::{bail, Result};
use async_trait::async_trait;
use bytes::Bytes;
use rust_threeq::tq3::{try_poll, Flight, Inflights};
use std::sync::Arc;

use log::{error, info, trace};
use rdkafka::{
    consumer::{CommitMode, Consumer, StreamConsumer},
    producer::{DeliveryFuture, FutureProducer, FutureRecord},
    ClientConfig, Message,
};

struct PubFlight {
    future: DeliveryFuture,
}

#[async_trait]
impl Flight for PubFlight {
    type Output = (i32, i64);
    async fn try_recv_ack(&mut self) -> Result<Option<Self::Output>> {
        let r = try_poll(&mut self.future).await;
        if r.is_none() {
            return Ok(None);
        }
        let r = check_pub_ack(r.unwrap()?)?;
        Ok(Some(r))
    }

    async fn recv_ack(self) -> Result<Self::Output> {
        let r = check_pub_ack(self.future.await?)?;
        Ok(r)
    }
}

fn check_pub_ack(
    r: Result<(i32, i64), (rdkafka::error::KafkaError, rdkafka::message::OwnedMessage)>,
) -> Result<(i32, i64)> {
    match r {
        Ok(r) => Ok(r),
        Err((e, _o)) => {
            bail!(e);
        }
    }
}

struct Puber {
    topic: String,
    producer: Option<FutureProducer>,
    inflights: Inflights<PubFlight>,
    config: Arc<Config>,
}

#[async_trait]
impl common::Puber for Puber {
    async fn connect(&mut self) -> Result<()> {
        Ok(())
    }

    async fn disconnect(&mut self) -> Result<()> {
        if let Some(producer) = self.producer.take() {
            drop(producer);
        }
        Ok(())
    }

    async fn send(&mut self, data: Bytes) -> Result<()> {
        loop {
            let slice = &data[..];
            let topic = self.topic.clone();
            let r = self
                .producer
                .as_mut()
                .unwrap()
                .send_result(FutureRecord::<String, _>::to(&topic).payload(slice));

            match r {
                Ok(f) => {
                    let f = PubFlight { future: f };
                    let _r = self
                        .inflights
                        .add_and_check(f, self.config.raw().pubs.inflights)
                        .await?;
                    return Ok(());
                }

                Err(_e) => {
                    self.inflights.wait_for_oldest_and_check().await?;
                }
            }
        }
    }

    async fn flush(&mut self) -> Result<()> {
        let _r = self.inflights.wait_for_newest().await?;
        Ok(())
    }

    async fn idle(&mut self) -> Result<()> {
        Ok(())
    }
}

struct Suber {
    topic: String,
    consumer: Option<StreamConsumer>,
}

#[async_trait]
impl common::Suber for Suber {
    async fn connect(&mut self) -> Result<()> {
        self.consumer
            .as_ref()
            .unwrap()
            .subscribe(&[&self.topic])
            .unwrap();
        Ok(())
    }

    async fn disconnect(&mut self) -> Result<()> {
        if let Some(consumer) = self.consumer.take() {
            drop(consumer);
        }
        Ok(())
    }

    async fn recv(&mut self) -> Result<Bytes> {
        let borrowed_message = self.consumer.as_ref().unwrap().recv().await.unwrap();
        let msg = borrowed_message.detach();
        self.consumer
            .as_ref()
            .unwrap()
            .commit_message(&borrowed_message, CommitMode::Async)
            .unwrap();
        let payload = msg.payload().unwrap();
        let b = Bytes::copy_from_slice(payload);
        Ok(b)
    }
}

pub async fn bench_all(cfgw: Arc<Config>, node_id: String) -> Result<()> {
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

    if sub_topics.len() > 0 {
        let _r = bencher
            .launch_sub_sessions(
                "subs".to_string(),
                &mut sub_id,
                sub_topics.len() as u64,
                cfgw.raw().subs.conn_per_sec,
                |n| {
                    let sub_id = format!("{}-sub-{}", node_id, n);
                    let consumer: StreamConsumer = ClientConfig::new()
                        .set("group.id", &sub_id)
                        .set("bootstrap.servers", &cfgw.env().address)
                        .set("enable.partition.eof", "false")
                        .set("session.timeout.ms", "6000")
                        .set("enable.auto.commit", "false")
                        .create()
                        .unwrap();

                    Suber {
                        topic: sub_topics.pop().unwrap(),
                        consumer: Some(consumer),
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
                    let item = pub_topics.pop().unwrap();

                    let r = ClientConfig::new()
                        .set("bootstrap.servers", &cfgw.env().address)
                        .set("message.timeout.ms", "5000")
                        .create();
                    let producer: FutureProducer = r.unwrap();

                    let o = Puber {
                        topic: item.1,
                        producer: Some(producer),
                        inflights: Inflights::new(),
                        config: cfgw.clone(),
                    };
                    (item.0, o)
                },
            )
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
    // let r = test().await;
    // if r.is_err() {
    //     error!("{:?}", r);
    // }

    let cfg = Config::load_from_file(&args.config_file);
    let cfg = Arc::new(cfg);
    trace!("cfg=[{:#?}]", cfg.raw());

    let node_id = if !args.node_id.is_empty() && args.node_id != " " {
        args.node_id.clone()
    } else {
        mac_address::get_mac_address().unwrap().unwrap().to_string()
    };
    info!("node_id = [{}]", node_id);

    match bench_all(cfg.clone(), node_id).await {
        Ok(_) => {}
        Err(e) => {
            error!("bench result error [{}]", e);
        }
    }
}
