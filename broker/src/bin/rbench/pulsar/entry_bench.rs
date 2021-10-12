use std::sync::Arc;

use super::config::Config;
use crate::common;
use crate::common::config::make_pubsub_topics;
use async_trait::async_trait;
use bytes::Bytes;
use futures::TryStreamExt;
use pulsar::{
    message::proto, producer, Error as PulsarError, Pulsar, SerializeMessage, TokioExecutor,
};
use pulsar::{
    message::proto::command_subscribe::SubType, message::Payload, Consumer, DeserializeMessage,
};
use pulsar::{ConsumerOptions, Producer};
// use serde::{Deserialize, Serialize};
use anyhow::Result;
use rust_threeq::tq3::{try_poll, Flight, Inflights};
use tracing::{error, info, trace};

struct Data(Bytes);

impl SerializeMessage for Data {
    fn serialize_message(input: Self) -> Result<producer::Message, PulsarError> {
        Ok(producer::Message {
            payload: input.0.to_vec(),
            ..Default::default()
        })
    }
}

impl DeserializeMessage for Data {
    type Output = Result<Data, serde_json::Error>;

    fn deserialize_message(payload: &Payload) -> Self::Output {
        Ok(Data(Bytes::copy_from_slice(&payload.data)))
    }
}

struct PubFlight {
    future: producer::SendFuture,
}

#[async_trait]
impl Flight for PubFlight {
    type Output = pulsar::CommandSendReceipt;

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
    topic: String,
    pulsar: Pulsar<TokioExecutor>,
    producer: Option<Producer<TokioExecutor>>,
    inflights: Inflights<PubFlight>,
    config: Arc<Config>,
}

#[async_trait]
impl common::Puber for Puber {
    async fn connect(&mut self) -> Result<()> {
        info!("pub topic {}", self.topic);
        let batch_size = if self.config.raw().pubs.inflights > 1 {
            Some(self.config.raw().pubs.inflights as u32)
        } else {
            None
        };
        let producer = self
            .pulsar
            .producer()
            .with_topic(&self.topic)
            // .with_name("my producer")
            .with_options(producer::ProducerOptions {
                batch_size,
                schema: Some(proto::Schema {
                    r#type: proto::schema::Type::None as i32,
                    ..Default::default()
                }),
                ..Default::default()
            })
            .build()
            .await?;
        self.producer = Some(producer);
        // self.pulsar = Some(pulsar);
        Ok(())
    }

    async fn disconnect(&mut self) -> Result<()> {
        if let Some(producer) = self.producer.take() {
            drop(producer);
        }
        Ok(())
    }

    async fn send(&mut self, data: Bytes) -> Result<()> {
        let _r = self.producer.as_mut().unwrap().send(Data(data)).await?;
        // self.inflights
        //     .add_and_check(PubFlight { future: r }, self.config.raw().pubs.inflights)
        //     .await?;
        Ok(())
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
    id: String,
    topic: String,
    pulsar: Pulsar<TokioExecutor>,
    consumer: Option<Consumer<Data, TokioExecutor>>,
}

#[async_trait]
impl common::Suber for Suber {
    async fn connect(&mut self) -> Result<()> {
        let consumer: Consumer<Data, _> = self
            .pulsar
            .consumer()
            .with_options(ConsumerOptions {
                schema: Some(proto::Schema {
                    r#type: proto::schema::Type::None as i32,
                    ..Default::default()
                }),
                durable: Some(false),
                ..Default::default()
            })
            .with_topic(&self.topic)
            .with_consumer_name(self.id.clone())
            .with_subscription_type(SubType::Exclusive)
            .with_subscription(self.id.clone())
            .build()
            .await?;
        self.consumer = Some(consumer);
        // self.pulsar = Some(pulsar);
        Ok(())
    }

    async fn disconnect(&mut self) -> Result<()> {
        if let Some(consumer) = self.consumer.take() {
            drop(consumer);
        }
        Ok(())
    }

    async fn recv(&mut self) -> Result<Bytes> {
        let msg = self.consumer.as_mut().unwrap().try_next().await?.unwrap();
        self.consumer.as_mut().unwrap().ack(&msg).await?;
        let data = msg.deserialize()?;
        Ok(data.0)
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
    info!("pub inflights: {}", cfgw.raw().pubs.inflights);

    let pulsar: Pulsar<_> = Pulsar::builder(&cfgw.env().address, TokioExecutor)
        .build()
        .await?;

    if sub_topics.len() > 0 {
        let _r = bencher
            .launch_sub_sessions(
                "subs".to_string(),
                &mut sub_id,
                sub_topics.len() as u64,
                cfgw.raw().subs.conn_per_sec,
                |n| Suber {
                    id: format!("{}-sub-{}", node_id, n),
                    topic: sub_topics.pop().unwrap(),
                    pulsar: pulsar.clone(),
                    consumer: None,
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
                    let o = Puber {
                        topic: item.1,
                        pulsar: pulsar.clone(),
                        producer: None,
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
