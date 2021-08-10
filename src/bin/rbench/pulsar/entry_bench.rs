use std::sync::Arc;

use super::config::Config;
use crate::common;
use async_trait::async_trait;
use bytes::Bytes;
use futures::TryStreamExt;
use pulsar::{
    error::ConsumerError, message::proto, producer, Error as PulsarError, Pulsar, SerializeMessage,
    TokioExecutor,
};
use pulsar::{
    message::proto::command_subscribe::SubType, message::Payload, Consumer, DeserializeMessage,
};
use pulsar::{ConsumerOptions, Producer};
// use serde::{Deserialize, Serialize};
use tracing::{error, trace};

// #[derive(Serialize, Deserialize)]
// struct TestData {
//     data: String,
// }

// // impl<'a> SerializeMessage for &'a TestData {
// //     fn serialize_message(input: Self) -> Result<producer::Message, PulsarError> {
// //         let payload = serde_json::to_vec(input).map_err(|e| PulsarError::Custom(e.to_string()))?;
// //         Ok(producer::Message {
// //             payload,
// //             ..Default::default()
// //         })
// //     }
// // }

// impl SerializeMessage for TestData {
//     fn serialize_message(input: Self) -> Result<producer::Message, PulsarError> {
//         let payload = serde_json::to_vec(&input).map_err(|e| PulsarError::Custom(e.to_string()))?;
//         Ok(producer::Message {
//             payload,
//             ..Default::default()
//         })
//     }
// }

// impl DeserializeMessage for TestData {
//     type Output = Result<TestData, serde_json::Error>;

//     fn deserialize_message(payload: &Payload) -> Self::Output {
//         serde_json::from_slice(&payload.data)
//     }
// }

// async fn test() -> Result<(), PulsarError> {
//     let addr = "pulsar://127.0.0.1:6650";

//     let pulsar: Pulsar<_> = Pulsar::builder(addr, TokioExecutor).build().await?;
//     let recv_h = tokio::spawn(async move {
//         let mut consumer: Consumer<TestData, _> = pulsar
//             .consumer()
//             .with_topic("persistent://public/default/test")
//             // .with_consumer_name("test_consumer")
//             .with_subscription_type(SubType::Exclusive)
//             // .with_subscription("test_subscription")
//             .build()
//             .await?;

//         let mut counter = 0usize;
//         while let Some(msg) = consumer.try_next().await? {
//         // while let Some(r) = consumer.next().await {
//             // let msg = r?;
//             consumer.ack(&msg).await?;
//             let data = match msg.deserialize() {
//                 Ok(data) => data,
//                 Err(e) => {
//                     error!("could not deserialize message: {:?}", e);
//                     break;
//                 }
//             };

//             if data.data.as_str() != "data" {
//                 error!("Unexpected payload: {}", &data.data);
//                 break;
//             }
//             counter += 1;
//             info!("got {} messages, meta={:?}", counter, msg.metadata());
//         }
//         info!("consume done");
//         Ok::<(), PulsarError>(())
//     });

//     let pulsar: Pulsar<_> = Pulsar::builder(addr, TokioExecutor).build().await?;
//     let send_h = tokio::spawn(async move {
//         let mut producer = pulsar
//             .producer()
//             .with_topic("persistent://public/default/test")
//             // .with_name("my producer")
//             .with_options(producer::ProducerOptions {
//                 schema: Some(proto::Schema {
//                     r#type: proto::schema::Type::String as i32,
//                     ..Default::default()
//                 }),
//                 ..Default::default()
//             })
//             .build()
//             .await?;

//         let mut counter = 0usize;
//         for _ in 0..3usize {
//             producer
//                 .send(TestData {
//                     data: "data".to_string(),
//                 })
//                 .await?;

//             counter += 1;
//             println!("producer: {} messages", counter);
//             tokio::time::sleep(std::time::Duration::from_millis(1000)).await;
//         }
//         Ok::<(), PulsarError>(())
//     });

//     let r = send_h.await;
//     debug!("send done with {:?}", r);
//     let r = recv_h.await;
//     debug!("recv done with {:?}", r);
//     std::process::exit(0);
// }

impl From<PulsarError> for common::Error {
    fn from(error: PulsarError) -> Self {
        common::Error::Generic(error.to_string())
    }
}

impl From<ConsumerError> for common::Error {
    fn from(error: ConsumerError) -> Self {
        common::Error::Generic(error.to_string())
    }
}

impl From<serde_json::Error> for common::Error {
    fn from(error: serde_json::Error) -> Self {
        common::Error::Generic(error.to_string())
    }
}

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

struct Puber {
    cfg: Arc<Config>,
    pulsar: Option<Pulsar<TokioExecutor>>,
    producer: Option<Producer<TokioExecutor>>,
}

#[async_trait]
impl common::Puber for Puber {
    async fn connect(&mut self) -> Result<(), common::Error> {
        let pulsar: Pulsar<_> = Pulsar::builder(&self.cfg.env().address, TokioExecutor)
            .build()
            .await?;
        let producer = pulsar
            .producer()
            .with_topic(self.cfg.pub_topic())
            // .with_name("my producer")
            .with_options(producer::ProducerOptions {
                schema: Some(proto::Schema {
                    r#type: proto::schema::Type::None as i32,
                    ..Default::default()
                }),
                ..Default::default()
            })
            .build()
            .await?;
        self.producer = Some(producer);
        self.pulsar = Some(pulsar);
        Ok(())
    }

    async fn disconnect(&mut self) -> Result<(), common::Error> {
        if let Some(pulsar) = self.pulsar.take() {
            self.producer.take().unwrap();
            drop(pulsar);
        }
        Ok(())
    }

    async fn send(&mut self, data: Bytes) -> Result<(), common::Error> {
        self.producer.as_mut().unwrap().send(Data(data)).await?;
        Ok(())
    }

    async fn idle(&mut self) -> Result<(), common::Error> {
        Ok(())
    }
}

struct Suber {
    cfg: Arc<Config>,
    pulsar: Option<Pulsar<TokioExecutor>>,
    consumer: Option<Consumer<Data, TokioExecutor>>,
}

#[async_trait]
impl common::Suber for Suber {
    async fn connect(&mut self) -> Result<(), common::Error> {
        let pulsar: Pulsar<_> = Pulsar::builder(&self.cfg.env().address, TokioExecutor)
            .build()
            .await?;
        let consumer: Consumer<Data, _> = pulsar
            .consumer()
            .with_options(ConsumerOptions {
                schema: Some(proto::Schema {
                    r#type: proto::schema::Type::None as i32,
                    ..Default::default()
                }),
                ..Default::default()
            })
            .with_topic(self.cfg.sub_topic())
            // .with_consumer_name("test_consumer")
            .with_subscription_type(SubType::Shared)
            // .with_subscription("test_subscription")
            .build()
            .await?;
        self.consumer = Some(consumer);
        self.pulsar = Some(pulsar);
        Ok(())
    }

    async fn disconnect(&mut self) -> Result<(), common::Error> {
        if let Some(pulsar) = self.pulsar.take() {
            self.consumer.take().unwrap();
            drop(pulsar);
        }
        Ok(())
    }

    async fn recv(&mut self) -> Result<Bytes, common::Error> {
        let msg = self.consumer.as_mut().unwrap().try_next().await?.unwrap();
        self.consumer.as_mut().unwrap().ack(&msg).await?;
        let data = msg.deserialize()?;
        Ok(data.0)
    }
}

pub async fn bench_all(cfgw: Arc<Config>) -> Result<(), common::Error> {
    let mut launcher = common::Launcher::new();
    let mut sub_id = 0u64;
    let mut pub_id = 0u64;

    if cfgw.raw().subs.connections > 0 {
        let _r = launcher
            .launch_sub_sessions(
                "subs".to_string(),
                &mut sub_id,
                cfgw.raw().subs.connections,
                cfgw.raw().subs.conn_per_sec,
                |_n| Suber {
                    cfg: cfgw.clone(),
                    pulsar: None,
                    consumer: None,
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

        let _r = launcher
            .launch_pub_sessions(
                "pubs".to_string(),
                &mut pub_id,
                cfgw.raw().pubs.connections,
                cfgw.raw().pubs.conn_per_sec,
                args,
                |_n| Puber {
                    cfg: cfgw.clone(),
                    pulsar: None,
                    producer: None,
                },
            )
            .await?;
    }

    let cfg = cfgw.raw();
    launcher.kick_and_wait(cfg.recv_timeout_ms).await?;

    Ok(())
}

pub async fn run(config_file: &str) {
    // let r = test().await;
    // if r.is_err() {
    //     error!("{:?}", r);
    // }

    let cfg = Config::load_from_file(&config_file);
    trace!("cfg=[{:#?}]", cfg.raw());
    let cfg = Arc::new(cfg);

    match bench_all(cfg.clone()).await {
        Ok(_) => {}
        Err(e) => {
            error!("bench result error [{}]", e);
        }
    }
}
