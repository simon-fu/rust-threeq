use std::time::Duration;

use futures::TryStreamExt;
use pulsar::{
    error::ConsumerError, producer, Consumer, DeserializeMessage, Error as PulsarError, Payload,
    Pulsar, SerializeMessage, TokioExecutor,
};
use serde::{Deserialize, Serialize};
use tracing::{debug, error, info, trace};

use super::config::Config;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("error: {0}")]
    Generic(String),
}

impl From<PulsarError> for Error {
    fn from(error: PulsarError) -> Self {
        Error::Generic(error.to_string())
    }
}

impl From<ConsumerError> for Error {
    fn from(error: ConsumerError) -> Self {
        Error::Generic(error.to_string())
    }
}

impl From<serde_json::Error> for Error {
    fn from(error: serde_json::Error) -> Self {
        Error::Generic(error.to_string())
    }
}

#[derive(Serialize, Deserialize)]
struct TestData(String);

impl SerializeMessage for TestData {
    fn serialize_message(input: Self) -> Result<producer::Message, PulsarError> {
        let payload = serde_json::to_vec(&input).map_err(|e| PulsarError::Custom(e.to_string()))?;
        Ok(producer::Message {
            payload,
            ..Default::default()
        })
    }
}

impl DeserializeMessage for TestData {
    type Output = Result<TestData, serde_json::Error>;

    fn deserialize_message(payload: &Payload) -> Self::Output {
        serde_json::from_slice(&payload.data)
    }
}

async fn verify_basic(cfgw: &Config) -> Result<(), Error> {
    let pub_topic = cfgw.raw().pubs.topic.clone(); // "persistent://tentent1/ns1/t1#";
    let sub_topic = if cfgw.raw().subs.topic != "-" {
        cfgw.raw().subs.topic.clone()
    } else {
        pub_topic.clone()
    };
    let content = "hello world".to_string();
    let num_msgs = 5usize;

    debug!("pub_topic: {}", pub_topic);
    debug!("sub_topic: {}", sub_topic);

    let pulsar: Pulsar<_> = Pulsar::builder(&cfgw.env().address, TokioExecutor)
        .build()
        .await?;

    // let mut cosumer: Consumer<TestData, _> = pulsar.consumer()
    // .with_topic_regex(regex::Regex::new(&sub_topic).unwrap())
    // // .with_topic_refresh(Duration::from_millis(100))
    // .build().await?;

    let mut cosumer: Consumer<TestData, _> =
        pulsar.consumer().with_topic(sub_topic).build().await?;

    let recv_h = tokio::spawn(async move {
        for _ in 0..num_msgs * 2 {
            let msg = cosumer.try_next().await?.unwrap();
            let data = msg.deserialize()?;
            debug!(
                "recv message, {:?}, content {:?}",
                msg.message_id.id, data.0
            );
            cosumer.ack(&msg).await?;
        }
        Ok::<(), Error>(())
    });

    tokio::time::sleep(Duration::from_secs(1)).await;
    let mut producer = pulsar.producer().with_topic(pub_topic).build().await?;

    for n in 0..num_msgs {
        tokio::time::sleep(Duration::from_secs(1)).await;
        let r = producer
            .send(TestData(format!("NO.{} {}", n, content)))
            .await?;
        let r = r.await?; // wait for ack
        debug!("sent message, n {}, {:?}", n, r.message_id);
    }

    recv_h.await.unwrap()?;

    Ok(())
}

async fn verify_with_cfg(cfgw: Config) -> Result<(), Error> {
    verify_basic(&cfgw).await?;

    Ok(())
}

pub async fn run(args: &super::Args) {
    let cfgw = Config::load_from_file(&args.config_file);
    trace!("cfg=[{:?}]", cfgw);
    info!("-");
    info!("env=[{}]", cfgw.raw().env);
    info!("-");

    match verify_with_cfg(cfgw).await {
        Ok(_) => {
            info!("final ok");
        }
        Err(e) => {
            error!("final error [{}]", e);
        }
    }
}
