use futures::TryStreamExt;
use pulsar::{
    message::proto, producer, Error as PulsarError, Pulsar, SerializeMessage, TokioExecutor,
};
use pulsar::{
    message::proto::command_subscribe::SubType, message::Payload, Consumer, DeserializeMessage,
};
use serde::{Deserialize, Serialize};
use tracing::{error, info};

#[derive(Serialize, Deserialize)]
struct TestData {
    data: String,
}

// impl<'a> SerializeMessage for &'a TestData {
//     fn serialize_message(input: Self) -> Result<producer::Message, PulsarError> {
//         let payload = serde_json::to_vec(input).map_err(|e| PulsarError::Custom(e.to_string()))?;
//         Ok(producer::Message {
//             payload,
//             ..Default::default()
//         })
//     }
// }

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

async fn test() -> Result<(), PulsarError> {
    let addr = "pulsar://127.0.0.1:6650";

    let pulsar: Pulsar<_> = Pulsar::builder(addr, TokioExecutor).build().await?;
    let recv_h = tokio::spawn(async move {
        let mut consumer: Consumer<TestData, _> = pulsar
            .consumer()
            .with_topic("non-persistent://public/default/test")
            .with_consumer_name("test_consumer")
            .with_subscription_type(SubType::Exclusive)
            .with_subscription("test_subscription")
            .build()
            .await?;

        let mut counter = 0usize;
        while let Some(msg) = consumer.try_next().await? {
            consumer.ack(&msg).await?;
            let data = match msg.deserialize() {
                Ok(data) => data,
                Err(e) => {
                    error!("could not deserialize message: {:?}", e);
                    break;
                }
            };

            if data.data.as_str() != "data" {
                error!("Unexpected payload: {}", &data.data);
                break;
            }
            counter += 1;
            info!("got {} messages", counter);
        }
        Ok::<(), PulsarError>(())
    });

    let pulsar: Pulsar<_> = Pulsar::builder(addr, TokioExecutor).build().await?;
    let send_h = tokio::spawn(async move {
        let mut producer = pulsar
            .producer()
            .with_topic("non-persistent://public/default/test")
            .with_name("my producer")
            .with_options(producer::ProducerOptions {
                schema: Some(proto::Schema {
                    r#type: proto::schema::Type::String as i32,
                    ..Default::default()
                }),
                ..Default::default()
            })
            .build()
            .await?;

        let mut counter = 0usize;
        for _ in 0..3usize {
            producer
                .send(TestData {
                    data: "data".to_string(),
                })
                .await?;

            counter += 1;
            println!("producer: {} messages", counter);
            tokio::time::sleep(std::time::Duration::from_millis(1000)).await;
        }
        Ok::<(), PulsarError>(())
    });

    let _r = send_h.await;
    let _r = recv_h.await;
    std::process::exit(0);
}

pub async fn run(_config_file: &str) {
    let r = test().await;
    if r.is_err() {
        error!("{:?}", r);
    }
}
