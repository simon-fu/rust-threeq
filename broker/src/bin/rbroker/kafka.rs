use std::time::Duration;

use anyhow::Context;
use anyhow::Result;

use log::error;
use log::info;

use rdkafka::config::ClientConfig;
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::Consumer;
use rdkafka::producer::FutureProducer;
use rdkafka::producer::FutureRecord;

async fn consume_loop(consumer: StreamConsumer) -> Result<()> {
    loop {
        let borrowed_message = consumer.recv().await.context("Consumer recv failed")?;
        let msg = borrowed_message.detach();
        info!("got msg {:?}", msg);
        // consumer.commit_message(&borrowed_message, rdkafka::consumer::CommitMode::Sync).unwrap();
    }
}

async fn run_consumer(brokers: String, topic: String, group_id: String) -> Result<()> {
    // config refer: https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
    let consumer: StreamConsumer = ClientConfig::new()
        .set("group.id", &group_id)
        .set("bootstrap.servers", &brokers)
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "6000")
        .set("enable.auto.commit", "false")
        .create()
        .context("Consumer creation failed")?;

    consumer
        .subscribe(&[&topic])
        .context("Can't subscribe to specified topic")?;
    info!("consuming topic [{}]", topic);
    consume_loop(consumer).await?;
    Ok(())
}

async fn run_producer(brokers: String, topic: String) -> Result<()> {
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .set("message.timeout.ms", "5000")
        .create()
        .context("Producer creation error")?;

    info!("Producer created");
    for _ in 0..100usize {
        let produce_future = producer.send(
            FutureRecord::to(&topic).key("some key").payload("abc"),
            rdkafka::util::Timeout::Never,
        );
        match produce_future.await {
            Ok(delivery) => info!("Sent: {:?}", delivery),
            Err((e, _)) => error!("Error: {:?}", e),
        }
    }
    Ok(())
}

pub async fn run() -> Result<()> {
    let brokers = "localhost:9092".to_owned();
    let topic = "test1".to_owned();
    let group_id = "group1".to_owned();

    let h = tokio::spawn(run_consumer(
        brokers.clone(),
        topic.clone(),
        group_id.clone(),
    ));

    if topic.len() == 0 {
        tokio::time::sleep(Duration::from_secs(3)).await;
        run_producer(brokers.clone(), topic.clone()).await?;
    }

    h.await??;
    // std::process::exit(0);
    Ok(())
}
