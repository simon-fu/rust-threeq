use std::fmt::Debug;

use clap::Clap;
use rust_threeq::tq3::{self, tt};
use tracing::{error, info};

// refer https://github.com/clap-rs/clap/tree/master/clap_derive/examples
#[derive(Clap, Debug, Default)]
#[clap(name = "threeq bench", author, about, version)]
struct Config {
    #[clap(
        short = 'a',
        long = "address",
        default_value = "127.0.0.1:1883",
        long_about = "broker address, ip:port."
    )]
    address: String,

    #[clap(
        short = 'u',
        long = "username",
        default_value = "",
        long_about = "username."
    )]
    username: String,

    #[clap(
        short = 'p',
        long = "password",
        default_value = "",
        long_about = "password."
    )]
    password: String,
}

async fn recv_loop(mut receiver: tt::client::Receiver) -> Result<(), tt::client::Error> {
    loop {
        let event = receiver.recv().await?;
        info!("event {:?}", event);
    }
}

async fn bench() -> Result<(), tt::client::Error> {
    // let addr = "broker.emqx.io:1883";

    let addr = "95kih0.cn1.mqtt.chat:1883";
    let username = "test1";
    let password = "YWMtXZ3kYN7QEeufbVFUddJ-E2vlGG4eL0ZcunS8bNvNvrlbpEww3tAR64PsX55PB1tdAwMAAAF6fu6lZABPGgCIn5vpQGRkEULFZamZ0xpDYdLKmou1tCflH8CPwNi4UA";
    let client_id = "dev111@95kih0";

    // let addr = "172.17.1.160:1883";
    // let username = "test1";
    // let password = "YWMtXZ3kYN7QEeufbVFUddJ-E2vlGG4eL0ZcunS8bNvNvrlbpEww3tAR64PsX55PB1tdAwMAAAF6fu6lZABPGgCIn5vpQGRkEULFZamZ0xpDYdLKmou1tCflH8CPwNi4UA";
    // let client_id = "dev111@1PGUGY";

    // let addr = "mqtt-ejabberd-hsb.easemob.com:2883";
    // let username = "test-ljh2";
    // let password = "YWMtBeUx0NfpEeuG9u0EJlumBegrzF8zZk2Wp8GS3pF-orBnUI9QkdAR66aBgQQ44eDgAwMAAAF6UbAwbwBPGgCZG2uBHDrvCLM7SH4UTlW3piJwMgU5bfGByO8pgLz77Q";
    // let client_id = "dev111@1PGUGY";

    let (mut sender, receiver) = tt::client::make_connection("", addr).await?.split();

    let task = tokio::spawn(async move {
        if let Err(e) = recv_loop(receiver).await {
            error!("recv_loop error: {}", e);
        }
    });

    let mut pkt = tt::Connect::new(client_id);
    pkt.protocol = tt::Protocol::V5;
    pkt.keep_alive = 5;
    pkt.set_login(username, password);
    let conn_ack = sender.connect(pkt).await?;
    info!("conn_ack {:?}", conn_ack);

    let sub_ack = sender
        .subscribe(tt::Subscribe::new("t1/t2/#", tt::QoS::ExactlyOnce))
        .await?;
    info!("sub_ack {:?}", sub_ack);

    let _r = sender
        .publish(tt::Publish::new(
            "t1/t2/qos0",
            tt::QoS::AtMostOnce,
            vec![00u8, 22u8],
        ))
        .await?;
    info!("publish qos0 ok");

    let _r = sender
        .publish(tt::Publish::new(
            "t1/t2/qos1",
            tt::QoS::AtLeastOnce,
            vec![11u8, 22u8],
        ))
        .await?;
    info!("publish qos1 ok");

    let _r = sender
        .publish(tt::Publish::new(
            "t1/t2/qos2",
            tt::QoS::ExactlyOnce,
            vec![22u8, 22u8],
        ))
        .await?;
    info!("publish qos2 ok");

    let unsub_ack = sender.unsubscribe(tt::Unsubscribe::new("t1/t2/#")).await?;
    info!("unsub_ack {:?}", unsub_ack);

    let _r = sender
        .publish(tt::Publish::new(
            "t1/t2/qos0",
            tt::QoS::AtMostOnce,
            vec![00u8, 22u8],
        ))
        .await?;
    info!("publish qos1 ok");

    let _ = task.await;

    let _ = sender.disconnect(tt::Disconnect::new()).await?;
    Ok(())
}

#[tokio::main]
async fn main() {
    tq3::log::tracing_subscriber::init();

    //let cfg = Config::parse();
    //debug!("cfg={:?}", cfg);

    match bench().await {
        Ok(_) => {
            info!("bench result ok");
        }
        Err(e) => {
            error!("bench result error [{}]", e);
        }
    }
}
