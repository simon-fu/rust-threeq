use std::{fmt::Debug};


use clap::Clap;
use rust_threeq::tq3::{self, tt};
use tracing::{debug, error, info};



// refer https://github.com/clap-rs/clap/tree/master/clap_derive/examples
#[derive(Clap, Debug, Default)]
#[clap(name = "threeq broker", author, about, version)]
struct Config {
    #[clap(
        short = 'l',
        long = "tcp-listen",
        default_value = "0.0.0.0:1883",
        long_about = "tcp listen address."
    )]
    tcp_listen_addr: String,

    #[clap(
        short = 'g',
        long = "enable_gc",
        long_about = "enable memory garbage collection"
    )]
    enable_gc: bool,
}

async fn recv_loop(mut receiver : tt::client::Receiver) -> Result<(), tt::client::Error>{
    loop{
        let event = receiver.recv().await?;
        info!("event {:?}", event);
        if let tt::client::Event::Error(e) = event {
            return Err(e);
        }
    }
}

async fn bench() -> Result<(), tt::client::Error>{
    // let addr = "broker.emqx.io:1883";

    // let addr = "95kih0.cn1.mqtt.chat:1883";
    // let username = "test1";
    // let password = "YWMtXZ3kYN7QEeufbVFUddJ-E2vlGG4eL0ZcunS8bNvNvrlbpEww3tAR64PsX55PB1tdAwMAAAF6fu6lZABPGgCIn5vpQGRkEULFZamZ0xpDYdLKmou1tCflH8CPwNi4UA";
    // let client_id = "dev111@95kih0";

    let addr = "172.17.1.160:1883";
    let username = "test1";
    let password = "YWMtXZ3kYN7QEeufbVFUddJ-E2vlGG4eL0ZcunS8bNvNvrlbpEww3tAR64PsX55PB1tdAwMAAAF6fu6lZABPGgCIn5vpQGRkEULFZamZ0xpDYdLKmou1tCflH8CPwNi4UA";
    let client_id = "dev111@1PGUGY";


    let (mut sender, receiver) = tt::client::make_connection(addr).await?;

    let task = tokio::spawn(async move {
        if let Err(e) = recv_loop(receiver).await {
            error!("recv_loop error: {}", e);
        }
    });

    let mut pkt = tt::Connect::new(client_id);
    pkt.set_login(username, password);
    let conn_ack = sender.connect(pkt).await?;
    info!("conn_ack {:?}", conn_ack);
    
    let sub_ack = sender.subscribe(tt::Subscribe::new("t1/t2/#", tt::QoS::ExactlyOnce)).await?;
    info!("sub_ack {:?}", sub_ack);

    // let _r = sender.publish(tt::Publish::new("t1/t2/qos0", tt::QoS::AtMostOnce, vec![00u8, 22u8])).await?;
    // info!("publish qos0 ok");

    // let _r = sender.publish(tt::Publish::new("t1/t2/qos1", tt::QoS::AtLeastOnce, vec![11u8, 22u8])).await?;
    // info!("publish qos1 ok");

    // let _r = sender.publish(tt::Publish::new("t1/t2/qos2", tt::QoS::ExactlyOnce, vec![22u8, 22u8])).await?;
    // info!("publish qos2 ok");

    let _ = task.await;

    let _ = sender.disconnect(tt::Disconnect::new()).await?;
    Ok(())
}


#[tokio::main]
async fn main() {
    tq3::log::tracing_subscriber::init();

    let cfg = Config::parse();
    debug!("cfg={:?}", cfg);

    match bench().await{
        Ok(_) => { info!("bench result ok"); },
        Err(e) => { error!("bench result error [{}]", e); },
    }

}
