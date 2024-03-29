use anyhow::Result;
// use clap::Clap;
use clap::{Parser, Subcommand};
use rust_threeq::tq3;
use rust_threeq::tq3::app;

mod kafka;
mod mqtt;
mod pcap;
mod pulsar;
mod util;
mod token;

// refer https://github.com/clap-rs/clap/tree/master/clap_derive/examples
#[derive(Parser, Debug)] 
#[clap(name = "rthreeq tools", author, about, version=app::version_long())]
struct CmdArgs {
    #[clap(subcommand)]
    cmd: SubCmd,
}

#[derive(Subcommand, Debug)]
enum SubCmd {
    PulsarRead(pulsar::ReadArgs),
    KafkaRead(kafka::ReadArgs),
    MqttPcap(pcap::ReadMqttPcapArgs),
    MqttSub(mqtt::SubArgs),
    Token(token::SubArgs),
}

#[tokio::main]
async fn main() -> Result<()> {
    // RUST_LOG=hyper=warn,reqwest=warn,debug cargo run --release --bin rtools -- pulsar-read  --topic persistent://easemob/default/ev0
    // tracing_subscriber::fmt::init();
    // tq3::log::tracing_subscriber::init();
    // tq3::log::tracing_subscriber::init_with_filters("debug,pulsar=warn,hyper=warn,reqwest=warn");
    tq3::log::init()?;

    let args = CmdArgs::parse();

    match args.cmd {
        SubCmd::PulsarRead(opts) => pulsar::run_read(&opts).await?,
        SubCmd::KafkaRead(opts) => kafka::run_read(&opts).await?,
        SubCmd::MqttPcap(opts) => pcap::run_read_mqtt_pcap_file(&opts).await?,
        SubCmd::MqttSub(opts) => mqtt::run_sub(&opts).await?,
        SubCmd::Token(opts) => token::run_sub(&opts).await?,
    }

    Ok(())
}
