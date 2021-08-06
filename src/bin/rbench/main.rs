use clap::Clap;
use rust_threeq::tq3;
use std::fmt::Debug;
mod mqtt;
mod pulsar;

// refer https://github.com/clap-rs/clap/tree/master/clap_derive/examples
#[derive(Clap, Debug)]
#[clap(name = "threeq bench", author, about, version)]
struct CmdArgs {
    //#[clap(arg_enum, short = 't', long = "type", long_about = "bench type", default_value = "mqtt")]
    #[clap(subcommand)]
    cmd: SubCmd,
}

// #[derive(ArgEnum, Debug, PartialEq)]
#[derive(Clap, Debug, PartialEq)]
enum SubCmd {
    Mqtt(mqtt::Args),
    Pulsar(pulsar::Args),
}

#[tokio::main]
async fn main() {
    tq3::log::tracing_subscriber::init();

    let args = CmdArgs::parse();

    match args.cmd {
        SubCmd::Mqtt(opt) => mqtt::run(&opt).await,
        SubCmd::Pulsar(opt) => pulsar::run(&opt).await,
    }
}
