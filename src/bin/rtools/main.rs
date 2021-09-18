use anyhow::Result;
use clap::Clap;
use rust_threeq::tq3;

mod pulsar;

// refer https://github.com/clap-rs/clap/tree/master/clap_derive/examples
#[derive(Clap, Debug)]
#[clap(name = "threeq tools", author, about, version)]
struct CmdArgs {
    #[clap(subcommand)]
    cmd: SubCmd,
}

#[derive(Clap, Debug, PartialEq)]
enum SubCmd {
    PulsarRead(pulsar::ReadArgs),
}

#[tokio::main]
async fn main() -> Result<()> {
    // RUST_LOG=hyper=warn,reqwest=warn,debug cargo run --release --bin rtools -- pulsar-read  --topic persistent://easemob/default/ev0
    // tracing_subscriber::fmt::init();
    // tq3::log::tracing_subscriber::init();
    tq3::log::tracing_subscriber::init_with_filters("pulsar=warn,hyper=warn,reqwest=warn");

    let args = CmdArgs::parse();

    match args.cmd {
        SubCmd::PulsarRead(opt) => pulsar::run_read(&opt).await?,
    }

    Ok(())
}
