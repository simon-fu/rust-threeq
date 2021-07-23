// cargo bench --bench bench-async

use clap::Clap;
use histogram::Histogram;
use rust_threeq::tq3;
use rust_threeq::tq3::hub;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tracing::info;

#[derive(Clap, Debug)]
#[clap(name = "bench-async", author, about, version)]
struct CmdArgs {
    #[clap(subcommand)]
    subcmd: SubCommand,
}

#[derive(Clap, Debug)]
enum SubCommand {
    Hub(HubArgs),
}

#[derive(Clap, Debug)]
struct HubArgs {
    #[clap(long, long_about = "num of subscribers", default_value = "100000")]
    subs: u64,

    #[clap(long, long_about = "num of hubs", default_value = "0")]
    hubs: u64,

    #[clap(long)]
    bench: bool,
}

type Message = Instant;
pub type Subscriptions = hub::LatestFirstQue<Arc<Message>>;
pub type Hub = hub::Hub<Arc<Message>, u64, Subscriptions>;

async fn spawn_subs(hub: &mut Hub, num: u64, tx: &mpsc::UnboundedSender<Duration>) {
    for n in 0..num {
        let subs = Subscriptions::new(1);
        let subs = Arc::new(subs);
        hub.add(n, subs.clone()).await;
        let tx0 = tx.clone();
        let _h1 = tokio::spawn(async move {
            let r = subs.recv().await.unwrap();
            let _r = tx0.send(Instant::now() - *r.0);
        });
    }
}

async fn run_hubs(hubs: u64, subs: u64, hist: &mut Histogram) {
    let (tx, mut rx) = mpsc::unbounded_channel();

    for _ in 0..hubs {
        let mut hub = Hub::new();
        spawn_subs(&mut hub, subs, &tx).await;
        hub.push(&Arc::new(Instant::now())).await;
    }
    drop(tx);

    loop {
        match rx.recv().await {
            Some(d) => {
                let _r = hist.increment(d.as_nanos() as u64);
            }
            None => {
                break;
            }
        }
    }
}

async fn measure_hubs(name: &str, hubs: u64, subs: u64) {
    info!("{}: hubs {}, subs {}", name, hubs, subs);
    let mut hist = Histogram::new();
    tq3::measure_and_print(name, hubs * subs, run_hubs(hubs, subs, &mut hist)).await;
    tq3::histogram::print_duration(&name, &hist);
}

async fn bench_hub(args: HubArgs) {
    {
        info!("-");
        info!("warm up ...");
        let mut hist = Histogram::new();
        run_hubs(1, 10000, &mut hist).await;
        info!("warm up done");
    }

    if args.hubs == 0 {
        info!("-");
        measure_hubs("Hub 1-to-N1", 1, 100000).await;

        info!("-");
        measure_hubs("Hub M-to-N2", 10, 100000).await;

        info!("-");
        measure_hubs("Hub M-to-N1", 10, 10000).await;
    } else {
        info!("-");
        measure_hubs("Hub latency", 1, 100000).await;
    }
}

#[tokio::main]
async fn main() {
    rust_threeq::tq3::log::tracing_subscriber::init();

    info!("-");

    let args: Vec<String> = std::env::args().collect();
    info!("{:?}", args);

    let args = CmdArgs::parse();
    info!("args={:?}", args);

    match args.subcmd {
        SubCommand::Hub(sub_args) => {
            bench_hub(sub_args).await;
        }
    }
}
