// cargo bench --bench bench-async

use histogram::Histogram;
use rust_threeq::tq3;
use rust_threeq::tq3::hub;
use std::sync::Arc;
use std::time::Instant;
use tracing::info;

async fn run_hub_1_to_n(num: u64, hist: &mut Histogram) {
    type Message = Instant;
    pub type Subscriptions = hub::LatestFirstQue<Arc<Message>>;
    pub type Hub = hub::Hub<Arc<Message>, u64, Subscriptions>;

    let hub = Hub::new();
    let (tx_final, mut rx_final) = tokio::sync::mpsc::unbounded_channel();

    for n in 0..num {
        let subs = Subscriptions::new(1);
        let subs = Arc::new(subs);
        hub.add(n, subs.clone()).await;
        let tx0 = tx_final.clone();
        let _h1 = tokio::spawn(async move {
            let r = subs.recv().await.unwrap();
            let _r = tx0.send(Instant::now() - *r.0);
        });
    }
    drop(tx_final);

    hub.push(&Arc::new(Instant::now())).await;

    loop {
        match rx_final.recv().await {
            Some(d) => {
                let _r = hist.increment(d.as_nanos() as u64);
            }
            None => {
                break;
            }
        }
    }
}

async fn measure_hub_1_to_n(num: u64) {
    let name = "Hub 1-to-N";
    let mut hist = Histogram::new();
    tq3::measure_and_print(&name, num, run_hub_1_to_n(num, &mut hist)).await;
    tq3::histogram::print_duration(&name, &hist);
}

#[tokio::main]
async fn main() {
    rust_threeq::tq3::log::tracing_subscriber::init();

    {
        info!("warm up ...");
        let mut hist = Histogram::new();
        run_hub_1_to_n(10000, &mut hist).await;
        info!("warm up done");
    }

    let num = 100_000;
    measure_hub_1_to_n(num).await;
}
