// cargo bench --bench bench-async

use async_trait::async_trait;
use clap::Clap;
use histogram::Histogram;
use rust_threeq::tq3;
use rust_threeq::tq3::hub;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::{broadcast, mpsc, watch};
use tracing::{debug, info};

#[derive(Clap, Debug)]
#[clap(name = "bench-async", author, about, version)]
struct CmdArgs {
    #[clap(subcommand)]
    subcmd: SubCommand,
}

#[derive(Clap, Debug)]
enum SubCommand {
    Wakeup(WakeupArgs),
}

#[derive(Clap, Debug)]
struct WakeupArgs {
    #[clap(long, long_about = "num of subscribers", default_value = "100000")]
    subs: u64,

    #[clap(long, long_about = "num of senders", default_value = "0")]
    senders: u64,

    #[clap(long)]
    bench: bool,
}

type Message = Instant;


#[async_trait]
trait SyncSender<T> {
    async fn send(&mut self, v: T);
}

#[async_trait]
trait SyncRecver<T> {
    async fn recv(&mut self) -> Result<T, String>;
}

#[derive(Debug)]
struct BroadcastSender<T> {
    tx: broadcast::Sender<T>,
}

#[async_trait]
impl<T: Send> SyncSender<T> for BroadcastSender<T> {
    async fn send(&mut self, v: T) {
        let _r = self.tx.send(v);
    }
}

#[derive(Debug)]
struct BroadcastRecver<T> {
    rx: broadcast::Receiver<T>,
}

#[async_trait]
impl<T: Send + Clone> SyncRecver<T> for BroadcastRecver<T> {
    async fn recv(&mut self) -> Result<T, String> {
        let r = self.rx.recv().await;
        match r {
            Ok(t) => Ok(t),
            Err(e) => Err(e.to_string()),
        }
    }
}

fn broadcast_new<T: Send + Clone>(_v: T) -> (BroadcastSender<T>, BroadcastRecver<T>) {
    let (tx, rx) = broadcast::channel(1);
    (BroadcastSender { tx }, BroadcastRecver { rx })
}

fn broadcast_clone_rx<T: Send + Clone>(
    tx: &mut BroadcastSender<T>,
    _rx: &mut BroadcastRecver<T>,
) -> BroadcastRecver<T> {
    let rx = tx.tx.subscribe();
    BroadcastRecver { rx }
}

#[derive(Debug)]
struct WatchSender<T> {
    tx: watch::Sender<T>,
}

#[async_trait]
impl<T: Send + Sync + Clone> SyncSender<T> for WatchSender<T> {
    async fn send(&mut self, v: T) {
        let _r = self.tx.send(v);
    }
}

#[derive(Debug)]
struct WatchRecver<T> {
    rx: watch::Receiver<T>,
}

#[async_trait]
impl<T: Send + Clone + Sync> SyncRecver<T> for WatchRecver<T> {
    async fn recv(&mut self) -> Result<T, String> {
        let r = self.rx.changed().await;
        match r {
            Ok(_n) => Ok(self.rx.borrow().clone()),
            Err(e) => Err(e.to_string()),
        }
    }
}

fn watch_new<T: Send + Clone>(v: T) -> (WatchSender<T>, WatchRecver<T>) {
    let (tx, rx) = watch::channel(v);
    (WatchSender { tx }, WatchRecver { rx })
}

fn watch_clone_rx<T: Send + Clone>(
    _tx: &mut WatchSender<T>,
    rx: &mut WatchRecver<T>,
) -> WatchRecver<T> {
    WatchRecver { rx: rx.rx.clone() }
}

pub type WakeSubscriptions<T> = hub::LatestFirstQue<T>;
pub type WakeHub<T> = hub::Hub<T, u64, WakeSubscriptions<T>>;

#[derive(Debug)]
struct HubSender<T: Send + Sync + Clone> {
    hub: WakeHub<T>,
}

#[async_trait]
impl<T: Send + Sync + Clone> SyncSender<T> for HubSender<T> {
    async fn send(&mut self, v: T) {
        let _r = self.hub.push(&v).await;
    }
}

#[derive(Debug)]
struct HubRecver<T> {
    rx: Arc<WakeSubscriptions<T>>,
}

#[async_trait]
impl<T: Send + Clone + Sync> SyncRecver<T> for HubRecver<T> {
    async fn recv(&mut self) -> Result<T, String> {
        let r = self.rx.recv().await;
        match r {
            Ok((v, _sz)) => Ok(v),
            Err(e) => Err(format!("{:?}", e)),
        }
    }
}

fn wakehub_next_key() -> u64 {
    lazy_static::lazy_static!(
        static ref UID: AtomicU64 = AtomicU64::new(0);
    );
    UID.fetch_add(1, Ordering::Relaxed)
}

fn wakehub_new<T: Send + Sync + Clone>(_v: T) -> (HubSender<T>, HubRecver<T>) {
    // static UID: AtomicU64 = AtomicU64::new(0);

    let rx = Arc::new(WakeSubscriptions::new(1));
    let hub = WakeHub::new();
    hub.add(wakehub_next_key(), rx.clone());
    (HubSender { hub }, HubRecver { rx })
}

fn wakehub_clone_rx<T: Send + Sync + Clone>(
    tx: &mut HubSender<T>,
    _rx: &mut HubRecver<T>,
) -> HubRecver<T> {
    let rx = Arc::new(WakeSubscriptions::new(1));
    tx.hub.add(wakehub_next_key(), rx.clone());
    HubRecver { rx }
}

async fn run_wakeup<SendT, RecvT, NewF, CloneRxF>(
    name: &str,
    mut new_fn: NewF,
    mut clone_rx_fn: CloneRxF,
    num_senders: u64,
    num_subs: u64,
) where
    SendT: SyncSender<Arc<Message>>,
    RecvT: SyncRecver<Arc<Message>> + Send + 'static,
    NewF: FnMut(Arc<Message>) -> (SendT, RecvT),
    CloneRxF: FnMut(&mut SendT, &mut RecvT) -> RecvT,
{
    info!("{}: sender {}, subs {}/sender", name, num_senders, num_subs);

    let mut hist = Histogram::new();
    let (tx, mut rx) = mpsc::unbounded_channel();

    let mut senders: Vec<SendT> = Vec::new();

    let kick_time = Instant::now();
    for _ in 0..num_senders {
        let (mut sender, mut recver) = new_fn(Arc::new(Instant::now()));
        for _ in 0..num_subs {
            let tx0 = tx.clone();
            let mut rx0 = clone_rx_fn(&mut sender, &mut recver);
            let _h1 = tokio::spawn(async move {
                let r = rx0.recv().await.unwrap();
                let _r = tx0.send(Instant::now() - *r);
            });
        }

        senders.push(sender);
    }
    drop(tx);
    debug!(
        "{}: spawn done, senders {}, subs {}, elapsed {:?}",
        name,
        num_senders,
        num_subs,
        Instant::now() - kick_time
    );

    let msg = Arc::new(Instant::now());
    for hub in &mut senders {
        let _r = hub.send(msg.clone()).await;
    }
    debug!(
        "{}: send done, senders {}, subs {}, elapsed {:?}",
        name,
        num_senders,
        num_subs,
        Instant::now() - kick_time
    );

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

    tq3::histogram::print_duration(&name, &hist);
}

async fn bench_wakeup(args: WakeupArgs) {
    {
        info!("-");
        info!("warm up ...");
        run_wakeup("Hub", wakehub_new, wakehub_clone_rx, 1, 10_000).await;
        info!("warm up done");
    }

    if args.senders == 0 {
        info!("-");
        run_wakeup("Broadcast", broadcast_new, broadcast_clone_rx, 1, 100_000).await;

        info!("-");
        run_wakeup("Watch", watch_new, watch_clone_rx, 1, 100_000).await;

        info!("-");
        run_wakeup("Hub", wakehub_new, wakehub_clone_rx, 1, 100_000).await;
    } else {
        info!("-");
        run_wakeup(
            "Hub",
            wakehub_new,
            wakehub_clone_rx,
            args.senders,
            args.subs,
        )
        .await;
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
        SubCommand::Wakeup(sub_args) => {
            bench_wakeup(sub_args).await;
        }
    }
}
