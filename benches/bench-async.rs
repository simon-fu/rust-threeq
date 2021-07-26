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

#[derive(Clap, Debug, Clone)]
struct WakeupArgs {
    #[clap(long, long_about = "num of subscribers", default_value = "100000")]
    subs: u64,

    #[clap(long, long_about = "num of senders", default_value = "1")]
    senders: u64,

    #[clap(long, long_about = "num of messages", default_value = "1")]
    messages: u64,

    #[clap(long, long_about = "size of queue", default_value = "0")]
    qsize: usize,

    #[clap(long)]
    bench: bool, // placeholder for cargo bench --bench

    #[clap(long)]
    disable_hub: bool,

    #[clap(long)]
    disable_watch: bool,

    #[clap(long)]
    disable_broadcast: bool,
}

impl WakeupArgs {
    fn comp(&mut self) {
        if self.qsize == 0 {
            self.qsize = self.messages as usize;
        }
    }
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

fn broadcast_new<T: Send + Clone>(
    _v: T,
    args: &Arc<WakeupArgs>,
) -> (BroadcastSender<T>, BroadcastRecver<T>) {
    let (tx, rx) = broadcast::channel(args.qsize);
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

fn watch_new<T: Send + Clone>(v: T, _args: &Arc<WakeupArgs>) -> (WatchSender<T>, WatchRecver<T>) {
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
            Ok(v) => Ok(v.0),
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

fn wakehub_new<T: Send + Sync + Clone>(
    _v: T,
    args: &Arc<WakeupArgs>,
) -> (HubSender<T>, HubRecver<T>) {
    // static UID: AtomicU64 = AtomicU64::new(0);

    let rx = Arc::new(WakeSubscriptions::new(args.qsize));
    let hub = WakeHub::new();
    hub.add(wakehub_next_key(), rx.clone());
    (HubSender { hub }, HubRecver { rx })
}

fn wakehub_clone_rx<T: Send + Sync + Clone>(
    tx: &mut HubSender<T>,
    rx0: &mut HubRecver<T>,
) -> HubRecver<T> {
    let rx = Arc::new(WakeSubscriptions::new(rx0.rx.max_qsize()));
    tx.hub.add(wakehub_next_key(), rx.clone());
    HubRecver { rx }
}

async fn run_wakeup<SendT, RecvT, NewF, CloneRxF>(
    name: &str,
    mut new_fn: NewF,
    mut clone_rx_fn: CloneRxF,
    args: Arc<WakeupArgs>,
) where
    SendT: SyncSender<Arc<Message>>,
    RecvT: SyncRecver<Arc<Message>> + Send + 'static,
    NewF: FnMut(Arc<Message>, &Arc<WakeupArgs>) -> (SendT, RecvT),
    CloneRxF: FnMut(&mut SendT, &mut RecvT) -> RecvT,
{
    info!(
        "{}: sender {}, subs {}/sender",
        name, args.senders, args.subs
    );

    let mut hist = Histogram::new();
    let (tx, mut rx) = mpsc::unbounded_channel();

    let mut senders: Vec<SendT> = Vec::new();

    let kick_time = Instant::now();
    for _ in 0..args.senders {
        let (mut sender, mut recver) = new_fn(Arc::new(Instant::now()), &args);
        for _ in 0..args.subs {
            let tx0 = tx.clone();
            let mut rx0 = clone_rx_fn(&mut sender, &mut recver);
            let args0 = args.clone();
            let _h1 = tokio::spawn(async move {
                for _ in 0..args0.messages {
                    let r = rx0.recv().await.unwrap();
                    let _r = tx0.send(Instant::now() - *r);
                }
            });
        }

        senders.push(sender);
    }
    drop(tx);
    debug!(
        "{}: spawn done, senders {}, subs {}, elapsed {:?}",
        name,
        args.senders,
        args.subs,
        Instant::now() - kick_time
    );

    let msg = Arc::new(Instant::now());
    for hub in &mut senders {
        for _ in 0..args.messages {
            let _r = hub.send(msg.clone()).await;
        }
    }
    debug!(
        "{}: send done, senders {}, subs {}, elapsed {:?}",
        name,
        args.senders,
        args.subs,
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
    // {
    //     let mut args0 = args.clone();
    //     args0.senders = 1;
    //     args0.subs = 10_000;
    //     args0.messages = 1;
    //     info!("-");
    //     info!("warm up ...");
    //     run_wakeup("Hub", wakehub_new, wakehub_clone_rx, Arc::new(args0)).await;
    //     info!("warm up done");
    //     info!("-");
    // }

    let args = Arc::new(args);

    if !args.disable_broadcast {
        info!("-");
        run_wakeup("Broadcast", broadcast_new, broadcast_clone_rx, args.clone()).await;
    }

    if !args.disable_watch && args.messages == 1 {
        info!("-");
        run_wakeup("Watch", watch_new, watch_clone_rx, args.clone()).await;
    }

    if !args.disable_hub {
        info!("-");
        run_wakeup("Hub", wakehub_new, wakehub_clone_rx, args.clone()).await;
    }
}

#[tokio::main]
async fn main() {
    rust_threeq::tq3::log::tracing_subscriber::init();

    info!("-");

    // let args: Vec<String> = std::env::args().collect();
    // info!("{:?}", args);

    let args = CmdArgs::parse();
    info!("args={:?}", args);

    match args.subcmd {
        SubCommand::Wakeup(mut sub_args) => {
            sub_args.comp();
            bench_wakeup(sub_args).await;
        }
    }
}
