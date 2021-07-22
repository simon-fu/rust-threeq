// cargo bench --bench bench-tokio -- mpsc-unbounded

use std::vec;

use criterion::*;

async fn run_mpsc_unbounded_channel(nsends: u64, nmsgs: u64, ngroups: u64) {
    let (tx_final, mut rx_final) = tokio::sync::mpsc::unbounded_channel();
    for _ in 0..ngroups {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();

        for _ in 0..nsends {
            let tx0 = tx.clone();
            let _h1 = tokio::spawn(async move {
                for i in 0..nmsgs {
                    let r = tx0.send(i);
                    if let Err(e) = r {
                        println!("send error {:?}", e);
                    }
                }
            });
        }
        drop(tx);

        {
            let tx0 = tx_final.clone();
            let _h2 = tokio::spawn(async move {
                loop {
                    if let None = rx.recv().await {
                        break;
                    }
                }
                let _ = tx0.send(());
            });
        }
    }
    drop(tx_final);

    loop {
        if let None = rx_final.recv().await {
            break;
        }
    }
}

fn bench_mpsc_unbounded_channel(
    group: &mut BenchmarkGroup<'_, measurement::WallTime>,
    nsends: u64,
    nmsgs: u64,
    ngroups: u64,
) {
    group.throughput(Throughput::Elements(nsends * nmsgs * ngroups));
    group.bench_function(
        format!("mpsc-unbounded-{}x{}x{}", nsends, nmsgs, ngroups),
        |b| {
            b.to_async(tokio::runtime::Runtime::new().unwrap())
                .iter(|| async {
                    run_mpsc_unbounded_channel(nsends, nmsgs, ngroups).await;
                    // rust_threeq::tq3::measure_async("mpsc_unbounded_aaa", nsends*nmsgs*ngroups, run_mpsc_unbounded_channel(nsends, nmsgs, ngroups)).await;
                })
        },
    );
}

async fn run_mpsc_bounded_channel(nsends: u64, nmsgs: u64, ngroups: u64, qsize: usize) {
    let (tx_final, mut rx_final) = tokio::sync::mpsc::channel(ngroups as usize);
    for _ in 0..ngroups {
        let (tx, mut rx) = tokio::sync::mpsc::channel(qsize);

        for _ in 0..nsends {
            let tx0 = tx.clone();
            let _h1 = tokio::spawn(async move {
                for i in 0..nmsgs {
                    let r = tx0.send(i).await;
                    if let Err(e) = r {
                        println!("send error {:?}", e);
                    }
                }
            });
        }
        drop(tx);

        {
            let tx0 = tx_final.clone();
            let _h2 = tokio::spawn(async move {
                loop {
                    if let None = rx.recv().await {
                        break;
                    }
                }
                let _ = tx0.send(());
            });
        }
    }
    drop(tx_final);

    loop {
        if let None = rx_final.recv().await {
            break;
        }
    }
}

fn bench_mpsc_bounded_channel(
    group: &mut BenchmarkGroup<'_, measurement::WallTime>,
    nsends: u64,
    nmsgs: u64,
    ngroups: u64,
    qsize: usize,
) {
    group.throughput(Throughput::Elements(nsends * nmsgs * ngroups));
    group.bench_function(
        format!("mpsc-bounded-{}x{}x{}x{}", nsends, nmsgs, ngroups, qsize),
        |b| {
            b.to_async(tokio::runtime::Runtime::new().unwrap())
                .iter(|| async {
                    run_mpsc_bounded_channel(nsends, nmsgs, ngroups, qsize).await;
                    // rust_threeq::tq3::measure_async("mpsc_bounded_aaa", nsends*nmsgs*ngroups, run_mpsc_bounded_channel(nsends, nmsgs, ngroups, qsize)).await;
                })
        },
    );
}

fn bench_mpsc(group: &mut BenchmarkGroup<'_, measurement::WallTime>) {
    let qsize = 2048usize;
    let args = vec![
        (1, 1_000_000, 1, qsize),
        (1, 100_000, 10, qsize),
        (1, 10_000, 100, qsize),
        (1, 1_000, 1000, qsize),
        (100, 10_000, 1, qsize),
        (100, 10_000, 10, qsize),
    ];

    for v in &args {
        bench_mpsc_unbounded_channel(group, v.0, v.1, v.2);
    }

    for v in &args {
        bench_mpsc_bounded_channel(group, v.0, v.1, v.2, v.3);
    }
}

fn criterion_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("bench-tokio");

    bench_mpsc(&mut group);

    group.finish();
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);