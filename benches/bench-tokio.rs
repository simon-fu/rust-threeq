// cargo bench --bench bench-tokio

// use std::time::Instant;

use criterion::*;

async fn bench_mpsc_unbounded_channel(nsends: u64, nmsgs: u64, ngroups: u64) {
    // let now = Instant::now();

    let (tx_final, mut rx_final) = tokio::sync::mpsc::unbounded_channel();
    for _ in 0..ngroups {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();

        for _ in 0..nsends {
            let tx0 = tx.clone();
            let _h1 = tokio::spawn(async move {
                for i in 0..nmsgs {
                    let _ = tx0.send(i);
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

    // let _ = tokio::join!(h2);

    // let elapsed = now.elapsed();
    // drop(now);
    // let num = (nsends * ngroups * nmsgs) as u128;
    // let each = elapsed.as_nanos() / num;
    // let rate = num * 1_000_000_000 / elapsed.as_nanos();
    // println!(
    //     "{}: num {}, elapsed {:?}, each {} ns, estimate {}/sec",
    //     "mpsc unbound", num, elapsed, each, rate
    // );
}

fn criterion_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("bench-tokio");

    let nsends = 1u64;
    let nmsg = 1_000_000u64;
    let ngroups = 1u64;
    group.throughput(Throughput::Elements(nsends * nmsg * ngroups));
    group.bench_function(
        format!("mpsc-unbounded-{}x{}x{}", nsends, nmsg, ngroups),
        |b| {
            b.to_async(tokio::runtime::Runtime::new().unwrap())
                .iter(|| async { bench_mpsc_unbounded_channel(nsends, nmsg, ngroups).await })
        },
    );

    let nsends = 1u64;
    let nmsg = 100_000u64;
    let ngroups = 10u64;
    group.throughput(Throughput::Elements(nsends * nmsg * ngroups));
    group.bench_function(
        format!("mpsc-unbounded-{}x{}x{}", nsends, nmsg, ngroups),
        |b| {
            b.to_async(tokio::runtime::Runtime::new().unwrap())
                .iter(|| async { bench_mpsc_unbounded_channel(nsends, nmsg, ngroups).await })
        },
    );

    let nsends = 1u64;
    let nmsg = 10_000u64;
    let ngroups = 100u64;
    group.throughput(Throughput::Elements(nsends * nmsg * ngroups));
    group.bench_function(
        format!("mpsc-unbounded-{}x{}x{}", nsends, nmsg, ngroups),
        |b| {
            b.to_async(tokio::runtime::Runtime::new().unwrap())
                .iter(|| async { bench_mpsc_unbounded_channel(nsends, nmsg, ngroups).await })
        },
    );

    let nsends = 1u64;
    let nmsg = 1_000u64;
    let ngroups = 1000u64;
    group.throughput(Throughput::Elements(nsends * nmsg * ngroups));
    group.bench_function(
        format!("mpsc-unbounded-{}x{}x{}", nsends, nmsg, ngroups),
        |b| {
            b.to_async(tokio::runtime::Runtime::new().unwrap())
                .iter(|| async { bench_mpsc_unbounded_channel(nsends, nmsg, ngroups).await })
        },
    );

    let nsends = 100u64;
    let nmsg = 10_000u64;
    let ngroups = 1u64;
    group.throughput(Throughput::Elements(nsends * nmsg * ngroups));
    group.bench_function(
        format!("mpsc-unbounded-{}x{}x{}", nsends, nmsg, ngroups),
        |b| {
            b.to_async(tokio::runtime::Runtime::new().unwrap())
                .iter(|| async { bench_mpsc_unbounded_channel(nsends, nmsg, ngroups).await })
        },
    );

    let nsends = 100u64;
    let nmsg = 1000u64;
    let ngroups = 10u64;
    group.throughput(Throughput::Elements(nsends * nmsg * ngroups));
    group.bench_function(
        format!("mpsc-unbounded-{}x{}x{}", nsends, nmsg, ngroups),
        |b| {
            b.to_async(tokio::runtime::Runtime::new().unwrap())
                .iter(|| async { bench_mpsc_unbounded_channel(nsends, nmsg, ngroups).await })
        },
    );

    group.finish();
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
