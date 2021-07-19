// cargo bench --bench bench-tokio

use std::time::Instant;

use criterion::*;

async fn bench_mpsc_unbounded_channel(num: u64) {
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();

    let now = Instant::now();

    let h1 = tokio::spawn(async move {
        for i in 0..num {
            let _ = tx.send(i);
        }
    });

    let h2 = tokio::spawn(async move {
        loop {
            if let None = rx.recv().await {
                break;
            }
        }
    });
    let _ = tokio::join!(h1, h2);

    let elapsed = now.elapsed();
    drop(now);
    let num = num as u128;
    let each = elapsed.as_nanos() / num;
    let rate = num * 1_000_000_000 / elapsed.as_nanos();

    println!(
        "{}: num {}, elapsed {:?}, each {} ns, estimate {}/sec",
        "mpsc unbound", num, elapsed, each, rate
    );
}

fn criterion_benchmark(c: &mut Criterion) {
    let num = 1_000_000u64;
    let mut group = c.benchmark_group("bench-tokio");
    group.throughput(Throughput::Elements(num));
    group.bench_function("mpsc-unbounded", |b| {
        b.to_async(tokio::runtime::Runtime::new().unwrap())
            .iter(|| async { bench_mpsc_unbounded_channel(num).await })
    });
    group.finish();
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
