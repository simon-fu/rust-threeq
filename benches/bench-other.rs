// cargo bench --bench bench-other -- bench-snowflake

use criterion::*;

fn bench_snowflake_id(group: &mut BenchmarkGroup<'_, measurement::WallTime>) {
    use rust_threeq::tq3::SnowflakeId;
    let num = 1000_000;
    
    group.throughput(Throughput::Elements(num));
    group.bench_function(
        format!("snowflake_next-{}", num),
        |b| {
            b.iter(|| {
                let mut gen = SnowflakeId::new(1);
                for _ in 0..num {
                    let _= gen.next();
                }
            })
        },
    );

    group.throughput(Throughput::Elements(num));
    group.bench_function(
        format!("snowflake_next_or_borrow-{}", num),
        |b| {
            b.iter(|| {
                let mut gen = SnowflakeId::new(1);
                for _ in 0..num {
                    gen.next_or_borrow();
                }
            })
        },
    );

    group.throughput(Throughput::Elements(num));
    group.bench_function(
        format!("snowflake_next_or_wait-{}", num),
        |b| {
            b.iter(|| {
                let mut gen = SnowflakeId::new(1);
                for _ in 0..num {
                    gen.next_or_wait();
                }
            })
        },
    );
}

fn criterion_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("bench-snowflake");

    bench_snowflake_id(&mut &mut group);

    group.finish();
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
