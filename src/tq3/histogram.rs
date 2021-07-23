use histogram::Histogram;
use std::time::Duration;
use tracing::info;

pub fn print_summary(name: &str, unit: &str, h: &Histogram) {
    if h.entries() == 0 {
        info!("{} Summary: (empty)", name);
        return;
    }

    info!("{} Summary: num {}", name, h.entries());
    info!("     Min: {} {}", h.minimum().unwrap(), unit);
    info!("     Avg: {} {}", h.mean().unwrap(), unit);
    info!("     Max: {} {}", h.maximum().unwrap(), unit);
    info!("  StdDev: {} {}", h.stddev().unwrap(), unit);
}

pub fn print_percent(name: &str, unit: &str, h: &Histogram) {
    if h.entries() == 0 {
        info!("{} Percentiles: (empty)", name);
        return;
    }

    info!("{} Percentiles: num {}", name, h.entries());
    info!("   P50: {} {}", h.percentile(50.0).unwrap(), unit,);
    info!("   P90: {} {}", h.percentile(90.0).unwrap(), unit,);
    info!("   P99: {} {}", h.percentile(99.0).unwrap(), unit,);
    info!("  P999: {} {}", h.percentile(99.9).unwrap(), unit,);
}

pub fn print(name: &str, unit: &str, h: &Histogram) {
    print_summary(name, unit, h);
    print_percent(name, unit, h);
}

pub fn print_duration_summary(name: &str, h: &Histogram) {
    if h.entries() == 0 {
        info!("{} Summary: (empty)", name);
        return;
    }

    info!("{} Summary: num {}", name, h.entries());
    info!("     Min: {:?}", Duration::from_nanos(h.minimum().unwrap()));
    info!("     Avg: {:?}", Duration::from_nanos(h.mean().unwrap()));
    info!("     Max: {:?}", Duration::from_nanos(h.maximum().unwrap()));
    info!(
        "     StdDev: {:?}",
        Duration::from_nanos(h.stddev().unwrap())
    );
}

pub fn print_duration_percent(name: &str, h: &Histogram) {
    if h.entries() == 0 {
        info!("{} Percentiles: (empty)", name);
        return;
    }

    info!("{} Percentiles: num {}", name, h.entries());
    info!(
        "   P50: {:?}",
        Duration::from_nanos(h.percentile(50.0).unwrap()),
    );
    info!(
        "   P90: {:?}",
        Duration::from_nanos(h.percentile(90.0).unwrap()),
    );
    info!(
        "   P99: {:?}",
        Duration::from_nanos(h.percentile(99.0).unwrap()),
    );
    info!(
        "  P999: {:?}",
        Duration::from_nanos(h.percentile(99.9).unwrap()),
    );
}

pub fn print_duration(name: &str, h: &Histogram) {
    print_duration_summary(name, h);
    print_duration_percent(name, h);
}
