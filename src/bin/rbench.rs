use tracing::info;


#[tokio::main]
async fn main() {

    use tracing_subscriber::EnvFilter;
    
    let env_filter = if std::env::var(EnvFilter::DEFAULT_ENV).is_ok() {
        EnvFilter::from_default_env()
    } else {
        EnvFilter::new("info")
    };

    tracing_subscriber::fmt()
    .with_target(false)
    .with_env_filter(env_filter)
    .init();

    use rust_threeq::tq3;
    let d = tq3::codec::Decoder::new();
    info!("d = {:?}", d);
}
