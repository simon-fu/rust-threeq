/*
TODO:
- 优化 NodeIdPool 
- support QoS1, Qos2, QoS match
- support retain, and publish empty msg to clean retain
- support cluster
- support in-flight window
- support publish topic alias
- support redis
- support kafka
- support websocket over mqtt and pure websocket
- support local disk storage
*/


use clap::Parser;
use rust_threeq::tq3;
use tracing::{error, info};
use actix_web::{get, HttpResponse, Responder};
use prometheus::{Encoder, TextEncoder};
use anyhow::Result;
use crate::args::Config;

pub mod falsework;
mod hub;
mod registry;
mod broker;
mod args;





// Register & measure some metrics.
lazy_static::lazy_static! {
    static ref SESSION_GAUGE: prometheus::IntGauge =
        prometheus::register_int_gauge!("sessions", "Number of sessions").unwrap();
}

#[get("/metrics")]
async fn metrics() -> impl Responder {
    SESSION_GAUGE.set(hub::get().num_sessions() as i64);
    let metric_families = prometheus::gather();

    let mut buffer = Vec::new();
    let encoder = TextEncoder::new();
    encoder.encode(&metric_families, &mut buffer).unwrap();
    let output = String::from_utf8(buffer).unwrap();
    // debug!("{}", output);
    HttpResponse::Ok().body(output)
}

async fn async_main() -> Result<()> {
    // tq3::log::tracing_subscriber::init();
    // use tracing_subscriber::fmt::format::FmtSpan;
    // tq3::log::tracing_subscriber::init_with_span_events(FmtSpan::ACTIVE);

    let cfg = Config::parse();
    info!("cfg={:#?}", cfg);

    if cfg.enable_gc {
        let r = falsework::discovery::spawn_service("127.0.0.1:50051", None, 1).await;
        if let Err(e) = r {
            error!("{:?}", e);
            std::process::exit(0);
        }
        
        let r = falsework::kafka::run().await;
        if let Err(e) = r {
            error!("{:?}", e);
            std::process::exit(0);
        }

        let r = falsework::cluster::run().await;
        if let Err(e) = r {
            error!("{:?}", e);
        }
        std::process::exit(0);
    }

    broker::run_server(&cfg).await

    // let tokio_h = tokio::spawn(async move {
    //     match broker::run_server(&cfg).await {
    //         Ok(_) => {}
    //         Err(e) => {
    //             error!("{}", e);
    //         }
    //     }
    // });
    // let _r = tokio_h.await;

    // // let actix_h = actix_web::HttpServer::new(|| actix_web::App::new().service(metrics))
    // //     .workers(1)
    // //     .bind("127.0.0.1:8080")
    // //     .expect("Couldn't bind to 127.0.0.1:8080")
    // //     .run();

    // // match futures::future::select(tokio_h, actix_h).await {
    // //     futures::future::Either::Left(_r) => {}
    // //     futures::future::Either::Right(_r) => {}
    // // }

    // Ok(())
}

// #[tokio::main]
// #[actix_web::main]
fn main() -> Result<()> {
    tq3::log::init_with_filters("debug,h2=warn,hyper=warn,tower=warn")?;
    // tq3::log::init()?;

    actix_web::rt::System::with_tokio_rt(|| {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            // .worker_threads(8)
            .thread_name("main-tokio")
            .build()
            .unwrap()
    })
    .block_on(async_main())?;

    Ok(())
}
