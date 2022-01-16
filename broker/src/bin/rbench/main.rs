use clap::Parser;
use rust_threeq::tq3;
use rust_threeq::tq3::app;
use std::fmt::Debug;

mod common;
mod kafka;
mod mqtt;
mod pulsar;

// refer https://github.com/clap-rs/clap/tree/master/clap_derive/examples
#[derive(Parser, Debug)]
#[clap(name = "rthreeq bench", author, about, version=app::version_long())]
struct CmdArgs {
    //#[clap(arg_enum, short = 't', long = "type", long_about = "bench type", default_value = "mqtt")]
    #[clap(subcommand)]
    cmd: SubCmd,
}

// #[derive(ArgEnum, Debug, PartialEq)]
#[derive(Parser, Debug, PartialEq)]
enum SubCmd {
    Mqtt(mqtt::Args),
    Pulsar(pulsar::Args),
    Kafka(kafka::Args),
}

#[tokio::main]
async fn main() {
    tq3::log::tracing_subscriber::init();

    // helloworld_service().await;
    // tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    // helloworld_client().await;

    let args = CmdArgs::parse();

    match args.cmd {
        SubCmd::Mqtt(opt) => mqtt::run(&opt).await,
        SubCmd::Pulsar(opt) => pulsar::run(&opt).await,
        SubCmd::Kafka(opt) => kafka::run(&opt).await,
    }
}

// use tonic::{transport::Server, Request, Response, Status};

// use hello_world::greeter_server::{Greeter, GreeterServer};
// use hello_world::{HelloReply, HelloRequest};

// pub mod hello_world {
//     tonic::include_proto!("helloworld"); // The string specified here must match the proto package name
// }
// use hello_world::greeter_client::GreeterClient;

// #[derive(Debug, Default)]
// pub struct MyGreeter {}

// #[tonic::async_trait]
// impl Greeter for MyGreeter {
//     async fn say_hello(
//         &self,
//         request: Request<HelloRequest>, // Accept request of type HelloRequest
//     ) -> Result<Response<HelloReply>, Status> { // Return an instance of type HelloReply
//         println!("Got a request: {:?}", request);

//         let reply = hello_world::HelloReply {
//             message: format!("Hello {}!", request.into_inner().name).into(), // We must use .into_inner() as the fields of gRPC requests and responses are private
//         };
//         tokio::time::sleep(std::time::Duration::from_secs(2)).await;
//         Ok(Response::new(reply)) // Send back our formatted greeting
//     }
// }

// async fn helloworld_service(){
//     let f = async move {
//         let addr = "127.0.0.1:50051".parse().unwrap();
//         let greeter = MyGreeter::default();
//         let r = Server::builder()
//         .add_service(GreeterServer::new(greeter))
//         .serve(addr)
//         .await;
//         match r {
//             Ok(_r) => {},
//             Err(e) => {
//                 tracing::error!("{}", e);
//             },
//         }
//     };

//     tokio::spawn(f);
// }

// async fn helloworld_client() {
//     let fut = async move {
//         let mut client = GreeterClient::connect("http://127.0.0.1:50051").await?;

//         let request = tonic::Request::new(HelloRequest {
//             name: "Tonic".into(),
//             data: [].to_vec(),
//         });

//         let response = client.say_hello(request).await?;

//         println!("RESPONSE={:?}", response);
//         Ok::<(), Box<dyn std::error::Error>>(())
//     };

//     let r = fut.await;
//     match r {
//         Ok(_r) => {},
//         Err(e) => {
//             tracing::error!("{}", e);
//         },
//     }
//     std::process::exit(0);
// }
