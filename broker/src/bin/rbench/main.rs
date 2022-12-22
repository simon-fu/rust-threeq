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

    // test_tonic::test_hello2().await.unwrap();
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

// mod test_tonic {
//     use std::ops::Range;

//     use anyhow::Result;
//     use tokio::sync::oneshot;
//     use tokio::task::JoinHandle;
//     // use futures_util::FutureExt;
//     use futures::FutureExt;
//     use tonic::transport::{Endpoint, Channel};
//     use tonic::{transport::Server, Request, Response, Status};
    
//     use hello_world::greeter_server::{Greeter, GreeterServer};
//     use hello_world::{HelloReply, HelloRequest};
    
//     pub mod hello_world {
//         tonic::include_proto!("helloworld"); // The string specified here must match the proto package name
//     }
//     use hello_world::greeter_client::GreeterClient;
    
//     #[derive(Debug, Default)]
//     pub struct MyGreeter {}
    
//     #[tonic::async_trait]
//     impl Greeter for MyGreeter {
//         async fn say_hello(
//             &self,
//             request: Request<HelloRequest>, // Accept request of type HelloRequest
//         ) -> Result<Response<HelloReply>, Status> { // Return an instance of type HelloReply
//             // println!("Got a request: {:?}", request.get_ref());
    
//             let reply = hello_world::HelloReply {
//                 message: format!("Hello {}!", request.into_inner().name).into(), // We must use .into_inner() as the fields of gRPC requests and responses are private
//             };
//             // tokio::time::sleep(std::time::Duration::from_secs(2)).await;
//             Ok(Response::new(reply)) // Send back our formatted greeting
//         }
//     }
    
//     pub async fn test_hello() -> Result<()> { 
//         let server1 = RpcServer::try_new("127.0.0.1:50051")?;
//         println!("server1 started");
//         tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    
//         let mut client = GreeterClient::connect("http://127.0.0.1:50051").await?;
//         println!("client connected");

//         let response = client.say_hello(tonic::Request::new(HelloRequest {
//             name: "m1".into(),
//         })).await?;
    
//         println!("RESPONSE1={:?}", response.get_ref());

//         server1.shutdown().await?;
//         println!("server1 shutdown");
//         tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        

//         let server2 = RpcServer::try_new("127.0.0.1:50051")?;
    
//         let response = client.say_hello(tonic::Request::new(HelloRequest {
//             name: "m2".into(),
//         })).await?;
    
//         println!("RESPONSE2={:?}", response.get_ref());

//         server2.shutdown().await?;

//         std::process::exit(0);
    
//     }

//     pub async fn test_hello2() -> Result<()> { 
//         let listen_addr = "127.0.0.1:60000";
//         let target_addr = "http://127.0.0.1:50000";
//         let channel = Endpoint::from_static(target_addr)
//         .connect_lazy()?;
//         let client = GreeterClient::new(channel);

//         // let mut client = GreeterClient::connect("http://127.0.0.1:50051").await?;
//         // println!("client connected");
//         // tokio::time::sleep(std::time::Duration::from_secs(5)).await;

//         let server1 = RpcServer::try_new(listen_addr)?;
//         println!("server1 started at [{}]", listen_addr);
//         tokio::time::sleep(std::time::Duration::from_secs(1)).await;

//         let num = 3;
//         let mut tasks = Vec::with_capacity(num);
//         for i in 0..num {
//             let mut client = client.clone();
//             let h = tokio::spawn(async move {
//                 say_hello_loop(&i.to_string(), &mut client, 0..10).await
//             });
//             tasks.push(h);
//         }

//         while let Some(h) = tasks.pop() {
//             h.await??;
//         }

//         // {
//         //     let d = std::time::Duration::from_secs(999);
//         //     println!("wait for [{:?}]", d);
//         //     tokio::time::sleep(d).await;
//         // }

//         server1.shutdown().await?;
//         println!("server1 shutdown");

//         std::process::exit(0);
    
//     }

//     async fn say_hello_loop(id: &str, client: &mut GreeterClient<Channel>,  range: Range<usize>) -> Result<()> {
//         for i in range {
            
//             let r = client.say_hello(tonic::Request::new(HelloRequest {
//                 name: format!("m{}", i)
//             })).await;
//             println!("Task {}: No.{} response [{:?}]\n", id, i, r);
//             tokio::time::sleep(std::time::Duration::from_secs(1)).await;
//         }
//         Ok(())
//     }
    
//     struct RpcServer{
//         task: JoinHandle<Result<()>>,
//         tx: oneshot::Sender<()>,
//     }

//     impl RpcServer {
//         pub fn try_new(addr : &str) -> Result<Self> { 
//             let addr = addr.parse()?;
//             let (tx, rx) = oneshot::channel::<()>();
//             let task = tokio::spawn(async move {
//                 let greeter = MyGreeter::default();
//                 let _r = Server::builder()
//                 .add_service(GreeterServer::new(greeter))
//                 .serve_with_shutdown(addr, rx.map(drop))
//                 .await?;
//                 Ok(())
//             });
            
//             Ok(RpcServer {
//                 task,
//                 tx,
//             })
//         }

//         pub async fn shutdown(self) -> Result<()> {
//             let _r = self.tx.send(());
//             self.task.await?
//         }

//     }
    
// }

