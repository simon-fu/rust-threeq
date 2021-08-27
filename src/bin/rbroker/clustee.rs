use super::znodes;
use super::zrpc;
use tracing::{debug, error, info};

// macro_rules! define_var {
//     ($val:expr,) => {

//     };
//     ($val:expr, $id:ident, $($ids:ident),*$(,)?) => {
//         let $id = $val;
//         define_var!($val + 1, $($ids,)*);
//     };
// }

// fn test() {
//     define_var!(100, a, b, c);
//     println!("{}", a);
//     println!("{}", b);
//     println!("{}", c);
// }

pub async fn run() {
    let addr = "127.0.0.1:15577";

    use async_trait::async_trait;

    struct Handler {}
    #[async_trait]
    impl znodes::Handler for Handler {
        async fn handle_nodes_sync(&self, req: znodes::NodeSyncRequest) -> znodes::NodeSyncReply {
            info!("test request: {:?}", req);
            let mut reply = znodes::NodeSyncReply::default();
            reply.uid = "reply 222".to_string();
            reply
        }
    }

    struct Watcher {}
    #[async_trait]
    impl zrpc::ClientWatcher for Watcher {
        async fn on_disconnect(&mut self, reason: zrpc::Reason, detail: &zrpc::Error) {
            error!("disconnect with [{:?}], detail [{:?}]", reason, detail);
        }
    }

    let server = zrpc::Server::builder()
        .add_service(znodes::Service::new(Handler {}))
        .build()
        .bind(addr)
        .await
        .expect(&format!("Couldn't bind to {}", addr));
    debug!("cluster service at {}", addr);

    tokio::spawn(async move {
        let mut client = zrpc::Client::builder()
            .service_type(znodes::service_type())
            .keep_alive(2)
            .watcher(Box::new(Watcher {}))
            .build();

        let r = client.connect(addr).await;
        match r {
            Ok(_r) => {
                debug!("client connect ok, {:?}", addr);
                let mut req = znodes::NodeSyncRequest::default();
                req.uid = "req 111".to_string();
                let r = client.call(req).await.unwrap();
                let reply: znodes::NodeSyncReply = r.await.unwrap();
                // let reply: znodes::NodeSyncReply = client.call(req).await.unwrap();
                info!("test reply: {:?}", reply);
            }
            Err(e) => {
                debug!("client fail to connect {:?}, {:?}", addr, e);
            }
        }

        tokio::time::sleep(std::time::Duration::from_secs(8)).await;
        client.close().await;
    });

    server.run().await;
}
