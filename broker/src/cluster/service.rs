


// use super::cluster::Cluster;
// use super::proto::rpc_server::Rpc;
// use super::proto::{StringObj, ReflexAddr};
// use tonic::{Request, Response, Status};


// pub struct ClusterService {
//     cluster: Cluster,
// }

// impl ClusterService {
//     pub fn new(cluster: Cluster) -> Self {
//         Self { cluster }
//     }
// }


// #[tonic::async_trait]
// impl Rpc for ClusterService {
//     async fn exchange_addr(
//         &self,
//         request: Request<StringObj>, // Accept request of type HelloRequest
//     ) -> Result<Response<ReflexAddr>, Status> { // Return an instance of type HelloReply

//         let reply = ReflexAddr {
//             value: request.remote_addr().map(|x|x.to_string()),
//         };
//         self.cluster.self_node().put_addr(request.into_inner().value);

//         Ok(Response::new(reply)) 
//     }
// }


