use tonic::transport::Channel;


tonic::include_proto!("cluster.rpc"); // The string specified here must match the proto package name

pub type RpcClient = rpc_client::RpcClient<Channel>;


pub(crate) trait MergeAddrs {
    fn merge_addrs<I: Iterator<Item = String>>(&mut self, addrs: I) -> bool;
}

impl MergeAddrs for NodeInfo {
    fn merge_addrs<I: Iterator<Item = String>>(&mut self, addrs: I) -> bool {
        let mut updated = false;
        for s in addrs {
            let exist = self.addrs.iter().position(|x| *x == s).is_some();
            if !exist {
                self.addrs.push(s);
                updated = true;
            }
        }
        updated
    }
}
