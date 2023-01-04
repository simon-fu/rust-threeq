
use std::net::{SocketAddr, Ipv4Addr, IpAddr};

use clap::Parser;
use lazy_static::lazy_static;
use rust_threeq::{tq3::app, util::AddrUrl, cluster::{ClusterConfig, NodeIdPool}, uid::NodeId};
use anyhow::Result;

// refer https://github.com/clap-rs/clap/tree/master/clap_derive/examples
#[derive(Parser, Debug, Default)]
#[clap(name = "rthreeq broker", author, about, version=app::version_long())]
pub struct Config {
    #[clap(
        short = 'l',
        long = "tcp-listen",
        // default_value = "0.0.0.0:1883",
        long_help = "tcp listen address. default 0.0.0.0:1883"
    )]
    pub tcp_listen_addr: Option<String>,

    #[clap(
        short = 'g',
        long = "enable_gc",
        long_help = "enable memory garbage collection"
    )]
    pub enable_gc: bool,

    #[clap(long = "node", long_help = "this node id", default_value = "0")]
    pub node_id: u64, // discovery::NodeId,

    #[clap(long = "bootstrap", long_help = "bootstrap node address")]
    pub bootstrap: Option<String>,

    #[clap(
        long = "cluster-listen",
        long_help = "cluster listen address. default 0.0.0.0:51000",
        // default_value = "127.0.0.1:50051"
    )]
    pub cluster_listen_addr: Option<String>,

    #[clap(
        long = "node-pool",
        long_help = "node id pool, eg. 1,50-90",
        // default_value = "127.0.0.1:50051"
    )]
    pub node_id_pool: Option<NodeIdPool>,
}

impl Config {
    pub fn tcp_listen_addr_default(&self) -> SocketAddr { 
        SocketAddr::new(Ipv4Addr::new(0, 0, 0, 0).into(), 1883)
    }

    pub fn tcp_listen_addr_by_id(&self, id: u64) -> SocketAddr { 
        let addr = self.tcp_listen_addr_default();
        if id == 0 {
            addr
        } else {
            let port = addr.port() as u64 + (id - 1) * 1000;
            let port = port.min(u16::MAX as u64) as u16;
            SocketAddr::new(addr.ip(), port)
        }
    }

    // pub fn tcp_listen_addr_iter(&self) -> AddrsIter { 
    //     AddrsIter::new(
    //         Ipv4Addr::new(0, 0, 0, 0).into(), 
    //         1883, 
    //         u16::MAX, 
    //         1000,
    //     )
    // }
    

    // pub fn cluster_listen_url(&self) -> Result<AddrUrl> {
    //     let s = self.cluster_listen_addr.as_deref().unwrap_or("127.0.0.1:50001");
    //     to_http_url(s)
    // }

    // pub fn bootstrap_url(&self) -> Result<Option<AddrUrl>> {
    //     match self.bootstrap.as_deref() {
    //         Some(s) => Ok(Some(to_http_url(s)?)),
    //         None => Ok(None),
    //     }
    // }
}

lazy_static! {
    static ref CLUSTER_DEFAULT_ADDR_STR: &'static str = "0.0.0.0:51000";
    static ref CLUSTER_DEFAULT_ADDR: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), 51000);
}

// pub struct AddrsIter {
//     ip: IpAddr,
//     currnet_port: u32,
//     end_port: u32,
//     delta_port: u32,
// }

// impl AddrsIter {
//     pub fn new(
//         ip: IpAddr,
//         start_port: u16,
//         end_port: u16,
//         delta_port: u16,
//     ) -> Self {
//         Self {
//             ip,
//             currnet_port: start_port as u32,
//             end_port: start_port.max(end_port).max(u16::MAX) as u32,
//             delta_port: delta_port as u32,
//         }
//     }
// }

// impl Iterator for AddrsIter {
//     type Item = SocketAddr;

//     fn next(&mut self) -> Option<Self::Item> {
//         if self.currnet_port <= self.end_port {
//             let addr = SocketAddr::new(self.ip, self.currnet_port as u16);
//             self.currnet_port += self.delta_port;
//             Some(addr)
//         } else {
//             None
//         }
//     }
// }

impl ClusterConfig for Config {
    fn cluster_node_id(&self) -> Option<NodeId> {
        if self.node_id == 0 {
            None
        } else {
            Some(self.node_id.into())
        }
    }

    fn cluster_listen_default(&self) -> Result<AddrUrl> {
        to_http_url(&*CLUSTER_DEFAULT_ADDR_STR)
    }

    fn cluster_listen_by_id(&self, id: u64) -> Result<AddrUrl> {
        let addr = *CLUSTER_DEFAULT_ADDR;
        let addr = if id == 0 {
            addr
        } else {
            let port = addr.port() as u64 + (id - 1) * 1000;
            let port = port.min(u16::MAX as u64) as u16;
            SocketAddr::new(addr.ip(), port)
        };
        to_http_url(&addr.to_string())
    }

    fn cluster_listen_url(&self) -> Result<Option<AddrUrl>> {
        match self.cluster_listen_addr.as_deref() {
            Some(s) => Ok(Some(to_http_url(s)?)),
            None => Ok(None),
        }
    }

    fn cluster_bootstrap_url(&self) -> Result<Option<AddrUrl>> {
        match self.bootstrap.as_deref() {
            Some(s) => Ok(Some(to_http_url(s)?)),
            None => Ok(None),
        }
    }

    fn cluster_node_id_pool(&self) -> &Option<NodeIdPool> {
        &self.node_id_pool
    }
}

fn to_http_url(s: &str) -> Result<AddrUrl> {
    AddrUrl::from_addr_str(s, "http", 80)
}
