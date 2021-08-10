use crate::common::config::*;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Default, Deserialize, Serialize, Clone)]
pub struct Environment {
    pub address: String,
}

#[derive(Debug, Default, Deserialize, Serialize)]
pub struct PubArgs {
    pub connections: u64,
    pub conn_per_sec: u64,
    topic: String,
    pub qps: u64,
    pub padding_to_size: usize,
    pub packets: u64,
    pub content: String,
}

impl PubArgs {
    pub fn new() -> Self {
        hjson_default_value(
            r"{
            connections: 1 
            conn_per_sec: 1
            topic: non-persistent://public/default/test
            qps: 1
            padding_to_size: 256
            packets: 1
            content: abc
            }",
        )
    }
}

#[derive(Debug, Default, Deserialize, Serialize)]
pub struct SubArgs {
    pub connections: u64,
    pub conn_per_sec: u64,
    topic: String,
}

impl SubArgs {
    pub fn new() -> Self {
        hjson_default_value(
            r"{
            connections: 1 
            conn_per_sec: 1
            topic: non-persistent://public/default/test
        }",
        )
    }
}

#[derive(Debug, Default, Deserialize, Serialize)]
pub struct Config0 {
    pub envs: HashMap<String, Environment>,
    pub env: String,
    pub recv_timeout_ms: u64,

    #[serde(default = "PubArgs::new")]
    pub pubs: PubArgs,

    #[serde(default = "SubArgs::new")]
    pub subs: SubArgs,
}

#[derive(Debug, Default)]
pub struct Config {
    cfg0: Config0,
    sub_topic_maker: VarStr,
    pub_topic_maker: VarStr,
    env: Option<Environment>,
}

impl Config {
    pub fn load_from_file(fname: &str) -> Self {
        let mut c = config::Config::default();
        c.merge(config::File::with_name(fname)).unwrap();
        let cfg0: Config0 = c.try_into().unwrap();
        let mut self0 = Config {
            sub_topic_maker: VarStr::new(&cfg0.subs.topic),
            pub_topic_maker: VarStr::new(&cfg0.pubs.topic),
            cfg0,
            env: None,
        };

        if let Some(env) = self0.cfg0.envs.get_mut(&self0.cfg0.env) {
            //self0.env = env as *mut Environment;
            self0.env = Some(env.clone());
        } else {
            panic!("Not found env {}", self0.cfg0.env);
        }
        self0
    }

    pub fn env(&self) -> &Environment {
        self.env.as_ref().unwrap()
    }

    pub fn raw(&self) -> &Config0 {
        &self.cfg0
    }

    pub fn pub_topic(&self) -> String {
        self.pub_topic_maker.random()
    }

    pub fn sub_topic(&self) -> String {
        self.sub_topic_maker.random()
    }
}
