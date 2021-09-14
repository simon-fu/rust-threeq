use std::collections::HashMap;

use crate::tq3::limit::Rate;
use crate::tq3::tt;
use rand::{distributions::Alphanumeric, Rng};

use crate::common::config::*;
use serde::{Deserialize, Deserializer, Serialize};

impl DeserializeWith for tt::QoS {
    fn deserialize_with<'de, D>(de: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(de)?;
        match s.as_ref() {
            "QoS0" => Ok(tt::QoS::AtMostOnce),
            "QoS1" => Ok(tt::QoS::AtLeastOnce),
            "QoS2" => Ok(tt::QoS::ExactlyOnce),
            _ => Err(serde::de::Error::custom("error trying to deserialize QoS")),
        }
    }
}

impl DeserializeWith for tt::Protocol {
    fn deserialize_with<'de, D>(de: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(de)?;
        match s.as_ref() {
            "4" => Ok(tt::Protocol::V4),
            "V4" => Ok(tt::Protocol::V4),
            "5" => Ok(tt::Protocol::V5),
            "V5" => Ok(tt::Protocol::V5),
            _ => Err(serde::de::Error::custom(
                "error trying to deserialize Protocol",
            )),
        }
    }
}

impl DeserializeWith for Rate {
    fn deserialize_with<'de, D>(de: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(de)?;
        let v: Vec<&str> = s.split('/').collect();

        let numer: u64 = v[0].parse().unwrap();
        let denom: u64 = if v.len() > 1 {
            v[1].parse().unwrap()
        } else {
            1
        };
        Ok(Rate::new(numer, denom))
    }
}

fn default_keep_alive_secs() -> u64 {
    30
}

#[derive(Debug, Deserialize, Serialize, Clone, Default)]
pub struct Account {
    pub user: Option<String>,
    pub password: Option<String>,
    pub client_id: Option<String>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Environment {
    pub address: String,
    pub accounts: Vec<Account>,
    #[serde(default = "RestApiArg::new")]
    pub rest_api: RestApiArg,
}

#[derive(Debug, Default, Deserialize, Serialize)]
pub struct PubArgs {
    pub connections: u64,
    pub conn_per_sec: u64,
    #[serde(default = "default_keep_alive_secs")]
    pub keep_alive_secs: u64,
    pub topic: String,
    #[serde(
        deserialize_with = "tt::QoS::deserialize_with",
        default = "tt::QoS::default"
    )]
    pub qos: tt::QoS,
    #[serde(deserialize_with = "Rate::deserialize_with", default = "Rate::default")]
    pub qps: Rate,
    pub padding_to_size: usize,
    pub content: String,
    pub packets: u64,

    #[serde(
        deserialize_with = "tt::Protocol::deserialize_with",
        default = "tt::Protocol::default"
    )]
    pub protocol: tt::Protocol,

    #[serde(default = "bool_true")]
    pub clean_session: bool,

    #[serde(default = "PubArgs::default_inflights")]
    pub inflights: usize,
}

impl PubArgs {
    pub fn new() -> Self {
        hjson_default_value(
            r"{
            connections: 1 
            conn_per_sec: 1
            topic: t1/t2
            qps: 1
            padding_to_size: 256
            content: 123
            packets: 1
        }",
        )
    }

    pub fn default_inflights() -> usize {
        1
    }
}

#[derive(Debug, Default, Deserialize, Serialize)]
pub struct SubArgs {
    pub connections: u64,
    pub conn_per_sec: u64,

    #[serde(default = "default_keep_alive_secs")]
    pub keep_alive_secs: u64,

    pub topic: String,

    #[serde(
        deserialize_with = "tt::QoS::deserialize_with",
        default = "tt::QoS::default"
    )]
    pub qos: tt::QoS,

    #[serde(
        deserialize_with = "tt::Protocol::deserialize_with",
        default = "tt::Protocol::default"
    )]
    pub protocol: tt::Protocol,

    #[serde(default = "bool_true")]
    pub clean_session: bool,
}

impl SubArgs {
    pub fn new() -> Self {
        hjson_default_value(
            r"{
            connections: 1 
            conn_per_sec: 1
            topic: t1/t2
        }",
        )
    }
}

#[derive(Debug, Default, Deserialize, Serialize, Clone)]
pub struct VerificationArgs {
    #[serde(default = "bool_true")]
    pub clean_up: bool,

    #[serde(default = "bool_true")]
    pub verify_v4: bool,

    #[serde(default = "bool_true")]
    pub verify_v5: bool,

    #[serde(default = "bool_true")]
    pub verify_basic: bool,

    #[serde(default = "bool_true")]
    pub verify_same_client_id: bool,

    #[serde(default = "bool_true")]
    pub verify_clean_session: bool,

    #[serde(default = "bool_true")]
    pub verify_retain: bool,

    #[serde(default = "bool_true")]
    pub verify_will: bool,

    #[serde(default = "bool_false")]
    pub verify_shared: bool,
}

impl VerificationArgs {
    fn new() -> VerificationArgs {
        hjson_default_value("{}")
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RestApiArg {
    pub url: String,
    pub headers: HashMap<String, String>,
    pub body: serde_json::Value,
    pub payload_in_body: Vec<String>,
}

impl RestApiArg {
    pub fn new() -> RestApiArg {
        hjson_default_value(
            r#"{
            "url": ""
            "headers": { }
            "body": { }
            payload_in_body:[]
        }"#,
        )
    }

    pub fn get_body_in<'a>(
        fields: &Vec<String>,
        root: &'a mut serde_json::Value,
    ) -> Option<&'a mut serde_json::Value> {
        if fields.is_empty() {
            return Some(root);
        } else {
            {
                let mut body = root.as_object_mut().unwrap();
                for n in 0..fields.len() {
                    let k = &fields[n];
                    if n == fields.len() - 1 {
                        return Some(&mut body[k]);
                    } else {
                        body = body[k].as_object_mut().unwrap();
                    }
                }
            }
            panic!("never reach here");
        }
    }

    pub fn get_body_mut<'a>(&self, root: &'a mut serde_json::Value) -> &'a mut String {
        let v = Self::get_body_in(&self.payload_in_body, root).unwrap();
        if let serde_json::Value::String(s) = v {
            return s;
        }
        panic!("never reach here, v {:?}", v);
    }

    pub fn make_body(&self, root: &mut serde_json::Value, str: String) -> String {
        if self.payload_in_body.is_empty() {
            return "".to_string();
        }
        let s = self.get_body_mut(root);
        *s = str;
        serde_json::to_string(root).unwrap()
    }
}

#[derive(Debug, Default, Deserialize, Serialize)]
pub struct RestPubsArg {
    pub packets: u64,
    #[serde(deserialize_with = "Rate::deserialize_with", default = "Rate::default")]
    pub qps: Rate,
    pub padding_to_size: usize,
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

    #[serde(default = "VerificationArgs::new")]
    pub verification: VerificationArgs,

    #[serde(default = "RestPubsArg::default")]
    pub rest_pubs: RestPubsArg,

    pub random_seed: Option<u64>,
}

#[derive(Debug, Default)]
pub struct Config {
    cfg0: Config0,
    // sub_topic_maker: VarStr,
    // pub_topic_maker: VarStr,
    env: Option<Environment>,
}

impl Config {
    pub fn load_from_file(fname: &str) -> Self {
        let mut c = config::Config::default();
        c.merge(config::File::with_name(fname)).unwrap();
        let cfg0: Config0 = c.try_into().unwrap();
        let mut self0 = Config {
            // sub_topic_maker: VarStr::new(&cfg0.subs.topic),
            // pub_topic_maker: VarStr::new(&cfg0.pubs.topic),
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

    pub fn verification(&self) -> &VerificationArgs {
        &self.cfg0.verification
    }

    // pub fn pub_topic(&self) -> String {
    //     self.pub_topic_maker.random()
    // }

    // pub fn sub_topic(&self) -> String {
    //     self.sub_topic_maker.random()
    // }
}

// const STAR_VAR: &str = "${*}";

pub struct AccountIter<'a> {
    iter: std::slice::Iter<'a, Account>,
    star: Option<&'a Account>,
    maker: Option<VarStr>,
}

impl AccountIter<'_> {
    pub fn new(accounts: &Vec<Account>) -> AccountIter {
        let mut o = AccountIter {
            iter: accounts.iter(),
            star: None,
            maker: None,
        };

        for v in accounts {
            if let Some(s) = &v.client_id {
                // if s.contains(STAR_VAR) {
                //     o.star = Some(v);
                //     break;
                // }
                let maker = VarStr::new(s);
                if maker.nvars() > 0 {
                    o.star = Some(v);
                    o.maker = Some(maker);
                }
            }
        }

        o
    }
}

impl<'a> Iterator for AccountIter<'a> {
    type Item = Account;
    fn next(&mut self) -> Option<Account> {
        while let Some(a) = self.iter.next() {
            if let Some(b) = self.star {
                if a as *const Account == b as *const Account {
                    break;
                }
            }
            return Some(a.clone());
        }

        if let Some(star) = self.star {
            let mut account = star.clone();
            if let Some(str) = account.client_id.as_mut() {
                // *str = str.replace(STAR_VAR, &rand_client_id());
                *str = self.maker.as_mut().unwrap().random();
            }
            return Some(account);
        }

        None
    }
}

pub fn rand_client_id() -> String {
    rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(8)
        .map(char::from)
        .collect()
}

pub fn init_conn_pkt(account: &Account, protocol: tt::Protocol) -> tt::Connect {
    let mut pkt = tt::Connect::new(account.client_id.as_deref().unwrap_or(&rand_client_id()));
    if account.user.is_some() || account.password.is_some() {
        pkt.set_login(
            account.user.as_deref().unwrap_or(""),
            account.password.as_deref().unwrap_or(""),
        );
    }
    pkt.protocol = protocol;
    pkt
}
