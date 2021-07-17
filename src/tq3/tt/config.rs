use std::collections::HashMap;

use crate::tq3::tt;
use rand::{distributions::Alphanumeric, Rng};

#[derive(Debug, Deserialize, Serialize, Clone, Default)]
pub struct Account {
    pub user: Option<String>,
    pub password: Option<String>,
    pub client_id: Option<String>,
}

#[derive(Debug, Default, Deserialize, Serialize, Clone)]
pub struct Environment {
    pub address: String,
    pub accounts: Vec<Account>,
}

use regex::Regex;
use serde::{Deserialize, Deserializer};
pub trait DeserializeWith: Sized {
    fn deserialize_with<'de, D>(de: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>;
}

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

#[derive(Debug, Default, Deserialize, Serialize)]
pub struct PubArgs {
    pub connections: u64,
    pub conn_per_sec: u64,
    topic: String,
    #[serde(
        deserialize_with = "tt::QoS::deserialize_with",
        default = "tt::QoS::default"
    )]
    pub qos: tt::QoS,
    pub qps: u64,
    pub size: usize,
    pub payload: String,
    pub packets: u64,
}

#[derive(Debug, Default, Deserialize, Serialize)]
pub struct SubArgs {
    pub connections: u64,
    pub conn_per_sec: u64,
    topic: String,

    #[serde(
        deserialize_with = "tt::QoS::deserialize_with",
        default = "tt::QoS::default"
    )]
    pub qos: tt::QoS,
}

#[derive(Debug, Default, Deserialize, Serialize)]
pub struct Config0 {
    pub envs: HashMap<String, Environment>,
    pub env: String,
    pub recv_timeout_ms: u64,
    pub pubs: PubArgs,
    pub subs: SubArgs,
}

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
                if a as *const Account != b as *const Account {
                    return Some(a.clone());
                } else {
                    break;
                }
            }
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

#[derive(Debug, Default, Clone, Deserialize, Serialize)]
pub struct VarStr {
    buf: Vec<char>,
    vars: Vec<(usize, usize)>,
}

impl<'t> VarStr {
    pub fn new(text: &str) -> Self {
        lazy_static::lazy_static! {
            static ref VAR_STR_RE: Regex = Regex::new(r"\$R\{(?P<N>\d{1,})\}").unwrap();
        }

        let text_vec: Vec<char> = text.chars().collect::<Vec<_>>();
        let mut self0 = Self {
            buf: Vec::new(),
            vars: Vec::new(),
        };
        let mut src = 0;

        for cap in VAR_STR_RE.captures_iter(text) {
            let c0 = cap.get(0).unwrap();
            let c1 = cap.get(1).unwrap();
            let n = c1.as_str().parse::<usize>().unwrap();
            while src < c0.start() {
                self0.buf.push(text_vec[src]);
                src += 1;
            }
            src = c0.end();

            self0.vars.push((self0.buf.len(), self0.buf.len() + n));

            for _i in 0..n {
                self0.buf.push('0');
            }
        }
        while src < text_vec.len() {
            self0.buf.push(text_vec[src]);
            src += 1;
        }

        return self0;
    }

    pub fn new_option(text: &str) -> Option<Self> {
        let v = Self::new(text);
        if v.nvars() > 0 {
            Some(v)
        } else {
            None
        }
    }

    pub fn nvars(&self) -> usize {
        self.vars.len()
    }

    pub fn make<F, T>(&self, mut f: F) -> String
    where
        F: FnMut(&mut [char]) -> T,
    {
        let mut buf = self.buf.clone();
        for v in &self.vars {
            f(&mut buf[v.0..v.1]);
        }
        return buf.iter().collect::<String>();
    }

    pub fn random(&self) -> String {
        let mut iter =
            rand::Rng::sample_iter(rand::thread_rng(), &rand::distributions::Alphanumeric)
                .map(char::from);

        return self.make(|s| {
            for v in s {
                *v = iter.next().unwrap();
            }
        });
    }

    pub fn fill(&self, c: char) -> String {
        return self.make(|s| {
            for v in s {
                *v = c;
            }
        });
    }

    pub fn number(&self) -> String {
        lazy_static::lazy_static! {
            static ref NUMBER:Vec<char> = vec!['0', '1', '2', '3', '4', '5', '6', '7', '8', '9',];
        }
        return self.make(|s| {
            let mut i = 0;
            for v in s {
                *v = NUMBER[i % NUMBER.len()];
                i += 1;
            }
        });
    }
}
