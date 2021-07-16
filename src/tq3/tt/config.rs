use crate::tq3::tt;
use rand::{distributions::Alphanumeric, Rng};

#[derive(Debug, Deserialize, Serialize, Clone, Default)]
pub struct Account {
    pub user: Option<String>,
    pub password: Option<String>,
    pub client_id: Option<String>,
}

#[derive(Debug, Default, Deserialize, Serialize)]
pub struct Environment {
    pub address: String,
    pub accounts: Vec<Account>,
}

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
    pub topic: String,
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
    pub topic: String,
    #[serde(
        deserialize_with = "tt::QoS::deserialize_with",
        default = "tt::QoS::default"
    )]
    pub qos: tt::QoS,
}

#[derive(Debug, Default, Deserialize, Serialize)]
pub struct Config {
    pub env: Environment,
    pub recv_timeout_ms: u64,
    pub pubs: PubArgs,
    pub subs: SubArgs,
}

const STAR_VAR: &str = "${*}";

pub struct AccountIter<'a> {
    iter: std::slice::Iter<'a, Account>,
    star: Option<&'a Account>,
}

impl AccountIter<'_> {
    pub fn new(accounts: &Vec<Account>) -> AccountIter {
        let mut o = AccountIter {
            iter: accounts.iter(),
            star: None,
        };

        for v in accounts {
            if let Some(s) = &v.client_id {
                if s.contains(STAR_VAR) {
                    o.star = Some(v);
                    break;
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
                *str = str.replace(STAR_VAR, &rand_client_id());
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
