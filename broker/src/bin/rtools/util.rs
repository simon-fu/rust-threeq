use std::{
    fmt::Debug,
    time::{Duration, SystemTime},
};

use anyhow::{bail, Result};
use chrono::{DateTime, Local, TimeZone};
use enumflags2::bitflags;
use regex::Regex;

const ARG_TIME_FORMAT: &'static str = "%Y-%m-%dT%H:%M:%S";

#[derive(PartialEq, Clone)]
pub struct TimeArg(pub u64);

impl TimeArg {
    pub fn format(&self) -> String {
        let t = SystemTime::UNIX_EPOCH + Duration::from_millis(self.0);
        let datetime: DateTime<Local> = t.into();
        format!("{}", datetime.format(ARG_TIME_FORMAT))
    }
}

impl std::str::FromStr for TimeArg {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if let Ok(t) = Local.datetime_from_str(s, ARG_TIME_FORMAT) {
            return Ok(Self(t.timestamp_millis() as u64));
        }

        if let Ok(n) = u64::from_str(s) {
            return Ok(Self(n));
        }

        bail!("invalid time format [{}]", s);
    }
}

impl Debug for TimeArg {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("TimeArg").field(&self.format()).finish()
    }
}

#[derive(Debug, Clone)]
pub struct RegexArg(pub Regex);

impl std::str::FromStr for RegexArg {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(Regex::new(s)?))
    }
}

#[bitflags]
#[repr(u8)]
#[derive(Copy, Clone, Debug, PartialEq)]
pub enum AMatchs {
    ClientId = 0b000001,
    MsgId = 0b000010,
    PayloadText = 0b000100,
    ConnId = 0b001000,
    Topic = 0b010000,
    User = 0b100000,
}

pub trait AMatch {
    fn flag(&self) -> String;
    fn regex(&self) -> &Regex;
}

#[macro_export]
macro_rules! define_match {
    ($id1:ident, $id2:expr) => {
        #[derive(Debug, Clone)]
        pub struct $id1(pub Regex);

        impl AMatch for $id1 {
            #[inline]
            fn flag(&self) -> String {
                $id2.into()
            }
            #[inline]
            fn regex(&self) -> &Regex {
                &self.0
            }
        }

        impl std::str::FromStr for $id1 {
            type Err = anyhow::Error;
            fn from_str(s: &str) -> Result<Self, Self::Err> {
                Ok(Self(Regex::new(s)?))
            }
        }
    };
}

// define_match!(MatchClientId, AMatchs::ClientId);
// define_match!(MatchMsgId, AMatchs::MsgId);
// define_match!(MatchPayloadText, AMatchs::PayloadText);
// define_match!(MatchConnId, AMatchs::ConnId);
// define_match!(MatchTopic, AMatchs::Topic);
// define_match!(MatchUser, AMatchs::User);

define_match!(MatchClientId, "ClientId");
define_match!(MatchMsgId, "MsgId");
define_match!(MatchPayloadText, "PayloadText");
define_match!(MatchConnId, "ConnId");
define_match!(MatchTopic, "Topic");
define_match!(MatchUser, "User");

#[derive(Debug, Default)]
pub struct MatchFlag {
    pub flags: Vec<String>,
}

impl MatchFlag {
    pub fn is_empty(&self) -> bool {
        self.flags.is_empty()
    }

    // pub fn flags(&self) -> &Vec<String> {
    //     &self.flags
    // }

    pub fn match_text<A: AMatch>(&mut self, arg: &Option<A>, text: &str) {
        if let Some(m) = arg {
            if m.regex().is_match(text) {
                self.flags.push(m.flag());
            }
        }
    }

    pub fn match_utf8<A: AMatch>(&mut self, arg: &Option<A>, data: Vec<u8>) {
        if let Some(m) = arg {
            let r = String::from_utf8(data);
            if let Ok(text) = r {
                if m.regex().is_match(&text) {
                    self.flags.push(m.flag());
                }
            }
        }
    }
}

// #[derive(Debug, Clone)]
// pub struct MatchClientId(pub Regex);

// impl AMatch for MatchClientId {
//     fn flag(&self) -> AMatchs {
//         AMatchs::ClientId
//     }
// }

// impl std::str::FromStr for MatchClientId {
//     type Err = anyhow::Error;
//     fn from_str(s: &str) -> Result<Self, Self::Err> {
//         Ok(Self(Regex::new(s)?))
//     }
// }
