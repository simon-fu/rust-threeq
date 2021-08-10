use std::collections::HashMap;

use regex::Regex;
use serde::{Deserialize, Deserializer, Serialize};

pub fn bool_true() -> bool {
    return true;
}

pub fn bool_false() -> bool {
    return false;
}

pub trait DeserializeWith: Sized {
    fn deserialize_with<'de, D>(de: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>;
}

pub fn hjson_default_value<'a, T: Deserialize<'a>>(contents: &str) -> T {
    let mut c = config::Config::default();
    c.merge(StringSource::new(config::FileFormat::Hjson, contents))
        .unwrap();
    let v: T = c.try_into().unwrap();
    v
}

#[derive(Debug, Clone)]
pub struct StringSource {
    format: config::FileFormat,
    contents: String,
}

impl StringSource {
    pub fn new(format: config::FileFormat, contents: &str) -> Self {
        Self {
            format,
            contents: contents.to_string(),
        }
    }
}

impl config::Source for StringSource {
    fn clone_into_box(&self) -> Box<dyn config::Source + Send + Sync> {
        Box::new((*self).clone())
    }

    fn collect(&self) -> Result<HashMap<String, config::Value>, config::ConfigError> {
        self.format
            .parse(None, &self.contents)
            .map_err(|cause| config::ConfigError::FileParse { uri: None, cause })
    }
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

    // pub fn new_option(text: &str) -> Option<Self> {
    //     let v = Self::new(text);
    //     if v.nvars() > 0 {
    //         Some(v)
    //     } else {
    //         None
    //     }
    // }

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

    // pub fn fill(&self, c: char) -> String {
    //     return self.make(|s| {
    //         for v in s {
    //             *v = c;
    //         }
    //     });
    // }

    // pub fn number(&self) -> String {
    //     lazy_static::lazy_static! {
    //         static ref NUMBER:Vec<char> = vec!['0', '1', '2', '3', '4', '5', '6', '7', '8', '9',];
    //     }
    //     return self.make(|s| {
    //         let mut i = 0;
    //         for v in s {
    //             *v = NUMBER[i % NUMBER.len()];
    //             i += 1;
    //         }
    //     });
    // }
}
