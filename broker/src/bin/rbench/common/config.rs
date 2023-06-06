use std::collections::HashMap;
use anyhow::{Result, anyhow};
use nom::{IResult, bytes::complete::{take_while, tag}, character::complete::space0};
use rand::SeedableRng;
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




#[derive(Debug, Default, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct VarStr {
    buf: Vec<char>,
    vars: Vec<Var>,
}

impl<'t> VarStr {
    pub fn new(text: &str) -> Self {
        let r = parse_var_full(text);
        match r {
            Ok(s) => s,
            Err(_e) => Self { 
                buf: text.chars().collect::<Vec<_>>(), 
                vars: Vec::new(),
            },
        }
    }

    // pub fn new(text: &str) -> Self {
    //     lazy_static::lazy_static! {
    //         // static ref VAR_STR_RE: Regex = Regex::new(r"\$R\{(?P<N>\d{1,})\}").unwrap();
    //         static ref VAR_STR_RE: Regex = Regex::new(r"\$S\{(?P<N>\d{1,})\}").unwrap();
    //     }

    //     let text_vec: Vec<char> = text.chars().collect::<Vec<_>>();
    //     let mut self0 = Self {
    //         buf: Vec::new(),
    //         vars: Vec::new(),
    //     };
    //     let mut src = 0;

    //     for cap in VAR_STR_RE.captures_iter(text) {
    //         let c0 = cap.get(0).unwrap();
    //         let c1 = cap.get(1).unwrap();
    //         let n = c1.as_str().parse::<usize>().unwrap();
    //         while src < c0.start() {
    //             self0.buf.push(text_vec[src]);
    //             src += 1;
    //         }
    //         src = c0.end();

    //         // self0.vars.push((self0.buf.len(), self0.buf.len() + n, 0));
    //         self0.vars.push(Var { 
    //             begin: self0.buf.len(), 
    //             end: self0.buf.len() + n, 
    //             seq: None,
    //         });

    //         for _i in 0..n {
    //             self0.buf.push('0');
    //         }
    //     }
    //     while src < text_vec.len() {
    //         self0.buf.push(text_vec[src]);
    //         src += 1;
    //     }

    //     return self0;
    // }

    // pub fn new_option(text: &str) -> Option<Self> {
    //     let v = Self::new(text);
    //     if v.nvars() > 0 {
    //         Some(v)
    //     } else {
    //         None
    //     }
    // }

    pub fn is_dyn(&self) -> bool {
        self.vars.len() > 0
    }

    pub fn nvars(&self) -> usize {
        self.vars.len()
    }

    // fn make<F, T>(&self, mut f: F) -> String
    // where
    //     F: FnMut(&mut [char]) -> T,
    // {
    //     let mut buf = self.buf.clone();
    //     for v in &self.vars {
    //         f(&mut buf[v.0..v.1]);
    //     }
    //     return buf.iter().collect::<String>();
    // }

    // pub fn random(&self) -> String {
    //     let mut iter =
    //         rand::Rng::sample_iter(rand::thread_rng(), &rand::distributions::Alphanumeric)
    //             .map(char::from);

    //     return self.make(|s| {
    //         for v in s {
    //             *v = iter.next().unwrap();
    //         }
    //     });
    // }

    pub fn random(&mut self) -> String {
        self.random_with(&mut rand::thread_rng())
    }

    // pub fn random_with<R: rand::Rng>(&self, random: &mut R) -> String {
    //     return self.make(|s| {
    //         for v in s {
    //             let r = random.sample(&rand::distributions::Alphanumeric);
    //             *v = char::from(r);
    //         }
    //     });
    // }

    pub fn random_with<R: rand::Rng>(&mut self, random: &mut R) -> String {

        let mut buf = self.buf.clone();
        for v in &mut self.vars {
            match &mut v.seq {
                Some(seq) => {
                    *seq += 1;
                    let src = seq.to_string();
                    let mut src = src.chars().rev();
                    let dst = &mut buf[v.begin..v.end];
                    let mut dst = dst.iter_mut().rev();
        
                    while let Some(s) = src.next() {
                        if let Some(d) = dst.next() {
                            *d = s;
                        } else {
                            break;
                        }
                    }
        
                    while let Some(d) = dst.next() {
                        *d = '0';
                    }
                },
                None => {
                    let dst = &mut buf[v.begin..v.end];
                    for d in dst.iter_mut() {
                        let r = random.sample(&rand::distributions::Alphanumeric);
                        *d = char::from(r);
                    }                    
                },
            }
            
        }
        return buf.iter().collect::<String>();
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

#[derive(Debug, Default, Clone, Deserialize, Serialize, PartialEq, Eq)]
struct Var {
    begin: usize,
    end: usize,
    seq: Option<u64>,
}

fn parse_var_full(text: &str) -> Result<VarStr> {
    let mut self0 = VarStr {
        buf: Vec::new(),
        vars: Vec::new(),
    };

    let mut input = text;
    while !input.is_empty() {
        {
            let s;
            (input, s) = take_until_dollar(input).map_err(|e|anyhow::anyhow!("{:?}", e))?;
            self0.buf.extend(s.chars());
        }
        
        if !input.is_empty() {
            let mut m;
            (input, m) = take_dollar_type(input).map_err(|e|anyhow::anyhow!("{:?}", e))?;

            while m == "$" {
                self0.buf.push('$');
                (input, m) = take_m(input, 1).map_err(|e|anyhow::anyhow!("{:?}", e))?;
            }

            if m == "R" {
                let v;
                (input, v) = parse_to_var(input, &mut self0.buf)?;
                self0.vars.push(v);
            } else if m == "S" {
                let mut v;
                (input, v) = parse_to_var(input, &mut self0.buf)?;
                v.seq = Some(0);
                self0.vars.push(v);
            } else {
                self0.buf.push('$');
                self0.buf.extend(m.chars());
            }
        }
    }
    Ok(self0)
}


fn take_m(input: &str, num: usize) -> IResult<&str, &str> {
    let n_remainings = std::cell::Cell::new(num);

    take_while(move |_c| {
        if n_remainings.get() == 0 {
            false
        } else {
            n_remainings.set(n_remainings.get() - 1);
            true
        }
    })(input)    
}

fn take_dollar_type(mut input: &str) -> IResult<&str, &str> {
    (input, _) = tag("$")(input)?;
    take_m(input, 1)
}

fn take_until_dollar(input: &str) -> IResult<&str, &str> {
    take_while(|c| {
        c != '$'
    })(input)
}


fn parse_to_var<'a>(mut input: &'a str, buf: &mut Vec<char>) -> Result<(&'a str, Var)> {
    let n;
    (input, n) = parse_to_var2(input).map_err(|e|anyhow!("{:?}", e))?;
    buf.extend((0..n).map(|_x|'0'));
    Ok((input, Var {
        begin: buf.len()-n,
        end: buf.len(),
        seq: None,
    }))
}

fn parse_to_var2(mut input: &str) -> IResult<&str, usize> {
    let s;
    (input, s) = parse_to_var_expr(input)?;

    let v: usize = s.parse().map_err(|_e|nom::Err::Error(nom::error::Error::new(s, nom::error::ErrorKind::Digit)))?;
    
    Ok((input, v))
}

fn parse_to_var_expr(mut input: &str) -> IResult<&str, &str> {
    (input, _) = space0(input)?;
    (input, _) = tag("{")(input)?;

    let s;
    (input, s) = take_while(|c| {
        c != '}'
    })(input)?;

    if !input.is_empty() {
        (input, _) = tag("}")(input)?;
    }

    Ok((input, s))
}

// fn take_until_left_brace(mut input: &str) -> IResult<&str, &str> {
//     let mut name;
//     (input, name) = take_until_delimiters(input, &['{', ','])?;
//     name = name.trim();
//     Ok((input, name))
// }

// #[inline]
// fn take_until_delimiters<'a>(input: &'a str, delimiters: &[char]) -> IResult<&'a str, &'a str> {
//     take_while(|c| {
//         delimiters.iter().position(|x|*x == c).is_none()
//     })(input)
// }


pub fn make_pubsub_topics(
    pub_n: u64,
    pub_topic: &str,
    sub_n: u64,
    sub_topic: &str,
    seed: &Option<u64>,
) -> (Vec<(u64, String)>, Vec<String>, String) {
    let mut random_ = match seed {
        Some(n) => rand::rngs::StdRng::seed_from_u64(*n),
        None => rand::rngs::StdRng::seed_from_u64(rand::Rng::gen(&mut rand::thread_rng())),
    };
    let random = &mut random_;

    let mut pubv = VarStr::new(pub_topic);
    let mut subv = VarStr::new(sub_topic);

    let mut pub_topics: Vec<(u64, String)> = Vec::new();
    let mut sub_topics: Vec<String> = Vec::new();

    if pubv.is_dyn() && sub_topic == "-" {
        for _ in 0..pub_n {
            let t = pubv.random_with(random);
            pub_topics.push((0, t.clone()));
            for _ in 0..sub_n {
                sub_topics.push(t.clone());
            }
        }
        (pub_topics, sub_topics, "subs-follow-pubs".to_string())
    } else if subv.is_dyn() && pub_topic == "-" {
        for _ in 0..sub_n {
            let t = subv.random_with(random);
            sub_topics.push(t.clone());
            for index in 0..pub_n {
                pub_topics.push((index, t.clone()));
            }
        }
        (pub_topics, sub_topics, "pubs-follow-subs".to_string())
    } else {
        if !pubv.is_dyn() {
            for index in 0..pub_n {
                pub_topics.push((index, pubv.random_with(random)));
            }
        } else {
            for _index in 0..pub_n {
                pub_topics.push((0, pubv.random_with(random)));
            }
        }

        for _ in 0..sub_n {
            sub_topics.push(subv.random_with(random));
        }
        (
            pub_topics,
            sub_topics,
            "pubs-subs-independently".to_string(),
        )
    }
}


// #[cfg(test)]
// mod test {
//     use super::{VarStr};

//     #[test]
//     fn test_parse_var() {
//         println!("{:?}", VarStr::new("abc"));
//         println!("{:?}", VarStr::new("abc$R"));
//         println!("{:?}", VarStr::new("abc$R{3}"));
//         println!("{:?}", VarStr::new("abc$$R{3}"));
//     }
// }
