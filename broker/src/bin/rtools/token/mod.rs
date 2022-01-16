use std::borrow::Cow;

use anyhow::{Result, Context, bail};
use clap::Parser;
use tracing::{info};

use crate::token::a::{Input, gen_rand, gen_expired};

#[derive(Parser, Debug, Clone)]
pub struct SubArgs {
    
    #[clap(long = "secret", long_help = "secret")]
    secret: Option<String>,

    #[clap( long = "clientid", long_help = "clientid",)]
    clientid: Option<String>,

    #[clap(long = "username", long_help = "username")]
    username: Option<String>,

    #[clap(long = "token", long_help = "token")]
    token: Option<String>,

    #[clap(long = "appid", long_help = "appid")]
    appid: Option<String>,

    #[clap(long = "rand", long_help = "random string")]
    rand: Option<String>,

    #[clap(long = "expired", long_help = "expired")]
    expired: Option<u64>,
}

mod base62 {
    use anyhow::{bail, Result};
    use rand::Rng;

     const BASE: usize = 62;
    lazy_static::lazy_static!(
        pub static ref CHARS: [char; BASE] = [
            '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
            'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 
            'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 
            'U', 'V', 'W', 'X', 'Y', 'Z',
            'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 
            'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 
            'u', 'v', 'w', 'x', 'y', 'z',
        ];
    
        pub static ref REV_U8: [u8; 256] = {
            let mut data = [255_u8; 256];
            for n in 0..CHARS.len() {
                let c = CHARS[n];
                let index = (c as u8) as usize;
                data[index] = n as u8;
            }
            data
        };
        
        static ref MAX: u64 = 62*62*62*62*62*62*62*62;
    );

    pub fn string_from_u64(mut value: u64, n: usize) -> String {
        let mut data = Vec::with_capacity(n);
        for _ in 0..n {
            let index = (value % BASE as u64) as usize;
            let c = CHARS[index] as u8;
            data.push(c);
            value = value / (BASE as u64);
        }
        String::from_utf8(data).unwrap()
    }

    // pub fn string_to_u64(str: &str) -> u64 {
    //     bytes_to_u64(str.as_bytes())
    // }

    pub fn bytes_to_u64(bytes: &[u8]) -> u64 {
        lazy_static::lazy_static!(        
            pub static ref MULTI: [u64; 11] = {
                let mut multi = 1_u64;
                let mut data = [0_u64; 11];
                data[0] = multi;
                for index in 1..data.len() {
                    multi *= BASE as u64;
                    data[index] = multi;
                }
                data
            };
        );

        let mut n = 0_u64;
        let mut multi = 0_usize;
        for c in bytes {
          let v = REV_U8[*c as usize];
          n += MULTI[multi] * v as u64;
          multi += 1;
        }
        n
    }

    pub fn random_string(n: usize) -> String {
        let mut data = Vec::with_capacity(n);
        for _ in 0..n {
            let index = rand::thread_rng().gen_range(0..BASE);
            data.push(CHARS[index] as u8);
        }
        String::from_utf8(data).unwrap()
    }

    pub fn rev_string(src: &str) -> Result<String> {
        let mut data = Vec::with_capacity(src.len());
        for c in src.as_bytes() {
            let index = REV_U8[*c as usize] as usize;
            if index >= CHARS.len() {
                bail!("rev string but out of range");
            }
            data.push(CHARS[CHARS.len()-index-1] as u8);
        }
        Ok(String::from_utf8(data)?)
    }
    

    #[cfg(test)]
    mod test {
        use anyhow::Result;
        use super::*;

        #[test]
        fn test_codec() -> Result<()>{
            // let value = 0x00616263646566_u64;
            let value = 123_u64;
            let s1 = string_from_u64(value, 11);
            let value2 = bytes_to_u64(s1.as_bytes());
            println!("{:#016X}, {}, {:#016X}", value, s1, value2);
            assert_eq!(value, value2);
            Ok(())
        }

        #[test]
        fn test_rev_string() -> Result<()> {
            let value = "1234";
            let s1 = rev_string(value)?;
            let value2 = rev_string(&s1)?;
            println!("{}, {}, {}", value, s1, value2);
            assert_eq!(value, value2);
            Ok(())
        }
    }
}

pub mod a {
    use std::{ops::Add};

    use chrono::{DateTime, Local, TimeZone};
    use rand::Rng;
    use super::base62;
    use bytes::Buf;
    use anyhow::{Result, bail};

    pub const MAX_EXPIRED: u64 = 62*62*62*62*62;
    pub const TOKEN_LEN: usize = 27;

    pub const USERNAME_MASK: u64 = 0x1;

    pub const CLIENTID_MASK: u64 = 0x2;

    #[derive(Debug, Clone, Default)]
    pub struct DecodedInfo {
        pub rand: String,
        pub expired: u64, 
        pub mask: u64,
        pub is_check_username: bool,
        pub is_check_clientid: bool,
        pub appid: String,
        pub hash: String,
    }

    fn compute_hash(text: &str) -> String {
        let hash = md5::compute(text);
        let mut hash = &hash.0[..];
        let hash = hash.get_u64() & !(1 << 63);
        let hash = base62::string_from_u64(hash, 11);
        hash
    }

    fn expired_encode(value: u64) -> Result<String> {
        Ok(base62::string_from_u64(value, 5))
    }

    fn expired_decode(bytes: &[u8]) -> Result<u64> {
        Ok(base62::bytes_to_u64(bytes))
    }

    fn appid_encode(src: &str) -> Result<String> {
        base62::rev_string(src)
    }

    fn appid_decode(src: &str) -> Result<String> {
        base62::rev_string(src)
    }

    pub struct Input<'a> {
        pub secret: &'a str, 
        pub appid: &'a str, 
        pub username: Option<&'a str>, 
        pub clientid: Option<&'a str>,
        pub rand: &'a str,
        pub expired: u64,
    }

    pub fn gen_rand() -> String {
        base62::random_string(3)
    }

    pub fn gen_expired() -> u64 {
        rand::thread_rng().gen_range(0..MAX_EXPIRED)
    }

    fn make_part1(input: &Input) -> Result<String> {
        let rand = input.rand;
        let expired = expired_encode(input.expired)?;


        let mask = (
            0_u64 
            | if input.username.is_some(){USERNAME_MASK} else {0x0}
            | if input.clientid.is_some(){CLIENTID_MASK} else {0x0}
        ) as u64;
        let mask = base62::string_from_u64(mask, 1);

        let appid = appid_encode(input.appid)?;

        let part1 = format!("A{}{}{}{}", rand, expired, mask, appid);
        Ok(part1)
    }

    fn make_text(input: &Input, part1: &str) -> Result<String> {
        let text = format!(
            "{}{}{}{}", 
            part1,
            if let Some(v) = &input.username { v } else { "" },
            if let Some(v) = &input.clientid { v } else { "" },
            input.secret
        );
        Ok(text)
    }
    // A+随机数（3字符）+ 过期时间（5字符）+ mask（1个字符）+ appid变换（6个字符）+ hash 值（11字符）= 26个字符
    pub fn make_token(
        // secret: &str, 
        // appid: &str, 
        // username: Option<&str>, 
        // clientid: Option<&str>,
        // rand: Option<&str>,
        // expired: Option<u64>,
        input: &Input,
    ) -> Result<String> {

        // let rand = if let Some(s) = input.rand { 
        //     Cow::from(s) 
        // } else {
        //     Cow::from(base62::random_string(3)) 
        // };
        
        // let expired = if let Some(s) = input.expired { 
        //     s
        // } else {
        //     rand::thread_rng().gen_range(0..MAX_EXPIRED)
        // };

        // let expired = expired_encode(expired)?;

        let part1 = make_part1(input)?;
        let text = make_text(input, &part1)?;
        let hash = compute_hash(&text);

        Ok(format!("{}{}", part1, hash))
    }

    pub fn decode_token(src: &str) -> Result<DecodedInfo> {

        if src.len() != TOKEN_LEN {
            bail!("invalid token length [{}]", src.len());
        }

        let token = src.as_bytes();
        if token[0] != 'A' as u8 {
            bail!("invalid token version [{}]", token[0] as char);
        }
        
        let rand = &token[1..4];
        let rand = String::from_utf8(rand.to_vec())?;

        let expired = expired_decode(&token[4..9])?;
        
        let mask = base62::bytes_to_u64(&token[9..10]);
        let is_check_username = mask & USERNAME_MASK != 0;
        let is_check_clientid = mask & CLIENTID_MASK != 0;

        let appid = appid_decode(&src[10..16])?;

        let hash = &token[16..27];
        let hash = String::from_utf8(hash.to_vec())?;

        Ok(DecodedInfo {
            rand,
            expired,
            mask,
            is_check_username,
            is_check_clientid,
            appid,
            hash,
        })
    }

    pub fn verify(
        secret: &str, 
        token: &str,
        username: Option<&str>, 
        clientid: Option<&str>,
    ) -> Result<DecodedInfo> {
        let info = decode_token(token)?;

        let text = format!(
            "{}{}{}{}", 
            &token[0..16],
            username.as_ref().unwrap_or(&""),
            clientid.as_ref().unwrap_or(&""),
            secret,
        );
        
        let hash = compute_hash(&text);

        if hash != info.hash {
            bail!("expect hash [{}] but [{}]", hash, info.hash);
        }

        Ok(info)
    }

    pub fn expired_time_local(expired: u64) -> DateTime<Local> {
        lazy_static::lazy_static!(
            // const BASE: u64 = 1609430400_u64; // 2021/12/01
            static ref BASE: DateTime<Local> = Local.ymd(2021, 12, 01).and_hms(0, 0, 0);
        );
        let dt = BASE.add(chrono::Duration::seconds(expired as i64));
        dt
    }

    #[cfg(test)]
    mod test {
        use anyhow::{Result};
        use rust_threeq::tq3::hex::BinStrLine;
        use bytes::Buf;
        use super::*;
        
        fn get_data1() -> Vec<(Input<'static>, &'static str)> {
            vec!{
                (
                    Input {
                        secret: "0123456789ABCDEFGHIJKLMNOPQRS",
                        appid: "1PGUGY",
                        username: Some("vimin1"),
                        clientid: Some("vimin1@1PGUGY"),
                        rand: "rnd",
                        expired: 1000,
                    },
                    "Arnd8G0003yajVjRKpyAZ1B3SG5"
                ),
                (
                    Input {
                        secret: "0123456789ABCDEFGHIJKLMNOPQRS",
                        appid: "1PGUGY",
                        username: None,
                        clientid: Some("vimin1@1PGUGY"),
                        rand: "rnd",
                        expired: 1000,
                    },
                    "Arnd8G0002yajVjRZ8Q5Dq9arA5"
                ),
                (
                    Input {
                        secret: "0123456789ABCDEFGHIJKLMNOPQRS",
                        appid: "1PGUGY",
                        username: Some("vimin1"),
                        clientid: None,
                        rand: "rnd",
                        expired: 1000,
                    },
                    "Arnd8G0001yajVjRzKNnmjiFvI0"
                ),
                (
                    Input {
                        secret: "0123456789ABCDEFGHIJKLMNOPQRS",
                        appid: "1PGUGY",
                        username: None,
                        clientid: None,
                        rand: "rnd",
                        expired: 1000,
                    },
                    "Arnd8G0000yajVjRjx58xtwYEX9"
                ),
            }
        }

        #[test]
        fn test_make_token1() -> Result<()>{
            for (input, expect_token) in &get_data1() {
                let token = make_token(input)?;
                assert_eq!(expect_token, &token);

                let info = decode_token(&token)?;

                assert_eq!(info.appid, input.appid);
                assert_eq!(info.rand, input.rand);
                assert_eq!(info.expired, input.expired);
                // assert_eq!(info.mask, input.mask);
                assert_eq!(info.is_check_username, input.username.is_some());
                assert_eq!(info.is_check_clientid, input.clientid.is_some());

                verify(input.secret, &token, input.username, input.clientid)?;
            }
            Ok(())
        }

        #[test]
        fn test_verfiy_token1() -> Result<()> {
            let cases = vec!{
                (
                    Input {
                        secret: "1c7ce5d858e54192827fcba98a39a77a",
                        appid: "",
                        username: Some("test1"),
                        clientid: None,
                        rand: "",
                        expired: 1000,
                    },
                    "AU9HPIz421xrx1OzX0sJMmbhc15"
                ),
                (
                    Input {
                        secret: "1c7ce5d858e54192827fcba98a39a77a",
                        appid: "",
                        username: None,
                        clientid: Some("5d9ae362-dafa-4d91-abc4-cbc6d7ecebf0@282yb0"),
                        rand: "",
                        expired: 1000,
                    },
                    "AwvnCJz422xrx1OzhY4QB7H2iH9"
                ),
                (
                    Input {
                        secret: "1c7ce5d858e54192827fcba98a39a77a",
                        appid: "",
                        username: Some("test1"),
                        clientid: Some("5d9ae362-dafa-4d91-abc4-cbc6d7ecebf0@282yb0"),
                        rand: "",
                        expired: 1000,
                    },
                    "APMLPKz423xrx1OzhmX33ZlxwF0"
                ),

            };

            for (input0, expect_token) in &cases {
                let info = decode_token(expect_token)?;
                let input = Input {
                    secret: input0.secret,
                    appid: &info.appid,
                    username: input0.username,
                    clientid: input0.clientid,
                    rand: &info.rand,
                    expired: info.expired,
                };
                let token = make_token(&input)?;

                assert_eq!(expect_token, &token);

                let dt = expired_time_local(info.expired);
                println!("expired = [{:?}]", dt)
            }

            Ok(())
        }
        

        // --------------------------------------

        #[test]
        fn test_token() -> Result<()>{
            let (input, _token) = &get_data1()[0];

            let part1 = make_part1(input)?;
            let text = make_text(input, &part1)?;
            let hash = compute_hash(&text);
            let token = format!("{}{}", part1, hash);

            println!("part1=[{}]", part1);
            println!("text =[{}]", text);
            println!("hash =[{}]", hash);
            println!("token=[{}]", token);

            Ok(())
        }

        #[test]
        fn test_expired_codec() -> Result<()>{
            let value = 123;
            let s1 = expired_encode(123)?;
            let value2 = expired_decode(s1.as_bytes())?;
            println!("{}, {}, {}", value, s1, value2);
            assert_eq!(value, value2);
            Ok(())
        }

        #[test]
        fn test_appid_codec() -> Result<()>{
            let value = "abcdef";
            let s1 = appid_encode(&value)?;
            let value2 = appid_decode(&s1)?;
            println!("{}, {}, {}", value, s1, value2);
            assert_eq!(value, value2);
            Ok(())
        }

        #[test]
        fn test_md5() -> Result<()> {
            let text = "abc";
            let digest = md5::compute(text);
            let data = &digest.0[..];
            println!("text = [{:?}]", text );
            println!("md5 = [{:?}]", data.dump_bin_limit(usize::MAX));
            Ok(())
        }

        #[test]
        fn test_hash() -> Result<()> {
            // let text = "Aabcz1003PONMLKuser1c1abc";
            // let expect_hash = "BfGBb0PVPU4";
            
            let text = "Arnd8G003yajVjRvimin1vimin1@1PGUGY0123456789ABCDEFGHIJKLMNOPQRS";
            let expect_hash = "ADYvU19mtRA";

            let hash = compute_hash(text);
            assert_eq!(hash, expect_hash);

            let hash_value = md5::compute(text);
            let hash_h8 = &hash_value.0[..8];
            let hash_l8 = &hash_value.0[8..];
            let mut hash_buf = &hash_value.0[..8];
            let hash_value = hash_buf.get_u64();
            let hash_value2 = hash_value & !(1 << 63);
            // let hash = base62::string_from_u64(hash_u64, 11);

            let digest = md5::compute(text);
            let data = &digest.0[..];

            println!("text = [{:?}]", text );
            println!("hash_H8  = [{:?}]-[{:?}]", hash_h8.dump_bin_limit(usize::MAX), hash_h8);
            println!("hash_L8  = [{:?}]-[{:?}]", hash_l8.dump_bin_limit(usize::MAX), hash_l8);
            println!("hash_value  = [{:#016X}]-[{}]", hash_value, hash_value);
            println!("hash_value2 = [{:#016X}]-[{}]", hash_value2, hash_value2);
            println!("hash = [{:?}]", hash);
            println!("md5 = [{:?}]", data.dump_bin_limit(usize::MAX));
            Ok(())
        }

        
    }
}


pub async fn run_sub(args: &SubArgs) -> Result<()> {
    if let Some(token) = &args.token {
        match &args.secret {
            None => {
                let info = a::decode_token(token)?;
                info!("info=[{:?}]", info);
                info!("expired=[{:?}]", a::expired_time_local(info.expired));
                info!("decode Ok");
            },
            Some(secret) => {
                let info = a::verify(
                    secret, 
                    token,
                    args.username.as_deref(), 
                    args.clientid.as_deref(),
                ).with_context(||"fail to verify")?;
                info!("info=[{:?}]", info);
                info!("expired=[{:?}]", a::expired_time_local(info.expired));

                if let Some(appid) = &args.appid {
                    if info.appid != *appid {
                        bail!("check appid fail");
                    }
                }

                info!("verify Ok");
            },
        }

    }  else {
        if args.secret.is_none() {
            bail!("make token but no secret");
        }

        if args.appid.is_none() {
            bail!("make token but no appid");
        }

        let secret = args.secret.as_ref().unwrap();
        let appid = args.appid.as_ref().unwrap();

        let rand = if let Some(s) = &args.rand { 
            Cow::from(s) 
        } else {
            Cow::from(gen_rand()) 
        };

        let expired = if let Some(s) = args.expired { 
            s
        } else {
            gen_expired()
        };


        let input = Input {
            secret: secret,
            appid: appid,
            username: args.username.as_deref(),
            clientid: args.clientid.as_deref(),
            rand: &rand,
            expired,
        };

        let token = a::make_token(
            &input
            // secret, 
            // appid, 
            // args.username.as_deref(), 
            // args.clientid.as_deref(),
            // args.rand.as_deref(),
            // args.expired,
        )?;

        info!("token=[{}]", token);
    }

    Ok(())
}
