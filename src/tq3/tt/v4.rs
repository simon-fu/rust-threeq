use bytes::Bytes;
use super::*;
pub struct TTT{
    s : String,
}

fn to_lastwill(src_opt : Option<mqttbytes::v4::LastWill>) -> Option<mqttbytes::v5::LastWill>{
    match src_opt{
        Some(src) => {
            let mut dst = mqttbytes::v5::LastWill::new(src.topic, [], src.qos, src.retain);
            dst.message = src.message;
            return Some(dst);
        },
        None => return None,
    };
}

fn to_login(src_opt : Option<mqttbytes::v4::Login>) -> Option<mqttbytes::v5::Login>{
    match src_opt{
        Some(src) => {
            let mut dst = mqttbytes::v5::Login::new("", "");
            return Some(dst);
        },
        None => return None,
    };
}


// fn to_connect(src : mqttbytes::v4::Connect) -> Connect{
//     let dst = Connect::new(src.client_id);
//     dst.keep_alive = src.keep_alive;
//     dst.client_id = src.client_id;
//     dst.clean_session = src.clean_session;
//     dst.last_will = to_lastwill(src.last_will);
//     //dst.login = src.login;
    
//     return dst;
// }


// pub fn decode_connect(fixed_header: FixedHeader, mut bytes: Bytes) -> Result<Connect, Error>{
//     let v4_pkt = mqttbytes::v4::Connect::read(fixed_header, bytes)?;
//     return Ok(to_connect(v4_pkt));
// }

// pub fn decode_publish(fixed_header: FixedHeader, mut packet: Bytes) -> Result<Publish, Error>{
//     let v4_pkt = mqttbytes::v4::Connect::read(fixed_header, packet)?;
// }

