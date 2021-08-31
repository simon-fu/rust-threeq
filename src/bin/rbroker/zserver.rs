// mod zserver {
// use super::zutil;
use async_trait::async_trait;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use tracing::Instrument;
use std::slice::Iter;
use std::{marker::PhantomData, net::SocketAddr, sync::Arc};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

pub fn decode_packet(
    ibuf: &mut BytesMut,
    max_packet_size: usize,
) -> std::io::Result<Option<(i32, Bytes)>> {
    if ibuf.len() < 4 {
        return Ok(None);
    }

    let (ptype, payload_len) = decode_header(ibuf.iter());
    if payload_len > max_packet_size {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "exceed max packet size",
        ));
    }

    if ibuf.len() < (4 + payload_len) {
        return Ok(None);
    }

    ibuf.advance(4);
    let bytes = ibuf.split_to(payload_len).freeze();
    return Ok(Some((ptype, bytes)));
}

fn decode_header(mut stream: Iter<u8>) -> (i32, usize) {
    let mut dw: u32 = 0;
    dw |= (*stream.next().unwrap() as u32) << 24;
    dw |= (*stream.next().unwrap() as u32) << 16;
    dw |= (*stream.next().unwrap() as u32) << 8;
    dw |= (*stream.next().unwrap() as u32) << 0;
    let payload_len: usize = (dw & 0x003FFFFF) as usize;
    let ptype: i32 = ((dw >> 22) & 0x3FF) as i32;
    (ptype, payload_len)
}

pub fn encode_header<B: BufMut>(ptype: i32, payload_len: usize, obuf: &mut B) {
    let mut dw: u32 = 0;
    dw |= payload_len as u32 & 0x003FFFFF;
    dw |= (ptype as u32 & 0x3FF) << 22;
    obuf.put_u32(dw);
}

#[derive(Clone, Debug)]
struct Config {
    max_packet_size: usize,
}

impl Config {
    fn new() -> Self {
        Self {
            max_packet_size: usize::MAX,
        }
    }
}

#[derive(Debug)]
pub struct Server<S: Session, F: SessionFactory<S>> {
    phantom: PhantomData<S>,
    cfg: Box<Config>,
    listener: Option<TcpListener>,
    factory: Option<F>,
}

impl<S: Send + Session + 'static, F: SessionFactory<S>> Server<S, F> {
    pub fn builder() -> Self {
        Self {
            phantom: PhantomData,
            cfg: Box::new(Config::new()),
            listener: None,
            factory: None,
        }
    }

    pub fn factory(mut self, f: F) -> Self {
        self.factory = Some(f);
        self
    }

    pub fn build(self) -> Self {
        self
    }

    pub async fn bind(&mut self, addr: &str) -> std::io::Result<()> {
        let r = TcpListener::bind(&addr).await;
        if let Err(e) = r {
            let e = std::io::Error::new(e.kind(), format!("{}, address {}", e.to_string(), addr));
            return Err(e);
        }
        self.listener = Some(r.unwrap());
        Ok(())
    }

    pub fn local_addr(&self) -> Option<SocketAddr> {
        if let Some(listener) = &self.listener {
            let r = listener.local_addr();
            if let Ok(addr) = r {
                return Some(addr);
            }
        }
        return None;
    }

    pub async fn run(&self) {
        let cfg = Arc::new(*self.cfg.clone());
        loop {
            let listener = self.listener.as_ref().unwrap();
            let (mut socket, addr) = listener.accept().await.expect("something wrong");
            let mut session = self.factory.as_ref().unwrap().make_session(&socket, &addr);
            let cfg0 = cfg.clone();
            let f = async move {
                let r = conn_entry(&mut socket, &mut session, cfg0).await;
                if let Err(e) = r {
                    session.handle_final_error(e);
                }
            };
            let span = tracing::span!(tracing::Level::INFO, "");
            tokio::spawn(Instrument::instrument(f, span));
            // tokio::spawn(f);
        }
    }
}

pub trait SessionFactory<S> {
    fn make_session(&self, socket: &TcpStream, addr: &SocketAddr) -> S;
}

pub enum Action {
    None,
    Close,
}

#[async_trait]
pub trait Session {
    async fn handle_packet(
        &mut self,
        ptype: i32,
        bytes: Bytes,
        obuf: &mut BytesMut,
    ) -> Result<Action, String>;
    fn handle_final_error(&self, e: std::io::Error);
}

async fn conn_entry<S: Send + Session>(
    socket: &mut TcpStream,
    session: &mut S,
    cfg: Arc<Config>,
) -> std::io::Result<()> {
    let (mut rd, mut wr) = socket.split();
    let mut ibuf = BytesMut::new();
    let mut obuf = BytesMut::new();

    loop {
        tokio::select! {
            r = rd.read_buf(&mut ibuf) => {
                let n = r?;
                if n == 0 {
                    return Err(std::io::Error::new(std::io::ErrorKind::BrokenPipe, "detect disconnect by client"));
                }
                let r = decode_packet(&mut ibuf, cfg.max_packet_size)?;
                if let Some((ptype, bytes)) = r {
                    let r = session.handle_packet(ptype, bytes, &mut obuf).await;
                    match r {
                        Err(e) =>  {
                            return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, e));
                        }
                        Ok(action) => {
                            match action {
                                Action::None => {},
                                Action::Close => {break;}
                            }
                        }
                    }
                }
            }

            r = wr.write_buf(&mut obuf), if obuf.len() > 0 => {
                match r{
                    Ok(_n) => { },
                    Err(e) => {
                        return Err(e);
                    },
                }
            }
        }
    }
    Ok(())
}
// }
