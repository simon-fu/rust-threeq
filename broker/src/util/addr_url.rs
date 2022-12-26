use std::net::{SocketAddr, IpAddr};
use anyhow::{Result, Context};
use reqwest::Url;

pub struct AddrUrl {
    url: Url,
    default_port: u16,
}

impl AddrUrl {
    pub fn from_addr_str(s: &str, scheme: &str, default_port: u16) -> Result<Self> {
        let url_str = format!("{}://{}", scheme, s);
        Self::from_url_str(&url_str, default_port)
    }

    pub fn from_url_str(s: &str, default_port: u16) -> Result<Self> {
        let url = Url::parse(s)?;
        Ok(Self::from_url(url, default_port))
    }

    pub fn from_url(url: Url, default_port: u16) -> Self {
        Self{url, default_port}
    }

    pub fn port(&self) -> u16 {
        self.url.port().unwrap_or(self.default_port)
    }

    pub fn to_sockaddr(&self) -> Result<SocketAddr> {
        let host = self.url.host_str()
        .with_context(||format!("no host of url [{}]", self.url))?;
        let port = self.url.port().unwrap_or(self.default_port);
        let ip: IpAddr = host.parse()?;
        Ok((ip, port).into())
    }

    pub fn listen_url(&self) -> ListenUrl<'_> {
        ListenUrl{owner: self}
    }
}

impl std::fmt::Display for AddrUrl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.listen_url(), f)
    }
}

pub struct ListenUrl<'a> {
    owner: &'a AddrUrl,
}

impl<'a> std::fmt::Display for ListenUrl<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.owner.url.scheme())?;
        f.write_str("://")?;
        if let Some(host) = self.owner.url.host_str() {
            f.write_str(host)?;
        }
        if let Some(port) = self.owner.url.port() {
            f.write_fmt(format_args!(":{}", port))?;
        }
        Ok(())
    }
}
