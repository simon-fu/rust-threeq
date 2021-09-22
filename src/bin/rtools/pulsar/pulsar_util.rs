// #![crate_type = "lib"]
// #![allow(dead_code)]

use std::str::FromStr;

use anyhow::{bail, Context, Result};
use chrono::{DateTime, Local};
use pulsar::{proto::MessageIdData, ConsumerOptions, DeserializeMessage, Pulsar};
use reqwest::RequestBuilder;
use rust_threeq::here;
use serde::{de::DeserializeOwned, Deserialize, Serialize};

pub struct Admin {
    client: reqwest::Client,
    url: String,
}

impl Admin {
    pub fn new(url: String) -> Self {
        Self {
            client: reqwest::Client::new(),
            url,
        }
    }

    async fn exec_text(builder: RequestBuilder) -> Result<String> {
        let res = builder.send().await?;

        let rsp_status = res.status();
        if !rsp_status.is_success() {
            bail!("error http status {}", rsp_status);
        }

        let rsp_body = res.text().await.context(here!())?;
        Ok(rsp_body)
    }

    pub async fn post_text(&self, path: &str, text: String) -> Result<String> {
        let builder = self.client.post(path).body(text);
        let s = Self::exec_text(builder)
            .await
            .context(here!())
            .context(format!("fail to post {}", path))?;
        Ok(s)
    }

    pub async fn get_text(&self, path: &str) -> Result<String> {
        let builder = self.client.get(path);
        let s = Self::exec_text(builder)
            .await
            .context(here!())
            .context(format!("fail to get {}", path))?;
        Ok(s)
    }

    pub async fn get<T: DeserializeOwned>(&self, path: &str) -> Result<T> {
        let rsp_body = self.get_text(path).await?;
        // log::debug!("{}", rsp_body);
        let v: T = serde_json::from_str(&rsp_body)
            .context(format!("json=[{}]", rsp_body))
            .context(here!())?;
        Ok(v)
    }

    pub async fn lookup_topic(&self, topic: &TopicParts) -> Result<TopicLocation> {
        // GET /lookup/v2/topic/:schema/:tenant/:namespace/:topic
        let path = format!(
            "{}/lookup/v2/topic/{}/{}/{}/{}",
            self.url, topic.schema, topic.tenant, topic.namespace, topic.topic
        );
        Ok(self
            .get(&path)
            .await
            .context("fail to lookup topic")
            .context(here!())?)
    }

    pub async fn get_last_msgid(&self, topic: &TopicParts) -> Result<EntryIndex> {
        // Get /admin/v2/:schema/:tenant/:namespace/:topic/lastMessageId?version=2.8.0
        let path = format!(
            "{}/admin/v2/{}/{}/{}/{}/lastMessageId?version=2.8.0",
            self.url, topic.schema, topic.tenant, topic.namespace, topic.topic
        );
        let s: EntryIndex = self
            .get(&path)
            .await
            .context(here!())
            .context("fail to get last msgid")?;
        Ok(s)
    }

    pub async fn reset_cursor(
        &self,
        topic: &TopicParts,
        sub_name: &str,
        timestamp: u64,
    ) -> Result<()> {
        // POST /admin/v2/:schema/:tenant/:namespace/:topic/subscription/:subName/resetcursor/:timestamp
        let path = format!(
            "{}/admin/v2/{}/{}/{}/{}/subscription/{}/resetcursor/{}",
            self.url, topic.schema, topic.tenant, topic.namespace, topic.topic, sub_name, timestamp
        );
        let _r = self.post_text(&path, "".to_string()).await?;
        // log::debug!("reset_cursor {}", _r);
        Ok(())
    }

    // pub async fn list_tenants(&self) -> Result<Vec<String>> {
    //     // curl http://localhost:18080/admin/v2/tenants
    //     let path = format!("{}/admin/v2/tenants", self.url);
    //     Ok(self.get(&path).await?)
    // }

    // pub async fn post<T: DeserializeOwned>(&self, path: &str, text: String) -> Result<T> {
    //     let rsp_body = self.post_text(path, text).await?;
    //     // log::debug!("{}", rsp_body);
    //     let v: T = serde_json::from_str(&rsp_body)
    //     .context(format!("json=[{}]",rsp_body))
    //     .context(here!())?;
    //     Ok(v)
    // }

    // pub async fn topic_internal_state(&self, topic: &Topic) -> Result<TopicInternalState> {
    //     //GET /admin/v2/:schema/:tenant/:namespace/:topic/internalStats
    //     let path = format!(
    //         "{}/admin/v2/{}/{}/{}/{}/internalStats",
    //         self.url, topic.schema, topic.tenant, topic.namespace, topic.topic
    //     );
    //     Ok(self
    //         .get(&path)
    //         .await
    //         .context(here!())
    //         .context("fail to get topic internal state")?)
    // }

    // pub async fn unsubscribe(&self, topic: &Topic, sub_name: &str) -> Result<()> {
    //     // DELETE /admin/v2/namespaces/:tenant/:namespace/:topic/subscription/:subscription
    //     let path = format!(
    //         "{}/admin/v2/{}/{}/{}/{}/subscription/{}",
    //         self.url, topic.schema, topic.tenant, topic.namespace, topic.topic, sub_name
    //     );
    //     let builder = self.client.delete(path);

    //     let res = builder
    //     .send()
    //     .await?;

    //     let rsp_status = res.status();
    //     if !rsp_status.is_success() {
    //         bail!("error http status {}", rsp_status);
    //     }
    //     Ok(())
    // }
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct EntryIndex {
    pub ledger_id: i64,
    pub entry_id: i64,
    pub partition_index: i32,
}

impl EntryIndex {
    pub fn equal_to(&self, d: &MessageIdData) -> bool {
        self.ledger_id as u64 == d.ledger_id
            && self.entry_id as u64 == d.entry_id
            && ((self.partition_index < 0
                && (d.partition.is_none() || *d.partition.as_ref().unwrap() < 0))
                || (self.partition_index >= 0
                    && d.partition.is_some()
                    && self.partition_index == *d.partition.as_ref().unwrap()))
    }
}

impl std::str::FromStr for EntryIndex {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let v: Vec<&str> = s.split(':').collect();
        if v.len() == 2 {
            Ok(Self {
                ledger_id: v[0].parse::<i64>()?,
                entry_id: v[1].parse::<i64>()?,
                partition_index: -1,
            })
        } else if v.len() == 3 {
            Ok(Self {
                partition_index: v[0].parse::<i32>()?,
                ledger_id: v[1].parse::<i64>()?,
                entry_id: v[2].parse::<i64>()?,
            })
        } else {
            bail!("entry index consist of {ledger_id}:{entry_id}");
        }
    }
}

struct DeserializeMessageIndexVisitor;

impl<'de> serde::de::Visitor<'de> for DeserializeMessageIndexVisitor {
    type Value = EntryIndex;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("{ledger_id}:{entry_id}")
    }

    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        let r: Result<EntryIndex, _> = v.parse();
        match r {
            Ok(m) => Ok(m),
            Err(_e) => Err(E::invalid_value(serde::de::Unexpected::Str(v), &self)),
        }
    }
}

fn deserialize_message_id<'de, D>(deserializer: D) -> Result<EntryIndex, D::Error>
where
    D: serde::Deserializer<'de>,
{
    deserializer.deserialize_any(DeserializeMessageIndexVisitor)
}

#[derive(Debug)]
pub struct TopicParts {
    pub schema: String,
    pub tenant: String,
    pub namespace: String,
    pub topic: String,
}

impl FromStr for TopicParts {
    type Err = anyhow::Error;

    fn from_str(topic_str: &str) -> Result<Self, Self::Err> {
        let url = reqwest::Url::parse(topic_str).context("parse topic url fail")?;
        if url.cannot_be_a_base() || !url.has_host() {
            bail!("invalid topic [{}]", topic_str);
        }

        let tenant = url.host_str().context(here!())?;
        let mut paths = url.path().split('/');
        paths.next().context("invalid topic: path-1")?;
        let namespace = paths.next().context("invalid topic: path-2")?;
        let topic = paths.collect::<String>();

        Ok(Self {
            schema: url.scheme().into(),
            tenant: tenant.into(),
            namespace: namespace.into(),
            topic,
        })
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TopicLocation {
    //{"brokerUrl":"pulsar://localhost:6650","httpUrl":"http://localhost:18080","nativeUrl":"pulsar://localhost:6650","brokerUrlSsl":""}
    pub broker_url: String,
    pub http_url: String,
    pub native_url: String,
    pub broker_url_ssl: String,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Ledger {
    ledger_id: i64,
    entries: i64,
    size: i64,
    offloaded: bool,
    under_replicated: bool,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TopicInternalState {
    pub entries_added_counter: u64,
    pub number_of_entries: u64,
    pub total_size: u64,
    pub current_ledger_entries: u64,
    pub current_ledger_size: u64,
    pub waiting_cursors_count: u64,
    pub pending_add_entries_count: u64,

    #[serde(with = "my_date_format")]
    pub last_ledger_created_timestamp: DateTime<Local>, // "2021-09-18T10:43:34.14+08:00"

    #[serde(deserialize_with = "deserialize_message_id")]
    pub last_confirmed_entry: EntryIndex, // "4549:27" "4549:-1"

    pub ledgers: Vec<Ledger>,
}

mod my_date_format {
    use chrono::{DateTime, Local, TimeZone};
    use serde::{self, Deserialize, Deserializer, Serializer};

    // https://docs.rs/chrono/0.4.19/chrono/format/strftime/index.html
    // 2021-09-18T10:43:34.14+08:00
    pub const FORMAT: &'static str = "%Y-%m-%dT%H:%M:%S%.f%:z";

    pub fn serialize<S>(date: &DateTime<Local>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let s = format!("{}", date.format(FORMAT));
        serializer.serialize_str(&s)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<DateTime<Local>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Local
            .datetime_from_str(&s, FORMAT)
            .map_err(serde::de::Error::custom)
    }
}

#[derive(Default)]
struct TopicOptionalArgs {
    topic: Option<String>,
    consumer_name: Option<String>,
    sub_name: Option<String>,
    begin_entry: Option<EntryIndex>,
    begin_timestamp: Option<u64>,
}

pub struct TopicConsumerBuilder<'a, Exe: pulsar::Executor> {
    pulsar: &'a Pulsar<Exe>,
    admin: &'a Admin,
    args: TopicOptionalArgs,
}

impl<'a, Exe: pulsar::Executor> TopicConsumerBuilder<'a, Exe> {
    pub fn new(pulsar: &'a Pulsar<Exe>, admin: &'a Admin) -> Self {
        Self {
            pulsar,
            admin,
            args: Default::default(),
        }
    }

    pub fn with_consumer_name<S: Into<String>>(mut self, s: S) -> Self {
        self.args.consumer_name = Some(s.into());
        self
    }

    pub fn with_sub_name<S: Into<String>>(mut self, s: S) -> Self {
        self.args.sub_name = Some(s.into());
        self
    }

    pub fn with_topic<S: Into<String>>(mut self, s: S) -> Self {
        self.args.topic = Some(s.into());
        self
    }

    pub fn with_begin_entry(mut self, s: EntryIndex) -> Self {
        self.args.begin_entry = Some(s);
        self
    }

    pub fn with_begin_timestamp(mut self, s: u64) -> Self {
        self.args.begin_timestamp = Some(s);
        self
    }

    pub async fn build<T: DeserializeMessage>(self) -> Result<pulsar::Consumer<T, Exe>> {
        if self.args.topic.is_none() {
            bail!("expect topic");
        }

        if self.args.consumer_name.is_none() {
            bail!("expect consumer_name");
        }

        if self.args.sub_name.is_none() {
            bail!("expect consumer_name");
        }

        let topic: TopicParts = self.args.topic.as_ref().unwrap().parse()?;
        let sub_name = self.args.sub_name.unwrap();

        let mut builder = self
            .pulsar
            .consumer()
            .with_topic(&self.args.topic.unwrap())
            .with_consumer_name(&self.args.consumer_name.unwrap())
            .with_subscription(&sub_name)
            .with_subscription_type(pulsar::SubType::Exclusive);

        let mut options = ConsumerOptions::default();
        if let Some(index) = &self.args.begin_entry {
            options = options.starting_on_message(MessageIdData {
                ledger_id: index.ledger_id as u64,
                entry_id: index.entry_id as u64,
                partition: None,
                batch_index: None,
                ack_set: Vec::new(), // TODO: aaa
                batch_size: None,
            });
        } else if let Some(time_milli) = self.args.begin_timestamp {
            // create durable subscription
            {
                let builder0 = builder.clone();
                let _consumer: pulsar::Consumer<T, Exe> = builder0.build().await?;
            }

            //debug!("reseting cursor to timestamp {} ...", time_milli);
            self.admin
                .reset_cursor(&topic, &sub_name, time_milli)
                .await?;
            // debug!("reseting cursor to timestamp {} done", time_milli);
        }

        options = options.durable(true);
        builder = builder.with_options(options);
        let consumer = builder.build().await?;

        Ok(consumer)
    }
}
