
use std::{collections::BTreeSet, str::FromStr};
use anyhow::Context;
use crate::uid::NodeId;


/// TODO: NodeIdPool 优化空间占用， 优化 std::fmt::Debug 打印 range 列表
#[derive(Default)]
pub struct NodeIdPool {
    pool: BTreeSet<NodeId>
}

impl std::fmt::Debug for NodeIdPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NodeIdPool").field("len", &self.pool.len()).finish()
    }
}

impl NodeIdPool {
    pub fn merge(&mut self, other: &Self) {
        for id in other.pool.iter() {
            self.pool.insert(id.clone());
        }
    }

    pub fn add_range(&mut self, start: NodeId, end: NodeId) {
        let end = start.max(end);
        for id in *start..=*end {
            self.pool.insert(id.into());
        }
    }

    pub fn remove(&mut self, id: &NodeId) {
        self.pool.remove(id);
    }

    pub fn pop(&mut self) -> Option<NodeId> {
        self.pool.pop_first()
    }
}


impl FromStr for NodeIdPool {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> { 
        let mut self0 = Self::default();
        for range_str in s.split(',') {
            let range_str = range_str.trim();
            if range_str.is_empty() {
                continue;
            }

            let mut parts = range_str.split('-');
            let start = parts.next().with_context(||format!("invalid range [{}]", range_str))?;
            let start: u64 = start.parse().with_context(||format!("expect number but [{}]", start))?;
            let end = match parts.next() {
                Some(v) => v.parse().with_context(||format!("expect number but [{}]", v))?,
                None => start,
            };
            self0.add_range(start.into(), end.into());
        }
        Ok(self0)
    }
}

