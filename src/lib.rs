mod api;
pub mod client;
pub mod error;
pub mod pool;
pub mod transaction;

use std::fmt::Display;

pub use api::{Latency, Metrics, Mutation, Operation, Payload, Response, operation::DropOp};
pub use client::{Client, EndpointAddresses};
pub use error::{DgraphError, Result};
pub use pool::{DgraphConn, DgraphPool};
pub use transaction::Transaction;

impl Mutation {
    pub fn new() -> Mutation {
        Mutation::default()
    }

    pub fn set_set_nquads(&mut self, set: impl Into<String>) {
        self.set_nquads = set.into().as_bytes().to_vec();
    }

    pub fn set_delete_nquads(&mut self, del: impl Into<String>) {
        self.del_nquads = del.into().as_bytes().to_vec();
    }

    pub fn set_cond(&mut self, cond: impl Into<String>) {
        self.cond = cond.into();
    }
}

impl From<Mutation> for Vec<Mutation> {
    fn from(value: Mutation) -> Vec<Mutation> {
        vec![value]
    }
}

impl Display for Mutation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "Mutation {{")?;

        // 使用 String::from_utf8_lossy 安全地转换 Vec<u8> 为可打印字符串
        writeln!(f, "  set_json: {}", String::from_utf8_lossy(&self.set_json))?;
        writeln!(
            f,
            "  delete_json: {}",
            String::from_utf8_lossy(&self.delete_json)
        )?;
        writeln!(
            f,
            "  set_nquads: {}",
            String::from_utf8_lossy(&self.set_nquads)
        )?;
        writeln!(
            f,
            "  del_nquads: {}",
            String::from_utf8_lossy(&self.del_nquads)
        )?;

        // 对于 NQuad 类型，假设它也实现了 Display
        writeln!(f, "  set: {:?}", self.set)?;
        writeln!(f, "  del: {:?}", self.del)?;

        writeln!(f, "  cond: {}", self.cond)?;
        writeln!(f, "  commit_now: {}", self.commit_now)?;

        writeln!(f, "}}")
    }
}
