use std::{collections::HashMap, fmt::Debug};
use tonic::transport::Endpoint;

use crate::{
    api::{Mutation, Request as DgraphRequest, Response},
    error::Result,
    DgraphConn,
};

#[derive(Debug)]
pub enum EndpointAddresses {
    Static(&'static Vec<String>),
    Owned(Vec<String>),
    StaticStr(Vec<&'static str>),
}

impl EndpointAddresses {
    pub(crate) fn to_endpoints(&self) -> Vec<Endpoint> {
        match self {
            EndpointAddresses::Static(v) => {
                v.iter().map(|url| Endpoint::from_static(url)).collect()
            }
            EndpointAddresses::Owned(v) => v
                .iter()
                .map(|url| {
                    Endpoint::from_shared(url.clone()).unwrap_or_else(|e| {
                        eprintln!("error dgraph url: {e:}");
                        std::process::exit(1);
                    })
                })
                .collect(),
            EndpointAddresses::StaticStr(v) => {
                v.iter().map(|url| Endpoint::from_static(url)).collect()
            }
        }
    }
}

// pub struct Client {
//     // 为了保持内部可变性，只能引入 Mutex
//     pub(crate) inner: DgraphClient<Channel>,
// }

pub struct Client {
    best_effort: bool,
    read_only: bool,
    pub(crate) inner: DgraphConn,
}

impl Client {
    pub(crate) fn new(inner: DgraphConn, read_only: bool, best_effort: bool) -> Self {
        Self {
            best_effort,
            read_only,
            inner,
        }
    }

    pub async fn query(&mut self, query: impl Into<String>) -> Result<Response> {
        let req = DgraphRequest {
            query: query.into(),
            read_only: self.read_only,
            best_effort: self.best_effort,
            commit_now: true,
            ..Default::default()
        };
        self.do_request(req).await
    }

    pub async fn query_with_vars(
        &mut self,
        query: impl Into<String>,
        var: HashMap<String, String>,
    ) -> Result<Response> {
        let req = DgraphRequest {
            query: query.into(),
            vars: var,
            read_only: self.read_only,
            best_effort: self.best_effort,
            commit_now: true,
            ..Default::default()
        };
        self.do_request(req).await
    }

    async fn do_request(&mut self, req: DgraphRequest) -> Result<Response> {
        let response = self.inner.query(req).await?;
        Ok(response.into_inner())
    }

    pub async fn mutate(&mut self, mus: impl Into<Vec<Mutation>>) -> Result<Response> {
        let req = DgraphRequest {
            mutations: mus.into(),
            commit_now: true,
            ..Default::default()
        };
        self.do_request(req).await
    }

    pub async fn upsert(
        &mut self,
        query: impl Into<String>,
        mus: impl Into<Vec<Mutation>>,
    ) -> Result<Response> {
        let req = DgraphRequest {
            query: query.into(),
            mutations: mus.into(),
            commit_now: true,
            ..Default::default()
        };
        self.do_request(req).await
    }

    pub async fn upsert_with_vars(
        &mut self,
        query: impl Into<String>,
        var: HashMap<String, String>,
        mus: impl Into<Vec<Mutation>>,
    ) -> Result<Response> {
        let req = DgraphRequest {
            query: query.into(),
            vars: var,
            mutations: mus.into(),
            commit_now: true,
            ..Default::default()
        };
        self.do_request(req).await
    }
}

impl Debug for Client {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Client")
            .field("inner", &self.inner)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use crate::pool::DgraphPool;

    use super::*;

    const DGRAPH_SERVER: &str = "http://127.0.0.1:9080";

    #[tokio::test]
    async fn test_upsert() {
        let server = EndpointAddresses::StaticStr(vec![DGRAPH_SERVER]);
        let mut c = DgraphPool::new(server, 1)
            .await
            .unwrap()
            .get()
            .await
            .unwrap();
        let q = r#"query {system as var(func: eq(system_id, "test"))
  envs as var(func: eq(env_id, ["uat","dev","prod"]))
  system(func: uid(system)) { uid }
  env(func: uid(envs)) { count(uid) }
  }"#;
        let mut mu = Mutation::new();
        let body = r#"_:system <system_id> "test" .
_:system <system_name> "test" .
_:system <create_time> "2023-04-01T06:34:52.643148+08:00" .
_:system <update_time> "2023-04-01T06:34:52.643148+08:00" .
_:system <dgraph.type> "System" .
uid(envs) <env_systems> _:system ."#;
        mu.set_set_nquads(body.to_string());
        mu.set_cond("@if(eq(len(system), 0) AND eq(len(envs), 3))".to_string());
        let resp = c.upsert(q, vec![mu]).await.unwrap();
        println!("{}", String::from_utf8(resp.json).unwrap())
    }

    #[tokio::test]
    async fn test_mutation() {
        let server = EndpointAddresses::StaticStr(vec![DGRAPH_SERVER]);
        let pool = DgraphPool::new(server, 1).await.unwrap();
        let mut c = pool.get().await.unwrap();
        let q = r#"{
q(func: eq(env_id, "prod")) {
	uid
  expand(_all_)
}
}"#;
        let mut mu1 = Mutation::new();
        let body = r#"_:system <system_id> "test1" .
_:system <system_name> "test1" .
_:system <create_time> "2023-04-01T06:34:52.643148+08:00" .
_:system <update_time> "2023-04-01T06:34:52.643148+08:00" .
_:system <dgraph.type> "System" .
"#;
        mu1.set_set_nquads(body.to_string());
        let mut mu2 = Mutation::new();
        let body = r#"
<0xef9093> * * .
<0x4fb5> <env_systems> <0xef9093> .
"#;
        mu2.set_delete_nquads(body.to_string());
        let resp = c.upsert(q, vec![mu1, mu2]).await.unwrap();
        println!("{}", String::from_utf8(resp.json).unwrap())
    }

    #[tokio::test]
    async fn test_txn() {
        let server = EndpointAddresses::StaticStr(vec![DGRAPH_SERVER]);
        let c = DgraphPool::new(server, 1).await.unwrap();
        let mut conn = c.get_best_effort().await.unwrap();
        let mut txn = c.new_txn().await.unwrap();
        let mut mu1 = Mutation::new();
        let body = r#"_:system <system_id> "test1" .
_:system <system_name> "test1" .
_:system <create_time> "2023-04-01T06:34:52.643148+08:00" .
_:system <update_time> "2023-04-01T06:34:52.643148+08:00" .
_:system <dgraph.type> "System" .
"#;
        mu1.set_set_nquads(body.to_string());
        let resp = txn.mutate(vec![mu1]).await.unwrap();
        println!("{:?}", resp);
        let q = r#"{
q(func: eq(system_id, "test1")) {
	uid
  expand(_all_)
}
}"#;
        let resp = conn.query(q).await.unwrap();
        println!("{}", String::from_utf8(resp.json).unwrap());
        txn.commit().await.unwrap();
    }
}
