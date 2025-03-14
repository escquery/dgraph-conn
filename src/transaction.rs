use std::collections::{HashMap, HashSet};

use crate::error::Result;
use crate::DgraphConn;
use crate::{
    api::{Mutation, Request as DgraphRequest, Response, TxnContext},
    DgraphError,
};

// 事务不允许出现 readonly 和 best_effort
#[derive(Default)]
pub struct Transaction {
    keys: HashSet<String>,
    preds: HashSet<String>,
    // read_only: bool,
    // best_effort: bool,
    ctx: TxnContext,
    finished: bool,
    mutated: bool,
    // 这个字段是用来配合 Drop 取消事务的
    dropped: bool,
    // 使用 Option 同样是为了方便复制，配合 Drop 取消事务
    conn: Option<DgraphConn>,
}

impl Transaction {
    pub fn new(conn: DgraphConn) -> Self {
        Self {
            conn: Some(conn),
            keys: HashSet::new(),
            preds: HashSet::new(),
            ctx: TxnContext::default(),
            finished: false,
            mutated: false,
            dropped: false,
        }
    }

    pub async fn discard(&mut self) -> Result<()> {
        self.ctx.aborted = true;
        self.commit_or_abort().await
    }

    pub async fn do_request(&mut self, mut req: DgraphRequest) -> Result<Response> {
        if self.finished {
            return Err(DgraphError::InvalidArgument(
                "Transaction already finished".to_string(),
            ));
        }
        if !req.mutations.is_empty() {
            self.mutated = true;
        }
        req.start_ts = self.ctx.start_ts;
        req.hash = std::mem::take(&mut self.ctx.hash);
        let commit_now = req.commit_now;
        // 事务执行失败也不要紧，drop 方法会取消事务
        let response = self.conn.as_mut().unwrap().query(req).await?;

        if commit_now {
            self.finished = true;
        }
        let mut resp = response.into_inner();
        if let Some(ctx) = resp.txn.take() {
            self.merge_context(ctx)?;
        }
        Ok(resp)
    }

    fn merge_context(&mut self, ctx: TxnContext) -> Result<()> {
        self.ctx.hash = ctx.hash;
        if self.ctx.start_ts == 0 {
            self.ctx.start_ts = ctx.start_ts;
        }
        if self.ctx.start_ts != ctx.start_ts {
            return Err(DgraphError::InvalidArgument(
                "Transaction start_ts mismatch".to_string(),
            ));
        }
        for key in ctx.keys {
            self.keys.insert(key);
        }
        for pred in ctx.preds {
            self.preds.insert(pred);
        }
        Ok(())
    }

    pub async fn query(&mut self, query: impl Into<String>) -> Result<Response> {
        self.do_query(query, HashMap::new()).await
    }

    pub async fn query_with_vars(
        &mut self,
        query: impl Into<String>,
        var: HashMap<String, String>,
    ) -> Result<Response> {
        self.do_query(query, var).await
    }

    async fn do_query(
        &mut self,
        query: impl Into<String>,
        var: HashMap<String, String>,
    ) -> Result<Response> {
        let req = DgraphRequest {
            query: query.into(),
            vars: var,
            // read_only: self.read_only,
            // best_effort: self.best_effort,
            ..Default::default()
        };

        self.do_request(req).await
    }

    pub async fn mutate(&mut self, mus: impl Into<Vec<Mutation>>) -> Result<Response> {
        let req = DgraphRequest {
            mutations: mus.into(),
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
            ..Default::default()
        };
        self.do_request(req).await
    }

    pub async fn commit(mut self) -> Result<()> {
        // if self.read_only {
        //     return Err(DgraphError::InvalidArgument(
        //         "Read-only transactions cannot be committed".to_string(),
        //     ));
        // }
        if self.finished {
            return Err(DgraphError::InvalidArgument(
                "Transaction already finished".to_string(),
            ));
        }
        self.commit_or_abort().await?;
        Ok(())
    }

    async fn commit_or_abort(&mut self) -> Result<()> {
        if self.finished {
            return Ok(());
        }
        self.finished = true;
        if !self.mutated {
            return Ok(());
        }
        for i in std::mem::take(&mut self.keys) {
            self.ctx.keys.push(i);
        }
        for i in std::mem::take(&mut self.preds) {
            self.ctx.preds.push(i);
        }
        self.conn
            .as_mut()
            .unwrap()
            .commit_or_abort(std::mem::take(&mut self.ctx))
            .await?;
        Ok(())
    }
}

impl Drop for Transaction {
    fn drop(&mut self) {
        if !self.dropped && !self.finished && self.mutated {
            let mut this = Self::default();
            std::mem::swap(&mut this, self);
            this.dropped = true;
            tokio::spawn(async move {
                let _ = this.discard().await;
            });
        }
    }
}
