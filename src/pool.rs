use deadpool::managed::{Manager, Metrics, Object, Pool, PoolConfig};
use tonic::transport::Channel;

use crate::api::dgraph_client::DgraphClient;
use crate::api::Check;
use crate::client::{Client, EndpointAddresses};
use crate::error::Result;
use crate::{DgraphError, Transaction};

#[derive(Debug)]
pub struct DgraphConnectionManager {
    endpoints: EndpointAddresses,
}

impl DgraphConnectionManager {
    pub fn new(endpoints: EndpointAddresses) -> Self {
        Self { endpoints }
    }
}

impl Manager for DgraphConnectionManager {
    type Type = DgraphClient<Channel>;
    type Error = tonic::Status;

    async fn create(&self) -> std::result::Result<Self::Type, Self::Error> {
        let channel = Channel::balance_list(self.endpoints.to_endpoints().into_iter());
        Ok(DgraphClient::new(channel))
    }

    async fn recycle(
        &self,
        client: &mut Self::Type,
        _metrics: &Metrics,
    ) -> deadpool::managed::RecycleResult<Self::Error> {
        // DgraphClient::new(client).check_version(Check {}).await?;
        client.check_version(Check {}).await?;
        Ok(())
    }
}

#[derive(Clone)]
pub struct DgraphPool {
    pool: Pool<DgraphConnectionManager>,
}

impl DgraphPool {
    pub async fn new(endpoints: EndpointAddresses, pool_size: usize) -> Result<Self> {
        let manager = DgraphConnectionManager::new(endpoints);

        let pool = Pool::builder(manager)
            .config(PoolConfig::new(pool_size))
            .build()
            .map_err(DgraphError::from)?;

        Ok(Self { pool })
    }

    pub fn pool(&self) -> &Pool<DgraphConnectionManager> {
        &self.pool
    }

    pub async fn get_readonly(&self) -> Result<Client> {
        let x = self.pool.get().await?;
        Ok(Client::new(x, true, false))
    }

    pub async fn get_best_effort(&self) -> Result<Client> {
        let x = self.pool.get().await?;
        Ok(Client::new(x, true, true))
    }

    pub async fn get(&self) -> Result<Client> {
        let x = self.pool.get().await?;
        Ok(Client::new(x, false, false))
    }

    pub async fn new_txn(&self) -> Result<Transaction> {
        let conn = self.pool.get().await?;
        Ok(Transaction::new(conn))
    }
}

pub type DgraphConn = Object<DgraphConnectionManager>;
