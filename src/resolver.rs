use anyhow::Result;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use futures::future::join_all;

#[async_trait]
pub trait Resolver: Sync + Send {
    async fn query(&mut self, query: &String) -> Result<RecordBatch>;
    async fn inform(&mut self, query: &String, data: RecordBatch);
}

#[async_trait]
pub trait SimpleResolver: Sync + Send {
    async fn lookup(&mut self, query: &String) -> Result<RecordBatch>;
    async fn inform(&mut self, query: &String, data: RecordBatch);
}

#[async_trait]
impl<T: SimpleResolver> Resolver for T {
    async fn query(&mut self, query: &String) -> Result<RecordBatch> {
        self.lookup(query).await
    }

    async fn inform(&mut self, query: &String, data: RecordBatch) {
        self.inform(query, data).await
    }
}

pub struct ResolverChain {
    pub resolvers: Vec<Box<dyn Resolver>>,
}

#[async_trait]
impl Resolver for ResolverChain {
    async fn query(&mut self, query: &String) -> Result<RecordBatch> {
        for resolver in self.resolvers.iter_mut() {
            let resolver_response = resolver.query(query).await;

            match resolver_response {
                Ok(data) => return Ok(data),
                Err(_) => continue,
            }
        }

        Err(anyhow::anyhow!("No Resolver returned any data"))
    }

    async fn inform(&mut self, query: &String, data: RecordBatch) {
        join_all(
            self.resolvers
                .iter_mut()
                .map(|r| r.inform(&query, data.clone())),
        )
        .await;
    }
}
