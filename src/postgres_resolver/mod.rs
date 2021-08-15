use anyhow::Result;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;

use crate::{
    connection::ProtocolStream, data::simple_query_response_to_record_batch,
    postgres_protocol::Message, Resolver,
};

use self::connection_pool::{ConnectionManager, ConnectionPool};

mod connection_pool;

#[derive(Clone)]
pub struct TargetConfig {
    pub host: String,
    pub port: String,
    pub database: String,
    pub user: String,
    pub password: String,
}

pub struct PostgresResolver {
    pool: ConnectionPool,
}

impl PostgresResolver {
    pub fn initialize(
        target_config: TargetConfig,
        pool_config: deadpool::managed::PoolConfig,
    ) -> PostgresResolver {
        let manager = ConnectionManager::new(target_config.clone());
        let pool = ConnectionPool::from_config(manager, pool_config);
        PostgresResolver { pool }
    }
}

#[async_trait]
impl Resolver for PostgresResolver {
    async fn query(&self, query: &String) -> Result<RecordBatch> {
        let mut backend = self.pool.get().await.map_err(|err| anyhow::anyhow!(err))?;

        backend
            .write_message(Message::SimpleQuery(query.clone()))
            .await?;

        let mut fields = vec![];
        let mut data_rows = vec![];
        loop {
            let response = backend.read_message().await?;
            match response {
                Message::ReadyForQuery => break,
                Message::RowDescription {
                    fields: mut message_fields,
                } => fields.append(&mut message_fields),
                Message::DataRow { field_data: _ } => {
                    data_rows.push(response);
                }
                Message::CommandComplete { tag: _ } => {}
                _ => unimplemented!(""),
            }
        }

        let data = simple_query_response_to_record_batch(&fields, &data_rows).await?;

        Ok(data)
    }

    // async fn parse(
    //     &self,
    //     statement_name: &String,
    //     query: &String,
    //     param_types: &Vec<u32>,
    // ) -> Result<()> {
    //     let mut backend = self.pool.get().await.map_err(|err| anyhow::anyhow!(err))?;

    //     backend
    //         .write_message(Message::Parse {
    //             statement_name: statement_name.clone(),
    //             query: query.clone(),
    //             param_types: param_types.clone(),
    //         })
    //         .await?;

    //     backend.write_message(Message::Sync).await?;

    //     let response = backend.read_message().await?;

    //     match response {
    //         Message::ParseComplete => Ok(()),
    //         _ => unimplemented!(),
    //     }
    // }

    async fn inform(&mut self, _query: &String, _data: RecordBatch) {}
}
