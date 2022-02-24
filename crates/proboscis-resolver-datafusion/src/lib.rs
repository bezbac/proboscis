use std::{
    collections::{HashMap, VecDeque},
    sync::{Arc, Mutex},
};

use anyhow::Result;
use async_trait::async_trait;
use datafusion::{
    arrow::{datatypes::Schema, record_batch::RecordBatch},
    prelude::{ExecutionContext, DataFrame},
};
use proboscis_core::resolver::{
    Bind, ClientId, Close, Describe, Execute, Parse, Resolver, SyncResponse,
};

#[derive(Debug)]
enum ClientOperation {
    Parse,
    Bind { statement: String, portal: String },
    Describe { statement: String },
    Execute { portal: String },
}

pub struct DatafusionResolver {
    ctx: Arc<Mutex<ExecutionContext>>,

    connections: HashMap<ClientId, VecDeque<ClientOperation>>,

    // Maps a statement to a schema
    statement_schema_cache: HashMap<String, Schema>,

    // Maps a statement to an sql string
    statement_query_cache: HashMap<String, String>,

    // Maps a portal to a statement
    portal_cache: HashMap<String, String>,
}

impl DatafusionResolver {
    pub async fn new(ctx: ExecutionContext) -> DatafusionResolver {
        DatafusionResolver {
            ctx: Arc::new(Mutex::new(ctx)),
            statement_schema_cache: HashMap::new(),
            statement_query_cache: HashMap::new(),
            portal_cache: HashMap::new(),
            connections: HashMap::new(),
        }
    }
}

async fn get_results_for_query(df: &Arc<dyn DataFrame>) -> Result<RecordBatch> {
        let batches: Vec<RecordBatch> = df.collect().await?;
        let schema = batches.first().unwrap().schema();

        let result = RecordBatch::concat(&schema, &batches)?;

        Ok(result)
    }

#[async_trait]
impl Resolver for DatafusionResolver {
    async fn query(&mut self, _client_id: ClientId, query: String) -> Result<RecordBatch> {
        let df = self.ctx.lock().unwrap().sql(&query)?;
        get_results_for_query(&df).await
    }

    async fn parse(&mut self, client_id: ClientId, parse: Parse) -> Result<()> {
        let requested_ops = self.connections.entry(client_id).or_insert(VecDeque::new());

        let statement_name = parse.statement_name.clone();
        let query = parse.query.clone();

        requested_ops.push_back(ClientOperation::Parse);
        self.statement_query_cache.insert(statement_name, query);

        Ok(())
    }

    async fn describe(&mut self, client_id: ClientId, describe: Describe) -> Result<()> {
        let requested_ops = self.connections.entry(client_id).or_insert(VecDeque::new());

        requested_ops.push_back(ClientOperation::Describe {
            statement: describe.name.clone(),
        });

        Ok(())
    }

    async fn bind(&mut self, client_id: ClientId, bind: Bind) -> Result<()> {
        let requested_ops = self.connections.entry(client_id).or_insert(VecDeque::new());

        let statement = bind.statement.clone();
        let portal = bind.portal.clone();

        requested_ops.push_back(ClientOperation::Bind { statement, portal });

        Ok(())
    }

    async fn execute(&mut self, client_id: ClientId, execute: Execute) -> Result<()> {
        let requested_ops = self.connections.entry(client_id).or_insert(VecDeque::new());

        let portal = execute.portal.clone();

        requested_ops.push_back(ClientOperation::Execute { portal });

        Ok(())
    }

    async fn sync(&mut self, client_id: ClientId) -> Result<Vec<SyncResponse>> {
        let requested_ops = self.connections.entry(client_id).or_insert(VecDeque::new());

        let mut responses = vec![];
        while let Some(operation) = &requested_ops.pop_front() {
            match operation {
                ClientOperation::Parse => responses.push(SyncResponse::ParseComplete),
                ClientOperation::Describe { statement } => loop {
                    // TODO Parameter Description

                    let query = self.statement_query_cache.get(statement).unwrap();

                    let df = self.ctx.lock().unwrap().sql(&query)?;
                    let record_batch = get_results_for_query(&df).await?;

                    if record_batch.num_rows() == 0 {
                        responses.push(SyncResponse::NoData);
                    } else {
                        let schema = Arc::<datafusion::arrow::datatypes::Schema>::try_unwrap(
                            record_batch.schema(),
                        )
                        .unwrap();

                        responses.push(SyncResponse::Schema {
                            schema: schema.clone(),
                            query: query.clone(),
                        });

                        self.statement_schema_cache
                            .insert(statement.clone(), schema.clone());
                    }
                },
                ClientOperation::Bind { statement, portal } => {
                    self.portal_cache.insert(portal.clone(), statement.clone());
                    responses.push(SyncResponse::BindComplete)
                }
                ClientOperation::Execute { portal } => {
                    let statement = self.portal_cache.get(portal).unwrap();

                    let query = self.statement_query_cache.get(statement).unwrap();

                    let df = self.ctx.lock().unwrap().sql(&query)?;
                    let record_batch = get_results_for_query(&df).await?;

                    let row_count = record_batch.num_rows();

                    responses.push(SyncResponse::Records {
                        data: record_batch,
                        query: query.clone(),
                    });

                    responses.push(SyncResponse::CommandComplete(format!(
                        "SELECT {}",
                        row_count
                    )))
                }
            }
        }

        responses.push(SyncResponse::ReadyForQuery);

        Ok(responses)
    }

    async fn close(&mut self, _client_id: ClientId, _close: Close) -> Result<()> {
        Ok(())
    }

    async fn initialize(&mut self, _client_id: ClientId) -> Result<()> {
        Ok(())
    }

    async fn terminate(&mut self, _client_id: ClientId) -> Result<()> {
        Ok(())
    }
}
