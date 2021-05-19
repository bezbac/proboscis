use std::collections::HashMap;

use crate::{
    proxy::{data::SimpleQueryResponse, resolver::ResolverResult},
    Resolver,
};
use async_trait::async_trait;

pub struct StupidCache {
    store: HashMap<String, SimpleQueryResponse>,
}

impl StupidCache {
    pub fn new() -> StupidCache {
        StupidCache {
            store: HashMap::new(),
        }
    }
}

#[async_trait]
impl Resolver for StupidCache {
    async fn lookup(&self, query: &String) -> ResolverResult<SimpleQueryResponse> {
        println!("Cache Lookup: {}", query);
        match self.store.get(query) {
            Some(data) => {
                println!("Cache Hit: {}", query);
                ResolverResult::Hit(data.clone())
            }
            None => ResolverResult::Miss,
        }
    }

    async fn inform(&mut self, query: &String, data: SimpleQueryResponse) {
        println!("Cache Inform: {}", query);
        self.store.insert(query.clone(), data);
    }
}
