#![cfg_attr(feature = "pedantic", warn(clippy::pedantic))]
#![warn(clippy::use_self)]
#![warn(clippy::map_flatten)]
#![warn(clippy::map_unwrap_or)]
#![warn(deprecated_in_future)]
#![warn(future_incompatible)]
#![warn(noop_method_call)]
#![warn(unreachable_pub)]
#![warn(missing_debug_implementations)]
#![warn(rust_2018_compatibility)]
#![warn(rust_2021_compatibility)]
#![warn(rust_2018_idioms)]
#![warn(trivial_casts)]
#![warn(unused)]
#![deny(warnings)]

use anyhow::Context;
use aws_sdk_dynamodb as dynamodb;
use serde_dynamo as dynamo;
use tokio_stream::StreamExt;
use uuid::Uuid;

pub use table::Table;

mod table;

#[derive(Debug)]
pub struct Client {
    /// DynamoDB connector client
    client: dynamodb::Client,
}

impl Client {
    pub async fn new() -> Self {
        let config = aws_config::load_from_env().await;
        let client = dynamodb::Client::new(&config);
        Self { client }
    }

    pub async fn table(&self, table_name: impl ToString) -> Table {
        let client = self.client.clone();
        let table = table_name.to_string();
        Table::new(client, table).await
    }
}
