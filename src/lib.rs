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

// use anyhow::Context;
use aws_sdk_dynamodb as dynamodb;
use serde_dynamo as dynamo;
// use tokio_stream::StreamExt;
use uuid::Uuid;

#[derive(Debug)]
pub struct Client {
    /// DynamoDB connector client
    client: dynamodb::Client,
}

#[derive(Debug)]
pub struct Table {
    client: dynamodb::Client,
    table: String,
}

impl Client {
    pub fn table(&self, table: impl ToString) -> Table {
        let client = self.client.clone();
        let table = table.to_string();
        Table { client, table }
    }
}

impl Table {
    async fn new(table: &str) -> Self {
        let config = aws_config::load_from_env().await;
        let client = dynamodb::Client::new(&config);
        let table = table.to_string();
        Self { client, table }
    }

    pub async fn get_item(
        &self,
        org_id: &str,
        cluster_id: Uuid,
    ) -> anyhow::Result<Option<Cluster>> {
        let org_id = dynamo::to_attribute_value(org_id)?;
        let cluster_id = dynamo::to_attribute_value(cluster_id)?;

        self.client
            .get_item()
            .table_name(&self.table)
            .consistent_read(true)
            .key("org_id", org_id)
            .key("cluster_id", cluster_id)
            .send()
            .await?
            .item
            .map(dynamo::from_item)
            .transpose()
            .context("Failed to decode dynamo data")
    }

    pub async fn put_item(&self, cluster: &Cluster) -> anyhow::Result<()> {
        let item = dynamo::to_item(cluster)?;
        tracing::info!(?item);
        let output = self
            .client
            .put_item()
            .table_name(&self.table)
            .set_item(Some(item))
            .send()
            .await?;

        tracing::info!(?output);

        Ok(())
    }

    pub async fn delete_item(&self, org_id: &str, cluster_id: Uuid) -> anyhow::Result<()> {
        tracing::info!("Deleting item");
        let org_id = dynamo::to_attribute_value(org_id)?;
        let cluster_id = dynamo::to_attribute_value(cluster_id)?;

        let output = self
            .client
            .delete_item()
            .table_name(&self.table)
            .key("org_id", org_id)
            .key("cluster_id", cluster_id)
            .send()
            .await?;

        tracing::info!(?output);

        Ok(())
    }

    pub async fn get_items(&self, org_id: &str) -> anyhow::Result<Vec<Cluster>> {
        let org_id = dynamo::to_attribute_value(org_id)?;

        self.client
            .query()
            .table_name(&self.table)
            .consistent_read(true)
            .key_condition_expression("org_id = :org_id")
            .expression_attribute_values(":org_id", org_id)
            .into_paginator()
            .items()
            .send()
            .collect::<Result<Vec<_>, _>>()
            .await?
            .into_iter()
            .map(dynamo::from_item)
            .collect::<Result<Vec<_>, _>>()
            .context("Failed to decode dynamo data")
    }
}
