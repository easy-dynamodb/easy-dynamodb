use std::collections::HashMap;

// use dynamo::AttributeValue;
// use dynamodb::types::AttributeAction;

use super::*;

#[derive(Debug)]
pub struct Table {
    client: dynamodb::Client,
    table: String,
}

impl Table {
    pub(crate) async fn new(client: dynamodb::Client, table: String) -> Self {
        Self { client, table }
    }

    pub async fn get_item<I, K, V, T>(&self, keys: I) -> anyhow::Result<Option<T>>
    where
        K: Into<String>,
        V: serde::Serialize,
        I: IntoIterator<Item = (K, V)>,
        T: serde::de::DeserializeOwned,
    {
        let keys = keys
            .into_iter()
            .map(|(k, v)| dynamo::to_attribute_value(v).map(|v| (k.into(), v)))
            .collect::<Result<HashMap<_, _>, _>>()?;

        self.client
            .get_item()
            .table_name(&self.table)
            .consistent_read(true)
            .set_key(Some(keys))
            // .key("org_id", org_id)
            // .key("cluster_id", cluster_id)
            .send()
            .await?
            .item
            .map(dynamo::from_item)
            .transpose()
            .context("Failed to decode dynamo data")
    }

    pub async fn get_item2<KV, T>(&self, attrs: KV) -> anyhow::Result<Option<T>>
    where
        KV: serde::Serialize,
        T: serde::de::DeserializeOwned,
    {
        self.client.get_item().set_key(input)
    }

    pub async fn put_item(&self, cluster: &u8) -> anyhow::Result<()> {
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

    pub async fn get_items(&self, org_id: &str) -> anyhow::Result<Vec<u8>> {
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
