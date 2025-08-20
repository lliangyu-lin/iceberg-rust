use std::path::{Path, PathBuf};
use std::time::Duration;
use anyhow::anyhow;
use datafusion_sqllogictest::{convert_schema_to_types, DFColumnType, DFOutput};
use spark_connect_rs::{SparkSession, SparkSessionBuilder};
use spark_connect_rs::functions::conv;
use sqllogictest::{AsyncDB, DBOutput};
use crate::engine::EngineRunner;
use crate::error::{Error, Result};
use toml::Table as TomlTable;

pub type SparkOutput = DBOutput<DFColumnType>;

pub struct SparkEngine {
    session: SparkSession,
}

#[async_trait::async_trait]
impl AsyncDB for SparkEngine {
    type Error = Error;
    type ColumnType = DFColumnType;

    async fn run(&mut self, sql: &str) -> Result<DBOutput<DFColumnType>> {
        Self::run_query(&self.session, sql)
    }

    async fn shutdown(&mut self) {}

    fn engine_name(&self) -> &str {
        "SparkConnect"
    }

    async fn sleep(dur: Duration) {
        tokio::time::sleep(dur).await;
    }
}

#[async_trait::async_trait]
impl EngineRunner for SparkEngine {
    async fn run_slt_file(&mut self, path: &Path) -> crate::error::Result<()> {
        todo!()
    }
}

impl SparkEngine {
    pub async fn new(configs: TomlTable) -> Result<Self> {
        let url = configs
            .get("url")
            .ok_or_else(|| anyhow!("url property doesn't exist for spark engine"))?
            .as_str()
            .ok_or_else(|| anyhow!("url property is not a string for spark engine"))?;

        let session = SparkSessionBuilder::remote(url)
            .app_name("SparkConnect")
            .build()
            .await
            .map_err(|e| anyhow!(e))?;

        Ok(Self { session })
    }

    pub async fn run_query(session: &SparkSession, sql: impl Into<String>) -> Result<DFOutput> {
        let df = session.sql(sql.into().as_str()).await.unwrap();
        let schema = df.collect().await.unwrap().schema();
        let types = convert_schema_to_types(schema.fields());

    }
}
