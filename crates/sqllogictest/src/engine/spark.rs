use std::path::{Path, PathBuf};
use std::time::Duration;
use datafusion_sqllogictest::{DFColumnType, DFOutput};
use indicatif::ProgressBar;
use log::Level::Debug;
use log::{debug, log_enabled};
use spark_connect_rs::SparkSession;
use sqllogictest::{AsyncDB, DBOutput};
use tokio::time::Instant;
use crate::engine::EngineRunner;
use crate::error::Error;
use toml::Table as TomlTable;

pub type SparkOutput = DBOutput<DFColumnType>;

pub struct SparkEngine {
    session: SparkSession,
}

#[async_trait::async_trait]
impl AsyncDB for SparkEngine {
    type Error = Error;
    type ColumnType = DFColumnType;

    async fn run(&mut self, sql: &str) -> Result<DBOutput<Self::ColumnType>, Self::Error> {
        let start = Instant::now();

        self.run_query(sql);

        let duration = start.elapsed();
    }

    async fn shutdown(&mut self) {}

    fn engine_name(&self) -> &str {
        "Spark"
    }

    async fn sleep(dur: Duration) {
        tokio::time::sleep(dur).await;
    }
}

impl EngineRunner for SparkEngine {
    async fn run_slt_file(&mut self, path: &Path) -> crate::error::Result<()> {
        todo!()
    }
}

impl SparkEngine {
    pub async fn new(config: TomlTable) -> crate::error::Result<()> {
        Ok(())
    }

    pub async fn run_query(sql: impl Into<String>) {

    }
}
