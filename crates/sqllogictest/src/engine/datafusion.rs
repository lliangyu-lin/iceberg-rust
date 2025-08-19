// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::sync::Arc;
use std::time::Duration;
use anyhow::{Context, anyhow};
use datafusion::catalog::CatalogProvider;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_sqllogictest::{DFColumnType, DFOutput, DataFusion};
use indicatif::ProgressBar;
use sqllogictest::{parse_file, DBOutput, MakeConnection, Record};
use sqllogictest::runner::AsyncDB;
use tempfile::TempDir;
use toml::Table as TomlTable;
use iceberg::io::FileIOBuilder;
use iceberg::{ErrorKind, MemoryCatalog};
use iceberg_datafusion::IcebergCatalogProvider;
use crate::engine::Engine;
use crate::error::{Error, Result};

pub struct DataFusionEngine {
    ctx: SessionContext,
    relative_path: PathBuf,
    pb: ProgressBar
}

#[async_trait::async_trait]
impl Engine for DataFusionEngine {
    async fn run_slt_file(&mut self, path: &Path) -> Result<()> {
        let path_dir = path.to_str().unwrap();
        println!("engine running slt file on path: {path_dir}");

        let mut runner = sqllogictest::Runner::new(|| async {
            Ok(DataFusion::new(
                self.ctx.clone(),
                self.relative_path.clone(),
                self.pb.clone(),
            ))
        });

        let result = Self::run_file_in_runner(path, runner).await;
        self.pb.finish_and_clear();

        // let content = std::fs::read_to_string(path)
        //     .with_context(|| format!("Failed to read slt file {:?}", path))
        //     .map_err(|e| anyhow!(e))?;
        //
        // self.datafusion
        //     .run(content.as_str())
        //     .await
        //     .with_context(|| format!("Failed to run slt file {:?}", path))
        //     .map_err(|e| anyhow!(e))?;

        result
    }
}

impl DataFusionEngine {
    pub async fn new(config: TomlTable) -> Result<Self> {
        let session_config = SessionConfig::new().with_target_partitions(4);
        let ctx = SessionContext::new_with_config(session_config);
        ctx.register_catalog("default", Self::create_catalog(&config).await?);
        Ok(Self {
            ctx,
            relative_path: PathBuf::from("testdata"),
            pb: ProgressBar::new(100)
        })
    }

    async fn create_catalog(_: &TomlTable) -> anyhow::Result<Arc<dyn CatalogProvider>> {
        let temp_dir = TempDir::new()?;
        let file_io = FileIOBuilder::new_fs_io().build()?;
        let iceberg_catalog = MemoryCatalog::new(file_io, Some(temp_dir.path().to_str().unwrap().to_string()));

        let client = Arc::new(iceberg_catalog);

        Ok(Arc::new(IcebergCatalogProvider::try_new(client).await?))
    }

    async fn run_file_in_runner<D: AsyncDB, M: MakeConnection<Conn = D>>(
        path: &Path,
        mut runner: sqllogictest::Runner<D, M>,
    ) -> Result<()> {
        println!("run file in runner");

        let records =
            parse_file(&path).context("Failed to parse slt file")?;

        let mut errs = vec![];
        for record in records.into_iter() {
            if let Record::Halt { .. } = record {
                break;
            }
            if let Err(err) = runner.run_async(record).await {
                errs.push(format!("{err}"));
            }
        }

        if !errs.is_empty() {
            let mut msg = format!("{} errors in file {}\n\n", errs.len(), path.display());
            for (i, err) in errs.iter().enumerate() {
                msg.push_str(&format!("{}. {err}\n\n", i + 1));
            }
            return Err(Error(anyhow!(msg)));
        }

        Ok(())
    }
}
