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
//! [`CliApp`]: Command Line User Interface

use crate::args::DftArgs;
use crate::execution::AppExecution;
use color_eyre::eyre::eyre;
use color_eyre::Result;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::sql::parser::DFParser;
use futures::{Stream, StreamExt};
use log::info;
use std::error::Error;
use std::path::{Path, PathBuf};
#[cfg(feature = "flightsql")]
use tonic::IntoRequest;

/// Encapsulates the command line interface
pub struct CliApp {
    /// Execution context for running queries
    app_execution: AppExecution,
    args: DftArgs,
}

impl CliApp {
    pub fn new(app_execution: AppExecution, args: DftArgs) -> Self {
        Self {
            app_execution,
            args,
        }
    }

    /// Execute the provided sql, which was passed as an argument from CLI.
    ///
    /// Optionally, use the FlightSQL client for execution.
    pub async fn execute_files_or_commands(&self) -> color_eyre::Result<()> {
        if self.args.run_ddl {
            self.app_execution.execution_ctx().execute_ddl().await;
        }

        #[cfg(not(feature = "flightsql"))]
        match (
            self.args.files.is_empty(),
            self.args.commands.is_empty(),
            self.args.flightsql,
            self.args.bench,
        ) {
            (_, _, true, _) => Err(eyre!(
                "FLightSQL feature isn't enabled. Reinstall `dft` with `--features=flightsql`"
            )),
            (true, true, _, _) => Err(eyre!("No files or commands provided to execute")),
            (false, true, _, false) => self.execute_files(&self.args.files).await,
            (false, true, _, true) => self.benchmark_files(&self.args.files).await,
            (true, false, _, false) => self.execute_commands(&self.args.commands).await,
            (true, false, _, true) => self.benchmark_commands(&self.args.commands).await,
            (false, false, _, false) => Err(eyre!(
                "Cannot execute both files and commands at the same time"
            )),
            (false, false, false, true) => Err(eyre!("Cannot benchmark without a command or file")),
        }
        #[cfg(feature = "flightsql")]
        match (
            self.args.files.is_empty(),
            self.args.commands.is_empty(),
            self.args.flightsql,
            self.args.bench,
        ) {
            (true, true, _, _) => Err(eyre!("No files or commands provided to execute")),
            (false, true, true, false) => self.flightsql_execute_files(&self.args.files).await,
            (false, true, true, true) => self.flightsql_benchmark_files(&self.args.files).await,
            (false, true, false, false) => self.execute_files(&self.args.files).await,
            (false, true, false, true) => self.benchmark_files(&self.args.files).await,

            (true, false, true, false) => {
                self.flightsql_execute_commands(&self.args.commands).await
            }
            (true, false, true, true) => {
                self.flightsql_benchmark_commands(&self.args.commands).await
            }
            (true, false, false, false) => self.execute_commands(&self.args.commands).await,
            (true, false, false, true) => self.benchmark_commands(&self.args.commands).await,
            (false, false, false, true) => Err(eyre!("Cannot benchmark without a command or file")),
            (false, false, _, _) => Err(eyre!(
                "Cannot execute both files and commands at the same time"
            )),
        }
    }

    async fn execute_files(&self, files: &[PathBuf]) -> Result<()> {
        info!("Executing files: {:?}", files);
        for file in files {
            self.exec_from_file(file).await?
        }

        Ok(())
    }

    async fn benchmark_files(&self, files: &[PathBuf]) -> Result<()> {
        info!("Benchmarking files: {:?}", files);
        for file in files {
            let query = std::fs::read_to_string(file)?;
            self.benchmark_from_string(&query).await?;
        }
        Ok(())
    }

    #[cfg(feature = "flightsql")]
    async fn flightsql_execute_files(&self, files: &[PathBuf]) -> color_eyre::Result<()> {
        info!("Executing FlightSQL files: {:?}", files);
        for (i, file) in files.iter().enumerate() {
            let file = std::fs::read_to_string(file)?;
            self.exec_from_flightsql(file, i).await?;
        }

        Ok(())
    }

    #[cfg(feature = "flightsql")]
    async fn flightsql_benchmark_files(&self, files: &[PathBuf]) -> Result<()> {
        info!("Benchmarking FlightSQL files: {:?}", files);
        for file in files {
            let query = std::fs::read_to_string(file)?;
            self.flightsql_benchmark_from_string(&query).await?;
        }

        Ok(())
    }

    #[cfg(feature = "flightsql")]
    async fn exec_from_flightsql(&self, sql: String, i: usize) -> color_eyre::Result<()> {
        let client = self.app_execution.flightsql_client();
        let mut guard = client.lock().await;
        if let Some(client) = guard.as_mut() {
            let start = if self.args.time {
                Some(std::time::Instant::now())
            } else {
                None
            };
            let flight_info = client.execute(sql, None).await?;
            for endpoint in flight_info.endpoint {
                if let Some(ticket) = endpoint.ticket {
                    let stream = client.do_get(ticket.into_request()).await?;
                    if let Some(start) = start {
                        self.exec_stream(stream).await;
                        let elapsed = start.elapsed();
                        println!("Query {i} executed in {:?}", elapsed);
                    } else {
                        self.print_any_stream(stream).await;
                    }
                }
            }
        } else {
            println!("No FlightSQL client configured.  Add one in `~/.config/dft/config.toml`");
        }

        Ok(())
    }

    async fn execute_commands(&self, commands: &[String]) -> color_eyre::Result<()> {
        info!("Executing commands: {:?}", commands);
        for command in commands {
            self.exec_from_string(command).await?
        }

        Ok(())
    }

    async fn benchmark_commands(&self, commands: &[String]) -> color_eyre::Result<()> {
        info!("Benchmarking commands: {:?}", commands);
        for command in commands {
            self.benchmark_from_string(command).await?;
        }
        Ok(())
    }

    #[cfg(feature = "flightsql")]
    async fn flightsql_execute_commands(&self, commands: &[String]) -> color_eyre::Result<()> {
        info!("Executing FlightSQL commands: {:?}", commands);
        for (i, command) in commands.iter().enumerate() {
            self.exec_from_flightsql(command.to_string(), i).await?
        }

        Ok(())
    }

    #[cfg(feature = "flightsql")]
    async fn flightsql_benchmark_commands(&self, commands: &[String]) -> color_eyre::Result<()> {
        info!("Benchmark FlightSQL commands: {:?}", commands);
        for command in commands {
            self.flightsql_benchmark_from_string(command).await?;
        }

        Ok(())
    }

    async fn exec_from_string(&self, sql: &str) -> Result<()> {
        let dialect = datafusion::sql::sqlparser::dialect::GenericDialect {};
        let statements = DFParser::parse_sql_with_dialect(sql, &dialect)?;
        let start = if self.args.time {
            Some(std::time::Instant::now())
        } else {
            None
        };
        for (i, statement) in statements.into_iter().enumerate() {
            let stream = self
                .app_execution
                .execution_ctx()
                .execute_statement(statement)
                .await?;
            if let Some(start) = start {
                self.exec_stream(stream).await;
                let elapsed = start.elapsed();
                println!("Query {i} executed in {:?}", elapsed);
            } else {
                self.print_any_stream(stream).await;
            }
        }
        Ok(())
    }

    async fn benchmark_from_string(&self, sql: &str) -> Result<()> {
        let stats = self
            .app_execution
            .execution_ctx()
            .benchmark_query(sql)
            .await?;
        println!("{}", stats);
        Ok(())
    }

    #[cfg(feature = "flightsql")]
    async fn flightsql_benchmark_from_string(&self, sql: &str) -> Result<()> {
        let stats = self
            .app_execution
            .flightsql_ctx()
            .benchmark_query(sql)
            .await?;
        println!("{}", stats);
        Ok(())
    }

    /// run and execute SQL statements and commands from a file, against a context
    /// with the given print options
    pub async fn exec_from_file(&self, file: &Path) -> color_eyre::Result<()> {
        let string = std::fs::read_to_string(file)?;

        self.exec_from_string(&string).await?;

        Ok(())
    }

    /// executes a sql statement and prints the result to stdout
    pub async fn execute_and_print_sql(&self, sql: &str) -> color_eyre::Result<()> {
        let stream = self.app_execution.execution_ctx().execute_sql(sql).await?;
        self.print_any_stream(stream).await;
        Ok(())
    }

    async fn exec_stream<S, E>(&self, mut stream: S)
    where
        S: Stream<Item = Result<RecordBatch, E>> + Unpin,
        E: Error,
    {
        while let Some(maybe_batch) = stream.next().await {
            match maybe_batch {
                Ok(_) => {}
                Err(e) => {
                    println!("Error executing SQL: {e}");
                    break;
                }
            }
        }
    }

    async fn print_any_stream<S, E>(&self, mut stream: S)
    where
        S: Stream<Item = Result<RecordBatch, E>> + Unpin,
        E: Error,
    {
        while let Some(maybe_batch) = stream.next().await {
            match maybe_batch {
                Ok(batch) => match pretty_format_batches(&[batch]) {
                    Ok(d) => println!("{}", d),
                    Err(e) => println!("Error formatting batch: {e}"),
                },
                Err(e) => println!("Error executing SQL: {e}"),
            }
        }
    }
}
