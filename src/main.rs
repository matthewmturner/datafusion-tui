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

use clap::Parser;
use color_eyre::Result;
use datafusion_dft::args::Command;
#[cfg(any(feature = "flightsql", feature = "http"))]
use datafusion_dft::server;
use datafusion_dft::{args::DftArgs, cli, config::create_config, tpch, tui};
#[cfg(feature = "http")]
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

fn main() -> Result<()> {
    let cli = DftArgs::parse();

    // With Runtimes configured correctly the main Tokio runtime should only be used for network
    // IO, in which a single thread should be sufficient.
    //
    // Ref: https://github.com/datafusion-contrib/datafusion-dft/pull/247#discussion_r1848270250
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()?;

    let entry_point = app_entry_point(cli);
    runtime.block_on(entry_point)
}

// TODO: FlightSQL should use tracing
fn should_init_env_logger(cli: &DftArgs) -> bool {
    #[cfg(feature = "flightsql")]
    if let Some(Command::ServeFlightSql { .. }) = cli.command {
        return true;
    }

    if let Some(Command::GenerateTpch { .. }) = cli.command {
        return true;
    }
    if !cli.files.is_empty() || !cli.commands.is_empty() {
        return true;
    }
    false
}

async fn app_entry_point(cli: DftArgs) -> Result<()> {
    if should_init_env_logger(&cli) {
        env_logger::init();
    }
    let cfg = create_config(cli.config_path());
    if let Some(Command::GenerateTpch { scale_factor }) = cli.command {
        tpch::generate(cfg.clone(), scale_factor).await?;
        return Ok(());
    }

    #[cfg(feature = "flightsql")]
    {
        if matches!(cli.command, Some(Command::FlightSql { .. })) {
            cli::try_run(cli, cfg).await?;
            return Ok(());
        } else if let Some(Command::ServeFlightSql { .. }) = cli.command {
            server::flightsql::try_run(cli.clone(), cfg.clone()).await?;
            return Ok(());
        }
    }

    #[cfg(feature = "http")]
    {
        if let Some(Command::ServeHttp { .. }) = cli.command {
            tracing_subscriber::registry()
                .with(
                    tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| {
                        format!(
                            "{}=debug,tower_http=debug,axum=trace",
                            env!("CARGO_CRATE_NAME")
                        )
                        .into()
                    }),
                )
                .with(tracing_subscriber::fmt::layer().without_time())
                .init();
            server::http::try_run(cli.clone(), cfg.clone()).await?;
            return Ok(());
        }
    }

    if !cli.files.is_empty() || !cli.commands.is_empty() {
        cli::try_run(cli, cfg).await?;
    } else {
        tui::try_run(cli, cfg).await?;
    }

    Ok(())
}
