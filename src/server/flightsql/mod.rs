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

pub mod service;

use crate::args::{Command, DftArgs};
use crate::config::AppConfig;
use crate::db::register_db;
use crate::execution::AppExecution;
use color_eyre::{eyre::eyre, Result};
use datafusion_app::config::merge_configs;
use datafusion_app::extensions::DftSessionStateBuilder;
use datafusion_app::local::ExecutionContext;
use log::info;
use service::FlightSqlServiceImpl;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tonic::transport::Server;
#[cfg(feature = "flightsql")]
use tower_http::validate_request::ValidateRequestHeaderLayer;

use super::try_start_metrics_server;

const DEFAULT_TIMEOUT_SECONDS: u64 = 60;

pub fn create_server_handle(
    config: &AppConfig,
    flightsql: FlightSqlServiceImpl,
    listener: TcpListener,
    rx: oneshot::Receiver<()>,
    // shutdown_future: impl Future<Output = ()> + Send,
) -> Result<JoinHandle<std::result::Result<(), tonic::transport::Error>>> {
    let server_timeout = Duration::from_secs(DEFAULT_TIMEOUT_SECONDS);
    let mut server_builder = Server::builder().timeout(server_timeout);
    let shutdown_future = async move {
        rx.await.ok();
    };

    // TODO: onlu include TrailersLayer for testing
    if cfg!(feature = "flightsql") {
        match (
            &config.flightsql_server.auth.basic_auth,
            &config.flightsql_server.auth.bearer_token,
        ) {
            (Some(_), Some(_)) => Err(eyre!("Only one auth type can be used at a time")),
            (Some(basic), None) => {
                let basic_auth_layer =
                    ValidateRequestHeaderLayer::basic(&basic.username, &basic.password);
                let f = server_builder
                    .layer(basic_auth_layer)
                    .add_service(flightsql.service())
                    .serve_with_incoming_shutdown(
                        tokio_stream::wrappers::TcpListenerStream::new(listener),
                        shutdown_future,
                    );
                Ok(tokio::task::spawn(f))
            }
            (None, Some(token)) => {
                let bearer_auth_layer = ValidateRequestHeaderLayer::bearer(token);
                let f = server_builder
                    .layer(bearer_auth_layer)
                    .add_service(flightsql.service())
                    .serve_with_incoming_shutdown(
                        tokio_stream::wrappers::TcpListenerStream::new(listener),
                        shutdown_future,
                    );
                Ok(tokio::task::spawn(f))
            }
            (None, None) => {
                let f = server_builder
                    .add_service(flightsql.service())
                    .serve_with_incoming_shutdown(
                        tokio_stream::wrappers::TcpListenerStream::new(listener),
                        shutdown_future,
                    );
                Ok(tokio::task::spawn(f))
            }
        }
    } else {
        let f = server_builder
            .add_service(flightsql.service())
            .serve_with_incoming_shutdown(
                tokio_stream::wrappers::TcpListenerStream::new(listener),
                shutdown_future,
            );
        Ok(tokio::task::spawn(f))
    }
}

/// Creates and manages a running FlightSqlServer with a background task
pub struct FlightSqlApp {
    /// channel to send shutdown command
    shutdown: Option<tokio::sync::oneshot::Sender<()>>,

    /// Address the server is listening on
    pub addr: SocketAddr,

    /// handle for the server task
    handle: Option<JoinHandle<Result<(), tonic::transport::Error>>>,
}

impl FlightSqlApp {
    /// create a new app for the flightsql server
    pub async fn try_new(
        app_execution: AppExecution,
        config: &AppConfig,
        addr: SocketAddr,
        metrics_addr: SocketAddr,
    ) -> Result<Self> {
        info!("listening to FlightSQL on {addr}");
        let flightsql = service::FlightSqlServiceImpl::new(app_execution);
        let listener = TcpListener::bind(addr).await.unwrap();

        // prepare the shutdown channel
        let (tx, rx) = tokio::sync::oneshot::channel();
        let handle = create_server_handle(config, flightsql, listener, rx)?;

        try_start_metrics_server(metrics_addr)?;

        let app = Self {
            shutdown: Some(tx),
            addr: metrics_addr,
            handle: Some(handle),
        };
        Ok(app)
    }

    /// Stops the server and waits for the server to shutdown
    pub async fn shutdown_and_wait(mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            shutdown.send(()).expect("server quit early");
        }
        if let Some(handle) = self.handle.take() {
            handle
                .await
                .expect("task join error (panic?)")
                .expect("Server Error found at shutdown");
        }
    }

    pub async fn run(self) {
        if let Some(handle) = self.handle {
            handle
                .await
                .expect("Unable to run server task")
                .expect("Server Error found at shutdown");
        } else {
            panic!("Server task not found");
        }
    }
}

pub async fn try_run(cli: DftArgs, config: AppConfig) -> Result<()> {
    let merged_exec_config = merge_configs(
        config.shared.clone(),
        config.flightsql_server.execution.clone(),
    );
    let session_state_builder = DftSessionStateBuilder::try_new(Some(merged_exec_config.clone()))?
        .with_extensions()
        .await?;
    let session_state = session_state_builder.build()?;
    // FlightSQL Server mode: start a FlightSQL server
    let execution_ctx = ExecutionContext::try_new(
        &merged_exec_config,
        session_state,
        crate::APP_NAME,
        env!("CARGO_PKG_VERSION"),
    )?;
    if cli.run_ddl {
        execution_ctx.execute_ddl().await;
    }
    let app_execution = AppExecution::new(execution_ctx);

    let (addr, metrics_addr) = if let Some(cmd) = cli.command.clone() {
        match cmd {
            Command::ServeFlightSql {
                addr: Some(addr),
                metrics_addr: Some(metrics_addr),
                ..
            } => (addr, metrics_addr),
            Command::ServeFlightSql {
                addr: Some(addr),
                metrics_addr: None,
                ..
            } => (addr, config.flightsql_server.server_metrics_addr),
            Command::ServeFlightSql {
                addr: None,
                metrics_addr: Some(metrics_addr),
                ..
            } => (
                SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 50051),
                metrics_addr,
            ),

            _ => (
                SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 50051),
                config.flightsql_server.server_metrics_addr,
            ),
        }
    } else {
        (
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 50051),
            config.flightsql_server.server_metrics_addr,
        )
    };
    register_db(app_execution.session_ctx(), &config.db).await?;
    let app = FlightSqlApp::try_new(app_execution, &config, addr, metrics_addr).await?;
    app.run().await;
    Ok(())
}
