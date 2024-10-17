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

use arrow_flight::sql::client::FlightSqlServiceClient;
use datafusion::sql::parser::DFParser;
use log::{error, info, warn};

use color_eyre::eyre::{self, Result};
use tokio::sync::Mutex;
use tokio_stream::StreamExt;
use tonic::{transport::Channel, IntoRequest};

use crate::config::FlightSQLConfig;

use crate::execution::flightsql_benchmarks::FlightSQLBenchmarkStats;

pub type FlightSQLClient = Mutex<Option<FlightSqlServiceClient<Channel>>>;

#[derive(Default)]
pub struct FlightSQLContext {
    config: FlightSQLConfig,
    flightsql_client: FlightSQLClient,
}

impl FlightSQLContext {
    pub fn new(config: FlightSQLConfig) -> Self {
        Self {
            config,
            flightsql_client: Mutex::new(None),
        }
    }

    pub fn client(&self) -> &FlightSQLClient {
        &self.flightsql_client
    }

    /// Create FlightSQL client from users FlightSQL config
    pub async fn create_client(&self) -> Result<()> {
        let url = Box::leak(self.config.connection_url.clone().into_boxed_str());
        info!("Connecting to FlightSQL host: {}", url);
        let channel = Channel::from_static(url).connect().await;
        match channel {
            Ok(c) => {
                let client = FlightSqlServiceClient::new(c);
                let mut guard = self.flightsql_client.lock().await;
                *guard = Some(client);
                Ok(())
            }
            Err(e) => Err(eyre::eyre!(
                "Error creating channel for FlightSQL client: {:?}",
                e
            )),
        }
    }

    pub async fn benchmark_query(&self, query: &str) -> Result<FlightSQLBenchmarkStats> {
        let iterations = self.config.benchmark_iterations;
        let mut get_flight_info_durations = Vec::with_capacity(iterations);
        let mut ttfb_durations = Vec::with_capacity(iterations);
        let mut do_get_durations = Vec::with_capacity(iterations);
        let mut total_durations = Vec::with_capacity(iterations);

        let dialect = datafusion::sql::sqlparser::dialect::GenericDialect {};
        let statements = DFParser::parse_sql_with_dialect(query, &dialect)?;
        if statements.len() == 1 {
            if let Some(ref mut client) = *self.flightsql_client.lock().await {
                for _ in 0..iterations {
                    let start = std::time::Instant::now();
                    let flight_info = client.execute(query.to_string(), None).await?;
                    if flight_info.endpoint.len() > 1 {
                        warn!("More than one endpoint: Benchmark results will not be reliable");
                    }
                    let get_flight_info_duration = start.elapsed();
                    // Current logic wont properly handle having multiple endpoints
                    for endpoint in flight_info.endpoint {
                        if let Some(ticket) = &endpoint.ticket {
                            match client.do_get(ticket.clone().into_request()).await {
                                Ok(ref mut s) => {
                                    while let Some((i, _)) =
                                        futures::stream::StreamExt::enumerate(&mut *s).next().await
                                    {
                                        if i == 0 {
                                            let ttfb_duration =
                                                start.elapsed() - get_flight_info_duration;
                                            ttfb_durations.push(ttfb_duration);
                                        }
                                    }
                                    let do_get_duration =
                                        start.elapsed() - get_flight_info_duration;
                                    do_get_durations.push(do_get_duration);
                                }
                                Err(e) => {
                                    error!("Error getting Flight stream: {:?}", e);
                                }
                            }
                        }
                    }
                    get_flight_info_durations.push(get_flight_info_duration);
                    let total_duration = start.elapsed();
                    total_durations.push(total_duration);
                }
            } else {
                return Err(eyre::eyre!("No FlightSQL client configured"));
            }
            Ok(FlightSQLBenchmarkStats::new(
                query.to_string(),
                get_flight_info_durations,
                ttfb_durations,
                do_get_durations,
                total_durations,
            ))
        } else {
            Err(eyre::eyre!("Only a single statement can be benchmarked"))
        }
    }
}