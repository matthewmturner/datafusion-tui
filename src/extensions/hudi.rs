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

//! DeltaLake integration: [DeltaLakeExtension]

use crate::config::ExecutionConfig;
use crate::extensions::{DftSessionStateBuilder, Extension};
use hudi::HudiTableFactory;
use std::sync::Arc;

#[derive(Debug, Default)]
pub struct HudiExtension {}

impl HudiExtension {
    pub fn new() -> Self {
        Self {}
    }
}

#[async_trait::async_trait]
impl Extension for HudiExtension {
    async fn register(
        &self,
        _config: ExecutionConfig,
        builder: &mut DftSessionStateBuilder,
    ) -> datafusion_common::Result<()> {
        builder.add_table_factory("HUDI", Arc::new(HudiTableFactory {}));
        Ok(())
    }
}
