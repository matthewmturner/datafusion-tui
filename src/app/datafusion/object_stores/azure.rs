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

use datafusion::prelude::ExecutionContext;

#[cfg(feature = "azure")]
pub async fn register_azure(ctx: ExecutionContext) -> ExecutionContext {
    use datafusion_objectstore_azure::object_store::azure::AzureFileSystem;
    use http::Uri;
    use log::info;
    use serde::Deserialize;
    use std::fs::File;
    use std::str::FromStr;
    use std::sync::Arc;

    #[derive(Deserialize, Debug)]
    struct AzureConfig {
        storage_account: String,
        storage_key: String,
    }

    async fn config_to_azure(cfg: AzureConfig) -> AzureFileSystem {
        info!("Creating Azure from: {:?}", cfg);
        AzureFileSystem::new(
            cfg.storage_account,
            cfg.storage_key,
        )
        .await
    }

    let home = dirs::home_dir();
    if let Some(p) = home {
        let azure_config_path = p.join(".datafusion/object_stores/azure.json");
        let azure = if azure_config_path.exists() {
            let cfg: AzureConfig =
                serde_json::from_reader(File::open(azure_config_path).unwrap()).unwrap();
            let azure = config_to_azure(cfg).await;
            info!("Created AzureFileSystem from custom endpoint");
            Arc::new(azure)
        } else {
            let azure = AzureFileSystem::default().await;
            info!("Created AzureFileSystem from default AWS credentials");
            Arc::new(azure)
        };

        ctx.register_object_store("adls2", Azure);
        info!("Registered Azure ObjectStore");
    }
    ctx
}
