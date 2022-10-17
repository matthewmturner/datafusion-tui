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

use datafusion::prelude::SessionContext;

#[cfg(feature = "s3")]
pub async fn register_s3(ctx: SessionContext) -> SessionContext {
    use http::Uri;
    use log::info;
    use object_store::aws::AmazonS3Builder;
    use serde::Deserialize;
    use std::fs::File;
    use std::str::FromStr;
    use std::sync::Arc;

    #[derive(Deserialize, Debug)]
    struct S3Config {
        bucket: String,
        endpoint: Option<String>,
        access_key_id: Option<String>,
        secret_access_key: Option<String>,
    }

    async fn config_to_s3(cfg: S3Config) -> AmazonS3Builder {
        info!("Creating S3 from: {:?}", cfg);
        let s3 = AmazonS3Builder::new()
            .with_access_key_id(cfg.access_key_id)
            .with_secret_access_key(cfg.secret_access_key)
            .with_endpoint(&cfg.endpoint)
            .build()
            .unwrap();
    }

    let home = dirs::home_dir();
    if let Some(p) = home {
        let s3_config_path = p.join(".datafusion/object_stores/s3.json");
        let s3 = if s3_config_path.exists() {
            let cfg: S3Config =
                serde_json::from_reader(File::open(s3_config_path).unwrap()).unwrap();
            let s3 = config_to_s3(cfg).await;
            info!("Created S3FileSystem from custom endpoint");
            Arc::new(s3)
        } else {
            let s3 = AmazonS3Builder::from_env();
            info!("Created S3FileSystem from default AWS credentials");
            Arc::new(s3)
        };

        ctx.runtime_env().register_object_store("s3", Arc::new(s3));
        info!("Registered S3 ObjectStore");
    }
    ctx
}
