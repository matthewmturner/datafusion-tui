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

use std::time::Duration;

use assert_cmd::Command;

use crate::{cli_cases::contains_str, config::TestConfigBuilder};

#[tokio::test]
pub async fn test_basic() {
    let mut config_builder = TestConfigBuilder::default();
    config_builder.with_auth(
        None,
        None,
        None,
        None,
        Some("\"User\"".to_string()),
        Some("\"Pass\"".to_string()),
    );
    let config = config_builder.build("my_config.toml");

    let assert = tokio::task::spawn_blocking(|| {
        Command::cargo_bin("dft")
            .unwrap()
            .arg("-c")
            .arg("SELECT 1 + 2;")
            .arg("--flightsql")
            .arg("--config")
            .arg(config.path)
            .timeout(Duration::from_secs(5))
            .assert()
            .success()
    })
    .await
    .unwrap();

    let expected = r##"
+---------------------+
| Int64(1) + Int64(2) |
+---------------------+
| 3                   |
+---------------------+
    "##;
    assert.stdout(contains_str(expected));
}
