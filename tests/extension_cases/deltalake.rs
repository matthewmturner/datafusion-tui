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

use url::Url;

use crate::extension_cases::TestExecution;

#[tokio::test(flavor = "multi_thread")]
async fn test_deltalake() {
    let test_exec = TestExecution::new();

    let cwd = std::env::current_dir().unwrap();
    let path = Url::from_file_path(cwd.join("data/deltalake/simple_table")).unwrap();

    let mut test_exec = test_exec
        .with_setup(&format!(
            "CREATE EXTERNAL TABLE d STORED AS DELTATABLE LOCATION '{}';",
            path
        ))
        .await;

    let output = test_exec
        .run_and_format("SELECT id FROM d ORDER BY id")
        .await;
    assert_eq!(
        output,
        vec!["+----+", "| id |", "+----+", "| 5  |", "| 7  |", "| 9  |", "+----+"]
    );
}
