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

use std::{io::Read, time::Duration};

use assert_cmd::Command;
use datafusion_app::local::ExecutionContext;
use datafusion_dft::{
    execution::AppExecution,
    server::flightsql::service::FlightSqlServiceImpl,
    test_utils::fixture::{TestFixture, TestFlightSqlServiceImpl},
};

use crate::{
    cli_cases::{contains_str, sql_in_file},
    config::TestConfigBuilder,
};

#[tokio::test]
pub async fn test_execute_with_no_flightsql_server() {
    let _ = env_logger::builder().is_test(true).try_init();
    let assert = Command::cargo_bin("dft")
        .unwrap()
        .arg("-c")
        .arg("SELECT 1 + 3;")
        .arg("--flightsql")
        .assert()
        .failure();

    assert.stderr(contains_str("Error creating channel for FlightSQL client"));
}

#[tokio::test]
pub async fn test_execute() {
    let test_server = TestFlightSqlServiceImpl::new();
    let fixture = TestFixture::new(test_server.service(), "127.0.0.1:50051").await;

    let assert = tokio::task::spawn_blocking(|| {
        Command::cargo_bin("dft")
            .unwrap()
            .arg("-c")
            .arg("SELECT 1 + 2;")
            .arg("--flightsql")
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
    fixture.shutdown_and_wait().await;
}

#[tokio::test]
pub async fn test_invalid_sql_command() {
    let test_server = TestFlightSqlServiceImpl::new();
    let fixture = TestFixture::new(test_server.service(), "127.0.0.1:50051").await;

    let assert = tokio::task::spawn_blocking(|| {
        Command::cargo_bin("dft")
            .unwrap()
            .arg("-c")
            .arg("SELEC 1;")
            .arg("--flightsql")
            .timeout(Duration::from_secs(5))
            .assert()
            .failure()
    })
    .await
    .unwrap();

    // I think its implementation specific how they decide to return errors but I believe they will
    // all be in the form of an IPC error
    let expected = r##"Error: Ipc error"##;
    assert.stderr(contains_str(expected));
    fixture.shutdown_and_wait().await;
}

#[tokio::test]
pub async fn test_execute_multiple_commands() {
    let test_server = TestFlightSqlServiceImpl::new();
    let fixture = TestFixture::new(test_server.service(), "127.0.0.1:50051").await;

    let assert = tokio::task::spawn_blocking(|| {
        Command::cargo_bin("dft")
            .unwrap()
            .arg("--flightsql")
            .arg("-c")
            .arg("SELECT 1 + 1;")
            .arg("-c")
            .arg("SELECT 1 + 2;")
            .assert()
            .success()
    })
    .await
    .unwrap();

    let expected = r##"
+---------------------+
| Int64(1) + Int64(1) |
+---------------------+
| 2                   |
+---------------------+
+---------------------+
| Int64(1) + Int64(2) |
+---------------------+
| 3                   |
+---------------------+
    "##;
    assert.stdout(contains_str(expected));
    fixture.shutdown_and_wait().await;
}

#[tokio::test]
pub async fn test_command_in_file() {
    let test_server = TestFlightSqlServiceImpl::new();
    let fixture = TestFixture::new(test_server.service(), "127.0.0.1:50051").await;
    let file = sql_in_file("SELECT 1 + 1");
    let assert = tokio::task::spawn_blocking(move || {
        Command::cargo_bin("dft")
            .unwrap()
            .arg("--flightsql")
            .arg("-f")
            .arg(file.path())
            .assert()
            .success()
    })
    .await
    .unwrap();
    let expected = r##"
+---------------------+
| Int64(1) + Int64(1) |
+---------------------+
| 2                   |
+---------------------+
    "##;
    assert.stdout(contains_str(expected));
    fixture.shutdown_and_wait().await;
}

#[tokio::test]
pub async fn test_invalid_sql_command_in_file() {
    let test_server = TestFlightSqlServiceImpl::new();
    let fixture = TestFixture::new(test_server.service(), "127.0.0.1:50051").await;
    let file = sql_in_file("SELEC 1");
    let assert = tokio::task::spawn_blocking(move || {
        Command::cargo_bin("dft")
            .unwrap()
            .arg("--flightsql")
            .arg("-f")
            .arg(file.path())
            .assert()
            .failure()
    })
    .await
    .unwrap();

    // I think its implementation specific how they decide to return errors but I believe they will
    // all be in the form of an IPC error
    let expected = r##"Error: Ipc error"##;
    assert.stderr(contains_str(expected));
    fixture.shutdown_and_wait().await;
}

#[tokio::test]
pub async fn test_command_multiple_files() {
    let test_server = TestFlightSqlServiceImpl::new();
    let fixture = TestFixture::new(test_server.service(), "127.0.0.1:50051").await;
    let file1 = sql_in_file("SELECT 1 + 1");
    let file2 = sql_in_file("SELECT 1 + 2");
    let assert = tokio::task::spawn_blocking(move || {
        Command::cargo_bin("dft")
            .unwrap()
            .arg("--flightsql")
            .arg("-f")
            .arg(file1.path())
            .arg("-f")
            .arg(file2.path())
            .assert()
            .success()
    })
    .await
    .unwrap();
    let expected = r##"
+---------------------+
| Int64(1) + Int64(1) |
+---------------------+
| 2                   |
+---------------------+
+---------------------+
| Int64(1) + Int64(2) |
+---------------------+
| 3                   |
+---------------------+
    "##;
    assert.stdout(contains_str(expected));
    fixture.shutdown_and_wait().await;
}

#[tokio::test]
pub async fn test_time_command() {
    let test_server = TestFlightSqlServiceImpl::new();
    let fixture = TestFixture::new(test_server.service(), "127.0.0.1:50051").await;
    let assert = tokio::task::spawn_blocking(move || {
        Command::cargo_bin("dft")
            .unwrap()
            .arg("--flightsql")
            .arg("-c")
            .arg("SELECT 1 + 1")
            .arg("--time")
            .assert()
            .success()
    })
    .await
    .unwrap();
    let expected = r##"executed in"##;
    assert.stdout(contains_str(expected));
    fixture.shutdown_and_wait().await;
}

#[tokio::test]
pub async fn test_time_files() {
    let test_server = TestFlightSqlServiceImpl::new();
    let fixture = TestFixture::new(test_server.service(), "127.0.0.1:50051").await;
    let file1 = sql_in_file("SELECT 1 + 1");
    let assert = tokio::task::spawn_blocking(move || {
        Command::cargo_bin("dft")
            .unwrap()
            .arg("--flightsql")
            .arg("-f")
            .arg(file1.path())
            .arg("--time")
            .assert()
            .success()
    })
    .await
    .unwrap();
    let expected = r##"executed in"##;
    assert.stdout(contains_str(expected));
    fixture.shutdown_and_wait().await;
}

#[tokio::test]
pub async fn test_bench_command() {
    let test_server = TestFlightSqlServiceImpl::new();
    let fixture = TestFixture::new(test_server.service(), "127.0.0.1:50051").await;
    let assert = tokio::task::spawn_blocking(move || {
        Command::cargo_bin("dft")
            .unwrap()
            .arg("-c")
            .arg("SELECT 1")
            .arg("--bench")
            .arg("--flightsql")
            .assert()
            .success()
    })
    .await
    .unwrap();

    let expected = r##"
----------------------------
Benchmark Stats (10 runs)
----------------------------
SELECT 1
----------------------------"##;
    assert.stdout(contains_str(expected));
    fixture.shutdown_and_wait().await;
}

#[tokio::test]
pub async fn test_bench_files() {
    let test_server = TestFlightSqlServiceImpl::new();
    let fixture = TestFixture::new(test_server.service(), "127.0.0.1:50051").await;
    let file = sql_in_file(r#"SELECT 1 + 1;"#);
    let assert = tokio::task::spawn_blocking(move || {
        Command::cargo_bin("dft")
            .unwrap()
            .arg("-f")
            .arg(file.path())
            .arg("--bench")
            .arg("--flightsql")
            .assert()
            .success()
    })
    .await
    .unwrap();

    let expected_err = r##"
----------------------------
Benchmark Stats (10 runs)
----------------------------
SELECT 1 + 1;
----------------------------"##;
    assert.code(0).stdout(contains_str(expected_err));
    fixture.shutdown_and_wait().await;
}

#[tokio::test]
async fn test_custom_config_benchmark_iterations() {
    let test_server = TestFlightSqlServiceImpl::new();
    let fixture = TestFixture::new(test_server.service(), "127.0.0.1:50051").await;
    let mut config_builder = TestConfigBuilder::default();
    config_builder.with_flightsql_benchmark_iterations(5);
    let config = config_builder.build("my_config.toml");

    let assert = tokio::task::spawn_blocking(move || {
        Command::cargo_bin("dft")
            .unwrap()
            .arg("--config")
            .arg(config.path)
            .arg("-c")
            .arg("SELECT 1")
            .arg("--flightsql")
            .arg("--bench")
            .assert()
            .success()
    })
    .await
    .unwrap();

    let expected = "5 runs";

    assert.stdout(contains_str(expected));
    fixture.shutdown_and_wait().await;
}

#[tokio::test]
pub async fn test_bench_command_and_save() {
    let test_server = TestFlightSqlServiceImpl::new();
    let fixture = TestFixture::new(test_server.service(), "127.0.0.1:50051").await;

    let temp_dir = tempfile::tempdir().unwrap();
    let file = temp_dir.path().join("results.csv");
    let cloned = file.clone();
    let assert = tokio::task::spawn_blocking(move || {
        Command::cargo_bin("dft")
            .unwrap()
            .arg("-c")
            .arg("SELECT 1")
            .arg("--bench")
            .arg("--flightsql")
            .arg("--save")
            .arg(cloned.to_str().unwrap())
            .assert()
            .success()
    })
    .await
    .unwrap();

    let expected = r##"
----------------------------
Benchmark Stats (10 runs)
----------------------------
SELECT 1
----------------------------"##;
    assert.stdout(contains_str(expected));
    assert!(file.exists());
    fixture.shutdown_and_wait().await;
}

#[tokio::test]
pub async fn test_bench_files_and_save() {
    let test_server = TestFlightSqlServiceImpl::new();
    let fixture = TestFixture::new(test_server.service(), "127.0.0.1:50051").await;
    let file = sql_in_file(r#"SELECT 1 + 1;"#);

    let temp_dir = tempfile::tempdir().unwrap();
    let results_file = temp_dir.path().join("results.csv");
    let cloned = results_file.clone();
    let assert = tokio::task::spawn_blocking(move || {
        Command::cargo_bin("dft")
            .unwrap()
            .arg("-f")
            .arg(file.path())
            .arg("--bench")
            .arg("--flightsql")
            .arg("--save")
            .arg(cloned.to_str().unwrap())
            .assert()
            .success()
    })
    .await
    .unwrap();

    let expected_err = r##"
----------------------------
Benchmark Stats (10 runs)
----------------------------
SELECT 1 + 1;
----------------------------"##;
    assert.code(0).stdout(contains_str(expected_err));
    assert!(results_file.exists());
    fixture.shutdown_and_wait().await;
}

#[tokio::test]
pub async fn test_bench_command_and_save_then_append() {
    let test_server = TestFlightSqlServiceImpl::new();
    let fixture = TestFixture::new(test_server.service(), "127.0.0.1:50051").await;

    let temp_dir = tempfile::tempdir().unwrap();
    let file = temp_dir.path().join("results.csv");
    let cloned = file.clone();
    let assert = tokio::task::spawn_blocking(move || {
        Command::cargo_bin("dft")
            .unwrap()
            .arg("-c")
            .arg("SELECT 1")
            .arg("--bench")
            .arg("--flightsql")
            .arg("--save")
            .arg(cloned.to_str().unwrap())
            .assert()
            .success()
    })
    .await
    .unwrap();

    let expected = r##"
----------------------------
Benchmark Stats (10 runs)
----------------------------
SELECT 1
----------------------------"##;
    assert.stdout(contains_str(expected));
    assert!(file.exists());

    let cloned_again = file.clone();
    tokio::task::spawn_blocking(move || {
        Command::cargo_bin("dft")
            .unwrap()
            .arg("-c")
            .arg("SELECT 1")
            .arg("--bench")
            .arg("--flightsql")
            .arg("--save")
            .arg(cloned_again.to_str().unwrap())
            .arg("--append")
            .assert()
            .success()
    })
    .await
    .unwrap();

    let contents = std::fs::read_to_string(file).unwrap();
    let lines: Vec<&str> = contents.lines().collect();
    assert_eq!(3, lines.len());

    fixture.shutdown_and_wait().await;
}

#[tokio::test]
pub async fn test_bench_command_customer_iterations() {
    let test_server = TestFlightSqlServiceImpl::new();
    let fixture = TestFixture::new(test_server.service(), "127.0.0.1:50051").await;

    let assert = tokio::task::spawn_blocking(move || {
        Command::cargo_bin("dft")
            .unwrap()
            .arg("-c")
            .arg("SELECT 1")
            .arg("--bench")
            .arg("--flightsql")
            .arg("-n")
            .arg("3")
            .assert()
            .success()
    })
    .await
    .unwrap();

    let expected = r##"
----------------------------
Benchmark Stats (3 runs)
----------------------------
SELECT 1
----------------------------"##;
    assert.stdout(contains_str(expected));
    fixture.shutdown_and_wait().await;
}

#[tokio::test]
pub async fn test_execute_custom_port() {
    let test_server = TestFlightSqlServiceImpl::new();
    let fixture = TestFixture::new(test_server.service(), "127.0.0.1:50052").await;

    let assert = tokio::task::spawn_blocking(|| {
        Command::cargo_bin("dft")
            .unwrap()
            .arg("-c")
            .arg("SELECT 1 + 2;")
            .arg("--flightsql")
            .arg("--host")
            .arg("http://localhost:50052")
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
    fixture.shutdown_and_wait().await;
}

#[tokio::test]
pub async fn test_output_csv() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("test.csv");

    let test_server = TestFlightSqlServiceImpl::new();
    let fixture = TestFixture::new(test_server.service(), "127.0.0.1:50051").await;

    let cloned_path = path.clone();

    tokio::task::spawn_blocking(|| {
        Command::cargo_bin("dft")
            .unwrap()
            .arg("-c")
            .arg("SELECT 1")
            .arg("--flightsql")
            .arg("-o")
            .arg(cloned_path)
            .assert()
            .success()
    })
    .await
    .unwrap();
    let mut file = std::fs::File::open(path).unwrap();
    let mut buffer = String::new();
    file.read_to_string(&mut buffer).unwrap();

    let expected = "Int64(1)\n1\n";
    assert_eq!(buffer, expected);

    fixture.shutdown_and_wait().await;
}

#[tokio::test]
pub async fn test_output_json() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("test.json");

    let test_server = TestFlightSqlServiceImpl::new();
    let fixture = TestFixture::new(test_server.service(), "127.0.0.1:50051").await;

    let cloned_path = path.clone();

    tokio::task::spawn_blocking(|| {
        Command::cargo_bin("dft")
            .unwrap()
            .arg("-c")
            .arg("SELECT 1")
            .arg("--flightsql")
            .arg("-o")
            .arg(cloned_path)
            .assert()
            .success()
    })
    .await
    .unwrap();
    let mut file = std::fs::File::open(path).unwrap();
    let mut buffer = String::new();
    file.read_to_string(&mut buffer).unwrap();

    let expected = "{\"Int64(1)\":1}\n";
    assert_eq!(buffer, expected);

    fixture.shutdown_and_wait().await;
}

#[tokio::test]
async fn test_output_parquet() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("test.parquet");

    let test_server = TestFlightSqlServiceImpl::new();
    let fixture = TestFixture::new(test_server.service(), "127.0.0.1:50051").await;

    let cloned_path = path.clone();

    tokio::task::spawn_blocking(|| {
        let sql = "SELECT 1".to_string();
        Command::cargo_bin("dft")
            .unwrap()
            .arg("-c")
            .arg(sql.clone())
            .arg("--flightsql")
            .arg("-o")
            .arg(cloned_path)
            .timeout(Duration::from_secs(5))
            .assert()
            .success();
    })
    .await
    .unwrap();

    let read_sql = format!("SELECT * FROM '{}'", path.to_str().unwrap());

    let assert = tokio::task::spawn_blocking(|| {
        Command::cargo_bin("dft")
            .unwrap()
            .arg("-c")
            .arg(read_sql)
            .assert()
            .success()
    })
    .await
    .unwrap();

    let expected = r#"
+----------+
| Int64(1) |
+----------+
| 1        |
+----------+"#;

    assert.stdout(contains_str(expected));

    fixture.shutdown_and_wait().await;
}

#[tokio::test]
async fn test_query_command() {
    let test_server = TestFlightSqlServiceImpl::new();
    let fixture = TestFixture::new(test_server.service(), "127.0.0.1:50051").await;

    let assert = tokio::task::spawn_blocking(|| {
        let sql = "SELECT 1".to_string();
        Command::cargo_bin("dft")
            .unwrap()
            .arg("flightsql")
            .arg("statement-query")
            .arg("--sql")
            .arg(sql.clone())
            .timeout(Duration::from_secs(5))
            .assert()
            .success()
    })
    .await
    .unwrap();

    let expected = r#"
+----------+
| Int64(1) |
+----------+
| 1        |
+----------+"#;

    assert.stdout(contains_str(expected));

    fixture.shutdown_and_wait().await;
}

#[tokio::test]
async fn test_get_catalogs() {
    let ctx = ExecutionContext::test();
    let exec = AppExecution::new(ctx);
    let test_server = FlightSqlServiceImpl::new(exec);
    let fixture = TestFixture::new(test_server.service(), "127.0.0.1:50051").await;

    let assert = tokio::task::spawn_blocking(|| {
        Command::cargo_bin("dft")
            .unwrap()
            .arg("flightsql")
            .arg("get-catalogs")
            .timeout(Duration::from_secs(5))
            .assert()
            .success()
    })
    .await
    .unwrap();

    let expected = r#"
+---------------+
| table_catalog |
+---------------+
| datafusion    |
+---------------+"#;

    assert.stdout(contains_str(expected));

    fixture.shutdown_and_wait().await;
}

#[tokio::test]
async fn test_get_schemas_no_filter() {
    let ctx = ExecutionContext::test();
    let exec = AppExecution::new(ctx);
    let test_server = FlightSqlServiceImpl::new(exec);
    let fixture = TestFixture::new(test_server.service(), "127.0.0.1:50051").await;

    let assert = tokio::task::spawn_blocking(|| {
        Command::cargo_bin("dft")
            .unwrap()
            .arg("flightsql")
            .arg("get-db-schemas")
            .timeout(Duration::from_secs(5))
            .assert()
            .success()
    })
    .await
    .unwrap();

    let expected = r#"
+---------------+--------------------+
| table_catalog | table_schema       |
+---------------+--------------------+
| datafusion    | information_schema |
| test          | information_schema |
| test          | meta               |
+---------------+--------------------+
"#;

    assert.stdout(contains_str(expected));

    fixture.shutdown_and_wait().await;
}

#[tokio::test]
async fn test_get_schemas_filter_catalog() {
    let ctx = ExecutionContext::test();
    let exec = AppExecution::new(ctx);
    let test_server = FlightSqlServiceImpl::new(exec);
    let fixture = TestFixture::new(test_server.service(), "127.0.0.1:50051").await;

    let assert = tokio::task::spawn_blocking(|| {
        Command::cargo_bin("dft")
            .unwrap()
            .arg("flightsql")
            .arg("get-db-schemas")
            .arg("--catalog")
            .arg("test")
            .timeout(Duration::from_secs(5))
            .assert()
            .success()
    })
    .await
    .unwrap();

    let expected = r#"
+---------------+--------------------+
| table_catalog | table_schema       |
+---------------+--------------------+
| test          | information_schema |
| test          | meta               |
+---------------+--------------------+
"#;

    assert.stdout(contains_str(expected));

    fixture.shutdown_and_wait().await;
}

#[tokio::test]
async fn test_get_schemas_filter_pattern() {
    let ctx = ExecutionContext::test();
    let exec = AppExecution::new(ctx);
    let test_server = FlightSqlServiceImpl::new(exec);
    let fixture = TestFixture::new(test_server.service(), "127.0.0.1:50051").await;

    let assert = tokio::task::spawn_blocking(|| {
        Command::cargo_bin("dft")
            .unwrap()
            .arg("flightsql")
            .arg("get-db-schemas")
            .arg("--db-schema-filter-pattern")
            .arg("information")
            .timeout(Duration::from_secs(5))
            .assert()
            .success()
    })
    .await
    .unwrap();

    let expected = r#"
+---------------+--------------------+
| table_catalog | table_schema       |
+---------------+--------------------+
| datafusion    | information_schema |
| test          | information_schema |
+---------------+--------------------+
"#;

    assert.stdout(contains_str(expected));

    fixture.shutdown_and_wait().await;
}

#[tokio::test]
async fn test_get_schemas_filter_pattern_and_catalog() {
    let ctx = ExecutionContext::test();
    let exec = AppExecution::new(ctx);
    let test_server = FlightSqlServiceImpl::new(exec);
    let fixture = TestFixture::new(test_server.service(), "127.0.0.1:50051").await;

    let assert = tokio::task::spawn_blocking(|| {
        Command::cargo_bin("dft")
            .unwrap()
            .arg("flightsql")
            .arg("get-db-schemas")
            .arg("--db-schema-filter-pattern")
            .arg("information")
            .arg("--catalog")
            .arg("test")
            .timeout(Duration::from_secs(5))
            .assert()
            .success()
    })
    .await
    .unwrap();

    let expected = r#"
+---------------+--------------------+
| table_catalog | table_schema       |
+---------------+--------------------+
| test          | information_schema |
+---------------+--------------------+
"#;

    assert.stdout(contains_str(expected));

    fixture.shutdown_and_wait().await;
}

#[tokio::test]
async fn test_get_tables_no_filter() {
    let ctx = ExecutionContext::test();
    let exec = AppExecution::new(ctx);
    let test_server = FlightSqlServiceImpl::new(exec);
    let fixture = TestFixture::new(test_server.service(), "127.0.0.1:50051").await;

    let assert = tokio::task::spawn_blocking(|| {
        Command::cargo_bin("dft")
            .unwrap()
            .arg("flightsql")
            .arg("get-tables")
            .timeout(Duration::from_secs(5))
            .assert()
            .success()
    })
    .await
    .unwrap();

    let expected = r#"
+---------------+--------------------+-------------+------------+
| table_catalog | table_schema       | table_name  | table_type |
+---------------+--------------------+-------------+------------+
| datafusion    | information_schema | columns     | VIEW       |
| datafusion    | information_schema | df_settings | VIEW       |
| datafusion    | information_schema | parameters  | VIEW       |
| datafusion    | information_schema | routines    | VIEW       |
| datafusion    | information_schema | schemata    | VIEW       |
| datafusion    | information_schema | tables      | VIEW       |
| datafusion    | information_schema | views       | VIEW       |
| test          | information_schema | columns     | VIEW       |
| test          | information_schema | df_settings | VIEW       |
| test          | information_schema | parameters  | VIEW       |
| test          | information_schema | routines    | VIEW       |
| test          | information_schema | schemata    | VIEW       |
| test          | information_schema | tables      | VIEW       |
| test          | information_schema | views       | VIEW       |
| test          | meta               | versions    | BASE TABLE |
+---------------+--------------------+-------------+------------+
"#;

    assert.stdout(contains_str(expected));

    fixture.shutdown_and_wait().await;
}

#[tokio::test]
async fn test_get_tables_catalog_filter() {
    let ctx = ExecutionContext::test();
    let exec = AppExecution::new(ctx);
    let test_server = FlightSqlServiceImpl::new(exec);
    let fixture = TestFixture::new(test_server.service(), "127.0.0.1:50051").await;

    let assert = tokio::task::spawn_blocking(|| {
        Command::cargo_bin("dft")
            .unwrap()
            .arg("flightsql")
            .arg("get-tables")
            .arg("--catalog")
            .arg("test")
            .timeout(Duration::from_secs(5))
            .assert()
            .success()
    })
    .await
    .unwrap();

    let expected = r#"
+---------------+--------------------+-------------+------------+
| table_catalog | table_schema       | table_name  | table_type |
+---------------+--------------------+-------------+------------+
| test          | information_schema | columns     | VIEW       |
| test          | information_schema | df_settings | VIEW       |
| test          | information_schema | parameters  | VIEW       |
| test          | information_schema | routines    | VIEW       |
| test          | information_schema | schemata    | VIEW       |
| test          | information_schema | tables      | VIEW       |
| test          | information_schema | views       | VIEW       |
| test          | meta               | versions    | BASE TABLE |
+---------------+--------------------+-------------+------------+
"#;

    assert.stdout(contains_str(expected));

    fixture.shutdown_and_wait().await;
}

#[tokio::test]
async fn test_get_tables_schema_filter() {
    let ctx = ExecutionContext::test();
    let exec = AppExecution::new(ctx);
    let test_server = FlightSqlServiceImpl::new(exec);
    let fixture = TestFixture::new(test_server.service(), "127.0.0.1:50051").await;

    let assert = tokio::task::spawn_blocking(|| {
        Command::cargo_bin("dft")
            .unwrap()
            .arg("flightsql")
            .arg("get-tables")
            .arg("--db-schema-filter-pattern")
            .arg("meta")
            .timeout(Duration::from_secs(5))
            .assert()
            .success()
    })
    .await
    .unwrap();

    let expected = r#"
+---------------+--------------+------------+------------+
| table_catalog | table_schema | table_name | table_type |
+---------------+--------------+------------+------------+
| test          | meta         | versions   | BASE TABLE |
+---------------+--------------+------------+------------+
"#;

    assert.stdout(contains_str(expected));

    fixture.shutdown_and_wait().await;
}

#[tokio::test]
async fn test_get_tables_table_filter() {
    let ctx = ExecutionContext::test();
    let exec = AppExecution::new(ctx);
    let test_server = FlightSqlServiceImpl::new(exec);
    let fixture = TestFixture::new(test_server.service(), "127.0.0.1:50051").await;

    let assert = tokio::task::spawn_blocking(|| {
        Command::cargo_bin("dft")
            .unwrap()
            .arg("flightsql")
            .arg("get-tables")
            .arg("--table-name-filter-pattern")
            .arg("tables")
            .timeout(Duration::from_secs(5))
            .assert()
            .success()
    })
    .await
    .unwrap();

    let expected = r#"
+---------------+--------------------+------------+------------+
| table_catalog | table_schema       | table_name | table_type |
+---------------+--------------------+------------+------------+
| datafusion    | information_schema | tables     | VIEW       |
| test          | information_schema | tables     | VIEW       |
+---------------+--------------------+------------+------------+
"#;

    assert.stdout(contains_str(expected));

    fixture.shutdown_and_wait().await;
}

#[tokio::test]
async fn test_get_tables_table_type() {
    let ctx = ExecutionContext::test();
    let exec = AppExecution::new(ctx);
    let test_server = FlightSqlServiceImpl::new(exec);
    let fixture = TestFixture::new(test_server.service(), "127.0.0.1:50051").await;

    let assert = tokio::task::spawn_blocking(|| {
        Command::cargo_bin("dft")
            .unwrap()
            .arg("flightsql")
            .arg("get-tables")
            .arg("--table-types")
            .arg("VIEW")
            .timeout(Duration::from_secs(5))
            .assert()
            .success()
    })
    .await
    .unwrap();

    let expected = r#"
+---------------+--------------------+-------------+------------+
| table_catalog | table_schema       | table_name  | table_type |
+---------------+--------------------+-------------+------------+
| datafusion    | information_schema | columns     | VIEW       |
| datafusion    | information_schema | df_settings | VIEW       |
| datafusion    | information_schema | parameters  | VIEW       |
| datafusion    | information_schema | routines    | VIEW       |
| datafusion    | information_schema | schemata    | VIEW       |
| datafusion    | information_schema | tables      | VIEW       |
| datafusion    | information_schema | views       | VIEW       |
| test          | information_schema | columns     | VIEW       |
| test          | information_schema | df_settings | VIEW       |
| test          | information_schema | parameters  | VIEW       |
| test          | information_schema | routines    | VIEW       |
| test          | information_schema | schemata    | VIEW       |
| test          | information_schema | tables      | VIEW       |
| test          | information_schema | views       | VIEW       |
+---------------+--------------------+-------------+------------+
"#;

    assert.stdout(contains_str(expected));

    fixture.shutdown_and_wait().await;
}
