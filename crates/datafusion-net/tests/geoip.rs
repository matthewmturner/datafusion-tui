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

//! Tests for the `geoip` scalar UDF, run against a minimal MaxMind DB
//! (`.mmdb`) file written from scratch below so the tests are offline and
//! carry no data licensing baggage. The database maps the single network
//! `1.1.1.0/24` to a Sydney, Australia city record.

use std::path::Path;

use datafusion::{logical_expr::ScalarUDF, prelude::SessionContext};
use datafusion_net::GeoIpUdf;

// ---------------------------------------------------------------------------
// Minimal MaxMind DB writer
//
// The format (https://maxmind.github.io/MaxMind-DB/) is a binary search tree
// over address bits, a 16-zero-byte separator, a data section, and metadata
// after a marker. Field encoding: a control byte carries the type in the top
// 3 bits (0 = extended: real type is 7 + the following byte) and the payload
// size in the low 5 bits.
// ---------------------------------------------------------------------------

/// One tree node per bit of the 1.1.1.0/24 prefix
const NODE_COUNT: u32 = 24;
/// The network encoded in the search tree
const PREFIX: [u8; 3] = [1, 1, 1];

fn control(type_num: u8, size: usize) -> u8 {
    assert!(size < 29, "sizes needing extra length bytes not supported");
    (type_num << 5) | size as u8
}

fn write_string(out: &mut Vec<u8>, s: &str) {
    out.push(control(2, s.len()));
    out.extend_from_slice(s.as_bytes());
}

fn write_double(out: &mut Vec<u8>, value: f64) {
    out.push(control(3, 8));
    out.extend_from_slice(&value.to_be_bytes());
}

fn write_map_header(out: &mut Vec<u8>, entries: usize) {
    out.push(control(7, entries));
}

/// Writes a uint16 (type 5) or uint32 (type 6) with minimal-length encoding
fn write_uint(out: &mut Vec<u8>, type_num: u8, value: u32) {
    let bytes = value.to_be_bytes();
    let skip = bytes.iter().take_while(|b| **b == 0).count();
    out.push(control(type_num, bytes.len() - skip));
    out.extend_from_slice(&bytes[skip..]);
}

/// Writes a uint64 (extended type 9)
fn write_uint64(out: &mut Vec<u8>, value: u64) {
    let bytes = value.to_be_bytes();
    let skip = bytes.iter().take_while(|b| **b == 0).count();
    out.push(control(0, bytes.len() - skip));
    out.push(9 - 7);
    out.extend_from_slice(&bytes[skip..]);
}

/// Writes an array (extended type 11) header
fn write_array_header(out: &mut Vec<u8>, entries: usize) {
    out.push(control(0, entries));
    out.push(11 - 7);
}

/// The single city record, at offset 0 of the data section
fn city_record() -> Vec<u8> {
    let mut d = Vec::new();
    write_map_header(&mut d, 3);
    write_string(&mut d, "city");
    write_map_header(&mut d, 1);
    write_string(&mut d, "names");
    write_map_header(&mut d, 1);
    write_string(&mut d, "en");
    write_string(&mut d, "Sydney");
    write_string(&mut d, "country");
    write_map_header(&mut d, 2);
    write_string(&mut d, "iso_code");
    write_string(&mut d, "AU");
    write_string(&mut d, "names");
    write_map_header(&mut d, 1);
    write_string(&mut d, "en");
    write_string(&mut d, "Australia");
    write_string(&mut d, "location");
    write_map_header(&mut d, 3);
    write_string(&mut d, "latitude");
    write_double(&mut d, -33.8688);
    write_string(&mut d, "longitude");
    write_double(&mut d, 151.2093);
    write_string(&mut d, "time_zone");
    write_string(&mut d, "Australia/Sydney");
    d
}

/// A chain of 24 nodes routing the bits of [`PREFIX`]. At each node the
/// on-prefix branch continues to the next node (the final one points at data
/// offset 0, encoded as `NODE_COUNT + 16`); the off-prefix branch is the
/// "no data" sentinel `NODE_COUNT`. Record size 24 bits: each node is two
/// 3-byte big-endian records.
fn search_tree() -> Vec<u8> {
    let mut tree = Vec::new();
    for i in 0..NODE_COUNT as usize {
        let bit = (PREFIX[i / 8] >> (7 - i % 8)) & 1;
        let matched = if i == NODE_COUNT as usize - 1 {
            NODE_COUNT + 16
        } else {
            i as u32 + 1
        };
        let (left, right) = if bit == 0 {
            (matched, NODE_COUNT)
        } else {
            (NODE_COUNT, matched)
        };
        tree.extend_from_slice(&left.to_be_bytes()[1..]);
        tree.extend_from_slice(&right.to_be_bytes()[1..]);
    }
    tree
}

fn metadata() -> Vec<u8> {
    let mut m = Vec::new();
    write_map_header(&mut m, 9);
    write_string(&mut m, "binary_format_major_version");
    write_uint(&mut m, 5, 2);
    write_string(&mut m, "binary_format_minor_version");
    write_uint(&mut m, 5, 0);
    write_string(&mut m, "build_epoch");
    write_uint64(&mut m, 1_735_689_600); // 2025-01-01T00:00:00Z
    write_string(&mut m, "database_type");
    write_string(&mut m, "GeoLite2-City");
    write_string(&mut m, "description");
    write_map_header(&mut m, 1);
    write_string(&mut m, "en");
    write_string(&mut m, "Test database");
    write_string(&mut m, "ip_version");
    write_uint(&mut m, 5, 4);
    write_string(&mut m, "languages");
    write_array_header(&mut m, 1);
    write_string(&mut m, "en");
    write_string(&mut m, "node_count");
    write_uint(&mut m, 6, NODE_COUNT);
    write_string(&mut m, "record_size");
    write_uint(&mut m, 5, 24);
    m
}

fn write_test_mmdb(path: &Path) {
    let mut buf = search_tree();
    buf.extend_from_slice(&[0u8; 16]); // data section separator
    buf.extend_from_slice(&city_record());
    buf.extend_from_slice(b"\xab\xcd\xefMaxMind.com"); // metadata marker
    buf.extend_from_slice(&metadata());
    std::fs::write(path, buf).unwrap();
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// Returns a context with `geoip` registered plus the tempdir holding the
/// test database and the database path
async fn setup() -> (SessionContext, tempfile::TempDir, String) {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("test.mmdb");
    write_test_mmdb(&db);
    let ctx = SessionContext::new();
    ctx.register_udf(ScalarUDF::from(GeoIpUdf::default()));
    let db = db.display().to_string();
    (ctx, dir, db)
}

async fn run(ctx: &SessionContext, sql: &str) -> String {
    let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
    datafusion::arrow::util::pretty::pretty_format_batches(&batches)
        .unwrap()
        .to_string()
}

#[tokio::test]
async fn test_geoip_hit() {
    let (ctx, _dir, db) = setup().await;
    let sql = format!(
        "SELECT geoip('1.1.1.1', '{db}')['country_code'] AS country_code, \
                geoip('1.1.1.1', '{db}')['country'] AS country, \
                geoip('1.1.1.1', '{db}')['city'] AS city, \
                geoip('1.1.1.1', '{db}')['latitude'] AS latitude, \
                geoip('1.1.1.1', '{db}')['longitude'] AS longitude, \
                geoip('1.1.1.1', '{db}')['time_zone'] AS time_zone"
    );
    let out = run(&ctx, &sql).await;
    assert!(out.contains("AU"), "missing country code in:\n{out}");
    assert!(out.contains("Australia"), "missing country in:\n{out}");
    assert!(out.contains("Sydney"), "missing city in:\n{out}");
    assert!(out.contains("-33.8688"), "missing latitude in:\n{out}");
    assert!(out.contains("151.2093"), "missing longitude in:\n{out}");
    assert!(
        out.contains("Australia/Sydney"),
        "missing time zone in:\n{out}"
    );
}

#[tokio::test]
async fn test_geoip_whole_network_matches() {
    let (ctx, _dir, db) = setup().await;
    // Any address in the encoded 1.1.1.0/24 network hits the same record
    let sql = format!("SELECT geoip('1.1.1.42', '{db}')['city'] = 'Sydney' AS same_record");
    let out = run(&ctx, &sql).await;
    assert!(out.contains("true"), "expected a hit in:\n{out}");
}

#[tokio::test]
async fn test_geoip_misses_are_null() {
    let (ctx, _dir, db) = setup().await;
    // An address outside the network, an unparseable address, a NULL
    // address, and an IPv6 address in an IPv4-only database are all NULL
    let sql = format!(
        "SELECT geoip('9.9.9.9', '{db}') IS NULL AS missing, \
                geoip('not-an-ip', '{db}') IS NULL AS unparseable, \
                geoip(NULL, '{db}') IS NULL AS null_input, \
                geoip('::1', '{db}') IS NULL AS wrong_ip_version"
    );
    let out = run(&ctx, &sql).await;
    // Four columns, all true
    assert_eq!(
        out.matches("true").count(),
        4,
        "expected all NULL in:\n{out}"
    );
}

#[tokio::test]
async fn test_geoip_invalid_ip_does_not_open_db() {
    let (ctx, _dir, _db) = setup().await;
    // The address is parsed before the database path is resolved, so an
    // unparseable address is NULL even when the database does not exist
    let out = run(
        &ctx,
        "SELECT geoip('not-an-ip', '/definitely/missing.mmdb') IS NULL AS missing",
    )
    .await;
    assert!(out.contains("true"), "expected NULL in:\n{out}");
}

#[tokio::test]
async fn test_geoip_unopenable_db_errors() {
    let (ctx, _dir, _db) = setup().await;
    let result = ctx
        .sql("SELECT geoip('1.1.1.1', '/definitely/missing.mmdb')")
        .await
        .unwrap()
        .collect()
        .await;
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("geoip failed to open database"),
        "unexpected error: {err}"
    );
}

#[tokio::test]
async fn test_geoip_default_db_from_constructor_and_missing_config_errors() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("test.mmdb");
    write_test_mmdb(&db);

    // With a default database the single-argument form works
    let ctx = SessionContext::new();
    ctx.register_udf(ScalarUDF::from(GeoIpUdf::with_db_path(&db)));
    let out = run(&ctx, "SELECT geoip('1.1.1.1')['city'] AS city").await;
    assert!(out.contains("Sydney"), "expected a hit in:\n{out}");

    // Without one (and without the GEOIP_DB environment variable, which the
    // test environment must not set) it is an error pointing at the fix
    if std::env::var_os(datafusion_net::GEOIP_DB_ENV_VAR).is_none() {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(GeoIpUdf::default()));
        let result = ctx
            .sql("SELECT geoip('1.1.1.1')")
            .await
            .unwrap()
            .collect()
            .await;
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("geoip has no database configured"),
            "unexpected error: {err}"
        );
    }
}

#[tokio::test]
async fn test_geoip_over_column() {
    let (ctx, _dir, db) = setup().await;
    // Exercise the array (non-literal) code path over a column of addresses
    let sql = format!(
        "SELECT ip, geoip(ip, '{db}')['country_code'] AS cc \
         FROM (VALUES ('1.1.1.1'), ('1.1.1.200'), ('8.8.8.8'), ('nope')) AS t(ip) \
         ORDER BY ip"
    );
    let out = run(&ctx, &sql).await;
    let hits = out.matches("AU").count();
    assert_eq!(hits, 2, "expected two hits in:\n{out}");
}
