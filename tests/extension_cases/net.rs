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

//! Tests for the `pcap` and `capture` table functions

use std::path::Path;

use datafusion_net::writer::PcapWriter;
use etherparse::PacketBuilder;

use crate::extension_cases::TestExecution;

const SRC_MAC: [u8; 6] = [0x02, 0x00, 0x00, 0x00, 0x00, 0x01];
const DST_MAC: [u8; 6] = [0x02, 0x00, 0x00, 0x00, 0x00, 0x02];
/// 2025-01-01T00:00:00Z
const BASE_TS_MICROS: i64 = 1_735_689_600_000_000;

/// Writes a small capture: a TCP handshake packet each way and a DNS-style
/// UDP query
fn write_test_pcap(path: &Path) {
    let file = std::fs::File::create(path).unwrap();
    let mut writer = PcapWriter::new(file, 1).unwrap();

    let builder = PacketBuilder::ethernet2(SRC_MAC, DST_MAC)
        .ipv4([10, 0, 0, 1], [10, 0, 0, 2], 64)
        .tcp(51000, 443, 1000, 1024)
        .syn();
    let mut frame = Vec::with_capacity(builder.size(0));
    builder.write(&mut frame, &[]).unwrap();
    writer.write_packet(BASE_TS_MICROS, &frame).unwrap();

    let builder = PacketBuilder::ethernet2(DST_MAC, SRC_MAC)
        .ipv4([10, 0, 0, 2], [10, 0, 0, 1], 64)
        .tcp(443, 51000, 2000, 1024)
        .syn()
        .ack(1001);
    let mut frame = Vec::with_capacity(builder.size(0));
    builder.write(&mut frame, &[]).unwrap();
    writer.write_packet(BASE_TS_MICROS + 500, &frame).unwrap();

    let builder = PacketBuilder::ethernet2(SRC_MAC, DST_MAC)
        .ipv4([10, 0, 0, 1], [8, 8, 8, 8], 64)
        .udp(53001, 53);
    let payload = b"dns query";
    let mut frame = Vec::with_capacity(builder.size(payload.len()));
    builder.write(&mut frame, payload).unwrap();
    writer.write_packet(BASE_TS_MICROS + 1000, &frame).unwrap();

    writer.flush().unwrap();
}

#[tokio::test]
async fn test_pcap_select() {
    let dir = tempfile::tempdir().unwrap();
    write_test_pcap(&dir.path().join("test.pcap"));
    let execution = TestExecution::new().await;
    let sql = format!(
        "SELECT frame_number, src_ip, dst_ip, protocol, src_port, dst_port, tcp_flags \
         FROM pcap('{}/test.pcap')",
        dir.path().display()
    );
    let output = execution.run_and_format(&sql).await;
    insta::assert_yaml_snapshot!(output, @r#"
    - +--------------+----------+----------+----------+----------+----------+-----------+
    - "| frame_number | src_ip   | dst_ip   | protocol | src_port | dst_port | tcp_flags |"
    - +--------------+----------+----------+----------+----------+----------+-----------+
    - "| 1            | 10.0.0.1 | 10.0.0.2 | tcp      | 51000    | 443      | SYN       |"
    - "| 2            | 10.0.0.2 | 10.0.0.1 | tcp      | 443      | 51000    | SYN|ACK   |"
    - "| 3            | 10.0.0.1 | 8.8.8.8  | udp      | 53001    | 53       |           |"
    - +--------------+----------+----------+----------+----------+----------+-----------+
    "#);
}

#[tokio::test]
async fn test_pcap_filter_and_aggregate() {
    let dir = tempfile::tempdir().unwrap();
    write_test_pcap(&dir.path().join("test.pcap"));
    let execution = TestExecution::new().await;
    let sql = format!(
        "SELECT protocol, count(*) AS packets, sum(length) AS bytes \
         FROM pcap('{}/test.pcap') WHERE dst_port = 443 GROUP BY protocol",
        dir.path().display()
    );
    let output = execution.run_and_format(&sql).await;
    insta::assert_yaml_snapshot!(output, @r#"
    - +----------+---------+-------+
    - "| protocol | packets | bytes |"
    - +----------+---------+-------+
    - "| tcp      | 1       | 54    |"
    - +----------+---------+-------+
    "#);
}

#[tokio::test]
async fn test_pcap_limit() {
    let dir = tempfile::tempdir().unwrap();
    write_test_pcap(&dir.path().join("test.pcap"));
    let execution = TestExecution::new().await;
    let sql = format!(
        "SELECT frame_number, protocol FROM pcap('{}/test.pcap') LIMIT 2",
        dir.path().display()
    );
    let output = execution.run_and_format(&sql).await;
    insta::assert_yaml_snapshot!(output, @r#"
    - +--------------+----------+
    - "| frame_number | protocol |"
    - +--------------+----------+
    - "| 1            | tcp      |"
    - "| 2            | tcp      |"
    - +--------------+----------+
    "#);
}

#[tokio::test]
async fn test_capture_explain_does_not_open_device() {
    let execution = TestExecution::new().await;
    // Planning must not open a capture device, which would fail without
    // elevated privileges (and this device does not exist anyway)
    let result = execution
        .run("EXPLAIN SELECT * FROM capture('definitely-not-a-device', 'tcp', 5)")
        .await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_reverse_dns_loopback() {
    let execution = TestExecution::new().await;
    // The loopback address must resolve to some hostname on any host with a
    // working resolver; the exact PTR record is environment dependent so we
    // only assert it is non-null
    let output = execution
        .run_and_format("SELECT reverse_dns('127.0.0.1') IS NOT NULL AS resolved")
        .await;
    insta::assert_yaml_snapshot!(output, @r#"
    - +----------+
    - "| resolved |"
    - +----------+
    - "| true     |"
    - +----------+
    "#);
}

#[tokio::test]
async fn test_reverse_dns_invalid_input_is_null() {
    let execution = TestExecution::new().await;
    // Unparseable input and a reserved address with no PTR record both yield
    // null rather than erroring the query
    let output = execution
        .run_and_format(
            "SELECT reverse_dns(ip) AS host FROM (VALUES ('not-an-ip'), ('192.0.2.1')) AS t(ip)",
        )
        .await;
    insta::assert_yaml_snapshot!(output, @r#"
    - +------+
    - "| host |"
    - +------+
    - "|      |"
    - "|      |"
    - +------+
    "#);
}

#[tokio::test]
async fn test_reverse_dns_over_pcap() {
    let dir = tempfile::tempdir().unwrap();
    write_test_pcap(&dir.path().join("test.pcap"));
    let execution = TestExecution::new().await;
    // reverse_dns composes with the pcap table function over the src_ip
    // column. The 10.0.0.0/8 addresses in the fixture will not resolve, but
    // the query must plan and execute successfully.
    let sql = format!(
        "SELECT DISTINCT src_ip, reverse_dns(src_ip) AS host FROM pcap('{}/test.pcap')",
        dir.path().display()
    );
    let result = execution.run(&sql).await;
    assert!(result.is_ok());
}
