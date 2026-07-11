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

//! `interfaces` table function: lists the system's network capture
//! interfaces, one row per interface, similar to `tshark -D`. Useful for
//! discovering what to pass to the `capture` table function:
//!
//! ```sql
//! SELECT name, description, addresses FROM interfaces() WHERE is_up;
//! ```
//!
//! Listing interfaces does not require elevated privileges (unlike opening
//! one for capture).

use std::sync::Arc;

use datafusion::{
    arrow::{
        array::{BooleanBuilder, ListBuilder, RecordBatch, StringBuilder},
        datatypes::{DataType, Field, Schema, SchemaRef},
    },
    catalog::{MemTable, TableFunctionImpl, TableProvider},
    common::{plan_err, DataFusionError, Result},
    prelude::Expr,
};
use pcap::ConnectionStatus;

/// Schema of the `interfaces` table function: one row per interface
pub fn interfaces_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, false),
        Field::new("description", DataType::Utf8, true),
        Field::new(
            "addresses",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            true,
        ),
        Field::new("is_up", DataType::Boolean, true),
        Field::new("is_running", DataType::Boolean, true),
        Field::new("is_loopback", DataType::Boolean, true),
        Field::new("is_wireless", DataType::Boolean, true),
        Field::new("connection_status", DataType::Utf8, true),
    ]))
}

/// Table function that lists the system's network capture interfaces
#[derive(Debug, Default)]
pub struct InterfacesFunc {}

impl TableFunctionImpl for InterfacesFunc {
    fn call(&self, exprs: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        if !exprs.is_empty() {
            return plan_err!("interfaces takes no arguments");
        }
        let schema = interfaces_schema();
        let batch = interfaces_batch(&schema)?;
        let table = MemTable::try_new(schema, vec![vec![batch]])?;
        Ok(Arc::new(table))
    }
}

/// Falls back to a static, human-readable description for well-known
/// interface name patterns when `pcap` doesn't supply one itself (libpcap
/// leaves `desc` empty for most interfaces on macOS).
fn static_description(name: &str) -> Option<&'static str> {
    let prefix = name.trim_end_matches(|c: char| c.is_ascii_digit());
    match prefix {
        "lo" => Some("Loopback interface"),
        "en" => Some("Ethernet or Wi-Fi adapter"),
        "utun" => Some("Utility tunnel interface (VPN, Back to My Mac, etc.)"),
        "ap" => Some("Wi-Fi Access Point (Personal Hotspot / Instant Hotspot)"),
        "awdl" => Some("Apple Wireless Direct Link (AirDrop, AirPlay)"),
        "llw" => Some("Low-Latency WLAN interface (AWDL companion interface)"),
        "anpi" => Some("Apple Network Platform Interface (Wi-Fi/Bluetooth coexistence)"),
        "bridge" => Some("Virtual bridge interface"),
        "gif" => Some("Generic tunnel interface (IPv6-in-IPv4)"),
        "stf" => Some("6to4 tunnel interface"),
        "vmnet" => Some("Virtual Machine network interface"),
        "vnic" => Some("Parallels/VMware virtual network interface"),
        "ppp" => Some("Point-to-Point Protocol interface"),
        "wlan" => Some("Wireless LAN interface"),
        "eth" => Some("Ethernet interface"),
        "docker" => Some("Docker virtual bridge interface"),
        "veth" => Some("Virtual Ethernet interface"),
        _ => None,
    }
}

/// Builds the single record batch of interfaces from `pcap`'s device list
fn interfaces_batch(schema: &SchemaRef) -> Result<RecordBatch> {
    let devices = pcap::Device::list().map_err(|e| {
        DataFusionError::External(format!("interfaces failed to list devices: {e}").into())
    })?;

    let mut name = StringBuilder::new();
    let mut description = StringBuilder::new();
    let mut addresses = ListBuilder::new(StringBuilder::new());
    let mut is_up = BooleanBuilder::new();
    let mut is_running = BooleanBuilder::new();
    let mut is_loopback = BooleanBuilder::new();
    let mut is_wireless = BooleanBuilder::new();
    let mut connection_status = StringBuilder::new();

    for device in devices {
        name.append_value(&device.name);
        description.append_option(
            device
                .desc
                .as_deref()
                .or_else(|| static_description(&device.name)),
        );
        for address in &device.addresses {
            addresses.values().append_value(address.addr.to_string());
        }
        addresses.append(true);
        is_up.append_value(device.flags.is_up());
        is_running.append_value(device.flags.is_running());
        is_loopback.append_value(device.flags.is_loopback());
        is_wireless.append_value(device.flags.is_wireless());
        connection_status.append_value(match device.flags.connection_status {
            ConnectionStatus::Unknown => "unknown",
            ConnectionStatus::Connected => "connected",
            ConnectionStatus::Disconnected => "disconnected",
            ConnectionStatus::NotApplicable => "not_applicable",
        });
    }

    Ok(RecordBatch::try_new(
        Arc::clone(schema),
        vec![
            Arc::new(name.finish()),
            Arc::new(description.finish()),
            Arc::new(addresses.finish()),
            Arc::new(is_up.finish()),
            Arc::new(is_running.finish()),
            Arc::new(is_loopback.finish()),
            Arc::new(is_wireless.finish()),
            Arc::new(connection_status.finish()),
        ],
    )?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::SessionContext;

    #[test]
    fn test_call_rejects_arguments() {
        let func = InterfacesFunc::default();
        let args = vec![Expr::Literal(
            datafusion::scalar::ScalarValue::Utf8(Some("en0".to_string())),
            None,
        )];
        let err = func.call(&args).unwrap_err();
        assert!(err.to_string().contains("takes no arguments"));
    }

    #[tokio::test]
    async fn test_interfaces_lists_devices() {
        let ctx = SessionContext::new();
        ctx.register_udtf("interfaces", Arc::new(InterfacesFunc::default()));
        let batches = ctx
            .sql("SELECT name, is_loopback, connection_status FROM interfaces()")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        // Any host running the tests has at least one interface
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert!(rows >= 1, "expected at least one interface");
    }

    #[tokio::test]
    async fn test_interfaces_projection_and_filter() {
        let ctx = SessionContext::new();
        ctx.register_udtf("interfaces", Arc::new(InterfacesFunc::default()));
        // Projection, filtering, and unnesting the address list all compose
        let batches = ctx
            .sql(
                "SELECT name, unnest(addresses) AS address \
                 FROM interfaces() WHERE is_up ORDER BY name",
            )
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        // No assertion on contents: an interface may legitimately have no
        // addresses, this only must not error
        let _ = batches;
    }
}
