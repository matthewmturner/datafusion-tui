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

//! DataFusion table functions for querying network packet captures with SQL,
//! similar to wireshark/tshark.
//!
//! - [`PcapFunc`] (`pcap`): reads a pcap/pcapng capture file as a table
//! - [`CaptureFunc`] (`capture`, requires the `live` feature): streams
//!   live-captured packets from a network interface
//!
//! ```sql
//! -- Query a capture file
//! SELECT src_ip, dst_ip, protocol FROM pcap('capture.pcap') WHERE dst_port = 443;
//!
//! -- Stream a live capture (requires elevated privileges)
//! SELECT * FROM capture('en0', 'tcp port 443') LIMIT 100;
//! ```
//!
//! Both functions share the same schema (see [`packet_schema`]); frames that
//! cannot be decoded produce rows with null columns for the missing layers.
//!
//! ```rust,no_run
//! use std::sync::Arc;
//! use datafusion::prelude::SessionContext;
//!
//! let ctx = SessionContext::new();
//! ctx.register_udtf("pcap", Arc::new(datafusion_net::PcapFunc::default()));
//! ```

use datafusion::{
    common::{plan_err, Column, Result},
    prelude::Expr,
    scalar::ScalarValue,
};

pub mod decode;
mod file;
#[cfg(feature = "live")]
mod live;
mod schema;
mod udfs;
pub mod writer;

pub use file::{PcapFunc, PcapTable};
#[cfg(feature = "live")]
pub use live::{CaptureFunc, CaptureTable};
pub use schema::packet_schema;
pub use udfs::ReverseDnsUdf;

/// Extracts a string argument from a table function expression
pub(crate) fn expr_to_string(expr: &Expr, func: &str, what: &str) -> Result<String> {
    match expr {
        Expr::Literal(
            ScalarValue::Utf8(Some(s))
            | ScalarValue::Utf8View(Some(s))
            | ScalarValue::LargeUtf8(Some(s)),
            _,
        ) => Ok(s.clone()),
        // Double quoted strings are parsed as columns
        Expr::Column(Column { name, .. }) => Ok(name.clone()),
        _ => plan_err!("{func} {what} must be a string literal"),
    }
}
