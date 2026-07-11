# datafusion-net

DataFusion table functions for querying network packet captures with SQL,
similar to wireshark / tshark.

## Table functions

### `pcap(path)`

Reads a pcap or pcapng capture file as a table:

```sql
SELECT src_ip, dst_ip, protocol, length
FROM pcap('capture.pcap')
WHERE dst_port = 443;
```

### `capture(interface [, bpf_filter [, duration_secs]])`

Requires the `live` feature (links against libpcap). Streams live-captured
packets from a network interface:

```sql
-- Stream until 100 packets have been captured
SELECT * FROM capture('en0') LIMIT 100;

-- Apply a BPF filter
SELECT * FROM capture('en0', 'tcp port 443') LIMIT 10;

-- Capture for 10 seconds; the stream terminates, so aggregations work
SELECT src_ip, count(*) FROM capture('en0', '', 10) GROUP BY src_ip;
```

Without a duration the source is unbounded: use a `LIMIT` or the query
streams until cancelled. Live capture requires elevated privileges (sudo,
`cap_net_raw`+`cap_net_admin` on Linux, or ChmodBPF on macOS).

## Schema

One row per captured frame. Layers that cannot be decoded (non-IP traffic,
truncated frames, unknown link types) yield null columns rather than errors.

| Column           | Type                     | Notes                                    |
| ---------------- | ------------------------ | ---------------------------------------- |
| `timestamp`      | `Timestamp(Microsecond)` | Capture timestamp                        |
| `frame_number`   | `UInt64`                 | 1-based position in the capture          |
| `length`         | `UInt32`                 | Original frame length on the wire        |
| `capture_length` | `UInt32`                 | Bytes actually captured                  |
| `eth_src`        | `Utf8`                   | Source MAC                               |
| `eth_dst`        | `Utf8`                   | Destination MAC                          |
| `ethertype`      | `Utf8`                   | `ipv4`, `ipv6`, `arp`, or hex value      |
| `vlan`           | `UInt16`                 | VLAN identifier (outer tag if double)    |
| `ip_version`     | `UInt8`                  | 4 or 6                                   |
| `src_ip`         | `Utf8`                   |                                          |
| `dst_ip`         | `Utf8`                   |                                          |
| `ttl`            | `UInt8`                  | Hop limit for IPv6                       |
| `protocol`       | `Utf8`                   | `tcp`, `udp`, `icmp`, `icmpv6`, ...      |
| `src_port`       | `UInt16`                 |                                          |
| `dst_port`       | `UInt16`                 |                                          |
| `tcp_flags`      | `Utf8`                   | e.g. `SYN\|ACK`                          |
| `tcp_seq`        | `UInt32`                 |                                          |
| `tcp_ack`        | `UInt32`                 |                                          |
| `payload_length` | `UInt32`                 | Transport payload bytes                  |
| `payload`        | `Binary`                 | Only materialized when projected         |

## Usage

```rust,no_run
use std::sync::Arc;
use datafusion::prelude::SessionContext;

let ctx = SessionContext::new();
ctx.register_udtf("pcap", Arc::new(datafusion_net::PcapFunc::default()));
// With the `live` feature:
// ctx.register_udtf("capture", Arc::new(datafusion_net::CaptureFunc::default()));
```
