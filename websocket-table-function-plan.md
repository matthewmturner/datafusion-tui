# WebSocket Table Function (`web_socket`)

## Context

Add a table function so users can query a live WebSocket feed with SQL:

```sql
SELECT * FROM web_socket('wss://abc.com', 'msg1', 'msg2', ...)
```

Arg 1 is the connection URL (`ws://` or `wss://`); the remaining variadic string args are messages sent right after the connection is established (e.g. subscription messages). Rows stream in as messages arrive — an **unbounded** source. This works naturally with the existing execution paths: the TUI pulls batches lazily (`src/tui/execution.rs` `next_batch`), and the CLI default print path streams batch-by-batch (`--concat` collects and would hang — documented, not fixed).

**Schema (user-confirmed):** `received_at` Timestamp(Millisecond) + `message` Utf8, one row (batch) per message. Row construction lives in one helper so the schema is trivially adjustable later.

## Key existing patterns to reuse

- UDTF template: `crates/datafusion-functions-parquet/src/lib.rs:149` (`TableFunctionImpl::call` parsing `Expr::Literal(ScalarValue::Utf8..)` / `Expr::Column` for double-quoted strings)
- Registration site: `crates/datafusion-app/src/local.rs:114-121` (`session_ctx.register_udtf`), feature-gated blocks just above at lines 103-112
- Custom provider/exec template: `crates/datafusion-app/src/tables/map_table.rs` (`TableProvider` + `ExecutionPlan` + `PlanProperties` + `DisplayAs`)
- I/O runtime bridge: `crates/datafusion-app/src/executor/io.rs` — dedicated-executor CPU runtime has **no tokio I/O driver** (`local.rs:94` never calls `enable_io`), but its threads register the main runtime's handle via `on_thread_start(register_io_runtime)` (`dedicated.rs:174`). The socket task must be spawned on that handle when present.
- Test harness: `tests/extension_cases/mod.rs` `TestExecution` (feature-gated modules); background-server pattern in `src/test_utils/fixture.rs`

Verified against DataFusion 51: `RecordBatchReceiverStream::builder(schema, capacity)` with `tx()/spawn()/spawn_on()/build()`, and `Boundedness::Unbounded { requires_infinite_memory }` in `datafusion::physical_plan::execution_plan`.

## Steps

### 1. Dependencies + features

- `crates/datafusion-app/Cargo.toml`:
  - `[dependencies]` add `tokio-tungstenite = { features = ["rustls-tls-native-roots"], optional = true, version = "0.27" }` (rustls + rustls-native-certs already in Cargo.lock; needed for `wss://`; verify resolved version)
  - `[features]` add `websocket = ["dep:tokio-tungstenite"]`
- Root `Cargo.toml`:
  - `[features]` add `websocket = ["datafusion-app/websocket"]`
  - `[dev-dependencies]` add `tokio-tungstenite = "0.27"` (mock server, `ws://` only — no TLS features)
  - Fix stale comment above `[features]`: CI matrix is `.github/workflows/test.yml`, not `rust.yml`
- Keep TOML taplo-formatted (`taplo format --check`)

### 2. Non-panicking IO-handle accessor

`crates/datafusion-app/src/executor/io.rs` — add alongside `spawn_io`:

```rust
/// Returns the IO runtime handle registered for this thread, if any
pub fn io_runtime_handle() -> Option<Handle> {
    IO_RUNTIME.with_borrow(|h| h.clone())
}
```

(`spawn_io` panics when unregistered; the default config has `dedicated_executor_enabled = false`, so a fallback to the current runtime is required.)

### 3. New module `crates/datafusion-app/src/tables/web_socket.rs`

ASF license header. Declare in `tables/mod.rs`:

```rust
#[cfg(feature = "websocket")]
pub mod web_socket;
```

Contents:

- **`web_socket_schema()`** → `Schema { received_at: Timestamp(Millisecond, None) NOT NULL, message: Utf8 NOT NULL }`
- **`WebSocketFunc`** (`#[derive(Debug, Default)]`) implementing `TableFunctionImpl::call`:
  - empty args → `plan_err!("web_socket requires at least one argument, the connection URL")`
  - arg 0 must start with `ws://`/`wss://` else plan_err
  - remaining args parsed by a shared `expr_to_string` helper accepting `Utf8 | Utf8View | LargeUtf8` literals and `Expr::Column` (double-quote quirk), else plan_err
  - returns `Arc<WebSocketTable>`
- **`WebSocketTable`** (`url`, `messages`, `schema`) implementing `TableProvider` (`TableType::Base`); `scan(&self, _state, projection, _filters, limit)` → `WebSocketExec::try_new(url, messages, schema, projection.cloned(), limit)`
- **`WebSocketExec`**: fields url/messages/full schema/projection/projected_schema (`project_schema`)/limit/`cache: PlanProperties`
  - `compute_properties`: `Partitioning::UnknownPartitioning(1)`, `EmissionType::Incremental`, `Boundedness::Unbounded { requires_infinite_memory: false }`
  - `DisplayAs`: `WebSocketExec: url=.., messages=<count>, limit=..`; `children()` empty; `with_new_children` like `MapExec`
  - `execute(partition, _ctx)`: error on partition != 0; `RecordBatchReceiverStream::builder(projected_schema, 16)`; spawn `read_web_socket(...)` via `builder.spawn_on(task, &handle)` when `io_runtime_handle()` returns Some, else `builder.spawn(task)`. No connection happens at plan/EXPLAIN time — only in `execute()`.
- **`read_web_socket` task**:
  - `tokio_tungstenite::connect_async(&url)`, then `ws.send(Message::text(..))` for each extra arg; connect/send errors are forwarded through `tx` as `Err(DataFusionError::External(..))` so they surface as stream errors
  - loop on `ws.next()`: `Text` → row; `Binary` → lossy-UTF8 row; `Close` → break (graceful EOS); `Ping/Pong/Frame` → continue (tungstenite auto-queues pong on read); read error → send Err, break
  - `tx.send(Ok(batch))` failing means the consumer dropped the stream (e.g. LIMIT satisfied) → break
  - self-enforce `limit`: after producing `limit` rows, `ws.close(None)` and break (prompt socket close; DataFusion's limit + `RecordBatchReceiverStream`'s JoinSet abort-on-drop is the backstop)
- **`message_to_batch(schema, projection, text)`**: builds `TimestampMillisecondArray` (from `SystemTime::now()`) + `StringArray`, then `batch.project(proj)` if projection set

### 4. Registration

`crates/datafusion-app/src/local.rs` (after the parquet UDTFs, ~line 121):

```rust
#[cfg(feature = "websocket")]
session_ctx.register_udtf(
    "web_socket",
    Arc::new(crate::tables::web_socket::WebSocketFunc::default()),
);
```

### 5. Tests

**Unit tests** (in-module `#[cfg(test)]`): zero args, `http://` scheme, non-string message arg → plan errors; exec properties assert `boundedness.is_unbounded()` + 1 partition; optional EXPLAIN test (no connection at plan time).

**Integration tests** — new `tests/extension_cases/websocket.rs`, registered in `tests/extension_cases/mod.rs` behind `#[cfg(feature = "websocket")]`:

- Mock server helper: bind `TcpListener` on `127.0.0.1:0`, spawn task that `accept_async`s, waits for the subscription message, sends N `msg-{i}` messages, then either closes or holds the connection open (`futures::future::pending`)
- Each test wrapped in `tokio::time::timeout(30s, ..)`:
  1. **Close frame ends stream**: server sends 3 then closes; `SELECT message FROM web_socket('ws://{addr}', 'subscribe')` via `TestExecution::run` (safe — close terminates the stream); assert 3 rows
  2. **LIMIT stops unbounded stream**: server never closes; `... LIMIT 2`; assert 2 rows (exercises drop → JoinSet abort → socket close)
  3. **Bad URL** → plan error mentioning `ws://`/`wss://`

**CI**: add a `test-websocket` job to `.github/workflows/test.yml` cloned from the Functions-JSON job (~lines 229-258) running `cargo test --features=websocket extension_cases::websocket`. (`checks.yml` clippy `--all-features` covers linting; datafusion-app `--all-features` test job picks up unit tests automatically.)

### 6. Docs

`docs/features.md` — new section following existing style, e.g.:

```markdown
### WebSocket (`--features=websocket`)
Adds a `web_socket` table function that connects to a WebSocket endpoint and streams
received messages as rows (`received_at`, `message`). First argument is the URL
(`ws://`/`wss://`); remaining arguments are messages sent after connecting.
The source is unbounded: without a LIMIT the query streams until the server closes
the connection. The CLI `--concat` flag collects all batches and should not be used
with unbounded queries.
```

Mention `websocket` where README.md lists installable features (~line 60).

## Verification

```bash
cargo check --features=websocket
cargo clippy --all-features --workspace -- -D warnings
cargo test -p datafusion-app --features=websocket
cargo test --features=websocket extension_cases::websocket
taplo format --check
```

Manual smoke test against a live feed:

```bash
cargo run --features=websocket -- -c "SELECT * FROM web_socket('wss://stream.binance.com:9443/ws/btcusdt@trade') LIMIT 3"
```

And in the TUI (`cargo run --features=websocket,tui`), run the same query without LIMIT to watch rows arrive incrementally via lazy batch pulls.

## Risks

1. **Runtime reactor** (main one): mitigated by `io_runtime_handle()` + `spawn_on` fallback; without it, enabling `dedicated_executor_enabled` panics inside tungstenite ("IO is disabled").
2. **`--concat` / collecting consumers hang on unbounded queries** — pre-existing behavior class; documented.
3. **tokio-tungstenite API drift**: `Message::Text` carries `Utf8Bytes` in ≥0.24 (use `t.to_string()`); confirm against the resolved version.
4. Binary frames are lossy-UTF8'd for now — single match arm to change if skip-with-log is preferred.
