# Features

## Internal Optional Features (Workspace Features)

`dft` incubates several optional features in it's `crates` directory.  This provides us with the ability to quickly iterate on new features and test them in the main application while at the same time making it easy to export them to their own crates when they are ready.

### Parquet Functions (`--features=functions-parquet`)

Includes functions from [datafusion-function-parquet] for querying Parquet files in DataFusion in `dft`.  For example:

```sql
SELECT * FROM parquet_metadata('my_parquet_file.parquet')
```

### WASM UDF Functions (`--features=udfs-wasm`)

Adds the ability to register WASM UDFs. Currently two different input types are supported:

1. Row => WASM native types only (`Int32`, `Int64`, `Float32`, or `Float64`) and the UDF is called once per row.
2. ArrowIpc => The input `ColumnarValue`'s are serialized to Arrow's IPC format and written to the WASM module's linear memory.

More details can be found in [datafusion-udfs-wasm](https://github.com/datafusion-contrib/datafusion-dft/tree/main/crates/datafusion-udfs-wasm).

```toml
[[execution]]
module_functions = {
    "/path/to/wasm" = [
        {
            name = "funcName1",
            input_data_type = "Row",
            input_types = [
                "Int32",
                "Int64",
            ],
            return_type = "Int32"
        },
        {
            name = "funcName2",
            input_data_type = "ArrowIpc",
            input_types = [
                "Float32",
                "Float64",
            ],
            return_type = "Int32"
        }

    ]
}
```

### WebSocket (`--features=websocket`)

Adds a `websocket` table function that connects to a WebSocket endpoint and streams received messages as rows with the schema (`received_at` Timestamp, `message` Utf8).  The first argument is the connection URL (`ws://` or `wss://`) and any remaining arguments are messages sent after the connection is established (for example, subscription messages).

```sql
SELECT * FROM websocket('wss://stream.example.com/ws', '{"op":"subscribe","channel":"trades"}') LIMIT 10
```

The source is unbounded: without a `LIMIT` (or an aggregation that can complete) the query streams until the server closes the connection.  Note the CLI `--concat` flag collects all batches and therefore should not be used with unbounded queries.

## External Features

`dft` also has several external optional (conditionally compiled features) integrations which are controlled by [Rust Crate Features]

To build with all features, you can run 

```shell
cargo install --path . --all-features
````

[Rust Crate Features]: https://doc.rust-lang.org/cargo/reference/features.html


### S3 (`--features=s3`)

Mutliple s3 `ObjectStore`s can be registered, following the below model in your configuration file.

```toml
[[execution.object_store.s3]]
bucket_name = "my_bucket"
object_store_url = "s3://my_bucket"
aws_endpoint = "https://s3.amazonaws"
aws_access_key_id = "MY_ACCESS_KEY"
aws_secret_access_key = "MY SECRET"

[[execution.object_store.s3]]
bucket_name = "my_bucket"
object_store_url = "ny1://my_bucket"
aws_endpoint = "https://s3.amazonaws"
aws_access_key_id = "MY_ACCESS_KEY"
aws_secret_access_key = "MY SECRET"
```

Then you can run DDL such as 

```sql
CREATE EXTERNAL TABLE my_table STORED AS PARQUET LOCATION 's3://my_bucket/table';

CREATE EXTERNAL TABLE other_table STORED AS PARQUET LOCATION 'ny1://other_bucket/table';
```

### FlightSQL (`--features=flightsql`)

A separate editor for connecting to a FlightSQL server is provided.

The default `connection_url` is `http://localhost:50051` but this can be configured your config as well:

```toml
[flightsql]
connection_url = "http://myhost:myport"
```

### Deltalake (`--features=deltalake`)

Register deltalake tables.  For example:

```sql
CREATE EXTERNAL TABLE table_name STORED AS DELTATABLE LOCATION 's3://bucket/table'
```

### ClickHouse (`--features=clickhouse`)

Register an entire ClickHouse instance as a catalog (backed by [datafusion-table-providers]).  Each non-system database in the instance becomes a schema in the catalog and all of its tables are queryable.  For example use the following config:

```toml
[[execution.clickhouse]]
name = "clickhouse"                # catalog name to register (default "clickhouse")
url = "http://localhost:8123"
user = "admin"
password = "secret"
# database = "my_db"               # optionally limit the catalog to a single database
# compression = "lz4"              # transport compression: "lz4" or "none"

# Additional ClickHouse client settings applied to queries. The setting below returns
# ClickHouse String columns as Arrow Utf8 instead of Binary.
options = { output_format_arrow_string_as_string = "1" }
```

And then query with:

```sql
SELECT * FROM clickhouse.my_db.my_table
```

Multiple `[[execution.clickhouse]]` entries can be defined, each registered as its own catalog.  Databases and table names are discovered once at startup; the data itself is queried from ClickHouse on demand.

### MongoDB (`--features=mongodb`)

Register an entire MongoDB instance as a catalog (backed by [datafusion-table-providers]).  Each non-system database in the instance becomes a schema in the catalog and its collections are queryable as tables.  For example use the following config:

```toml
[[execution.mongodb]]
name = "mongodb"                   # catalog name to register (default "mongodb")
host = "localhost"
port = 27017
user = "admin"
password = "secret"
# database = "my_db"               # optionally limit the catalog to a single database

# Alternatively provide a full connection string (takes precedence over the fields above and
# limits the catalog to the database in the connection string)
# connection_string = "mongodb://admin:secret@localhost:27017/my_db?authSource=admin"

# Additional connection parameters passed to the underlying pool.  Note that TLS is
# required by default; use sslmode = "disabled" for local instances without TLS.
options = { auth_source = "admin", sslmode = "disabled" }
```

And then query with:

```sql
SELECT * FROM mongodb.my_db.my_collection
```

Multiple `[[execution.mongodb]]` entries can be defined, each registered as its own catalog.  Databases and collection names are discovered once at startup; the data itself is queried from MongoDB on demand.

Because MongoDB is schemaless, the Arrow schema of a collection is inferred by sampling documents (400 by default, configurable with `options = { schema_infer_max_records = "1000" }`) the first time the collection is used in a query.  Nested documents can be flattened into top-level columns with `options = { unnest_depth = "2" }`.

[datafusion-table-providers]: https://github.com/datafusion-contrib/datafusion-table-providers

### Json Functions (`--features=function-json`)

Adds functions from [datafusion-function-json] for querying JSON strings in DataFusion in `dft`.  For example:

```sql
select * from foo where json_get(attributes, 'bar')::string='ham'
(show examples of using operators too)
```

[datafusion-function-json]: https://github.com/datafusion-contrib/datafusion-functions-json

### HuggingFace (`--features=huggingface`)

Register tables from HuggingFace datasets.  For example use the following config:

```toml
[[execution.object_store.huggingface]]
repo_type = "dataset"
repo_id = "HuggingFaceTB/finemath"
revision = "main"
```

and then you can register external tables like so:

```sql
CREATE EXTERNAL TABLE hf4 STORED AS PARQUET LOCATION 'hf://HuggingFaceTB-finemath/finemath-3plus/';
```

The "/" in the `repo_id` is replaced with a "-" for the base url that is registered with DataFusion to work better with its path parsing.


