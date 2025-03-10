# dft

`dft` provides two client interfaces to the [DataFusion](https://github.com/apache/arrow-datafusion) query execution engine:
1. Text User Interface (TUI): An IDE for DataFusion developers and users that provides a local database experience with utilities to analyze / benchmark queries.
2. Command Line Interface (CLI): Scriptable engine for executing queries from files.

Additionally, it provides a FlightSQL server implementation leveraging the same execution engine behind the TUI and CLI.  This allows users to iterate quickly develop a database and then seamlessly deploy it.

`dft` is inspired by  [`datafusion-cli`], but has some differences:
1. `dft` TUI focuses on more complete and interactive experience for users.
2. `dft` contains many built in integrations such as Delta Lake, Iceberg, and MySQL (Coming Soon) that are not available in `datafusion-cli`.

[`datafusion-cli`]: https://datafusion.apache.org/user-guide/cli/overview.html

## `dft` TUI

The objective of `dft` is to provide users with the experience of having their own local database that allows them to query and join data from disparate data sources all from the terminal.  

<table width="100%">
    <tr>
        <th>SQL & FlightSQL Editor and Results</th>
        <th>Query History and Stats</th>
    </tr>
    <tr>
        <td width="50%">
            <img width="1728" alt="image" src="https://github.com/user-attachments/assets/f9af22c2-665b-487b-bd8c-d714fc7c65d4">
        </td>
        <td width="50%">
            <img width="1728" alt="image" src="https://github.com/user-attachments/assets/e05a8ff3-17e8-4663-a297-5bb4ab19fdc7">
        </td>
    </tr>
    <tr>
        <th>Filterable Logs</th>
        <th>DataFusion Session Context Details</th>
    </tr>
    <tr>
        <td width="50%">
            <img width="1728" alt="image" src="https://github.com/user-attachments/assets/94861585-e1ca-481d-9b46-c1ced4976b9a">
        </td>
        <td width="50%">
            <img width="1728" alt="image" src="https://github.com/user-attachments/assets/0f272db1-3432-4dd7-9fb6-2e438db1b268">
        </td>
    </tr>
</table>

Some of the current and planned features are:

- Tab management to provide clean and structured organization of DataFusion queries, results, and context
  - SQL editor
    - Write query results to file (TODO)
  - Query history
    - History and statistics of executed queries
  - ExecutionContext information
    - Information from ExecutionContext / Catalog / ObjectStore / State / Config
  - Logs
    - Logs from `dft` and `DataFusion`
- Custom `ObjectStore` Support
  - S3, Azure(TODO), GCP(TODO)
  - `ObjectStore` explorer. I.e. able to list files in `ObjectStore`
- `TableProviderFactory` data sources
  - Deltalake
  - Iceberg
  - Hudi
- Preloading DDL from `~/.config/dft/ddl.sql` (or a user defined path) for local database available on startup
- Benchmarking local and FlightSQL queries with breakdown of query execution time
- "Catalog File" support - see [#122](https://github.com/datafusion-contrib/datafusion-tui/issues/122)
  - Save table definitions *and* data
  - Save parquet metadata from remote object stores

### Limitations

Currently `dft` does not display wide result sets well (because the widget library that we use does not support horizontal scrolling - we are working with them to see what we can do about this).  As a result, when working with wide data sets its best to be selective with the columns that you would like to view in the ouput.

## User Guide

### Installation

Currently, the only supported packaging is on [crates.io](https://crates.io/search?q=datafusion-dft).  If you already have Rust installed it can be installed by running `cargo install datafusion-tui`.  If rust is not installed you can download following the directions [here](https://www.rust-lang.org/tools/install).

Once installed you can run `dft` to start the application.

#### Configuration

`dft` is configuration through a TOML file with default location of `~/.config/dft/config.toml`.  Within the config there is a shared configuration that can be used across all apps and app specific configuration.  The shared and app specific configs are merged to come up with a final configuration that is used.  DataFusion's execution configuration can be fully customized as part of this.

The sections for configuring each app are shown below.
```toml
[shared]

[cli]

[tui]

[flightsql_client]

[flightsql_server]

```
  
## `dft` CLI

The `dft` CLI is a scriptable interface to the `tui` engine for executing queries from files or the command line. The CLI is used in a similar manner to `datafusion-cli` but with the added benefit of supporting multiple pre-integrated data sources.

### Example: Run the contents of `query.sql` 

```shell
$ dft -f query.sql
```

### Example: Run a query from the command line

```shell
$ dft -c "SELECT 1+2"
```

#### FlightSQL

Both of the commands above support the `--flightsql` parameter to run the SQL with your configured FlightSQL client.  You can also configure the host used for creating FlightSQL client per command with `--flightsql-host` - for example `--flightsql-host "http://127.0.0.1:50052"`.

##### Auth

Auth is built into the FlightSQL implementation so you can require a bearer token or username / password when running `dft` as a server or adds the relevant auth headers when using `dft` as a FlightSQL client.

```toml
[flightsql_server.auth]
bearer_token = "MyToken"
basic_auth.username = "User"
basic_auth.password = "Pass"

[flightsql_client.auth]
bearer_token = "MyToken"
basic_auth.username = "User"
basic_auth.password = "Pass"
```

#### DDL

The CLI can also run your configured DDL prior to executing the query by adding the `--run-ddl` parameter.

#### Benchmarking Queries
You can benchmark queries by adding the `--bench` parameter.  This will run the query a configurable number of times and output a breakdown of the queries execution time with summary statistics for each component of the query (logical planning, physical planning, execution time, and total time).

Optionally you can use the `--run-before` param to run a query before the benchmark is run.  This is useful in cases where you want to hit a temp table or write a file to disk that your benchmark query will use.

To save benchmark results to a file use the `--save` parameter with a file path.  Further, you can use the `--append` parameter to append to the file instead of overwriting it.

The number of benchmark iterations is defined in your configuration (default is 10) and can be configured per benchmark run with `-n` parameter.

#### Analyze Queries

The output from `EXPLAIN ANALYZE` provides a wealth of information on a queries execution - however, the amount of information and connecting the dots can be difficult and manual.  Further, there is detail in the `MetricSet`'s of the underlying `ExecutionPlan`'s that is lost in the output.

To help with this the `--analyze` flag can used to generate a summary of the underlying `ExecutionPlan` `MetricSet`s.  The summary presents the information in a way that is hopefully easier to understand and easier to draw conclusions on a query's performance.

This feature is still in it's early stages and is expected to evolve.  Once it has gone through enough real world testing and it has been confirmed the metrics make sense documentation will be added on the exact calculations - until then the source will need to be inspected to see the calculations.

## `dft` FlightSQL Server

The `dft` FlightSQL server (feature flag `flightsql`) is a Flight service that can be used to execute SQL queries against DataFusion.  The server is started by running `dft serve-flight-sql` and can optionally run your configured DDL with the `--run-ddl` parameter.  Prometheus metrics are automatically exported as part of this.

This feature is experimental and does not currently implement all FlightSQL endpoints.  Endpoints will be added in tandem with adding more features to the FlightSQL clients within the TUI and CLI.


#### Internal Optional Features (Workspace Features)

`dft` incubates several optional features in it's `crates` directory.  This provides us with the ability to quickly iterate on new features and test them in the main application while at the same time making it easy to export them to their own crates when they are ready.

##### Parquet Functions (`--features=functions-parquet`)

Includes functions from [datafusion-function-parquet] for querying Parquet files in DataFusion in `dft`.  For example:

```sql
SELECT * FROM parquet_metadata('my_parquet_file.parquet')
```

##### WASM UDF Functions (`--features=udfs-wasm`)

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

#### External Optional Features (Rust Crate Features)

`dft` also has several external optional (conditionally compiled features) integrations which are controlled by [Rust Crate Features]

To build with all features, you can run 

```shell
cargo install --path . --all-features
````

[Rust Crate Features]: https://doc.rust-lang.org/cargo/reference/features.html


##### S3 (`--features=s3`)

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

##### FlightSQL (`--features=flightsql`)

A separate editor for connecting to a FlightSQL server is provided.

The default `connection_url` is `http://localhost:50051` but this can be configured your config as well:

```toml
[flightsql]
connection_url = "http://myhost:myport"
```

##### Deltalake (`--features=deltalake`)

Register deltalake tables.  For example:

```sql
CREATE EXTERNAL TABLE table_name STORED AS DELTATABLE LOCATION 's3://bucket/table'
```

##### Iceberg (`--features=iceberg`)

Register iceberg tables.  For example:

```sql
CREATE EXTERNAL TABLE table_name STORED AS ICEBERG LOCATION 's3://bucket/table'
```

Register Iceberg Rest Catalog

```toml
[[execution.iceberg.rest_catalog]]
name = "my_iceberg_catalog"
addr = "192.168.1.1:8181"
```


##### Json Functions (`--features=function-json`)

Adds functions from [datafusion-function-json] for querying JSON strings in DataFusion in `dft`.  For example:

```sql
select * from foo where json_get(attributes, 'bar')::string='ham'
(show examples of using operators too)
```

[datafusion-function-json]: https://github.com/datafusion-contrib/datafusion-functions-json

##### HuggingFace (`--features=huggingface`)

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


### Getting Started

To have the best experience with `dft` it is highly recommended to define all of your DDL in `~/.config/ddl.sql` so that any tables you wish to query are available at startup.  Additionally, now that DataFusion supports `CREATE VIEW` via sql you can also make a `VIEW` based on these tables.

For example, your DDL file could look like the following:

```
CREATE EXTERNAL TABLE users STORED AS NDJSON LOCATION 's3://bucket/users';

CREATE EXTERNAL TABLE transactions STORED AS PARQUET LOCATION 's3://bucket/transactions';

CREATE EXTERNAL TABLE listings STORED AS PARQUET LOCATION 'file://folder/listings';

CREATE VIEW OR REPLACE users_listings AS SELECT * FROM users LEFT JOIN listings USING (user_id);
```

This would make the tables `users`, `transactions`, `listings`, and the view  `users_listings` available at startup.  Any of these DDL statements could also be run interactively from the SQL editor as well to create the tables.

### Key Mappings

The interface is split into several tabs and modes so that relevant information can be viewed and controlled in a clean and organized manner. When not writing a SQL query keys can be entered to navigate and control the interface.

#### SQL Tab

Editor for executing SQL with local DataFusion `SessionContext`.

- Normal mode
    - Not editable
        - `q` => quit datafusion-tui
        - `e` => start editing SQL Editor in Edit mode
        - `c` => clear contents of SQL Editor
        - `Enter` => execute query
        - Enter the tab number in brackets after a tabs name to navigate to that tab
        - If query results are longer or wider than screen, you can use arrow keys to scroll
    - Editable
        - Character keys to write queries
        - Backspace / tab / enter work same as normal
        - `Shift` + Up/Down/Left/Right => Select text
        - `Alt` + `Enter` => execute query
        - `esc` to exit Edit mode and go back to Normal mode
- DDL mode
    - Not editable
        - `l` => load configured DDL file into editor
        - `enter` => rerun configured DDL file
        - `s` => write editor contents to configured DDL file
    - Editable
        - Character keys to write queries
        - Backspace / tab / enter work same as normal
        - `Shift` + Up/Down/Left/Right => Select text
        - `Alt` + `Enter` => execute query
        - `esc` to exit Edit mode and go back to Normal mode

#### FlightSQL Tab

Same interface as SQL tab but sends SQL queries to FlightSQL server.

- Normal mode
    - `q` => quit datafusion-tui
    - `e` => start editing SQL Editor in Edit mode
    - `c` => clear contents of SQL Editor
    - `Enter` => execute query
    - Enter the tab number in brackets after a tabs name to navigate to that tab
    - If query results are longer or wider than screen, you can use arrow keys to scroll
  - Edit mode
    - Character keys to write queries
    - Backspace / tab / enter work same as normal
    - `Shift` + Up/Down/Left/Right => Select text
    - `Alt` + `Enter` => execute query
    - `esc` to exit Edit mode and go back to Normal mode

#### History Tab

TODO

#### Logs Tab

  - Logging mode (coming from [tui_logger](https://docs.rs/tui-logger/latest/tui_logger/index.html))
    - `h` => Toggles target selector widget hidden/visible
    - `f` => Toggle focus on the selected target only
    - `UP` => Select previous target in target selector widget
    - `DOWN` => Select next target in target selector widget
    - `LEFT` => Reduce SHOWN (!) log messages by one level
    - `RIGHT` => Increase SHOWN (!) log messages by one level
    - `-` => Reduce CAPTURED (!) log messages by one level
    - `+` => Increase CAPTURED (!) log messages by one level
    - `PAGEUP` => Enter Page Mode and scroll approx. half page up in log history.
    - `PAGEDOWN` => Only in page mode: scroll 10 events down in log history.
    - `ESCAPE` => Exit page mode and go back to scrolling mode
    - `SPACE` => Toggles hiding of targets, which have logfilter set to off

### Config Reference

The `dft` configuration is stored in `~/.config/dft/config.toml`.  All configuration options are listed below.

#### Execution Config

The execution config is where you can define query execution properties for each app (so the below would each expect to be in a relevant app section like `shared`, `tui`, `cli`, or `flightsql_server` (The FlightSQL client doesnt actually execute so doesnt have an execution config).  You can configure the `ObjectStore`s that you want to use in your queries and path of a DDL file that you want to run on startup.  For example, if you have an S3 bucket you want to query you could define it like so:

```toml
[[execution.object_store.s3]]
bucket_name = "my_bucket"
object_store_url = "s3://my_bucket"
aws_endpoint = "https://s3.amazonaws"
aws_access_key_id = "MY_ACCESS_KEY"
aws_secret = "MY SECRET"
aws_session_token = "MY_SESSION"
aws_allow_http = false
```

And define a custom DDL path like so (the default is `~/.config/dft/ddl.sql`).

```toml
[execution]
ddl_path = "/path/to/my/ddl.sql"
```

Multiple `ObjectStore`s can be defined in the config file. In the future datafusion `SessionContext` and `SessionState` options can be configured here.

Set the number of iterations for benchmarking queries (10 is the default).

```toml
[execution]
benchmark_iterations = 10
```

The batch size for query execution can be configured based on the app being used (TUI, CLI, or FlightSQL Server). For the TUI it defaults to 100, which may slow down queries, because a Record Batch is used as a unit of pagination and too many rows can cause the TUI to hang. For the CLI and FlightSQL Server, the default is 8092.

```toml
[execution]
cli_batch_size = 8092
tui_batch_size = 100
flightsql_server_batch_size = 8092
```

#### Display Config

The display config is where you can define the frame rate of the TUI.

```toml
[tui.display]
frame_rate = 60
```

#### Interaction Config

The interaction config is where mouse and paste behavior can be defined.  This is not currently implemented.

```toml
[tui.interaction]
mouse = true
paste = true
```

#### FlightSQL Config

The FlightSQL config is where you can define the connection URL for the FlightSQL client & server.

```toml
[flightsql_client]
connection_url = "http://localhost:50051"
benchmark_iterations = 10

[flightsql_server]
connection_url = "http://localhost:50051"
server_metrics_port = "0.0.0.0:9000"
```

#### Editor Config

The editor config is where you can set your preferred editor settings.

Currently only syntax highlighting is supported.  It is experimental because currently the regex that is used to determine keywords only works in simple cases.

```toml
[tui.editor]
experimental_syntax_highlighting = true
```

