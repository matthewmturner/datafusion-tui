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

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;

use datafusion::catalog::Session;
use datafusion::catalog::TableFunctionImpl;
use datafusion::common::{plan_err, Column};
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::datasource::TableProvider;
use datafusion::error::Result;
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::scalar::ScalarValue;
use parquet::basic::ConvertedType;
use parquet::data_type::{ByteArray, FixedLenByteArray};
use parquet::file::reader::FileReader;
use parquet::file::serialized_reader::SerializedFileReader;
use parquet::file::statistics::Statistics;
use std::fs::File;
use std::sync::Arc;

// Copied from https://github.com/apache/datafusion/blob/main/datafusion-cli/src/functions.rs
/// PARQUET_META table function
#[derive(Debug)]
struct ParquetMetadataTable {
    schema: SchemaRef,
    batch: RecordBatch,
}

#[async_trait]
impl TableProvider for ParquetMetadataTable {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> arrow::datatypes::SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> datafusion::logical_expr::TableType {
        datafusion::logical_expr::TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(MemorySourceConfig::try_new_exec(
            &[vec![self.batch.clone()]],
            TableProvider::schema(self),
            projection.cloned(),
        )?)
    }
}

fn convert_parquet_statistics(
    value: &Statistics,
    converted_type: ConvertedType,
) -> (Option<String>, Option<String>) {
    match (value, converted_type) {
        (Statistics::Boolean(val), _) => (
            val.min_opt().map(|v| v.to_string()),
            val.max_opt().map(|v| v.to_string()),
        ),
        (Statistics::Int32(val), _) => (
            val.min_opt().map(|v| v.to_string()),
            val.max_opt().map(|v| v.to_string()),
        ),
        (Statistics::Int64(val), _) => (
            val.min_opt().map(|v| v.to_string()),
            val.max_opt().map(|v| v.to_string()),
        ),
        (Statistics::Int96(val), _) => (
            val.min_opt().map(|v| v.to_string()),
            val.max_opt().map(|v| v.to_string()),
        ),
        (Statistics::Float(val), _) => (
            val.min_opt().map(|v| v.to_string()),
            val.max_opt().map(|v| v.to_string()),
        ),
        (Statistics::Double(val), _) => (
            val.min_opt().map(|v| v.to_string()),
            val.max_opt().map(|v| v.to_string()),
        ),
        (Statistics::ByteArray(val), ConvertedType::UTF8) => (
            byte_array_to_string(val.min_opt()),
            byte_array_to_string(val.max_opt()),
        ),
        (Statistics::ByteArray(val), _) => (
            val.min_opt().map(|v| v.to_string()),
            val.max_opt().map(|v| v.to_string()),
        ),
        (Statistics::FixedLenByteArray(val), ConvertedType::UTF8) => (
            fixed_len_byte_array_to_string(val.min_opt()),
            fixed_len_byte_array_to_string(val.max_opt()),
        ),
        (Statistics::FixedLenByteArray(val), _) => (
            val.min_opt().map(|v| v.to_string()),
            val.max_opt().map(|v| v.to_string()),
        ),
    }
}

/// Convert to a string if it has utf8 encoding, otherwise print bytes directly
fn byte_array_to_string(val: Option<&ByteArray>) -> Option<String> {
    val.map(|v| {
        v.as_utf8()
            .map(|s| s.to_string())
            .unwrap_or_else(|_e| v.to_string())
    })
}

/// Convert to a string if it has utf8 encoding, otherwise print bytes directly
fn fixed_len_byte_array_to_string(val: Option<&FixedLenByteArray>) -> Option<String> {
    val.map(|v| {
        v.as_utf8()
            .map(|s| s.to_string())
            .unwrap_or_else(|_e| v.to_string())
    })
}

#[derive(Debug)]
pub struct ParquetMetadataFunc {}

impl TableFunctionImpl for ParquetMetadataFunc {
    fn call(&self, exprs: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        let filename = match exprs.first() {
            Some(Expr::Literal(ScalarValue::Utf8(Some(s)))) => s, // single quote: parquet_metadata('x.parquet')
            Some(Expr::Column(Column { name, .. })) => name, // double quote: parquet_metadata("x.parquet")
            _ => {
                return plan_err!("parquet_metadata requires string argument as its input");
            }
        };

        let file = File::open(filename.clone())?;
        let reader = SerializedFileReader::new(file)?;
        let metadata = reader.metadata();

        let schema = Arc::new(Schema::new(vec![
            Field::new("filename", DataType::Utf8, true),
            Field::new("row_group_id", DataType::Int64, true),
            Field::new("row_group_num_rows", DataType::Int64, true),
            Field::new("row_group_num_columns", DataType::Int64, true),
            Field::new("row_group_bytes", DataType::Int64, true),
            Field::new("column_id", DataType::Int64, true),
            Field::new("file_offset", DataType::Int64, true),
            Field::new("num_values", DataType::Int64, true),
            Field::new("path_in_schema", DataType::Utf8, true),
            Field::new("type", DataType::Utf8, true),
            Field::new("stats_min", DataType::Utf8, true),
            Field::new("stats_max", DataType::Utf8, true),
            Field::new("stats_null_count", DataType::Int64, true),
            Field::new("stats_distinct_count", DataType::Int64, true),
            Field::new("stats_min_value", DataType::Utf8, true),
            Field::new("stats_max_value", DataType::Utf8, true),
            Field::new("compression", DataType::Utf8, true),
            Field::new("encodings", DataType::Utf8, true),
            Field::new("index_page_offset", DataType::Int64, true),
            Field::new("dictionary_page_offset", DataType::Int64, true),
            Field::new("data_page_offset", DataType::Int64, true),
            Field::new("total_compressed_size", DataType::Int64, true),
            Field::new("total_uncompressed_size", DataType::Int64, true),
        ]));

        // construct record batch from metadata
        let mut filename_arr = vec![];
        let mut row_group_id_arr = vec![];
        let mut row_group_num_rows_arr = vec![];
        let mut row_group_num_columns_arr = vec![];
        let mut row_group_bytes_arr = vec![];
        let mut column_id_arr = vec![];
        let mut file_offset_arr = vec![];
        let mut num_values_arr = vec![];
        let mut path_in_schema_arr = vec![];
        let mut type_arr = vec![];
        let mut stats_min_arr = vec![];
        let mut stats_max_arr = vec![];
        let mut stats_null_count_arr = vec![];
        let mut stats_distinct_count_arr = vec![];
        let mut stats_min_value_arr = vec![];
        let mut stats_max_value_arr = vec![];
        let mut compression_arr = vec![];
        let mut encodings_arr = vec![];
        let mut index_page_offset_arr = vec![];
        let mut dictionary_page_offset_arr = vec![];
        let mut data_page_offset_arr = vec![];
        let mut total_compressed_size_arr = vec![];
        let mut total_uncompressed_size_arr = vec![];
        for (rg_idx, row_group) in metadata.row_groups().iter().enumerate() {
            for (col_idx, column) in row_group.columns().iter().enumerate() {
                filename_arr.push(filename.clone());
                row_group_id_arr.push(rg_idx as i64);
                row_group_num_rows_arr.push(row_group.num_rows());
                row_group_num_columns_arr.push(row_group.num_columns() as i64);
                row_group_bytes_arr.push(row_group.total_byte_size());
                column_id_arr.push(col_idx as i64);
                file_offset_arr.push(column.file_offset());
                num_values_arr.push(column.num_values());
                path_in_schema_arr.push(column.column_path().to_string());
                type_arr.push(column.column_type().to_string());
                let converted_type = column.column_descr().converted_type();

                if let Some(s) = column.statistics() {
                    let (min_val, max_val) = convert_parquet_statistics(s, converted_type);
                    stats_min_arr.push(min_val.clone());
                    stats_max_arr.push(max_val.clone());
                    stats_null_count_arr.push(s.null_count_opt().map(|c| c as i64));
                    stats_distinct_count_arr.push(s.distinct_count_opt().map(|c| c as i64));
                    stats_min_value_arr.push(min_val);
                    stats_max_value_arr.push(max_val);
                } else {
                    stats_min_arr.push(None);
                    stats_max_arr.push(None);
                    stats_null_count_arr.push(None);
                    stats_distinct_count_arr.push(None);
                    stats_min_value_arr.push(None);
                    stats_max_value_arr.push(None);
                };
                compression_arr.push(format!("{:?}", column.compression()));
                encodings_arr.push(format!("{:?}", column.encodings()));
                index_page_offset_arr.push(column.index_page_offset());
                dictionary_page_offset_arr.push(column.dictionary_page_offset());
                data_page_offset_arr.push(column.data_page_offset());
                total_compressed_size_arr.push(column.compressed_size());
                total_uncompressed_size_arr.push(column.uncompressed_size());
            }
        }

        let rb = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(filename_arr)),
                Arc::new(Int64Array::from(row_group_id_arr)),
                Arc::new(Int64Array::from(row_group_num_rows_arr)),
                Arc::new(Int64Array::from(row_group_num_columns_arr)),
                Arc::new(Int64Array::from(row_group_bytes_arr)),
                Arc::new(Int64Array::from(column_id_arr)),
                Arc::new(Int64Array::from(file_offset_arr)),
                Arc::new(Int64Array::from(num_values_arr)),
                Arc::new(StringArray::from(path_in_schema_arr)),
                Arc::new(StringArray::from(type_arr)),
                Arc::new(StringArray::from(stats_min_arr)),
                Arc::new(StringArray::from(stats_max_arr)),
                Arc::new(Int64Array::from(stats_null_count_arr)),
                Arc::new(Int64Array::from(stats_distinct_count_arr)),
                Arc::new(StringArray::from(stats_min_value_arr)),
                Arc::new(StringArray::from(stats_max_value_arr)),
                Arc::new(StringArray::from(compression_arr)),
                Arc::new(StringArray::from(encodings_arr)),
                Arc::new(Int64Array::from(index_page_offset_arr)),
                Arc::new(Int64Array::from(dictionary_page_offset_arr)),
                Arc::new(Int64Array::from(data_page_offset_arr)),
                Arc::new(Int64Array::from(total_compressed_size_arr)),
                Arc::new(Int64Array::from(total_uncompressed_size_arr)),
            ],
        )?;

        let parquet_metadata = ParquetMetadataTable { schema, batch: rb };
        Ok(Arc::new(parquet_metadata))
    }
}
