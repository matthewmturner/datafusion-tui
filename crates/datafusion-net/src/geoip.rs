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

//! IP geolocation scalar UDF.
//!
//! [`GeoIpUdf`] (`geoip`) takes an IP address string and looks it up in a
//! MaxMind-format (`.mmdb`) geolocation database such as [GeoLite2-City],
//! returning a struct of location fields:
//!
//! ```sql
//! SELECT src_ip,
//!        geoip(src_ip)['country_code'] AS country,
//!        geoip(src_ip)['city'] AS city
//! FROM pcap('capture.pcap')
//! ```
//!
//! The database path is taken from the optional second argument
//! (`geoip(ip, '/path/GeoLite2-City.mmdb')`), or, for the single-argument
//! form, from the `GEOIP_DB` environment variable ([`GEOIP_DB_ENV_VAR`]) read
//! when the UDF is constructed. Opened databases are cached process-wide.
//!
//! Addresses that fail to parse or have no entry in the database yield a
//! NULL struct; a database that cannot be opened is a query error.
//!
//! [GeoLite2-City]: https://dev.maxmind.com/geoip/geolite2-free-geolocation-data

use std::{
    collections::HashMap,
    net::IpAddr,
    path::{Path, PathBuf},
    sync::{Arc, Mutex, OnceLock},
};

use datafusion::{
    arrow::{
        array::{
            Array, ArrayRef, Float64Builder, NullBufferBuilder, StringArray, StringBuilder,
            StructArray,
        },
        datatypes::{DataType, Field, Fields},
    },
    common::{cast::as_string_array, exec_datafusion_err, exec_err, Result},
    logical_expr::{
        ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature, Volatility,
    },
};
use maxminddb::geoip2;

/// Environment variable consulted by [`GeoIpUdf::default`] for the database
/// path used by the single-argument form of `geoip`
pub const GEOIP_DB_ENV_VAR: &str = "GEOIP_DB";

/// A cached, shared handle to an opened database
type SharedReader = Arc<maxminddb::Reader<Vec<u8>>>;

/// Process-wide cache of opened databases, keyed by path. Readers hold the
/// whole database in memory, so each distinct path is loaded once and shared
/// across batches and queries.
fn readers() -> &'static Mutex<HashMap<PathBuf, SharedReader>> {
    static READERS: OnceLock<Mutex<HashMap<PathBuf, SharedReader>>> = OnceLock::new();
    READERS.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Fields of the struct returned by `geoip`
fn geoip_fields() -> Fields {
    Fields::from(vec![
        Field::new("country_code", DataType::Utf8, true),
        Field::new("country", DataType::Utf8, true),
        Field::new("city", DataType::Utf8, true),
        Field::new("latitude", DataType::Float64, true),
        Field::new("longitude", DataType::Float64, true),
        Field::new("time_zone", DataType::Utf8, true),
    ])
}

/// Owned location values extracted from a database record
struct GeoRecord {
    country_code: Option<String>,
    country: Option<String>,
    city: Option<String>,
    latitude: Option<f64>,
    longitude: Option<f64>,
    time_zone: Option<String>,
}

impl GeoRecord {
    fn from_city(city: &geoip2::City<'_>) -> Self {
        Self {
            country_code: city.country.iso_code.map(str::to_string),
            country: city.country.names.english.map(str::to_string),
            city: city.city.names.english.map(str::to_string),
            latitude: city.location.latitude,
            longitude: city.location.longitude,
            time_zone: city.location.time_zone.map(str::to_string),
        }
    }
}

/// Scalar UDF that geolocates an IP address string using a MaxMind-format
/// (`.mmdb`) database
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct GeoIpUdf {
    signature: Signature,
    default_db: Option<PathBuf>,
}

impl GeoIpUdf {
    /// Creates the UDF with `path` as the database for the single-argument
    /// form of `geoip`
    pub fn with_db_path(path: impl Into<PathBuf>) -> Self {
        Self {
            signature: Self::make_signature(),
            default_db: Some(path.into()),
        }
    }

    fn make_signature() -> Signature {
        // Accept the string types our packet columns and SQL literals use.
        // Stable (not Immutable): results depend on the database file, which
        // can change between queries.
        Signature::one_of(
            vec![TypeSignature::String(1), TypeSignature::String(2)],
            Volatility::Stable,
        )
    }

    /// Returns the cached reader for `path`, opening (and caching) the
    /// database on first use
    fn reader_for(path: &Path) -> Result<SharedReader> {
        let mut readers = readers().lock().expect("geoip reader cache poisoned");
        if let Some(reader) = readers.get(path) {
            return Ok(Arc::clone(reader));
        }
        let reader = maxminddb::Reader::open_readfile(path).map_err(|e| {
            exec_datafusion_err!("geoip failed to open database '{}': {e}", path.display())
        })?;
        let reader = Arc::new(reader);
        readers.insert(path.to_path_buf(), Arc::clone(&reader));
        Ok(reader)
    }
}

impl Default for GeoIpUdf {
    fn default() -> Self {
        Self {
            signature: Self::make_signature(),
            default_db: std::env::var_os(GEOIP_DB_ENV_VAR).map(PathBuf::from),
        }
    }
}

impl ScalarUDFImpl for GeoIpUdf {
    fn name(&self) -> &str {
        "geoip"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Struct(geoip_fields()))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.is_empty() || args.args.len() > 2 {
            return exec_err!("geoip expects one or two arguments: geoip(ip [, db_path])");
        }
        let ips = to_utf8_array(&args.args[0].to_array(args.number_rows)?)?;
        let ips = as_string_array(&ips)?;
        let paths = match args.args.get(1) {
            Some(arg) => Some(to_utf8_array(&arg.to_array(args.number_rows)?)?),
            None => None,
        };
        let paths = paths.as_ref().map(|p| as_string_array(p)).transpose()?;

        let mut country_code = StringBuilder::new();
        let mut country = StringBuilder::new();
        let mut city = StringBuilder::new();
        let mut latitude = Float64Builder::new();
        let mut longitude = Float64Builder::new();
        let mut time_zone = StringBuilder::new();
        let mut validity = NullBufferBuilder::new(ips.len());

        for row in 0..ips.len() {
            match self.lookup_row(ips, paths, row)? {
                Some(record) => {
                    country_code.append_option(record.country_code);
                    country.append_option(record.country);
                    city.append_option(record.city);
                    latitude.append_option(record.latitude);
                    longitude.append_option(record.longitude);
                    time_zone.append_option(record.time_zone);
                    validity.append_non_null();
                }
                None => {
                    country_code.append_null();
                    country.append_null();
                    city.append_null();
                    latitude.append_null();
                    longitude.append_null();
                    time_zone.append_null();
                    validity.append_null();
                }
            }
        }

        let arrays: Vec<ArrayRef> = vec![
            Arc::new(country_code.finish()),
            Arc::new(country.finish()),
            Arc::new(city.finish()),
            Arc::new(latitude.finish()),
            Arc::new(longitude.finish()),
            Arc::new(time_zone.finish()),
        ];
        let structs = StructArray::new(geoip_fields(), arrays, validity.finish());
        Ok(ColumnarValue::Array(Arc::new(structs)))
    }
}

impl GeoIpUdf {
    /// Geolocates a single row. `Ok(None)` (a NULL struct) covers the
    /// row-level misses: NULL or unparseable address, NULL path argument, or
    /// an address with no database entry. Configuration problems — no
    /// database available or a database that cannot be opened — are errors.
    fn lookup_row(
        &self,
        ips: &StringArray,
        paths: Option<&StringArray>,
        row: usize,
    ) -> Result<Option<GeoRecord>> {
        if ips.is_null(row) {
            return Ok(None);
        }
        // Parse before touching the database so bad addresses are NULL even
        // when no database is configured
        let Ok(ip) = ips.value(row).parse::<IpAddr>() else {
            return Ok(None);
        };
        let path: PathBuf = match (paths, &self.default_db) {
            (Some(paths), _) if paths.is_null(row) => return Ok(None),
            (Some(paths), _) => PathBuf::from(paths.value(row)),
            (None, Some(default)) => default.clone(),
            (None, None) => {
                return exec_err!(
                    "geoip has no database configured; pass a path as the second argument \
                     (e.g. geoip(ip, '/path/GeoLite2-City.mmdb')) or set the \
                     {GEOIP_DB_ENV_VAR} environment variable"
                )
            }
        };
        let reader = Self::reader_for(&path)?;
        // Lookup errors (e.g. an IPv6 address in an IPv4-only database) are
        // row-level data issues, not query errors
        let record = reader
            .lookup(ip)
            .ok()
            .and_then(|result| result.decode::<geoip2::City>().ok().flatten())
            .map(|city| GeoRecord::from_city(&city));
        Ok(record)
    }
}

/// Normalizes the input (Utf8View / LargeUtf8) to Utf8 so a single code path
/// handles every accepted string type
fn to_utf8_array(array: &ArrayRef) -> Result<ArrayRef> {
    if array.data_type() == &DataType::Utf8 {
        Ok(Arc::clone(array))
    } else {
        Ok(datafusion::arrow::compute::cast(array, &DataType::Utf8)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_return_type_is_struct() {
        let udf = GeoIpUdf::default();
        assert_eq!(
            udf.return_type(&[DataType::Utf8]).unwrap(),
            DataType::Struct(geoip_fields())
        );
    }

    #[test]
    fn test_with_db_path_sets_default() {
        let udf = GeoIpUdf::with_db_path("/some/db.mmdb");
        assert_eq!(udf.default_db, Some(PathBuf::from("/some/db.mmdb")));
    }
}
