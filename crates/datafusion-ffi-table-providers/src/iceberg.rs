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

use abi_stable::{
    declare_root_module_statics,
    library::{LibraryError, RootModule},
    package_version_strings,
    sabi_types::VersionStrings,
    StableAbi,
};
use datafusion_ffi::table_provider::FFI_TableProvider;

#[repr(C)]
#[derive(StableAbi)]
#[sabi(kind(Prefix(prefix_ref = IcebergTableProviderModuleRef)))]
pub struct IcebergTableProviderModule {
    /// Constructs the table provider
    pub create_table: extern "C" fn() -> FFI_TableProvider,
}

impl RootModule for IcebergTableProviderModuleRef {
    declare_root_module_statics! {IcebergTableProviderModuleRef}
    const BASE_NAME: &'static str = "ffi_iceberg_table_provider";
    const NAME: &'static str = "ffi_iceberg_table_provider";
    const VERSION_STRINGS: VersionStrings = package_version_strings!();

    fn initialization(self) -> Result<Self, LibraryError> {
        Ok(self)
    }
}
