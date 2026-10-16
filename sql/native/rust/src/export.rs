// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

/// Exports one batch-read provider through version 1 of Spark's C data source ABI.
///
/// Call this macro once in a cdylib. The only exported symbol is spark_ds_init;
/// providers implement the safe Rust traits and do not handle C pointers.
#[macro_export]
macro_rules! export_data_source {
    ($source:ty) => {
        mod __spark_native_datasource_exports {
            use super::*;
            use std::ffi::{c_char, c_int, c_void};
            use $crate::abi::{self, ArrowArrayStream, ArrowSchema, SparkDsLibrary, SparkDsSink};
            use $crate::bridge::{open_reader, Bridge};

            type Factory = <<$source as $crate::BatchSource>::Scan as $crate::ScanBuilder>::Factory;

            fn bridge() -> &'static Bridge<$source> {
                static BRIDGE: std::sync::OnceLock<Bridge<$source>> = std::sync::OnceLock::new();
                BRIDGE.get_or_init(Bridge::default)
            }

            static LIBRARY: SparkDsLibrary = SparkDsLibrary {
                size: std::mem::size_of::<SparkDsLibrary>() as i64,
                abi_version: abi::ABI_VERSION,
                last_error: Some(abi::last_error),
                create: Some(create),
                schema: Some(schema),
                create_reader: Some(create_reader),
                create_stream_reader: None,
                create_writer: None,
                close: Some(close),
                push_predicates: Some(push_predicates),
                push_limit: Some(push_limit),
                prune_columns: Some(prune_columns),
                partitions: Some(partitions),
                serialize_reader: Some(serialize_reader),
                close_reader: Some(close_reader),
                initial_offset: None,
                latest_offset: None,
                stream_partitions: None,
                serialize_stream_reader: None,
                commit_offset: None,
                close_stream_reader: None,
                read: Some(read),
                serialize_writer: None,
                commit: None,
                abort: None,
                close_writer: None,
                create_data_writer: None,
                write: None,
                commit_data_writer: None,
                abort_data_writer: None,
                close_data_writer: None,
            };

            #[export_name = "spark_ds_init"]
            /// # Safety
            /// The arguments must follow the spark_ds_init contract of spark_datasource.h.
            pub unsafe extern "C" fn initialize(
                host: *const abi::SparkDsHost,
                library: *mut *const SparkDsLibrary,
            ) -> c_int {
                unsafe { abi::initialize(host, library, &LIBRARY) }
            }

            unsafe extern "C" fn create(
                info: *const abi::SparkDsCreateInfo,
                capabilities: *mut u64,
                source: *mut *mut c_void,
            ) -> c_int {
                abi::call(|| {
                    abi::require_output(capabilities)?;
                    abi::require_output(source)?;
                    let options = unsafe { abi::options(info)? };
                    let (handle, declared_capabilities) = bridge().create(options)?;
                    unsafe {
                        capabilities.write(declared_capabilities);
                        source.write(handle as usize as *mut c_void);
                    }
                    Ok(())
                })
            }

            unsafe extern "C" fn schema(source: *mut c_void, output: *mut ArrowSchema) -> c_int {
                abi::call(|| {
                    let schema = bridge().schema(source as usize as i64)?;
                    unsafe { abi::export_schema(&schema, output) }
                })
            }

            unsafe extern "C" fn create_reader(
                source: *mut c_void,
                schema: *mut ArrowSchema,
                reader: *mut *mut c_void,
            ) -> c_int {
                abi::call(|| {
                    abi::require_output(reader)?;
                    let schema = unsafe { abi::import_schema(schema)? };
                    let handle = bridge().create_reader(source as usize as i64, schema)?;
                    unsafe { reader.write(handle as usize as *mut c_void) };
                    Ok(())
                })
            }

            unsafe extern "C" fn close(source: *mut c_void) {
                let _ = abi::call(|| bridge().close_source(source as usize as i64));
            }

            unsafe extern "C" fn push_predicates(
                reader: *mut c_void,
                predicates: *const *const c_char,
                count: i64,
                uses: *mut i32,
            ) -> c_int {
                abi::call(|| {
                    if count > 0 {
                        abi::require_output(uses)?;
                    }
                    let values = unsafe { abi::strings(predicates, count)? };
                    let predicates = $crate::bridge::parse_predicates(&values)?;
                    let support = bridge().push_predicates(reader as usize as i64, &predicates)?;
                    for (index, support) in support.into_iter().enumerate() {
                        unsafe { uses.add(index).write(abi::predicate_use(support)) };
                    }
                    Ok(())
                })
            }

            unsafe extern "C" fn push_limit(
                reader: *mut c_void,
                limit: i32,
                applied: *mut bool,
            ) -> c_int {
                abi::call(|| {
                    abi::require_output(applied)?;
                    let result = bridge().push_limit(reader as usize as i64, limit.try_into()?)?;
                    unsafe { applied.write(result) };
                    Ok(())
                })
            }

            unsafe extern "C" fn prune_columns(
                reader: *mut c_void,
                columns: *const *const c_char,
                count: i64,
                pruned: *mut bool,
            ) -> c_int {
                abi::call(|| {
                    abi::require_output(pruned)?;
                    let columns = unsafe { abi::strings(columns, count)? };
                    let result = bridge().prune_columns(reader as usize as i64, &columns)?;
                    unsafe { pruned.write(result) };
                    Ok(())
                })
            }

            unsafe extern "C" fn partitions(
                reader: *mut c_void,
                sink: SparkDsSink,
                data: *mut c_void,
            ) -> c_int {
                abi::call(|| {
                    let plan = bridge().planned(reader as usize as i64)?;
                    for partition in &plan.partitions {
                        unsafe { abi::emit(sink, data, partition)? };
                    }
                    Ok(())
                })
            }

            unsafe extern "C" fn serialize_reader(
                reader: *mut c_void,
                sink: SparkDsSink,
                data: *mut c_void,
            ) -> c_int {
                abi::call(|| {
                    let plan = bridge().planned(reader as usize as i64)?;
                    unsafe { abi::emit(sink, data, &plan.state) }
                })
            }

            unsafe extern "C" fn close_reader(reader: *mut c_void) {
                let _ = abi::call(|| bridge().close_reader(reader as usize as i64));
            }

            unsafe extern "C" fn read(
                task: *const abi::SparkDsTaskContext,
                state: *const u8,
                state_length: i64,
                partition: *const u8,
                partition_length: i64,
                output: *mut ArrowArrayStream,
            ) -> c_int {
                abi::call(|| {
                    abi::require_output(output)?;
                    let state = unsafe { abi::slice(state, state_length)? };
                    let partition = unsafe { abi::slice(partition, partition_length)? };
                    let stream = unsafe { open_reader::<Factory>(task, state, partition)? };
                    unsafe { output.write(stream) };
                    Ok(())
                })
            }
        }
    };
}
