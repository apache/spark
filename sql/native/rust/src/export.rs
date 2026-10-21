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

/// Exports a data source through version 1 of Spark's C data source ABI.
///
/// Call this macro once in a cdylib:
///
/// ```rust,ignore
/// export_data_source!(RangeSource);
/// ```
///
/// The associated types of [`DataSource`](crate::DataSource) determine the operations:
/// the SDK installs the native callbacks of each operation whose type is not
/// [`Unsupported`](crate::Unsupported), and leaves the others null. The only exported
/// symbol is `spark_ds_init`; sources implement the safe Rust traits and do not handle
/// C pointers. The library must be built with `panic = "unwind"`, so that the SDK can
/// report a panic as an error.
#[macro_export]
macro_rules! export_data_source {
    ($source:ty $(,)?) => {
        #[cfg(panic = "abort")]
        compile_error!(
            "spark_native_source requires panic = \"unwind\": with panic = \"abort\", a panic \
             in the data source aborts the JVM of Spark instead of failing the operation"
        );

        mod __spark_native_datasource_exports {
            use super::*;
            use std::ffi::{c_char, c_int, c_void};
            use $crate::__private::abi::{
                self, ArrowArrayStream, ArrowSchema, SparkDsLibrary, SparkDsSink,
            };
            use $crate::__private::bridge::{open_reader, parse_predicates, Bridge};

            // Prefixed so that they cannot capture the name of the exported source type.
            type __SparkSource = $source;
            type __SparkScanBuilder = <__SparkSource as $crate::DataSource>::ScanBuilder;
            type __SparkBatchWrite = <__SparkSource as $crate::DataSource>::BatchWrite;
            type __SparkReaderFactory = <__SparkScanBuilder as $crate::ScanBuilder>::Factory;
            type __SparkWriterFactory = <__SparkBatchWrite as $crate::BatchWrite>::Factory;
            type __SparkMessage = <__SparkBatchWrite as $crate::BatchWrite>::Message;

            fn bridge() -> &'static Bridge<__SparkSource> {
                static BRIDGE: std::sync::OnceLock<Bridge<__SparkSource>> =
                    std::sync::OnceLock::new();
                BRIDGE.get_or_init(Bridge::default)
            }

            const READ: bool = Bridge::<__SparkSource>::READ;
            const WRITE: bool = Bridge::<__SparkSource>::WRITE;

            static LIBRARY: SparkDsLibrary = SparkDsLibrary {
                size: std::mem::size_of::<SparkDsLibrary>() as i64,
                abi_version: abi::ABI_VERSION,
                last_error: Some(abi::last_error),
                create: Some(create),
                schema: if <__SparkSource as $crate::DataSource>::INFERS_SCHEMA {
                    Some(schema)
                } else {
                    None
                },
                create_reader: if READ { Some(create_reader) } else { None },
                create_stream_reader: None,
                create_writer: if WRITE { Some(create_writer) } else { None },
                close: Some(close),
                push_predicates: if READ { Some(push_predicates) } else { None },
                push_limit: if READ { Some(push_limit) } else { None },
                prune_columns: if READ { Some(prune_columns) } else { None },
                partitions: if READ { Some(partitions) } else { None },
                serialize_reader: if READ { Some(serialize_reader) } else { None },
                close_reader: if READ { Some(close_reader) } else { None },
                initial_offset: None,
                latest_offset: None,
                stream_partitions: None,
                serialize_stream_reader: None,
                commit_offset: None,
                close_stream_reader: None,
                read: if READ { Some(read) } else { None },
                serialize_writer: if WRITE { Some(serialize_writer) } else { None },
                commit: if WRITE { Some(commit) } else { None },
                abort: if WRITE { Some(abort) } else { None },
                close_writer: if WRITE { Some(close_writer) } else { None },
                create_data_writer: if WRITE {
                    Some(create_data_writer)
                } else {
                    None
                },
                write: if WRITE { Some(write) } else { None },
                commit_data_writer: if WRITE {
                    Some(commit_data_writer)
                } else {
                    None
                },
                abort_data_writer: if WRITE { Some(abort_data_writer) } else { None },
                close_data_writer: if WRITE { Some(close_data_writer) } else { None },
            };

            #[export_name = "spark_ds_init"]
            /// # Safety
            /// The arguments must follow the spark_ds_init contract of spark_datasource.h.
            pub unsafe extern "C" fn initialize(
                host: *const abi::SparkDsHost,
                library: *mut *const SparkDsLibrary,
            ) -> c_int {
                unsafe {
                    abi::initialize(
                        host,
                        library,
                        &LIBRARY,
                        <__SparkSource as $crate::DataSource>::init,
                    )
                }
            }

            unsafe extern "C" fn create(
                info: *const abi::SparkDsCreateInfo,
                capabilities: *mut u64,
                source: *mut *mut c_void,
            ) -> c_int {
                abi::call(|| {
                    abi::require_output(capabilities)?;
                    abi::require_output(source)?;
                    let info = unsafe { abi::create_info(info)? };
                    let (handle, declared_capabilities) = bridge().create(info)?;
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
                    let predicates = parse_predicates(&values);
                    let support = bridge().push_predicates(reader as usize as i64, predicates)?;
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
                    let stream =
                        unsafe { open_reader::<__SparkReaderFactory>(task, state, partition)? };
                    unsafe { output.write(stream) };
                    Ok(())
                })
            }

            unsafe extern "C" fn create_writer(
                source: *mut c_void,
                schema: *mut ArrowSchema,
                info: *const abi::SparkDsWriteInfo,
                writer: *mut *mut c_void,
            ) -> c_int {
                abi::call(|| {
                    abi::require_output(writer)?;
                    let mode = unsafe { abi::write_mode(info)? };
                    let schema = unsafe { abi::import_schema(schema)? };
                    let info = $crate::WriteInfo::new(schema, mode);
                    let handle = bridge().create_writer(source as usize as i64, info)?;
                    unsafe { writer.write(handle as usize as *mut c_void) };
                    Ok(())
                })
            }

            unsafe extern "C" fn serialize_writer(
                writer: *mut c_void,
                sink: SparkDsSink,
                data: *mut c_void,
            ) -> c_int {
                abi::call(|| {
                    let state = bridge().serialized_writer(writer as usize as i64)?;
                    unsafe { abi::emit(sink, data, &state) }
                })
            }

            unsafe extern "C" fn commit(
                writer: *mut c_void,
                epoch: i64,
                messages: *const *const u8,
                lengths: *const i64,
                count: i64,
            ) -> c_int {
                abi::call(|| {
                    abi::batch_epoch(epoch)?;
                    let messages = unsafe { abi::messages(messages, lengths, count)? };
                    bridge().commit(writer as usize as i64, &messages)
                })
            }

            unsafe extern "C" fn abort(
                writer: *mut c_void,
                epoch: i64,
                messages: *const *const u8,
                lengths: *const i64,
                count: i64,
            ) -> c_int {
                abi::call(|| {
                    abi::batch_epoch(epoch)?;
                    let messages = unsafe { abi::messages(messages, lengths, count)? };
                    bridge().abort(writer as usize as i64, &messages)
                })
            }

            unsafe extern "C" fn close_writer(writer: *mut c_void) {
                let _ = abi::call(|| bridge().close_writer(writer as usize as i64));
            }

            unsafe extern "C" fn create_data_writer(
                task: *const abi::SparkDsTaskContext,
                state: *const u8,
                length: i64,
                epoch: i64,
                writer: *mut *mut c_void,
            ) -> c_int {
                abi::call(|| {
                    abi::require_output(writer)?;
                    abi::batch_epoch(epoch)?;
                    let state = unsafe { abi::slice(state, length)? };
                    let handle = unsafe {
                        $crate::__private::write::open::<__SparkWriterFactory>(task, state)?
                    };
                    unsafe { writer.write(handle) };
                    Ok(())
                })
            }

            unsafe extern "C" fn write(
                writer: *mut c_void,
                array: *mut abi::ArrowArray,
                schema: *mut ArrowSchema,
            ) -> c_int {
                abi::call(|| unsafe {
                    $crate::__private::write::write_batch::<__SparkMessage>(writer, array, schema)
                })
            }

            unsafe extern "C" fn commit_data_writer(
                writer: *mut c_void,
                sink: SparkDsSink,
                data: *mut c_void,
            ) -> c_int {
                abi::call(|| {
                    sink.ok_or("null result sink")?;
                    let message =
                        unsafe { $crate::__private::write::commit_task::<__SparkMessage>(writer)? };
                    unsafe { abi::emit(sink, data, &message) }
                })
            }

            unsafe extern "C" fn abort_data_writer(writer: *mut c_void) -> c_int {
                abi::call(|| unsafe {
                    $crate::__private::write::abort_task::<__SparkMessage>(writer)
                })
            }

            unsafe extern "C" fn close_data_writer(writer: *mut c_void) {
                let _ = abi::call(|| unsafe {
                    $crate::__private::write::close_task::<__SparkMessage>(writer)
                });
            }
        }
    };
}
