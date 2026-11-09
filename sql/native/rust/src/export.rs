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

//! The function table of an exported source: native callbacks that adapt the C ABI to the
//! bridge of the source.

use std::ffi::{c_char, c_int, c_void};

use crate::abi::{
    self, ArrowArray, ArrowArrayStream, ArrowSchema, SparkDsCommitOffsetInfo, SparkDsCreateInfo,
    SparkDsDataWriterInfo, SparkDsFinishDataWriterInfo, SparkDsFinishWriteInfo,
    SparkDsInitialOffsetInfo, SparkDsLatestOffsetInfo, SparkDsLibrary, SparkDsPartitionsInfo,
    SparkDsReadInfo, SparkDsReadLimit, SparkDsReaderInfo, SparkDsSink, SparkDsStreamPartitionsInfo,
    SparkDsStreamReaderInfo, SparkDsTaskContext, SparkDsWriteInfo,
};
use crate::bridge::{open_reader, Bridge, EncodedPlan};
use crate::{
    host, write, BatchWrite, DataSource, MicroBatchStream, MicroBatchStreamInfo, ScanBuilder,
    StreamingWrite,
};

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
/// [`Unsupported`](crate::Unsupported), and leaves the others null. Batch and streaming
/// operations share the executor callbacks, `read` and the data writer callbacks. The only exported
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

        const _: () = {
            impl $crate::__private::Exported for $source {
                fn bridge() -> &'static $crate::__private::Bridge<Self> {
                    static BRIDGE: std::sync::OnceLock<$crate::__private::Bridge<$source>> =
                        std::sync::OnceLock::new();
                    BRIDGE.get_or_init(Default::default)
                }
            }

            static LIBRARY: $crate::__private::abi::SparkDsLibrary =
                $crate::__private::library::<$source>();

            #[export_name = "spark_ds_init"]
            /// # Safety
            /// The arguments must follow the spark_ds_init contract of spark_datasource.h.
            unsafe extern "C" fn spark_ds_init(
                host: *const $crate::__private::abi::SparkDsHost,
                library: *mut *const $crate::__private::abi::SparkDsLibrary,
            ) -> std::ffi::c_int {
                unsafe {
                    $crate::__private::abi::initialize(
                        host,
                        library,
                        &LIBRARY,
                        <$source as $crate::DataSource>::init,
                    )
                }
            }
        };
    };
}

/// A data source that [`export_data_source!`] exports, with the registry of its handles.
pub trait Exported: DataSource {
    fn bridge() -> &'static Bridge<Self>;
}

// The factories that the executors restore for each kind of operation.
type ReaderFactory<S> = <<S as DataSource>::ScanBuilder as ScanBuilder>::Factory;
type StreamReaderFactory<S> = <<S as DataSource>::MicroBatchStream as MicroBatchStream>::Factory;
type WriterFactory<S> = <<S as DataSource>::BatchWrite as BatchWrite>::Factory;
type StreamingWriterFactory<S> = <<S as DataSource>::StreamingWrite as StreamingWrite>::Factory;

/// The function table of a source, with the callbacks of the operations that it implements.
pub const fn library<S: Exported>() -> SparkDsLibrary {
    // The callback when the source supports its operation, and null otherwise.
    macro_rules! when {
        ($supported:expr, $callback:expr) => {
            if $supported { Some($callback) } else { None }
        };
    }
    let read = Bridge::<S>::READ;
    let micro_batch_read = Bridge::<S>::MICRO_BATCH_READ;
    // Batch and streaming writes share all the callbacks of writers.
    let write = Bridge::<S>::WRITE || Bridge::<S>::STREAMING_WRITE;
    SparkDsLibrary {
        size: std::mem::size_of::<SparkDsLibrary>() as i64,
        abi_version: abi::ABI_VERSION,
        last_error: Some(abi::last_error),
        create: Some(create::<S>),
        schema: when!(S::INFERS_SCHEMA, schema::<S>),
        create_reader: when!(read, create_reader::<S>),
        create_stream_reader: when!(micro_batch_read, create_stream_reader::<S>),
        create_writer: when!(write, create_writer::<S>),
        close: Some(close::<S>),
        push_limit: when!(read, push_limit::<S>),
        prune_columns: when!(read, prune_columns::<S>),
        partitions: when!(read, partitions::<S>),
        close_reader: when!(read, close_reader::<S>),
        initial_offset: when!(micro_batch_read, initial_offset::<S>),
        latest_offset: when!(micro_batch_read, latest_offset::<S>),
        stream_partitions: when!(micro_batch_read, stream_partitions::<S>),
        commit_offset: when!(micro_batch_read, commit_offset::<S>),
        close_stream_reader: when!(micro_batch_read, close_stream_reader::<S>),
        // Batch scans and micro-batches share the executor reader.
        read: when!(read || micro_batch_read, read_partition::<S>),
        commit: when!(write, commit::<S>),
        abort: when!(write, abort::<S>),
        close_writer: when!(write, close_writer::<S>),
        create_data_writer: when!(write, create_data_writer::<S>),
        write: when!(write, write_batch),
        commit_data_writer: when!(write, commit_data_writer),
        abort_data_writer: when!(write, abort_data_writer),
        close_data_writer: when!(write, close_data_writer),
        default_read_limit: when!(micro_batch_read, default_read_limit::<S>),
    }
}

/// The ID of the driver handle that Spark passes back, which the SDK returned as a pointer.
fn id(handle: *mut c_void) -> i64 {
    handle as usize as i64
}

fn handle(id: i64) -> *mut c_void {
    id as usize as *mut c_void
}

/// Passes the partitions of a plan, and its state, to the sinks of Spark.
///
/// # Safety
/// The sinks must follow the contract of `partitions` and `stream_partitions`.
unsafe fn emit_plan(
    plan: EncodedPlan,
    add_partition: SparkDsSink,
    set_state: SparkDsSink,
    sink: *mut c_void,
) -> crate::Result<()> {
    for partition in &plan.partitions {
        unsafe { abi::emit(add_partition, sink, partition)? };
    }
    unsafe { abi::emit(set_state, sink, &plan.state) }
}

unsafe extern "C" fn create<S: Exported>(
    info: *const SparkDsCreateInfo,
    capabilities: *mut u64,
    source: *mut *mut c_void,
) -> c_int {
    abi::call(|| {
        abi::require_output(capabilities)?;
        abi::require_output(source)?;
        // An operation starts on the driver: follow the level of the log of Spark.
        host::follow_level();
        let info = unsafe { abi::create_info(info)? };
        let (created, declared) = S::bridge().create(info)?;
        unsafe {
            capabilities.write(declared);
            source.write(handle(created));
        }
        Ok(())
    })
}

unsafe extern "C" fn schema<S: Exported>(source: *mut c_void, output: *mut ArrowSchema) -> c_int {
    abi::call(|| {
        let schema = S::bridge().schema(id(source))?;
        unsafe { abi::export_schema(&schema, output) }
    })
}

unsafe extern "C" fn create_reader<S: Exported>(
    source: *mut c_void,
    schema: *mut ArrowSchema,
    _info: *const SparkDsReaderInfo,
    reader: *mut *mut c_void,
) -> c_int {
    abi::call(|| {
        abi::require_output(reader)?;
        let schema = unsafe { abi::import_schema(schema)? };
        let created = S::bridge().create_reader(id(source), schema)?;
        unsafe { reader.write(handle(created)) };
        Ok(())
    })
}

unsafe extern "C" fn close<S: Exported>(source: *mut c_void) {
    let _ = abi::call(|| S::bridge().close_source(id(source)));
}

unsafe extern "C" fn push_limit<S: Exported>(
    reader: *mut c_void,
    limit: i32,
    applied: *mut bool,
) -> c_int {
    abi::call(|| {
        abi::require_output(applied)?;
        let result = S::bridge().push_limit(id(reader), limit.try_into()?)?;
        unsafe { applied.write(result) };
        Ok(())
    })
}

unsafe extern "C" fn prune_columns<S: Exported>(
    reader: *mut c_void,
    columns: *const *const c_char,
    count: i64,
) -> c_int {
    abi::call(|| {
        let columns = unsafe { abi::strings(columns, count)? };
        S::bridge().prune_columns(id(reader), &columns)
    })
}

unsafe extern "C" fn partitions<S: Exported>(
    reader: *mut c_void,
    _info: *const SparkDsPartitionsInfo,
    add_partition: SparkDsSink,
    set_state: SparkDsSink,
    sink: *mut c_void,
) -> c_int {
    abi::call(|| {
        let plan = S::bridge().plan(id(reader))?;
        unsafe { emit_plan(plan, add_partition, set_state, sink) }
    })
}

unsafe extern "C" fn close_reader<S: Exported>(reader: *mut c_void) {
    let _ = abi::call(|| S::bridge().close_reader(id(reader)));
}

unsafe extern "C" fn create_stream_reader<S: Exported>(
    source: *mut c_void,
    schema: *mut ArrowSchema,
    info: *const SparkDsStreamReaderInfo,
    reader: *mut *mut c_void,
) -> c_int {
    abi::call(|| {
        abi::require_output(reader)?;
        let info = unsafe { abi::input(info)? };
        let checkpoint_location = unsafe { abi::string(info.checkpoint_location)? };
        let schema = unsafe { abi::import_schema(schema)? };
        let info = MicroBatchStreamInfo::new(schema, checkpoint_location);
        let created = S::bridge().create_stream_reader(id(source), info)?;
        unsafe { reader.write(handle(created)) };
        Ok(())
    })
}

unsafe extern "C" fn initial_offset<S: Exported>(
    reader: *mut c_void,
    _info: *const SparkDsInitialOffsetInfo,
    set_offset: SparkDsSink,
    sink: *mut c_void,
) -> c_int {
    abi::call(|| {
        let offset = S::bridge().initial_offset(id(reader))?;
        unsafe { abi::emit(set_offset, sink, offset.as_bytes()) }
    })
}

unsafe extern "C" fn default_read_limit<S: Exported>(
    reader: *mut c_void,
    limit: *mut SparkDsReadLimit,
) -> c_int {
    abi::call(|| {
        let value = S::bridge().default_read_limit(id(reader))?;
        unsafe { abi::set_read_limit(limit, value) }
    })
}

unsafe extern "C" fn latest_offset<S: Exported>(
    reader: *mut c_void,
    info: *const SparkDsLatestOffsetInfo,
    set_offset: SparkDsSink,
    sink: *mut c_void,
) -> c_int {
    abi::call(|| {
        let info = unsafe { abi::latest_offset_info(info)? };
        let offset = S::bridge().latest_offset(id(reader), info)?;
        unsafe { abi::emit(set_offset, sink, offset.as_bytes()) }
    })
}

unsafe extern "C" fn stream_partitions<S: Exported>(
    reader: *mut c_void,
    info: *const SparkDsStreamPartitionsInfo,
    add_partition: SparkDsSink,
    set_state: SparkDsSink,
    sink: *mut c_void,
) -> c_int {
    abi::call(|| {
        let info = unsafe { abi::input(info)? };
        let start = unsafe { abi::string(info.start)? };
        let end = unsafe { abi::string(info.end)? };
        let plan = S::bridge().stream_partitions(id(reader), &start, &end)?;
        unsafe { emit_plan(plan, add_partition, set_state, sink) }
    })
}

unsafe extern "C" fn commit_offset<S: Exported>(
    reader: *mut c_void,
    info: *const SparkDsCommitOffsetInfo,
) -> c_int {
    abi::call(|| {
        let info = unsafe { abi::input(info)? };
        let end = unsafe { abi::string(info.end)? };
        S::bridge().commit_offset(id(reader), &end)
    })
}

unsafe extern "C" fn close_stream_reader<S: Exported>(reader: *mut c_void) {
    let _ = abi::call(|| S::bridge().close_stream_reader(id(reader)));
}

unsafe extern "C" fn read_partition<S: Exported>(
    task: *const SparkDsTaskContext,
    info: *const SparkDsReadInfo,
    output: *mut ArrowArrayStream,
) -> c_int {
    abi::call(|| {
        abi::require_output(output)?;
        // A task starts on an executor: follow the level of the log of Spark.
        host::follow_level();
        let info = unsafe { abi::input(info)? };
        let schema = unsafe { abi::import_schema(info.schema)? };
        let state = unsafe { abi::slice(info.reader_state, info.reader_state_length)? };
        let partition = unsafe { abi::slice(info.partition, info.partition_length)? };
        let stream = match info.kind {
            abi::READ_BATCH => unsafe {
                open_reader::<ReaderFactory<S>>(task, schema, state, partition)?
            },
            abi::READ_MICRO_BATCH => unsafe {
                open_reader::<StreamReaderFactory<S>>(task, schema, state, partition)?
            },
            kind => return Err(format!("unsupported kind of read: {kind}").into()),
        };
        unsafe { output.write(stream) };
        Ok(())
    })
}

unsafe extern "C" fn create_writer<S: Exported>(
    source: *mut c_void,
    schema: *mut ArrowSchema,
    info: *const SparkDsWriteInfo,
    set_state: SparkDsSink,
    sink: *mut c_void,
    writer: *mut *mut c_void,
) -> c_int {
    abi::call(|| {
        abi::require_output(writer)?;
        set_state.ok_or("null result sink")?;
        let (mode, query_id) = unsafe { abi::write_info(info)? };
        let schema = unsafe { abi::import_schema(schema)? };
        let (created, state) = S::bridge().create_writer(id(source), schema, mode, query_id)?;
        if let Err(error) = unsafe { abi::emit(set_state, sink, &state) } {
            S::bridge().close_writer(created)?;
            return Err(error);
        }
        unsafe { writer.write(handle(created)) };
        Ok(())
    })
}

unsafe extern "C" fn commit<S: Exported>(
    writer: *mut c_void,
    info: *const SparkDsFinishWriteInfo,
) -> c_int {
    abi::call(|| {
        let info = unsafe { abi::input(info)? };
        let messages =
            unsafe { abi::messages(info.messages, info.message_lengths, info.num_messages)? };
        S::bridge().commit(id(writer), info.epoch_id, &messages)
    })
}

unsafe extern "C" fn abort<S: Exported>(
    writer: *mut c_void,
    info: *const SparkDsFinishWriteInfo,
) -> c_int {
    abi::call(|| {
        let info = unsafe { abi::input(info)? };
        // Spark rejected what create_writer returned, before any task ran: the SDK returns a
        // single state, so only when Spark could not copy it, for example when it is too
        // large. The coordinator is dropped at close, as when its creation fails.
        if info.rejected != 0 {
            return Ok(());
        }
        let messages =
            unsafe { abi::messages(info.messages, info.message_lengths, info.num_messages)? };
        S::bridge().abort(id(writer), info.epoch_id, &messages)
    })
}

unsafe extern "C" fn close_writer<S: Exported>(writer: *mut c_void) {
    let _ = abi::call(|| S::bridge().close_writer(id(writer)));
}

unsafe extern "C" fn create_data_writer<S: Exported>(
    task: *const SparkDsTaskContext,
    info: *const SparkDsDataWriterInfo,
    data_writer: *mut *mut c_void,
) -> c_int {
    abi::call(|| {
        abi::require_output(data_writer)?;
        // A task starts on an executor: follow the level of the log of Spark.
        host::follow_level();
        let info = unsafe { abi::input(info)? };
        let schema = unsafe { abi::import_schema(info.schema)? };
        let state = unsafe { abi::slice(info.writer_state, info.writer_state_length)? };
        let created = unsafe {
            write::open::<WriterFactory<S>, StreamingWriterFactory<S>>(
                task,
                schema,
                state,
                info.epoch_id,
            )?
        };
        unsafe { data_writer.write(created) };
        Ok(())
    })
}

unsafe extern "C" fn write_batch(data_writer: *mut c_void, array: *mut ArrowArray) -> c_int {
    abi::call(|| unsafe { write::write_batch(data_writer, array) })
}

unsafe extern "C" fn commit_data_writer(
    data_writer: *mut c_void,
    _info: *const SparkDsFinishDataWriterInfo,
    set_message: SparkDsSink,
    sink: *mut c_void,
) -> c_int {
    abi::call(|| {
        set_message.ok_or("null result sink")?;
        let message = unsafe { write::commit_task(data_writer)? };
        unsafe { abi::emit(set_message, sink, &message) }
    })
}

unsafe extern "C" fn abort_data_writer(
    data_writer: *mut c_void,
    _info: *const SparkDsFinishDataWriterInfo,
) -> c_int {
    abi::call(|| unsafe { write::abort_task(data_writer) })
}

unsafe extern "C" fn close_data_writer(data_writer: *mut c_void) {
    let _ = abi::call(|| unsafe { write::close_task(data_writer) });
}
