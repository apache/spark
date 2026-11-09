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

//! Bindings and adapters for version 1 of `spark_datasource.h`.

use std::cell::RefCell;
use std::ffi::{c_char, c_int, c_void, CStr, CString};
use std::mem::{offset_of, size_of};
use std::sync::Arc;

pub use arrow_array::ffi::FFI_ArrowArray as ArrowArray;
pub use arrow_array::ffi_stream::FFI_ArrowArrayStream as ArrowArrayStream;
pub use arrow_schema::ffi::FFI_ArrowSchema as ArrowSchema;
use arrow_schema::{Schema, SchemaRef};

use crate::bridge::{c_message, provider_call};
use crate::host::{self, LogEnabledFunction, LogFunction};
use crate::{CreateInfo, HostInfo, LatestOffsetInfo, Options, ReadLimit, Result, WriteMode};

pub const ABI_VERSION: i32 = 1;
pub const BATCH_READ: u64 = 1;
pub const MICRO_BATCH_READ: u64 = 2;
pub const BATCH_WRITE: u64 = 4;
pub const STREAMING_WRITE: u64 = 8;
pub const BATCH_TRUNCATE: u64 = 16;
pub const STREAMING_TRUNCATE: u64 = 32;
pub const ADMISSION_CONTROL: u64 = 64;
pub const READ_BATCH: i32 = 0;
pub const READ_MICRO_BATCH: i32 = 1;
pub const READ_ALL_AVAILABLE: i32 = 0;
pub const READ_MAX_ROWS: i32 = 1;
pub const WRITE_APPEND: i32 = 0;
pub const WRITE_TRUNCATE: i32 = 1;
pub const LOG_ERROR: i32 = 1;

pub type SparkDsSink = Option<unsafe extern "C" fn(*mut c_void, *const u8, i64)>;
pub type Interrupted = unsafe extern "C" fn(*const SparkDsTaskContext) -> bool;

// What Spark passes to the library.

#[repr(C)]
pub struct SparkDsHost {
    pub size: i64,
    pub abi_version: i32,
    pub spark_version: *const c_char,
    pub log: Option<LogFunction>,
    pub is_log_enabled: Option<LogEnabledFunction>,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct SparkDsCreateInfo {
    pub size: i64,
    pub name: *const c_char,
    pub option_keys: *const *const c_char,
    pub option_values: *const *const c_char,
    pub num_options: i64,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct SparkDsReaderInfo {
    pub size: i64,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct SparkDsPartitionsInfo {
    pub size: i64,
}

/// The domain of a column, which `push_domains` gets. The SDK binds it, so that the function
/// table has the size of the header, but does not expose it yet.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct SparkDsDomain {
    pub size: i64,
    pub path: *const i32,
    pub path_length: i64,
    pub null_allowed: bool,
    pub nan_allowed: bool,
    pub ranges: *mut ArrowArray,
    pub ranges_schema: *mut ArrowSchema,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct SparkDsStreamReaderInfo {
    pub size: i64,
    pub checkpoint_location: *const c_char,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct SparkDsInitialOffsetInfo {
    pub size: i64,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct SparkDsReadLimit {
    pub size: i64,
    pub kind: i32,
    pub max_rows: i64,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct SparkDsLatestOffsetInfo {
    pub size: i64,
    pub start: *const c_char,
    pub limit: *const SparkDsReadLimit,
    pub run_end: *const c_char,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct SparkDsStreamPartitionsInfo {
    pub size: i64,
    pub start: *const c_char,
    pub end: *const c_char,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct SparkDsCommitOffsetInfo {
    pub size: i64,
    pub end: *const c_char,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct SparkDsTaskContext {
    pub size: i64,
    pub stage_id: i32,
    pub stage_attempt_number: i32,
    pub partition_id: i32,
    pub attempt_number: i32,
    pub task_attempt_id: i64,
    pub is_interrupted: Option<Interrupted>,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct SparkDsReadInfo {
    pub size: i64,
    pub kind: i32,
    pub schema: *mut ArrowSchema,
    pub reader_state: *const u8,
    pub reader_state_length: i64,
    pub partition: *const u8,
    pub partition_length: i64,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct SparkDsWriteInfo {
    pub size: i64,
    pub mode: i32,
    pub query_id: *const c_char,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct SparkDsDataWriterInfo {
    pub size: i64,
    pub schema: *mut ArrowSchema,
    pub writer_state: *const u8,
    pub writer_state_length: i64,
    pub epoch_id: i64,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct SparkDsFinishWriteInfo {
    pub size: i64,
    pub epoch_id: i64,
    pub messages: *const *const u8,
    pub message_lengths: *const i64,
    pub num_messages: i64,
    pub rejected: i64,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct SparkDsFinishDataWriterInfo {
    pub size: i64,
}

// What the library returns to Spark.

/// The complete version 1 table. Unsupported operations have null function slots.
#[repr(C)]
pub struct SparkDsLibrary {
    pub size: i64,
    pub abi_version: i32,
    pub last_error: Option<unsafe extern "C" fn() -> *const c_char>,
    pub create:
        Option<unsafe extern "C" fn(*const SparkDsCreateInfo, *mut u64, *mut *mut c_void) -> c_int>,
    pub schema: Option<unsafe extern "C" fn(*mut c_void, *mut ArrowSchema) -> c_int>,
    pub create_reader: Option<
        unsafe extern "C" fn(
            *mut c_void,
            *mut ArrowSchema,
            *const SparkDsReaderInfo,
            *mut *mut c_void,
        ) -> c_int,
    >,
    pub create_stream_reader: Option<
        unsafe extern "C" fn(
            *mut c_void,
            *mut ArrowSchema,
            *const SparkDsStreamReaderInfo,
            *mut *mut c_void,
        ) -> c_int,
    >,
    pub create_writer: Option<
        unsafe extern "C" fn(
            *mut c_void,
            *mut ArrowSchema,
            *const SparkDsWriteInfo,
            SparkDsSink,
            *mut c_void,
            *mut *mut c_void,
        ) -> c_int,
    >,
    pub close: Option<unsafe extern "C" fn(*mut c_void)>,
    pub push_limit: Option<unsafe extern "C" fn(*mut c_void, i32, *mut bool) -> c_int>,
    pub prune_columns:
        Option<unsafe extern "C" fn(*mut c_void, *const *const c_char, i64) -> c_int>,
    pub partitions: Option<
        unsafe extern "C" fn(
            *mut c_void,
            *const SparkDsPartitionsInfo,
            SparkDsSink,
            SparkDsSink,
            *mut c_void,
        ) -> c_int,
    >,
    pub close_reader: Option<unsafe extern "C" fn(*mut c_void)>,
    pub initial_offset: Option<
        unsafe extern "C" fn(
            *mut c_void,
            *const SparkDsInitialOffsetInfo,
            SparkDsSink,
            *mut c_void,
        ) -> c_int,
    >,
    pub latest_offset: Option<
        unsafe extern "C" fn(
            *mut c_void,
            *const SparkDsLatestOffsetInfo,
            SparkDsSink,
            *mut c_void,
        ) -> c_int,
    >,
    pub stream_partitions: Option<
        unsafe extern "C" fn(
            *mut c_void,
            *const SparkDsStreamPartitionsInfo,
            SparkDsSink,
            SparkDsSink,
            *mut c_void,
        ) -> c_int,
    >,
    pub commit_offset:
        Option<unsafe extern "C" fn(*mut c_void, *const SparkDsCommitOffsetInfo) -> c_int>,
    pub close_stream_reader: Option<unsafe extern "C" fn(*mut c_void)>,
    pub read: Option<
        unsafe extern "C" fn(
            *const SparkDsTaskContext,
            *const SparkDsReadInfo,
            *mut ArrowArrayStream,
        ) -> c_int,
    >,
    pub commit: Option<unsafe extern "C" fn(*mut c_void, *const SparkDsFinishWriteInfo) -> c_int>,
    pub abort: Option<unsafe extern "C" fn(*mut c_void, *const SparkDsFinishWriteInfo) -> c_int>,
    pub close_writer: Option<unsafe extern "C" fn(*mut c_void)>,
    pub create_data_writer: Option<
        unsafe extern "C" fn(
            *const SparkDsTaskContext,
            *const SparkDsDataWriterInfo,
            *mut *mut c_void,
        ) -> c_int,
    >,
    pub write: Option<unsafe extern "C" fn(*mut c_void, *mut ArrowArray) -> c_int>,
    pub commit_data_writer: Option<
        unsafe extern "C" fn(
            *mut c_void,
            *const SparkDsFinishDataWriterInfo,
            SparkDsSink,
            *mut c_void,
        ) -> c_int,
    >,
    pub abort_data_writer:
        Option<unsafe extern "C" fn(*mut c_void, *const SparkDsFinishDataWriterInfo) -> c_int>,
    pub close_data_writer: Option<unsafe extern "C" fn(*mut c_void)>,
    pub default_read_limit:
        Option<unsafe extern "C" fn(*mut c_void, *mut SparkDsReadLimit) -> c_int>,
    // Added to version 1 by SPARK-61200, after the other functions.
    pub push_domains: Option<
        unsafe extern "C" fn(*mut c_void, *const *const SparkDsDomain, i64, *mut bool) -> c_int,
    >,
}

// The sizes and field offsets of the header on 64-bit targets, so that a binding that does
// not match it fails to compile wherever the SDK builds, including in each library.
macro_rules! layout {
    ($($name:ident: $size:literal { $($field:ident: $offset:literal),* $(,)? })*) => {
        #[cfg(target_pointer_width = "64")]
        const _: () = {
            $(
                assert!(size_of::<$name>() == $size);
                $(assert!(offset_of!($name, $field) == $offset);)*
            )*
        };
    };
}

layout! {
    SparkDsHost: 40 { size: 0, abi_version: 8, spark_version: 16, log: 24, is_log_enabled: 32 }
    SparkDsCreateInfo: 40 {
        size: 0, name: 8, option_keys: 16, option_values: 24, num_options: 32,
    }
    SparkDsReaderInfo: 8 { size: 0 }
    SparkDsPartitionsInfo: 8 { size: 0 }
    SparkDsDomain: 48 {
        size: 0, path: 8, path_length: 16, null_allowed: 24, nan_allowed: 25, ranges: 32,
        ranges_schema: 40,
    }
    SparkDsStreamReaderInfo: 16 { size: 0, checkpoint_location: 8 }
    SparkDsInitialOffsetInfo: 8 { size: 0 }
    SparkDsReadLimit: 24 { size: 0, kind: 8, max_rows: 16 }
    SparkDsLatestOffsetInfo: 32 { size: 0, start: 8, limit: 16, run_end: 24 }
    SparkDsStreamPartitionsInfo: 24 { size: 0, start: 8, end: 16 }
    SparkDsCommitOffsetInfo: 16 { size: 0, end: 8 }
    SparkDsTaskContext: 40 {
        size: 0, stage_id: 8, stage_attempt_number: 12, partition_id: 16, attempt_number: 20,
        task_attempt_id: 24, is_interrupted: 32,
    }
    SparkDsReadInfo: 56 {
        size: 0, kind: 8, schema: 16, reader_state: 24, reader_state_length: 32, partition: 40,
        partition_length: 48,
    }
    SparkDsWriteInfo: 24 { size: 0, mode: 8, query_id: 16 }
    SparkDsDataWriterInfo: 40 {
        size: 0, schema: 8, writer_state: 16, writer_state_length: 24, epoch_id: 32,
    }
    SparkDsFinishWriteInfo: 48 {
        size: 0, epoch_id: 8, messages: 16, message_lengths: 24, num_messages: 32, rejected: 40,
    }
    SparkDsFinishDataWriterInfo: 8 { size: 0 }
    SparkDsLibrary: 232 {
        size: 0, abi_version: 8, last_error: 16, create: 24, schema: 32, create_reader: 40,
        create_stream_reader: 48, create_writer: 56, close: 64, push_limit: 72,
        prune_columns: 80, partitions: 88, close_reader: 96, initial_offset: 104,
        latest_offset: 112, stream_partitions: 120, commit_offset: 128,
        close_stream_reader: 136, read: 144, commit: 152, abort: 160, close_writer: 168,
        create_data_writer: 176, write: 184, commit_data_writer: 192, abort_data_writer: 200,
        close_data_writer: 208, default_read_limit: 216, push_domains: 224,
    }
}

thread_local! {
    static LAST_ERROR: RefCell<Option<CString>> = const { RefCell::new(None) };
}

/// Converts provider failures and unwinding panics into the C error convention.
pub fn call(action: impl FnOnce() -> Result<()>) -> c_int {
    LAST_ERROR.with(|error| *error.borrow_mut() = None);
    let Err(message) = provider_call(action) else {
        return 0;
    };
    LAST_ERROR.with(|error| *error.borrow_mut() = Some(c_message(message)));
    1
}

/// Returns the calling thread's error until its next adapter call.
///
/// # Safety
/// The returned pointer must not be freed or used after another adapter call on this thread.
pub unsafe extern "C" fn last_error() -> *const c_char {
    LAST_ERROR.with(|error| {
        error
            .borrow()
            .as_ref()
            .map_or(std::ptr::null(), |message| message.as_ptr())
    })
}

/// Reads only a field that fits in a sized C structure.
///
/// # Safety
/// `pointer` must address `size` readable bytes, and `offset` must identify a field of type `T`.
pub unsafe fn field<T: Copy>(pointer: *const u8, size: i64, offset: usize) -> Option<T> {
    let end = offset.checked_add(size_of::<T>())?;
    if usize::try_from(size).ok()? < end {
        return None;
    }
    Some(unsafe { pointer.add(offset).cast::<T>().read_unaligned() })
}

/// # Safety
/// A non-null pointer must address the size prefix of a live C structure.
pub unsafe fn struct_size<T>(pointer: *const T) -> Result<i64> {
    if pointer.is_null() {
        return Err("null sized structure".into());
    }
    let size = unsafe { pointer.cast::<i64>().read_unaligned() };
    if size < size_of::<i64>() as i64 {
        return Err("structure is smaller than its size field".into());
    }
    Ok(size)
}

/// Copies a structure that Spark passes, which has every field of version 1 of the ABI: fields
/// are only appended to the structures, so a shorter one is an error, not partly read.
///
/// # Safety
/// A non-null pointer must address a live structure whose first field is its size.
pub unsafe fn input<T: Copy>(pointer: *const T) -> Result<T> {
    let size = unsafe { struct_size(pointer)? };
    if size < size_of::<T>() as i64 {
        let name = std::any::type_name::<T>()
            .rsplit("::")
            .next()
            .unwrap_or("structure");
        return Err(
            format!("{name} has {size} bytes, fewer than in version 1 of the native ABI").into(),
        );
    }
    Ok(unsafe { pointer.read_unaligned() })
}

pub fn require_output<T>(pointer: *mut T) -> Result<()> {
    if pointer.is_null() {
        return Err("null output pointer".into());
    }
    Ok(())
}

/// Negotiates the ABI version, installs the host's log, and runs the source's `init`.
///
/// # Safety
/// `host` and `output` must follow the `spark_ds_init` contract of the C header.
pub unsafe fn initialize(
    host: *const SparkDsHost,
    output: *mut *const SparkDsLibrary,
    library: &'static SparkDsLibrary,
    init: fn(&HostInfo) -> Result<()>,
) -> c_int {
    let result = call(|| {
        require_output(output)?;
        unsafe { output.write(std::ptr::null()) };
        let size = unsafe { struct_size(host)? };
        let pointer = host.cast();
        let version: i32 = unsafe { field(pointer, size, offset_of!(SparkDsHost, abi_version)) }
            .ok_or("host does not provide an ABI version")?;
        if version < ABI_VERSION {
            return Err(format!("host ABI {version} does not support ABI {ABI_VERSION}").into());
        }
        let log: Option<Option<LogFunction>> =
            unsafe { field(pointer, size, offset_of!(SparkDsHost, log)) };
        let is_log_enabled: Option<Option<LogEnabledFunction>> =
            unsafe { field(pointer, size, offset_of!(SparkDsHost, is_log_enabled)) };
        host::install(log.flatten(), is_log_enabled.flatten());
        let spark_version: Option<*const c_char> =
            unsafe { field(pointer, size, offset_of!(SparkDsHost, spark_version)) };
        let spark_version = spark_version
            .filter(|version| !version.is_null())
            .and_then(|version| unsafe { CStr::from_ptr(version) }.to_str().ok())
            .map(str::to_owned);
        init(&HostInfo {
            abi_version: version,
            spark_version,
        })?;
        unsafe { output.write(library) };
        Ok(())
    });
    if result != 0 {
        if let Ok(size) = unsafe { struct_size(host) } {
            let log: Option<Option<LogFunction>> =
                unsafe { field(host.cast(), size, offset_of!(SparkDsHost, log)) };
            if let Some(Some(log)) = log {
                unsafe { log(LOG_ERROR, last_error()) };
            }
        }
    }
    result
}

/// # Safety
/// `data` must address `length` initialized values that remain valid for the returned borrow.
pub unsafe fn slice<'a, T>(data: *const T, length: i64) -> Result<&'a [T]> {
    let length = usize::try_from(length)?;
    if length == 0 {
        return Ok(&[]);
    }
    if data.is_null()
        || length
            .checked_mul(size_of::<T>())
            .is_none_or(|size| size > isize::MAX as usize)
    {
        return Err("invalid array pointer or length".into());
    }
    Ok(unsafe { std::slice::from_raw_parts(data, length) })
}

/// # Safety
/// A non-null `pointer` must address a NUL-terminated string, readable during this call.
pub unsafe fn string(pointer: *const c_char) -> Result<String> {
    if pointer.is_null() {
        return Err("null string".into());
    }
    Ok(unsafe { CStr::from_ptr(pointer) }.to_str()?.to_owned())
}

/// # Safety
/// As for [`string`], for a pointer that may be null.
pub unsafe fn optional_string(pointer: *const c_char) -> Result<Option<String>> {
    if pointer.is_null() {
        Ok(None)
    } else {
        Ok(Some(unsafe { string(pointer)? }))
    }
}

/// # Safety
/// Each element must be a NUL-terminated UTF-8 string, readable during this call.
pub unsafe fn strings(data: *const *const c_char, length: i64) -> Result<Vec<String>> {
    unsafe { slice(data, length)? }
        .iter()
        .map(|pointer| unsafe { string(*pointer) })
        .collect()
}

/// # Safety
/// `info` must follow the `create` contract of the C header.
pub unsafe fn create_info(info: *const SparkDsCreateInfo) -> Result<CreateInfo> {
    let info = unsafe { input(info)? };
    if info.name.is_null() {
        return Err("create info has a null name".into());
    }
    let name = unsafe { string(info.name)? };
    let keys = unsafe { strings(info.option_keys, info.num_options)? };
    let values = unsafe { strings(info.option_values, info.num_options)? };
    let options: Options = keys.into_iter().zip(values).collect();
    Ok(CreateInfo::new(name, options))
}

/// The mode of a write, and the ID of the query of a streaming write.
///
/// # Safety
/// `info` must follow the create_writer contract of the C header.
pub unsafe fn write_info(info: *const SparkDsWriteInfo) -> Result<(WriteMode, Option<String>)> {
    let info = unsafe { input(info)? };
    let mode = match info.mode {
        WRITE_APPEND => WriteMode::Append,
        WRITE_TRUNCATE => WriteMode::Truncate,
        mode => return Err(format!("unsupported native write mode: {mode}").into()),
    };
    Ok((mode, unsafe { optional_string(info.query_id)? }))
}

/// The start, the limit, and the end of the run of a request for the latest offset, with the
/// offsets as JSON.
///
/// # Safety
/// `info` must follow the latest_offset contract of the C header.
pub unsafe fn latest_offset_info(
    info: *const SparkDsLatestOffsetInfo,
) -> Result<LatestOffsetInfo<String>> {
    let info = unsafe { input(info)? };
    let limit = unsafe { input(info.limit)? };
    let limit = match limit.kind {
        READ_ALL_AVAILABLE => ReadLimit::AllAvailable,
        READ_MAX_ROWS => ReadLimit::MaxRows(u64::try_from(limit.max_rows)?),
        kind => return Err(format!("unsupported kind of read limit: {kind}").into()),
    };
    Ok(LatestOffsetInfo {
        start: unsafe { string(info.start)? },
        limit,
        run_end: unsafe { optional_string(info.run_end)? },
    })
}

/// Sets the limit that `default_read_limit` returns.
///
/// # Safety
/// `output` must be the SparkDsReadLimit that Spark initialized with its size.
pub unsafe fn set_read_limit(output: *mut SparkDsReadLimit, limit: ReadLimit) -> Result<()> {
    let mut value = unsafe { input(output.cast_const())? };
    match limit {
        ReadLimit::AllAvailable => value.kind = READ_ALL_AVAILABLE,
        ReadLimit::MaxRows(rows) => {
            value.kind = READ_MAX_ROWS;
            value.max_rows = i64::try_from(rows)
                .map_err(|_| format!("a read limit of {rows} rows exceeds the native ABI"))?;
        }
    }
    unsafe { output.write_unaligned(value) };
    Ok(())
}

/// # Safety
/// Both arrays must contain count readable values. Non-null message pointers
/// must address their lengths of bytes and remain valid for the returned borrow.
pub unsafe fn messages<'a>(
    pointers: *const *const u8,
    lengths: *const i64,
    count: i64,
) -> Result<Vec<Option<&'a [u8]>>> {
    let pointers = unsafe { slice(pointers, count)? };
    let lengths = unsafe { slice(lengths, count)? };
    pointers
        .iter()
        .zip(lengths)
        .map(|(&pointer, &length)| {
            if pointer.is_null() {
                if length != 0 {
                    return Err("null commit message must have zero length".into());
                }
                Ok(None)
            } else {
                Ok(Some(unsafe { slice(pointer, length)? }))
            }
        })
        .collect()
}

/// # Safety
/// The sink must be valid on this thread for this call and synchronously copy the bytes.
pub unsafe fn emit(callback: SparkDsSink, sink: *mut c_void, bytes: &[u8]) -> Result<()> {
    let callback = callback.ok_or("null result sink")?;
    unsafe { callback(sink, bytes.as_ptr(), bytes.len().try_into()?) };
    Ok(())
}

/// # Safety
/// `input` must be a live ArrowSchema that this call is permitted to move.
pub unsafe fn import_schema(input: *mut ArrowSchema) -> Result<SchemaRef> {
    require_output(input)?;
    let schema = unsafe { ArrowSchema::from_raw(input) };
    Ok(Arc::new(Schema::try_from(&schema)?))
}

/// # Safety
/// `output` must be writable ArrowSchema storage with a null release callback.
pub unsafe fn export_schema(schema: &SchemaRef, output: *mut ArrowSchema) -> Result<()> {
    require_output(output)?;
    let schema = ArrowSchema::try_from(schema.as_ref())?;
    unsafe { output.write(schema) };
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn errors_and_panics_are_thread_local_c_strings() {
        assert_ne!(call(|| Err("failed\0operation".into())), 0);
        let error = unsafe { CStr::from_ptr(last_error()) }.to_str().unwrap();
        assert_eq!(error, "failed\\0operation");
        std::thread::spawn(|| {
            assert!(unsafe { last_error() }.is_null());
            assert_ne!(call(|| panic!("provider panic")), 0);
            let error = unsafe { CStr::from_ptr(last_error()) }.to_str().unwrap();
            assert!(error.contains("provider panic"));
        })
        .join()
        .unwrap();
        assert_eq!(
            unsafe { CStr::from_ptr(last_error()) }.to_str().unwrap(),
            error
        );
        assert_eq!(call(|| Ok(())), 0);
        assert!(unsafe { last_error() }.is_null());
    }

    #[test]
    fn empty_arrays_accept_null_and_truncated_structures_fail() {
        assert!(unsafe { strings(std::ptr::null(), 0) }.unwrap().is_empty());
        let prefix = size_of::<i64>() as i64;
        let info = (&prefix as *const i64).cast::<SparkDsCreateInfo>();
        let error = unsafe { create_info(info) }.unwrap_err().to_string();
        assert_eq!(
            error,
            "SparkDsCreateInfo has 8 bytes, fewer than in version 1 of the native ABI"
        );
        assert!(unsafe { input::<SparkDsReadInfo>(std::ptr::null()) }.is_err());
    }

    #[test]
    fn create_info_carries_the_source_name_and_options() {
        let keys = [c"path".as_ptr()];
        let values = [c"/tmp/data".as_ptr()];
        let mut info = SparkDsCreateInfo {
            size: size_of::<SparkDsCreateInfo>() as i64,
            name: c"my_source".as_ptr(),
            option_keys: keys.as_ptr(),
            option_values: values.as_ptr(),
            num_options: 1,
        };
        let created = unsafe { create_info(&info) }.unwrap();
        assert_eq!(created.name, "my_source");
        assert_eq!(
            created.options,
            Options::from([("path".into(), "/tmp/data".into())])
        );
        info.name = std::ptr::null();
        assert!(unsafe { create_info(&info) }.is_err());
    }

    #[test]
    fn write_info_carries_the_mode_and_the_query_of_a_streaming_write() {
        let mut info = SparkDsWriteInfo {
            size: size_of::<SparkDsWriteInfo>() as i64,
            mode: WRITE_APPEND,
            query_id: std::ptr::null(),
        };
        assert_eq!(
            unsafe { write_info(&info) }.unwrap(),
            (WriteMode::Append, None)
        );
        info.mode = WRITE_TRUNCATE;
        info.query_id = c"stream".as_ptr();
        assert_eq!(
            unsafe { write_info(&info) }.unwrap(),
            (WriteMode::Truncate, Some("stream".into()))
        );
        info.mode = 2;
        assert!(unsafe { write_info(&info) }.is_err());
    }

    #[test]
    fn latest_offset_info_carries_the_start_the_limit_and_the_end_of_the_run() {
        let mut limit = SparkDsReadLimit {
            size: size_of::<SparkDsReadLimit>() as i64,
            kind: READ_ALL_AVAILABLE,
            max_rows: 0,
        };
        let mut info = SparkDsLatestOffsetInfo {
            size: size_of::<SparkDsLatestOffsetInfo>() as i64,
            start: c"{\"next\":3}".as_ptr(),
            limit: &limit,
            run_end: std::ptr::null(),
        };
        let parsed = unsafe { latest_offset_info(&info) }.unwrap();
        assert_eq!(parsed.start, r#"{"next":3}"#);
        assert_eq!(parsed.limit, ReadLimit::AllAvailable);
        assert_eq!(parsed.run_end, None);

        limit.kind = READ_MAX_ROWS;
        limit.max_rows = 5;
        info.limit = &limit;
        info.run_end = c"{\"next\":9}".as_ptr();
        let parsed = unsafe { latest_offset_info(&info) }.unwrap();
        assert_eq!(parsed.limit, ReadLimit::MaxRows(5));
        assert_eq!(parsed.run_end.as_deref(), Some(r#"{"next":9}"#));

        limit.kind = 7;
        info.limit = &limit;
        assert!(unsafe { latest_offset_info(&info) }.is_err());
    }

    #[test]
    fn default_read_limits_set_the_kind_and_the_rows() {
        let mut limit = SparkDsReadLimit {
            size: size_of::<SparkDsReadLimit>() as i64,
            kind: READ_ALL_AVAILABLE,
            max_rows: 0,
        };
        unsafe { set_read_limit(&mut limit, ReadLimit::MaxRows(4)) }.unwrap();
        assert_eq!((limit.kind, limit.max_rows), (READ_MAX_ROWS, 4));
        unsafe { set_read_limit(&mut limit, ReadLimit::AllAvailable) }.unwrap();
        assert_eq!(limit.kind, READ_ALL_AVAILABLE);
        assert!(unsafe { set_read_limit(&mut limit, ReadLimit::MaxRows(u64::MAX)) }.is_err());
    }

    #[test]
    fn abort_messages_distinguish_missing_and_empty_messages() {
        let bytes = [7_u8, 8];
        let pointers = [std::ptr::null(), bytes.as_ptr(), bytes.as_ptr()];
        let lengths = [0, 0, 2];
        let values = unsafe { messages(pointers.as_ptr(), lengths.as_ptr(), 3) }.unwrap();
        assert_eq!(values, vec![None, Some(&[][..]), Some(&bytes[..])]);
        assert!(unsafe { messages(std::ptr::null(), std::ptr::null(), 0) }
            .unwrap()
            .is_empty());
        assert!(unsafe { messages(std::ptr::null(), std::ptr::null(), -1) }.is_err());
        let invalid_lengths = [1, 0, 2];
        assert!(unsafe { messages(pointers.as_ptr(), invalid_lengths.as_ptr(), 3) }.is_err());
        let invalid_lengths = [0, -1, 2];
        assert!(unsafe { messages(pointers.as_ptr(), invalid_lengths.as_ptr(), 3) }.is_err());
    }
}
