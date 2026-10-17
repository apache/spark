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
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::Arc;

pub use arrow_array::ffi::FFI_ArrowArray as ArrowArray;
pub use arrow_array::ffi_stream::FFI_ArrowArrayStream as ArrowArrayStream;
pub use arrow_schema::ffi::FFI_ArrowSchema as ArrowSchema;
use arrow_schema::{Schema, SchemaRef};

use crate::bridge::panic_message;
use crate::{FilterSupport, Options, Result, WriteMode};

pub const ABI_VERSION: i32 = 1;
pub const BATCH_READ: u64 = 1;
pub const BATCH_WRITE: u64 = 4;
pub const TRUNCATE: u64 = 16;
pub const LOG_ERROR: i32 = 1;

pub type SparkDsSink = Option<unsafe extern "C" fn(*mut c_void, *const u8, i64)>;
pub type Interrupted = unsafe extern "C" fn(*const SparkDsTaskContext) -> bool;

#[repr(C)]
pub struct SparkDsHost {
    pub size: i64,
    pub abi_version: i32,
    pub spark_version: *const c_char,
    pub log: Option<unsafe extern "C" fn(i32, *const c_char)>,
}

#[repr(C)]
pub struct SparkDsCreateInfo {
    pub size: i64,
    pub name: *const c_char,
    pub option_keys: *const *const c_char,
    pub option_values: *const *const c_char,
    pub num_options: i64,
}

#[repr(C)]
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
pub struct SparkDsWriteInfo {
    pub size: i64,
    pub mode: i32,
    pub query_id: *const c_char,
}

/// The complete version 1 table. Unsupported operations have null function slots.
#[repr(C)]
pub struct SparkDsLibrary {
    pub size: i64,
    pub abi_version: i32,
    pub last_error: Option<unsafe extern "C" fn() -> *const c_char>,
    pub create:
        Option<unsafe extern "C" fn(*const SparkDsCreateInfo, *mut u64, *mut *mut c_void) -> c_int>,
    pub schema: Option<unsafe extern "C" fn(*mut c_void, *mut ArrowSchema) -> c_int>,
    pub create_reader:
        Option<unsafe extern "C" fn(*mut c_void, *mut ArrowSchema, *mut *mut c_void) -> c_int>,
    pub create_stream_reader:
        Option<unsafe extern "C" fn(*mut c_void, *mut ArrowSchema, *mut *mut c_void) -> c_int>,
    pub create_writer: Option<
        unsafe extern "C" fn(
            *mut c_void,
            *mut ArrowSchema,
            *const SparkDsWriteInfo,
            *mut *mut c_void,
        ) -> c_int,
    >,
    pub close: Option<unsafe extern "C" fn(*mut c_void)>,
    pub push_predicates:
        Option<unsafe extern "C" fn(*mut c_void, *const *const c_char, i64, *mut i32) -> c_int>,
    pub push_limit: Option<unsafe extern "C" fn(*mut c_void, i32, *mut bool) -> c_int>,
    pub prune_columns:
        Option<unsafe extern "C" fn(*mut c_void, *const *const c_char, i64, *mut bool) -> c_int>,
    pub partitions: Option<unsafe extern "C" fn(*mut c_void, SparkDsSink, *mut c_void) -> c_int>,
    pub serialize_reader:
        Option<unsafe extern "C" fn(*mut c_void, SparkDsSink, *mut c_void) -> c_int>,
    pub close_reader: Option<unsafe extern "C" fn(*mut c_void)>,
    pub initial_offset:
        Option<unsafe extern "C" fn(*mut c_void, SparkDsSink, *mut c_void) -> c_int>,
    pub latest_offset: Option<unsafe extern "C" fn(*mut c_void, SparkDsSink, *mut c_void) -> c_int>,
    pub stream_partitions: Option<
        unsafe extern "C" fn(
            *mut c_void,
            *const c_char,
            *const c_char,
            SparkDsSink,
            *mut c_void,
        ) -> c_int,
    >,
    pub serialize_stream_reader:
        Option<unsafe extern "C" fn(*mut c_void, SparkDsSink, *mut c_void) -> c_int>,
    pub commit_offset: Option<unsafe extern "C" fn(*mut c_void, *const c_char) -> c_int>,
    pub close_stream_reader: Option<unsafe extern "C" fn(*mut c_void)>,
    pub read: Option<
        unsafe extern "C" fn(
            *const SparkDsTaskContext,
            *const u8,
            i64,
            *const u8,
            i64,
            *mut ArrowArrayStream,
        ) -> c_int,
    >,
    pub serialize_writer:
        Option<unsafe extern "C" fn(*mut c_void, SparkDsSink, *mut c_void) -> c_int>,
    pub commit:
        Option<unsafe extern "C" fn(*mut c_void, i64, *const *const u8, *const i64, i64) -> c_int>,
    pub abort:
        Option<unsafe extern "C" fn(*mut c_void, i64, *const *const u8, *const i64, i64) -> c_int>,
    pub close_writer: Option<unsafe extern "C" fn(*mut c_void)>,
    pub create_data_writer: Option<
        unsafe extern "C" fn(
            *const SparkDsTaskContext,
            *const u8,
            i64,
            i64,
            *mut *mut c_void,
        ) -> c_int,
    >,
    pub write:
        Option<unsafe extern "C" fn(*mut c_void, *mut ArrowArray, *mut ArrowSchema) -> c_int>,
    pub commit_data_writer:
        Option<unsafe extern "C" fn(*mut c_void, SparkDsSink, *mut c_void) -> c_int>,
    pub abort_data_writer: Option<unsafe extern "C" fn(*mut c_void) -> c_int>,
    pub close_data_writer: Option<unsafe extern "C" fn(*mut c_void)>,
}

thread_local! {
    static LAST_ERROR: RefCell<Option<CString>> = const { RefCell::new(None) };
}

/// Converts provider failures and unwinding panics into the C error convention.
pub fn call(action: impl FnOnce() -> Result<()>) -> c_int {
    LAST_ERROR.with(|error| *error.borrow_mut() = None);
    let message = match catch_unwind(AssertUnwindSafe(|| action().map_err(|e| e.to_string()))) {
        Ok(Ok(())) => return 0,
        Ok(Err(message)) => message,
        Err(payload) => panic_message(payload),
    };
    let message = CString::new(message.replace('\0', "\\0")).expect("NULs removed");
    LAST_ERROR.with(|error| *error.borrow_mut() = Some(message));
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

pub fn require_output<T>(pointer: *mut T) -> Result<()> {
    if pointer.is_null() {
        return Err("null output pointer".into());
    }
    Ok(())
}

/// # Safety
/// `host` and `output` must follow the `spark_ds_init` contract of the C header.
pub unsafe fn initialize(
    host: *const SparkDsHost,
    output: *mut *const SparkDsLibrary,
    library: &'static SparkDsLibrary,
) -> c_int {
    let result = call(|| {
        require_output(output)?;
        unsafe { output.write(std::ptr::null()) };
        let size = unsafe { struct_size(host)? };
        let version: i32 =
            unsafe { field(host.cast(), size, offset_of!(SparkDsHost, abi_version)) }
                .ok_or("host does not provide an ABI version")?;
        if version < ABI_VERSION {
            return Err(format!("host ABI {version} does not support ABI {ABI_VERSION}").into());
        }
        unsafe { output.write(library) };
        Ok(())
    });
    if result != 0 {
        if let Ok(size) = unsafe { struct_size(host) } {
            let log: Option<Option<unsafe extern "C" fn(i32, *const c_char)>> =
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
/// Each element must be a NUL-terminated UTF-8 string, readable during this call.
pub unsafe fn strings(data: *const *const c_char, length: i64) -> Result<Vec<String>> {
    unsafe { slice(data, length)? }
        .iter()
        .map(|pointer| {
            if pointer.is_null() {
                return Err("null string".into());
            }
            Ok(unsafe { CStr::from_ptr(*pointer) }.to_str()?.to_owned())
        })
        .collect()
}

/// # Safety
/// `info` must follow the `create` contract of the C header.
pub unsafe fn options(info: *const SparkDsCreateInfo) -> Result<Options> {
    let size = unsafe { struct_size(info)? };
    let pointer = info.cast();
    let keys = unsafe { field(pointer, size, offset_of!(SparkDsCreateInfo, option_keys)) }
        .ok_or("create info does not provide option keys")?;
    let values = unsafe { field(pointer, size, offset_of!(SparkDsCreateInfo, option_values)) }
        .ok_or("create info does not provide option values")?;
    let count = unsafe { field(pointer, size, offset_of!(SparkDsCreateInfo, num_options)) }
        .ok_or("create info does not provide an option count")?;
    Ok(unsafe { strings(keys, count)? }
        .into_iter()
        .zip(unsafe { strings(values, count)? })
        .collect())
}

/// # Safety
/// `info` must follow the create_writer contract of the C header.
pub unsafe fn write_mode(info: *const SparkDsWriteInfo) -> Result<WriteMode> {
    let size = unsafe { struct_size(info)? };
    let pointer = info.cast();
    let mode: i32 = unsafe { field(pointer, size, offset_of!(SparkDsWriteInfo, mode)) }
        .ok_or("write info does not provide a mode")?;
    let query_id: *const c_char =
        unsafe { field(pointer, size, offset_of!(SparkDsWriteInfo, query_id)) }
            .ok_or("write info does not provide a query ID")?;
    if !query_id.is_null() {
        return Err("streaming writes are not supported".into());
    }
    match mode {
        0 => Ok(WriteMode::Append),
        1 => Ok(WriteMode::Truncate),
        _ => Err(format!("unsupported native write mode: {mode}").into()),
    }
}

pub fn batch_epoch(epoch: i64) -> Result<()> {
    if epoch != -1 {
        return Err("batch writes require epoch ID -1".into());
    }
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

pub fn predicate_use(support: FilterSupport) -> i32 {
    match support {
        FilterSupport::Unsupported => 0,
        FilterSupport::PruningOnly => 1,
        FilterSupport::Exact => 2,
    }
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
    fn empty_arrays_accept_null_and_truncated_create_info_fails() {
        assert!(unsafe { strings(std::ptr::null(), 0) }.unwrap().is_empty());
        let prefix = size_of::<i64>() as i64;
        let info = (&prefix as *const i64).cast::<SparkDsCreateInfo>();
        assert!(unsafe { options(info) }.is_err());
    }

    #[test]
    fn write_info_rejects_truncated_unknown_and_streaming_requests() {
        let prefix = size_of::<i64>() as i64;
        let truncated = (&prefix as *const i64).cast::<SparkDsWriteInfo>();
        assert!(unsafe { write_mode(truncated) }.is_err());
        let mut info = SparkDsWriteInfo {
            size: size_of::<SparkDsWriteInfo>() as i64,
            mode: 0,
            query_id: std::ptr::null(),
        };
        assert_eq!(unsafe { write_mode(&info) }.unwrap(), WriteMode::Append);
        info.mode = 1;
        assert_eq!(unsafe { write_mode(&info) }.unwrap(), WriteMode::Truncate);
        info.mode = 2;
        assert!(unsafe { write_mode(&info) }.is_err());
        info.mode = 0;
        info.query_id = c"stream".as_ptr();
        assert!(unsafe { write_mode(&info) }.is_err());
        assert!(batch_epoch(-1).is_ok());
        assert!(batch_epoch(0).is_err());
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
