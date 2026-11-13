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

use std::ffi::{c_char, c_int, c_void, CString};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::Arc;

use arrow_array::ffi::FFI_ArrowArray;
use arrow_array::ffi_stream::FFI_ArrowArrayStream;
use arrow_array::{Array, StructArray};
use arrow_schema::{ffi::FFI_ArrowSchema, Schema, SchemaRef};

use super::panic_message;
use crate::{BatchReader, Result};

struct StreamState {
    reader: Option<BatchReader>,
    schema: SchemaRef,
    last_error: Option<CString>,
    finished: bool,
}

impl Drop for StreamState {
    fn drop(&mut self) {
        // release has no error return channel, but it must never unwind into C.
        if let Some(reader) = self.reader.take() {
            if let Err(payload) = catch_unwind(AssertUnwindSafe(|| drop(reader))) {
                let _ = panic_message(payload);
            }
        }
    }
}

pub(super) fn export(reader: BatchReader) -> Result<FFI_ArrowArrayStream> {
    // Establish guarded destruction before calling the provider's schema().
    let mut state = Box::new(StreamState {
        reader: Some(reader),
        schema: Arc::new(Schema::empty()),
        last_error: None,
        finished: false,
    });
    state.schema = catch_unwind(AssertUnwindSafe(|| {
        state.reader.as_ref().expect("reader is present").schema()
    }))
    .map_err(panic_message)?;
    Ok(FFI_ArrowArrayStream {
        get_schema: Some(get_schema),
        get_next: Some(get_next),
        get_last_error: Some(get_last_error),
        release: Some(release),
        private_data: Box::into_raw(state).cast::<c_void>(),
    })
}

fn callback(state: &mut StreamState, action: impl FnOnce(&mut StreamState) -> Result<()>) -> c_int {
    let result = catch_unwind(AssertUnwindSafe(|| {
        action(state).map_err(|e| e.to_string())
    }));
    let message = match result {
        Ok(Ok(())) => return 0,
        Ok(Err(message)) => message,
        Err(payload) => panic_message(payload),
    };
    state.finished = true;
    // Error text is a C string. A provider error may contain embedded NULs.
    state.last_error = Some(CString::new(message.replace('\0', "\\0")).expect("NULs removed"));
    5 // EIO; get_last_error supplies the provider's actual error message.
}

unsafe extern "C" fn get_schema(
    stream: *mut FFI_ArrowArrayStream,
    output: *mut FFI_ArrowSchema,
) -> c_int {
    let state = unsafe { &mut *(*stream).private_data.cast::<StreamState>() };
    callback(state, |state| {
        let schema = FFI_ArrowSchema::try_from(state.schema.as_ref())?;
        unsafe { output.write(schema) };
        Ok(())
    })
}

unsafe extern "C" fn get_next(
    stream: *mut FFI_ArrowArrayStream,
    output: *mut FFI_ArrowArray,
) -> c_int {
    let state = unsafe { &mut *(*stream).private_data.cast::<StreamState>() };
    callback(state, |state| {
        if state.finished {
            unsafe { output.write(FFI_ArrowArray::empty()) };
            return Ok(());
        }
        match state.reader.as_mut().expect("reader is present").next() {
            Some(batch) => {
                let batch = batch?;
                if batch.schema() != state.schema {
                    return Err("reader returned a batch with a different schema".into());
                }
                let data = StructArray::from(batch).to_data();
                let array = FFI_ArrowArray::new(&data);
                unsafe { output.write(array) };
            }
            None => {
                state.finished = true;
                // A null release callback denotes EOF, including for zero-column data.
                unsafe { output.write(FFI_ArrowArray::empty()) };
            }
        }
        Ok(())
    })
}

unsafe extern "C" fn get_last_error(stream: *mut FFI_ArrowArrayStream) -> *const c_char {
    let state = unsafe { &*(*stream).private_data.cast::<StreamState>() };
    state
        .last_error
        .as_ref()
        .map_or(std::ptr::null(), |error| error.as_ptr())
}

unsafe extern "C" fn release(stream: *mut FFI_ArrowArrayStream) {
    let state = unsafe { (*stream).private_data.cast::<StreamState>() };
    // Mark released before destruction. The consumer owns the stream structure.
    unsafe { stream.write(FFI_ArrowArrayStream::empty()) };
    if let Err(payload) = catch_unwind(AssertUnwindSafe(|| unsafe { drop(Box::from_raw(state)) })) {
        let _ = panic_message(payload);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use arrow_array::ffi_stream::ArrowArrayStreamReader;
    use arrow_array::{Int64Array, RecordBatch, RecordBatchReader};
    use arrow_schema::{ArrowError, DataType, Field};

    use super::*;

    struct Reader {
        schema: SchemaRef,
        batches: std::vec::IntoIter<std::result::Result<RecordBatch, ArrowError>>,
        panic_next: bool,
        panic_schema: bool,
        panic_drop: bool,
        drops: Arc<AtomicUsize>,
    }

    impl Iterator for Reader {
        type Item = std::result::Result<RecordBatch, ArrowError>;

        fn next(&mut self) -> Option<Self::Item> {
            assert!(!self.panic_next, "iterator failed");
            self.batches.next()
        }
    }

    impl RecordBatchReader for Reader {
        fn schema(&self) -> SchemaRef {
            assert!(!self.panic_schema, "schema failed");
            self.schema.clone()
        }
    }

    impl Drop for Reader {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::SeqCst);
            assert!(!self.panic_drop, "drop failed");
        }
    }

    fn reader() -> Reader {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let values = Arc::new(Int64Array::from(vec![3, 5]));
        let batch = RecordBatch::try_new(schema.clone(), vec![values]).unwrap();
        Reader {
            schema,
            batches: vec![Ok(batch)].into_iter(),
            panic_next: false,
            panic_schema: false,
            panic_drop: false,
            drops: Arc::default(),
        }
    }

    #[test]
    fn imported_batch_survives_early_stream_release() {
        let reader = reader();
        let drops = reader.drops.clone();
        let mut stream =
            ArrowArrayStreamReader::try_new(export(Box::new(reader)).unwrap()).unwrap();
        let batch = stream.next().unwrap().unwrap();
        drop(stream);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        assert_eq!(batch.num_rows(), 2);
        let values = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(values.values(), &[3, 5]);
    }

    #[test]
    fn empty_batch_is_not_eof() {
        let mut reader = reader();
        let empty = RecordBatch::new_empty(reader.schema.clone());
        reader.batches = vec![Ok(empty), reader.batches.next().unwrap()].into_iter();
        let mut stream =
            ArrowArrayStreamReader::try_new(export(Box::new(reader)).unwrap()).unwrap();
        assert_eq!(stream.next().unwrap().unwrap().num_rows(), 0);
        assert_eq!(stream.next().unwrap().unwrap().num_rows(), 2);
        assert!(stream.next().is_none());
    }

    #[test]
    fn iterator_panics_and_nul_errors_become_stream_errors() {
        let mut panicking = reader();
        panicking.panic_next = true;
        let mut stream =
            ArrowArrayStreamReader::try_new(export(Box::new(panicking)).unwrap()).unwrap();
        assert!(stream
            .next()
            .unwrap()
            .unwrap_err()
            .to_string()
            .contains("iterator failed"));

        let mut failing = reader();
        failing.batches = vec![Err(ArrowError::ComputeError("bad\0value".into()))].into_iter();
        let mut stream =
            ArrowArrayStreamReader::try_new(export(Box::new(failing)).unwrap()).unwrap();
        assert!(stream
            .next()
            .unwrap()
            .unwrap_err()
            .to_string()
            .contains("bad\\0value"));
    }

    #[test]
    fn schema_and_drop_panics_do_not_escape_the_stream_boundary() {
        let mut failing = reader();
        let drops = failing.drops.clone();
        failing.panic_schema = true;
        failing.panic_drop = true;
        assert!(export(Box::new(failing))
            .unwrap_err()
            .to_string()
            .contains("schema failed"));
        assert_eq!(drops.load(Ordering::SeqCst), 1);

        let mut failing = reader();
        let drops = failing.drops.clone();
        failing.panic_drop = true;
        drop(export(Box::new(failing)).unwrap());
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }
}
