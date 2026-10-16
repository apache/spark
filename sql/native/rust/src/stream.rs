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
use arrow_data::ArrayData;
use arrow_schema::{
    ffi::FFI_ArrowSchema, DataType, Field, Fields, IntervalUnit, Schema, SchemaRef, TimeUnit,
};

use super::panic_message;
use crate::{BatchReader, Result};

struct StreamState<'task> {
    reader: Option<BatchReader<'task>>,
    schema: SchemaRef,
    last_error: Option<CString>,
    finished: bool,
}

impl Drop for StreamState<'_> {
    fn drop(&mut self) {
        // release has no error return channel, but it must never unwind into C.
        if let Some(reader) = self.reader.take() {
            if let Err(payload) = catch_unwind(AssertUnwindSafe(|| drop(reader))) {
                let _ = panic_message(payload);
            }
        }
    }
}

/// The reader may borrow the host task context until the consumer releases
/// this stream. Erasure into C private_data is confined to this boundary.
///
/// # Safety
/// The caller must keep everything borrowed by the reader alive until release,
/// and the consumer must obey the Arrow C Stream ownership protocol.
pub(super) unsafe fn export<'task>(
    reader: BatchReader<'task>,
    expected_schema: &SchemaRef,
) -> Result<FFI_ArrowArrayStream> {
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
    if canonical_fields(state.schema.fields())? != canonical_fields(expected_schema.fields())? {
        return Err("executor reader schema does not match the negotiated schema".into());
    }
    Ok(FFI_ArrowArrayStream {
        get_schema: Some(get_schema),
        get_next: Some(get_next),
        get_last_error: Some(get_last_error),
        release: Some(release),
        private_data: Box::into_raw(state).cast::<c_void>(),
    })
}

fn callback(
    state: &mut StreamState<'_>,
    action: impl FnOnce(&mut StreamState<'_>) -> Result<()>,
) -> c_int {
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
    let state = unsafe { &mut *(*stream).private_data.cast::<StreamState<'_>>() };
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
    let state = unsafe { &mut *(*stream).private_data.cast::<StreamState<'_>>() };
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
                validate_layout(&data)?;
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
    let state = unsafe { &*(*stream).private_data.cast::<StreamState<'_>>() };
    state
        .last_error
        .as_ref()
        .map_or(std::ptr::null(), |error| error.as_ptr())
}

unsafe extern "C" fn release(stream: *mut FFI_ArrowArrayStream) {
    let state = unsafe { (*stream).private_data.cast::<StreamState<'_>>() };
    // Mark released before destruction. The consumer owns the stream structure.
    unsafe { stream.write(FFI_ArrowArrayStream::empty()) };
    if let Err(payload) = catch_unwind(AssertUnwindSafe(|| unsafe { drop(Box::from_raw(state)) })) {
        let _ = panic_message(payload);
    }
}

// Compare Spark logical types, ignoring Arrow nullability/metadata and the
// interchangeable physical encodings of strings and binary data. Spark also
// distinguishes only the presence of a time zone, not its spelling.
pub(crate) fn validate_schema(schema: &SchemaRef) -> Result<()> {
    canonical_fields(schema.fields())?;
    Ok(())
}

pub(crate) fn canonical_fields(fields: &Fields) -> Result<Fields> {
    fields
        .iter()
        .map(|field| {
            Ok(Arc::new(Field::new(
                field.name(),
                canonical_type(field.data_type())?,
                false,
            )))
        })
        .collect()
}

fn canonical_type(data_type: &DataType) -> Result<DataType> {
    use DataType::*;
    Ok(match data_type {
        Null | Boolean | Int8 | Int16 | Int32 | Int64 | Float32 | Float64 | Date32 => {
            data_type.clone()
        }
        Utf8 | LargeUtf8 | Utf8View => Utf8,
        Binary | LargeBinary | BinaryView => Binary,
        Decimal128(precision, _) if *precision > 0 && *precision <= 38 => data_type.clone(),
        Timestamp(TimeUnit::Microsecond, tz) => {
            Timestamp(TimeUnit::Microsecond, tz.as_ref().map(|_| "UTC".into()))
        }
        Duration(TimeUnit::Microsecond)
        | Interval(IntervalUnit::YearMonth | IntervalUnit::MonthDayNano) => data_type.clone(),
        Struct(fields) => Struct(canonical_fields(fields)?),
        List(field) => List(Arc::new(Field::new(
            "item",
            canonical_type(field.data_type())?,
            false,
        ))),
        Map(entries, _) => {
            let Struct(fields) = entries.data_type() else {
                return Err("Arrow map entries must be a struct".into());
            };
            if fields.len() != 2 {
                return Err("Arrow map entries must have a key and a value".into());
            }
            let entries = Struct(
                vec![
                    Field::new("key", canonical_type(fields[0].data_type())?, false),
                    Field::new("value", canonical_type(fields[1].data_type())?, false),
                ]
                .into(),
            );
            Map(Arc::new(Field::new("entries", entries, false)), false)
        }
        _ => return Err(format!("unsupported Spark Arrow type: {data_type}").into()),
    })
}

fn validate_layout(data: &ArrayData) -> Result<()> {
    if data.offset() != 0 {
        return Err(format!(
            "Spark requires zero Arrow array offsets: {} has offset {}; normalize sliced arrays",
            data.data_type(),
            data.offset(),
        )
        .into());
    }
    for child in data.child_data() {
        validate_layout(child)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use arrow_array::ffi_stream::ArrowArrayStreamReader;
    use arrow_array::{BooleanArray, Int32Array, Int64Array, RecordBatch, RecordBatchReader};
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

    fn export(reader: BatchReader<'static>) -> Result<FFI_ArrowArrayStream> {
        let expected = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        // Test readers own all their state and cannot borrow a host context.
        unsafe { super::export(reader, &expected) }
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

    #[test]
    fn negotiated_schema_mismatch_drops_the_reader() {
        let reader = reader();
        let drops = reader.drops.clone();
        let expected = Arc::new(Schema::new(vec![Field::new("id", DataType::Utf8, false)]));
        // The reader owns its state; no task context needs to outlive this call.
        let result = unsafe { super::export(Box::new(reader), &expected) };
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("does not match the negotiated schema"));
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn logical_schema_comparison_accepts_equivalent_arrow_representations() {
        let actual = Schema::new(vec![
            Field::new("text", DataType::LargeUtf8, true),
            Field::new("bytes", DataType::BinaryView, true),
            Field::new(
                "time",
                DataType::Timestamp(TimeUnit::Microsecond, Some("Europe/Paris".into())),
                true,
            ),
            Field::new(
                "nested",
                DataType::Struct(vec![Field::new("text", DataType::Utf8View, true)].into()),
                true,
            ),
        ]);
        let expected = Schema::new(vec![
            Field::new("text", DataType::Utf8, false),
            Field::new("bytes", DataType::Binary, false),
            Field::new(
                "time",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                false,
            ),
            Field::new(
                "nested",
                DataType::Struct(vec![Field::new("text", DataType::Utf8, false)].into()),
                false,
            ),
        ]);
        assert_eq!(
            canonical_fields(actual.fields()).unwrap(),
            canonical_fields(expected.fields()).unwrap()
        );
        // A timestamp without a time zone remains a distinct Spark logical type.
        assert_ne!(
            canonical_type(&DataType::Timestamp(TimeUnit::Microsecond, None)).unwrap(),
            canonical_type(&DataType::Timestamp(
                TimeUnit::Microsecond,
                Some("UTC".into())
            ))
            .unwrap()
        );
    }

    #[test]
    fn unsupported_types_are_rejected_inside_nested_fields() {
        let nested_types = [
            DataType::List(Arc::new(Field::new("item", DataType::UInt64, false))),
            DataType::Struct(
                vec![Field::new(
                    "dictionary",
                    DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                    false,
                )]
                .into(),
            ),
            DataType::Map(
                Arc::new(Field::new(
                    "entries",
                    DataType::Struct(
                        vec![
                            Field::new("key", DataType::Utf8, false),
                            Field::new("value", DataType::Decimal256(40, 2), true),
                        ]
                        .into(),
                    ),
                    false,
                )),
                false,
            ),
        ];
        for data_type in nested_types {
            let schema = Arc::new(Schema::new(vec![Field::new("nested", data_type, true)]));
            assert!(validate_schema(&schema)
                .unwrap_err()
                .to_string()
                .contains("unsupported Spark Arrow type"));
        }
    }

    #[test]
    fn a_valid_sliced_boolean_inside_a_struct_is_rejected_before_export() {
        let values = BooleanArray::from(vec![true, false, true]).slice(1, 2);
        assert_eq!(values.to_data().offset(), 1);
        let fields: Fields = vec![Field::new("flag", DataType::Boolean, false)].into();
        let nested = StructArray::new(fields.clone(), vec![Arc::new(values)], None);
        let schema = Arc::new(Schema::new(vec![Field::new(
            "nested",
            DataType::Struct(fields),
            false,
        )]));
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(nested)]).unwrap();
        let mut reader = reader();
        reader.schema = schema.clone();
        reader.batches = vec![Ok(batch)].into_iter();
        let drops = reader.drops.clone();
        // All arrays are valid, owned Arrow values, without a borrowed task context.
        let exported = unsafe { super::export(Box::new(reader), &schema) }.unwrap();
        let mut stream = ArrowArrayStreamReader::try_new(exported).unwrap();
        let error = stream.next().unwrap().unwrap_err().to_string();
        assert!(error.contains("zero Arrow array offsets"), "{error}");
        assert!(error.contains("offset 1"), "{error}");
        assert!(stream.next().is_none());
        drop(stream);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn a_batch_cannot_change_the_readers_advertised_schema() {
        let mut reader = reader();
        let first = reader.batches.next().unwrap();
        let changed_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let changed =
            RecordBatch::try_new(changed_schema, vec![Arc::new(Int32Array::from(vec![7, 9]))])
                .unwrap();
        reader.batches = vec![first, Ok(changed)].into_iter();
        let drops = reader.drops.clone();
        let mut stream =
            ArrowArrayStreamReader::try_new(export(Box::new(reader)).unwrap()).unwrap();
        assert_eq!(stream.next().unwrap().unwrap().num_rows(), 2);
        assert!(stream
            .next()
            .unwrap()
            .unwrap_err()
            .to_string()
            .contains("different schema"));
        assert!(stream.next().is_none());
        drop(stream);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }
}
