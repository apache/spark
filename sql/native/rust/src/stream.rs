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
use arrow_schema::{ffi::FFI_ArrowSchema, DataType, Field, Fields, Schema, SchemaRef, TimeUnit};

use super::{describe, panic_message};
use crate::{PartitionReader, Result};

struct StreamState<'task> {
    reader: Option<PartitionReader<'task>>,
    // The reader's own schema, which each batch must have.
    schema: SchemaRef,
    // The negotiated schema, whose nullability each batch must respect.
    expected: SchemaRef,
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
    reader: PartitionReader<'task>,
    expected_schema: &SchemaRef,
) -> Result<FFI_ArrowArrayStream> {
    // Establish guarded destruction before calling the provider's schema().
    let mut state = Box::new(StreamState {
        reader: Some(reader),
        schema: Arc::new(Schema::empty()),
        expected: expected_schema.clone(),
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
        action(state).map_err(|e| describe(&*e))
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
                for (column, field) in data.child_data().iter().zip(state.expected.fields()) {
                    check_nulls(column, field, field.name(), None)?;
                }
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

// Compare the Spark logical types of version 1 of the ABI, ignoring Arrow
// nullability/metadata and the interchangeable physical encodings of strings and
// binary data. Spark also distinguishes only the presence of a time zone, not its
// spelling. Nullability is checked against the data instead, in check_nulls.
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
        Boolean | Int8 | Int16 | Int32 | Int64 | Float32 | Float64 | Date32 => data_type.clone(),
        Utf8 | LargeUtf8 | Utf8View => Utf8,
        Binary | LargeBinary | BinaryView => Binary,
        Decimal128(precision, _) if *precision > 0 && *precision <= 38 => data_type.clone(),
        Timestamp(TimeUnit::Microsecond, tz) => {
            Timestamp(TimeUnit::Microsecond, tz.as_ref().map(|_| "UTC".into()))
        }
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

/// Rejects a null where the negotiated schema declares a column, a field, an element,
/// or a map value not nullable, unless its parent is null, as Spark does after reading.
/// `referenced` tells which values belong to a parent value that is not null.
fn check_nulls(
    data: &ArrayData,
    field: &Field,
    column: &str,
    referenced: Option<&[bool]>,
) -> Result<()> {
    let is_referenced = |index: usize| referenced.is_none_or(|mask| mask[index]);
    if !field.is_nullable()
        && data.null_count() > 0
        && (0..data.len()).any(|index| data.is_null(index) && is_referenced(index))
    {
        return Err(format!(
            "The column {column} has a null, but the schema to read declares it not nullable"
        )
        .into());
    }
    let valid = |index: usize| data.is_valid(index) && is_referenced(index);
    match field.data_type() {
        DataType::Struct(fields) => {
            let mask: Vec<bool> = (0..data.len()).map(valid).collect();
            for (child, field) in data.child_data().iter().zip(fields) {
                let name = format!("{column}.{}", field.name());
                check_nulls(child, field, &name, Some(&mask))?;
            }
        }
        DataType::List(element) => {
            let mask = elements(data, valid)?;
            let name = format!("{column}.element");
            check_nulls(&data.child_data()[0], element, &name, Some(&mask))?;
        }
        DataType::Map(entries, _) => {
            let DataType::Struct(fields) = entries.data_type() else {
                return Err("Arrow map entries must be a struct".into());
            };
            let mask = elements(data, valid)?;
            let entries = &data.child_data()[0];
            // Spark does not allow null keys.
            let key = fields[0].as_ref().clone().with_nullable(false);
            let name = format!("{column}.key");
            check_nulls(&entries.child_data()[0], &key, &name, Some(&mask))?;
            let name = format!("{column}.value");
            check_nulls(&entries.child_data()[1], &fields[1], &name, Some(&mask))?;
        }
        _ => {}
    }
    Ok(())
}

/// Whether each element of a list or map belongs to a value that `valid` accepts.
fn elements(data: &ArrayData, valid: impl Fn(usize) -> bool) -> Result<Vec<bool>> {
    let offsets = data.buffer::<i32>(0);
    let mut mask = vec![false; data.child_data()[0].len()];
    for index in 0..data.len() {
        if valid(index) {
            let start = usize::try_from(offsets[index])?;
            let end = usize::try_from(offsets[index + 1])?;
            mask.get_mut(start..end)
                .ok_or("Arrow offsets exceed the length of their values")?
                .fill(true);
        }
    }
    Ok(mask)
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

    use arrow_array::builder::{
        BooleanBuilder, Int32Builder, ListBuilder, MapBuilder, StringBuilder, StructBuilder,
    };
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

    fn export(reader: PartitionReader<'static>) -> Result<FFI_ArrowArrayStream> {
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

    fn reader_of(schema: SchemaRef, batch: RecordBatch) -> Reader {
        let mut reader = reader();
        reader.schema = schema;
        reader.batches = vec![Ok(batch)].into_iter();
        reader
    }

    fn first_batch_error(reader: Reader, expected: &SchemaRef) -> Option<String> {
        let exported = unsafe { super::export(Box::new(reader), expected) }.unwrap();
        let mut stream = ArrowArrayStreamReader::try_new(exported).unwrap();
        stream.next().unwrap().err().map(|error| error.to_string())
    }

    #[test]
    fn intervals_and_the_null_type_are_not_version_1_types() {
        for data_type in [
            DataType::Null,
            DataType::Duration(TimeUnit::Microsecond),
            DataType::Interval(arrow_schema::IntervalUnit::YearMonth),
            DataType::Interval(arrow_schema::IntervalUnit::MonthDayNano),
        ] {
            let schema = Arc::new(Schema::new(vec![Field::new("value", data_type, true)]));
            assert!(validate_schema(&schema).is_err());
        }
    }

    #[test]
    fn nulls_must_respect_the_nullability_of_the_negotiated_schema() {
        let values = Arc::new(Int64Array::from(vec![Some(1), None]));
        let nullable = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
        let required = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(nullable.clone(), vec![values]).unwrap();
        let error = first_batch_error(reader_of(nullable.clone(), batch.clone()), &required);
        assert!(error.unwrap().contains("The column id has a null"));
        assert_eq!(
            first_batch_error(reader_of(nullable.clone(), batch), &nullable),
            None
        );
    }

    #[test]
    fn nested_nulls_are_allowed_only_under_a_null_parent() {
        let required = Fields::from(vec![Field::new("flag", DataType::Boolean, false)]);
        let nullable = Fields::from(vec![Field::new("flag", DataType::Boolean, true)]);
        let schema = |fields: &Fields| {
            Arc::new(Schema::new(vec![Field::new(
                "nested",
                DataType::Struct(fields.clone()),
                true,
            )]))
        };
        fn flags(builder: &mut StructBuilder) -> &mut BooleanBuilder {
            builder.field_builder::<BooleanBuilder>(0).unwrap()
        }
        for (parent_is_valid, rejected) in [(false, false), (true, true)] {
            let mut builder =
                StructBuilder::new(nullable.clone(), vec![Box::new(BooleanBuilder::new())]);
            flags(&mut builder).append_value(true);
            builder.append(true);
            flags(&mut builder).append_null();
            builder.append(parent_is_valid);
            let batch =
                RecordBatch::try_new(schema(&nullable), vec![Arc::new(builder.finish())]).unwrap();
            let error = first_batch_error(reader_of(schema(&nullable), batch), &schema(&required));
            assert_eq!(error.is_some(), rejected, "{error:?}");
            if let Some(error) = error {
                assert!(
                    error.contains("The column nested.flag has a null"),
                    "{error}"
                );
            }
        }
    }

    #[test]
    fn list_elements_and_map_values_respect_their_nullability() {
        let list_schema = |nullable| {
            let element = Arc::new(Field::new("item", DataType::Int32, nullable));
            Arc::new(Schema::new(vec![Field::new(
                "values",
                DataType::List(element),
                true,
            )]))
        };
        for (first, rejected) in [(Some(1), false), (None, true)] {
            let mut builder = ListBuilder::new(Int32Builder::new());
            builder.values().append_option(first);
            builder.append(true);
            // The second list is null, so its null element does not count.
            builder.values().append_null();
            builder.append(false);
            let lists = Arc::new(builder.finish());
            let batch = RecordBatch::try_new(list_schema(true), vec![lists]).unwrap();
            let error = first_batch_error(reader_of(list_schema(true), batch), &list_schema(false));
            assert_eq!(error.is_some(), rejected, "{error:?}");
            if let Some(error) = error {
                assert!(
                    error.contains("The column values.element has a null"),
                    "{error}"
                );
            }
        }

        let mut builder = MapBuilder::new(None, StringBuilder::new(), Int32Builder::new());
        builder.keys().append_value("a");
        builder.values().append_null();
        builder.append(true).unwrap();
        let map = builder.finish();
        let actual = Arc::new(Schema::new(vec![Field::new(
            "map",
            map.data_type().clone(),
            true,
        )]));
        let entries = Fields::from(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("value", DataType::Int32, false),
        ]);
        let entries = Arc::new(Field::new("entries", DataType::Struct(entries), false));
        let required = Arc::new(Schema::new(vec![Field::new(
            "map",
            DataType::Map(entries, false),
            true,
        )]));
        let batch = RecordBatch::try_new(actual.clone(), vec![Arc::new(map)]).unwrap();
        let error = first_batch_error(reader_of(actual.clone(), batch.clone()), &required);
        assert!(error.unwrap().contains("The column map.value has a null"));
        assert_eq!(
            first_batch_error(reader_of(actual.clone(), batch), &actual),
            None
        );
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
