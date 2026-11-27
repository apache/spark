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

//! Driver write coordination and task-local writers restored from serialized state, for batch
//! writes and for the writes of micro-batches.

use std::ffi::c_void;

use arrow_array::{ffi, Array, RecordBatch, StructArray};
use arrow_schema::{DataType, SchemaRef};
use serde::Serialize;

use crate::abi::{self, ArrowArray, SparkDsTaskContext};
use crate::bridge::stream::validate_schema;
use crate::bridge::{decode, describe, drop_provider, encode, provider_call};
use crate::{
    AbortInfo, BatchWrite, CommitInfo, DataWriter, DataWriterFactory, PartitionWriter, Result,
    StreamingAbortInfo, StreamingCommitInfo, StreamingWrite, TaskContext, WriterContext,
};

/// A batch write, which a driver writer drives as a streaming write whose epoch ID is always
/// -1 and is ignored.
pub struct Batch<W>(pub W);

impl<W: BatchWrite> StreamingWrite for Batch<W> {
    type Factory = W::Factory;
    type Message = W::Message;

    fn new_writer_factory(&self) -> Result<W::Factory> {
        self.0.new_writer_factory()
    }

    fn commit(&mut self, info: StreamingCommitInfo<W::Message>) -> Result<()> {
        self.0.commit(CommitInfo::new(info.messages))
    }

    fn abort(&mut self, info: StreamingAbortInfo<W::Message>) -> Result<()> {
        self.0.abort(AbortInfo::new(info.messages))
    }
}

/// A driver writer that decodes its own commit messages, so that one handle type holds the
/// driver writers of batch writes and of the writes of micro-batches, which share the writer
/// handles of the ABI.
pub trait DriverWrite: Send {
    fn commit(&mut self, epoch_id: i64, messages: &[Option<&[u8]>]) -> Result<()>;

    fn abort(&mut self, epoch_id: i64, messages: &[Option<&[u8]>]) -> Result<()>;
}

/// The driver keeps this coordinator until close, including after a failed commit, which
/// Spark aborts next. Spark closes the writer right after it commits or aborts the write.
pub struct DriverWriter<C: StreamingWrite> {
    coordinator: Option<C>,
}

impl<C: StreamingWrite> DriverWriter<C> {
    /// Wraps a coordinator, with the state that its task writers need: its serialized
    /// writer factory. On failure, the coordinator is dropped.
    pub fn create(coordinator: C) -> Result<(Box<dyn DriverWrite>, Vec<u8>)> {
        let writer = Self {
            coordinator: Some(coordinator),
        };
        // The factory's destructor is connector code too, so it runs in provider_call.
        let state = provider_call(|| encode(&writer.coordinator().new_writer_factory()?))?;
        Ok((Box::new(writer), state))
    }

    fn coordinator(&self) -> &C {
        self.coordinator.as_ref().expect("coordinator is present")
    }

    fn coordinator_mut(&mut self) -> &mut C {
        self.coordinator.as_mut().expect("coordinator is present")
    }
}

impl<C: StreamingWrite> DriverWrite for DriverWriter<C> {
    fn commit(&mut self, epoch_id: i64, messages: &[Option<&[u8]>]) -> Result<()> {
        let messages = messages
            .iter()
            .map(|message| decode(message.ok_or("missing committed task message")?))
            .collect::<Result<Vec<_>>>()?;
        self.coordinator_mut()
            .commit(StreamingCommitInfo::new(epoch_id, messages))
    }

    fn abort(&mut self, epoch_id: i64, messages: &[Option<&[u8]>]) -> Result<()> {
        // Abort is the cleanup path: a message that cannot be decoded is passed as None
        // instead of preventing the coordinator from cleaning up.
        let messages = messages
            .iter()
            .enumerate()
            .map(|(partition, message)| match decode((*message)?) {
                Ok(message) => Some(message),
                Err(error) => {
                    log::warn!(
                        "Aborting a write without the commit message of partition \
                         {partition}, which cannot be decoded: {}",
                        describe(&*error)
                    );
                    None
                }
            })
            .collect();
        self.coordinator_mut()
            .abort(StreamingAbortInfo::new(epoch_id, messages))
    }
}

impl<C: StreamingWrite> Drop for DriverWriter<C> {
    fn drop(&mut self) {
        // A panic of the coordinator's Drop is logged instead of unwinding into close.
        if let Some(coordinator) = self.coordinator.take() {
            drop_provider(coordinator);
        }
    }
}

/// A task writer that encodes its own commit message, so that one handle type holds the
/// writers of batch and streaming writes, whatever their messages.
trait TaskWriter: Send {
    fn write(&mut self, batch: RecordBatch) -> Result<()>;

    fn commit(&mut self) -> Result<Vec<u8>>;

    fn abort(&mut self) -> Result<()>;
}

impl<M: Serialize> TaskWriter for PartitionWriter<'_, M> {
    fn write(&mut self, batch: RecordBatch) -> Result<()> {
        DataWriter::write(&mut **self, batch)
    }

    fn commit(&mut self) -> Result<Vec<u8>> {
        encode(&DataWriter::commit(&mut **self)?)
    }

    fn abort(&mut self) -> Result<()> {
        DataWriter::abort(&mut **self)
    }
}

struct TaskState<'task> {
    writer: Option<Box<dyn TaskWriter + 'task>>,
    // The struct type of the arrays that write gets: the schema that create_data_writer got.
    data_type: DataType,
}

impl TaskState<'_> {
    fn writer(&mut self) -> &mut (dyn TaskWriter + '_) {
        self.writer.as_deref_mut().expect("writer is present")
    }
}

impl Drop for TaskState<'_> {
    fn drop(&mut self) {
        if let Some(writer) = self.writer.take() {
            drop_provider(writer);
        }
    }
}

/// Restores the task writer of a batch write, with factory `B`, or of a micro-batch, with
/// factory `S`, without creating or consulting any driver registry. The epoch ID tells them
/// apart: it is -1 for a batch write.
///
/// # Safety
/// `task` must satisfy TaskContext::from_raw and remain valid until close_task.
/// The returned handle must never be used concurrently, and must be closed exactly
/// once while the plugin stays loaded.
pub unsafe fn open<B: DataWriterFactory, S: DataWriterFactory>(
    task: *const SparkDsTaskContext,
    schema: SchemaRef,
    state: &[u8],
    epoch_id: i64,
) -> Result<*mut c_void> {
    match epoch_id {
        -1 => unsafe { open_writer::<B>(task, schema, state, None) },
        epoch_id if epoch_id >= 0 => unsafe {
            open_writer::<S>(task, schema, state, Some(epoch_id))
        },
        _ => Err(format!("invalid epoch ID {epoch_id}").into()),
    }
}

/// # Safety
/// As for open.
unsafe fn open_writer<F: DataWriterFactory>(
    task: *const SparkDsTaskContext,
    schema: SchemaRef,
    state: &[u8],
    epoch_id: Option<i64>,
) -> Result<*mut c_void> {
    validate_schema(&schema)?;
    let factory: F = decode(state)?;
    let (task, partition_id) = unsafe { TaskContext::from_raw(task)? };
    let data_type = DataType::Struct(schema.fields().clone());
    let mut context = WriterContext::new(schema, partition_id, task);
    context.epoch_id = epoch_id;
    let opened = provider_call(|| factory.open(context));
    // Keep guarded writer destruction in place before dropping the temporary
    // factory, whose destructor is connector code too.
    let state = opened.map(|writer| {
        Box::new(TaskState {
            writer: Some(Box::new(writer)),
            data_type,
        })
    });
    let factory_drop = provider_call(|| {
        drop(factory);
        Ok(())
    });
    let state = state?;
    factory_drop?;
    Ok(Box::into_raw(state).cast())
}

/// # Safety
/// `handle` must be a live handle from open, accessed without concurrency.
unsafe fn task_state<'a>(handle: *mut c_void) -> Result<&'a mut TaskState<'a>> {
    abi::require_output(handle)?;
    Ok(unsafe { &mut *handle.cast::<TaskState<'a>>() })
}

/// Imports owned Arrow data, with the schema of the writer, and passes it to the task writer.
///
/// # Safety
/// `handle` must follow open's contract. The array must be valid Arrow C data of the schema
/// of the writer, which this call may move.
pub unsafe fn write_batch(handle: *mut c_void, array: *mut ArrowArray) -> Result<()> {
    let state = unsafe { task_state(handle)? };
    abi::require_output(array)?;
    let array = unsafe { import(ArrowArray::from_raw(array), &state.data_type)? };
    if array.null_count() != 0 {
        return Err("write input cannot contain null rows".into());
    }
    state.writer().write(RecordBatch::from(array))
}

/// Imports a batch to write, aligned and validated with [`abi::align_and_validate`].
///
/// # Safety
/// As for write_batch: the array must be Arrow C data of `data_type`.
unsafe fn import(array: ArrowArray, data_type: &DataType) -> Result<StructArray> {
    let data = unsafe { ffi::from_ffi_and_data_type(array, data_type.clone())? };
    let data = abi::align_and_validate(data)
        .map_err(|e| format!("write input is not valid Arrow data: {e}"))?;
    Ok(StructArray::from(data))
}

/// Commits a task, and returns its serialized message.
///
/// # Safety
/// `handle` must follow open's contract.
pub unsafe fn commit_task(handle: *mut c_void) -> Result<Vec<u8>> {
    unsafe { task_state(handle)? }.writer().commit()
}

/// Aborts a task, including after write or commit failures.
///
/// # Safety
/// `handle` must follow open's contract.
pub unsafe fn abort_task(handle: *mut c_void) -> Result<()> {
    unsafe { task_state(handle)? }.writer().abort()
}

/// Releases a task writer without committing or aborting it implicitly.
///
/// # Safety
/// `handle` must follow open's contract and must not be used again.
pub unsafe fn close_task(handle: *mut c_void) -> Result<()> {
    abi::require_output(handle)?;
    drop(unsafe { Box::from_raw(handle.cast::<TaskState<'_>>()) });
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::builder::{
        Int64BufferBuilder, Int64Builder, ListBuilder, MapBuilder, StringBuilder,
    };
    use arrow_array::cast::AsArray;
    use arrow_array::ffi::FFI_ArrowArray;
    use arrow_array::types::{Decimal128Type, Int64Type};
    use arrow_array::{
        make_array, Array, ArrayRef, BinaryArray, Int64Array, ListArray, StringArray, StructArray,
    };
    use arrow_data::ArrayData;
    use arrow_schema::{DataType, Field, Fields};

    use super::import;

    /// String data whose second value is not UTF-8, which only unchecked construction builds.
    fn invalid_utf8() -> ArrayData {
        let bytes = BinaryArray::from_vec(vec![b"ok".as_slice(), [0xFFu8].as_slice()]);
        unsafe {
            bytes
                .to_data()
                .into_builder()
                .data_type(DataType::Utf8)
                .build_unchecked()
        }
    }

    /// Exports a batch of an ID column and the given label column, and imports it to write.
    fn import_batch(labels: ArrayData) -> crate::Result<StructArray> {
        let ids = Int64Array::from_iter_values(0..labels.len() as i64);
        let fields = Fields::from(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("label", labels.data_type().clone(), true),
        ]);
        let columns: Vec<ArrayRef> = vec![Arc::new(ids), make_array(labels)];
        let batch = StructArray::try_new(fields, columns, None)?;
        import_data(&batch.to_data())
    }

    /// Exports a batch and imports it to write, without checking it as an array would.
    fn import_data(batch: &ArrayData) -> crate::Result<StructArray> {
        unsafe { import(FFI_ArrowArray::new(batch), batch.data_type()) }
    }

    #[test]
    fn import_accepts_valid_strings() {
        let batch = import_batch(StringArray::from(vec!["ok", "fine"]).to_data()).unwrap();
        assert_eq!(batch.column(0).as_primitive::<Int64Type>().value(1), 1);
        assert_eq!(batch.column(1).as_string::<i32>().value(1), "fine");
    }

    #[test]
    fn import_rejects_strings_that_are_not_utf8() {
        let error = import_batch(invalid_utf8()).unwrap_err().to_string();
        assert!(error.contains("not valid Arrow data"), "{error}");
        assert!(
            error.contains("Invalid UTF8 sequence at string index 1"),
            "{error}"
        );
    }

    #[test]
    fn import_rejects_nested_strings_that_are_not_utf8() {
        let mut labels = ListBuilder::new(StringBuilder::new());
        labels.values().append_value("ok");
        labels.values().append_value("x");
        labels.append(true);
        let labels: ListArray = labels.finish();
        let labels = labels
            .to_data()
            .into_builder()
            .child_data(vec![invalid_utf8()]);
        let error = import_batch(unsafe { labels.build_unchecked() })
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("Invalid UTF8 sequence at string index 1"),
            "{error}"
        );
    }

    #[test]
    fn import_accepts_nulls_under_null_slots_of_non_nullable_elements() {
        // [[1], null] of array<bigint not null>, where the null list still spans a null element,
        // as a native scan may return and Spark accepts.
        let mut labels = ListBuilder::new(Int64Builder::new());
        labels.values().append_value(1);
        labels.append(true);
        labels.values().append_null();
        labels.append(false);
        let labels: ListArray = labels.finish();
        let element = Arc::new(Field::new("element", DataType::Int64, false));
        let labels = labels
            .to_data()
            .into_builder()
            .data_type(DataType::List(element));
        let batch = import_batch(unsafe { labels.build_unchecked() }).unwrap();
        let labels = batch.column(1).as_list::<i32>();
        assert!(labels.is_null(1));
        assert_eq!(labels.value(0).as_primitive::<Int64Type>().value(0), 1);
    }

    #[test]
    fn import_accepts_nulls_under_null_slots_of_non_nullable_map_values() {
        // [{1: 10}, null] of map<bigint, bigint not null>, where the null map still spans an
        // entry with a null value.
        let mut labels = MapBuilder::new(None, Int64Builder::new(), Int64Builder::new());
        labels.keys().append_value(1);
        labels.values().append_value(10);
        labels.append(true).unwrap();
        labels.keys().append_value(2);
        labels.values().append_null();
        labels.append(false).unwrap();
        let labels = labels.finish().to_data();
        let DataType::Map(entries, sorted) = labels.data_type().clone() else {
            unreachable!()
        };
        let DataType::Struct(fields) = entries.data_type() else {
            unreachable!()
        };
        let fields = Fields::from(vec![
            fields[0].as_ref().clone(),
            fields[1].as_ref().clone().with_nullable(false),
        ]);
        let entries_data = labels.child_data()[0]
            .clone()
            .into_builder()
            .data_type(DataType::Struct(fields.clone()));
        let entries = entries
            .as_ref()
            .clone()
            .with_data_type(DataType::Struct(fields));
        let labels = labels
            .into_builder()
            .data_type(DataType::Map(Arc::new(entries), sorted))
            .child_data(vec![unsafe { entries_data.build_unchecked() }]);
        let batch = import_batch(unsafe { labels.build_unchecked() }).unwrap();
        let labels = batch.column(1).as_map();
        assert!(labels.is_null(1));
        assert_eq!(
            labels
                .value(0)
                .column(1)
                .as_primitive::<Int64Type>()
                .value(0),
            10
        );
    }

    #[test]
    fn import_rejects_a_struct_whose_child_is_shorter_than_it() {
        let ids = Int64Array::from_iter_values(0..1).to_data();
        let fields = Fields::from(vec![Field::new("id", DataType::Int64, false)]);
        let batch = unsafe {
            ArrayData::builder(DataType::Struct(fields))
                .len(2)
                .child_data(vec![ids])
                .build_unchecked()
        };
        let error = import_data(&batch).unwrap_err().to_string();
        assert!(error.contains("not valid Arrow data"), "{error}");
        assert!(
            error.contains("has length smaller than expected"),
            "{error}"
        );
    }

    #[test]
    fn import_copies_decimal_values_that_are_not_aligned_to_i128() {
        // The decimals [123.45, -0.01], as the low and high halves of each value, eight bytes
        // into an allocation of a buffer builder, which is aligned to at least 32 bytes.
        let mut words = Int64BufferBuilder::new(5);
        words.append_slice(&[0, 12345, 0, -1, -1]);
        let values = words.finish().slice(8);
        let decimal = DataType::Decimal128(10, 2);
        let prices = unsafe {
            ArrayData::builder(decimal.clone())
                .len(2)
                .add_buffer(values)
                .build_unchecked()
        };
        let fields = Fields::from(vec![Field::new("price", decimal, false)]);
        let batch = unsafe {
            ArrayData::builder(DataType::Struct(fields))
                .len(2)
                .child_data(vec![prices])
                .build_unchecked()
        };
        let batch = import_data(&batch).unwrap();
        let prices = batch.column(0).as_primitive::<Decimal128Type>();
        let alignment = align_of::<i128>();
        assert_eq!(prices.values().inner().as_ptr().align_offset(alignment), 0);
        assert_eq!(prices.value(0), 12345);
        assert_eq!(prices.value(1), -1);
    }

    #[test]
    fn import_rejects_a_null_count_that_disagrees_with_the_validity_buffer() {
        let labels = Int64Array::from(vec![Some(1), None, Some(3)]);
        let validity = labels.nulls().unwrap().buffer().clone();
        let labels = labels
            .to_data()
            .into_builder()
            .nulls(None)
            .null_bit_buffer(Some(validity))
            .null_count(2);
        let error = import_batch(unsafe { labels.build_unchecked() })
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("null count 2 does not match the 1 nulls of the validity buffer"),
            "{error}"
        );
    }
}
