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

/// The driver keeps this coordinator until a successful commit or an abort, after which Spark
/// only closes the writer, and until close otherwise, including after a failed commit, which
/// Spark aborts next. Dropping it before close releases its resources even when Spark closes
/// the writer late, such as a stream writer after the next micro-batch created its own.
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

    fn coordinator_mut(&mut self) -> Result<&mut C> {
        Ok(self
            .coordinator
            .as_mut()
            .ok_or("the write was already committed or aborted")?)
    }

    /// Drops the coordinator, logging a panic of its Drop instead of failing the call.
    fn release(&mut self) {
        if let Some(coordinator) = self.coordinator.take() {
            drop_provider(coordinator);
        }
    }
}

impl<C: StreamingWrite> DriverWrite for DriverWriter<C> {
    fn commit(&mut self, epoch_id: i64, messages: &[Option<&[u8]>]) -> Result<()> {
        let messages = messages
            .iter()
            .map(|message| decode(message.ok_or("missing committed task message")?))
            .collect::<Result<Vec<_>>>()?;
        self.coordinator_mut()?
            .commit(StreamingCommitInfo::new(epoch_id, messages))?;
        self.release();
        Ok(())
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
        let result = self
            .coordinator_mut()?
            .abort(StreamingAbortInfo::new(epoch_id, messages));
        self.release();
        result
    }
}

impl<C: StreamingWrite> Drop for DriverWriter<C> {
    fn drop(&mut self) {
        self.release();
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
    let task = unsafe { TaskContext::from_raw(task)? };
    let data_type = DataType::Struct(schema.fields().clone());
    let mut context = WriterContext::new(schema, task);
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
    let array = unsafe { ArrowArray::from_raw(array) };
    let data = unsafe { ffi::from_ffi_and_data_type(array, state.data_type.clone())? };
    let array = StructArray::from(data);
    if array.null_count() != 0 {
        return Err("write input cannot contain null rows".into());
    }
    state.writer().write(RecordBatch::from(array))
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
