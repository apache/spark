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
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::Arc;

use arrow_array::{ffi, Array, RecordBatch, StructArray};
use arrow_schema::{DataType, Schema, SchemaRef};
use serde::{de::DeserializeOwned, Deserialize, Serialize};

use crate::abi::{self, ArrowArray, ArrowSchema, SparkDsTaskContext};
use crate::bridge::stream::{canonical_fields, validate_schema};
use crate::bridge::{decode, describe, encode, panic_message, provider_call, Kind};
use crate::{
    BatchWrite, DataWriter, DataWriterFactory, PartitionWriter, Result, StreamingWrite,
    TaskContext, Unsupported, WriterContext,
};

type Cached<T> = std::result::Result<T, String>;

enum Phase<T> {
    Active,
    Committed(T),
    Failed(String),
    Aborted(Cached<()>),
}

impl<T> Phase<T> {
    fn active(&self) -> Result<()> {
        match self {
            Self::Active => Ok(()),
            Self::Failed(message) => Err(message.clone().into()),
            Self::Committed(_) => Err("writer was already committed".into()),
            Self::Aborted(_) => Err("writer was already aborted".into()),
        }
    }
}

fn cached<T: Clone>(result: &Cached<T>) -> Result<T> {
    result.clone().map_err(Into::into)
}

#[derive(Serialize, Deserialize)]
struct WriterState<F> {
    factory: F,
    schema: Schema,
}

/// What a driver writer calls: a batch write, or the write of a micro-batch.
pub trait Coordinator: Send + 'static {
    type Factory: DataWriterFactory<Message = Self::Message>;
    type Message: Serialize + DeserializeOwned + Send + 'static;

    /// Whether this is the write of a micro-batch, whose epoch ID is not -1.
    const STREAMING: bool;

    fn writer_factory(&self) -> Result<Self::Factory>;

    fn commit(&mut self, epoch_id: i64, messages: Vec<Self::Message>) -> Result<()>;

    fn abort(&mut self, epoch_id: i64, messages: Vec<Option<Self::Message>>) -> Result<()>;
}

/// A batch write, whose epoch ID is always -1.
pub struct Batch<W>(pub W);

impl<W: BatchWrite> Coordinator for Batch<W> {
    type Factory = W::Factory;
    type Message = W::Message;

    const STREAMING: bool = false;

    fn writer_factory(&self) -> Result<W::Factory> {
        self.0.writer_factory()
    }

    fn commit(&mut self, _: i64, messages: Vec<W::Message>) -> Result<()> {
        self.0.commit(messages)
    }

    fn abort(&mut self, _: i64, messages: Vec<Option<W::Message>>) -> Result<()> {
        self.0.abort(messages)
    }
}

/// The write of a micro-batch.
pub struct Streaming<W>(pub W);

impl<W: StreamingWrite> Coordinator for Streaming<W> {
    type Factory = W::Factory;
    type Message = W::Message;

    const STREAMING: bool = true;

    fn writer_factory(&self) -> Result<W::Factory> {
        self.0.writer_factory()
    }

    fn commit(&mut self, epoch_id: i64, messages: Vec<W::Message>) -> Result<()> {
        self.0.commit(epoch_id, messages)
    }

    fn abort(&mut self, epoch_id: i64, messages: Vec<Option<W::Message>>) -> Result<()> {
        self.0.abort(epoch_id, messages)
    }
}

/// Checks the epoch ID that Spark passes with a write: -1 for a batch write, and the ID of
/// the micro-batch, which is not negative, for a streaming write.
fn check_epoch(streaming: bool, epoch_id: i64) -> Result<()> {
    if streaming && epoch_id < 0 {
        return Err(
            format!("the write of a micro-batch requires its epoch ID, not {epoch_id}").into(),
        );
    }
    if !streaming && epoch_id != -1 {
        return Err(format!("a batch write requires epoch ID -1, not {epoch_id}").into());
    }
    Ok(())
}

/// The driver keeps this coordinator until close, including after a failed commit.
pub struct DriverWriter<C: Coordinator> {
    coordinator: Option<C>,
    schema: SchemaRef,
    serialized: Option<Cached<Arc<Vec<u8>>>>,
    phase: Phase<()>,
}

impl<C: Coordinator> DriverWriter<C> {
    pub fn new(coordinator: C, schema: SchemaRef) -> Self {
        Self {
            coordinator: Some(coordinator),
            schema,
            serialized: None,
            phase: Phase::Active,
        }
    }

    /// Creates and serializes the writer factory once, caching the result.
    pub fn serialized(&mut self) -> Result<Arc<Vec<u8>>> {
        if let Some(result) = &self.serialized {
            return cached(result);
        }
        self.phase.active()?;
        let result = provider_call(|| {
            validate_schema(&self.schema)?;
            let coordinator = self.coordinator.as_ref().expect("coordinator is present");
            // The factory's destructor is connector code too, so it runs in provider_call.
            let factory = coordinator.writer_factory()?;
            let state = WriterState {
                factory: &factory,
                schema: self.schema.as_ref().clone(),
            };
            let state = if C::STREAMING {
                encode(&Kind::<Unsupported, _>::Streaming(state))?
            } else {
                encode(&Kind::<_, Unsupported>::Batch(state))?
            };
            Ok(Arc::new(state))
        });
        if let Err(message) = &result {
            self.phase = Phase::Failed(message.clone());
        }
        self.serialized = Some(result);
        cached(
            self.serialized
                .as_ref()
                .expect("serialization result is present"),
        )
    }

    pub fn commit(&mut self, epoch_id: i64, messages: &[Option<&[u8]>]) -> Result<()> {
        check_epoch(C::STREAMING, epoch_id)?;
        if matches!(self.phase, Phase::Committed(())) {
            return Ok(());
        }
        self.phase.active()?;
        let result = provider_call(|| {
            let messages = messages
                .iter()
                .map(|message| decode(message.ok_or("missing committed task message")?))
                .collect::<Result<Vec<_>>>()?;
            self.coordinator
                .as_mut()
                .expect("coordinator is present")
                .commit(epoch_id, messages)
        });
        self.phase = match &result {
            Ok(()) => Phase::Committed(()),
            Err(message) => Phase::Failed(message.clone()),
        };
        cached(&result)
    }

    pub fn abort(&mut self, epoch_id: i64, messages: &[Option<&[u8]>]) -> Result<()> {
        check_epoch(C::STREAMING, epoch_id)?;
        match &self.phase {
            Phase::Committed(()) => return Ok(()),
            Phase::Aborted(result) => return cached(result),
            Phase::Active | Phase::Failed(_) => {}
        }
        let result = provider_call(|| {
            // Abort is the cleanup path: a message that cannot be decoded is passed as
            // None instead of preventing the coordinator from cleaning up.
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
            self.coordinator
                .as_mut()
                .expect("coordinator is present")
                .abort(epoch_id, messages)
        });
        self.phase = Phase::Aborted(result.clone());
        cached(&result)
    }
}

impl<C: Coordinator> Drop for DriverWriter<C> {
    fn drop(&mut self) {
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
    schema: SchemaRef,
    phase: Phase<Arc<Vec<u8>>>,
}

impl Drop for TaskState<'_> {
    fn drop(&mut self) {
        if let Some(writer) = self.writer.take() {
            drop_provider(writer);
        }
    }
}

fn drop_provider<T>(value: T) {
    // Close has no error channel and must never unwind across the native ABI.
    if let Err(payload) = catch_unwind(AssertUnwindSafe(|| drop(value))) {
        let _ = panic_message(payload);
    }
}

/// Restores the task writer of a batch write, with factory `B`, or of a micro-batch, with
/// factory `S`, without creating or consulting any driver registry.
///
/// # Safety
/// `task` must satisfy TaskContext::from_raw and remain valid until close_task.
/// The returned handle must never be used concurrently, and must be closed exactly
/// once while the plugin stays loaded.
pub unsafe fn open<B: DataWriterFactory, S: DataWriterFactory>(
    task: *const SparkDsTaskContext,
    state: &[u8],
    epoch_id: i64,
) -> Result<*mut c_void> {
    match decode::<Kind<WriterState<B>, WriterState<S>>>(state)? {
        Kind::Batch(state) => {
            check_epoch(false, epoch_id)?;
            unsafe { open_writer(task, state, None) }
        }
        Kind::Streaming(state) => {
            check_epoch(true, epoch_id)?;
            unsafe { open_writer(task, state, Some(epoch_id)) }
        }
    }
}

/// # Safety
/// As for open.
unsafe fn open_writer<F: DataWriterFactory>(
    task: *const SparkDsTaskContext,
    state: WriterState<F>,
    epoch_id: Option<i64>,
) -> Result<*mut c_void> {
    let WriterState { factory, schema } = state;
    let schema = Arc::new(schema);
    validate_schema(&schema)?;
    let task = unsafe { TaskContext::from_raw(task)? };
    let mut context = WriterContext::new(schema.clone(), task);
    context.epoch_id = epoch_id;
    let (writer, open_error) = match provider_call(|| factory.open(context)) {
        Ok(writer) => (Some(Box::new(writer) as Box<dyn TaskWriter + '_>), None),
        Err(message) => (None, Some(message)),
    };
    let state = Box::new(TaskState {
        writer,
        schema,
        phase: Phase::Active,
    });
    // Keep guarded writer destruction in place before dropping the temporary
    // factory, whose destructor is connector code too.
    let factory_drop = provider_call(|| {
        drop(factory);
        Ok(())
    });
    if let Some(message) = open_error {
        return Err(message.into());
    }
    factory_drop.map_err(crate::Error::from)?;
    Ok(Box::into_raw(state).cast())
}

/// # Safety
/// `handle` must be a live handle from open, accessed without concurrency.
unsafe fn task_state<'a>(handle: *mut c_void) -> Result<&'a mut TaskState<'a>> {
    abi::require_output(handle)?;
    Ok(unsafe { &mut *handle.cast::<TaskState<'a>>() })
}

/// Imports owned Arrow data and passes it to the task writer.
///
/// # Safety
/// `handle` must follow open's contract. Both input structs must be valid Arrow
/// C data that this call may move; their buffers must satisfy the Arrow schema.
pub unsafe fn write_batch(
    handle: *mut c_void,
    array: *mut ArrowArray,
    schema: *mut ArrowSchema,
) -> Result<()> {
    let state = unsafe { task_state(handle)? };
    state.phase.active()?;
    let result = provider_call(|| {
        let batch = unsafe { import_batch(array, schema, &state.schema)? };
        state
            .writer
            .as_mut()
            .expect("writer is present")
            .write(batch)
    });
    if let Err(message) = &result {
        state.phase = Phase::Failed(message.clone());
    }
    cached(&result)
}

unsafe fn import_batch(
    array: *mut ArrowArray,
    schema: *mut ArrowSchema,
    expected: &SchemaRef,
) -> Result<RecordBatch> {
    abi::require_output(array)?;
    abi::require_output(schema)?;
    let array = unsafe { ArrowArray::from_raw(array) };
    let schema = unsafe { ArrowSchema::from_raw(schema) };
    let data = unsafe { ffi::from_ffi(array, &schema)? };
    if !matches!(data.data_type(), DataType::Struct(_)) {
        return Err("write input must be an Arrow struct array".into());
    }
    let array = StructArray::from(data);
    if array.null_count() != 0 {
        return Err("write input cannot contain null rows".into());
    }
    let batch = RecordBatch::from(array);
    if canonical_fields(batch.schema().fields())? != canonical_fields(expected.fields())? {
        return Err("write batch schema does not match the planned schema".into());
    }
    Ok(batch)
}

/// Commits a task once and caches its serialized message, including failures.
///
/// # Safety
/// `handle` must follow open's contract.
pub unsafe fn commit_task(handle: *mut c_void) -> Result<Arc<Vec<u8>>> {
    let state = unsafe { task_state(handle)? };
    if let Phase::Committed(message) = &state.phase {
        return Ok(message.clone());
    }
    state.phase.active()?;
    let result = provider_call(|| {
        let message = state.writer.as_mut().expect("writer is present").commit()?;
        Ok(Arc::new(message))
    });
    match result {
        Ok(message) => {
            state.phase = Phase::Committed(message.clone());
            Ok(message)
        }
        Err(message) => {
            state.phase = Phase::Failed(message.clone());
            Err(message.into())
        }
    }
}

/// Aborts an unfinished task once, including after write or commit failures.
///
/// # Safety
/// `handle` must follow open's contract.
pub unsafe fn abort_task(handle: *mut c_void) -> Result<()> {
    let state = unsafe { task_state(handle)? };
    match &state.phase {
        Phase::Committed(_) => return Ok(()),
        Phase::Aborted(result) => return cached(result),
        Phase::Active | Phase::Failed(_) => {}
    }
    let result = provider_call(|| state.writer.as_mut().expect("writer is present").abort());
    state.phase = Phase::Aborted(result.clone());
    cached(&result)
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
