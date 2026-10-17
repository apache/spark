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

//! Driver write coordination and task-local writers restored from serialized state.

use std::ffi::c_void;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::Arc;

use arrow_array::{ffi, Array, RecordBatch, StructArray};
use arrow_schema::{DataType, Schema, SchemaRef};
use serde::{de::DeserializeOwned, Deserialize, Serialize};

use crate::abi::{self, ArrowArray, ArrowSchema, SparkDsTaskContext};
use crate::bridge::stream::{canonical_fields, validate_schema};
use crate::bridge::{decode, encode, panic_message, provider_call};
use crate::{BatchWrite, BatchWriterFactory, Result, TaskContext, TaskWriter};

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

/// The driver keeps this coordinator until close, including after a failed commit.
pub struct DriverWriter<W: BatchWrite> {
    writer: Option<W>,
    schema: SchemaRef,
    serialized: Option<Cached<Arc<Vec<u8>>>>,
    phase: Phase<()>,
}

impl<W: BatchWrite> DriverWriter<W> {
    pub fn new(writer: W, schema: SchemaRef) -> Self {
        Self {
            writer: Some(writer),
            schema,
            serialized: None,
            phase: Phase::Active,
        }
    }

    pub fn serialized(&mut self) -> Result<Arc<Vec<u8>>> {
        if let Some(result) = &self.serialized {
            return cached(result);
        }
        self.phase.active()?;
        let result = provider_call(|| {
            validate_schema(&self.schema)?;
            let factory = self.writer.as_mut().expect("writer is present").factory()?;
            Ok(Arc::new(encode(&WriterState {
                factory,
                schema: self.schema.as_ref().clone(),
            })?))
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

    pub fn commit(&mut self, messages: &[Option<&[u8]>]) -> Result<()> {
        if matches!(self.phase, Phase::Committed(())) {
            return Ok(());
        }
        self.phase.active()?;
        let result = provider_call(|| {
            let messages = messages
                .iter()
                .map(|message| decode(message.ok_or("missing committed task message")?))
                .collect::<Result<Vec<_>>>()?;
            self.writer
                .as_mut()
                .expect("writer is present")
                .commit(messages)
        });
        self.phase = match &result {
            Ok(()) => Phase::Committed(()),
            Err(message) => Phase::Failed(message.clone()),
        };
        cached(&result)
    }

    pub fn abort(&mut self, messages: &[Option<&[u8]>]) -> Result<()> {
        match &self.phase {
            Phase::Committed(()) => return Ok(()),
            Phase::Aborted(result) => return cached(result),
            Phase::Active | Phase::Failed(_) => {}
        }
        let result = provider_call(|| {
            let messages = messages
                .iter()
                .map(|message| message.map(decode).transpose())
                .collect::<Result<Vec<_>>>()?;
            self.writer
                .as_mut()
                .expect("writer is present")
                .abort(messages)
        });
        self.phase = Phase::Aborted(result.clone());
        cached(&result)
    }
}

impl<W: BatchWrite> Drop for DriverWriter<W> {
    fn drop(&mut self) {
        if let Some(writer) = self.writer.take() {
            drop_provider(writer);
        }
    }
}

struct TaskState<'task, M> {
    writer: Option<TaskWriter<'task, M>>,
    schema: SchemaRef,
    phase: Phase<Arc<Vec<u8>>>,
}

impl<M> Drop for TaskState<'_, M> {
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

/// Restores a task writer without creating or consulting any driver registry.
///
/// # Safety
/// `task` must satisfy TaskContext::from_raw and remain valid until close_task.
/// The returned handle must only be used with callbacks for F::Message, never
/// concurrently, and must be closed exactly once while the plugin stays loaded.
pub unsafe fn open<F: BatchWriterFactory>(
    task: *const SparkDsTaskContext,
    state: &[u8],
) -> Result<*mut c_void> {
    let WriterState { factory, schema }: WriterState<F> = decode(state)?;
    let schema = Arc::new(schema);
    validate_schema(&schema)?;
    let task = unsafe { TaskContext::from_raw(task)? };
    let (writer, open_error) = match provider_call(|| factory.open(task)) {
        Ok(writer) => (Some(writer), None),
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
/// `handle` must be a live handle from open for M, accessed without concurrency.
unsafe fn task_state<'a, M>(handle: *mut c_void) -> Result<&'a mut TaskState<'a, M>> {
    abi::require_output(handle)?;
    Ok(unsafe { &mut *handle.cast::<TaskState<'a, M>>() })
}

/// Imports owned Arrow data and passes it to the task writer.
///
/// # Safety
/// `handle` must follow open's contract. Both input structs must be valid Arrow
/// C data that this call may move; their buffers must satisfy the Arrow schema.
pub unsafe fn write_batch<M>(
    handle: *mut c_void,
    array: *mut ArrowArray,
    schema: *mut ArrowSchema,
) -> Result<()>
where
    M: Serialize + DeserializeOwned + Send + 'static,
{
    let state = unsafe { task_state::<M>(handle)? };
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
/// `handle` must follow open's contract for M.
pub unsafe fn commit_task<M>(handle: *mut c_void) -> Result<Arc<Vec<u8>>>
where
    M: Serialize + DeserializeOwned + Send + 'static,
{
    let state = unsafe { task_state::<M>(handle)? };
    if let Phase::Committed(message) = &state.phase {
        return Ok(message.clone());
    }
    state.phase.active()?;
    let result = provider_call(|| {
        let message = state.writer.as_mut().expect("writer is present").commit()?;
        Ok(Arc::new(encode(&message)?))
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
/// `handle` must follow open's contract for M.
pub unsafe fn abort_task<M>(handle: *mut c_void) -> Result<()>
where
    M: Serialize + DeserializeOwned + Send + 'static,
{
    let state = unsafe { task_state::<M>(handle)? };
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
/// `handle` must follow open's contract for M and must not be used again.
pub unsafe fn close_task<M>(handle: *mut c_void) -> Result<()>
where
    M: Serialize + DeserializeOwned + Send + 'static,
{
    abi::require_output(handle)?;
    drop(unsafe { Box::from_raw(handle.cast::<TaskState<'_, M>>()) });
    Ok(())
}
