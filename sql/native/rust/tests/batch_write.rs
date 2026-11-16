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

use std::ffi::{c_int, c_void, CStr, CString};
use std::mem::size_of;
use std::ptr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

use arrow_array::ffi::FFI_ArrowArray;
use arrow_array::{Array, Int64Array, RecordBatch, StructArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use serde::{Deserialize, Serialize, Serializer};
use spark_datasource::__private::abi::{
    ArrowSchema, SparkDsCreateInfo, SparkDsDataWriterInfo, SparkDsFinishDataWriterInfo,
    SparkDsFinishWriteInfo, SparkDsHost, SparkDsLibrary, SparkDsTaskContext, SparkDsWriteInfo,
    ABI_VERSION,
};
use spark_datasource::{
    AbortInfo, BatchWrite, Capabilities, CommitInfo, CreateInfo, DataSource, DataWriter,
    DataWriterFactory, PartitionWriter, Result, TaskContext, Unsupported, WriteInfo, WriteMode,
    WriterContext,
};

// Each test exercises the actual exported table; this lock isolates fixture observations.
static TEST_LOCK: Mutex<()> = Mutex::new(());
static EVENTS: Mutex<Vec<Event>> = Mutex::new(Vec::new());

#[derive(Clone, Debug, PartialEq)]
enum Event {
    WriteCreated(bool),
    FactoryCreated,
    Batch(usize),
    TaskCommit,
    TaskAbort(bool),
    TaskDropped(bool),
    DriverCommit(Vec<Message>),
    DriverAbort(Vec<Option<Message>>),
    DriverDropped,
}

fn record(event: Event) {
    EVENTS.lock().unwrap().push(event);
}

fn events() -> Vec<Event> {
    EVENTS.lock().unwrap().clone()
}

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        false,
    )]))
}

struct Source {
    failure: String,
    capabilities: String,
}

impl DataSource for Source {
    type ScanBuilder = Unsupported;
    type MicroBatchStream = Unsupported;
    type BatchWrite = Driver;
    type StreamingWrite = Unsupported;

    // A sink of any input: Spark requires a user-specified schema to read it.
    const INFERS_SCHEMA: bool = false;

    fn new(info: CreateInfo) -> Result<Self> {
        Ok(Self {
            failure: info.options.get("failure").cloned().unwrap_or_default(),
            capabilities: info
                .options
                .get("capabilities")
                .cloned()
                .unwrap_or_default(),
        })
    }

    fn capabilities(&self) -> Capabilities {
        match self.capabilities.as_str() {
            "none" => Capabilities::empty(),
            "append" => Capabilities::all().without(Capabilities::BATCH_TRUNCATE),
            _ => Capabilities::all(),
        }
    }

    fn new_write(&self, info: WriteInfo) -> Result<Driver> {
        if info.schema != schema() {
            return Err("unsupported input schema".into());
        }
        record(Event::WriteCreated(info.mode == WriteMode::Truncate));
        Ok(Driver {
            failure: self.failure.clone(),
        })
    }
}

struct Driver {
    failure: String,
}

impl BatchWrite for Driver {
    type Factory = Factory;
    type Message = Message;

    const SUPPORTS_TRUNCATE: bool = true;

    fn new_writer_factory(&self) -> Result<Factory> {
        record(Event::FactoryCreated);
        if self.failure == "writer_factory_error" {
            return Err("creating the writer factory failed".into());
        }
        Ok(Factory {
            failure: self.failure.clone(),
        })
    }

    fn commit(&mut self, info: CommitInfo<Message>) -> Result<()> {
        record(Event::DriverCommit(info.messages));
        match self.failure.as_str() {
            "driver_commit_error" => Err("driver commit failed".into()),
            "driver_commit_panic" => panic!("driver commit panicked"),
            _ => Ok(()),
        }
    }

    fn abort(&mut self, info: AbortInfo<Message>) -> Result<()> {
        record(Event::DriverAbort(info.messages));
        Ok(())
    }
}

impl Drop for Driver {
    fn drop(&mut self) {
        record(Event::DriverDropped);
    }
}

#[derive(Serialize, Deserialize)]
struct Factory {
    #[serde(serialize_with = "serialize_factory_mode")]
    failure: String,
}

fn serialize_factory_mode<S: Serializer>(
    mode: &str,
    serializer: S,
) -> std::result::Result<S::Ok, S::Error> {
    if mode == "factory_serialization" {
        return Err(serde::ser::Error::custom("factory serialization failed"));
    }
    serializer.serialize_str(mode)
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct Message {
    partition: i32,
    attempt: i64,
    values: Vec<i64>,
    batches: usize,
    #[serde(serialize_with = "serialize_message_mode")]
    failure: String,
}

fn serialize_message_mode<S: Serializer>(
    mode: &str,
    serializer: S,
) -> std::result::Result<S::Ok, S::Error> {
    if mode == "message_serialization" {
        return Err(serde::ser::Error::custom("message serialization failed"));
    }
    serializer.serialize_str(mode)
}

impl DataWriterFactory for Factory {
    type Message = Message;

    fn open<'task>(
        &self,
        context: WriterContext<'task>,
    ) -> Result<PartitionWriter<'task, Message>> {
        if context.schema != schema() {
            return Err("the writer context lost the input schema".into());
        }
        Ok(Box::new(Writer {
            task: context.task,
            failure: self.failure.clone(),
            batches: vec![],
        }))
    }
}

struct Writer<'task> {
    task: TaskContext<'task>,
    failure: String,
    batches: Vec<RecordBatch>,
}

impl DataWriter for Writer<'_> {
    type Message = Message;

    fn write(&mut self, batch: RecordBatch) -> Result<()> {
        record(Event::Batch(batch.num_rows()));
        // Retain the imported buffers after the ABI call returns and its inputs are dropped.
        self.batches.push(batch);
        match self.failure.as_str() {
            "write_error" => Err("writing batch failed".into()),
            "write_panic" => panic!("writing batch panicked"),
            _ => Ok(()),
        }
    }

    fn commit(&mut self) -> Result<Message> {
        record(Event::TaskCommit);
        if self.task.is_interrupted() {
            return Err("task cancelled during commit".into());
        }
        match self.failure.as_str() {
            "task_commit_error" => return Err("task commit failed".into()),
            "task_commit_panic" => panic!("task commit panicked"),
            _ => (),
        }
        let values = self
            .batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .values()
                    .iter()
                    .copied()
            })
            .collect();
        Ok(Message {
            partition: self.task.partition_id(),
            attempt: self.task.task_attempt_id(),
            values,
            batches: self.batches.len(),
            failure: self.failure.clone(),
        })
    }

    fn abort(&mut self) -> Result<()> {
        record(Event::TaskAbort(self.task.is_interrupted()));
        self.batches.clear();
        Ok(())
    }
}

impl Drop for Writer<'_> {
    fn drop(&mut self) {
        record(Event::TaskDropped(self.task.is_interrupted()));
    }
}

spark_datasource::export_data_source!(Source);

extern "C" {
    fn spark_ds_init(host: *const SparkDsHost, library: *mut *const SparkDsLibrary) -> c_int;
}

fn library() -> &'static SparkDsLibrary {
    // Spark's host structure stays valid until the JVM exits.
    let host = Box::leak(Box::new(SparkDsHost {
        size: size_of::<SparkDsHost>() as i64,
        abi_version: ABI_VERSION,
        spark_version: c"test".as_ptr(),
        log: None,
        is_log_enabled: None,
    }));
    let mut table = ptr::null();
    assert_eq!(unsafe { spark_ds_init(host, &mut table) }, 0);
    assert!(!table.is_null());
    unsafe { &*table }
}

fn error(library: &SparkDsLibrary) -> String {
    let message = unsafe { library.last_error.unwrap()() };
    assert!(!message.is_null());
    unsafe { CStr::from_ptr(message) }.to_str().unwrap().into()
}

fn success(library: &SparkDsLibrary, status: c_int) {
    assert_eq!(status, 0, "{}", error(library));
}

struct Handle {
    library: &'static SparkDsLibrary,
    pointer: *mut c_void,
    close: unsafe extern "C" fn(*mut c_void),
}

impl Drop for Handle {
    fn drop(&mut self) {
        unsafe { (self.close)(self.pointer) };
    }
}

fn source(library: &'static SparkDsLibrary, failure: &str, capabilities: &str) -> (Handle, u64) {
    let failure = CString::new(failure).unwrap();
    let capabilities = CString::new(capabilities).unwrap();
    let keys = [c"failure".as_ptr(), c"capabilities".as_ptr()];
    let values = [failure.as_ptr(), capabilities.as_ptr()];
    let info = SparkDsCreateInfo {
        size: size_of::<SparkDsCreateInfo>() as i64,
        name: c"write_test".as_ptr(),
        option_keys: keys.as_ptr(),
        option_values: values.as_ptr(),
        num_options: keys.len() as i64,
    };
    let mut mask = 0;
    let mut pointer = ptr::null_mut();
    let status = unsafe { library.create.unwrap()(&info, &mut mask, &mut pointer) };
    success(library, status);
    assert!(!pointer.is_null());
    (
        Handle {
            library,
            pointer,
            close: library.close.unwrap(),
        },
        mask,
    )
}

/// Creates a writer, with the state that create_writer returns for its data writers.
fn create_driver(
    source: &Handle,
    mode: i32,
    query: *const std::ffi::c_char,
) -> (c_int, *mut c_void, Vec<Vec<u8>>) {
    let mut input = ArrowSchema::try_from(schema().as_ref()).unwrap();
    let info = SparkDsWriteInfo {
        size: size_of::<SparkDsWriteInfo>() as i64,
        mode,
        query_id: query,
    };
    let mut pointer = ptr::null_mut();
    let mut states = Vec::<Vec<u8>>::new();
    let status = unsafe {
        source.library.create_writer.unwrap()(
            source.pointer,
            &mut input,
            &info,
            Some(collect),
            (&mut states as *mut Vec<Vec<u8>>).cast(),
            &mut pointer,
        )
    };
    (status, pointer, states)
}

fn driver(source: &Handle, mode: i32) -> (Handle, Vec<u8>) {
    let (status, pointer, mut states) = create_driver(source, mode, ptr::null());
    success(source.library, status);
    assert!(!pointer.is_null());
    assert_eq!(states.len(), 1, "a writer returns a single state");
    let driver = Handle {
        library: source.library,
        pointer,
        close: source.library.close_writer.unwrap(),
    };
    (driver, states.pop().unwrap())
}

unsafe extern "C" fn collect(sink: *mut c_void, bytes: *const u8, length: i64) {
    let output = unsafe { &mut *sink.cast::<Vec<Vec<u8>>>() };
    output.push(unsafe { std::slice::from_raw_parts(bytes, length as usize) }.to_vec());
}

#[repr(C)]
struct HostTask {
    context: SparkDsTaskContext,
    cancelled: AtomicBool,
}

unsafe extern "C" fn interrupted(context: *const SparkDsTaskContext) -> bool {
    unsafe { &*context.cast::<HostTask>() }
        .cancelled
        .load(Ordering::SeqCst)
}

struct Task {
    library: &'static SparkDsLibrary,
    pointer: *mut c_void,
    host: Box<HostTask>,
}

impl Drop for Task {
    fn drop(&mut self) {
        // The host context must outlive the provider's destructor as well as its callbacks.
        unsafe { self.library.close_data_writer.unwrap()(self.pointer) };
    }
}

fn host_task(partition: i32) -> Box<HostTask> {
    Box::new(HostTask {
        context: SparkDsTaskContext {
            size: size_of::<SparkDsTaskContext>() as i64,
            stage_id: 3,
            stage_attempt_number: 1,
            partition_id: partition,
            attempt_number: 2,
            task_attempt_id: 100 + i64::from(partition),
            is_interrupted: Some(interrupted),
        },
        cancelled: AtomicBool::new(false),
    })
}

fn create_data_writer(
    library: &SparkDsLibrary,
    host: &HostTask,
    state: &[u8],
    epoch_id: i64,
) -> (c_int, *mut c_void) {
    let mut input = ArrowSchema::try_from(schema().as_ref()).unwrap();
    let info = SparkDsDataWriterInfo {
        size: size_of::<SparkDsDataWriterInfo>() as i64,
        schema: &mut input,
        writer_state: state.as_ptr(),
        writer_state_length: state.len() as i64,
        epoch_id,
    };
    let mut pointer = ptr::null_mut();
    let status = unsafe { library.create_data_writer.unwrap()(&host.context, &info, &mut pointer) };
    (status, pointer)
}

fn task(library: &'static SparkDsLibrary, state: &[u8], partition: i32) -> Task {
    let host = host_task(partition);
    let (status, pointer) = create_data_writer(library, &host, state, -1);
    success(library, status);
    assert!(!pointer.is_null());
    Task {
        library,
        pointer,
        host,
    }
}

fn batch(values: Vec<i64>) -> RecordBatch {
    RecordBatch::try_new(schema(), vec![Arc::new(Int64Array::from(values))]).unwrap()
}

fn write(task: &Task, batch: RecordBatch) -> c_int {
    let mut array = FFI_ArrowArray::new(&StructArray::from(batch).to_data());
    let status = unsafe { task.library.write.unwrap()(task.pointer, &mut array) };
    if status == 0 {
        assert!(
            array.is_released(),
            "retained input must be moved from the caller"
        );
    }
    status
}

const FINISH_DATA_WRITER: SparkDsFinishDataWriterInfo = SparkDsFinishDataWriterInfo {
    size: size_of::<SparkDsFinishDataWriterInfo>() as i64,
};

fn task_commit(task: &Task) -> (c_int, Vec<Vec<u8>>) {
    let mut output = Vec::<Vec<u8>>::new();
    let status = unsafe {
        task.library.commit_data_writer.unwrap()(
            task.pointer,
            &FINISH_DATA_WRITER,
            Some(collect),
            (&mut output as *mut Vec<Vec<u8>>).cast(),
        )
    };
    (status, output)
}

fn task_message(task: &Task) -> Vec<u8> {
    let (status, mut messages) = task_commit(task);
    success(task.library, status);
    assert_eq!(messages.len(), 1);
    messages.pop().unwrap()
}

fn task_abort(task: &Task) -> c_int {
    unsafe { task.library.abort_data_writer.unwrap()(task.pointer, &FINISH_DATA_WRITER) }
}

fn driver_finish(driver: &Handle, abort: bool, messages: &[Option<&[u8]>]) -> c_int {
    let pointers: Vec<_> = messages
        .iter()
        .map(|message| message.map_or(ptr::null(), |bytes| bytes.as_ptr()))
        .collect();
    let lengths: Vec<_> = messages
        .iter()
        .map(|message| message.map_or(0, |bytes| bytes.len() as i64))
        .collect();
    let info = SparkDsFinishWriteInfo {
        size: size_of::<SparkDsFinishWriteInfo>() as i64,
        epoch_id: -1,
        messages: pointers.as_ptr(),
        message_lengths: lengths.as_ptr(),
        num_messages: messages.len() as i64,
        rejected: 0,
    };
    let callback = if abort {
        driver.library.abort.unwrap()
    } else {
        driver.library.commit.unwrap()
    };
    unsafe { callback(driver.pointer, &info) }
}

#[test]
fn write_types_and_options_control_capabilities() {
    let _guard = TEST_LOCK.lock().unwrap_or_else(PoisonError::into_inner);
    EVENTS.lock().unwrap().clear();
    let library = library();
    assert!(
        library.schema.is_none(),
        "INFERS_SCHEMA = false leaves schema inference to Spark's error"
    );
    assert!(library.create_reader.is_none());
    assert!(library.push_domains.is_none());
    assert!(library.read.is_none());
    assert!(library.create_stream_reader.is_none());
    assert!(library.default_read_limit.is_none());
    assert!(library.create_writer.is_some());
    assert!(library.write.is_some());

    for (capabilities, expected) in [("", 4 | 16), ("append", 4), ("none", 0)] {
        let (source, mask) = source(library, "", capabilities);
        assert_eq!(mask, expected);
        for mode in [0, 1] {
            let (status, pointer, states) = create_driver(&source, mode, ptr::null());
            let supported = expected & 4 != 0 && (mode == 0 || expected & 16 != 0);
            if supported {
                success(library, status);
                unsafe { library.close_writer.unwrap()(pointer) };
            } else {
                assert_ne!(status, 0);
                assert!(pointer.is_null() && states.is_empty());
                assert!(error(library).contains("support"));
            }
        }
    }
    assert_eq!(
        events(),
        vec![
            Event::WriteCreated(false),
            Event::FactoryCreated,
            Event::DriverDropped,
            Event::WriteCreated(true),
            Event::FactoryCreated,
            Event::DriverDropped,
            Event::WriteCreated(false),
            Event::FactoryCreated,
            Event::DriverDropped,
        ]
    );

    let (source, _) = source(library, "", "");
    for (mode, query) in [(99, ptr::null()), (0, c"streaming-query".as_ptr())] {
        let (status, pointer, _) = create_driver(&source, mode, query);
        assert_ne!(status, 0);
        assert!(pointer.is_null());
    }
}

#[test]
fn serialized_factory_and_messages_roundtrip_without_driver_source_or_input_buffers() {
    let _guard = TEST_LOCK.lock().unwrap_or_else(PoisonError::into_inner);
    EVENTS.lock().unwrap().clear();
    let library = library();
    let (source, _) = source(library, "", "");
    let (driver, state) = driver(&source, 1);
    drop(source);

    let first = task(library, &state, 7);
    let input = batch(vec![2, 5, 9]);
    let buffer = input.column(0).to_data().buffers()[0].clone();
    success(library, write(&first, input));
    assert!(
        buffer.strong_count() > 1,
        "writer must own retained Arrow buffers"
    );
    success(library, write(&first, batch(vec![])));
    let first_message = task_message(&first);
    drop(first);
    assert_eq!(
        buffer.strong_count(),
        1,
        "close must release retained Arrow buffers"
    );

    let second = task(library, &state, 8);
    let second_message = task_message(&second);
    drop(second);
    let messages = [
        Some(first_message.as_slice()),
        Some(second_message.as_slice()),
    ];
    success(library, driver_finish(&driver, false, &messages));
    // The coordinator is dropped as soon as its commit succeeds, before Spark closes the writer.
    assert_eq!(
        events(),
        vec![
            Event::WriteCreated(true),
            Event::FactoryCreated,
            Event::Batch(3),
            Event::Batch(0),
            Event::TaskCommit,
            Event::TaskDropped(false),
            Event::TaskCommit,
            Event::TaskDropped(false),
            Event::DriverCommit(vec![
                Message {
                    partition: 7,
                    attempt: 107,
                    values: vec![2, 5, 9],
                    batches: 2,
                    failure: String::new(),
                },
                Message {
                    partition: 8,
                    attempt: 108,
                    values: vec![],
                    batches: 0,
                    failure: String::new(),
                },
            ]),
            Event::DriverDropped,
        ]
    );
    drop(driver);
}

#[test]
fn task_writers_see_live_task_cancellation() {
    let _guard = TEST_LOCK.lock().unwrap_or_else(PoisonError::into_inner);
    let library = library();
    for cancelled in [false, true] {
        EVENTS.lock().unwrap().clear();
        let (source, _) = source(library, "", "");
        let (driver, state) = driver(&source, 0);
        let task = task(library, &state, 0);
        success(library, write(&task, batch(vec![1])));
        if cancelled {
            task.host.cancelled.store(true, Ordering::SeqCst);
            let (status, messages) = task_commit(&task);
            assert_ne!(status, 0);
            assert!(messages.is_empty());
            assert!(error(library).contains("cancelled"));
        }
        success(library, task_abort(&task));
        drop(task);
        assert!(events().contains(&Event::TaskAbort(cancelled)));
        assert!(events().contains(&Event::TaskDropped(cancelled)));
        success(library, driver_finish(&driver, true, &[None]));
    }
}

#[test]
fn task_failures_and_message_serialization_failures_remain_abortable() {
    let _guard = TEST_LOCK.lock().unwrap_or_else(PoisonError::into_inner);
    let library = library();
    for failure in [
        "write_error",
        "write_panic",
        "task_commit_error",
        "task_commit_panic",
        "message_serialization",
    ] {
        EVENTS.lock().unwrap().clear();
        let (source, _) = source(library, failure, "");
        let (driver, state) = driver(&source, 0);
        let task = task(library, &state, 0);
        let status = write(&task, batch(vec![4]));
        let write_failed = failure.starts_with("write_");
        assert_eq!(status != 0, write_failed, "{failure}");
        // Spark commits only after successful writes, and aborts after any failure.
        if !write_failed {
            let (status, messages) = task_commit(&task);
            assert_ne!(status, 0, "{failure}");
            assert!(messages.is_empty());
        }
        let message = error(library);
        assert!(
            message.contains("failed") || message.contains("panicked"),
            "{message}"
        );
        // A commit whose message cannot be serialized counts as failed, so abort follows
        // a provider commit that returned a message.
        success(library, task_abort(&task));
        drop(task);
        let count = |expected: Event| events().iter().filter(|e| **e == expected).count();
        assert_eq!(count(Event::TaskAbort(false)), 1);
        assert_eq!(count(Event::TaskCommit), usize::from(!write_failed));
        success(library, driver_finish(&driver, true, &[None]));
    }
}

#[test]
fn driver_failures_preserve_messages_and_missing_task_positions_for_abort() {
    let _guard = TEST_LOCK.lock().unwrap_or_else(PoisonError::into_inner);
    let library = library();
    for failure in ["driver_commit_error", "driver_commit_panic"] {
        EVENTS.lock().unwrap().clear();
        let (source, _) = source(library, failure, "");
        let (driver, state) = driver(&source, 0);
        let task = task(library, &state, 1);
        success(library, write(&task, batch(vec![42])));
        let message = task_message(&task);
        drop(task);
        let committed = [Some(message.as_slice())];
        assert_ne!(driver_finish(&driver, false, &committed), 0);
        assert!(error(library).contains("driver commit"));
        // Spark aborts a write whose commit failed, so the coordinator is kept for the abort,
        // after which it is dropped, before Spark closes the writer.
        assert!(!events().contains(&Event::DriverDropped));
        let aborted = [None, Some(message.as_slice()), None];
        success(library, driver_finish(&driver, true, &aborted));
        assert_eq!(events().last(), Some(&Event::DriverDropped));
        let expected = Message {
            partition: 1,
            attempt: 101,
            values: vec![42],
            batches: 1,
            failure: failure.to_owned(),
        };
        assert!(events().contains(&Event::DriverCommit(vec![expected.clone()])));
        assert!(events().contains(&Event::DriverAbort(vec![None, Some(expected), None])));
    }
}

#[test]
fn writer_factory_failures_fail_the_creation_of_the_writer() {
    let _guard = TEST_LOCK.lock().unwrap_or_else(PoisonError::into_inner);
    let library = library();
    for (failure, expected) in [
        ("writer_factory_error", "creating the writer factory failed"),
        ("factory_serialization", "factory serialization failed"),
    ] {
        EVENTS.lock().unwrap().clear();
        let (source, _) = source(library, failure, "");
        let (status, pointer, states) = create_driver(&source, 0, ptr::null());
        assert_ne!(status, 0);
        assert!(pointer.is_null() && states.is_empty());
        assert!(error(library).contains(expected));
        // The coordinator is dropped without abort, as the write never started.
        assert_eq!(
            events(),
            vec![
                Event::WriteCreated(false),
                Event::FactoryCreated,
                Event::DriverDropped
            ],
        );
    }
}

#[test]
fn the_epoch_of_a_data_writer_selects_its_write() {
    let _guard = TEST_LOCK.lock().unwrap_or_else(PoisonError::into_inner);
    EVENTS.lock().unwrap().clear();
    let library = library();
    let (source, _) = source(library, "", "");
    let (driver, state) = driver(&source, 0);
    let host = host_task(0);
    // A micro-batch, which this source does not write, and an invalid epoch.
    for epoch_id in [0, -2] {
        let (status, pointer) = create_data_writer(library, &host, &state, epoch_id);
        assert_ne!(status, 0);
        assert!(pointer.is_null());
    }
    assert!(error(library).contains("invalid epoch ID -2"));
    success(library, driver_finish(&driver, true, &[]));
    assert_eq!(
        events(),
        vec![
            Event::WriteCreated(false),
            Event::FactoryCreated,
            Event::DriverAbort(vec![]),
            // Dropped right after the abort, before Spark closes the writer.
            Event::DriverDropped,
        ],
    );
}

#[test]
fn undecodable_messages_fail_commit_but_abort_still_cleans_up() {
    let _guard = TEST_LOCK.lock().unwrap_or_else(PoisonError::into_inner);
    EVENTS.lock().unwrap().clear();
    let library = library();
    let (source, _) = source(library, "", "");
    let (driver, state) = driver(&source, 0);
    let task = task(library, &state, 2);
    success(library, write(&task, batch(vec![6])));
    let message = task_message(&task);
    drop(task);

    let garbage = b"not a commit message";
    assert_ne!(driver_finish(&driver, false, &[Some(garbage)]), 0);
    assert!(!events()
        .iter()
        .any(|event| matches!(event, Event::DriverCommit(_))));
    let aborted = [Some(&garbage[..]), Some(message.as_slice()), None];
    success(library, driver_finish(&driver, true, &aborted));
    let expected = Message {
        partition: 2,
        attempt: 102,
        values: vec![6],
        batches: 1,
        failure: String::new(),
    };
    assert!(events().contains(&Event::DriverAbort(vec![None, Some(expected), None])));
}
