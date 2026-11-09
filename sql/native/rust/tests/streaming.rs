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

use std::ffi::{c_char, c_int, c_void, CStr, CString};
use std::mem::size_of;
use std::ptr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

use arrow_array::ffi::FFI_ArrowArray;
use arrow_array::ffi_stream::{ArrowArrayStreamReader, FFI_ArrowArrayStream};
use arrow_array::{Array, Int64Array, RecordBatch, StructArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use serde::{Deserialize, Serialize};
use spark_native_source::__private::abi::{
    self, ArrowSchema, SparkDsCommitOffsetInfo, SparkDsCreateInfo, SparkDsDataWriterInfo,
    SparkDsFinishDataWriterInfo, SparkDsFinishWriteInfo, SparkDsHost, SparkDsInitialOffsetInfo,
    SparkDsLatestOffsetInfo, SparkDsLibrary, SparkDsPartitionsInfo, SparkDsReadInfo,
    SparkDsReadLimit, SparkDsReaderInfo, SparkDsStreamPartitionsInfo, SparkDsStreamReaderInfo,
    SparkDsTaskContext, SparkDsWriteInfo, ABI_VERSION,
};
use spark_native_source::{
    Capabilities, CreateInfo, DataSource, DataWriter, DataWriterFactory, LatestOffsetInfo,
    MicroBatchStream, MicroBatchStreamInfo, PartitionReader, PartitionReaderFactory,
    PartitionWriter, ReadLimit, ReaderContext, Result, ScanBuilder, ScanInfo, ScanPlan,
    StreamingWrite, StreamingWriteInfo, Unsupported, WriterContext,
};

// Each test exercises the actual exported table; this lock isolates fixture observations.
static TEST_LOCK: Mutex<()> = Mutex::new(());
static EVENTS: Mutex<Vec<Event>> = Mutex::new(Vec::new());
// The position of the latest data of the streams.
static LATEST: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Debug, PartialEq)]
enum Event {
    StreamCreated(String),
    LatestOffset(u64, ReadLimit, Option<u64>),
    OffsetCommitted(u64),
    WriteCreated { query_id: String, truncate: bool },
    EpochCommitted(i64, Vec<Message>),
    EpochAborted(i64, Vec<Option<Message>>),
    WriteDropped,
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

fn batch(values: Vec<i64>) -> RecordBatch {
    RecordBatch::try_new(schema(), vec![Arc::new(Int64Array::from(values))]).unwrap()
}

/// Batch and micro-batch reads, whose reader factories have different types, and streaming
/// writes without batch writes.
struct Source {
    failure: String,
    capabilities: String,
    max_rows: Option<u64>,
}

impl DataSource for Source {
    type ScanBuilder = Scan;
    type MicroBatchStream = Stream;
    type BatchWrite = Unsupported;
    type StreamingWrite = Write;

    fn new(info: CreateInfo) -> Result<Self> {
        let option = |name: &str| info.options.get(name).cloned().unwrap_or_default();
        Ok(Self {
            failure: option("failure"),
            capabilities: option("capabilities"),
            max_rows: info
                .options
                .get("maxrows")
                .map(|rows| rows.parse())
                .transpose()?,
        })
    }

    fn infer_schema(&self) -> Result<SchemaRef> {
        Ok(schema())
    }

    fn capabilities(&self) -> Capabilities {
        match self.capabilities.as_str() {
            "no_streaming_write" => Capabilities::all().without(Capabilities::STREAMING_WRITE),
            "append_only" => Capabilities::all().without(Capabilities::STREAMING_TRUNCATE),
            _ => Capabilities::all(),
        }
    }

    fn new_scan(&self, _: ScanInfo) -> Result<Scan> {
        Ok(Scan)
    }

    fn new_micro_batch_stream(&self, info: MicroBatchStreamInfo) -> Result<Stream> {
        if info.schema != schema() {
            return Err("unsupported stream schema".into());
        }
        record(Event::StreamCreated(info.checkpoint_location));
        Ok(Stream {
            failure: self.failure.clone(),
            max_rows: self.max_rows,
        })
    }

    fn new_streaming_write(&self, info: StreamingWriteInfo) -> Result<Write> {
        record(Event::WriteCreated {
            query_id: info.query_id,
            truncate: info.mode == spark_native_source::WriteMode::Truncate,
        });
        Ok(Write)
    }
}

struct Scan;

impl ScanBuilder for Scan {
    type Factory = BatchFactory;

    fn plan(self) -> Result<ScanPlan<BatchFactory>> {
        Ok(ScanPlan::new(BatchFactory(100), vec![5]))
    }
}

/// Reads `factory + partition` from each partition of a batch scan.
#[derive(Serialize, Deserialize)]
struct BatchFactory(i64);

impl PartitionReaderFactory for BatchFactory {
    type Partition = i64;

    fn open<'task>(
        &self,
        partition: i64,
        _: ReaderContext<'task>,
    ) -> Result<PartitionReader<'task>> {
        Ok(Box::new(std::iter::once(Ok(batch(vec![
            self.0 + partition,
        ])))))
    }
}

struct Stream {
    failure: String,
    max_rows: Option<u64>,
}

/// The note checks that offsets stay on one line, as Spark requires.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Offset {
    position: u64,
    note: String,
}

impl Offset {
    fn at(position: u64) -> Self {
        Self {
            position,
            note: "two\nlines".into(),
        }
    }
}

impl MicroBatchStream for Stream {
    type Offset = Offset;
    type Factory = StreamFactory;

    fn initial_offset(&mut self) -> Result<Offset> {
        Ok(Offset::at(0))
    }

    fn default_read_limit(&self) -> Result<ReadLimit> {
        Ok(self
            .max_rows
            .map_or(ReadLimit::AllAvailable, ReadLimit::MaxRows))
    }

    fn latest_offset(&mut self, info: LatestOffsetInfo<Offset>) -> Result<Offset> {
        if self.failure == "latest_panic" {
            panic!("finding the latest offset panicked");
        }
        let start = info.start.position;
        let run_end = info.run_end.map(|end| end.position);
        record(Event::LatestOffset(start, info.limit, run_end));
        let mut end = LATEST.load(Ordering::SeqCst);
        if let ReadLimit::MaxRows(rows) = info.limit {
            end = end.min(start + rows);
        }
        Ok(Offset::at(run_end.map_or(end, |run_end| end.min(run_end))))
    }

    fn plan(&mut self, start: Offset, end: Offset) -> Result<ScanPlan<StreamFactory>> {
        let positions = (start.position..end.position).collect();
        Ok(ScanPlan::new(StreamFactory { scale: 10 }, positions))
    }

    fn commit(&mut self, end: Offset) -> Result<()> {
        record(Event::OffsetCommitted(end.position));
        Ok(())
    }
}

/// Reads `scale * position` from the partition of each position of a micro-batch.
#[derive(Serialize, Deserialize)]
struct StreamFactory {
    scale: i64,
}

impl PartitionReaderFactory for StreamFactory {
    type Partition = u64;

    fn open<'task>(
        &self,
        position: u64,
        _: ReaderContext<'task>,
    ) -> Result<PartitionReader<'task>> {
        let value = self.scale * position as i64;
        Ok(Box::new(std::iter::once(Ok(batch(vec![value])))))
    }
}

struct Write;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct Message {
    epoch_id: Option<i64>,
    values: Vec<i64>,
}

impl StreamingWrite for Write {
    type Factory = WriteFactory;
    type Message = Message;

    const SUPPORTS_TRUNCATE: bool = true;

    fn writer_factory(&self) -> Result<WriteFactory> {
        Ok(WriteFactory)
    }

    fn commit(&mut self, epoch_id: i64, messages: Vec<Message>) -> Result<()> {
        record(Event::EpochCommitted(epoch_id, messages));
        Ok(())
    }

    fn abort(&mut self, epoch_id: i64, messages: Vec<Option<Message>>) -> Result<()> {
        record(Event::EpochAborted(epoch_id, messages));
        Ok(())
    }
}

impl Drop for Write {
    fn drop(&mut self) {
        record(Event::WriteDropped);
    }
}

#[derive(Serialize, Deserialize)]
struct WriteFactory;

impl DataWriterFactory for WriteFactory {
    type Message = Message;

    fn open<'task>(
        &self,
        context: WriterContext<'task>,
    ) -> Result<PartitionWriter<'task, Message>> {
        Ok(Box::new(Writer {
            message: Message {
                epoch_id: context.epoch_id,
                values: vec![],
            },
        }))
    }
}

struct Writer {
    message: Message,
}

impl DataWriter for Writer {
    type Message = Message;

    fn write(&mut self, batch: RecordBatch) -> Result<()> {
        let values = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        self.message.values.extend(values.values());
        Ok(())
    }

    fn commit(&mut self) -> Result<Message> {
        Ok(self.message.clone())
    }

    fn abort(&mut self) -> Result<()> {
        Ok(())
    }
}

spark_native_source::export_data_source!(Source);

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

/// The values that a call passes to its sinks: each sink gets a list of its own.
#[derive(Default)]
struct Sinks {
    values: Vec<Vec<u8>>,
    states: Vec<Vec<u8>>,
}

unsafe extern "C" fn add_value(sink: *mut c_void, bytes: *const u8, length: i64) {
    let sinks = unsafe { &mut *sink.cast::<Sinks>() };
    sinks
        .values
        .push(unsafe { std::slice::from_raw_parts(bytes, length as usize) }.to_vec());
}

unsafe extern "C" fn set_state(sink: *mut c_void, bytes: *const u8, length: i64) {
    let sinks = unsafe { &mut *sink.cast::<Sinks>() };
    sinks
        .states
        .push(unsafe { std::slice::from_raw_parts(bytes, length as usize) }.to_vec());
}

/// Calls a function of the table that passes its results to sinks.
fn emitted(library: &SparkDsLibrary, call: impl FnOnce(*mut c_void) -> c_int) -> (c_int, Sinks) {
    let mut sinks = Sinks::default();
    let status = call((&mut sinks as *mut Sinks).cast());
    if status != 0 {
        assert!(sinks.values.is_empty() && sinks.states.is_empty());
        let _ = error(library);
    }
    (status, sinks)
}

/// The single value of a call, such as an offset.
fn single(library: &SparkDsLibrary, call: impl FnOnce(*mut c_void) -> c_int) -> Vec<u8> {
    let (status, mut sinks) = emitted(library, call);
    success(library, status);
    assert_eq!(sinks.values.len(), 1);
    assert!(sinks.states.is_empty());
    sinks.values.pop().unwrap()
}

/// The partitions of a plan, and its state, which a planning call returns once.
fn planned(library: &SparkDsLibrary, call: impl FnOnce(*mut c_void) -> c_int) -> Sinks {
    let (status, sinks) = emitted(library, call);
    success(library, status);
    assert_eq!(sinks.states.len(), 1, "a plan has a single state");
    sinks
}

fn source(library: &SparkDsLibrary, options: &[(&str, &str)]) -> (*mut c_void, u64) {
    let options: Vec<(CString, CString)> = options
        .iter()
        .map(|(key, value)| (CString::new(*key).unwrap(), CString::new(*value).unwrap()))
        .collect();
    let keys: Vec<*const c_char> = options.iter().map(|(key, _)| key.as_ptr()).collect();
    let values: Vec<*const c_char> = options.iter().map(|(_, value)| value.as_ptr()).collect();
    let info = SparkDsCreateInfo {
        size: size_of::<SparkDsCreateInfo>() as i64,
        name: c"streaming_test".as_ptr(),
        option_keys: keys.as_ptr(),
        option_values: values.as_ptr(),
        num_options: keys.len() as i64,
    };
    let mut mask = 0;
    let mut pointer = ptr::null_mut();
    success(library, unsafe {
        library.create.unwrap()(&info, &mut mask, &mut pointer)
    });
    (pointer, mask)
}

fn stream_reader(library: &SparkDsLibrary, options: &[(&str, &str)]) -> *mut c_void {
    let (source, _) = source(library, options);
    let mut schema = ArrowSchema::try_from(schema().as_ref()).unwrap();
    let info = SparkDsStreamReaderInfo {
        size: size_of::<SparkDsStreamReaderInfo>() as i64,
        checkpoint_location: c"file:/tmp/checkpoint/sources/0".as_ptr(),
    };
    let mut reader = ptr::null_mut();
    let status =
        unsafe { library.create_stream_reader.unwrap()(source, &mut schema, &info, &mut reader) };
    // Before close, which clears the error of the thread.
    success(library, status);
    // A stream reader outlives its source, which Spark closes right after creating it.
    unsafe { library.close.unwrap()(source) };
    reader
}

fn read_limit(kind: i32, max_rows: i64) -> SparkDsReadLimit {
    SparkDsReadLimit {
        size: size_of::<SparkDsReadLimit>() as i64,
        kind,
        max_rows,
    }
}

fn latest(
    library: &SparkDsLibrary,
    reader: *mut c_void,
    start: &CStr,
    limit: &SparkDsReadLimit,
    run_end: Option<&CStr>,
) -> (c_int, Sinks) {
    let info = SparkDsLatestOffsetInfo {
        size: size_of::<SparkDsLatestOffsetInfo>() as i64,
        start: start.as_ptr(),
        limit,
        run_end: run_end.map_or(ptr::null(), CStr::as_ptr),
    };
    emitted(library, |sink| unsafe {
        library.latest_offset.unwrap()(reader, &info, Some(add_value), sink)
    })
}

fn stream_partitions(
    library: &SparkDsLibrary,
    reader: *mut c_void,
    start: &CStr,
    end: &CStr,
) -> (c_int, Sinks) {
    let info = SparkDsStreamPartitionsInfo {
        size: size_of::<SparkDsStreamPartitionsInfo>() as i64,
        start: start.as_ptr(),
        end: end.as_ptr(),
    };
    emitted(library, |sink| unsafe {
        library.stream_partitions.unwrap()(reader, &info, Some(add_value), Some(set_state), sink)
    })
}

fn task(partition: i32) -> SparkDsTaskContext {
    SparkDsTaskContext {
        size: size_of::<SparkDsTaskContext>() as i64,
        stage_id: 1,
        stage_attempt_number: 0,
        partition_id: partition,
        attempt_number: 0,
        task_attempt_id: 10 + i64::from(partition),
        is_interrupted: None,
    }
}

/// Reads a partition with the read function that batch scans and micro-batches share.
fn read(library: &SparkDsLibrary, kind: i32, state: &[u8], partition: &[u8]) -> Vec<i64> {
    let task = task(0);
    let mut read_schema = ArrowSchema::try_from(schema().as_ref()).unwrap();
    let info = SparkDsReadInfo {
        size: size_of::<SparkDsReadInfo>() as i64,
        kind,
        schema: &mut read_schema,
        reader_state: state.as_ptr(),
        reader_state_length: state.len() as i64,
        partition: partition.as_ptr(),
        partition_length: partition.len() as i64,
    };
    let mut stream = FFI_ArrowArrayStream::empty();
    success(library, unsafe {
        library.read.unwrap()(&task, &info, &mut stream)
    });
    ArrowArrayStreamReader::try_new(stream)
        .unwrap()
        .flat_map(|batch| {
            let batch = batch.unwrap();
            let values = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            values.values().to_vec()
        })
        .collect()
}

#[test]
fn streaming_operations_declare_their_capabilities_and_callbacks() {
    let _guard = TEST_LOCK.lock().unwrap_or_else(PoisonError::into_inner);
    let library = library();
    assert!(library.create_reader.is_some() && library.create_stream_reader.is_some());
    assert!(library.initial_offset.is_some() && library.latest_offset.is_some());
    assert!(library.stream_partitions.is_some() && library.default_read_limit.is_some());
    assert!(library.commit_offset.is_some() && library.close_stream_reader.is_some());
    assert!(library.read.is_some() && library.create_writer.is_some());
    assert!(library.create_data_writer.is_some() && library.close_data_writer.is_some());

    // Every stream supports admission control, and only streaming writes truncate.
    let streaming_truncate = abi::STREAMING_WRITE | abi::STREAMING_TRUNCATE;
    let reads = abi::BATCH_READ | abi::MICRO_BATCH_READ | abi::ADMISSION_CONTROL;
    for (capabilities, expected) in [
        ("", reads | streaming_truncate),
        ("append_only", reads | abi::STREAMING_WRITE),
        // Truncation goes with the only write that supports it.
        ("no_streaming_write", reads),
    ] {
        let (handle, mask) = source(library, &[("capabilities", capabilities)]);
        assert_eq!(mask, expected, "{capabilities}");
        unsafe { library.close.unwrap()(handle) };
    }
}

#[test]
fn streams_plan_micro_batches_between_json_offsets() {
    let _guard = TEST_LOCK.lock().unwrap_or_else(PoisonError::into_inner);
    EVENTS.lock().unwrap().clear();
    let library = library();
    let reader = stream_reader(library, &[]);
    let checkpoint = "file:/tmp/checkpoint/sources/0".to_owned();
    assert_eq!(events(), vec![Event::StreamCreated(checkpoint.clone())]);

    let mut limit = read_limit(abi::READ_MAX_ROWS, 99);
    success(library, unsafe {
        library.default_read_limit.unwrap()(reader, &mut limit)
    });
    assert_eq!(limit.kind, abi::READ_ALL_AVAILABLE, "no limit by default");

    let initial_info = SparkDsInitialOffsetInfo {
        size: size_of::<SparkDsInitialOffsetInfo>() as i64,
    };
    let initial = single(library, |sink| unsafe {
        library.initial_offset.unwrap()(reader, &initial_info, Some(add_value), sink)
    });
    assert_eq!(initial, br#"{"position":0,"note":"two\nlines"}"#);
    LATEST.store(3, Ordering::SeqCst);
    let start = CString::new(initial).unwrap();
    let all = read_limit(abi::READ_ALL_AVAILABLE, 0);
    let (status, mut offsets) = latest(library, reader, &start, &all, None);
    success(library, status);
    let latest_offset = offsets.values.pop().unwrap();
    assert!(
        !latest_offset.contains(&b'\n'),
        "an offset must be on one line"
    );

    let end = CString::new(latest_offset).unwrap();
    let (status, plan) = stream_partitions(library, reader, &start, &end);
    success(library, status);
    assert_eq!(plan.states.len(), 1, "a plan has a single state");
    let values: Vec<i64> = plan
        .values
        .iter()
        .flat_map(|partition| read(library, abi::READ_MICRO_BATCH, &plan.states[0], partition))
        .collect();
    assert_eq!(values, vec![0, 10, 20]);
    let commit = SparkDsCommitOffsetInfo {
        size: size_of::<SparkDsCommitOffsetInfo>() as i64,
        end: end.as_ptr(),
    };
    success(library, unsafe {
        library.commit_offset.unwrap()(reader, &commit)
    });
    assert_eq!(
        events(),
        vec![
            Event::StreamCreated(checkpoint),
            Event::LatestOffset(0, ReadLimit::AllAvailable, None),
            Event::OffsetCommitted(3)
        ]
    );

    // A batch scan of the same source reads its own partitions through the same function.
    let (handle, _) = source(library, &[]);
    let mut read_schema = ArrowSchema::try_from(schema().as_ref()).unwrap();
    let reader_info = SparkDsReaderInfo {
        size: size_of::<SparkDsReaderInfo>() as i64,
    };
    let mut planner = ptr::null_mut();
    success(library, unsafe {
        library.create_reader.unwrap()(handle, &mut read_schema, &reader_info, &mut planner)
    });
    unsafe { library.close.unwrap()(handle) };
    let partitions_info = SparkDsPartitionsInfo {
        size: size_of::<SparkDsPartitionsInfo>() as i64,
    };
    let batch_plan = planned(library, |sink| unsafe {
        library.partitions.unwrap()(
            planner,
            &partitions_info,
            Some(add_value),
            Some(set_state),
            sink,
        )
    });
    unsafe { library.close_reader.unwrap()(planner) };
    let batch_state = &batch_plan.states[0];
    assert_eq!(
        read(library, abi::READ_BATCH, batch_state, &batch_plan.values[0]),
        vec![105]
    );

    // The kind of the read selects the factory: a batch state is not that of a micro-batch.
    let task = task(0);
    let mut stream = FFI_ArrowArrayStream::empty();
    for kind in [abi::READ_MICRO_BATCH, 7] {
        let mut read_schema = ArrowSchema::try_from(schema().as_ref()).unwrap();
        let info = SparkDsReadInfo {
            size: size_of::<SparkDsReadInfo>() as i64,
            kind,
            schema: &mut read_schema,
            reader_state: batch_state.as_ptr(),
            reader_state_length: batch_state.len() as i64,
            partition: batch_plan.values[0].as_ptr(),
            partition_length: batch_plan.values[0].len() as i64,
        };
        assert_ne!(
            unsafe { library.read.unwrap()(&task, &info, &mut stream) },
            0
        );
        assert!(stream.release.is_none());
    }
    assert!(error(library).contains("unsupported kind of read: 7"));

    // An offset that does not decode fails the call, after which Spark closes the reader.
    let invalid = c"not an offset";
    let (status, _) = stream_partitions(library, reader, &start, invalid);
    assert_ne!(status, 0);
    let failure = error(library);
    assert!(
        failure.contains("cannot decode the offset not an offset"),
        "{failure}"
    );
    unsafe { library.close_stream_reader.unwrap()(reader) };
}

#[test]
fn streams_limit_their_micro_batches_and_runs_with_admission_control() {
    let _guard = TEST_LOCK.lock().unwrap_or_else(PoisonError::into_inner);
    EVENTS.lock().unwrap().clear();
    let library = library();
    let reader = stream_reader(library, &[("maxrows", "2")]);
    let mut limit = read_limit(abi::READ_ALL_AVAILABLE, 0);
    success(library, unsafe {
        library.default_read_limit.unwrap()(reader, &mut limit)
    });
    assert_eq!((limit.kind, limit.max_rows), (abi::READ_MAX_ROWS, 2));

    LATEST.store(5, Ordering::SeqCst);
    let start = CString::new(serde_json::to_string(&Offset::at(1)).unwrap()).unwrap();
    let run_end = CString::new(serde_json::to_string(&Offset::at(2)).unwrap()).unwrap();
    // The micro-batch of a limit, then that of a limit within the end of a run.
    for (run_end, expected) in [(None, 3), (Some(run_end.as_c_str()), 2)] {
        let (status, mut offsets) = latest(library, reader, &start, &limit, run_end);
        success(library, status);
        let offset: Offset = serde_json::from_slice(&offsets.values.pop().unwrap()).unwrap();
        assert_eq!(offset, Offset::at(expected));
    }
    let unknown = read_limit(9, 0);
    let (status, _) = latest(library, reader, &start, &unknown, None);
    assert_ne!(status, 0);
    assert!(error(library).contains("unsupported kind of read limit: 9"));
    assert_eq!(
        events()[1..],
        [
            Event::LatestOffset(1, ReadLimit::MaxRows(2), None),
            Event::LatestOffset(1, ReadLimit::MaxRows(2), Some(2)),
        ]
    );
    unsafe { library.close_stream_reader.unwrap()(reader) };
}

#[test]
fn a_panic_is_the_failure_of_its_call() {
    let _guard = TEST_LOCK.lock().unwrap_or_else(PoisonError::into_inner);
    let library = library();
    let reader = stream_reader(library, &[("failure", "latest_panic")]);
    let start = CString::new(serde_json::to_string(&Offset::at(0)).unwrap()).unwrap();
    let (status, _) = latest(library, reader, &start, &read_limit(0, 0), None);
    assert_ne!(status, 0);
    let failure = error(library);
    assert!(
        failure.contains("finding the latest offset panicked"),
        "{failure}"
    );
    unsafe { library.close_stream_reader.unwrap()(reader) };
}

fn create_writer(
    library: &SparkDsLibrary,
    options: &[(&str, &str)],
    mode: i32,
    query: *const c_char,
) -> std::result::Result<(*mut c_void, Vec<u8>), String> {
    let (source, _) = source(library, options);
    let mut input = ArrowSchema::try_from(schema().as_ref()).unwrap();
    let info = SparkDsWriteInfo {
        size: size_of::<SparkDsWriteInfo>() as i64,
        mode,
        query_id: query,
    };
    let mut writer = ptr::null_mut();
    let mut sinks = Sinks::default();
    let status = unsafe {
        library.create_writer.unwrap()(
            source,
            &mut input,
            &info,
            Some(set_state),
            (&mut sinks as *mut Sinks).cast(),
            &mut writer,
        )
    };
    // Before close, which clears the error of the thread.
    let result = if status == 0 {
        assert_eq!(sinks.states.len(), 1, "a writer has a single state");
        Ok((writer, sinks.states.pop().unwrap()))
    } else {
        assert!(writer.is_null() && sinks.states.is_empty());
        Err(error(library))
    };
    unsafe { library.close.unwrap()(source) };
    result
}

fn finish(
    library: &SparkDsLibrary,
    writer: *mut c_void,
    abort: bool,
    epoch_id: i64,
    messages: &[Option<&[u8]>],
    rejected: bool,
) -> c_int {
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
        epoch_id,
        messages: pointers.as_ptr(),
        message_lengths: lengths.as_ptr(),
        num_messages: messages.len() as i64,
        rejected: i64::from(rejected),
    };
    let callback = if abort {
        library.abort.unwrap()
    } else {
        library.commit.unwrap()
    };
    unsafe { callback(writer, &info) }
}

#[test]
fn streaming_writes_pass_the_query_and_the_epoch_to_the_write_and_its_tasks() {
    let _guard = TEST_LOCK.lock().unwrap_or_else(PoisonError::into_inner);
    EVENTS.lock().unwrap().clear();
    let library = library();
    let (writer, state) = create_writer(library, &[], 1, c"query-1".as_ptr()).unwrap();

    // Each task gets the epoch of its micro-batch, and the schema of the data.
    let task = task(0);
    let mut data_writer = ptr::null_mut();
    let create = |epoch_id: i64, data_writer: &mut *mut c_void| {
        let mut input = ArrowSchema::try_from(schema().as_ref()).unwrap();
        let info = SparkDsDataWriterInfo {
            size: size_of::<SparkDsDataWriterInfo>() as i64,
            schema: &mut input,
            writer_state: state.as_ptr(),
            writer_state_length: state.len() as i64,
            epoch_id,
        };
        unsafe { library.create_data_writer.unwrap()(&task, &info, data_writer) }
    };
    assert_ne!(create(-2, &mut data_writer), 0);
    assert!(error(library).contains("invalid epoch ID -2"));
    // -1 is the epoch of a batch write, which this source does not have.
    assert_ne!(create(-1, &mut data_writer), 0);
    assert!(data_writer.is_null());
    success(library, create(7, &mut data_writer));
    let mut array = FFI_ArrowArray::new(&StructArray::from(batch(vec![4, 2])).to_data());
    success(library, unsafe {
        library.write.unwrap()(data_writer, &mut array)
    });
    let finish_info = SparkDsFinishDataWriterInfo {
        size: size_of::<SparkDsFinishDataWriterInfo>() as i64,
    };
    let message = single(library, |sink| unsafe {
        library.commit_data_writer.unwrap()(data_writer, &finish_info, Some(add_value), sink)
    });
    unsafe { library.close_data_writer.unwrap()(data_writer) };

    success(
        library,
        finish(library, writer, false, 7, &[Some(&message)], false),
    );
    // Spark closes the writer of a micro-batch only after the next micro-batch created its
    // own, but the SDK drops a write as soon as it is committed or aborted.
    let previous = writer;
    let (writer, _) = create_writer(library, &[], 0, c"query-1".as_ptr()).unwrap();
    unsafe { library.close_writer.unwrap()(previous) };
    success(library, finish(library, writer, true, 8, &[None], false));

    // A rejected writer is dropped without its abort at close, as when its creation fails.
    let previous = writer;
    let (writer, _) = create_writer(library, &[], 0, c"query-1".as_ptr()).unwrap();
    unsafe { library.close_writer.unwrap()(previous) };
    success(library, finish(library, writer, true, -1, &[], true));
    unsafe { library.close_writer.unwrap()(writer) };

    // The source has no batch writes, and an append-only source does not truncate.
    let error = create_writer(library, &[], 0, ptr::null()).unwrap_err();
    assert!(error.contains("does not support batch writes"), "{error}");
    let options = [("capabilities", "append_only")];
    let error = create_writer(library, &options, 1, c"query-2".as_ptr()).unwrap_err();
    assert!(
        error.contains("does not support the output mode complete"),
        "{error}"
    );

    let committed = Message {
        epoch_id: Some(7),
        values: vec![4, 2],
    };
    let created = |truncate| Event::WriteCreated {
        query_id: "query-1".into(),
        truncate,
    };
    assert_eq!(
        events(),
        vec![
            created(true),
            Event::EpochCommitted(7, vec![committed]),
            Event::WriteDropped,
            created(false),
            Event::EpochAborted(8, vec![None]),
            Event::WriteDropped,
            created(false),
            Event::WriteDropped,
        ]
    );
}
