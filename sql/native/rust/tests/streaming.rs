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
use std::sync::{Arc, Mutex};

use arrow_array::ffi::to_ffi;
use arrow_array::ffi_stream::{ArrowArrayStreamReader, FFI_ArrowArrayStream};
use arrow_array::{Array, Int64Array, RecordBatch, StructArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use serde::{Deserialize, Serialize};
use spark_native_source::__private::abi::{
    ArrowSchema, SparkDsCreateInfo, SparkDsHost, SparkDsLibrary, SparkDsTaskContext,
    SparkDsWriteInfo, ABI_VERSION,
};
use spark_native_source::{
    Capabilities, CreateInfo, DataSource, DataWriter, DataWriterFactory, MicroBatchStream,
    MicroBatchStreamInfo, PartitionReader, PartitionReaderFactory, PartitionWriter, ReaderContext,
    Result, ScanBuilder, ScanInfo, ScanPlan, StreamingWrite, StreamingWriteInfo, Unsupported,
    WriterContext,
};

// Each test exercises the actual exported table; this lock isolates fixture observations.
static TEST_LOCK: Mutex<()> = Mutex::new(());
static EVENTS: Mutex<Vec<Event>> = Mutex::new(Vec::new());
// The position of the latest data of the streams.
static LATEST: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Debug, PartialEq)]
enum Event {
    StreamCreated,
    OffsetCommitted(u64),
    WriteCreated { query_id: String, truncate: bool },
    EpochCommitted(i64, Vec<Message>),
    EpochAborted(i64, Vec<Option<Message>>),
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
        })
    }

    fn schema(&self) -> Result<SchemaRef> {
        Ok(schema())
    }

    fn capabilities(&self) -> Capabilities {
        match self.capabilities.as_str() {
            "no_streaming_write" => Capabilities::all().without(Capabilities::STREAMING_WRITE),
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
        record(Event::StreamCreated);
        Ok(Stream {
            failure: self.failure.clone(),
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

    fn latest_offset(&mut self) -> Result<Offset> {
        if self.failure == "latest_panic" {
            panic!("finding the latest offset panicked");
        }
        Ok(Offset::at(LATEST.load(Ordering::SeqCst)))
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

unsafe extern "C" fn collect(sink: *mut c_void, bytes: *const u8, length: i64) {
    let output = unsafe { &mut *sink.cast::<Vec<Vec<u8>>>() };
    output.push(unsafe { std::slice::from_raw_parts(bytes, length as usize) }.to_vec());
}

/// Calls a function of the table that passes its results to a sink.
fn emitted(
    library: &SparkDsLibrary,
    call: impl FnOnce(*mut c_void) -> c_int,
) -> (c_int, Vec<Vec<u8>>) {
    let mut output = Vec::<Vec<u8>>::new();
    let status = call((&mut output as *mut Vec<Vec<u8>>).cast());
    if status != 0 {
        assert!(output.is_empty());
        let _ = error(library);
    }
    (status, output)
}

fn single(library: &SparkDsLibrary, call: impl FnOnce(*mut c_void) -> c_int) -> Vec<u8> {
    let (status, mut output) = emitted(library, call);
    success(library, status);
    assert_eq!(output.len(), 1);
    output.pop().unwrap()
}

fn source(library: &SparkDsLibrary, failure: &str, capabilities: &str) -> (*mut c_void, u64) {
    let failure = CString::new(failure).unwrap();
    let capabilities = CString::new(capabilities).unwrap();
    let keys = [c"failure".as_ptr(), c"capabilities".as_ptr()];
    let values = [failure.as_ptr(), capabilities.as_ptr()];
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

fn stream_reader(library: &SparkDsLibrary, failure: &str) -> *mut c_void {
    let (source, _) = source(library, failure, "");
    let mut schema = ArrowSchema::try_from(schema().as_ref()).unwrap();
    let mut reader = ptr::null_mut();
    let status = unsafe { library.create_stream_reader.unwrap()(source, &mut schema, &mut reader) };
    // Before close, which clears the error of the thread.
    success(library, status);
    // A stream reader outlives its source, which Spark closes right after creating it.
    unsafe { library.close.unwrap()(source) };
    reader
}

fn latest(library: &SparkDsLibrary, reader: *mut c_void) -> (c_int, Vec<Vec<u8>>) {
    emitted(library, |sink| unsafe {
        library.latest_offset.unwrap()(reader, Some(collect), sink)
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
fn read(library: &SparkDsLibrary, state: &[u8], partition: &[u8]) -> Vec<i64> {
    let task = task(0);
    let mut stream = FFI_ArrowArrayStream::empty();
    success(library, unsafe {
        library.read.unwrap()(
            &task,
            state.as_ptr(),
            state.len() as i64,
            partition.as_ptr(),
            partition.len() as i64,
            &mut stream,
        )
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
    let _guard = TEST_LOCK.lock().unwrap();
    let library = library();
    assert!(library.create_reader.is_some() && library.create_stream_reader.is_some());
    assert!(library.initial_offset.is_some() && library.latest_offset.is_some());
    assert!(library.stream_partitions.is_some() && library.serialize_stream_reader.is_some());
    assert!(library.commit_offset.is_some() && library.close_stream_reader.is_some());
    assert!(library.read.is_some() && library.create_writer.is_some());
    assert!(library.create_data_writer.is_some() && library.close_data_writer.is_some());

    let (handle, mask) = source(library, "", "");
    assert_eq!(mask, 1 | 2 | 8 | 16);
    unsafe { library.close.unwrap()(handle) };
    // Truncation goes with the only write that supports it.
    let (handle, mask) = source(library, "", "no_streaming_write");
    assert_eq!(mask, 1 | 2);
    unsafe { library.close.unwrap()(handle) };
}

#[test]
fn streams_plan_micro_batches_between_json_offsets() {
    let _guard = TEST_LOCK.lock().unwrap();
    EVENTS.lock().unwrap().clear();
    let library = library();
    let reader = stream_reader(library, "");
    assert_eq!(events(), vec![Event::StreamCreated]);
    let (status, _) = emitted(library, |sink| unsafe {
        library.serialize_stream_reader.unwrap()(reader, Some(collect), sink)
    });
    assert_ne!(
        status, 0,
        "a stream has no state before its first micro-batch"
    );
    assert!(error(library).contains("no micro-batch"));

    let initial = single(library, |sink| unsafe {
        library.initial_offset.unwrap()(reader, Some(collect), sink)
    });
    assert_eq!(initial, br#"{"position":0,"note":"two\nlines"}"#);
    LATEST.store(3, Ordering::SeqCst);
    let (status, mut offsets) = latest(library, reader);
    success(library, status);
    let latest_offset = offsets.pop().unwrap();
    assert!(
        !latest_offset.contains(&b'\n'),
        "an offset must be on one line"
    );

    let start = CString::new(initial).unwrap();
    let end = CString::new(latest_offset).unwrap();
    let (status, partitions) = emitted(library, |sink| unsafe {
        library.stream_partitions.unwrap()(
            reader,
            start.as_ptr(),
            end.as_ptr(),
            Some(collect),
            sink,
        )
    });
    success(library, status);
    let state = single(library, |sink| unsafe {
        library.serialize_stream_reader.unwrap()(reader, Some(collect), sink)
    });
    let values: Vec<i64> = partitions
        .iter()
        .flat_map(|partition| read(library, &state, partition))
        .collect();
    assert_eq!(values, vec![0, 10, 20]);
    success(library, unsafe {
        library.commit_offset.unwrap()(reader, end.as_ptr())
    });
    assert_eq!(
        events(),
        vec![Event::StreamCreated, Event::OffsetCommitted(3)]
    );

    // A batch scan of the same source reads its own partitions through the same function.
    let (handle, _) = source(library, "", "");
    let mut read_schema = ArrowSchema::try_from(schema().as_ref()).unwrap();
    let mut planner = ptr::null_mut();
    success(library, unsafe {
        library.create_reader.unwrap()(handle, &mut read_schema, &mut planner)
    });
    unsafe { library.close.unwrap()(handle) };
    let (status, partitions) = emitted(library, |sink| unsafe {
        library.partitions.unwrap()(planner, Some(collect), sink)
    });
    success(library, status);
    let batch_state = single(library, |sink| unsafe {
        library.serialize_reader.unwrap()(planner, Some(collect), sink)
    });
    unsafe { library.close_reader.unwrap()(planner) };
    assert_eq!(read(library, &batch_state, &partitions[0]), vec![105]);

    // An offset that does not decode fails the stream, and so every later call.
    let invalid = c"not an offset";
    let (status, _) = emitted(library, |sink| unsafe {
        library.stream_partitions.unwrap()(
            reader,
            start.as_ptr(),
            invalid.as_ptr(),
            Some(collect),
            sink,
        )
    });
    assert_ne!(status, 0);
    let failure = error(library);
    assert!(
        failure.contains("cannot decode the offset not an offset"),
        "{failure}"
    );
    let (status, _) = latest(library, reader);
    assert_ne!(status, 0);
    assert_eq!(error(library), failure);
    unsafe { library.close_stream_reader.unwrap()(reader) };
}

#[test]
fn a_panic_fails_the_stream_for_every_later_call() {
    let _guard = TEST_LOCK.lock().unwrap();
    let library = library();
    let reader = stream_reader(library, "latest_panic");
    let (status, _) = latest(library, reader);
    assert_ne!(status, 0);
    let failure = error(library);
    assert!(
        failure.contains("finding the latest offset panicked"),
        "{failure}"
    );
    let (status, _) = emitted(library, |sink| unsafe {
        library.initial_offset.unwrap()(reader, Some(collect), sink)
    });
    assert_ne!(status, 0);
    assert_eq!(error(library), failure);
    unsafe { library.close_stream_reader.unwrap()(reader) };
}

fn create_writer(
    library: &SparkDsLibrary,
    mode: i32,
    query: *const c_char,
) -> std::result::Result<*mut c_void, String> {
    let (source, _) = source(library, "", "");
    let mut input = ArrowSchema::try_from(schema().as_ref()).unwrap();
    let info = SparkDsWriteInfo {
        size: size_of::<SparkDsWriteInfo>() as i64,
        mode,
        query_id: query,
    };
    let mut writer = ptr::null_mut();
    let status = unsafe { library.create_writer.unwrap()(source, &mut input, &info, &mut writer) };
    // Before close, which clears the error of the thread.
    let result = if status == 0 {
        Ok(writer)
    } else {
        assert!(writer.is_null());
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
) -> c_int {
    let pointers: Vec<_> = messages
        .iter()
        .map(|message| message.map_or(ptr::null(), |bytes| bytes.as_ptr()))
        .collect();
    let lengths: Vec<_> = messages
        .iter()
        .map(|message| message.map_or(0, |bytes| bytes.len() as i64))
        .collect();
    let callback = if abort {
        library.abort.unwrap()
    } else {
        library.commit.unwrap()
    };
    unsafe {
        callback(
            writer,
            epoch_id,
            pointers.as_ptr(),
            lengths.as_ptr(),
            messages.len() as i64,
        )
    }
}

#[test]
fn streaming_writes_pass_the_query_and_the_epoch_to_the_write_and_its_tasks() {
    let _guard = TEST_LOCK.lock().unwrap();
    EVENTS.lock().unwrap().clear();
    let library = library();
    let writer = create_writer(library, 1, c"query-1".as_ptr()).unwrap();
    let state = single(library, |sink| unsafe {
        library.serialize_writer.unwrap()(writer, Some(collect), sink)
    });

    // Each task gets the epoch of its micro-batch, which must not be that of a batch write.
    let task = task(0);
    let mut data_writer = ptr::null_mut();
    let create = |epoch_id: i64, data_writer: &mut *mut c_void| unsafe {
        library.create_data_writer.unwrap()(
            &task,
            state.as_ptr(),
            state.len() as i64,
            epoch_id,
            data_writer,
        )
    };
    assert_ne!(create(-1, &mut data_writer), 0);
    assert!(error(library).contains("epoch"));
    assert!(data_writer.is_null());
    success(library, create(7, &mut data_writer));
    let data = StructArray::from(batch(vec![4, 2])).to_data();
    let (mut array, mut array_schema) = to_ffi(&data).unwrap();
    success(library, unsafe {
        library.write.unwrap()(data_writer, &mut array, &mut array_schema)
    });
    let message = single(library, |sink| unsafe {
        library.commit_data_writer.unwrap()(data_writer, Some(collect), sink)
    });
    unsafe { library.close_data_writer.unwrap()(data_writer) };

    assert_ne!(finish(library, writer, false, -1, &[Some(&message)]), 0);
    assert!(error(library).contains("epoch"));
    success(
        library,
        finish(library, writer, false, 7, &[Some(&message)]),
    );
    unsafe { library.close_writer.unwrap()(writer) };

    let writer = create_writer(library, 0, c"query-1".as_ptr()).unwrap();
    success(library, finish(library, writer, true, 8, &[None]));
    unsafe { library.close_writer.unwrap()(writer) };

    // The source has no batch writes.
    let error = create_writer(library, 0, ptr::null()).unwrap_err();
    assert!(error.contains("does not support batch writes"), "{error}");

    let committed = Message {
        epoch_id: Some(7),
        values: vec![4, 2],
    };
    assert_eq!(
        events(),
        vec![
            Event::WriteCreated {
                query_id: "query-1".into(),
                truncate: true
            },
            Event::EpochCommitted(7, vec![committed]),
            Event::WriteCreated {
                query_id: "query-1".into(),
                truncate: false
            },
            Event::EpochAborted(8, vec![None]),
        ]
    );
}
