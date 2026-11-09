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

//! Driver-only handles of sources, scans, streams, and writes, and executor readers restored
//! from serialized values.

use std::any::Any;
use std::collections::HashMap;
use std::ffi::CString;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

use arrow_array::ffi_stream::FFI_ArrowArrayStream;
use arrow_schema::SchemaRef;
use serde::{de::DeserializeOwned, Deserialize, Serialize};

use crate::write::{Batch, DriverWrite, DriverWriter};
use crate::{
    abi, BatchWrite, Capabilities, CreateInfo, DataSource, LatestOffsetInfo, LimitSupport,
    MicroBatchStream, MicroBatchStreamInfo, PartitionReaderFactory, ReadLimit, ReaderContext,
    Result, ScanBuilder, ScanInfo, ScanPlan, StreamingWrite, StreamingWriteInfo, TaskContext,
    WriteInfo, WriteMode,
};

#[path = "stream.rs"]
pub(crate) mod stream;

const STATE_VERSION: u32 = 4;

type HandleMap<T> = Mutex<HashMap<i64, Arc<Mutex<T>>>>;

/// One registry per loaded plugin. Provider code runs under a per-handle lock,
/// so independent sources, scans, and streams do not serialize behind a global lock.
pub struct Bridge<S: DataSource> {
    next_handle: AtomicI64,
    sources: HandleMap<SourceState<S>>,
    // The builder of each scan, until Spark plans the scan.
    scans: HandleMap<Option<S::ScanBuilder>>,
    streams: HandleMap<S::MicroBatchStream>,
    writers: HandleMap<Box<dyn DriverWrite>>,
}

impl<S: DataSource> Default for Bridge<S> {
    fn default() -> Self {
        Self {
            next_handle: AtomicI64::new(1),
            sources: Mutex::new(HashMap::new()),
            scans: Mutex::new(HashMap::new()),
            streams: Mutex::new(HashMap::new()),
            writers: Mutex::new(HashMap::new()),
        }
    }
}

/// The partitions of a scan or a micro-batch, and the state of its reader: its factory.
pub struct EncodedPlan {
    pub state: Vec<u8>,
    pub partitions: Vec<Vec<u8>>,
}

impl<S: DataSource> Bridge<S> {
    // Whether the source implements each operation, whose native callbacks the SDK installs.

    pub const READ: bool = <S::ScanBuilder as ScanBuilder>::SUPPORTED;

    pub const MICRO_BATCH_READ: bool = <S::MicroBatchStream as MicroBatchStream>::SUPPORTED;

    pub const WRITE: bool = <S::BatchWrite as BatchWrite>::SUPPORTED;

    pub const STREAMING_WRITE: bool = <S::StreamingWrite as StreamingWrite>::SUPPORTED;

    const CAPABILITIES: Capabilities = Capabilities::implemented(&[
        (Capabilities::BATCH_READ, Self::READ),
        (Capabilities::MICRO_BATCH_READ, Self::MICRO_BATCH_READ),
        (Capabilities::BATCH_WRITE, Self::WRITE),
        (Capabilities::STREAMING_WRITE, Self::STREAMING_WRITE),
        (
            Capabilities::BATCH_TRUNCATE,
            <S::BatchWrite as BatchWrite>::SUPPORTS_TRUNCATE,
        ),
        (
            Capabilities::STREAMING_TRUNCATE,
            <S::StreamingWrite as StreamingWrite>::SUPPORTS_TRUNCATE,
        ),
    ]);

    fn insert<T>(&self, handles: &HandleMap<T>, value: T) -> Result<i64> {
        let handle = self
            .next_handle
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |id| id.checked_add(1))
            .map_err(|_| "native handle IDs exhausted")?;
        lock(handles).insert(handle, Arc::new(Mutex::new(value)));
        Ok(handle)
    }

    pub fn create(&self, info: CreateInfo) -> Result<(i64, u64)> {
        self.register_source(S::new(info)?)
    }

    fn register_source(&self, source: S) -> Result<(i64, u64)> {
        let capabilities = Self::effective(source.capabilities());
        let handle = self.insert(
            &self.sources,
            SourceState {
                source,
                capabilities,
            },
        )?;
        // Every stream supports admission control: its default read limit is all the
        // available data unless it overrides MicroBatchStream::default_read_limit.
        let admission_control = if capabilities.contains(Capabilities::MICRO_BATCH_READ) {
            abi::ADMISSION_CONTROL
        } else {
            0
        };
        Ok((handle, capabilities.bits() | admission_control))
    }

    /// Keeps the implemented operations of a declaration, so that a source can narrow its
    /// operations but never add one, and each truncation only with its write.
    fn effective(declared: Capabilities) -> Capabilities {
        let mut capabilities = declared.intersection(Self::CAPABILITIES);
        for (write, truncate) in [
            (Capabilities::BATCH_WRITE, Capabilities::BATCH_TRUNCATE),
            (
                Capabilities::STREAMING_WRITE,
                Capabilities::STREAMING_TRUNCATE,
            ),
        ] {
            if !capabilities.contains(write) {
                capabilities = capabilities.without(truncate);
            }
        }
        capabilities
    }

    pub fn schema(&self, handle: i64) -> Result<SchemaRef> {
        let source = get(&self.sources, handle)?;
        let schema = lock(&source).source.schema()?;
        stream::validate_schema(&schema)?;
        Ok(schema)
    }

    pub fn create_reader(&self, handle: i64, schema: SchemaRef) -> Result<i64> {
        stream::validate_schema(&schema)?;
        let source = get(&self.sources, handle)?;
        let builder = {
            let source = lock(&source);
            require(source.capabilities, Capabilities::BATCH_READ, "batch reads")?;
            source.source.new_scan(ScanInfo::new(schema))?
        };
        self.insert(&self.scans, Some(builder))
    }

    pub fn push_limit(&self, handle: i64, limit: usize) -> Result<bool> {
        let scan = get(&self.scans, handle)?;
        let mut scan = lock(&scan);
        Ok(builder(&mut scan)?.push_limit(limit)? == LimitSupport::Global)
    }

    pub fn prune_columns(&self, handle: i64, columns: &[String]) -> Result<()> {
        let scan = get(&self.scans, handle)?;
        let mut scan = lock(&scan);
        builder(&mut scan)?.prune_columns(columns)
    }

    /// Plans a scan, which consumes its builder.
    pub fn plan(&self, handle: i64) -> Result<EncodedPlan> {
        let scan = get(&self.scans, handle)?;
        let builder = lock(&scan)
            .take()
            .ok_or("the scan has already been planned")?;
        encode_plan(builder.plan()?)
    }

    pub fn close_reader(&self, handle: i64) -> Result<()> {
        remove(&self.scans, handle);
        Ok(())
    }

    pub fn create_stream_reader(&self, handle: i64, info: MicroBatchStreamInfo) -> Result<i64> {
        stream::validate_schema(&info.schema)?;
        let source = get(&self.sources, handle)?;
        let stream = {
            let source = lock(&source);
            require(
                source.capabilities,
                Capabilities::MICRO_BATCH_READ,
                "micro-batch streaming reads",
            )?;
            source.source.new_micro_batch_stream(info)?
        };
        self.insert(&self.streams, stream)
    }

    pub fn initial_offset(&self, handle: i64) -> Result<String> {
        let stream = get(&self.streams, handle)?;
        let offset = lock(&stream).initial_offset()?;
        encode_offset(&offset)
    }

    pub fn default_read_limit(&self, handle: i64) -> Result<ReadLimit> {
        let stream = get(&self.streams, handle)?;
        let limit = lock(&stream).default_read_limit();
        limit
    }

    /// Decodes the offsets of a request, and encodes the latest offset.
    pub fn latest_offset(&self, handle: i64, info: LatestOffsetInfo<String>) -> Result<String> {
        let stream = get(&self.streams, handle)?;
        let info = LatestOffsetInfo {
            start: decode_offset(&info.start)?,
            limit: info.limit,
            run_end: info.run_end.as_deref().map(decode_offset).transpose()?,
        };
        let offset = lock(&stream).latest_offset(info)?;
        encode_offset(&offset)
    }

    pub fn stream_partitions(&self, handle: i64, start: &str, end: &str) -> Result<EncodedPlan> {
        let stream = get(&self.streams, handle)?;
        let plan = lock(&stream).plan(decode_offset(start)?, decode_offset(end)?)?;
        encode_plan(plan)
    }

    pub fn commit_offset(&self, handle: i64, end: &str) -> Result<()> {
        let stream = get(&self.streams, handle)?;
        let end = decode_offset(end)?;
        let result = lock(&stream).commit(end);
        result
    }

    pub fn close_stream_reader(&self, handle: i64) -> Result<()> {
        remove(&self.streams, handle);
        Ok(())
    }

    /// Creates a batch write, or the write of a micro-batch of the streaming query `query_id`,
    /// with the state of its task writers.
    pub fn create_writer(
        &self,
        handle: i64,
        schema: SchemaRef,
        mode: WriteMode,
        query_id: Option<String>,
    ) -> Result<(i64, Vec<u8>)> {
        stream::validate_schema(&schema)?;
        let source = get(&self.sources, handle)?;
        let (writer, state) = {
            let source = lock(&source);
            let capabilities = source.capabilities;
            let truncate = mode == WriteMode::Truncate;
            match query_id {
                None => {
                    require(capabilities, Capabilities::BATCH_WRITE, "batch writes")?;
                    if truncate {
                        let overwrite = "the save mode overwrite";
                        require(capabilities, Capabilities::BATCH_TRUNCATE, overwrite)?;
                    }
                    let write = source.source.new_write(WriteInfo::new(schema, mode))?;
                    DriverWriter::create(Batch(write))?
                }
                Some(query_id) => {
                    require(
                        capabilities,
                        Capabilities::STREAMING_WRITE,
                        "streaming writes",
                    )?;
                    if truncate {
                        let complete = "the output mode complete";
                        require(capabilities, Capabilities::STREAMING_TRUNCATE, complete)?;
                    }
                    let info = StreamingWriteInfo::new(schema, mode, query_id);
                    let write = source.source.new_streaming_write(info)?;
                    DriverWriter::create(write)?
                }
            }
        };
        Ok((self.insert(&self.writers, writer)?, state))
    }

    pub fn commit(&self, handle: i64, epoch_id: i64, messages: &[Option<&[u8]>]) -> Result<()> {
        let writer = get(&self.writers, handle)?;
        let result = lock(&writer).commit(epoch_id, messages);
        result
    }

    pub fn abort(&self, handle: i64, epoch_id: i64, messages: &[Option<&[u8]>]) -> Result<()> {
        let writer = get(&self.writers, handle)?;
        let result = lock(&writer).abort(epoch_id, messages);
        result
    }

    pub fn close_writer(&self, handle: i64) -> Result<()> {
        remove(&self.writers, handle);
        Ok(())
    }

    pub fn close_source(&self, handle: i64) -> Result<()> {
        remove(&self.sources, handle);
        Ok(())
    }
}

/// Opens the executor reader of a partition with factory `F`, using only serialized values.
/// No source, scan, stream, or driver handle registry is created or consulted.
///
/// # Safety
/// `task` must satisfy TaskContext::from_raw and remain valid until the
/// returned stream is released. The stream's consumer must follow the
/// Arrow C Stream ownership protocol and keep the plugin loaded.
pub unsafe fn open_reader<F: PartitionReaderFactory>(
    task: *const abi::SparkDsTaskContext,
    schema: SchemaRef,
    state: &[u8],
    partition: &[u8],
) -> Result<FFI_ArrowArrayStream> {
    stream::validate_schema(&schema)?;
    let factory: F = decode(state)?;
    let task = unsafe { TaskContext::from_raw(task)? };
    let reader = factory.open(decode(partition)?, ReaderContext::new(schema.clone(), task))?;
    Ok(unsafe { stream::export(reader, &schema) })
}

fn require(capabilities: Capabilities, capability: Capabilities, operation: &str) -> Result<()> {
    if capabilities.contains(capability) {
        Ok(())
    } else {
        Err(format!("source does not support {operation}").into())
    }
}

/// The builder of a scan that is not planned yet.
fn builder<B>(scan: &mut Option<B>) -> Result<&mut B> {
    scan.as_mut()
        .ok_or_else(|| "the scan has already been planned".into())
}

/// Encodes the factory of a plan, as the state of its reader, and each of its partitions.
fn encode_plan<F: PartitionReaderFactory>(plan: ScanPlan<F>) -> Result<EncodedPlan> {
    Ok(EncodedPlan {
        state: encode(&plan.factory)?,
        partitions: plan.partitions.iter().map(encode).collect::<Result<_>>()?,
    })
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    // A provider panic becomes an ABI error. Recover the lock so close can
    // still remove and release the handle after that failed call.
    mutex.lock().unwrap_or_else(|error| error.into_inner())
}

fn get<T>(handles: &HandleMap<T>, handle: i64) -> Result<Arc<Mutex<T>>> {
    lock(handles)
        .get(&handle)
        .cloned()
        .ok_or_else(|| format!("unknown or closed native handle: {handle}").into())
}

fn remove<T>(handles: &HandleMap<T>, handle: i64) {
    let value = lock(handles).remove(&handle);
    // Provider destructors run after releasing the registry lock. Repeated
    // closes are harmless, and IDs are never reused.
    drop_provider(value);
}

struct SourceState<S> {
    source: S,
    capabilities: Capabilities,
}

/// Offsets are compact JSON on a single line, which Spark stores in the checkpoint as they
/// are, so they have no envelope: the library owns their format.
fn encode_offset<T: Serialize>(offset: &T) -> Result<String> {
    Ok(serde_json::to_string(offset)?)
}

fn decode_offset<T: DeserializeOwned>(json: &str) -> Result<T> {
    serde_json::from_str(json)
        .map_err(|error| format!("cannot decode the offset {json}: {error}").into())
}

/// The state of a reader or a writer, the partitions and the commit messages are MessagePack
/// in a versioned envelope. MessagePack is self-describing, and the SDK writes structs as maps
/// with the names of their fields, so the serde attributes that need them, such as `untagged`,
/// `flatten` and `default`, work; byte vectors are compact, and floats, including NaN and the
/// infinities, round-trip as they are. These values never outlive the application, and the same
/// build of the library reads them, so the format is an implementation detail; see the README.
#[derive(Serialize, Deserialize)]
struct Envelope<T> {
    version: u32,
    value: T,
}

pub(crate) fn encode<T: Serialize>(value: &T) -> Result<Vec<u8>> {
    encode_envelope(STATE_VERSION, value)
}

fn encode_envelope<T: Serialize>(version: u32, value: &T) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    let mut serializer = rmp_serde::Serializer::new(&mut bytes)
        .with_struct_map()
        .with_bytes(rmp_serde::config::BytesMode::ForceAll);
    Envelope { version, value }.serialize(&mut serializer)?;
    Ok(bytes)
}

pub(crate) fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T> {
    let envelope: Envelope<T> = rmp_serde::from_slice(bytes)?;
    if envelope.version != STATE_VERSION {
        return Err(format!("unsupported native state version: {}", envelope.version).into());
    }
    Ok(envelope.value)
}

pub(crate) fn provider_call<T>(
    action: impl FnOnce() -> Result<T>,
) -> std::result::Result<T, String> {
    catch_unwind(AssertUnwindSafe(|| {
        action().map_err(|error| describe(&*error))
    }))
    .unwrap_or_else(|payload| Err(panic_message(payload)))
}

pub(crate) fn drop_provider<T>(value: T) {
    // Close has no error channel and must never unwind across the native ABI, so log the panic.
    if let Err(payload) = catch_unwind(AssertUnwindSafe(|| drop(value))) {
        log::warn!("Ignoring a panic during a drop: {}", panic_message(payload));
    }
}

/// The message of an error followed by those of its sources, which `Display` of many
/// errors, such as those with added context, omits.
pub(crate) fn describe(error: &(dyn std::error::Error + 'static)) -> String {
    let mut message = error.to_string();
    let mut source = error.source();
    while let Some(cause) = source {
        let cause_message = cause.to_string();
        if !message.contains(&cause_message) {
            message.push_str(": ");
            message.push_str(&cause_message);
        }
        source = cause.source();
    }
    message
}

pub(crate) fn panic_message(payload: Box<dyn Any + Send>) -> String {
    let message = if let Some(message) = payload.downcast_ref::<String>() {
        message.clone()
    } else if let Some(message) = payload.downcast_ref::<&str>() {
        (*message).to_owned()
    } else {
        // An arbitrary panic payload can itself panic when dropped.
        std::mem::forget(payload);
        return "native provider panicked with a non-string payload".to_owned();
    };
    format!("native provider panicked: {message}")
}

/// Error and log text is a C string. A provider message may contain embedded NULs.
pub(crate) fn c_message(message: String) -> CString {
    CString::new(message.replace('\0', "\\0")).expect("NULs removed")
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;

    use arrow_array::ffi_stream::ArrowArrayStreamReader;
    use arrow_array::{Int64Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};

    use super::*;
    use crate::{PartitionReader, Unsupported};

    #[derive(Clone, Copy)]
    enum FailureMode {
        None,
        Error,
        Panic,
    }

    impl FailureMode {
        fn fail(self, message: &str) -> Result<()> {
            match self {
                Self::None => Ok(()),
                Self::Error => Err(message.into()),
                Self::Panic => panic!("{message}"),
            }
        }
    }

    struct Source {
        builds: Arc<AtomicUsize>,
        limits: Arc<AtomicUsize>,
        drops: Arc<AtomicUsize>,
        builder_drops: Arc<AtomicUsize>,
        limit_support: LimitSupport,
        failure: FailureMode,
    }

    impl Drop for Source {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::SeqCst);
        }
    }

    struct Builder {
        builds: Arc<AtomicUsize>,
        limits: Arc<AtomicUsize>,
        drops: Arc<AtomicUsize>,
        limit_support: LimitSupport,
        failure: FailureMode,
        value: u64,
    }

    impl Drop for Builder {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::SeqCst);
        }
    }

    impl DataSource for Source {
        type ScanBuilder = Builder;
        type MicroBatchStream = Unsupported;
        type BatchWrite = Unsupported;
        type StreamingWrite = Unsupported;

        fn new(_: CreateInfo) -> Result<Self> {
            Ok(Self {
                builds: Arc::default(),
                limits: Arc::default(),
                drops: Arc::default(),
                builder_drops: Arc::default(),
                limit_support: LimitSupport::Unsupported,
                failure: FailureMode::None,
            })
        }

        fn schema(&self) -> Result<SchemaRef> {
            Ok(id_schema())
        }

        fn new_scan(&self, _: ScanInfo) -> Result<Builder> {
            Ok(Builder {
                builds: self.builds.clone(),
                limits: self.limits.clone(),
                drops: self.builder_drops.clone(),
                limit_support: self.limit_support,
                failure: self.failure,
                value: 42,
            })
        }
    }

    #[derive(Serialize, Deserialize)]
    struct Factory(u64);

    impl PartitionReaderFactory for Factory {
        type Partition = u64;

        // Ignores the schema of the context, to test that the SDK checks it.
        fn open<'task>(
            &self,
            partition: u64,
            _: ReaderContext<'task>,
        ) -> Result<PartitionReader<'task>> {
            let values = Arc::new(Int64Array::from(vec![(self.0 + partition) as i64]));
            let batch = RecordBatch::try_new(id_schema(), vec![values])?;
            Ok(Box::new(std::iter::once(Ok(batch))))
        }
    }

    impl ScanBuilder for Builder {
        type Factory = Factory;

        fn push_limit(&mut self, limit: usize) -> Result<LimitSupport> {
            self.limits.fetch_add(1, Ordering::SeqCst);
            self.value = limit as u64;
            self.failure.fail("negotiation failed")?;
            Ok(self.limit_support)
        }

        fn plan(self) -> Result<ScanPlan<Factory>> {
            self.builds.fetch_add(1, Ordering::SeqCst);
            self.failure.fail("planning failed")?;
            Ok(ScanPlan::new(Factory(self.value), vec![2, 7]))
        }
    }

    fn id_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]))
    }

    fn info() -> CreateInfo {
        CreateInfo::new("bridge_test", Default::default())
    }

    fn task(partition: i32, attempt: i32) -> abi::SparkDsTaskContext {
        abi::SparkDsTaskContext {
            size: std::mem::size_of::<abi::SparkDsTaskContext>() as i64,
            stage_id: 1,
            stage_attempt_number: 0,
            partition_id: partition,
            attempt_number: attempt,
            task_attempt_id: i64::from(attempt) * 2 + i64::from(partition),
            is_interrupted: None,
        }
    }

    fn error_message<T>(result: Result<T>) -> String {
        result
            .err()
            .expect("expected a provider failure")
            .to_string()
    }

    #[test]
    fn source_can_close_before_planning_and_a_scan_is_planned_once() {
        let bridge = Bridge::<Source>::default();
        let source = Source::new(info()).unwrap();
        let builds = source.builds.clone();
        let drops = source.drops.clone();
        let builder_drops = source.builder_drops.clone();
        let (handle, _) = bridge.register_source(source).unwrap();
        let schema = bridge.schema(handle).unwrap();
        let reader = bridge.create_reader(handle, schema).unwrap();
        bridge.close_source(handle).unwrap();
        bridge.close_source(handle).unwrap();
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        assert!(bridge.schema(handle).is_err());

        let plan = bridge.plan(reader).unwrap();
        assert_eq!(decode::<u64>(&plan.partitions[0]).unwrap(), 2);
        assert_eq!(decode::<u64>(&plan.partitions[1]).unwrap(), 7);
        assert_eq!(decode::<Factory>(&plan.state).unwrap().0, 42);
        assert_eq!(builds.load(Ordering::SeqCst), 1);
        // Planning consumed the builder: no other call reaches it.
        assert_eq!(builder_drops.load(Ordering::SeqCst), 1);
        assert!(error_message(bridge.plan(reader)).contains("already been planned"));
        assert!(bridge.prune_columns(reader, &[]).is_err());
        bridge.close_reader(reader).unwrap();
        assert!(bridge.plan(reader).is_err());
    }

    #[test]
    fn only_global_limits_return_true_to_the_abi() {
        for support in [LimitSupport::Unsupported, LimitSupport::Global] {
            let bridge = Bridge::<Source>::default();
            let mut source = Source::new(info()).unwrap();
            source.limit_support = support;
            let limits = source.limits.clone();
            let (handle, _) = bridge.register_source(source).unwrap();
            let reader = bridge
                .create_reader(handle, bridge.schema(handle).unwrap())
                .unwrap();
            assert_eq!(
                bridge.push_limit(reader, 4).unwrap(),
                support == LimitSupport::Global
            );
            assert_eq!(limits.load(Ordering::SeqCst), 1);
        }
    }

    #[test]
    fn negotiation_and_planning_errors_and_panics_are_reported() {
        for (failure, expected) in [
            (FailureMode::Error, "failed"),
            (FailureMode::Panic, "native provider panicked: "),
        ] {
            let bridge = Bridge::<Source>::default();
            let mut source = Source::new(info()).unwrap();
            source.failure = failure;
            let builds = source.builds.clone();
            let drops = source.builder_drops.clone();
            let (handle, _) = bridge.register_source(source).unwrap();
            let schema = bridge.schema(handle).unwrap();
            let negotiated = bridge.create_reader(handle, schema.clone()).unwrap();
            let planned = bridge.create_reader(handle, schema).unwrap();
            bridge.close_source(handle).unwrap();

            let call = |action: &dyn Fn() -> Result<()>| {
                let status = abi::call(action);
                assert_ne!(status, 0);
                let error = unsafe { std::ffi::CStr::from_ptr(abi::last_error()) };
                error.to_str().unwrap().to_owned()
            };
            let error = call(&|| bridge.push_limit(negotiated, 1).map(drop));
            assert!(error.contains(expected), "{error}");
            assert!(error.contains("negotiation failed"), "{error}");
            let error = call(&|| bridge.plan(planned).map(drop));
            assert!(error.contains(expected), "{error}");
            assert!(error.contains("planning failed"), "{error}");
            assert_eq!(builds.load(Ordering::SeqCst), 1);
            // The failed plan consumed its builder; Spark only closes the other scan.
            assert_eq!(drops.load(Ordering::SeqCst), 1);
            bridge.close_reader(negotiated).unwrap();
            bridge.close_reader(planned).unwrap();
            assert_eq!(drops.load(Ordering::SeqCst), 2);
        }
    }

    #[test]
    fn scans_of_one_source_keep_pushdown_and_plans_independent() {
        let bridge = Bridge::<Source>::default();
        let mut source = Source::new(info()).unwrap();
        source.limit_support = LimitSupport::Global;
        let builds = source.builds.clone();
        let limits = source.limits.clone();
        let (handle, _) = bridge.register_source(source).unwrap();
        let first = bridge
            .create_reader(handle, bridge.schema(handle).unwrap())
            .unwrap();
        let second = bridge
            .create_reader(handle, bridge.schema(handle).unwrap())
            .unwrap();
        bridge.close_source(handle).unwrap();

        assert!(bridge.push_limit(first, 5).unwrap());
        let first_plan = bridge.plan(first).unwrap();
        let second_plan = bridge.plan(second).unwrap();
        assert_eq!(decode::<Factory>(&first_plan.state).unwrap().0, 5);
        assert_eq!(decode::<Factory>(&second_plan.state).unwrap().0, 42);
        assert_eq!(builds.load(Ordering::SeqCst), 2);
        assert_eq!(limits.load(Ordering::SeqCst), 1);
        bridge.close_reader(first).unwrap();
        bridge.close_reader(second).unwrap();
    }

    #[test]
    fn executor_retries_use_only_serialized_values_after_the_driver_is_dropped() {
        let bridge = Bridge::<Source>::default();
        let source = Source::new(info()).unwrap();
        let builds = source.builds.clone();
        let source_drops = source.drops.clone();
        let builder_drops = source.builder_drops.clone();
        let (handle, _) = bridge.register_source(source).unwrap();
        let reader = bridge.create_reader(handle, id_schema()).unwrap();
        bridge.close_source(handle).unwrap();
        // Model Spark copying the factory and partition descriptors out of the plugin.
        let EncodedPlan { state, partitions } = bridge.plan(reader).unwrap();
        bridge.close_reader(reader).unwrap();
        drop(bridge);
        assert_eq!(builds.load(Ordering::SeqCst), 1);
        assert_eq!(source_drops.load(Ordering::SeqCst), 1);
        assert_eq!(builder_drops.load(Ordering::SeqCst), 1);

        for attempt in 0..2 {
            for (index, partition) in partitions.iter().enumerate() {
                let task = task(index as i32, attempt);
                // The executor entry point depends only on Factory, not Source or Builder.
                let stream =
                    unsafe { open_reader::<Factory>(&task, id_schema(), &state, partition) }
                        .unwrap();
                let mut reader = ArrowArrayStreamReader::try_new(stream).unwrap();
                let batch = reader.next().unwrap().unwrap();
                let ids = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                assert_eq!(ids.values(), &[42 + [2, 7][index]]);
                assert!(reader.next().is_none());
            }
        }
    }

    #[test]
    fn executor_rejects_batches_that_do_not_have_the_schema_to_read() {
        let bridge = Bridge::<Source>::default();
        let (handle, _) = bridge.create(info()).unwrap();
        let reader = bridge
            .create_reader(handle, bridge.schema(handle).unwrap())
            .unwrap();
        let plan = bridge.plan(reader).unwrap();
        bridge.close_reader(reader).unwrap();
        bridge.close_source(handle).unwrap();
        drop(bridge);

        // The factory returns a bigint id column, but Spark reads a string id or a label.
        for (schema, expected) in [
            (DataType::Utf8, "does not match the schema to read"),
            (DataType::Int64, "without the column label"),
        ] {
            let name = if schema == DataType::Utf8 {
                "id"
            } else {
                "label"
            };
            let schema = Arc::new(Schema::new(vec![Field::new(name, schema, false)]));
            let task = task(0, 0);
            let mut stream =
                unsafe { open_reader::<Factory>(&task, schema, &plan.state, &plan.partitions[0]) }
                    .unwrap();
            // get_schema reads the first batch, which does not have the schema to read.
            let mut schema = arrow_schema::ffi::FFI_ArrowSchema::empty();
            assert_ne!(
                unsafe { stream.get_schema.unwrap()(&mut stream, &mut schema) },
                0
            );
            let error =
                unsafe { std::ffi::CStr::from_ptr(stream.get_last_error.unwrap()(&mut stream)) };
            let error = error.to_str().unwrap();
            assert!(error.contains(expected), "{error}");
        }
    }

    #[test]
    fn capabilities_keep_only_implemented_operations() {
        struct Narrowing(Capabilities);

        impl DataSource for Narrowing {
            type ScanBuilder = Builder;
            type MicroBatchStream = Unsupported;
            type BatchWrite = Unsupported;
            type StreamingWrite = Unsupported;

            fn new(_: CreateInfo) -> Result<Self> {
                unreachable!("registered directly")
            }

            fn capabilities(&self) -> Capabilities {
                self.0
            }
        }

        let bridge = Bridge::<Narrowing>::default();
        for (declared, expected) in [
            (Capabilities::all(), abi::BATCH_READ),
            (Capabilities::empty(), 0),
            (Capabilities::all().without(Capabilities::BATCH_READ), 0),
        ] {
            let (handle, mask) = bridge.register_source(Narrowing(declared)).unwrap();
            assert_eq!(mask, expected, "{declared:?}");
            bridge.close_source(handle).unwrap();
        }
    }

    #[test]
    fn error_messages_include_their_sources_once() {
        #[derive(Debug)]
        struct Context(std::io::Error);

        impl std::fmt::Display for Context {
            fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("cannot open the table")
            }
        }

        impl std::error::Error for Context {
            fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
                Some(&self.0)
            }
        }

        let cause = std::io::Error::new(std::io::ErrorKind::NotFound, "no such file");
        let error: crate::Error = Box::new(Context(cause));
        assert_eq!(describe(&*error), "cannot open the table: no such file");
        let error = provider_call::<()>(|| Err(Box::new(Context(std::io::Error::other("io")))));
        assert_eq!(error.unwrap_err(), "cannot open the table: io");
        // A message that already contains its source is not repeated.
        let arrow = arrow_schema::ArrowError::ExternalError(Box::new(std::io::Error::other("io")));
        assert_eq!(describe(&arrow), arrow.to_string());
    }

    #[test]
    fn rejects_a_different_wire_version() {
        let other = encode_envelope(STATE_VERSION + 1, &42_u64).unwrap();
        let error = decode::<u64>(&other).unwrap_err();
        assert!(error
            .to_string()
            .contains("unsupported native state version"));
        assert_eq!(decode::<u64>(&encode(&42_u64).unwrap()).unwrap(), 42);
    }

    #[test]
    fn state_keeps_floats_and_bytes_and_supports_self_describing_serde_attributes() {
        #[derive(Serialize, Deserialize, Debug, PartialEq)]
        #[serde(untagged)]
        enum Split {
            Rows { first: i64 },
            Files { paths: Vec<String> },
        }

        #[derive(Serialize, Deserialize, Debug, PartialEq)]
        struct Statistics {
            min: f64,
            #[serde(default, skip_serializing_if = "Option::is_none")]
            max: Option<f64>,
        }

        #[derive(Serialize, Deserialize, Debug, PartialEq)]
        struct Partition {
            footer: Vec<u8>,
            split: Split,
            #[serde(flatten)]
            statistics: Statistics,
        }

        let partition = Partition {
            footer: (0..=255).collect(),
            split: Split::Files {
                paths: vec!["a".into()],
            },
            statistics: Statistics {
                min: f64::NEG_INFINITY,
                max: Some(f64::NAN),
            },
        };
        let bytes = encode(&partition).unwrap();
        // The 256 bytes are a bin16 value of 256 bytes, not a sequence of 256 integers, half
        // of which would take two bytes each.
        assert!(bytes.windows(3).any(|marker| marker == [0xc5, 0x01, 0x00]));
        assert!(bytes.len() < 256 + 128, "{}", bytes.len());
        let decoded: Partition = decode(&bytes).unwrap();
        assert_eq!(decoded.footer, partition.footer);
        assert_eq!(decoded.split, partition.split);
        assert_eq!(decoded.statistics.min, f64::NEG_INFINITY);
        assert!(decoded.statistics.max.unwrap().is_nan());
    }
}
