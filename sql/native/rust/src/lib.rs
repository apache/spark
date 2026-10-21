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

//! Experimental batch data source SDK for the C ABI proposed in Spark PR #59424.
//!
//! Implement [`DataSource`], naming the types of the operations that it implements: a
//! [`ScanBuilder`] for batch reads, a [`MicroBatchStream`] for micro-batch streaming reads, a
//! [`BatchWrite`] for batch writes, and a [`StreamingWrite`] for streaming writes, or
//! [`Unsupported`] for an operation that it does not implement. Then invoke
//! [`export_data_source!`] once in a `cdylib` crate. The SDK
//! owns handles, plan/message serialization, C ABI adaptation, and Arrow import/export.
//! Rust traits stay inside the plugin; they are not an ABI between dynamic libraries.
//!
//! Methods receive their inputs in `#[non_exhaustive]` structs, so that new ABI fields
//! can reach connectors without breaking them: `*Info` structs on the driver
//! ([`CreateInfo`], [`ScanInfo`], [`MicroBatchStreamInfo`], [`WriteInfo`],
//! [`StreamingWriteInfo`], and [`HostInfo`]), and `*Context` structs, which carry the live
//! [`TaskContext`], on the executors ([`ReaderContext`] and [`WriterContext`]).

use std::collections::BTreeMap;

use arrow_array::RecordBatch;
use arrow_schema::{ArrowError, SchemaRef};
use serde::de::DeserializeOwned;
use serde::Serialize;

mod abi;
mod bridge;
mod capabilities;
mod export;
mod host;
mod predicate;
mod task;
mod unsupported;
mod write;

/// Implementation details of [`export_data_source!`] and of the SDK's own tests, not an API.
#[doc(hidden)]
pub mod __private {
    pub mod abi {
        pub use crate::abi::*;
    }

    pub mod bridge {
        pub use crate::bridge::{open_reader, parse_predicates, Bridge};
    }

    pub mod write {
        pub use crate::write::*;
    }
}

pub use capabilities::Capabilities;
pub use host::HostInfo;
/// The `log` crate, whose macros write to the log of the Spark driver or executor, with
/// the path of the library, once Spark initializes the library.
///
/// Any thread can call them, including in the dependencies of the library. Outside
/// Spark, for example in unit tests, their messages are dropped. By default, the SDK
/// passes messages of the `Info` and more severe levels; to pass more, call
/// `log::set_max_level`, for example in [`DataSource::init`].
pub use log;
pub use predicate::{LiteralValue, Predicate};
pub use task::TaskContext;
pub use unsupported::Unsupported;

pub type Error = Box<dyn std::error::Error + Send + Sync>;
pub type Result<T> = std::result::Result<T, Error>;
pub type Options = BTreeMap<String, String>;

// Data sources: Spark creates a DataSource for each operation, and its associated types
// name the operations that the source implements.

/// What Spark passes to create a data source.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct CreateInfo {
    /// The name of the data source, as listed in the manifest of its package, or in
    /// lower case for an installed library. A library that implements several data
    /// sources, installed or listed under each of their names, can choose by this name.
    pub name: String,
    /// The options, with keys in lower case. A capability probe uses the options of the
    /// table; an operation may override them with the options of its query.
    pub options: Options,
}

impl CreateInfo {
    /// The information of a source created outside Spark, for example in a test.
    pub fn new(name: impl Into<String>, options: Options) -> Self {
        Self {
            name: name.into(),
            options,
        }
    }
}

/// Driver-side source configuration, schema discovery, and the operations of a source.
///
/// The associated types name the operations that the source implements, and
/// [`Unsupported`] stands for one that it does not implement:
///
/// ```rust,ignore
/// impl DataSource for RangeSource {
///     type ScanBuilder = RangeScan;
///     type MicroBatchStream = RangeStream;
///     type BatchWrite = Unsupported;
///     type StreamingWrite = Unsupported;
///     ...
/// }
/// ```
///
/// The SDK installs native callbacks and reports capabilities only for the implemented
/// operations, and calls the `new_*` method of an operation only for them.
///
/// Spark creates a source for each schema inference, scan, stream, and write, including
/// the write of each micro-batch, and may create one only to inspect its capabilities,
/// then drop it. Keep construction lightweight. Scan builders, streams, and writes cannot
/// borrow their source, which Spark drops first.
pub trait DataSource: Send + Sized + 'static {
    fn new(info: CreateInfo) -> Result<Self>;

    /// Called once in each JVM when Spark loads the library, before it creates any
    /// source. On failure, Spark does not use the library, and the error is logged.
    fn init(_host: &HostInfo) -> Result<()> {
        Ok(())
    }

    // Schema discovery.

    /// Whether [`schema`](Self::schema) infers the schema of the data. When false, the
    /// SDK leaves the native function unset: Spark then requires a user-specified
    /// schema, reports its standard error without one, and never calls `schema`.
    const INFERS_SCHEMA: bool = true;

    /// Returns the schema of the data, when the user does not specify one.
    fn schema(&self) -> Result<SchemaRef> {
        Err(
            "DataSource::schema is not implemented: implement it, or set \
             DataSource::INFERS_SCHEMA to false to require a user-specified schema"
                .into(),
        )
    }

    // Batch reads.

    /// The scan builder of batch reads, which [`new_scan`](Self::new_scan) creates, or
    /// [`Unsupported`].
    type ScanBuilder: ScanBuilder;

    /// Creates an independent scan of [`ScanInfo::schema`], which the user may have
    /// specified; reject unsupported schemas.
    fn new_scan(&self, _info: ScanInfo) -> Result<Self::ScanBuilder> {
        Err(
            "DataSource::new_scan is not implemented: implement it, or set \
             DataSource::ScanBuilder to Unsupported"
                .into(),
        )
    }

    // Micro-batch streaming reads.

    /// The stream of micro-batch streaming reads, which
    /// [`new_micro_batch_stream`](Self::new_micro_batch_stream) creates, or [`Unsupported`].
    type MicroBatchStream: MicroBatchStream;

    /// Creates the stream of a streaming query that reads [`MicroBatchStreamInfo::schema`],
    /// which the user may have specified; reject unsupported schemas. Spark pushes nothing
    /// down into a streaming scan, so its readers return all the columns of the schema.
    fn new_micro_batch_stream(
        &self,
        _info: MicroBatchStreamInfo,
    ) -> Result<Self::MicroBatchStream> {
        Err(
            "DataSource::new_micro_batch_stream is not implemented: implement it, or set \
             DataSource::MicroBatchStream to Unsupported"
                .into(),
        )
    }

    // Batch writes.

    /// The coordinator of batch writes, which [`new_write`](Self::new_write) creates, or
    /// [`Unsupported`].
    type BatchWrite: BatchWrite;

    /// Validate the input schema and create an independent write. Construction may be
    /// followed only by Drop, without commit or abort, so it must not make irreversible
    /// changes. The SDK calls this only while the instance's capabilities include
    /// `BATCH_WRITE`, and passes [`WriteMode::Truncate`] only when they include `TRUNCATE`
    /// and the write supports it.
    fn new_write(&self, _info: WriteInfo) -> Result<Self::BatchWrite> {
        Err(
            "DataSource::new_write is not implemented: implement it, or set \
             DataSource::BatchWrite to Unsupported"
                .into(),
        )
    }

    // Streaming writes.

    /// The coordinator of the write of each micro-batch of a streaming query, which
    /// [`new_streaming_write`](Self::new_streaming_write) creates, or [`Unsupported`].
    type StreamingWrite: StreamingWrite;

    /// Validate the input schema and create the write of a micro-batch. As with
    /// [`new_write`](Self::new_write), construction may be followed only by Drop, so it must
    /// not make irreversible changes. The SDK calls this only while the instance's
    /// capabilities include `STREAMING_WRITE`, and passes [`WriteMode::Truncate`] only when
    /// they include `TRUNCATE` and the write supports it.
    fn new_streaming_write(&self, _info: StreamingWriteInfo) -> Result<Self::StreamingWrite> {
        Err(
            "DataSource::new_streaming_write is not implemented: implement it, or set \
             DataSource::StreamingWrite to Unsupported"
                .into(),
        )
    }

    // Instance restrictions.

    /// Returns the operations of this instance. The SDK keeps only those that the source
    /// implements, so the default, [`Capabilities::all`], keeps them all. Override this to
    /// restrict an instance, for example for a read-only destination. The SDK evaluates
    /// it once when Spark creates the source. An instance without operations still
    /// supports schema discovery.
    fn capabilities(&self) -> Capabilities {
        Capabilities::all()
    }
}

// Batch reads: a ScanBuilder negotiates pushdown and plans the scan on the driver, and a
// PartitionReaderFactory opens a reader of each partition on the executors.

/// What Spark passes to create a scan.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct ScanInfo {
    /// The schema to read: the schema of the source, or one that the user specified.
    pub schema: SchemaRef,
}

impl ScanInfo {
    /// The information of a scan created outside Spark, for example in a test.
    pub fn new(schema: SchemaRef) -> Self {
        Self { schema }
    }
}

/// Negotiates pushdown on the driver, then plans the scan.
///
/// The SDK calls [`plan`](Self::plan) once, when Spark first asks for the partitions,
/// and caches the result, including a failure. Only the serialized factory and
/// partitions reach the executors.
pub trait ScanBuilder: Send + Sized + 'static {
    type Factory: PartitionReaderFactory;

    /// False only for [`Unsupported`], from which the SDK derives the capabilities.
    #[doc(hidden)]
    const SUPPORTED: bool = true;

    /// Return one result per input predicate in order. Unknown functions and
    /// types must not be reported as exact. Spark currently rechecks all filters.
    /// A predicate that the SDK cannot parse arrives as [`Predicate::Unknown`], and
    /// the SDK reports it as unsupported whatever this method returns for it.
    fn push_filters(&mut self, filters: &[Predicate]) -> Result<Vec<FilterSupport>> {
        Ok(vec![FilterSupport::Unsupported; filters.len()])
    }

    /// Count rows after filtering. Spark currently pushes limits only for
    /// scans with no filters; the SDK also declines a limit when any predicate
    /// it received remains residual.
    fn push_limit(&mut self, _limit: usize) -> Result<LimitSupport> {
        Ok(LimitSupport::Unsupported)
    }

    /// On true, output exactly these top-level columns in this order. On false,
    /// keep the original schema. Empty projection must preserve row counts.
    /// Retain internally needed filter columns without returning them. Readers get
    /// the resulting schema in [`ReaderContext::schema`].
    fn prune_columns(&mut self, _columns: &[String]) -> Result<bool> {
        Ok(false)
    }

    fn plan(self) -> Result<ScanPlan<Self::Factory>>;
}

/// How completely a connector evaluates a pushed predicate.
/// Spark currently rechecks all predicates, including exact ones.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum FilterSupport {
    /// Implements Spark SQL semantics and returns only matching rows.
    Exact,
    /// May skip data that cannot match, but may also return nonmatching rows.
    PruningOnly,
    /// Does not evaluate this predicate.
    Unsupported,
}

/// Whether a connector limits the rows produced by the whole scan.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum LimitSupport {
    /// The scan may return more rows than the limit. A connector may still cap the
    /// rows of each partition on its own: Spark applies its limit after reading.
    Unsupported,
    /// The whole scan returns at most the limit. Spark still applies its limit.
    Global,
}

/// A driver-created plan containing only serializable values.
///
/// The SDK serializes the reader factory once and each partition independently.
/// Include snapshot identifiers when needed for repeatable task attempts. Do not
/// put pointers, open files, connections, or process-local resources here.
#[non_exhaustive]
pub struct ScanPlan<F: PartitionReaderFactory> {
    pub factory: F,
    pub partitions: Vec<F::Partition>,
}

impl<F: PartitionReaderFactory> ScanPlan<F> {
    pub fn new(factory: F, partitions: Vec<F::Partition>) -> Self {
        Self {
            factory,
            partitions,
        }
    }
}

/// Serializable configuration from which executors open partition readers.
///
/// The factory carries configuration or a snapshot descriptor; open connections and
/// other live resources in [`open`](Self::open). The SDK serializes the negotiated
/// schema with the factory, so the factory need not carry it.
pub trait PartitionReaderFactory: Serialize + DeserializeOwned + Send + 'static {
    type Partition: Serialize + DeserializeOwned + Send + 'static;

    /// Opens a lazy reader of the batches of `partition`, with the columns of
    /// [`ReaderContext::schema`].
    ///
    /// - Return batches lazily, with bounded memory.
    /// - The reader owns its resources and cannot borrow this short-lived factory. It
    ///   may keep `context` until Spark releases the stream.
    /// - Spark may call the reader on different threads, never concurrently, so it must
    ///   be `Send` but not necessarily `Sync`.
    /// - Check [`TaskContext::is_interrupted`] during long operations, and use timeouts
    ///   for blocking I/O.
    /// - Batches must own their buffers, because Spark may release the stream before
    ///   its batches.
    ///
    /// The SDK checks Spark-compatible Arrow types, the negotiated schema and its
    /// nullability, and zero array offsets recursively. It reports incompatible layouts
    /// instead of copying buffers. Batches may use any supported Arrow encoding of a
    /// Spark type, such as `LargeUtf8` for a string, but the first batch determines the
    /// Arrow schema of the stream, which every later batch must have.
    fn open<'task>(
        &self,
        partition: Self::Partition,
        context: ReaderContext<'task>,
    ) -> Result<PartitionReader<'task>>;
}

/// What an executor reader receives with its partition.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct ReaderContext<'task> {
    /// The negotiated schema: the schema to read, or its pruned columns in order.
    /// The reader returns batches with exactly these columns.
    pub schema: SchemaRef,
    /// The task that reads the partition.
    pub task: TaskContext<'task>,
}

impl<'task> ReaderContext<'task> {
    /// The context of a reader opened outside Spark, for example in a test.
    pub fn new(schema: SchemaRef, task: TaskContext<'task>) -> Self {
        Self { schema, task }
    }
}

/// The lazy batches of one partition, which [`PartitionReaderFactory::open`] returns.
/// Any Arrow `RecordBatchReader`, such as a Parquet reader, is such an iterator.
pub type PartitionReader<'task> =
    Box<dyn Iterator<Item = std::result::Result<RecordBatch, ArrowError>> + Send + 'task>;

// Micro-batch streaming reads: a MicroBatchStream finds new data and plans each micro-batch on
// the driver, and the PartitionReaderFactory of each plan reads its partitions on the executors,
// as in batch reads.

/// What Spark passes to create the stream of a streaming query.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct MicroBatchStreamInfo {
    /// The schema to read: the schema of the source, or one that the user specified.
    pub schema: SchemaRef,
}

impl MicroBatchStreamInfo {
    /// The information of a stream created outside Spark, for example in a test.
    pub fn new(schema: SchemaRef) -> Self {
        Self { schema }
    }
}

/// Finds new data and plans the micro-batches of a streaming query, on the driver.
///
/// Spark creates a stream when a streaming query starts, or restarts from its checkpoint,
/// and drops it when the query stops. For each micro-batch, it asks for the
/// [`latest_offset`](Self::latest_offset), and when that differs from the end of the
/// previous micro-batch, [`plan`](Self::plan)s the data in between. Only the serialized
/// factory and partitions of each plan reach the executors, which read them as those of a
/// batch scan.
///
/// # Offsets
///
/// Spark stores offsets in the checkpoint of the query, as the JSON of
/// [`Offset`](Self::Offset), and compares them by that JSON, so serialize a position to the
/// same JSON each time: for example, use structs and `BTreeMap` rather than `HashMap`. A
/// restarted query passes the offsets of its checkpoint, which an earlier version of the
/// library may have written, to `plan` and `commit`.
///
/// # Lifecycle
///
/// - Calls never run concurrently.
/// - A failed call, including a panic, stops the query, so the SDK returns that failure
///   from every later call.
/// - After a restart, Spark may plan the last range of the query again, so `plan` must
///   return partitions that read the same data for the same range.
pub trait MicroBatchStream: Send + 'static {
    /// A position in the data, such as the next record of each partition of a log.
    type Offset: Serialize + DeserializeOwned;

    /// The reader factory of the micro-batches, which can be that of batch scans.
    type Factory: PartitionReaderFactory;

    /// False only for [`Unsupported`], from which the SDK derives the capabilities.
    #[doc(hidden)]
    const SUPPORTED: bool = true;

    /// Returns the offset to start from when the query has no checkpoint: the first
    /// micro-batch reads the data after it.
    fn initial_offset(&mut self) -> Result<Self::Offset>;

    /// Returns the offset of the most recent data available.
    fn latest_offset(&mut self) -> Result<Self::Offset>;

    /// Plans the micro-batch that reads the data after `start`, up to and including `end`.
    fn plan(&mut self, start: Self::Offset, end: Self::Offset) -> Result<ScanPlan<Self::Factory>>;

    /// Tells the stream that Spark has processed the data up to and including `end` and
    /// will not read it again, so that the stream can release it.
    fn commit(&mut self, _end: Self::Offset) -> Result<()> {
        Ok(())
    }
}

// Batch writes: a BatchWrite coordinates the write on the driver, and a DataWriterFactory
// opens a DataWriter for each task attempt on the executors.

/// What Spark passes to create a batch write.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct WriteInfo {
    /// The schema of the data to write.
    pub schema: SchemaRef,
    /// How the write changes the destination.
    pub mode: WriteMode,
}

impl WriteInfo {
    /// The information of a write created outside Spark, for example in a test.
    pub fn new(schema: SchemaRef, mode: WriteMode) -> Self {
        Self { schema, mode }
    }
}

/// How a batch write changes the destination.
///
/// New modes may be added. The SDK passes a mode only when the source instance
/// has the matching capability, so a source never receives a mode it did not
/// opt in to; a wildcard arm can return an error.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum WriteMode {
    Append,
    /// Replace existing data at driver commit, never at writer creation.
    Truncate,
}

/// A driver coordinator for one batch write, which also creates the serializable
/// configuration of its task writers.
///
/// Keep irreversible publication, including removing overwritten data, in commit.
/// Drop releases resources; it must not publish data or perform fallible flushing.
///
/// # Lifecycle
///
/// The SDK enforces the following, so implementations need not track their own
/// phase or guard against repeated calls:
///
/// - `writer_factory` runs at most once, before any task opens a writer.
/// - `commit` runs at most once, only while no earlier call has failed, including
///   `writer_factory` and the serialization of its factory. It is skipped, and the
///   write fails, when any task message is missing or cannot be decoded.
/// - `abort` runs at most once, never after a successful `commit`. It may follow
///   a failed `commit`, or run without `commit`.
/// - Drop runs exactly once, possibly without `commit` or `abort`. Spark closes the
///   coordinator without `abort` when `writer_factory` or the serialization of its
///   factory fails, so Drop must also release what only this write staged.
/// - A panic in any method is reported as that method's failure.
///
/// Message `i` of `commit` and `abort` belongs to input partition `i`.
pub trait BatchWrite: Send + 'static {
    /// The configuration from which each task attempt opens its writer.
    type Factory: DataWriterFactory<Message = Self::Message>;

    /// The message with which a task attempt reports its staged output to the driver.
    type Message: Serialize + DeserializeOwned + Send + 'static;

    /// Whether the write supports [`WriteMode::Truncate`], which adds the `TRUNCATE`
    /// capability to batch writes.
    const SUPPORTS_TRUNCATE: bool = false;

    /// False only for [`Unsupported`], from which the SDK derives the capabilities.
    #[doc(hidden)]
    const SUPPORTED: bool = true;

    /// Returns the configuration of the task writers. The SDK serializes it once, with
    /// the schema of the input, and caches the result, including a failure. Include a
    /// unique operation ID when needed; Spark supplies no batch query ID.
    fn writer_factory(&self) -> Result<Self::Factory>;

    /// Publish only the winning attempt from each input partition. Task retries
    /// and speculative attempts can stage other data that must not be published.
    fn commit(&mut self, messages: Vec<Self::Message>) -> Result<()>;

    /// Abort after input, task, or driver commit failure. None stands for a partition
    /// whose commit message did not reach the driver, or cannot be decoded.
    /// Executor loss can leave orphaned staged data; plan for its cleanup.
    fn abort(&mut self, messages: Vec<Option<Self::Message>>) -> Result<()>;
}

/// Serializable configuration for independent task-attempt writers, which
/// [`BatchWrite::writer_factory`] or [`StreamingWrite::writer_factory`] creates.
///
/// Open connections and files in open, not in the factory. A returned writer
/// cannot borrow this temporary factory, but may retain the context until
/// close. Callbacks can move between threads sequentially; writers must be Send.
pub trait DataWriterFactory: Serialize + DeserializeOwned + Send + 'static {
    type Message: Serialize + DeserializeOwned + Send + 'static;

    fn open<'task>(
        &self,
        context: WriterContext<'task>,
    ) -> Result<PartitionWriter<'task, Self::Message>>;
}

/// What a task writer receives when it opens.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct WriterContext<'task> {
    /// The schema of the batches to write, as in [`WriteInfo::schema`].
    pub schema: SchemaRef,
    /// The task attempt that writes.
    pub task: TaskContext<'task>,
    /// The epoch of the micro-batch of a streaming write, or `None` for a batch write.
    pub epoch_id: Option<i64>,
}

impl<'task> WriterContext<'task> {
    /// The context of a batch writer opened outside Spark, for example in a test. Set
    /// [`epoch_id`](Self::epoch_id) for a streaming writer.
    pub fn new(schema: SchemaRef, task: TaskContext<'task>) -> Self {
        Self {
            schema,
            task,
            epoch_id: None,
        }
    }
}

/// One task attempt's writer. The SDK closes it after commit or abort.
///
/// # Lifecycle
///
/// The SDK enforces the following, so implementations need not track their own
/// phase or guard against repeated calls:
///
/// - `write` runs zero or more times, and never after any call has failed or
///   after `commit` or `abort`.
/// - `commit` runs at most once, never after a failed `write` or after `abort`.
/// - `abort` runs at most once, never after a `commit` whose message reached Spark.
///   It may follow a failed `write` or `commit`, including a `commit` that returned
///   a message that the SDK could not serialize, so it must handle a task attempt
///   that already finished its output.
/// - Drop runs exactly once when Spark closes the writer, possibly without
///   `commit` or `abort`.
/// - A panic in any method is reported as that method's failure.
pub trait DataWriter: Send {
    type Message;

    /// Takes ownership of an imported Arrow batch, which may be retained after
    /// this call. Its columns match [`WriterContext::schema`] by Spark type.
    /// Process batches incrementally and check task cancellation.
    fn write(&mut self, batch: RecordBatch) -> Result<()>;

    /// Finish staging and return a typed message for the driver. Do not make the
    /// whole write visible here. On failure (including message serialization),
    /// the SDK keeps this writer available for abort.
    fn commit(&mut self) -> Result<Self::Message>;

    fn abort(&mut self) -> Result<()>;
}

/// The writer of one task attempt, which [`DataWriterFactory::open`] returns.
pub type PartitionWriter<'task, M> = Box<dyn DataWriter<Message = M> + 'task>;

// Streaming writes: a StreamingWrite coordinates the write of one micro-batch on the driver,
// and its DataWriterFactory opens a DataWriter for each task attempt on the executors, as in
// batch writes.

/// What Spark passes to create the write of a micro-batch.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct StreamingWriteInfo {
    /// The schema of the data to write.
    pub schema: SchemaRef,
    /// How the micro-batch changes the destination: [`WriteMode::Truncate`] for the output
    /// mode "complete", which replaces the output of the previous micro-batches.
    pub mode: WriteMode,
    /// The ID of the streaming query, which stays the same when the query restarts from
    /// its checkpoint.
    pub query_id: String,
}

impl StreamingWriteInfo {
    /// The information of a write created outside Spark, for example in a test.
    pub fn new(schema: SchemaRef, mode: WriteMode, query_id: impl Into<String>) -> Self {
        Self {
            schema,
            mode,
            query_id: query_id.into(),
        }
    }
}

/// A driver coordinator for the write of one micro-batch, or epoch, of a streaming query,
/// which also creates the serializable configuration of its task writers.
///
/// Spark creates a write for each micro-batch. The SDK enforces the lifecycle of a
/// [`BatchWrite`], with the epoch ID of the micro-batch passed to `commit` and `abort`,
/// and to the task writers in [`WriterContext::epoch_id`].
///
/// # Exactly-once output
///
/// Spark may commit an epoch again, with a new write, for example when the query restarts
/// after a failure: committing an epoch that is already committed must have no effect. The
/// ID of the query, [`StreamingWriteInfo::query_id`], and the epoch ID identify the output
/// of a micro-batch, also when other queries write to the same destination.
pub trait StreamingWrite: Send + 'static {
    /// The configuration from which each task attempt opens its writer.
    type Factory: DataWriterFactory<Message = Self::Message>;

    /// The message with which a task attempt reports its staged output to the driver.
    type Message: Serialize + DeserializeOwned + Send + 'static;

    /// Whether the write supports [`WriteMode::Truncate`], for the output mode "complete",
    /// which adds the `TRUNCATE` capability.
    const SUPPORTS_TRUNCATE: bool = false;

    /// False only for [`Unsupported`], from which the SDK derives the capabilities.
    #[doc(hidden)]
    const SUPPORTED: bool = true;

    /// Returns the configuration of the task writers. The SDK serializes it once, with the
    /// schema of the input, and caches the result, including a failure.
    fn writer_factory(&self) -> Result<Self::Factory>;

    /// Publish the winning attempt of each input partition as the output of epoch
    /// `epoch_id`, unless that epoch is already committed.
    fn commit(&mut self, epoch_id: i64, messages: Vec<Self::Message>) -> Result<()>;

    /// Abort the micro-batch after input, task, or driver commit failure. None stands for
    /// a partition whose commit message did not reach the driver, or cannot be decoded.
    fn abort(&mut self, epoch_id: i64, messages: Vec<Option<Self::Message>>) -> Result<()>;
}
