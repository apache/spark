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

//! Experimental SDK for native data sources in Rust, over version 1 of the C ABI of Spark,
//! `spark_datasource.h`.
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
//! ([`CreateInfo`], [`ScanInfo`], [`MicroBatchStreamInfo`], [`LatestOffsetInfo`],
//! [`MicroBatchInfo`], [`CommitOffsetInfo`], [`WriteInfo`], [`CommitInfo`], [`AbortInfo`],
//! [`StreamingWriteInfo`], [`StreamingCommitInfo`], and [`StreamingAbortInfo`]), `*Context`
//! structs, which carry the live [`TaskContext`], on the executors ([`ReaderContext`] and
//! [`WriterContext`]), and [`HostInfo`], which [`DataSource::init`] receives in every JVM
//! that loads the library, executors included. The methods whose function of the header
//! passes nothing but the size of its info struct yet, [`ScanBuilder::plan`],
//! [`MicroBatchStream::initial_offset`], and the `commit` and `abort` of [`DataWriter`], take
//! no struct: a field added there would be a breaking change of the SDK.

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
mod task;
mod unsupported;
mod write;

/// Implementation details of [`export_data_source!`] and of the SDK's own tests, not an API.
#[doc(hidden)]
pub mod __private {
    pub use crate::bridge::Bridge;
    pub use crate::export::{library, Exported};

    pub mod abi {
        pub use crate::abi::*;
    }
}

/// The Arrow crate of [`RecordBatch`], which connectors can use to have the same version of
/// Arrow as the SDK.
pub use arrow_array;
/// The Arrow crate of [`SchemaRef`] and [`ArrowError`], which connectors can use to have the
/// same version of Arrow as the SDK.
pub use arrow_schema;
pub use capabilities::Capabilities;
pub use host::HostInfo;
/// The `log` crate, whose macros write to the log of the Spark driver or executor, with
/// the path of the library, once Spark initializes the library.
///
/// Any thread can call them, including in the dependencies of the library. Outside
/// Spark, for example in unit tests, their messages are dropped. The SDK sets the level of
/// the `log` crate to that of the log of Spark, such as `Debug` when Spark logs debug
/// messages, so the library does not set it.
pub use log;
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

    /// Whether [`infer_schema`](Self::infer_schema) infers the schema of the data. When
    /// false, the SDK leaves the native `schema` function unset: Spark then requires a
    /// user-specified schema, reports its standard error without one, and never calls
    /// `infer_schema`.
    const INFERS_SCHEMA: bool = true;

    /// Returns the schema of the data, when the user does not specify one.
    fn infer_schema(&self) -> Result<SchemaRef> {
        Err(
            "DataSource::infer_schema is not implemented: implement it, or set \
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

    /// Creates the stream of a streaming query that reads the columns of
    /// [`MicroBatchStreamInfo::schema`]; reject unsupported schemas.
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
    /// `BATCH_WRITE`, and passes [`WriteMode::Truncate`] only when they include
    /// `BATCH_TRUNCATE`, which the write declares with [`BatchWrite::SUPPORTS_TRUNCATE`].
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
    /// they include `STREAMING_TRUNCATE`, which the write declares with
    /// [`StreamingWrite::SUPPORTS_TRUNCATE`].
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
/// Spark calls [`push_limit`](Self::push_limit) and [`prune_columns`](Self::prune_columns),
/// each at most once and in this order, then [`plan`](Self::plan), and drops the builder.
/// After a call fails, it makes no other call. It drops a builder without planning it when it
/// does not read the scan, for example to only explain a query, once the scan is no longer
/// referenced, which may be late and on another thread, so a builder must not hold a resource
/// that other operations need exclusively, such as a lock, until Drop. Only the serialized
/// factory and partitions reach the executors.
pub trait ScanBuilder: Send + Sized + 'static {
    type Factory: PartitionReaderFactory;

    /// False only for [`Unsupported`], from which the SDK derives the capabilities.
    #[doc(hidden)]
    const SUPPORTED: bool = true;

    /// Pushes a LIMIT of `limit` rows down to the scan. Return [`LimitSupport::Global`] only
    /// if the whole scan then returns at most `limit` rows. Spark pushes a limit only when the
    /// query does not filter the rows, since it filters them after reading.
    fn push_limit(&mut self, _limit: usize) -> Result<LimitSupport> {
        Ok(LimitSupport::Unsupported)
    }

    /// Tells the scan the top-level columns that the query needs, in order, so that it can
    /// plan to read only them. There may be none, for example for `SELECT count(*)`: the
    /// batches then only have a number of rows. Readers get these columns in
    /// [`ReaderContext::schema`] whether or not the scan implements this method.
    fn prune_columns(&mut self, _columns: &[String]) -> Result<()> {
        Ok(())
    }

    fn plan(self) -> Result<ScanPlan<Self::Factory>>;
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
/// other live resources in [`open`](Self::open). Spark passes the schema to read with
/// each partition, so the factory need not carry it.
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
    /// - Spark may open the same partition at the same time in several attempts of its
    ///   task, so a reader must not take a resource that another attempt needs exclusively.
    ///
    /// The SDK checks Spark-compatible Arrow types, the schema to read and its
    /// nullability, the precision of decimal values, and zero array offsets recursively. It
    /// reports incompatible layouts instead of copying buffers. After a batch fails these
    /// checks, or the reader returns an error, the stream fails every later call with the
    /// same error. A batch may have more columns than the schema to read,
    /// such as all the columns of the source: the SDK keeps those of the schema, by name.
    /// Batches may use any supported Arrow encoding of a Spark type, such as `LargeUtf8`
    /// for a string, but the first batch determines the Arrow schema of the stream, which
    /// every later batch must have.
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
    /// The schema to read: the columns that the scan reads, in order, which are those of
    /// [`ScanBuilder::prune_columns`] for a batch scan, and of
    /// [`MicroBatchStreamInfo::schema`] for a micro-batch.
    pub schema: SchemaRef,
    /// The index of this partition in the scan or micro-batch's [`ScanPlan::partitions`].
    /// This is not the Spark task's partition ID: one task may read several partitions,
    /// for example after a coalesce or union. The index is unique only within that plan.
    pub partition_index: i32,
    /// The task that reads the partition.
    pub task: TaskContext<'task>,
}

impl<'task> ReaderContext<'task> {
    /// The context of a reader opened outside Spark, for example in a test.
    pub fn new(schema: SchemaRef, partition_index: i32, task: TaskContext<'task>) -> Self {
        Self {
            schema,
            partition_index,
            task,
        }
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
    /// The columns to read: those of the schema of the source, or of one that the user
    /// specified, or only the top-level columns of it that the query needs. The readers of
    /// the micro-batches get it in [`ReaderContext::schema`].
    pub schema: SchemaRef,
    /// A directory in the checkpoint of the query that is reserved for this source, such as
    /// `file:/tmp/checkpoint/sources/0`, where the stream may keep what it needs to recover
    /// from a failure. It stays the same when the query restarts. It is a fully qualified
    /// Hadoop path, whose characters are not escaped as in a URI, on a file system that the
    /// library may not be able to access, such as `s3a`. Spark keeps what it needs there under
    /// the names that start with `_spark`, which the library must not use.
    pub checkpoint_location: String,
}

impl MicroBatchStreamInfo {
    /// The information of a stream created outside Spark, for example in a test.
    pub fn new(schema: SchemaRef, checkpoint_location: impl Into<String>) -> Self {
        Self {
            schema,
            checkpoint_location: checkpoint_location.into(),
        }
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
/// An offset is a progress boundary whose representation and mapping to records the connector
/// defines. A micro-batch reads the data between its starting and ending boundaries. For example,
/// when an offset stores the next row to read, `{"next":10}` to `{"next":13}` reads rows 10, 11,
/// and 12: the row range is `[10, 13)`. Equal boundaries describe no data, and adjacent ranges
/// must neither repeat nor skip records.
///
/// Spark stores offsets in the checkpoint of the query, as the JSON of
/// [`Offset`](Self::Offset), and compares them by that JSON, so serialize a position to the
/// same JSON each time: for example, use structs and `BTreeMap` rather than `HashMap`. The
/// SDK stores exactly that JSON, and later versions will not wrap or re-encode it: the format
/// of an offset is that of the connector's type, which outlives any build of the library. A
/// restarted query passes the offsets of its checkpoint, which an earlier version of the
/// library may have written, to `latest_offset`, `plan`, and `commit`. When that version had a
/// different `Offset` type, the same position may encode to different JSON, so Spark runs one
/// micro-batch without rows.
///
/// # Admission control
///
/// [`default_read_limit`](Self::default_read_limit) limits the data of each micro-batch,
/// typically with an option of the source. Spark passes the limit to `latest_offset`, except
/// when a micro-batch reads all the available data, as with `Trigger.Once`. With
/// `Trigger.AvailableNow`, a run reads the data that was available when it started: with a
/// limit, in several micro-batches, whose latest offsets must not go beyond
/// [`LatestOffsetInfo::run_end`].
///
/// # Lifecycle
///
/// - Calls never run concurrently.
/// - After a call fails, including with a panic, Spark makes no other call: it stops the
///   query, and drops the stream.
/// - After a restart, Spark may plan the last range of the query again, so `plan` must
///   return partitions that read the same data for the same range.
pub trait MicroBatchStream: Send + 'static {
    /// A progress boundary in the data, such as the next record of each partition of a log.
    type Offset: Serialize + DeserializeOwned;

    /// The reader factory of the micro-batches, which can be that of batch scans.
    type Factory: PartitionReaderFactory;

    /// False only for [`Unsupported`], from which the SDK derives the capabilities.
    #[doc(hidden)]
    const SUPPORTED: bool = true;

    /// Returns the starting progress boundary when the query has no checkpoint. Spark keeps
    /// it in the checkpoint, and asks only when the checkpoint does not have it yet, so a query
    /// that restarts before its first micro-batch is committed starts from the same offset.
    fn initial_offset(&mut self) -> Result<Self::Offset>;

    /// Returns the limit on the data of each micro-batch. Spark asks once, when the query
    /// starts. The default, [`ReadLimit::AllAvailable`], reads all the available data in
    /// each micro-batch.
    fn default_read_limit(&self) -> Result<ReadLimit> {
        Ok(ReadLimit::AllAvailable)
    }

    /// Returns the end of the next micro-batch: the offset of the most recent data after
    /// [`LatestOffsetInfo::start`], within its limit, and not beyond its end of the run.
    /// Return `start` when there is no new data. When there is, return an offset after
    /// `start`, even if the next unit of data, such as a file, is larger than the limit:
    /// Spark sees no new data otherwise, and never reads further.
    ///
    /// With a limit, Spark also asks without a limit for some micro-batches, to report the
    /// progress of the query, so do not count the calls as micro-batches, or reserve data in
    /// them.
    fn latest_offset(&mut self, info: LatestOffsetInfo<Self::Offset>) -> Result<Self::Offset>;

    /// Plans the micro-batch between the progress boundaries [`MicroBatchInfo::start`] and
    /// [`MicroBatchInfo::end`], using the connector's mapping from offsets to records.
    fn plan(&mut self, info: MicroBatchInfo<Self::Offset>) -> Result<ScanPlan<Self::Factory>>;

    /// Tells the stream that Spark has processed the data represented by the progress boundary
    /// [`CommitOffsetInfo::end`] and will not read that data again, so the stream can release it.
    fn commit(&mut self, _info: CommitOffsetInfo<Self::Offset>) -> Result<()> {
        Ok(())
    }
}

/// A limit on the data of a micro-batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ReadLimit {
    /// All the available data.
    AllAvailable,
    /// A maximum number of rows, from 1 through [`i64::MAX`]. The SDK rejects values outside
    /// this range at the native ABI boundary. A stream may exceed this limit when its next
    /// unit of data is larger, as described by [`MicroBatchStream::latest_offset`].
    MaxRows(u64),
}

/// What Spark passes to ask a stream for its latest offset.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct LatestOffsetInfo<O> {
    /// The starting progress boundary: the end of the previous micro-batch, or the initial
    /// offset.
    pub start: O,
    /// The limit on the data after `start`: that of
    /// [`MicroBatchStream::default_read_limit`], or [`ReadLimit::AllAvailable`] when Spark
    /// reads all the available data, such as for the progress of the query or with
    /// `Trigger.Once`.
    pub limit: ReadLimit,
    /// With `Trigger.AvailableNow` and a limit, the end of the run: the latest offset must not
    /// go beyond it, so that the run reads the data that was available when it started. It is
    /// not the end of the next micro-batch. `None` otherwise.
    pub run_end: Option<O>,
}

impl<O> LatestOffsetInfo<O> {
    /// A request without a limit, made outside Spark, for example in a test. Set
    /// [`limit`](Self::limit) and [`run_end`](Self::run_end) for other requests.
    pub fn new(start: O) -> Self {
        Self {
            start,
            limit: ReadLimit::AllAvailable,
            run_end: None,
        }
    }
}

/// What Spark passes to plan a micro-batch: the offsets that delimit its data.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct MicroBatchInfo<O> {
    /// The starting progress boundary: the end of the previous micro-batch, or the initial
    /// offset. See [`MicroBatchStream`]'s offset contract for how boundaries map to records.
    pub start: O,
    /// The ending progress boundary, which [`MicroBatchStream::latest_offset`] returned.
    pub end: O,
}

impl<O> MicroBatchInfo<O> {
    /// The information of a micro-batch planned outside Spark, for example in a test.
    pub fn new(start: O, end: O) -> Self {
        Self { start, end }
    }
}

/// What Spark passes to commit the data of a stream.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct CommitOffsetInfo<O> {
    /// The progress boundary of the processed data, which Spark will not read again.
    pub end: O,
}

impl<O> CommitOffsetInfo<O> {
    /// The information of a commit made outside Spark, for example in a test.
    pub fn new(end: O) -> Self {
        Self { end }
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

/// How a write, or the write of a micro-batch, changes the destination.
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
/// Spark guarantees the following callback order:
///
/// - `new_writer_factory` runs once, right after [`DataSource::new_write`], before any task
///   opens a writer. When it fails, the SDK cannot serialize its factory, or Spark cannot
///   copy the serialized factory, for example when it is too large, the write fails and
///   the coordinator is dropped without `abort`, so Drop must also release what only this
///   write staged.
/// - `commit` runs at most once, when every task succeeded. The SDK fails the write
///   without calling it when a task message cannot be decoded.
/// - `abort` runs at most once, never after a successful `commit`. It follows a failed
///   `commit`, or a failed task, also when computing the data to write failed.
/// - Drop runs exactly once, otherwise when Spark closes the writer: right after a
///   successful `commit` or after `abort`, or, for a write that Spark neither commits nor
///   aborts, possibly late and on another thread.
/// - A panic in any method is reported as that method's failure.
///
/// These guarantees do not determine the outcome of a remote transaction. A destination
/// may publish the write even when `commit` returns an error, for example if its
/// acknowledgement is lost. Use a durable operation identity to check the destination's
/// outcome and make commit and cleanup idempotent. `abort` and Drop must preserve published
/// output, including output whose commit was not acknowledged. If the outcome cannot be
/// resolved, leave potentially published data intact for later recovery.
pub trait BatchWrite: Send + 'static {
    /// The configuration from which each task attempt opens its writer.
    type Factory: DataWriterFactory<Message = Self::Message>;

    /// The message with which a task attempt reports its staged output to the driver.
    type Message: Serialize + DeserializeOwned + Send + 'static;

    /// Whether the write supports [`WriteMode::Truncate`], for the save mode "overwrite",
    /// which adds the `BATCH_TRUNCATE` capability.
    const SUPPORTS_TRUNCATE: bool = false;

    /// False only for [`Unsupported`], from which the SDK derives the capabilities.
    #[doc(hidden)]
    const SUPPORTED: bool = true;

    /// Returns the configuration of the task writers, which the SDK serializes for them.
    /// Include a unique operation ID when needed; Spark supplies no batch query ID.
    fn new_writer_factory(&self) -> Result<Self::Factory>;

    /// Publish only the winning attempt from each input partition, whose messages
    /// [`CommitInfo::messages`] has. Task retries and speculative attempts can stage other
    /// data that must not be published.
    fn commit(&mut self, info: CommitInfo<Self::Message>) -> Result<()>;

    /// Abort after input, task, or driver commit failure, with the messages that reached the
    /// driver in [`AbortInfo::messages`]. Executor loss can leave orphaned staged data; plan
    /// for its cleanup.
    fn abort(&mut self, info: AbortInfo<Self::Message>) -> Result<()>;
}

/// What Spark passes to commit a batch write.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct CommitInfo<M> {
    /// The message of the winning task attempt of each input partition, at the position of
    /// its partition.
    pub messages: Vec<M>,
}

impl<M> CommitInfo<M> {
    /// The information of a commit made outside Spark, for example in a test.
    pub fn new(messages: Vec<M>) -> Self {
        Self { messages }
    }
}

/// What Spark passes to abort a batch write.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct AbortInfo<M> {
    /// The message of each input partition, at the position of its partition, or `None` for a
    /// partition whose commit message did not reach the driver, or cannot be decoded.
    pub messages: Vec<Option<M>>,
}

impl<M> AbortInfo<M> {
    /// The information of an abort made outside Spark, for example in a test.
    pub fn new(messages: Vec<Option<M>>) -> Self {
        Self { messages }
    }
}

/// Serializable configuration for independent task-attempt writers, which
/// [`BatchWrite::new_writer_factory`] or [`StreamingWrite::new_writer_factory`] creates.
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
    /// The Spark task's partition ID. Its winning commit message has this position in
    /// [`CommitInfo::messages`] or [`StreamingCommitInfo::messages`].
    pub partition_id: i32,
    /// The task attempt that writes.
    pub task: TaskContext<'task>,
    /// The epoch of the micro-batch of a streaming write, or `None` for a batch write.
    pub epoch_id: Option<i64>,
}

impl<'task> WriterContext<'task> {
    /// The context of a batch writer opened outside Spark, for example in a test. Set
    /// [`epoch_id`](Self::epoch_id) for a streaming writer.
    pub fn new(schema: SchemaRef, partition_id: i32, task: TaskContext<'task>) -> Self {
        Self {
            schema,
            partition_id,
            task,
            epoch_id: None,
        }
    }
}

/// One task attempt's writer. Spark drops it after commit or abort.
///
/// # Lifecycle
///
/// Spark guarantees the following callback order:
///
/// - `write` runs zero or more times, before `commit` or `abort`, and never after a
///   call has failed.
/// - `commit` runs at most once, never after a failed `write` or after `abort`.
/// - `abort` runs at most once, never after a `commit` whose message reached Spark.
///   It may follow a failed `write` or `commit`, including a `commit` that returned
///   a message that the SDK could not serialize, so it must handle a task attempt
///   that already finished its output.
/// - Drop runs exactly once when Spark closes the writer, possibly without
///   `commit` or `abort`.
/// - The writer of a task attempt that Spark kills, such as a speculative one, may still
///   write, abort, and drop after the driver committed or aborted the write.
/// - A panic in any method is reported as that method's failure.
pub trait DataWriter: Send {
    type Message;

    /// Takes ownership of an imported Arrow batch, which may be retained after this call,
    /// but not after the writer is dropped: its buffers belong to the data writer of Spark,
    /// and the ABI requires the library to release them before Spark closes the writer, so do
    /// not pass the batch to a thread or a value that outlives the writer. Spark cannot free a
    /// batch that is still retained when it closes the writer: it logs an error, and the memory
    /// stays allocated until the batch is dropped. It has the schema of
    /// [`WriterContext::schema`], and at most `spark.sql.execution.arrow.maxRecordsPerBatch`
    /// rows. When Spark writes the batches of a native scan without converting them to rows,
    /// it copies their values as the scan returned them, without the checks of a conversion,
    /// such as the precision of a decimal. The SDK validates each batch before this call, and
    /// fails the write of one that Arrow rejects, such as a Spark string that is not UTF-8.
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
/// Spark creates a write for each micro-batch, with the lifecycle of a [`BatchWrite`], and
/// passes the epoch ID of the micro-batch to `commit` and `abort`, in [`StreamingCommitInfo`]
/// and [`StreamingAbortInfo`], and to the task writers in [`WriterContext::epoch_id`]. Spark
/// closes a write that it committed or aborted before it creates the write of the next
/// micro-batch, so the write is dropped by then.
///
/// # Exactly-once output
///
/// Spark may commit an epoch again, with a new write, for example when the query restarts
/// after a failure: committing an epoch that is already committed must have no effect. A
/// restarted query may also abort an epoch that an earlier run committed, so `abort` must
/// only remove what the task writers of its own write wrote. The ID of the query,
/// [`StreamingWriteInfo::query_id`], and the epoch ID identify the output of a micro-batch,
/// also when other queries write to the same destination.
///
/// The remote transaction contract of [`BatchWrite`] applies here too: `abort` and Drop
/// must preserve an epoch published by the current write if its commit acknowledgement
/// was lost. Record the query and epoch identity atomically with publication so a restarted
/// query can determine whether that epoch is already committed.
pub trait StreamingWrite: Send + 'static {
    /// The configuration from which each task attempt opens its writer.
    type Factory: DataWriterFactory<Message = Self::Message>;

    /// The message with which a task attempt reports its staged output to the driver.
    type Message: Serialize + DeserializeOwned + Send + 'static;

    /// Whether the write supports [`WriteMode::Truncate`], for the output mode "complete",
    /// which adds the `STREAMING_TRUNCATE` capability.
    const SUPPORTS_TRUNCATE: bool = false;

    /// False only for [`Unsupported`], from which the SDK derives the capabilities.
    #[doc(hidden)]
    const SUPPORTED: bool = true;

    /// Returns the configuration of the task writers, which the SDK serializes for them.
    fn new_writer_factory(&self) -> Result<Self::Factory>;

    /// Publish the winning attempt of each input partition as the output of the epoch
    /// [`StreamingCommitInfo::epoch_id`], unless that epoch is already committed.
    fn commit(&mut self, info: StreamingCommitInfo<Self::Message>) -> Result<()>;

    /// Abort the micro-batch after input, task, or driver commit failure, with the messages
    /// that reached the driver in [`StreamingAbortInfo::messages`].
    fn abort(&mut self, info: StreamingAbortInfo<Self::Message>) -> Result<()>;
}

/// What Spark passes to commit the write of a micro-batch.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct StreamingCommitInfo<M> {
    /// The epoch of the micro-batch, which with [`StreamingWriteInfo::query_id`] identifies
    /// its output.
    pub epoch_id: i64,
    /// The message of the winning task attempt of each input partition, at the position of
    /// its partition.
    pub messages: Vec<M>,
}

impl<M> StreamingCommitInfo<M> {
    /// The information of a commit made outside Spark, for example in a test.
    pub fn new(epoch_id: i64, messages: Vec<M>) -> Self {
        Self { epoch_id, messages }
    }
}

/// What Spark passes to abort the write of a micro-batch.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct StreamingAbortInfo<M> {
    /// The epoch of the micro-batch.
    pub epoch_id: i64,
    /// The message of each input partition, at the position of its partition, or `None` for a
    /// partition whose commit message did not reach the driver, or cannot be decoded.
    pub messages: Vec<Option<M>>,
}

impl<M> StreamingAbortInfo<M> {
    /// The information of an abort made outside Spark, for example in a test.
    pub fn new(epoch_id: i64, messages: Vec<Option<M>>) -> Self {
        Self { epoch_id, messages }
    }
}
