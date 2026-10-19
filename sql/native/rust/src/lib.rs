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
//! Implement [`DataSource`] and the batch-read or batch-write traits, then invoke
//! [`export_data_source!`] once in a `cdylib` crate. The SDK owns handles,
//! plan/message serialization, C ABI adaptation, and Arrow import/export. Rust traits
//! stay inside the plugin; they are not an ABI between dynamic libraries.

use std::collections::BTreeMap;
use std::fmt;

use arrow_array::{RecordBatch, RecordBatchReader};
use arrow_schema::SchemaRef;
use serde::de::DeserializeOwned;
use serde::Serialize;

mod abi;
mod bridge;
mod export;
mod host;
mod predicate;
mod provider;
mod task;
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

    pub mod provider {
        pub use crate::provider::*;
    }

    pub mod write {
        pub use crate::write::*;
    }
}

pub use host::{log, HostInfo, LogLevel};
pub use predicate::{LiteralValue, Predicate};
pub use task::{TaskContext, TaskContextBuilder};

pub type Error = Box<dyn std::error::Error + Send + Sync>;
pub type Result<T> = std::result::Result<T, Error>;
pub type Options = BTreeMap<String, String>;
/// The lazy Arrow reader of one partition, which [`PartitionReaderFactory::open`] returns.
pub type PartitionReader<'task> = Box<dyn RecordBatchReader + Send + 'task>;
/// The writer of one task attempt, which [`DataWriterFactory::open`] returns.
pub type PartitionWriter<'task, M> = Box<dyn DataWriter<Message = M> + 'task>;

/// A native operation of a source instance.
///
/// These are native capabilities, not Spark's Data Source V2 routing flags or
/// per-scan pushdown results. Streaming operations are not implemented by this SDK yet.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum SourceCapability {
    BatchRead,
    BatchWrite,
    /// Batch writes that replace the existing data. Requires `BatchWrite`.
    Truncate,
}

impl SourceCapability {
    const ALL: [Self; 3] = [Self::BatchRead, Self::BatchWrite, Self::Truncate];

    const fn bit(self) -> u64 {
        match self {
            Self::BatchRead => abi::BATCH_READ,
            Self::BatchWrite => abi::BATCH_WRITE,
            Self::Truncate => abi::TRUNCATE,
        }
    }
}

/// The native operations that a source instance supports with its effective options.
///
/// Registration determines the operations of a library. [`DataSource::capabilities`]
/// receives them and can only remove some, so a source cannot enable an operation
/// that it did not register. Removing `BatchWrite` also removes `Truncate`.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Capabilities(u64);

impl Capabilities {
    /// No operations. A source without capabilities still supports schema discovery.
    pub const fn empty() -> Self {
        Self(0)
    }

    /// Every operation that this SDK implements, to test [`DataSource::capabilities`].
    /// The SDK keeps only the operations that the library registers.
    pub const fn all() -> Self {
        Self(abi::BATCH_READ | abi::BATCH_WRITE | abi::TRUNCATE)
    }

    pub const fn contains(self, capability: SourceCapability) -> bool {
        self.0 & capability.bit() != 0
    }

    pub const fn is_empty(self) -> bool {
        self.0 == 0
    }

    /// Removes an operation, and also `Truncate` when it removes `BatchWrite`.
    #[must_use]
    pub const fn without(self, capability: SourceCapability) -> Self {
        let removed = match capability {
            SourceCapability::BatchWrite => abi::BATCH_WRITE | abi::TRUNCATE,
            other => other.bit(),
        };
        Self(self.0 & !removed)
    }

    /// The operations that registering batch reads, batch writes, and truncation enables.
    pub(crate) const fn registered(read: bool, write: bool, truncate: bool) -> Self {
        let mut bits = 0;
        if read {
            bits |= abi::BATCH_READ;
        }
        if write {
            bits |= abi::BATCH_WRITE;
            if truncate {
                bits |= abi::TRUNCATE;
            }
        }
        Self(bits)
    }

    /// The operations in both sets, without `Truncate` unless `BatchWrite` remains.
    pub(crate) const fn intersection(self, other: Self) -> Self {
        let bits = self.0 & other.0;
        if bits & abi::BATCH_WRITE == 0 {
            Self(bits & !abi::TRUNCATE)
        } else {
            Self(bits)
        }
    }

    /// The native capability mask, a combination of the SPARK_DS_* flags.
    pub(crate) const fn bits(self) -> u64 {
        self.0
    }
}

impl fmt::Debug for Capabilities {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_set()
            .entries(
                SourceCapability::ALL
                    .into_iter()
                    .filter(|capability| self.contains(*capability)),
            )
            .finish()
    }
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

/// Driver-side source configuration, schema discovery, and capability restrictions.
///
/// Spark creates a source for each schema inference, scan, and write, and may create
/// one only to inspect its capabilities, then drop it. Keep construction lightweight.
/// Scan builders and writes cannot borrow their source, which Spark drops first.
pub trait DataSource: Send + Sized + 'static {
    /// Whether [`schema`](Self::schema) infers the schema of the data. When false, the
    /// SDK leaves the native function unset: Spark then requires a user-specified
    /// schema, reports its standard error without one, and never calls `schema`.
    const INFERS_SCHEMA: bool = true;

    /// Called once in each JVM when Spark loads the library, before it creates any
    /// source. On failure, Spark does not use the library, and the error is logged.
    fn init(_host: &HostInfo) -> Result<()> {
        Ok(())
    }

    fn new(info: CreateInfo) -> Result<Self>;

    /// Returns the operations of this instance, a subset of the registered ones. Override
    /// this to restrict an instance, for example for a read-only destination. The SDK
    /// evaluates it once when Spark creates the source. An instance without operations
    /// still supports schema discovery.
    fn capabilities(&self, registered: Capabilities) -> Capabilities {
        registered
    }

    /// Returns the schema of the data, when the user does not specify one.
    fn schema(&self) -> Result<SchemaRef> {
        Err(
            "DataSource::schema is not implemented: implement it, or set \
             DataSource::INFERS_SCHEMA to false to require a user-specified schema"
                .into(),
        )
    }
}

/// Driver-side entry point for batch reads.
///
/// Register with `export_data_source!(Source, batch_read)`. Metadata and construction
/// belong to [`DataSource`].
pub trait BatchReadSource: DataSource {
    type ScanBuilder: ScanBuilder;

    /// Creates an independent scan of the schema to read, which the user may have
    /// specified; reject unsupported schemas.
    fn new_scan(&self, schema: SchemaRef) -> Result<Self::ScanBuilder>;
}

/// Mutable driver-side negotiation, followed by immutable planning.
///
/// The SDK consumes this builder on the first native `partitions` or
/// `serialize_reader` call and caches the result, including a planning failure.
/// This is later than Java DSv2 `ScanBuilder.build()`. Only serialized factory
/// and partition descriptors travel to executors; driver handles stay local.
/// It corresponds to Spark's driver-only `ColumnarBatchPlanner`: the native
/// planner calls `partitions`, then `serialize_reader`, then closes the handle.
pub trait ScanBuilder: Send + Sized + 'static {
    type Factory: PartitionReaderFactory;

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

/// Serializable configuration for creating independent executor readers.
///
/// This type carries the configuration or snapshot descriptor; connections and
/// other live resources are opened on the executor. The returned reader owns
/// those resources and cannot borrow this short-lived factory. The SDK serializes
/// the negotiated schema with the factory, so the factory need not carry it.
pub trait PartitionReaderFactory: Serialize + DeserializeOwned + Send + 'static {
    type Partition: Serialize + DeserializeOwned + Send + 'static;

    /// Return lazy batches with the schema of `context` and bounded memory. The reader
    /// may retain the context until the stream is released. Check cancellation during
    /// long operations and use timeouts for blocking I/O.
    /// Spark may call stream callbacks on different threads, never concurrently;
    /// the reader must be `Send` but does not need to be `Sync`.
    ///
    /// The SDK checks Spark-compatible Arrow types, the negotiated schema and its
    /// nullability, and zero array offsets recursively. It reports incompatible layouts
    /// instead of copying buffers. Exported batches must own their buffers independently
    /// of the reader, because Spark may release the stream before its batches.
    fn open<'task>(
        &self,
        partition: Self::Partition,
        context: ReaderContext<'task>,
    ) -> Result<PartitionReader<'task>>;
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

/// Driver-side entry point for batch writes, independent of read support.
///
/// Register with `export_data_source!(Source, batch_write)`. The SDK derives native
/// capabilities and callbacks from registration, then applies instance restrictions.
pub trait BatchWriteSource: DataSource {
    type BatchWrite: BatchWrite;

    /// Whether [`new_write`](Self::new_write) supports [`WriteMode::Truncate`], which
    /// registers the `Truncate` capability.
    const SUPPORTS_TRUNCATE: bool = false;

    /// Validate the input schema and create an independent write. Construction may be
    /// followed only by Drop, without commit or abort, so it must not make irreversible
    /// changes. The SDK calls this only while the instance's capabilities include
    /// BatchWrite, and passes `Truncate` only when they include Truncate.
    fn new_write(&self, info: WriteInfo) -> Result<WritePlan<Self::BatchWrite>>;
}

/// A driver coordinator and the serializable configuration of its task writers.
///
/// The SDK serializes the factory once, with the schema of the input, and caches the
/// result, including a serialization failure. Include a unique operation ID in the
/// factory when needed; Spark supplies no batch query ID.
#[non_exhaustive]
pub struct WritePlan<W: BatchWrite> {
    /// The coordinator that commits or aborts the write on the driver.
    pub coordinator: W,
    /// The configuration from which each task attempt opens its writer.
    pub factory: W::Factory,
}

impl<W: BatchWrite> WritePlan<W> {
    pub fn new(coordinator: W, factory: W::Factory) -> Self {
        Self {
            coordinator,
            factory,
        }
    }
}

/// A driver coordinator for one batch write.
///
/// Keep irreversible publication, including removing overwritten data, in commit.
/// Drop releases resources; it must not publish data or perform fallible flushing.
///
/// # Lifecycle
///
/// The SDK enforces the following, so implementations need not track their own
/// phase or guard against repeated calls:
///
/// - `commit` runs at most once, only while no earlier call has failed, including
///   the serialization of the factory. It is skipped, and the write fails, when
///   any task message is missing or cannot be decoded.
/// - `abort` runs at most once, never after a successful `commit`. It may follow
///   a failed `commit`, or run without `commit`.
/// - Drop runs exactly once, possibly without `commit` or `abort`. Spark closes the
///   coordinator without `abort` when serializing the factory fails, so Drop must
///   also release what only this write staged.
/// - A panic in any method is reported as that method's failure.
///
/// Message `i` of `commit` and `abort` belongs to input partition `i`.
pub trait BatchWrite: Send + 'static {
    type Factory: DataWriterFactory;

    /// Publish only the winning attempt from each input partition. Task retries
    /// and speculative attempts can stage other data that must not be published.
    fn commit(
        &mut self,
        messages: Vec<<Self::Factory as DataWriterFactory>::Message>,
    ) -> Result<()>;

    /// Abort after input, task, or driver commit failure. None stands for a partition
    /// whose commit message did not reach the driver, or cannot be decoded.
    /// Executor loss can leave orphaned staged data; plan for its cleanup.
    fn abort(
        &mut self,
        messages: Vec<Option<<Self::Factory as DataWriterFactory>::Message>>,
    ) -> Result<()>;
}

/// What a task writer receives when it opens.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct WriterContext<'task> {
    /// The schema of the batches to write, as in [`WriteInfo::schema`].
    pub schema: SchemaRef,
    /// The task attempt that writes.
    pub task: TaskContext<'task>,
}

impl<'task> WriterContext<'task> {
    /// The context of a writer opened outside Spark, for example in a test.
    pub fn new(schema: SchemaRef, task: TaskContext<'task>) -> Self {
        Self { schema, task }
    }
}

/// Serializable configuration for independent task-attempt writers.
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
