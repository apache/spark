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

use arrow_array::{RecordBatch, RecordBatchReader};
use arrow_schema::SchemaRef;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

#[doc(hidden)]
pub mod abi;
#[doc(hidden)]
pub mod bridge;
mod export;
mod predicate;
#[doc(hidden)]
pub mod provider;
mod task;
#[doc(hidden)]
pub mod write;

pub use predicate::Predicate;
pub use task::TaskContext;

pub type Error = Box<dyn std::error::Error + Send + Sync>;
pub type Result<T> = std::result::Result<T, Error>;
pub type Options = BTreeMap<String, String>;
pub type BatchReader<'task> = Box<dyn RecordBatchReader + Send + 'task>;
pub type TaskWriter<'task, M> = Box<dyn DataWriter<Message = M> + 'task>;

/// Native operations supported by a source instance with its effective options.
///
/// These are native capabilities, not Spark's Data Source V2 routing flags or
/// per-scan pushdown results. Registration determines the available operations;
/// a source instance may disable them but cannot enable unregistered operations.
/// Streaming operations are not implemented by this SDK yet.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum SourceCapability {
    BatchRead,
    MicroBatchRead,
    BatchWrite,
    StreamingWrite,
    /// Supports writes that replace the table's existing data.
    Truncate,
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

/// How a connector limits the rows produced by the whole scan.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum LimitSupport {
    Unsupported,
    /// Caps each partition; the ABI boolean is false because it promises a
    /// bound on the whole scan. Spark retains its final limit in either case.
    PerPartition,
    /// Caps the whole scan; the ABI boolean is true.
    Global,
}

/// A driver-created plan containing only serializable values.
///
/// The SDK serializes the reader factory once and each partition independently.
/// Include snapshot identifiers when needed for repeatable task attempts. Do not
/// put pointers, open files, connections, or process-local resources here.
pub struct ScanPlan<F: BatchReaderFactory> {
    pub factory: F,
    pub partitions: Vec<F::Partition>,
}

/// Driver-side source configuration, schema discovery, and capability restrictions.
///
/// Spark may create a source only to inspect its capabilities, then drop it.
/// Keep construction lightweight. Readers and writers must outlive their source.
pub trait DataSource: Send + Sized + 'static {
    /// Spark passes option keys in lower case. A capability probe uses table
    /// options; an operation may override them with query options.
    fn new(options: Options) -> Result<Self>;

    /// By default, enable every registered operation. Override this to restrict
    /// the current instance, for example for a read-only destination. The SDK
    /// evaluates this once, ignores duplicates, and rejects unregistered flags.
    /// Truncate requires BatchWrite. An empty set still permits schema discovery.
    fn capabilities(&self, supported: &[SourceCapability]) -> Vec<SourceCapability> {
        supported.to_vec()
    }

    fn schema(&self) -> Result<SchemaRef>;
}

/// Driver-side entry point for batch reads.
///
/// Register with `export_data_source!(Source, batch_read)` (or its one-argument
/// shorthand). Metadata and construction belong to [`DataSource`].
pub trait BatchSource: DataSource {
    type Scan: ScanBuilder;

    /// The schema may be supplied by the user; reject unsupported schemas.
    /// The returned builder must remain valid after this source is dropped.
    fn scan(&self, schema: SchemaRef) -> Result<Self::Scan>;
}

/// How a batch write changes the destination.
///
/// New modes may be added. The SDK passes a mode only when the source instance
/// has the matching capability, so a source never receives a mode it did not
/// opt in to; a wildcard arm can return an error.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum WriteMode {
    Append,
    /// Replace existing data at driver commit, never at writer creation.
    Truncate,
}

/// Driver-side entry point for batch writes, independent of read support.
///
/// Register `batch_write` and, if implemented, `truncate`. The SDK derives native
/// capabilities and callbacks from registration, then applies instance restrictions.
pub trait BatchWriteSource: DataSource {
    type Write: BatchWrite;

    /// Validate the input schema and create an independent write coordinator.
    /// Construction may be followed only by Drop, without commit or abort.
    /// The SDK calls this only while the instance's capabilities include
    /// BatchWrite, and passes `Truncate` only when they include Truncate.
    fn write(&self, schema: SchemaRef, mode: WriteMode) -> Result<Self::Write>;
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
/// - `factory` runs at most once, and never after `commit` or `abort`.
/// - `commit` runs at most once, only while no earlier call has failed. It is
///   skipped, and the write fails, when any task message is missing.
/// - `abort` runs at most once, never after a successful `commit`. It may follow
///   a failed `factory` or `commit`, or run without either being called.
/// - Drop runs exactly once, possibly without `commit` or `abort`.
/// - A panic in any method is reported as that method's failure.
pub trait BatchWrite: Send + 'static {
    type Factory: DataWriterFactory;

    /// Return serializable executor configuration. The SDK caches the encoded
    /// factory and negotiated schema, including serialization failures.
    /// Include a unique operation ID when needed; Spark supplies no batch query ID.
    fn factory(&mut self) -> Result<Self::Factory>;

    /// Publish only the winning attempt from each input partition. Task retries
    /// and speculative attempts can stage other data that must not be published.
    fn commit(
        &mut self,
        messages: Vec<<Self::Factory as DataWriterFactory>::Message>,
    ) -> Result<()>;

    /// Abort after input, task, or driver commit failure. None preserves the
    /// position of a partition whose commit message did not reach the driver.
    /// Executor loss can leave orphaned staged data; plan for its cleanup.
    fn abort(
        &mut self,
        messages: Vec<Option<<Self::Factory as DataWriterFactory>::Message>>,
    ) -> Result<()>;
}

/// Serializable configuration for independent task-attempt writers.
///
/// Open connections and files in open, not in the factory. A returned writer
/// cannot borrow this temporary factory, but may retain the task context until
/// close. Callbacks can move between threads sequentially; writers must be Send.
pub trait DataWriterFactory: Serialize + DeserializeOwned + Send + 'static {
    type Message: Serialize + DeserializeOwned + Send + 'static;

    fn open<'task>(&self, task: TaskContext<'task>) -> Result<TaskWriter<'task, Self::Message>>;
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
/// - `abort` runs at most once, never after a successful `commit`. It may follow
///   a failed `write` or `commit`.
/// - Drop runs exactly once when Spark closes the writer, possibly without
///   `commit` or `abort`.
/// - A panic in any method is reported as that method's failure.
pub trait DataWriter: Send {
    type Message;

    /// Takes ownership of an imported Arrow batch, which may be retained after
    /// this call. Process batches incrementally and check task cancellation.
    fn write(&mut self, batch: RecordBatch) -> Result<()>;

    /// Finish staging and return a typed message for the driver. Do not make the
    /// whole write visible here. On failure (including message serialization),
    /// the SDK keeps this writer available for abort.
    fn commit(&mut self) -> Result<Self::Message>;

    fn abort(&mut self) -> Result<()>;
}

/// Serializable configuration for creating independent executor readers.
///
/// This type carries the configuration or snapshot descriptor; connections and
/// other live resources are opened on the executor. The returned reader owns
/// those resources and cannot borrow this short-lived factory.
pub trait BatchReaderFactory: Serialize + DeserializeOwned + Send + 'static {
    type Partition: Serialize + DeserializeOwned + Send + 'static;

    /// Return lazy batches with a fixed schema and bounded memory. The reader
    /// may retain `task` until the stream is released. Check cancellation during
    /// long operations and use timeouts for blocking I/O.
    /// Spark may call stream callbacks on different threads, never concurrently;
    /// the reader must be `Send` but does not need to be `Sync`.
    ///
    /// The SDK checks Spark-compatible Arrow types, the negotiated schema, and
    /// zero array offsets recursively. It reports incompatible layouts instead
    /// of copying buffers. Exported batches must own their buffers independently
    /// of the reader, because Spark may release the stream before its batches.
    fn open<'task>(
        &self,
        partition: Self::Partition,
        task: TaskContext<'task>,
    ) -> Result<BatchReader<'task>>;
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
    type Factory: BatchReaderFactory;

    /// Return one result per input predicate in order. Unknown functions and
    /// types must not be reported as exact. Spark currently rechecks all filters.
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
    /// Retain internally needed filter columns without returning them.
    fn prune_columns(&mut self, _columns: &[String]) -> Result<bool> {
        Ok(false)
    }

    fn plan(self) -> Result<ScanPlan<Self::Factory>>;
}
