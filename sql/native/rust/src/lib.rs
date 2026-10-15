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

//! Experimental batch-read SDK for the C ABI proposed in Spark PR #59424.
//!
//! Implement [`BatchSource`], [`ScanBuilder`] and [`BatchReaderFactory`], then
//! invoke [`export_data_source!`] once in a `cdylib` crate. The SDK owns handles,
//! plan serialization, C ABI adaptation, and Arrow C Stream export. Rust traits
//! stay inside the plugin; they are not an ABI between dynamic libraries.

use std::collections::BTreeMap;

use arrow_array::RecordBatchReader;
use arrow_schema::SchemaRef;
use serde::de::DeserializeOwned;
use serde::Serialize;

#[doc(hidden)]
pub mod abi;
#[doc(hidden)]
pub mod bridge;
mod export;
mod predicate;
mod task;

pub use predicate::Predicate;
pub use task::TaskContext;

pub type Error = Box<dyn std::error::Error + Send + Sync>;
pub type Result<T> = std::result::Result<T, Error>;
pub type Options = BTreeMap<String, String>;
pub type BatchReader<'task> = Box<dyn RecordBatchReader + Send + 'task>;

/// How completely a connector evaluates a pushed predicate.
/// Spark currently rechecks all predicates, including exact ones.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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

/// Driver-side source configuration and schema discovery.
///
/// Registering this trait declares only the batch-read capability. Writes and
/// streaming remain absent from the native function table.
pub trait BatchSource: Send + Sized + 'static {
    type Scan: ScanBuilder;

    /// Spark passes option keys in lower case.
    fn new(options: Options) -> Result<Self>;

    fn schema(&self) -> Result<SchemaRef>;

    /// The schema may be supplied by the user; reject unsupported schemas.
    /// The returned builder must remain valid after this source is dropped.
    fn scan(&self, schema: SchemaRef) -> Result<Self::Scan>;
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
