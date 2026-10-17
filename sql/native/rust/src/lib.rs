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

//! Experimental batch-read SDK for the JNI ABI proposed in Spark PR #59313.
//!
//! Implement [`BatchSource`] and [`ScanBuilder`], then invoke
//! [`export_data_source!`] once in a `cdylib` crate. The SDK owns JNI conversion,
//! handles, plan serialization, and Arrow C Stream export. Rust traits stay
//! inside the plugin; they are not a binary interface between dynamic libraries.

use std::collections::BTreeMap;

use arrow_array::RecordBatchReader;
use arrow_schema::SchemaRef;
use serde::de::DeserializeOwned;
use serde::Serialize;

#[doc(hidden)]
pub mod bridge;
mod export;
mod predicate;

pub use predicate::Predicate;

pub type Error = Box<dyn std::error::Error + Send + Sync>;
pub type Result<T> = std::result::Result<T, Error>;
pub type Options = BTreeMap<String, String>;
pub type BatchReader = Box<dyn RecordBatchReader + Send>;

/// How completely a connector evaluates a pushed predicate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FilterSupport {
    /// Implements Spark SQL semantics and returns only matching rows.
    Exact,
    /// May skip data, but Spark must evaluate the predicate again.
    PruningOnly,
    /// Spark must evaluate the predicate.
    Unsupported,
}

/// How a connector limits the rows produced by the whole scan.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LimitSupport {
    Unsupported,
    /// Caps each partition; Spark retains the global limit. JNI returns false
    /// because ABI v1's boolean promises a bound on the *whole* scan.
    PerPartition,
    /// Caps the whole scan; JNI returns true. Spark still retains its limit.
    Global,
}

/// A driver-created plan containing only serializable values.
///
/// The SDK serializes the state once and each partition independently. Include
/// snapshot identifiers here when needed for repeatable task attempts. Do not
/// put pointers, open files, connections, or credentials with an assumed
/// process-local lifetime into these values.
pub struct ScanPlan<S, P> {
    pub state: S,
    pub partitions: Vec<P>,
}

/// A provider's driver-side configuration and executor-side reader factory.
pub trait BatchSource: Send + Sized + 'static {
    type Scan: ScanBuilder;

    /// Spark passes option keys in lower case.
    fn new(options: Options) -> Result<Self>;

    fn schema(&self) -> Result<SchemaRef>;

    /// The schema may be supplied by the user; reject unsupported schemas.
    /// The returned builder must remain valid after this source is dropped.
    fn scan(&self, schema: SchemaRef) -> Result<Self::Scan>;

    /// Called in an executor with restored values, without a driver object.
    ///
    /// Return batches lazily with a fixed schema and bounded memory. Buffers
    /// already exported to Spark must remain valid until Spark releases them.
    /// Use Arrow types supported by the pinned Spark ABI, with zero array
    /// offsets at every nesting level. Normalize sliced arrays when necessary;
    /// the SDK does not rewrite buffers or expand dictionary encoding.
    /// Blocking I/O needs timeouts: ABI v1 does not provide task cancellation.
    fn open(
        state: <Self::Scan as ScanBuilder>::State,
        partition: <Self::Scan as ScanBuilder>::Partition,
    ) -> Result<BatchReader>;
}

/// Builds one scan. Spark negotiates filters, limit, and projection before
/// planning partitions; the SDK consumes the builder and caches the plan.
pub trait ScanBuilder: Send + Sized + 'static {
    type State: Serialize + DeserializeOwned + Send;
    type Partition: Serialize + DeserializeOwned + Send;

    /// Return one result per input predicate in the same order. Only `Exact`
    /// allows Spark to remove a residual filter. Unknown functions and types
    /// must not be reported as exact. Unsupported filters need not be stored.
    fn push_filters(&mut self, filters: &[Predicate]) -> Result<Vec<FilterSupport>> {
        Ok(vec![FilterSupport::Unsupported; filters.len()])
    }

    /// Count rows after filtering, never merely scanned rows. Spark decides
    /// whether pushing a limit is safe for the query; the SDK also declines
    /// limit pushdown when any predicate it received remains residual.
    fn push_limit(&mut self, _limit: usize) -> Result<LimitSupport> {
        Ok(LimitSupport::Unsupported)
    }

    /// On true, output exactly these top-level columns in this order. On
    /// false, keep the original schema. An empty projection is valid and
    /// must preserve row counts. Keep internally needed filter columns.
    fn prune_columns(&mut self, _columns: &[String]) -> Result<bool> {
        Ok(false)
    }

    fn build(self) -> Result<ScanPlan<Self::State, Self::Partition>>;
}
