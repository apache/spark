<!--
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements. See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License. You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Rust native data source SDK prototype

This experimental SDK implements **batch reads** over the C ABI proposed in
[Spark PR #59424](https://github.com/apache/spark/pull/59424), pinned to commit
`534dfca15723c7208061b1e9e4d80c5757360a2a`. Spark calls the library through the
Java Foreign Function and Memory (FFM) API. This PR contains the SDK and example,
not the Spark framework; running the Spark example requires that framework and
Java 22 or newer on the driver and executors.

The Rust and C tests run independently of Spark and Java. The workspace is not
part of Maven/SBT and introduces no Rust requirement for building Spark. This is
an API prototype, not a published crate or a compatibility commitment.

The [Range connector][range] is a separate workspace member under
`examples/src/main/rust/native-data-source`, with a C integration test, packaging
helper, and Spark smoke script. Both crates share this directory's lockfile and
`target` directory. Cargo commands select the SDK by default; use `--workspace`
to include the example.

## Connector API and lifecycle

A connector implements three traits from [`src/lib.rs`](src/lib.rs):

* `BatchSource`: parse driver options, discover the schema, and create an
  independent scan builder. Spark can also create and immediately drop a source
  just to discover its capabilities; schema inference or a scan need not follow.
* `ScanBuilder`: negotiate predicates, limit, and projection, then consume itself
  in `plan()` to return a typed `ScanPlan<Factory>`.
* `BatchReaderFactory`: serialize executor configuration and open one lazy Arrow
  reader from a typed partition and a borrowed `TaskContext`.

The factory and partition types implement `Serialize` and `Deserialize`.
Connections, pointers, and open readers remain in their creating process.
Include a snapshot identifier in the factory or partitions when retrying a task
must read the same snapshot. Multiple task attempts open independent readers.

```text
Driver:
  BatchSource -> ScanBuilder -> ScanPlan { factory, partitions }
                     |                   |
              mutable negotiation   immutable serialized descriptors
                                         |
Executor, per task:                       v
  restored factory + partition + TaskContext -> Arrow reader -> Arrow C Stream
```

The Rust API maps to the framework's driver-only `ColumnarBatchPlanner` and its
`ColumnarPlannedBatch` containing a serializable `PartitionReaderFactory`.
`ColumnarScanBuilder.build()` builds a single scan and transfers planner
ownership; it does not plan partitions. `ColumnarBatchScan` plans on the first
request for partitions or a reader factory and caches success or failure.

For a native source, `NativeBatchPlanner` calls `partitions`, then
`serialize_reader`, and closes the native planner handle in `finally`. The
Rust `ScanBuilder::plan()` consumes its builder at that first native planning
call. The SDK still caches the encoded result so both native callbacks use the
same factory and partitions without planning twice or cloning byte buffers.
A failed negotiation or planning call retains its original error, including a
panic. Batch planner creation remains lazy for scans that do not read in batch.

The JVM's `NativePartitionReaderFactory` carries only the library location,
source name, read schema, and serialized state. The native executor `read` entry
point restores the Rust factory from those bytes without creating or accessing
the driver registry. Closing the driver handles cannot invalidate factory or
partition bytes already copied to Spark, including for retried tasks.

The reader owns its executor resources and cannot borrow its temporary factory.
It may borrow `TaskContext` until the returned Arrow stream is released. Task IDs
are optional for compatibility with shorter ABI structures. `is_interrupted()`
checks Spark's live cancellation callback, not a snapshot of its initial value.
Check it during long operations; blocking I/O still requires timeouts.

Register one provider in the connector's `cdylib` crate:

```rust
spark_native_source::export_data_source!(RangeSource);
```

The macro exports only `spark_ds_init` and returns Spark's versioned function
table. The connector uses safe Rust traits without C pointers. Rust trait objects
never cross the binary boundary between libraries.

The implementation separates public connector traits, driver handle/planning
state in `bridge.rs`, Arrow ownership in `stream.rs`, and C bindings/adapters in
`abi.rs` and `export.rs`. It does not change Spark's native ABI or JVM classes.

## Source capabilities

Every `BatchSource` created by this SDK declares exactly `SPARK_DS_BATCH_READ`.
Streaming, write, and truncate flags remain absent, and unsupported function
slots are null. These native flags describe what the source implements;
query-level predicate, limit, and projection results are separate contracts.

Spark also calls `create` and `close` during query analysis to discover table
capabilities. A probe uses the table's options, while an operation may override
them with query options and checks capabilities again. Keep `BatchSource::new`
lightweight and safe to drop immediately; do not assume that `schema` or `scan`
will follow. Executor resources belong in `BatchReaderFactory::open`.

The pinned framework propagates native `BATCH_WRITE` and `TRUNCATE` flags to
`Table.capabilities()`, so a batch-only source rejects batch writes during
analysis, before Spark computes their input. Its DSv2 read and streaming-write
flags remain present to avoid V1 fallback; the native flags still reject
unsupported reads or streaming writes during planning. Thus the DSv2 table's
flags are not a literal copy of this SDK's batch-read-only native mask.

## Arrow contract

`BatchReaderFactory::open` returns a lifetime-bound `Box<dyn RecordBatchReader +
Send>`. Return fixed-schema batches lazily with bounded memory. Exported arrays
own their buffers independently of the reader: Spark may release a stream before
releasing its previously returned batches.

Stream callbacks, including release, may run on different threads, but never
concurrently. Readers must be `Send`; they do not need to be `Sync` or tied to
the thread that opened the stream. Stream errors are owned by the stream and
remain readable after a callback moves to another thread.

The SDK validates the restored negotiated schema before exporting a reader and
checks each batch's schema and recursive array offsets. It compares Spark
logical types, ignoring nullability and Arrow metadata, accepting the supported
string/binary encodings and equivalent timestamp time zones. It rejects
unsupported types, schema drift, and nonzero array offsets with explicit errors.
It does not silently copy arrays, cast types, or expand dictionary encoding.

Supported types include signed integers, float32/64, boolean, strings and binary
(including Large and View variants), decimal128 up to precision 38, date32,
microsecond timestamps, supported Spark intervals, null, structs, lists, and
maps. Dictionary encoding, unsigned integers, decimal256, large/fixed-size
lists, and non-microsecond timestamps are outside this prototype's contract.
Normalize sliced arrays when necessary before returning them.

An empty projection must retain the number of rows even with no columns. The
[Range implementation][range-source] demonstrates this together with bounded
batches, disjoint partitions, exact integer filtering, and cooperative
cancellation.

## Pushdown contracts

Results correspond to input predicates in order:

| Result | C enum | Connector responsibility |
| --- | --- | --- |
| `Exact` | `SPARK_DS_PREDICATE_COMPLETE` | Return only matching rows under Spark SQL semantics |
| `PruningOnly` | `SPARK_DS_PREDICATE_PARTIAL` | Skip only data that cannot match |
| `Unsupported` | `SPARK_DS_PREDICATE_UNUSED` | No evaluation |

The pinned framework rechecks **all** filters, including `Exact`. Unknown
expression names and unsupported types must remain unsupported. The SDK
preserves literal JSON values, including exact 64-bit integers, rather than
converting all numbers to floating point. It is not a generic Spark expression
evaluator; connectors remain responsible for Spark SQL semantics.

| Limit result | ABI boolean | Meaning |
| --- | --- | --- |
| `Unsupported` | `false` | No limit applied |
| `PerPartition` | `false` | A local cap; the total may exceed the limit |
| `Global` | `true` | The entire scan returns at most the limit |

Spark retains its final limit and currently pushes a limit only when the scan
has no filters. The SDK also declines limit pushdown after any predicate that is
not `Exact`. Limiting rows before a residual filter can omit matching rows, even
when Spark retains its final limit.

Projection returns true only when the reader produces exactly the requested
top-level columns in order. Keep internally needed predicate columns even if
they are absent from the output projection. All pushdown methods have
conservative defaults so a connector can add support incrementally.

## Build and test without Spark

Use Rust 1.85 or newer (as required by Arrow 58) and a C compiler. The C integration
test uses `dlopen` and pthreads on Linux or macOS. From this directory:

```bash
cargo build --workspace --locked
cargo test --workspace --locked
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo fmt --all -- --check
```

Build before testing so the C harness loads the current example dynamic library.
The harness compiles against the unmodified framework header in
[`tests/fixtures/spark_datasource.h`](tests/fixtures/spark_datasource.h), copied
from the pinned commit above. It calls the actual function table and validates
capability-only source discovery and optional slots, independent driver planners,
pushdown and planning, serialized executor reads, cancellation, sequential
cross-thread stream callbacks, and Arrow buffers surviving stream release.
This checks the C boundary without requiring Java; it does not substitute for
Spark planner and task-lifecycle testing.

Unit tests additionally cover error and panic containment, terminal planning
failures, retries after the driver registry is dropped, schema/layout validation,
sized ABI structures, task-context lifetimes, integer boundaries, projections,
and deterministic partition plans.

## Package and run with Spark

See the [Range example guide][range] for packaging and `spark-submit` commands.
Its `package.py` creates a single-platform `.sparkpkg`; `smoke.py` checks reading,
filtered reads, projection, unfiltered limit pushdown, and analysis-time rejection
of unsupported batch writes against the pinned #59424 framework. The write
checks use failing input behind a shuffle to verify Spark rejects the operation
before computing its data. No full Spark integration is implied by the
standalone Rust/C tests.

## Scope and limitations

Native sources run trusted code in the driver and executors. The SDK translates
provider errors and unwinding panics into C/Arrow errors. Process aborts,
allocation failure, or panics in custom Arrow buffer destructors cannot be
recovered. A reader's custom background work must stop before its task context
expires. Native memory is not reserved through Spark's memory manager.

This prototype does not include writes, Structured Streaming, nested projection,
custom metrics, memory accounting, a generic filter evaluator, C++ bindings, or
an ABI extension. The versioned JSON envelope is an SDK implementation detail;
use the same plugin build on the driver and executors. Only the pinned framework
revision is targeted until the native API stabilizes.

[range]: ../../../examples/src/main/rust/native-data-source/README.md
[range-source]: ../../../examples/src/main/rust/native-data-source/src/lib.rs
