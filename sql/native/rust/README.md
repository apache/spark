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

This experimental SDK implements **batch reads and writes** over the C ABI proposed in
[Spark PR #59424](https://github.com/apache/spark/pull/59424), pinned to commit
`74aa0dffc53463034fa6549796c4dd599e9705de`. Spark calls the library through the
Java Foreign Function and Memory (FFM) API. This PR contains the SDK and example,
not the Spark framework; running the Spark example requires that framework and
Java 22 or newer on the driver and executors.

The Rust and C tests run independently of Spark and Java. The workspace is not
part of Maven/SBT and introduces no Rust requirement for building Spark. This is
an API prototype, not a published crate or a compatibility commitment.

The [Range connector][range] is a separate workspace member under
`examples/src/main/rust/native-data-source`, with a C integration test, packaging
helper, and Spark smoke script. The [file sink example][sink] is a separate write-only connector. All crates
share this directory's lockfile and `target` directory. Cargo commands select the SDK by default; use `--workspace`
to include the example.

## Connector API and lifecycle

A connector implements [`DataSource`](src/lib.rs) for construction, schema discovery,
its operations, and optional capability restrictions. `DataSource::new` receives a
`CreateInfo` with the name of the data source and its options. Spark can create and
immediately drop a source just to discover capabilities; schema inference or an
operation need not follow.

Two associated types name the operations that the source implements, and
`Unsupported` stands for one that it does not implement:

```rust
impl DataSource for RangeSource {
    type ScanBuilder = RangeScan; // batch reads, created by new_scan
    type BatchWrite = Unsupported; // no batch writes
    // new, schema, and new_scan
}
```

Read support uses three pieces:

* `DataSource::new_scan`: create an independent scan builder.
* `ScanBuilder`: negotiate predicates, limit, and projection, then consume itself
  in `plan()` to return a typed `ScanPlan<Factory>`.
* `PartitionReaderFactory`: serialize executor configuration and open one lazy Arrow
  reader from a typed partition and a `ReaderContext`, which carries the negotiated
  schema and a borrowed `TaskContext`.

Write support is independent: a write-only connector sets `ScanBuilder` to
`Unsupported` and implements `DataSource::new_write`, `BatchWrite`,
`DataWriterFactory`, and `DataWriter`. The write lifecycle is described below.

The inputs of these methods are `#[non_exhaustive]` structs (`CreateInfo`, `WriteInfo`,
`ReaderContext`, and `WriterContext`), and the plans (`ScanPlan` and `WritePlan`) have
constructors, so that new ABI fields can reach connectors without breaking them.

The factory and partition types implement `Serialize` and `Deserialize`.
Connections, pointers, and open readers remain in their creating process.
Include a snapshot identifier in the factory or partitions when retrying a task
must read the same snapshot. Multiple task attempts open independent readers.

```text
Driver:
  DataSource::new_scan -> ScanBuilder -> ScanPlan { factory, partitions }
                              |                   |
                       mutable negotiation   immutable serialized descriptors
                                                  |
Executor, per task:                               v
  restored factory + partition + ReaderContext -> Arrow reader -> Arrow C Stream
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
point restores the Rust factory and the negotiated schema from those bytes without
creating or accessing the driver registry, and passes the schema to the reader in
`ReaderContext`. Closing the driver handles cannot invalidate factory or partition
bytes already copied to Spark, including for retried tasks.

The reader owns its executor resources and cannot borrow its temporary factory.
It may borrow `TaskContext` until the returned Arrow stream is released. Every host
provides the task IDs of version 1 of the ABI, so the SDK rejects a shorter task
context. `is_interrupted()` checks Spark's live cancellation callback, not a snapshot
of its initial value. Check it during long operations; blocking I/O still requires
timeouts. `TaskContext::for_testing()` builds a context with chosen IDs and an
optional interruption flag for tests outside Spark.

Export the source from the connector's `cdylib` crate:

```rust
spark_native_source::export_data_source!(RangeSource);
```

The macro exports only `spark_ds_init` and returns Spark's versioned function
table. The connector uses safe Rust traits without C pointers. Rust trait objects
never cross the binary boundary between libraries.

`DataSource::init` runs once in each JVM when Spark loads the library, with the
Spark version in `HostInfo`; a failure prevents Spark from using the library and is
logged. `spark_native_source::log` writes to the driver or executor log through
Spark from any thread, and drops messages outside Spark, such as in unit tests.

A source that cannot infer its schema, such as a sink of any input, sets
`DataSource::INFERS_SCHEMA` to false. The SDK then leaves the native `schema`
function unset, so Spark requires a user-specified schema and reports its standard
error without one.

A library that implements several data sources is installed or listed in its
package under each of their names. Spark passes the name in `CreateInfo::name`, so
one exported `DataSource` can choose the source to create; the macro exports a
single Rust type per library.

The implementation separates public connector traits, driver handle/planning
state in `bridge.rs`, Arrow ownership in `stream.rs`, and C bindings/adapters in
`abi.rs` and `export.rs`. The macro reaches them through a hidden `__private`
module, which is not part of the API. It does not change Spark's native ABI or JVM
classes.

## Source capabilities

The associated types of `DataSource` determine the operations of the source: batch
reads unless `ScanBuilder` is `Unsupported`, and batch writes unless `BatchWrite` is
`Unsupported`. The SDK installs the native callbacks of these operations, leaves the
others null, and derives the default capability set from them, so the macro takes
only the source type. A source opts into overwrite support with
`const SUPPORTS_TRUNCATE: bool = true`, next to the `new_write` that handles
`WriteMode::Truncate`; without it the source supports only append writes. The macro
also rejects a library built with `panic = "abort"`, because the SDK can report a
panic as an error only when it unwinds.

Most connectors do not implement `capabilities`. A connector can restrict the
supported set for the current options by overriding this `DataSource` method:

```rust
fn capabilities(&self, supported: Capabilities) -> Capabilities {
    if self.read_only {
        // Also removes Truncate, which requires batch writes.
        supported.without(SourceCapability::BatchWrite)
    } else {
        supported
    }
}
```

The SDK evaluates this once during `create` and keeps only implemented operations,
so a source can narrow its capabilities but never enable one that it does not
implement.
An empty set permits schema discovery and close, but rejects operation creation
before calling the connector. The declaration stays fixed for that instance.
`Capabilities::all()` exists to test this method outside Spark. Streaming
capabilities are not supported yet. Predicate, limit, and projection support remain
per-scan contracts.

Spark also calls `create` and `close` during query analysis to discover table
capabilities. A probe uses the table's options, while an operation may override
them with query options and checks capabilities again on a new source instance.
Keep `DataSource::new` and `capabilities` lightweight and safe to drop
immediately; do not assume that `schema` or `new_scan` will follow. Executor
resources belong in `PartitionReaderFactory::open`.

The pinned framework propagates native `BATCH_WRITE` and `TRUNCATE` flags to
`Table.capabilities()`, so a read-only source rejects batch writes during
analysis, before Spark computes their input. Its DSv2 read and streaming-write
flags remain present to avoid V1 fallback; the native flags still reject
unsupported reads or streaming writes during planning. Thus the DSv2 table's
flags are not a literal copy of the native mask declared by the Rust source.

## Batch write lifecycle

```text
Driver:
  DataSource::new_write -> WritePlan { coordinator, factory }
                                 |             ^ commit(messages) / abort(messages)
                serialized factory + schema    | typed messages
                                 |             |
Executor, per task attempt:      v             |
                          DataWriterFactory::open(WriterContext)
                                   -> DataWriter::write(RecordBatch)*
                                   -> commit() / abort() -> Drop
```

`DataSource::new_write(WriteInfo)` validates the input schema and returns a
`WritePlan`: an independent driver coordinator and the serializable executor
configuration of its task writers. The SDK encodes the factory and input schema
once, including a terminal error if serialization fails. Restoring an executor
writer uses only those bytes, without creating a driver registry.

Each `DataWriterFactory::open` receives a `WriterContext` with the input schema and
the task, and returns a task-bound writer. `write` takes ownership of a
`RecordBatch`, so its Arrow buffers can be retained after the callback. The SDK
moves the incoming Arrow array and schema, checks the schema against the negotiated
input, and releases imported resources on failures. Input slices need not have zero
offsets; the zero-offset restriction applies to arrays exported to Spark by readers.
Check cancellation during lengthy writes and use I/O timeouts.

Task `commit()` returns a typed, serde-serializable message. Driver `commit`
receives one winning message per partition, at the position of its partition;
driver `abort` receives `Vec<Option<Message>>`, with `None` for a partition whose
message did not arrive or cannot be decoded, so a corrupted message cannot prevent
cleanup. A failed task or driver commit leaves the same object available for abort.
A task commit whose message cannot be serialized counts as failed, so `abort` can
follow a `commit` that returned a message. The SDK prevents further writes after a
failure or successful commit, caches successful task commit messages, and avoids
repeating successful commit/abort callbacks. Drop only releases resources.
Connectors therefore do not need to track their own phase; the `# Lifecycle`
sections of `BatchWrite` and `DataWriter` list the exact call-order guarantees.

Stage data per task attempt and publish it only at driver commit. In particular,
`WriteMode::Truncate` must leave existing data intact until that commit. Retries
and speculative attempts may stage data for the same partition; publish only the
attempts whose messages Spark passes to commit. Executor loss can prevent cleanup,
including after task commit, so connectors need a strategy for orphaned data.
Spark provides no batch query ID: generate and serialize an operation identity
when needed. Task-attempt IDs alone are not globally unique across applications.

Spark closes a coordinator without commit or abort if factory serialization fails,
so Drop must also release what only that write staged. It also creates writers
late, after prerequisite AQE shuffle stages, but input computation can still fail
during write tasks. Constructors must therefore avoid irreversible changes. This
API is batch-only: non-null streaming query IDs and epoch IDs other than `-1` are
rejected.

The [file sink][sink] demonstrates append/truncate using staged JSONL files and
atomic publication of a manifest on a local or shared filesystem. Its documented
single-writer assumption and orphan cleanup limitations are example constraints,
not transaction guarantees provided by the SDK.

## Arrow contract

`PartitionReaderFactory::open` returns a lifetime-bound `PartitionReader`, a
`Box<dyn RecordBatchReader + Send>`. Return fixed-schema batches lazily with bounded
memory, with the columns of `ReaderContext::schema`. Exported arrays own their
buffers independently of the reader: Spark may release a stream before releasing
its previously returned batches.

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
Batches must also respect the nullability of the negotiated schema: as Spark does,
the SDK rejects a null in a column, field, element, or map value that the schema
declares not nullable, unless its parent value is null.

The supported types are those of version 1 of the ABI: signed integers,
float32/64, boolean, strings and binary (including Large and View variants), decimal128
up to precision 38, date32, microsecond timestamps with or without a time zone,
structs, lists, and maps. Spark rejects other Spark types, such as intervals,
variant, time, and the null type, before it passes a schema to the library.
Dictionary encoding, unsigned integers, decimal256, large/fixed-size lists, and
non-microsecond timestamps are outside this prototype's contract. Normalize sliced
arrays when necessary before returning them.

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
expression names and unsupported types must remain unsupported. The SDK parses
each predicate on its own: one that it cannot parse, for example because a later
Spark encodes it differently, reaches `push_filters` as `Predicate::Unknown` and
stays unsupported instead of failing the scan. The SDK preserves literal JSON
values, including exact 64-bit integers, rather than converting all numbers to
floating point. `Predicate::literal_value` decodes a literal by its Spark SQL type:
integers within the range of their type, exact floats, strings, base64 binary,
plain decimal strings, dates, and timestamps. A literal that it does not decode
returns `None`; keep its predicate unsupported. The SDK is not a generic Spark
expression evaluator; connectors remain responsible for Spark SQL semantics.

| Limit result | ABI boolean | Meaning |
| --- | --- | --- |
| `Unsupported` | `false` | No promise; a connector may still cap each partition on its own |
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
pushdown and planning, serialized executor reads, rejection of task contexts shorter
than version 1, cancellation, sequential cross-thread stream callbacks, and Arrow
buffers surviving stream release. This checks the C boundary without requiring
Java; it does not substitute for Spark planner and task-lifecycle testing.

Rust tests additionally exercise the exported function table with option-dependent
capabilities, narrowed and unimplemented declarations, the source name, library
initialization and logging, and panics during capability discovery. They also
cover terminal planning failures, predicates that do not parse, typed literals,
retries after the driver registry is dropped, schema, layout, and nullability
validation, sized ABI structures, task-context lifetimes, integer boundaries,
projections, and deterministic partition plans. Write ABI tests cover typed
factory/message round trips, retained Arrow buffers, cancellation, failed writes and
commits, serialization failures, missing and undecodable messages, and repeated
callbacks. The sink tests cover append/truncate publication, losing attempts,
messages at the wrong partition position, and cleanup after failed publication while
preserving the previous manifest.

## Package and run with Spark

See the [Range example guide][range] for packaging and `spark-submit` commands.
Its `package.py` creates a single-platform `.sparkpkg`; `smoke.py` checks reading,
filtered reads, projection, unfiltered limit pushdown, and analysis-time rejection
of unsupported batch writes against the pinned #59424 framework. The write
checks use failing input behind a shuffle to verify Spark rejects the operation
before computing its data. No full Spark integration is implied by the
standalone Rust/C tests.

The [sink guide][sink] uses the same packager with `--source rust_sdk_sink`. Its
Spark smoke script checks append, overwrite, failed-overwrite cleanup, and
analysis-time rejection for a source instance with writes disabled.

## Scope and limitations

Native sources run trusted code in the driver and executors. The SDK translates
provider errors, with the messages of their sources, and unwinding panics into
C/Arrow errors. Process aborts, allocation failure, double panics during unwinding,
or panics in custom Arrow buffer destructors cannot be recovered. A reader's or
writer's background work must stop before its task context expires. Native memory
is not reserved through Spark's memory manager.

This prototype does not include Structured Streaming, nested projection,
custom metrics, memory accounting, a generic filter evaluator, C++ bindings, or
an ABI extension. The versioned JSON envelope is an SDK implementation detail;
use the same plugin build on the driver and executors. Only the pinned framework
revision is targeted until the native API stabilizes.

[range]: ../../../examples/src/main/rust/native-data-source/README.md
[range-source]: ../../../examples/src/main/rust/native-data-source/src/lib.rs

[sink]: ../../../examples/src/main/rust/native-data-sink/README.md
