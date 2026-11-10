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

This experimental SDK implements **batch and micro-batch streaming reads, with admission
control, and batch and streaming writes** over version 1 of the C ABI of native data sources,
[`spark_datasource.h`][header], which Spark 4.4.0 introduces. Spark calls the library through
the Java Foreign Function and Memory (FFM) API, so running a library in Spark requires Java 22
or newer on the driver and executors. The [guide of native data sources][guide] describes the
ABI.

The Rust and C tests run independently of Spark and Java. The workspace is not
part of Maven/SBT and introduces no Rust requirement for building Spark. This is
an API prototype, not a compatibility commitment.

The crate, `spark-datasource`, after `spark_datasource.h`, is not published yet: depend on it
with a path dependency on a Spark source tree, as the examples do, until SPARK-61307 publishes
it to crates.io. It is versioned on its own, from 0.x, and may need a breaking release in any
release of Spark: for a new major version of Arrow, whose types it exposes, or for a new kind
of operation, which adds an associated type to `DataSource` that every connector must name.
Growth within an operation, such as a method with a default implementation or a field of an
info struct, does not break connectors. The crate supports version 1 of the ABI, which Spark
supports from 4.4.0 in all the releases of Spark 4.

The API uses the types of Arrow 58, such as `RecordBatch`, `SchemaRef`, and `ArrowError`, so
a connector must use the same major version of Arrow: the SDK re-exports the crates as
`spark_datasource::arrow_array` and `spark_datasource::arrow_schema`. Upgrading the
major version of Arrow is a breaking change of the SDK.

The [Range connector][range] is a separate workspace member under
`examples/src/main/rust/native-data-source`, with a C integration test, packaging
helper, and Spark smoke script. The [file sink example][sink] is a separate write-only
connector, and the [Parquet example][parquet] a batch-read connector on the Parquet reader of
arrow-rs. All crates share this directory's lockfile and `target` directory. Cargo commands
select the SDK by default; use `--workspace` to include the examples.

## Connector API and lifecycle

A connector implements [`DataSource`](src/lib.rs) for construction, schema discovery,
its operations, and optional capability restrictions. `DataSource::new` receives a
`CreateInfo` with the name of the data source and its options. Spark can create and
immediately drop a source just to discover capabilities; schema inference or an
operation need not follow.

Four associated types name the operations that the source implements, and
`Unsupported` stands for one that it does not implement:

```rust
impl DataSource for RangeSource {
    type ScanBuilder = RangeScan; // batch reads, created by new_scan
    type MicroBatchStream = RangeStream; // streaming reads, by new_micro_batch_stream
    type BatchWrite = Unsupported; // no batch writes
    type StreamingWrite = Unsupported; // no streaming writes
    // new, schema, new_scan, and new_micro_batch_stream
}
```

Read support uses three pieces:

* `DataSource::new_scan`: create an independent scan builder from a `ScanInfo`, which
  carries the schema to read.
* `ScanBuilder`: negotiate the domains of the filtered columns, the limit, and the columns to
  read, then consume itself in `plan()` to return a typed `ScanPlan<Factory>`.
* `PartitionReaderFactory`: serialize executor configuration and open one lazy reader,
  an iterator of Arrow batches, from a typed partition and a `ReaderContext`, which
  carries the schema to read and a borrowed `TaskContext`.

Streaming reads reuse the executor side: `DataSource::new_micro_batch_stream` creates a
`MicroBatchStream` from a `MicroBatchStreamInfo`, which carries the columns to read and the
location of the checkpoint of the source, and the stream plans each micro-batch as a
`ScanPlan` of a `PartitionReaderFactory`; see
[Streaming reads and writes](#streaming-reads-and-writes).

Write support is independent: a write-only connector sets `ScanBuilder` and
`MicroBatchStream` to `Unsupported` and implements `DataSource::new_write`, `BatchWrite`,
`DataWriterFactory`, and `DataWriter`. Streaming writes add
`DataSource::new_streaming_write` and `StreamingWrite`, with the same task writers. The
write lifecycle is described below.

The inputs of these methods are `#[non_exhaustive]` structs, so that new ABI fields can
reach connectors without breaking them: `*Info` structs on the driver (`CreateInfo`,
`ScanInfo`, `MicroBatchStreamInfo`, `LatestOffsetInfo`, `MicroBatchInfo`, `CommitOffsetInfo`,
`WriteInfo`, `CommitInfo`, `AbortInfo`, `StreamingWriteInfo`, `StreamingCommitInfo`, and
`StreamingAbortInfo`), `*Context` structs, which carry the live `TaskContext`, on the
executors (`ReaderContext` and `WriterContext`), and `HostInfo`, which `DataSource::init`
receives in every JVM that loads the library, executors included. `ScanPlan` has a
constructor for the same reason. The methods whose function of the header passes nothing but
the size of its info struct yet take no struct: `ScanBuilder::plan`,
`MicroBatchStream::initial_offset`, and the `commit` and `abort` of `DataWriter`. A field
added there would be a breaking change of the SDK, which its 0.x version allows.

The factory and partition types implement `Serialize` and `Deserialize`, as the commit
messages of writes do. The SDK encodes them with serde in MessagePack, a self-describing binary
format in which structs are maps with the names of their fields, so the serde attributes that
need them, such as `untagged`, `flatten` and `default`, work, including on byte vectors, and
floats, including NaN and the infinities, round-trip as they are. Offsets are JSON instead,
since Spark stores them as text: see [Streaming reads and writes](#streaming-reads-and-writes).
Connections, pointers, and open readers remain in their creating process.
Include a snapshot identifier in the factory or partitions when retrying a task
must read the same snapshot. Several attempts of a task may open readers of the same
partition at the same time, so a reader must not take a resource that another attempt needs
exclusively, and a `ScanBuilder`, which Spark may drop late, must not hold one until Drop.

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

For a native source, `NativeBatchPlanner` pushes the domains of the filtered columns, the
limit, and the columns to read, each at most once and in this order, then calls `partitions`
once, which returns the partitions and the state of the reader, and closes the native planner
handle. The Rust `ScanBuilder::plan()` consumes its builder in that call, and the SDK encodes the
factory as the state. After a call fails, Spark calls no other function on the handle than its
close function. Batch planner creation remains lazy for scans that do not read in batch.

The JVM's `NativePartitionReaderFactory` carries only the library location,
source name, read schema, and serialized state. The native executor `read` entry
point gets the kind of the read, batch or micro-batch, and the schema to read with
the state and the partition, so it restores the Rust factory from those bytes without
creating or accessing the driver registry, and passes the schema to the reader in
`ReaderContext`. Closing the driver handles cannot invalidate factory or partition
bytes already copied to Spark, including for retried tasks.

The reader owns its executor resources and cannot borrow its temporary factory.
It may borrow `TaskContext` until the returned Arrow stream is released. Every host
provides the task IDs of version 1 of the ABI, so the SDK rejects a task context shorter
than in version 1, as it rejects the info structures and the read limit that Spark passes to
the calls when they are shorter. `is_interrupted()` checks
Spark's live cancellation callback, not a snapshot of its initial value; for a reader, it
is also true once the task ended. Check it during long operations; blocking I/O still
requires timeouts. `TaskContext::for_testing()`, with its `with_*` methods and
`interrupted_by`, builds a context with chosen IDs and an optional interruption flag for
tests outside Spark.

Export the source from the connector's `cdylib` crate:

```rust
spark_datasource::export_data_source!(RangeSource);
```

The macro exports only `spark_ds_init` and returns Spark's versioned function
table. The connector uses safe Rust traits without C pointers. Rust trait objects
never cross the binary boundary between libraries.

`DataSource::init` runs once in each JVM when Spark loads the library, with the
Spark version in `HostInfo`; a failure prevents Spark from using the library and is
logged. The macros of the `log` crate, which the SDK re-exports as
`spark_datasource::log`, write to the driver or executor log through Spark from any
thread, including in the library's dependencies, and drop messages outside Spark, such
as in unit tests. The SDK sets the level of the `log` crate to that of the log of Spark,
which it asks with `SparkDsHost.is_log_enabled` when Spark loads the library and when an
operation or a task starts, so it also follows a level that changes while Spark runs.

A source that cannot infer its schema, such as a sink of any input, sets
`DataSource::INFERS_SCHEMA` to false. The SDK then leaves the native `schema`
function unset, so Spark requires a user-specified schema and reports its standard
error without one.

A library that implements several data sources is installed or listed in its
package under each of their names. Spark passes the name in `CreateInfo::name`, so
one exported `DataSource` can choose the source to create; the macro exports a
single Rust type per library. Its four associated types are shared by all of its sources:
when they have different scan builders, factories, partitions, offsets, or messages, the
library wraps them in enums whose variants delegate to the types of each source.

The implementation separates public connector traits, driver handle state in
`bridge.rs`, Arrow ownership in `stream.rs`, the writers in `write.rs`, C bindings in
`abi.rs`, and the function table in `export.rs`. The macro reaches them through a
hidden `__private` module, which is not part of the API.

## Source capabilities

The associated types of `DataSource` determine the operations of the source: batch
reads unless `ScanBuilder` is `Unsupported`, micro-batch reads unless `MicroBatchStream`
is, batch writes unless `BatchWrite` is, and streaming writes unless `StreamingWrite` is.
The SDK installs the native callbacks of these operations, leaves the others null, and
derives the default capability set from them, so the macro takes only the source type.
A `BatchWrite` or a `StreamingWrite` opts into truncation with
`const SUPPORTS_TRUNCATE: bool = true`, which declares `BATCH_TRUNCATE`, for the save mode
"overwrite" of a batch write, or `STREAMING_TRUNCATE`, for the output mode "complete" of a
streaming write. Its `new_*` method then gets `WriteMode::Truncate` for them; without it
the write only appends. Every stream also declares the native capability
`SPARK_DS_ADMISSION_CONTROL`, whose default read limit, all the available data, reads as
a stream without it does. The macro also rejects a library built with `panic = "abort"`,
because the SDK can report a panic as an error only when it unwinds.

Most connectors do not implement `capabilities`. A connector can restrict the
supported set for the current options by overriding this `DataSource` method:

```rust
fn capabilities(&self) -> Capabilities {
    if self.read_only {
        // The SDK also drops the truncations, which require their writes.
        Capabilities::all().without(Capabilities::BATCH_WRITE | Capabilities::STREAMING_WRITE)
    } else {
        Capabilities::all()
    }
}
```

The SDK evaluates this once during `create` and keeps only implemented operations,
so a source can narrow its capabilities but never enable one that it does not
implement, and keeps each truncation only with its write.
An empty set permits schema discovery and close, but rejects operation creation
before calling the connector. The declaration stays fixed for that instance.
The default declaration is `Capabilities::all()`, every operation of the SDK. Limit and
column pruning remain per-scan contracts.

Spark also calls `create` and `close` during query analysis to discover table
capabilities. A probe uses the table's options, while an operation may override
them with query options and checks capabilities again on a new source instance.
Keep `DataSource::new` and `capabilities` lightweight and safe to drop
immediately; do not assume that `infer_schema` or `new_scan` will follow. Executor
resources belong in `PartitionReaderFactory::open`.

Spark propagates the native `BATCH_WRITE` and `BATCH_TRUNCATE` flags to
`Table.capabilities()`, as `BATCH_WRITE` and `TRUNCATE`, so a read-only source rejects
batch writes during analysis, before Spark computes their input. Its DSv2 read and
streaming-write flags remain present to avoid V1 fallback; the native flags still reject
unsupported reads or streaming writes during planning. Thus the DSv2 table's flags are not
a literal copy of the native mask declared by the Rust source, and an unsupported
streaming read or write fails when its query starts or plans its first micro-batch, after
`start()` returns.

## Batch write lifecycle

```text
Driver:
  DataSource::new_write -> BatchWrite
                             | new_writer_factory()  ^ commit(CommitInfo) / abort(AbortInfo)
                    serialized factory               | typed messages
                             |                       |
Executor, per task attempt:  v                       |
                       DataWriterFactory::open(WriterContext)
                                -> DataWriter::write(RecordBatch)*
                                -> commit() / abort() -> Drop
```

`DataSource::new_write(WriteInfo)` validates the input schema and returns an
independent driver coordinator, a `BatchWrite`. Its `new_writer_factory` returns the
serializable executor configuration of its task writers, which the SDK encodes as the
state of the writer that native `create_writer` returns. Restoring an executor writer
uses only those bytes and the schema that Spark passes with them, without creating a
driver registry.

Each `DataWriterFactory::open` receives a `WriterContext` with the input schema and
the task, and returns a task-bound writer. `write` takes ownership of a
`RecordBatch`, so its Arrow buffers can be retained after the callback, until the writer
is dropped: they belong to the data writer of Spark, and the ABI requires the library to
release them before Spark closes the writer. Spark cannot free a batch that is still
retained when it closes the writer: it logs an error, and the memory stays allocated until
the batch is dropped. The SDK
moves the incoming Arrow array, which has the schema that the data writer got, and
releases imported resources on failures. Input slices need not have zero offsets;
the zero-offset restriction applies to arrays exported to Spark by readers. When Spark
writes the batches of a native scan without converting them to rows, it copies their
values as the scan returned them, without the checks of a conversion, such as the
precision of a decimal. Check cancellation during lengthy writes and use I/O timeouts.

Task `commit()` returns a typed, serde-serializable message, the `Message` of the
`BatchWrite`. Driver `commit` receives a `CommitInfo` with one winning message per partition,
at the position of its partition; driver `abort` receives an `AbortInfo` whose messages are
`Option<Message>`, with `None` for a partition whose message did not arrive or cannot be
decoded, so a corrupted message cannot prevent cleanup. A failed task or driver commit leaves
the same object available for abort.
A task commit whose message cannot be serialized counts as failed, so `abort` can
follow a `commit` that returned a message. Spark calls each method at most once, never
writes after a failure or a commit, and never aborts a successful commit, as the header
documents; the `# Lifecycle` sections of `BatchWrite` and `DataWriter` list the exact
call-order guarantees. Drop only releases resources. The SDK drops a driver coordinator right
after a successful `commit` or after `abort`, so the write of a micro-batch is dropped before
Spark creates that of the next one. A write that Spark neither commits nor aborts, and a
`ScanBuilder` that Spark never plans, are dropped when Spark closes their handles, which may
be late and on another thread.

The callback order does not determine the outcome of a remote transaction. A destination
can publish a write successfully but lose its acknowledgement, so `commit` returns an error
and Spark calls `abort`. Connectors must track that outcome with a durable operation identity
and make commit and cleanup idempotent. After an uncertain driver commit, `abort` and Drop
must preserve data that the destination may have published. Use the operation identity to
resolve the outcome before deleting that data; if it remains unknown, leave the data for
later recovery. For streaming,
record the query ID and epoch ID atomically with publication, so a restarted query can
recognize a committed epoch even if the earlier write did not receive its acknowledgement.
The file sink avoids this ambiguity during normal execution by doing no fallible work after
its manifest rename; a remote sink needs its own outcome checks and recovery protocol.

Stage data per task attempt and publish it only at driver commit. In particular,
`WriteMode::Truncate` must leave existing data intact until that commit. Retries
and speculative attempts may stage data for the same partition; publish only the
attempts whose messages Spark passes to commit. Executor loss can prevent cleanup,
including after task commit, so connectors need a strategy for orphaned data.
Spark provides no batch query ID: generate and serialize an operation identity
when needed. Task-attempt IDs alone are not globally unique across applications.

When `new_writer_factory` or the serialization of its factory fails, `create_writer` fails,
and when Spark cannot copy the serialized factory, for example when it is too large, Spark
fails the write. Either way, the SDK drops the coordinator without commit or abort, so
Drop must also release what only that write staged. Spark also creates writers late, after
prerequisite AQE shuffle stages, but input computation can still fail during write tasks.
Constructors must therefore avoid irreversible changes. The epoch ID tells the task writers
of batch writes, whose epoch ID is `-1`, from those of micro-batches.

The [file sink][sink] demonstrates append/truncate using staged JSONL files and
atomic publication of a manifest on a local or shared filesystem. Its documented
single-writer assumption and orphan cleanup limitations are example constraints,
not transaction guarantees provided by the SDK.

## Streaming reads and writes

Micro-batch streaming reads reuse the executor side of batch scans:

```text
Driver, for each run of a query:
  DataSource::new_micro_batch_stream -> MicroBatchStream
      initial_offset(), default_read_limit()
      latest_offset(LatestOffsetInfo) -> Offset, stored as JSON in the checkpoint
      plan(MicroBatchInfo) -> ScanPlan { factory, partitions }, for each micro-batch
      commit(CommitOffsetInfo)
Executor, per task:
  restored factory + partition + ReaderContext -> Arrow reader -> Arrow C Stream
```

Spark creates a stream when a query starts or restarts from its checkpoint, and closes it
when the query stops. It keeps the initial offset in the checkpoint, and asks for it only
when the checkpoint does not have it yet. For each micro-batch, it asks for the latest
offset after the end of the previous micro-batch, and plans the data up to that offset.
The executors read the partitions of a micro-batch exactly as those of a batch scan: the
two share `read`, which gets the kind of the read. `MicroBatchStreamInfo::schema` has the
columns to read, which are all the columns of the schema until Spark prunes the columns of
streaming scans.

`MicroBatchStream::default_read_limit` limits the data of each micro-batch, for example
to `ReadLimit::MaxRows` from an option of the source; the default reads all the available
data. Spark passes the limit to `latest_offset` in `LatestOffsetInfo::limit`, and with
`Trigger.AvailableNow` and a `ReadLimit::MaxRows` limit, the end of the run in
`LatestOffsetInfo::run_end`, which the latest offset must not go beyond. Spark also asks
without a limit for the progress of the query, so a stream does not count the calls of
`latest_offset` as micro-batches.

The SDK serializes `MicroBatchStream::Offset` with serde as compact JSON on a single line,
which Spark stores in the checkpoint as it is, without the SDK's versioned envelope, and
decodes the offsets that Spark passes back. Later versions of the SDK will not wrap or
re-encode offsets: the format of an offset is that of the connector's `Offset` type, which
outlives any build of the library. Spark compares offsets by their JSON to decide
whether a micro-batch has new data, so a position must always serialize to the same JSON:
use structs and `BTreeMap`, not `HashMap`. A restarted query may plan its last range again,
with offsets from its checkpoint that an earlier version of the library may have written.
When that version had a different `Offset` type, for example without a field that the new
one adds with `#[serde(default)]`, the same position encodes to different JSON, so Spark
runs one micro-batch without rows. A failed call, including a panic or an offset that does
not decode, stops the query: Spark makes no other call on the stream before it closes it.

Streaming writes reuse the task writers of batch writes. Spark creates a `StreamingWrite`
for each micro-batch, from a `StreamingWriteInfo` with the ID of the query, which stays the
same across restarts. It follows the lifecycle of a `BatchWrite`; the SDK passes the epoch ID
of the micro-batch to `commit` and `abort` in `StreamingCommitInfo` and `StreamingAbortInfo`,
and gives it to the task writers in `WriterContext::epoch_id`. Spark may commit an epoch
again after a restart, with a new
write, so a write must skip an epoch that is already committed: the query ID and the epoch
ID identify the output of a micro-batch. A restarted query may also abort an epoch that an
earlier run committed, so an abort removes only what its own task writers wrote. The
[file sink][sink] records the last epoch of each query in its manifest for this. Continuous
processing is not supported.

## Arrow contract

`PartitionReaderFactory::open` returns a lifetime-bound `PartitionReader`, a boxed
`Send` iterator of `Result<RecordBatch, ArrowError>`, as any Arrow `RecordBatchReader`
is. Return batches lazily with bounded memory, with the columns of
`ReaderContext::schema`. A batch may also have other columns, such as all the columns of
the source: the SDK keeps those of the schema to read, by name and in its order, since
Spark rejects a stream with other columns. The first batch determines the Arrow schema of
the stream, which every later batch must have; a reader without batches exports the
schema to read. Exported arrays own their buffers independently of the reader: Spark may
release a stream before releasing its previously returned batches.

Stream callbacks, including release, may run on different threads, but never
concurrently. Readers must be `Send`; they do not need to be `Sync` or tied to
the thread that opened the stream. Stream errors are owned by the stream and
remain readable after a callback moves to another thread.

The SDK validates the schema to read before exporting a reader and checks each
batch's schema and recursive array offsets. It compares Spark logical types,
ignoring nullability and Arrow metadata, accepting the supported string/binary
encodings and equivalent timestamp time zones. It rejects unsupported types, schema
drift, and nonzero array offsets with explicit errors. It does not silently copy
arrays, cast types, or expand dictionary encoding. Batches must also respect the
nullability of the schema to read: as Spark does, the SDK rejects a null in a column,
field, element, or map value that the schema declares not nullable, unless its parent
value is null. The values of a decimal column must fit in the precision of its type: Spark
checks them when it converts them to rows, which a native scan written to a native sink
skips, so the SDK checks them on read, except under a null parent. After a callback of the
stream fails, every later call fails with the same error, so an engine that reads a stream
after an error cannot take a failed partition as complete.

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
batches, disjoint partitions, admission control, and cooperative cancellation.

## Pushdown contracts

| Limit result | ABI boolean | Meaning |
| --- | --- | --- |
| `Unsupported` | `false` | No promise; a connector may still cap each partition on its own |
| `Global` | `true` | The entire scan returns at most the limit |

Spark retains its final limit and pushes a limit only when the query does not filter
the rows of the scan, since it filters them after reading.

`ScanBuilder::prune_columns` gets the top-level columns that the query needs, in order,
so that the scan can plan to read only them. The readers get them as the schema to read
whether or not the scan implements it, and return exactly them, so a connector can add
pruning to the planning of its scans incrementally.

`ScanBuilder::push_domains` gets the domains of the columns that the filters of the query
constrain: for each column, whether null and NaN are in its domain, and the ranges of its other
values, sorted and without overlaps, or none for all of them. A scan can skip the data that has
no value in the domain of a column, such as a file or a row group whose statistics exclude every
value of the domain, but never a row whose values are all in their domains. Spark evaluates all
the filters after reading anyway, so the rows of a scan need not match them. The method returns
whether the scan uses each domain: Spark shows the filters of the used domains as pushed in the
formatted plan of the query. The default uses none, so a connector can ignore the method.

| `Domain` field | ABI field | Meaning |
| --- | --- | --- |
| `path` | `path` | The names of the column and of its fields in structs, from the indices of the ABI |
| `null_allowed` | `null_allowed` | Whether null is in the domain; never for a field of a struct |
| `nan_allowed` | `nan_allowed` | Whether NaN is in the domain, for `float` and `double` columns |
| `ranges` | `ranges` | The ranges of the other values, with `Bound` limits, or `None` for all |

The SDK resolves the path of a domain in `ScanInfo::schema`, the schema that `create_reader`
got, because Spark prunes the columns after it pushes the domains, so their indices in the
schema to read of the readers may differ. It converts the bounds of the ranges, an Arrow struct
array, to `Value`s, which hold the types that version 1 passes domains for, and skips a domain of
another type, which a later version of Spark may pass, as unused. `Domain`, `ValueRange` and
`Value` are serializable, so that a reader factory can carry the domains to the executors.
Values compare as Spark compares them: compare floats with `<` and `==`, under which -0.0 equals
0.0, not with `f64::total_cmp`, strings by their UTF-8 bytes, and binary values as unsigned bytes.

## Build and test without Spark

Use Rust 1.85 or newer, the `rust-version` of the workspace, which Arrow 58 requires, and a
C compiler. The C integration test uses `dlopen` and pthreads on Linux or macOS. From this
directory:

```bash
cargo test --workspace --locked
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo fmt --all -- --check
```

The SDK's layout test compiles C assertions against [Spark's header][header], comparing the
size, alignment, and every field offset of all native ABI structs with the values computed
by Rust, including write-side structs and the function table. It uses the same field list as
the SDK's compile-time layout assertions, but compares actual layouts rather than copied
numeric constants, so a header-only change can fail the test. The test runs on Linux and
macOS, accepts `CC` with a launcher or leading arguments, and adds no build-time C dependency
for connectors.

The C harness loads the example dynamic library that `cargo test` builds. It compiles
against [Spark's header][header], as a library that copies it would. It calls the actual
function table and validates capability-only source discovery and optional slots,
independent driver planners, pushdown and planning, serialized executor reads, rejection of
structures shorter than in version 1, cancellation, sequential cross-thread stream
callbacks, Arrow buffers surviving stream release, micro-batch reads with their offsets,
including an offset that does not decode, and admission control. This checks the C
boundary without requiring Java; it does not substitute for Spark planner and task-lifecycle
testing.

Rust tests additionally exercise the exported function table with option-dependent
capabilities, narrowed and unimplemented declarations, the source name, library
initialization, and logging that follows the level of the host, and panics during
capability discovery. They also cover planning failures, retries after the driver registry
is dropped, schema, layout, and nullability validation, the columns of the schema to read
that the SDK keeps from a batch, sized ABI structures, task-context lifetimes, integer
boundaries, projections, deterministic partition plans, and the domains that Spark pushes: the
names of their columns, the values and bounds of each type, the ranges that the SDK moves to own,
invalid paths, and the number of results. Write ABI tests cover typed
factory/message round trips, retained Arrow buffers, cancellation, failed writes and
commits, factory creation and serialization failures, missing and undecodable messages,
and the epoch IDs that select the task writers. Streaming ABI tests cover single-line JSON
offsets, the state of each planned micro-batch, reads that restore batch or micro-batch
factories of different types, read limits and the end of a run, the checkpoint location,
and the query and epoch IDs of streaming writes and their task writers. The sink tests
cover append/truncate publication, losing attempts, messages at the wrong partition
position, cleanup after failed publication while preserving the previous manifest, and
streaming epochs that are published once.

## Package and run with Spark

See the [Range example guide][range] for packaging and `spark-submit` commands.
Its `package.py` creates a single-platform `.sparkpkg`; `smoke.py` checks reading,
filtered reads, projection, unfiltered limits, streaming reads with and without a
limit on their micro-batches, and analysis-time rejection of unsupported batch writes. The
write checks use failing input behind a shuffle to verify Spark rejects the operation
before computing its data. No full Spark integration is implied by the standalone Rust/C
tests.

The [sink guide][sink] uses the same packager with `--source rust_sdk_sink`. Its
Spark smoke script checks append, overwrite, failed-overwrite cleanup,
analysis-time rejection for a source instance with writes disabled, and the epochs of a
streaming write across restarts.

The [Parquet guide][parquet] uses the same packager with `--source rust_sdk_parquet`. Its smoke
script checks that queries over files that Spark writes return what Spark's own Parquet reader
returns, with and without its row filter, which evaluates the pushed predicates on the batches
that it decodes, and its benchmark script compares the two readers.

## Scope and limitations

Native sources run trusted code in the driver and executors. The SDK translates
provider errors, with the messages of their sources, and unwinding panics into
C/Arrow errors. Process aborts, allocation failure, double panics during unwinding,
or panics in custom Arrow buffer destructors cannot be recovered. A reader's or
writer's background work must stop before its task context expires. Native memory
is not reserved through Spark's memory manager.

This prototype does not include continuous processing, filters that a source evaluates in
place of Spark, nested projection, custom metrics, memory accounting, or C++ bindings, and does
not extend the ABI. The versioned MessagePack envelope of the serialized state, partitions and
messages is an SDK implementation detail: Spark loads the same build of the library on the
driver and the executors, which it checks with its checksum.

[header]: ../../core/src/main/resources/org/apache/spark/sql/datasource/spark_datasource.h
[guide]: ../../../docs/sql-native-data-sources.md
[range]: ../../../examples/src/main/rust/native-data-source/README.md
[range-source]: ../../../examples/src/main/rust/native-data-source/src/lib.rs

[sink]: ../../../examples/src/main/rust/native-data-sink/README.md

[parquet]: ../../../examples/src/main/rust/native-parquet/README.md
