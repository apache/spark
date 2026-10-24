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

# C++ native data source SDK prototype

This experimental SDK implements **batch and micro-batch streaming reads, and batch and
streaming writes** over the C ABI proposed in
[Spark PR #59424](https://github.com/apache/spark/pull/59424), pinned to commit
`74aa0dffc53463034fa6549796c4dd599e9705de`. Spark calls the library through the Java Foreign
Function and Memory (FFM) API. This directory contains the SDK and its tests, not the Spark
framework; running the examples in Spark requires that framework and Java 22 or newer on the
driver and executors.

The SDK is header-only C++20 on top of [Arrow C++](https://arrow.apache.org/docs/cpp/) and
[nlohmann/json](https://github.com/nlohmann/json). Each data source library compiles it, so no
C++ type crosses the boundary of a library: Spark sees only the C function table of
[`spark_datasource.h`](include/spark_datasource.h), an unmodified copy of the header of the
pinned commit. The CMake project is not part of Maven/SBT and introduces no C++ requirement for
building Spark. This is an API prototype, not a published library or a compatibility commitment.

The [Range connector][range] under `examples/src/main/cpp/native-data-source` has unit tests, an
independent C client of its library, a packaging helper, and a Spark smoke script. The
[file sink example][sink] is a write-only connector. The CMake project in this directory builds
both, with all the tests.

## Connector API and lifecycle

A connector derives from [`DataSource`](include/spark/native/data_source.h) for construction,
schema discovery, its operations, and optional capability restrictions. Its static `Make`
receives a `CreateInfo` with the name of the data source and its options. Spark can create and
immediately destroy a source just to discover capabilities; schema inference or an operation
need not follow.

Four type aliases name the operations that the source implements. `DataSource` declares each of
them as `Unsupported`, and a source redeclares those of the operations that it implements, with
the method that creates each:

```cpp
class RangeSource : public spark::native::DataSource {
 public:
  using ScanBuilder = RangeScan;         // batch reads, created by NewScan
  using MicroBatchStream = RangeStream;  // streaming reads, by NewMicroBatchStream
  // No BatchWrite or StreamingWrite: the source is read-only.

  static arrow::Result<RangeSource> Make(const spark::native::CreateInfo& info);
  arrow::Result<std::shared_ptr<arrow::Schema>> InferSchema() const override;
  arrow::Result<RangeScan> NewScan(const spark::native::ScanInfo& info) const;
  arrow::Result<RangeStream> NewMicroBatchStream(
      const spark::native::MicroBatchStreamInfo& info) const;
};

SPARK_EXPORT_DATA_SOURCE(RangeSource);
```

The operations are abstract class templates, whose template parameters are the types that the
SDK serializes, and whose virtual methods have the defaults of optional behavior:

* `ScanBuilder<Factory>`: negotiate predicates, limit, and projection, then `Plan()` the scan
  into a `ScanPlan<Factory>`. `DataSource::NewScan` creates an independent builder from a
  `ScanInfo`, which carries the schema to read.
* `PartitionReaderFactory<Partition>`: open one lazy reader, any `arrow::RecordBatchReader`, from
  a partition and a `ReaderContext`, which carries the negotiated schema and the `TaskContext`.
* `MicroBatchStream<Offset, Factory>`: find new data and plan each micro-batch as a
  `ScanPlan<Factory>`; see [Streaming reads and writes](#streaming-reads-and-writes).
* `BatchWrite<Factory>` and `StreamingWrite<Factory>`: coordinate a write on the driver, with a
  `DataWriterFactory<Message>` that opens a `DataWriter<Message>` for each task attempt. The
  write lifecycle is described below.

A write-only connector names only `BatchWrite` or `StreamingWrite`, and implements no read
classes. `SPARK_EXPORT_DATA_SOURCE` checks these declarations when the library compiles: a
source must derive from `DataSource`, each implemented operation must derive from its class
template and have its `New*` method, and the serialized types must be serializable, as described
below.

The inputs of these methods are structs with constructors, so that new ABI fields can reach
connectors without breaking them: `*Info` structs on the driver (`CreateInfo`, `ScanInfo`,
`MicroBatchStreamInfo`, `WriteInfo`, `StreamingWriteInfo`, and `HostInfo`), and `*Context`
structs, which carry the live `TaskContext`, on the executors (`ReaderContext` and
`WriterContext`). `ScanPlan` has a constructor for the same reason.

```text
Driver:
  DataSource::NewScan -> ScanBuilder -> ScanPlan { factory, partitions }
                             |                   |
                      mutable negotiation   immutable serialized descriptors
                                                 |
Executor, per task:                              v
  restored factory + partition + ReaderContext -> arrow::RecordBatchReader -> Arrow C stream
```

As the Rust SDK prototype of PR #59520 does, the SDK maps this API to the driver-only planner:
Spark's `NativeBatchPlanner` calls `partitions`, then `serialize_reader`, and closes the native
planner handle. The SDK calls `Plan()` once, at the first of these calls, and caches the encoded
result, so both use the same factory and partitions without planning twice. A failed negotiation
or planning call keeps its first error, and the SDK destroys a builder that failed, so that a
partially mutated builder is never reused. The executor `read` entry point restores the factory
and the negotiated schema from serialized bytes only, without any driver state, and passes the
schema to the reader in `ReaderContext`.

Factories and partitions carry configuration, not live resources. Connections, files, and open
readers belong in `PartitionReaderFactory::Open`. Include a snapshot identifier in the factory or
the partitions when retrying a task must read the same snapshot. Multiple task attempts open
independent readers.

A reader may keep its `TaskContext` until Spark releases the stream, and a writer until Spark
closes it. Every host provides the task IDs of version 1 of the ABI, so the SDK rejects a shorter
task context. `TaskContext::IsInterrupted()` calls Spark's live cancellation callback; check it
during long operations, and use timeouts for blocking I/O. `TaskContext::ForTesting()`, with its
`With*` methods and `InterruptedBy`, builds a context for tests outside Spark.

`DataSource::Init` runs once in each JVM when Spark loads the library, with the Spark version in
`HostInfo`; a failure prevents Spark from using the library and is logged. `spark::native::Log`,
and `LogError` to `LogTrace`, write to the driver or executor log through Spark from any thread,
and drop messages outside Spark, such as in unit tests. The SDK passes `kInfo` and more severe
messages by default; `SetMaxLogLevel`, for example in `Init`, passes more.

A source that cannot infer its schema, such as a sink of any input, sets
`static constexpr bool kInfersSchema = false;`. The SDK then leaves the native `schema` function
unset, so Spark requires a user-specified schema and reports its standard error without one.

A library that implements several data sources is installed or listed in its package under each
of their names. Spark passes the name in `CreateInfo::name`, so one exported `DataSource` can
choose the source to create; the macro exports a single source type per library.

The headers separate the public API (`data_source.h`, `capabilities.h`, `predicate.h`,
`task_context.h`, and `log.h`) from the implementation under `internal/`: C adapters in
`abi.h`, driver and executor state in `bridge.h`, Arrow checks and the exported stream in
`arrow.h`, serialization in `serde.h`, and the function table in `export.h`.

## Errors and exceptions

Methods return `arrow::Status` or `arrow::Result`, whose message Spark reports as a
`NATIVE_DATA_SOURCE_ERROR`, with the detail of an Arrow error, such as the errno of an I/O
error. The SDK also catches an exception that connector code throws, and reports it as the error
of the call: a `std::exception` with its `what()`. No exception crosses the C ABI. As C++
requires, destructors must not throw: an exception that escapes a destructor terminates the
process, and so the JVM of Spark.

## Source capabilities

The type aliases of a source determine its operations: batch reads with a `ScanBuilder`,
micro-batch reads with a `MicroBatchStream`, batch writes with a `BatchWrite`, and streaming
writes with a `StreamingWrite`. The SDK installs the native callbacks of these operations, leaves
the others null, and derives the default capability set from them, so the macro takes only the
source type. A `BatchWrite` or a `StreamingWrite` opts into truncation with
`static constexpr bool kSupportsTruncate = true;`: the save mode "overwrite" of a batch write,
or the output mode "complete" of a streaming write, which its `New*` method then gets as
`WriteMode::kTruncate`; without it the write only appends. The ABI has one `TRUNCATE` flag for
both, which the SDK declares while a write that supports truncation remains, and the SDK rejects
the truncation of a write that does not support it when Spark creates that write.

Most connectors do not override `capabilities()`. A connector can restrict the supported set for
the current options:

```cpp
spark::native::Capabilities capabilities() const override {
  using spark::native::Capabilities;
  if (readonly_) {
    // Also removes kTruncate, which requires a write.
    return Capabilities::All().Without(Capabilities::kBatchWrite | Capabilities::kStreamingWrite);
  }
  return Capabilities::All();
}
```

The SDK evaluates this once during `create` and keeps only implemented operations, so a source
can narrow its capabilities but never enable one that it does not implement. An empty set
permits schema discovery and close, but rejects operation creation before calling the connector.
The declaration stays fixed for that instance. Predicate, limit, and projection support remain
per-scan contracts.

Spark also calls `create` and `close` during query analysis to discover table capabilities. A
probe uses the table's options, while an operation may override them with query options and
checks capabilities again on a new source instance. Keep `Make` and `capabilities()` lightweight
and safe to destroy immediately; do not assume that `InferSchema` or `NewScan` will follow.

The pinned framework propagates native `BATCH_WRITE` and `TRUNCATE` flags to
`Table.capabilities()`, so a read-only source rejects batch writes during analysis, before Spark
computes their input. Its DSv2 read and streaming-write flags remain present to avoid V1
fallback; the native flags still reject unsupported reads or streaming writes during planning.

## Serialization

The SDK serializes factories, partitions, offsets, and commit messages with nlohmann/json, which
also parses the predicates that Spark pushes. A connector makes a type serializable with
`NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE` or `NLOHMANN_DEFINE_TYPE_INTRUSIVE`, or with its own
`to_json` and `from_json`; serialized types must be default constructible. A serialization
failure, such as an exception in `to_json`, fails the call that needed it.

The SDK wraps the factory, the negotiated schema, as Arrow IPC, and the kind of operation, batch
or streaming, in a versioned JSON envelope. The envelope is an SDK implementation detail: use the
same build of a library on the driver and the executors.

## Batch write lifecycle

```text
Driver:
  DataSource::NewWrite -> BatchWrite
                            | CreateWriterFactory()  ^ Commit(messages) / Abort(messages)
              serialized factory + schema            | typed messages
                            |                        |
Executor, per task attempt: v                        |
                      DataWriterFactory::Open(WriterContext)
                               -> DataWriter::Write(RecordBatch)*
                               -> Commit() / Abort() -> destructor
```

`DataSource::NewWrite(WriteInfo)` validates the input schema and returns an independent driver
coordinator, a `BatchWrite`. Its `CreateWriterFactory` returns the serializable executor
configuration of its task writers, which the SDK creates and encodes with the input schema once,
including a terminal error if either step fails. Restoring an executor writer uses only those
bytes.

Each `DataWriterFactory::Open` receives a `WriterContext` with the input schema and the task, and
returns a task-bound writer. `Write` receives an `arrow::RecordBatch` imported from Spark, whose
Arrow buffers the writer may retain after the call. The SDK checks its schema against the
negotiated input, and releases imported data on failures. Check cancellation during lengthy
writes and use I/O timeouts.

Task `Commit()` returns the message of the factory, the `Message` of the `BatchWrite`. Driver
`Commit` receives one winning message per partition, at the position of its partition; driver
`Abort` receives `std::vector<std::optional<Message>>`, with nothing for a partition whose
message did not arrive or cannot be decoded, so a corrupted message cannot prevent cleanup. A
failed task or driver commit leaves the same object available for abort. A task commit whose
message cannot be serialized counts as failed, so `Abort` can follow a `Commit` that returned a
message. The SDK prevents further writes after a failure or successful commit, caches successful
task commit messages, and avoids repeating successful commit or abort callbacks. Destructors
only release resources. Connectors therefore do not need to track their own phase; the
documentation of `BatchWrite` and `DataWriter` lists the exact call-order guarantees.

Stage data per task attempt and publish it only at driver commit. In particular,
`WriteMode::kTruncate` must leave existing data intact until that commit. Retries and
speculative attempts may stage data for the same partition; publish only the attempts whose
messages Spark passes to commit. Executor loss can prevent cleanup, including after task commit,
so connectors need a strategy for orphaned data. Spark provides no batch query ID: generate and
serialize an operation identity when needed.

Spark closes a coordinator without commit or abort if creating or serializing its factory fails,
so the destructor must also release what only that write staged. Spark also creates writers
late, after prerequisite AQE shuffle stages, but input computation can still fail during write
tasks, so constructors must avoid irreversible changes. The SDK rejects a batch write with an
epoch ID other than `-1`, and the write of a micro-batch with a negative one.

## Streaming reads and writes

Micro-batch streaming reads reuse the executor side of batch scans:

```text
Driver, for each run of a query:
  DataSource::NewMicroBatchStream -> MicroBatchStream
      InitialOffset() / LatestOffset() -> Offset, stored as JSON in the checkpoint
      Plan(start, end) -> ScanPlan { factory, partitions }, for each micro-batch
      Commit(end)
Executor, per task:
  restored factory + partition + ReaderContext -> arrow::RecordBatchReader -> Arrow C stream
```

Spark creates a stream when a query starts or restarts from its checkpoint, and closes it when
the query stops. For each micro-batch, it asks for the latest offset and plans the data after the
end of the previous micro-batch, up to that offset. The executors read the partitions of a
micro-batch exactly as those of a batch scan: the two share `read`, and the SDK tags the
serialized state to restore the right factory. Spark pushes nothing down into a streaming scan,
so the readers return all the columns of the schema.

The SDK serializes the `Offset` of a `MicroBatchStream` as compact JSON on a single line, which
Spark stores in the checkpoint as it is, without the SDK's envelope, and decodes the offsets that
Spark passes back. Spark compares offsets by their JSON to decide whether a micro-batch has new
data, so a position must always serialize to the same JSON: nlohmann/json orders the keys of
objects, but a custom `to_json` must be deterministic too. A restarted query may plan its last
range again, with offsets from its checkpoint that an earlier version of the library may have
written. A failed call, including an exception or an offset that does not decode, stops the
query, so the SDK returns that failure from every later call on the stream.

Streaming writes reuse the task writers of batch writes. Spark creates a `StreamingWrite` for
each micro-batch, from a `StreamingWriteInfo` with the ID of the query, which stays the same
across restarts. The SDK enforces the lifecycle of a `BatchWrite`, passes the epoch ID of the
micro-batch to `Commit` and `Abort`, and gives it to the task writers in
`WriterContext::epoch_id`. Spark may commit an epoch again after a restart, with a new write, so
a write must skip an epoch that is already committed: the query ID and the epoch ID identify the
output of a micro-batch. The [file sink][sink] records the last epoch of each query in its
manifest for this. Continuous processing is not supported.

## Arrow contract

`PartitionReaderFactory::Open` returns a `std::shared_ptr<arrow::RecordBatchReader>`, so a
Parquet reader, a `RecordBatchReader::MakeFromIterator` of a generator, or any other Arrow reader
can be returned directly. Return batches lazily with bounded memory, with the columns of
`ReaderContext::schema`. The schema of the reader is the schema of the stream, which every batch
must have; it may use any supported encoding of a Spark type, such as `large_utf8` for a string.
Exported arrays own their buffers independently of the reader: Spark may release a stream before
releasing its previously returned batches.

Stream callbacks, including release, may run on different threads, but never concurrently.
Readers do not need to be thread-safe or tied to the thread that opened the stream. Stream errors
are kept by the stream, not in a thread-local, and remain readable after a callback moves to
another thread.

The SDK checks the schema of the reader against the negotiated schema before exporting the
stream, and checks each batch: its schema, its `arrow::RecordBatch::Validate()`, because the C
data interface does not carry buffer sizes for Spark to check, and its recursive array offsets.
It compares Spark logical types, ignoring nullability and Arrow metadata, accepting the supported
string and binary encodings and equivalent timestamp time zones. It rejects unsupported types,
schema drift, and nonzero array offsets with explicit errors. It does not silently copy arrays,
cast types, or expand dictionary encoding. Batches must also respect the nullability of the
negotiated schema: as Spark does, the SDK rejects a null in a column, field, element, or map
value that the schema declares not nullable, unless its parent value is null.

The supported types are those of version 1 of the ABI: signed integers, float32/64, boolean,
strings and binary (including the large and view variants), decimal128 up to precision 38,
date32, microsecond timestamps with or without a time zone, structs, lists, and maps. Spark
rejects other Spark types, such as intervals, variant, time, and the null type, before it passes
a schema to the library. Dictionary encoding, unsigned integers, decimal256, large or fixed-size
lists, and other timestamp units are outside this prototype's contract. Normalize sliced arrays
before returning them.

An empty projection must retain the number of rows even with no columns: build such batches with
`arrow::RecordBatch::Make(schema, num_rows, {})`. The [Range implementation][range-source]
demonstrates this together with bounded batches, disjoint partitions, exact integer filtering,
and cooperative cancellation.

## Pushdown contracts

Results correspond to input predicates in order:

| Result | C enum | Connector responsibility |
| --- | --- | --- |
| `kExact` | `SPARK_DS_PREDICATE_COMPLETE` | Return only matching rows under Spark SQL semantics |
| `kPruningOnly` | `SPARK_DS_PREDICATE_PARTIAL` | Skip only data that cannot match |
| `kUnsupported` | `SPARK_DS_PREDICATE_UNUSED` | No evaluation |

The pinned framework rechecks **all** filters, including `kExact`. Unknown expression names and
unsupported types must remain unsupported. The SDK parses each predicate on its own: one that it
cannot parse, for example because a later Spark encodes it differently, reaches `PushFilters` as
a `Predicate` of kind `kUnknown` and stays unsupported instead of failing the scan. The SDK keeps
literal JSON values, including exact 64-bit integers, rather than converting all numbers to
floating point. `Predicate::literal_value()` decodes a literal by its Spark SQL type into a
`LiteralValue`: integers within the range of their type, exact floats, strings, base64 binary,
plain decimal strings, dates, and timestamps. A literal that it does not decode returns nothing;
keep its predicate unsupported. The SDK is not a generic Spark expression evaluator; connectors
remain responsible for Spark SQL semantics.

| Limit result | ABI boolean | Meaning |
| --- | --- | --- |
| `kUnsupported` | `false` | No promise; a connector may still cap each partition on its own |
| `kGlobal` | `true` | The entire scan returns at most the limit |

Spark retains its final limit and currently pushes a limit only when the scan has no filters.
The SDK also declines limit pushdown after any predicate that is not `kExact`. Limiting rows
before a residual filter can omit matching rows, even when Spark retains its final limit.

Projection returns true only when the reader produces exactly the requested top-level columns in
order. Keep internally needed predicate columns even if they are absent from the output
projection. All pushdown methods have conservative defaults so a connector can add support
incrementally.

## Build and test without Spark

The SDK requires a C++20 compiler, CMake 3.16 or newer, Arrow C++ 15 or newer, for the view
types, and nlohmann/json 3.10 or newer, which `find_package` locates, for example from
`CMAKE_PREFIX_PATH`. It was tested with GCC 10.5 and Arrow C++ 26 on Linux x86-64. From the
repository root:

```bash
cmake -S sql/native/cpp -B sql/native/cpp/build
cmake --build sql/native/cpp/build
(cd sql/native/cpp/build && ctest --output-on-failure)
```

`SPARK_NATIVE_BUILD_EXAMPLES` and `SPARK_NATIVE_BUILD_TESTS`, both on by default, control what
the project builds. `spark_add_native_data_source(<name> <sources>)` builds the library of a data
source, such as `libspark_datasource_<name>.so` on Linux, with hidden symbols and a version
script (an exported symbol list on macOS), so that it exports only `spark_ds_init`. Format the
code with
`clang-format --style=file:sql/native/cpp/.clang-format`.

The tests need no test framework. The SDK tests cover error and exception conversion, sized ABI
structures, task contexts, capabilities, predicates and typed literals, serialization, and the
exported Arrow stream: early release, schema and nullability checks of nested data, layouts,
and cross-type encodings. Each of four more test executables exports a data source and calls
its actual function table through `spark_ds_init`:

* `library_test`: initialization and its failure, logging through the host and its levels, the
  source name, and narrowed, unimplemented, and failing capability declarations.
* `read_test`: cached plans and planning failures, discarded builders after failed negotiation,
  predicates that do not parse, limits after residual filters, independent planners, executor
  retries from serialized values only, and the rejection of a reader that does not return the
  negotiated schema.
* `batch_write_test`: factory and message round trips, retained Arrow buffers, cancellation,
  failed writes and commits, factory creation and serialization failures, missing and
  undecodable messages, repeated callbacks, and the rejection of streaming epochs.
* `streaming_test`: single-line JSON offsets, the state of each planned micro-batch, reads that
  restore batch or micro-batch factories of different types, exceptions that fail a stream, and
  the query and epoch IDs of streaming writes and their task writers.

The Range example adds its unit tests and `range_c_abi_smoke`, a C client that compiles against
the C header only, loads the actual library with `dlopen`, and checks it as Spark would, without
a JVM; the sink example adds its own tests. These checks do not substitute for Spark planner and
task-lifecycle testing.

## Package and run with Spark

See the [Range example guide][range] for packaging and `spark-submit` commands. Its `package.py`
creates a single-platform `.sparkpkg`; `smoke.py` checks reading, filtered reads, projection,
unfiltered limit pushdown, a streaming read, and analysis-time rejection of unsupported batch
writes against the pinned #59424 framework. The [sink guide][sink] uses the same packager with
`--source cpp_sdk_sink`. Its smoke script checks append, overwrite, failed-overwrite cleanup,
analysis-time rejection for a source instance with writes disabled, and the epochs of a
streaming write across restarts.

The libraries link Arrow C++ dynamically, so the driver and executors must find the Arrow
libraries that they were built with, as installed or through the run path of the build. To ship
a self-contained package, link Arrow statically.

## Scope and limitations

Native sources run trusted code in the driver and executors. The SDK translates errors and
exceptions into C and Arrow errors. Process aborts, exceptions that escape destructors, or
crashes in connector code cannot be recovered. A reader's or writer's background work must stop
before its task context expires. Native memory is not reserved through Spark's memory manager.

This prototype does not include continuous processing, admission control for streaming reads,
nested projection, custom metrics, memory accounting, a generic filter evaluator, or an ABI
extension. Only the pinned framework revision is targeted until the native API stabilizes.

[range]: ../../../examples/src/main/cpp/native-data-source/README.md
[range-source]: ../../../examples/src/main/cpp/native-data-source/range_source.cc
[sink]: ../../../examples/src/main/cpp/native-data-sink/README.md
