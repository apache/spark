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

This experimental SDK implements **batch and micro-batch streaming reads, with admission
control, and batch and streaming writes** over version 1 of the C ABI of native data sources,
[`spark_datasource.h`][header], which Spark 4.4.0 introduces. Spark calls the library through the
Java Foreign Function and Memory (FFM) API, so running a library in Spark requires Java 22 or
newer on the driver and executors. The [guide of native data sources][guide] describes the ABI.

The SDK is header-only C++20 on top of [Arrow C++](https://arrow.apache.org/docs/cpp/) and
[nlohmann/json](https://github.com/nlohmann/json). Each data source library compiles it, so no
C++ type crosses the boundary of a library: Spark sees only the C function table of
[`spark_datasource.h`][header], which the CMake project takes from the source tree of Spark. A
library built outside of it copies the header, or uses the copy in the include directory of a
Spark distribution. The CMake project is not part of Maven/SBT and introduces no C++ requirement
for building Spark. This is an API prototype, not a published library or a compatibility
commitment.

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

* `ScanBuilder<Factory>`: negotiate the limit and the columns to read, then `Plan()` the scan
  into a `ScanPlan<Factory>`. `DataSource::NewScan` creates an independent builder from a
  `ScanInfo`, which carries the schema to read.
* `PartitionReaderFactory<Partition>`: open one lazy reader, any `arrow::RecordBatchReader`, from
  a partition and a `ReaderContext`, which carries the schema to read and the `TaskContext`.
* `MicroBatchStream<Offset, Factory>`: find new data, within a read limit, and plan each
  micro-batch as a `ScanPlan<Factory>`; see
  [Streaming reads and writes](#streaming-reads-and-writes).
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
`MicroBatchStreamInfo`, `LatestOffsetInfo`, `WriteInfo`, and `StreamingWriteInfo`), `*Context`
structs, which carry the live `TaskContext`, on the executors (`ReaderContext` and
`WriterContext`), and `HostInfo`, which `DataSource::Init` gets in every JVM, executors included.
`ScanPlan` has a constructor for the same reason.

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
Spark's `NativeBatchPlanner` pushes the limit and the columns to read, each at most once and in
this order, then calls `partitions` once, which returns the partitions and the state of the
reader, and closes the native planner handle. The SDK calls `Plan()` in that call, which consumes
the builder, and encodes the factory as the state. After a call fails, Spark calls no other
function on the handle than its close function. The executor `read` entry point gets the kind of
the read, batch or micro-batch, and the schema to read with the state and the partition, so it
restores the factory from serialized bytes only, without any driver state, and passes the schema
to the reader in `ReaderContext`.

Factories and partitions carry configuration, not live resources. Connections, files, and open
readers belong in `PartitionReaderFactory::Open`. Include a snapshot identifier in the factory or
the partitions when retrying a task must read the same snapshot. Multiple task attempts open
independent readers.

A reader may keep its `TaskContext` until Spark releases the stream, and a writer until Spark
closes it. Every host provides the task IDs of version 1 of the ABI, so the SDK rejects a shorter
task context, as it rejects any structure shorter than its layout in version 1. It compares with
that layout, not with the header it is compiled against: a later version 1 header may append
fields, which read as zero when Spark passes a smaller structure, and the SDK sets only the fields
that fit in a structure that Spark fills.
`TaskContext::IsInterrupted()` calls Spark's live cancellation callback, which for a reader is
also true once the task ended; check it during long operations, and use timeouts for blocking
I/O. `TaskContext::ForTesting()`, with its `With*` methods and `InterruptedBy`, builds a context
for tests outside Spark.

`DataSource::Init` runs once in each JVM when Spark loads the library, with the Spark version in
`HostInfo`; a failure prevents Spark from using the library and is logged. `spark::native::Log`,
and `LogError` to `LogTrace`, write to the driver or executor log through Spark from any thread,
and drop messages outside Spark, such as in unit tests. The SDK follows the level of the log of
Spark, which it asks with `SparkDsHost.is_log_enabled` when Spark loads the library and when an
operation or a task starts, so it also follows a level that changes while Spark runs;
`IsLogEnabled` tells whether a message of a level reaches Spark, to skip formatting it.

A source that cannot infer its schema, such as a sink of any input, sets
`static constexpr bool kInfersSchema = false;`. The SDK then leaves the native `schema` function
unset, so Spark requires a user-specified schema and reports its standard error without one.

A library that implements several data sources is installed or listed in its package under each
of their names. Spark passes the name in `CreateInfo::name`, so one exported `DataSource` can
choose the source to create; the macro exports a single source type per library.

The headers separate the public API (`data_source.h`, `capabilities.h`, `task_context.h`, and
`log.h`) from the implementation under `internal/`: C adapters in `abi.h`, driver and executor
state in `bridge.h`, Arrow checks and the exported stream in `arrow.h`, serialization in
`serde.h`, and the function table in `export.h`.

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
`static constexpr bool kSupportsTruncate = true;`, which declares `kBatchTruncate`, for the save
mode "overwrite" of a batch write, or `kStreamingTruncate`, for the output mode "complete" of a
streaming write. Its `New*` method then gets `WriteMode::kTruncate` for them; without it the
write only appends. Every stream also declares the native capability
`SPARK_DS_ADMISSION_CONTROL`, whose default read limit, all the available data, reads as a stream
without it does.

Most connectors do not override `capabilities()`. A connector can restrict the supported set for
the current options:

```cpp
spark::native::Capabilities capabilities() const override {
  using spark::native::Capabilities;
  if (readonly_) {
    // The SDK also drops the truncations, which require their writes.
    return Capabilities::All().Without(Capabilities::kBatchWrite | Capabilities::kStreamingWrite);
  }
  return Capabilities::All();
}
```

The SDK evaluates this once during `create` and keeps only implemented operations, so a source
can narrow its capabilities but never enable one that it does not implement, and keeps each
truncation only with its write. An empty set permits schema discovery and close, but rejects
operation creation before calling the connector. The declaration stays fixed for that instance.
Limit and column pruning remain per-scan contracts.

Spark also calls `create` and `close` during query analysis to discover table capabilities. A
probe uses the table's options, while an operation may override them with query options and
checks capabilities again on a new source instance. Keep `Make` and `capabilities()` lightweight
and safe to destroy immediately; do not assume that `InferSchema` or `NewScan` will follow.

Spark propagates the native `BATCH_WRITE` and `BATCH_TRUNCATE` flags to `Table.capabilities()`,
as `BATCH_WRITE` and `TRUNCATE`, so a read-only source rejects batch writes during analysis,
before Spark computes their input. Its DSv2 read and streaming-write flags remain present to
avoid V1 fallback; the native flags still reject unsupported reads or streaming writes during
planning.

## Serialization

The SDK serializes factories, partitions, offsets, and commit messages with nlohmann/json. A
connector makes a type serializable with `NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE` or
`NLOHMANN_DEFINE_TYPE_INTRUSIVE`, or with its own `to_json` and `from_json`; serialized types must
be default constructible. A serialization failure, such as an exception in `to_json`, fails the
call that needed it.

The state of a reader or a writer is its factory, which the SDK wraps, as it wraps partitions
and messages, in a versioned JSON envelope. Spark passes the schema to read or to write, and the
kind of the operation, with the state, so the state carries neither. The envelope is an SDK
implementation detail: use the same build of a library on the driver and the executors.

## Batch write lifecycle

```text
Driver:
  DataSource::NewWrite -> BatchWrite
                            | NewWriterFactory()  ^ Commit(messages) / Abort(messages)
                    serialized factory            | typed messages
                            |                     |
Executor, per task attempt: v                     |
                      DataWriterFactory::Open(WriterContext)
                               -> DataWriter::Write(RecordBatch)*
                               -> Commit() / Abort() -> destructor
```

`DataSource::NewWrite(WriteInfo)` validates the input schema and returns an independent driver
coordinator, a `BatchWrite`. Its `NewWriterFactory` returns the serializable executor
configuration of its task writers, which the SDK encodes as the state of the writer that native
`create_writer` returns. Restoring an executor writer uses only those bytes and the schema that
Spark passes with them.

Each `DataWriterFactory::Open` receives a `WriterContext` with the input schema and the task, and
returns a task-bound writer. `Write` receives an `arrow::RecordBatch` imported from Spark, with
the schema of the writer, whose Arrow buffers the writer may retain after the call, until it is
destroyed: they belong to the data writer of Spark, which frees them when it closes the writer.
When Spark writes the batches of a native scan without converting them to rows, it copies their
values as the scan returned them, without the checks of a conversion, such as the precision of a
decimal. Check cancellation during lengthy writes and use I/O timeouts.

Task `Commit()` returns the message of the factory, the `Message` of the `BatchWrite`. Driver
`Commit` receives one winning message per partition, at the position of its partition; driver
`Abort` receives `std::vector<std::optional<Message>>`, with nothing for a partition whose
message did not arrive or cannot be decoded, so a corrupted message cannot prevent cleanup. A
failed task or driver commit leaves the same object available for abort. A task commit whose
message cannot be serialized counts as failed, so `Abort` can follow a `Commit` that returned a
message. Spark calls each method at most once, never writes after a failure or a commit, and
never aborts a successful commit, as the header documents, so connectors do not need to track
their own phase; the documentation of `BatchWrite` and `DataWriter` lists the exact call-order
guarantees. Destructors only release resources.

Stage data per task attempt and publish it only at driver commit. In particular,
`WriteMode::kTruncate` must leave existing data intact until that commit. Retries and
speculative attempts may stage data for the same partition; publish only the attempts whose
messages Spark passes to commit. Executor loss can prevent cleanup, including after task commit,
so connectors need a strategy for orphaned data. Spark provides no batch query ID: generate and
serialize an operation identity when needed.

When `NewWriterFactory` or the serialization of its factory fails, `create_writer` fails and
the SDK destroys the coordinator without commit or abort, so the destructor must also release
what only that write staged. Spark also creates writers late, after prerequisite AQE shuffle
stages, but input computation can still fail during write tasks, so constructors must avoid
irreversible changes. The epoch ID tells the task writers of batch writes, whose epoch ID is
`-1`, from those of micro-batches.

## Streaming reads and writes

Micro-batch streaming reads reuse the executor side of batch scans:

```text
Driver, for each run of a query:
  DataSource::NewMicroBatchStream -> MicroBatchStream
      InitialOffset(), DefaultReadLimit()
      LatestOffset(LatestOffsetInfo) -> Offset, stored as JSON in the checkpoint
      Plan(start, end) -> ScanPlan { factory, partitions }, for each micro-batch
      Commit(end)
Executor, per task:
  restored factory + partition + ReaderContext -> arrow::RecordBatchReader -> Arrow C stream
```

Spark creates a stream when a query starts or restarts from its checkpoint, and closes it when
the query stops. It keeps the initial offset in the checkpoint, and asks for it only when the
checkpoint does not have it yet. For each micro-batch, it asks for the latest offset after the
end of the previous micro-batch, and plans the data up to that offset. The executors read the
partitions of a micro-batch exactly as those of a batch scan: the two share `read`, which gets
the kind of the read. `MicroBatchStreamInfo::schema` has the columns to read, which are all the
columns of the schema until Spark prunes the columns of streaming scans, and
`MicroBatchStreamInfo::checkpoint_location` the directory of the checkpoint of the source.

`MicroBatchStream::DefaultReadLimit` limits the data of each micro-batch, for example to
`ReadLimit::MaxRows` from an option of the source; the default reads all the available data.
Spark passes the limit to `LatestOffset` in `LatestOffsetInfo::limit`, and with
`Trigger.AvailableNow`, the end of the run in `LatestOffsetInfo::run_end`, which the latest
offset must not go beyond. Spark also asks without a limit for the progress of the query, so a
stream does not count the calls of `LatestOffset` as micro-batches.

The SDK serializes the `Offset` of a `MicroBatchStream` as compact JSON on a single line, which
Spark stores in the checkpoint as it is, without the SDK's envelope, and decodes the offsets that
Spark passes back. Spark compares offsets by their JSON to decide whether a micro-batch has new
data, so a position must always serialize to the same JSON: nlohmann/json orders the keys of
objects, but a custom `to_json` must be deterministic too. A restarted query may plan its last
range again, with offsets from its checkpoint that an earlier version of the library may have
written. A failed call, including an exception or an offset that does not decode, stops the
query: Spark makes no other call on the stream before it closes it.

Streaming writes reuse the task writers of batch writes. Spark creates a `StreamingWrite` for
each micro-batch, from a `StreamingWriteInfo` with the ID of the query, which stays the same
across restarts. It gets the calls of a `BatchWrite`; the SDK passes the epoch ID of the
micro-batch to `Commit` and `Abort`, and gives it to the task writers in
`WriterContext::epoch_id`. Its destruction is deferred, though: Spark destroys the write of a
micro-batch only after it creates that of the next one, and the last write of a query once it is
no longer referenced, possibly long after the query stops and on another thread. Release what a
micro-batch holds, such as a lock on the destination, in `Commit` and `Abort` rather than in the
destructor. Spark may commit an epoch again after a restart, with a new write, so
a write must skip an epoch that is already committed: the query ID and the epoch ID identify the
output of a micro-batch. A restarted query may also abort an epoch that an earlier run
committed, so an abort removes only what its own task writers wrote. The [file sink][sink]
records the last epoch of each query in its manifest for this. Continuous processing is not
supported.

## Arrow contract

`PartitionReaderFactory::Open` returns a `std::shared_ptr<arrow::RecordBatchReader>`, so a
Parquet reader, a `RecordBatchReader::MakeFromIterator` of a generator, or any other Arrow reader
can be returned directly. Return batches lazily with bounded memory, with the columns of
`ReaderContext::schema`. The reader may also have other columns, such as all the columns of the
source: the stream has those of the schema to read, by name and in its order, since Spark rejects
a stream with other columns. The schema of the reader determines that of the stream, and every
batch must have it; it may use any supported encoding of a Spark type, such as `large_utf8` for a
string. Exported arrays own their buffers independently of the reader: Spark may release a stream
before releasing its previously returned batches.

Stream callbacks, including release, may run on different threads, but never concurrently.
Readers do not need to be thread-safe or tied to the thread that opened the stream. Stream errors
are kept by the stream, not in a thread-local, and remain readable after a callback moves to
another thread.

The SDK checks the schema of the reader against the schema to read before exporting the stream,
and checks each batch: its schema, its `arrow::RecordBatch::Validate()`, because the C data
interface does not carry buffer sizes for Spark to check, and its recursive array offsets. Since
`Validate()` does not check every offset of a string or binary array, nor the views of a view
array, the SDK also checks, at any level, that offsets never decrease and stay within the data
buffer, and that each view names an existing data buffer and a range within it, without reading
the values or validating UTF-8. It
compares Spark logical types, ignoring nullability and Arrow metadata, accepting the supported
string and binary encodings and equivalent timestamp time zones. It rejects unsupported types,
schema drift, and nonzero array offsets with explicit errors. It does not silently copy arrays,
cast types, or expand dictionary encoding. Batches must also respect the nullability of the
schema to read: as Spark does, the SDK rejects a null in a column, field, element, or map value
that the schema declares not nullable, unless its parent value is null.

The supported types are those of version 1 of the ABI: signed integers, float32/64, boolean,
strings and binary (including the large and view variants), decimal128 up to precision 38,
date32, microsecond timestamps with or without a time zone, structs, lists, and maps. Spark
rejects other Spark types, such as intervals, variant, time, and the null type, before it passes
a schema to the library. Dictionary encoding, unsigned integers, decimal256, large or fixed-size
lists, and other timestamp units are outside this prototype's contract. Normalize sliced arrays
before returning them.

An empty projection must retain the number of rows even with no columns: build such batches with
`arrow::RecordBatch::Make(schema, num_rows, {})`. The [Range implementation][range-source]
demonstrates this together with bounded batches, disjoint partitions, admission control, and
cooperative cancellation.

## Pushdown contracts

| Limit result | ABI boolean | Meaning |
| --- | --- | --- |
| `kUnsupported` | `false` | No promise; a connector may still cap each partition on its own |
| `kGlobal` | `true` | The entire scan returns at most the limit |

Spark retains its final limit and pushes a limit only when the query does not filter the rows of
the scan, since it filters them after reading.

`ScanBuilder::PruneColumns` gets the top-level columns that the query needs, in order, so that
the scan can plan to read only them. The readers get them as the schema to read whether or not
the scan implements it, and return exactly them, so a connector can add pruning to the planning
of its scans incrementally.

Version 1 of the ABI does not push filters down yet; Spark evaluates them after reading.

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
the project builds, and `SPARK_DATASOURCE_HEADER_DIR` the directory of `spark_datasource.h`.
`spark_add_native_data_source(<name> <sources>)` builds the library of a data source, such as
`libspark_datasource_<name>.so` on Linux, with hidden symbols and a version script (an exported
symbol list on macOS), so that it exports only `spark_ds_init`. Format the code with
`clang-format --style=file:sql/native/cpp/.clang-format`.

The tests need no test framework. The SDK tests cover error and exception conversion, sized ABI
structures, read limits, task contexts, capabilities, serialization, and the exported Arrow
stream: early release, schema and nullability checks of nested data, layouts, cross-type
encodings, and the columns of the schema to read that the SDK keeps from a reader. Each of four
more test executables exports a data source and calls its actual function table through
`spark_ds_init`:

* `library_test`: initialization and its failure, logging through the host and its level, which
  the SDK follows when an operation starts, the source name, and narrowed, unimplemented, and
  failing capability declarations.
* `read_test`: plans and planning failures, negotiation failures, independent planners, executor
  retries from serialized values only, the rejection of a reader without the schema to read, and
  the kind of a read, which selects the factory.
* `batch_write_test`: factory and message round trips through the state of `create_writer`,
  retained Arrow buffers, cancellation, failed writes and commits, factory creation and
  serialization failures, which destroy the coordinator without abort, missing and undecodable
  messages, and the epoch IDs that select the task writers.
* `streaming_test`: single-line JSON offsets, the checkpoint location, the state of each planned
  micro-batch, reads that restore batch or micro-batch factories of different types, read limits
  and the end of a run, exceptions, a rejected writer, and the query and epoch IDs of streaming
  writes and their task writers.

The Range example adds its unit tests and `range_c_abi_smoke`, a C client that compiles against
the C header only, loads the actual library with `dlopen`, and checks it as Spark would, without
a JVM; the sink example adds its own tests. These checks do not substitute for Spark planner and
task-lifecycle testing.

## Package and run with Spark

See the [Range example guide][range] for packaging and `spark-submit` commands. Its `package.py`
creates a single-platform `.sparkpkg`; `smoke.py` checks reading, filtered reads, projection,
unfiltered limit pushdown, streaming reads with and without a limit on their micro-batches, and
analysis-time rejection of unsupported batch writes. The [sink guide][sink] uses the same
packager with `--source cpp_sdk_sink`. Its smoke script checks append, overwrite,
failed-overwrite cleanup, analysis-time rejection for a source instance with writes disabled,
and the epochs of a streaming write across restarts.

The libraries link Arrow C++ dynamically, so the driver and executors must find the Arrow
libraries that they were built with, as installed or through the run path of the build. To ship
a self-contained package, link Arrow statically.

## Scope and limitations

Native sources run trusted code in the driver and executors. The SDK translates errors and
exceptions into C and Arrow errors. Process aborts, exceptions that escape destructors, or
crashes in connector code cannot be recovered. A reader's or writer's background work must stop
before its task context expires. Native memory is not reserved through Spark's memory manager.

This prototype does not include continuous processing, filter pushdown, nested projection,
custom metrics, or memory accounting, and does not extend the ABI.

[header]: ../../core/src/main/resources/org/apache/spark/sql/datasource/spark_datasource.h
[guide]: ../../../docs/sql-native-data-sources.md
[range]: ../../../examples/src/main/cpp/native-data-source/README.md
[range-source]: ../../../examples/src/main/cpp/native-data-source/range_source.cc
[sink]: ../../../examples/src/main/cpp/native-data-sink/README.md
