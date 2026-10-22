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

// Experimental C++ SDK for the native data source C ABI proposed in Spark PR #59424.
//
// A data source derives from DataSource, and names the types of the operations that it
// implements: a ScanBuilder for batch reads, a MicroBatchStream for micro-batch streaming reads, a
// BatchWrite for batch writes, and a StreamingWrite for streaming writes. Then
// SPARK_EXPORT_DATA_SOURCE exports it once from a shared library. The SDK owns handles,
// serialization, C ABI adaptation, Arrow import and export, and the conversion of errors and
// exceptions. C++ types stay inside the library: only the C ABI crosses its boundary.
//
// Methods receive their inputs in structs with constructors, so that new ABI fields can reach
// connectors without breaking them: *Info structs on the driver (CreateInfo, ScanInfo,
// MicroBatchStreamInfo, WriteInfo, StreamingWriteInfo, and HostInfo), and *Context structs, which
// carry the live TaskContext, on the executors (ReaderContext and WriterContext).
//
// Errors are arrow::Status values. The SDK also reports an exception that connector code throws as
// the error of the call. Destructors must not throw, as C++ requires.

#pragma once

#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include <arrow/record_batch.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/type.h>

#include "spark/native/capabilities.h"
#include "spark/native/log.h"
#include "spark/native/predicate.h"
#include "spark/native/task_context.h"

namespace spark::native {

using Options = std::map<std::string, std::string>;

/// The type of an operation that a source does not implement, which DataSource names for every
/// operation by default.
struct Unsupported {};

// Data sources: Spark creates a DataSource for each operation, and its types name the operations
// that the source implements.

/// What Spark passes to create a data source.
struct CreateInfo {
  /// The information of a source created outside Spark, for example in a test.
  CreateInfo(std::string name, Options options)
      : name(std::move(name)), options(std::move(options)) {}

  /// The name of the data source, as listed in the manifest of its package, or in lower case for
  /// an installed library. A library that implements several data sources, installed or listed
  /// under each of their names, can choose by this name.
  std::string name;
  /// The options, with keys in lower case. A capability probe uses the options of the table; an
  /// operation may override them with the options of its query.
  Options options;
};

/// Driver-side source configuration, schema discovery, and the operations of a source.
///
/// A source derives from this class, and defines a factory and the operations that it implements:
///
///   class RangeSource : public spark::native::DataSource {
///    public:
///     using ScanBuilder = RangeScan;
///     using MicroBatchStream = RangeStream;
///
///     static arrow::Result<RangeSource> Make(const spark::native::CreateInfo& info);
///     arrow::Result<RangeScan> NewScan(const spark::native::ScanInfo& info) const;
///     arrow::Result<RangeStream> NewMicroBatchStream(
///         const spark::native::MicroBatchStreamInfo& info) const;
///     ...
///   };
///
/// Each operation has a type, which is Unsupported unless the source names another, and a method
/// that creates it:
///
/// - ScanBuilder, a ScanBuilder<Factory> for batch reads, created by
///   `arrow::Result<ScanBuilder> NewScan(const ScanInfo&) const`;
/// - MicroBatchStream, a MicroBatchStream<Offset, Factory> for micro-batch streaming reads,
///   created by `arrow::Result<MicroBatchStream> NewMicroBatchStream(
///   const MicroBatchStreamInfo&) const`;
/// - BatchWrite, a BatchWrite<Factory> for batch writes, created by
///   `arrow::Result<BatchWrite> NewWrite(const WriteInfo&) const`;
/// - StreamingWrite, a StreamingWrite<Factory> for streaming writes, created by
///   `arrow::Result<StreamingWrite> NewStreamingWrite(const StreamingWriteInfo&) const`.
///
/// The SDK installs native callbacks and reports capabilities only for the implemented
/// operations, and calls the New* method of an operation only for them. SPARK_EXPORT_DATA_SOURCE
/// checks these declarations when it compiles.
///
/// Spark creates a source with `static arrow::Result<S> Make(const CreateInfo&)` for each schema
/// inference, scan, stream, and write, including the write of each micro-batch, and may create one
/// only to inspect its capabilities, then destroy it. Keep construction lightweight. Scan builders,
/// streams, and writes must not refer to their source, which Spark destroys first. Sources, and the
/// objects that they create, must be movable.
class DataSource {
 public:
  virtual ~DataSource() = default;

  // The operations of the source, which a source implements by naming their types.

  using ScanBuilder = Unsupported;
  using MicroBatchStream = Unsupported;
  using BatchWrite = Unsupported;
  using StreamingWrite = Unsupported;

  /// Called once in each JVM when Spark loads the library, before it creates any source. On
  /// failure, Spark does not use the library, and the error is logged. A source hides it to
  /// initialize the library.
  static arrow::Status Init(const HostInfo& host) {
    (void)host;
    return arrow::Status::OK();
  }

  // Schema discovery.

  /// Whether InferSchema infers the schema of the data. A source that sets it to false, with
  /// `static constexpr bool kInfersSchema = false;`, leaves the native function unset: Spark then
  /// requires a user-specified schema, reports its standard error without one, and never calls
  /// InferSchema.
  static constexpr bool kInfersSchema = true;

  /// Returns the schema of the data, when the user does not specify one.
  virtual arrow::Result<std::shared_ptr<arrow::Schema>> InferSchema() const {
    return arrow::Status::NotImplemented(
        "DataSource::InferSchema is not implemented: implement it, or set "
        "DataSource::kInfersSchema to false to require a user-specified schema");
  }

  // Instance restrictions.

  /// Returns the operations of this instance. The SDK keeps only those that the source
  /// implements, so the default, Capabilities::All(), keeps them all. Override this to restrict an
  /// instance, for example for a read-only destination. The SDK evaluates it once when Spark
  /// creates the source. An instance without operations still supports schema discovery.
  virtual Capabilities capabilities() const { return Capabilities::All(); }

 protected:
  DataSource() = default;
  DataSource(const DataSource&) = default;
  DataSource(DataSource&&) = default;
  DataSource& operator=(const DataSource&) = default;
  DataSource& operator=(DataSource&&) = default;
};

// Batch reads: a ScanBuilder negotiates pushdown and plans the scan on the driver, and a
// PartitionReaderFactory opens a reader of each partition on the executors.

/// What Spark passes to create a scan.
struct ScanInfo {
  /// The information of a scan created outside Spark, for example in a test.
  explicit ScanInfo(std::shared_ptr<arrow::Schema> schema) : schema(std::move(schema)) {}

  /// The schema to read: the schema of the source, or one that the user specified.
  std::shared_ptr<arrow::Schema> schema;
};

/// How completely a connector evaluates a pushed predicate. Spark currently rechecks all
/// predicates, including exact ones.
enum class FilterSupport {
  /// Implements Spark SQL semantics and returns only matching rows.
  kExact,
  /// May skip data that cannot match, but may also return nonmatching rows.
  kPruningOnly,
  /// Does not evaluate this predicate.
  kUnsupported,
};

/// Whether a connector limits the rows produced by the whole scan.
enum class LimitSupport {
  /// The scan may return more rows than the limit. A connector may still cap the rows of each
  /// partition on its own: Spark applies its limit after reading.
  kUnsupported,
  /// The whole scan returns at most the limit. Spark still applies its limit.
  kGlobal,
};

/// What an executor reader receives with its partition.
struct ReaderContext {
  /// The context of a reader opened outside Spark, for example in a test.
  ReaderContext(std::shared_ptr<arrow::Schema> schema, TaskContext task)
      : schema(std::move(schema)), task(task) {}

  /// The negotiated schema: the schema to read, or its pruned columns in order. The reader
  /// returns batches with exactly these columns.
  std::shared_ptr<arrow::Schema> schema;
  /// The task that reads the partition.
  TaskContext task;
};

/// Serializable configuration from which executors open partition readers.
///
/// The factory carries configuration or a snapshot descriptor; open connections and other live
/// resources in Open. The SDK serializes the negotiated schema with the factory, so the factory
/// need not carry it.
///
/// The factory and its partitions are serialized with nlohmann::json, for example with
/// NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE, and must be default constructible.
template <typename PartitionT>
class PartitionReaderFactory {
 public:
  using Partition = PartitionT;

  virtual ~PartitionReaderFactory() = default;

  /// Opens a lazy reader of the batches of `partition`, with the columns of
  /// ReaderContext::schema.
  ///
  /// - Return batches lazily, with bounded memory.
  /// - The reader owns its resources and must not refer to this short-lived factory. It may keep
  ///   `context` until Spark releases the stream.
  /// - Spark may call the reader on different threads, never concurrently.
  /// - Check TaskContext::IsInterrupted during long operations, and use timeouts for blocking I/O.
  /// - Batches must own their buffers, because Spark may release the stream before its batches.
  ///
  /// Any arrow::RecordBatchReader, such as a Parquet reader, can be returned. Its schema may use
  /// any supported Arrow encoding of a Spark type, such as large_utf8 for a string, and every batch
  /// must have that schema. The SDK checks Spark-compatible Arrow types, the negotiated schema and
  /// its nullability, and zero array offsets recursively. It reports incompatible layouts instead
  /// of copying buffers.
  virtual arrow::Result<std::shared_ptr<arrow::RecordBatchReader>> Open(
      const Partition& partition, const ReaderContext& context) const = 0;

 protected:
  PartitionReaderFactory() = default;
  PartitionReaderFactory(const PartitionReaderFactory&) = default;
  PartitionReaderFactory(PartitionReaderFactory&&) = default;
  PartitionReaderFactory& operator=(const PartitionReaderFactory&) = default;
  PartitionReaderFactory& operator=(PartitionReaderFactory&&) = default;
};

/// A driver-created plan containing only serializable values.
///
/// The SDK serializes the reader factory once and each partition independently. Include snapshot
/// identifiers when needed for repeatable task attempts. Do not put pointers, open files,
/// connections, or process-local resources here.
template <typename FactoryT>
struct ScanPlan {
  using Factory = FactoryT;
  using Partition = typename FactoryT::Partition;

  ScanPlan(Factory factory, std::vector<Partition> partitions)
      : factory(std::move(factory)), partitions(std::move(partitions)) {}

  Factory factory;
  std::vector<Partition> partitions;
};

/// Negotiates pushdown on the driver, then plans the scan.
///
/// The SDK calls Plan once, when Spark first asks for the partitions, and caches the result,
/// including a failure. Only the serialized factory and partitions reach the executors.
template <typename FactoryT>
class ScanBuilder {
 public:
  using Factory = FactoryT;

  virtual ~ScanBuilder() = default;

  /// Returns one result per input predicate in order. Unknown functions and types must not be
  /// reported as exact. Spark currently rechecks all filters. A predicate that the SDK cannot
  /// parse arrives as Predicate::Kind::kUnknown, and the SDK reports it as unsupported whatever
  /// this method returns for it.
  virtual arrow::Result<std::vector<FilterSupport>> PushFilters(
      const std::vector<Predicate>& filters) {
    return std::vector<FilterSupport>(filters.size(), FilterSupport::kUnsupported);
  }

  /// Counts rows after filtering. Spark currently pushes limits only for scans with no filters;
  /// the SDK also declines a limit when any predicate that it received remains residual.
  virtual arrow::Result<LimitSupport> PushLimit(int64_t limit) {
    (void)limit;
    return LimitSupport::kUnsupported;
  }

  /// On true, output exactly these top-level columns in this order. On false, keep the original
  /// schema. An empty projection must preserve row counts. Retain internally needed filter
  /// columns without returning them. Readers get the resulting schema in ReaderContext::schema.
  virtual arrow::Result<bool> PruneColumns(const std::vector<std::string>& columns) {
    (void)columns;
    return false;
  }

  /// Plans the scan. The SDK calls it at most once, as the last call on the builder, so it may
  /// move out of the builder.
  virtual arrow::Result<ScanPlan<Factory>> Plan() = 0;

 protected:
  ScanBuilder() = default;
  ScanBuilder(const ScanBuilder&) = default;
  ScanBuilder(ScanBuilder&&) = default;
  ScanBuilder& operator=(const ScanBuilder&) = default;
  ScanBuilder& operator=(ScanBuilder&&) = default;
};

// Micro-batch streaming reads: a MicroBatchStream finds new data and plans each micro-batch on the
// driver, and the PartitionReaderFactory of each plan reads its partitions on the executors, as in
// batch reads.

/// What Spark passes to create the stream of a streaming query.
struct MicroBatchStreamInfo {
  /// The information of a stream created outside Spark, for example in a test.
  explicit MicroBatchStreamInfo(std::shared_ptr<arrow::Schema> schema)
      : schema(std::move(schema)) {}

  /// The schema to read: the schema of the source, or one that the user specified.
  std::shared_ptr<arrow::Schema> schema;
};

/// Finds new data and plans the micro-batches of a streaming query, on the driver.
///
/// Spark creates a stream when a streaming query starts, or restarts from its checkpoint, and
/// destroys it when the query stops. For each micro-batch, it asks for the LatestOffset, and when
/// that differs from the end of the previous micro-batch, plans the data in between. Only the
/// serialized factory and partitions of each plan reach the executors, which read them as those of
/// a batch scan. Spark pushes nothing down into a streaming scan, so its readers return all the
/// columns of the schema.
///
/// Offsets: Spark stores offsets in the checkpoint of the query, as the JSON of OffsetT, which the
/// SDK serializes with nlohmann::json, and compares them by that JSON, so serialize a position to
/// the same JSON each time: nlohmann::json orders the keys of objects, but a type with a custom
/// to_json must be deterministic too. A restarted query passes the offsets of its checkpoint, which
/// an earlier version of the library may have written, to Plan and Commit.
///
/// Lifecycle:
///
/// - Calls never run concurrently.
/// - A failed call, including an exception, stops the query, so the SDK returns that failure from
///   every later call.
/// - After a restart, Spark may plan the last range of the query again, so Plan must return
///   partitions that read the same data for the same range.
template <typename OffsetT, typename FactoryT>
class MicroBatchStream {
 public:
  /// A position in the data, such as the next record of each partition of a log.
  using Offset = OffsetT;
  /// The reader factory of the micro-batches, which can be that of batch scans.
  using Factory = FactoryT;

  virtual ~MicroBatchStream() = default;

  /// Returns the offset to start from when the query has no checkpoint: the first micro-batch
  /// reads the data after it.
  virtual arrow::Result<Offset> InitialOffset() = 0;

  /// Returns the offset of the most recent data available.
  virtual arrow::Result<Offset> LatestOffset() = 0;

  /// Plans the micro-batch that reads the data after `start`, up to and including `end`.
  virtual arrow::Result<ScanPlan<Factory>> Plan(const Offset& start, const Offset& end) = 0;

  /// Tells the stream that Spark has processed the data up to and including `end` and will not
  /// read it again, so that the stream can release it.
  virtual arrow::Status Commit(const Offset& end) {
    (void)end;
    return arrow::Status::OK();
  }

 protected:
  MicroBatchStream() = default;
  MicroBatchStream(const MicroBatchStream&) = default;
  MicroBatchStream(MicroBatchStream&&) = default;
  MicroBatchStream& operator=(const MicroBatchStream&) = default;
  MicroBatchStream& operator=(MicroBatchStream&&) = default;
};

// Batch writes: a BatchWrite coordinates the write on the driver, and a DataWriterFactory opens a
// DataWriter for each task attempt on the executors.

/// How a write changes the destination.
///
/// New modes may be added. The SDK passes a mode only when the source instance has the matching
/// capability, so a source never receives a mode that it did not opt in to.
enum class WriteMode {
  kAppend,
  /// Replace existing data at driver commit, never at writer creation.
  kTruncate,
};

/// What Spark passes to create a batch write.
struct WriteInfo {
  /// The information of a write created outside Spark, for example in a test.
  WriteInfo(std::shared_ptr<arrow::Schema> schema, WriteMode mode)
      : schema(std::move(schema)), mode(mode) {}

  /// The schema of the data to write.
  std::shared_ptr<arrow::Schema> schema;
  /// How the write changes the destination.
  WriteMode mode;
};

/// What a task writer receives when it opens.
struct WriterContext {
  /// The context of a batch writer opened outside Spark, for example in a test. Set epoch_id for a
  /// streaming writer.
  WriterContext(std::shared_ptr<arrow::Schema> schema, TaskContext task)
      : schema(std::move(schema)), task(task) {}

  /// The schema of the batches to write, as in WriteInfo::schema.
  std::shared_ptr<arrow::Schema> schema;
  /// The task attempt that writes.
  TaskContext task;
  /// The epoch of the micro-batch of a streaming write, or nothing for a batch write.
  std::optional<int64_t> epoch_id;
};

/// The writer of one task attempt. The SDK destroys it after commit or abort.
///
/// Lifecycle: the SDK enforces the following, so implementations need not track their own phase
/// or guard against repeated calls.
///
/// - Write runs zero or more times, and never after any call has failed or after Commit or Abort.
/// - Commit runs at most once, never after a failed Write or after Abort.
/// - Abort runs at most once, never after a Commit whose message reached Spark. It may follow a
///   failed Write or Commit, including a Commit that returned a message that the SDK could not
///   serialize, so it must handle a task attempt that already finished its output.
/// - The destructor runs exactly once when Spark closes the writer, possibly without Commit or
///   Abort.
/// - An exception in any method is reported as that method's failure.
template <typename MessageT>
class DataWriter {
 public:
  using Message = MessageT;

  virtual ~DataWriter() = default;

  /// Takes an imported Arrow batch, which may be retained after this call. Its columns match
  /// WriterContext::schema by Spark type. Process batches incrementally and check task
  /// cancellation.
  virtual arrow::Status Write(std::shared_ptr<arrow::RecordBatch> batch) = 0;

  /// Finishes staging and returns a message for the driver. Do not make the whole write visible
  /// here. On failure, including the failure to serialize the message, the SDK keeps this writer
  /// available for Abort.
  virtual arrow::Result<Message> Commit() = 0;

  virtual arrow::Status Abort() = 0;
};

/// Serializable configuration for independent task-attempt writers, which
/// BatchWrite::CreateWriterFactory or StreamingWrite::CreateWriterFactory creates.
///
/// Open connections and files in Open, not in the factory. A returned writer must not refer to
/// this temporary factory, but may keep the context until it is destroyed. Calls on a writer can
/// move between threads, never concurrently.
///
/// The factory and its messages are serialized with nlohmann::json, and must be default
/// constructible.
template <typename MessageT>
class DataWriterFactory {
 public:
  /// The message with which a task attempt reports its staged output to the driver.
  using Message = MessageT;

  virtual ~DataWriterFactory() = default;

  virtual arrow::Result<std::unique_ptr<DataWriter<Message>>> Open(
      const WriterContext& context) const = 0;

 protected:
  DataWriterFactory() = default;
  DataWriterFactory(const DataWriterFactory&) = default;
  DataWriterFactory(DataWriterFactory&&) = default;
  DataWriterFactory& operator=(const DataWriterFactory&) = default;
  DataWriterFactory& operator=(DataWriterFactory&&) = default;
};

/// A driver coordinator for one batch write, which also creates the serializable configuration of
/// its task writers.
///
/// Keep irreversible publication, including removing overwritten data, in Commit. The destructor
/// releases resources; it must not publish data or perform fallible flushing.
///
/// Lifecycle: the SDK enforces the following, so implementations need not track their own phase
/// or guard against repeated calls.
///
/// - CreateWriterFactory runs at most once, before any task opens a writer.
/// - Commit runs at most once, only while no earlier call has failed, including
///   CreateWriterFactory and the serialization of its factory. It is skipped, and the write fails,
///   when any task message is missing or cannot be decoded.
/// - Abort runs at most once, never after a successful Commit. It may follow a failed Commit, or
///   run without Commit.
/// - The destructor runs exactly once, possibly without Commit or Abort. Spark closes the
///   coordinator without Abort when CreateWriterFactory or the serialization of its factory fails,
///   so the destructor must also release what only this write staged.
/// - An exception in any method is reported as that method's failure.
///
/// Message i of Commit and Abort belongs to input partition i.
template <typename FactoryT>
class BatchWrite {
 public:
  /// The configuration from which each task attempt opens its writer.
  using Factory = FactoryT;
  /// The message with which a task attempt reports its staged output to the driver.
  using Message = typename FactoryT::Message;

  /// Whether the write supports WriteMode::kTruncate, which adds the kTruncate capability to batch
  /// writes. A write sets it with `static constexpr bool kSupportsTruncate = true;`.
  static constexpr bool kSupportsTruncate = false;

  virtual ~BatchWrite() = default;

  /// Returns the configuration of the task writers. The SDK serializes it once, with the schema of
  /// the input, and caches the result, including a failure. Include a unique operation ID when
  /// needed; Spark supplies no batch query ID.
  virtual arrow::Result<Factory> CreateWriterFactory() const = 0;

  /// Publishes only the winning attempt from each input partition. Task retries and speculative
  /// attempts can stage other data that must not be published.
  virtual arrow::Status Commit(std::vector<Message> messages) = 0;

  /// Aborts after an input, task, or driver commit failure. Nothing stands for a partition whose
  /// commit message did not reach the driver, or cannot be decoded. Executor loss can leave
  /// orphaned staged data; plan for its cleanup.
  virtual arrow::Status Abort(std::vector<std::optional<Message>> messages) = 0;

 protected:
  BatchWrite() = default;
  BatchWrite(const BatchWrite&) = default;
  BatchWrite(BatchWrite&&) = default;
  BatchWrite& operator=(const BatchWrite&) = default;
  BatchWrite& operator=(BatchWrite&&) = default;
};

// Streaming writes: a StreamingWrite coordinates the write of one micro-batch on the driver, and
// its DataWriterFactory opens a DataWriter for each task attempt on the executors, as in batch
// writes.

/// What Spark passes to create the write of a micro-batch.
struct StreamingWriteInfo {
  /// The information of a write created outside Spark, for example in a test.
  StreamingWriteInfo(std::shared_ptr<arrow::Schema> schema, WriteMode mode, std::string query_id)
      : schema(std::move(schema)), mode(mode), query_id(std::move(query_id)) {}

  /// The schema of the data to write.
  std::shared_ptr<arrow::Schema> schema;
  /// How the micro-batch changes the destination: WriteMode::kTruncate for the output mode
  /// "complete", which replaces the output of the previous micro-batches.
  WriteMode mode;
  /// The ID of the streaming query, which stays the same when the query restarts from its
  /// checkpoint.
  std::string query_id;
};

/// A driver coordinator for the write of one micro-batch, or epoch, of a streaming query, which
/// also creates the serializable configuration of its task writers.
///
/// Spark creates a write for each micro-batch. The SDK enforces the lifecycle of a BatchWrite,
/// with the epoch ID of the micro-batch passed to Commit and Abort, and to the task writers in
/// WriterContext::epoch_id.
///
/// Exactly-once output: Spark may commit an epoch again, with a new write, for example when the
/// query restarts after a failure: committing an epoch that is already committed must have no
/// effect. The ID of the query, StreamingWriteInfo::query_id, and the epoch ID identify the output
/// of a micro-batch, also when other queries write to the same destination.
template <typename FactoryT>
class StreamingWrite {
 public:
  /// The configuration from which each task attempt opens its writer.
  using Factory = FactoryT;
  /// The message with which a task attempt reports its staged output to the driver.
  using Message = typename FactoryT::Message;

  /// Whether the write supports WriteMode::kTruncate, for the output mode "complete", which adds
  /// the kTruncate capability.
  static constexpr bool kSupportsTruncate = false;

  virtual ~StreamingWrite() = default;

  /// Returns the configuration of the task writers. The SDK serializes it once, with the schema of
  /// the input, and caches the result, including a failure.
  virtual arrow::Result<Factory> CreateWriterFactory() const = 0;

  /// Publishes the winning attempt of each input partition as the output of epoch `epoch_id`,
  /// unless that epoch is already committed.
  virtual arrow::Status Commit(int64_t epoch_id, std::vector<Message> messages) = 0;

  /// Aborts the micro-batch after an input, task, or driver commit failure. Nothing stands for a
  /// partition whose commit message did not reach the driver, or cannot be decoded.
  virtual arrow::Status Abort(int64_t epoch_id, std::vector<std::optional<Message>> messages) = 0;

 protected:
  StreamingWrite() = default;
  StreamingWrite(const StreamingWrite&) = default;
  StreamingWrite(StreamingWrite&&) = default;
  StreamingWrite& operator=(const StreamingWrite&) = default;
  StreamingWrite& operator=(StreamingWrite&&) = default;
};

}  // namespace spark::native

// SPARK_EXPORT_DATA_SOURCE and the implementation of the C ABI.
#include "spark/native/internal/export.h"
