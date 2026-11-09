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

// Experimental C++ SDK for native data sources, over version 1 of the C ABI of Spark,
// spark_datasource.h.
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
// MicroBatchStreamInfo, LatestOffsetInfo, WriteInfo, and StreamingWriteInfo), *Context structs,
// which carry the live TaskContext, on the executors (ReaderContext and WriterContext), and
// HostInfo, which DataSource::Init gets in every JVM, executors included.
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
/// operations, and calls the New* method of an operation only while the capabilities of the
/// instance include it. It passes WriteMode::kTruncate only for a write that declares
/// kSupportsTruncate, while the capabilities include kBatchTruncate or kStreamingTruncate.
/// SPARK_EXPORT_DATA_SOURCE checks these declarations when it compiles.
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

  /// The schema to read: the columns that the scan reads, in order, which are those of
  /// ScanBuilder::PruneColumns for a batch scan, and of MicroBatchStreamInfo::schema for a
  /// micro-batch.
  std::shared_ptr<arrow::Schema> schema;
  /// The task that reads the partition.
  TaskContext task;
};

/// Serializable configuration from which executors open partition readers.
///
/// The factory carries configuration or a snapshot descriptor; open connections and other live
/// resources in Open. Spark passes the schema to read with each partition, so the factory need not
/// carry it.
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
  /// must have that schema. It may also have more columns than the schema to read, such as all the
  /// columns of the source: the SDK keeps those of the schema to read, by name. The SDK checks
  /// Spark-compatible Arrow types, the schema to read and its nullability, and zero array offsets
  /// recursively. It reports incompatible layouts instead of copying buffers.
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
/// Spark calls PushLimit and PruneColumns, each at most once and in this order, then Plan, and
/// destroys the builder. After a call fails, it makes no other call. It destroys a builder without
/// planning it when it does not read the scan, for example to only explain a query. Only the
/// serialized factory and partitions reach the executors.
template <typename FactoryT>
class ScanBuilder {
 public:
  using Factory = FactoryT;

  virtual ~ScanBuilder() = default;

  /// Receives the LIMIT of the query. Returns LimitSupport::kGlobal when the whole scan then
  /// returns at most `limit` rows, or LimitSupport::kUnsupported, the default, when it may return
  /// more; Spark applies the limit again after reading either way. Spark pushes a limit only when
  /// the query does not filter the rows, since it filters them after reading.
  virtual arrow::Result<LimitSupport> PushLimit(int64_t limit) {
    (void)limit;
    return LimitSupport::kUnsupported;
  }

  /// Tells the scan the top-level columns that the query needs, in order, so that it can plan to
  /// read only them. There may be none, for example for SELECT count(*): the batches then only
  /// have a number of rows. Readers get these columns in ReaderContext::schema whether or not the
  /// scan implements this method.
  virtual arrow::Status PruneColumns(const std::vector<std::string>& columns) {
    (void)columns;
    return arrow::Status::OK();
  }

  /// Plans the scan. Spark calls it at most once, as the last call on the builder, so it may move
  /// out of the builder.
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
  MicroBatchStreamInfo(std::shared_ptr<arrow::Schema> schema, std::string checkpoint_location)
      : schema(std::move(schema)), checkpoint_location(std::move(checkpoint_location)) {}

  /// The columns to read: those of the schema of the source, or of one that the user specified, or
  /// only the top-level columns of it that the query needs. The readers of the micro-batches get
  /// it in ReaderContext::schema.
  std::shared_ptr<arrow::Schema> schema;
  /// A directory in the checkpoint of the query that is reserved for this source, such as
  /// file:/tmp/checkpoint/sources/0, where the stream may keep what it needs to recover from a
  /// failure. It stays the same when the query restarts. It is a fully qualified Hadoop path, whose
  /// characters are not escaped as in a URI, on a file system that the library may not be able to
  /// access, such as s3a. Spark keeps what it needs there under the names that start with _spark,
  /// which the library must not use.
  std::string checkpoint_location;
};

/// A limit on the data of a micro-batch.
class ReadLimit {
 public:
  enum class Kind {
    /// All the available data.
    kAllAvailable,
    /// At most max_rows() rows, at least 1.
    kMaxRows,
  };

  static constexpr ReadLimit AllAvailable() { return ReadLimit(Kind::kAllAvailable, 0); }
  static constexpr ReadLimit MaxRows(int64_t rows) { return ReadLimit(Kind::kMaxRows, rows); }

  constexpr Kind kind() const { return kind_; }
  /// The maximum number of rows of a limit of the kind kMaxRows.
  constexpr int64_t max_rows() const { return max_rows_; }

  constexpr bool operator==(const ReadLimit& other) const = default;

 private:
  constexpr ReadLimit(Kind kind, int64_t max_rows) : kind_(kind), max_rows_(max_rows) {}

  Kind kind_;
  int64_t max_rows_;
};

/// What Spark passes to ask a stream for its latest offset.
template <typename OffsetT>
struct LatestOffsetInfo {
  /// A request without a limit, made outside Spark, for example in a test. Set limit and run_end
  /// for other requests.
  explicit LatestOffsetInfo(OffsetT start) : start(std::move(start)) {}

  /// The offset that the next micro-batch starts after: the end of the previous one, or the
  /// initial offset.
  OffsetT start;
  /// The limit on the data after start: that of MicroBatchStream::DefaultReadLimit, or
  /// ReadLimit::AllAvailable() when Spark reads all the available data, such as for the progress
  /// of the query or with Trigger.Once.
  ReadLimit limit = ReadLimit::AllAvailable();
  /// With Trigger.AvailableNow and a limit, the end of the run: the latest offset must not go
  /// beyond it, so that the run reads the data that was available when it started. It is not the
  /// end of the next micro-batch. Nothing otherwise.
  std::optional<OffsetT> run_end;
};

/// Finds new data and plans the micro-batches of a streaming query, on the driver.
///
/// Spark creates a stream when a streaming query starts, or restarts from its checkpoint, and
/// destroys it when the query stops. For each micro-batch, it asks for the LatestOffset, and when
/// that differs from the end of the previous micro-batch, plans the data in between. Only the
/// serialized factory and partitions of each plan reach the executors, which read them as those of
/// a batch scan.
///
/// Offsets: Spark stores offsets in the checkpoint of the query, as the JSON of OffsetT, which the
/// SDK serializes with nlohmann::json, and compares them by that JSON, so serialize a position to
/// the same JSON each time: nlohmann::json orders the keys of objects, but a type with a custom
/// to_json must be deterministic too. A restarted query passes the offsets of its checkpoint, which
/// an earlier version of the library may have written, to LatestOffset, Plan, and Commit.
///
/// Admission control: DefaultReadLimit limits the data of each micro-batch, typically with an
/// option of the source. Spark passes the limit to LatestOffset, except when a micro-batch reads
/// all the available data, as with Trigger.Once. With Trigger.AvailableNow, a run reads the data
/// that was available when it started: with a limit, in several micro-batches, whose latest offsets
/// must not go beyond LatestOffsetInfo::run_end.
///
/// Lifecycle:
///
/// - Calls never run concurrently.
/// - After a call fails, including with an exception, Spark makes no other call: it stops the
///   query, and destroys the stream.
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
  /// reads the data after it. Spark keeps it in the checkpoint, and asks only when the checkpoint
  /// does not have it yet, so a query that restarts before its first micro-batch is committed
  /// starts from the same offset.
  virtual arrow::Result<Offset> InitialOffset() = 0;

  /// Returns the limit on the data of each micro-batch. Spark asks once, when the query starts.
  /// The default, ReadLimit::AllAvailable(), reads all the available data in each micro-batch.
  virtual arrow::Result<ReadLimit> DefaultReadLimit() const { return ReadLimit::AllAvailable(); }

  /// Returns the end of the next micro-batch: the offset of the most recent data after info.start,
  /// within info.limit, and not beyond info.run_end. Return start when there is no new data. When
  /// there is, return an offset after start, even if the next unit of data, such as a file, is
  /// larger than the limit: Spark sees no new data otherwise, and never reads further.
  ///
  /// With a limit, Spark also asks without a limit for some micro-batches, to report the progress
  /// of the query, so do not count the calls as micro-batches, or reserve data in them.
  virtual arrow::Result<Offset> LatestOffset(const LatestOffsetInfo<Offset>& info) = 0;

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

/// The writer of one task attempt. Spark destroys it after commit or abort.
///
/// Lifecycle: Spark calls the methods in this order, so implementations need not track their own
/// phase or guard against repeated calls.
///
/// - Write runs zero or more times, before Commit or Abort, and never after a call has failed.
/// - Commit runs at most once, never after a failed Write or after Abort.
/// - Abort runs at most once, never after a Commit whose message reached Spark. It may follow a
///   failed Write or Commit, including a Commit that returned a message that the SDK could not
///   serialize, so it must handle a task attempt that already finished its output.
/// - The destructor runs exactly once when Spark closes the writer, possibly without Commit or
///   Abort.
/// - The writer of a task attempt that Spark kills, such as a speculative one, may still write,
///   abort, and be destroyed after the driver committed or aborted the write.
/// - An exception in any method is reported as that method's failure.
template <typename MessageT>
class DataWriter {
 public:
  using Message = MessageT;

  virtual ~DataWriter() = default;

  /// Takes an imported Arrow batch, which may be retained after this call, but not after the writer
  /// is destroyed: its buffers belong to the data writer of Spark, which frees them once it closes
  /// the writer, so do not pass the batch to a thread or an object that outlives the writer. It has
  /// the schema of WriterContext::schema, and at most spark.sql.execution.arrow.maxRecordsPerBatch
  /// rows. When Spark writes the batches of a native scan without converting them to rows, it
  /// copies their values as the scan returned them, without the checks of a conversion, such as the
  /// precision of a decimal. Process batches incrementally and check task cancellation.
  virtual arrow::Status Write(std::shared_ptr<arrow::RecordBatch> batch) = 0;

  /// Finishes staging and returns a message for the driver. Do not make the whole write visible
  /// here. On failure, including the failure of the SDK to serialize the message, Spark calls
  /// Abort.
  virtual arrow::Result<Message> Commit() = 0;

  virtual arrow::Status Abort() = 0;
};

/// Serializable configuration for independent task-attempt writers, which
/// BatchWrite::NewWriterFactory or StreamingWrite::NewWriterFactory creates.
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
/// Lifecycle: Spark calls the methods in this order, so implementations need not track their own
/// phase or guard against repeated calls.
///
/// - NewWriterFactory runs once, right after the NewWrite method of the source, before any task
///   opens a writer. When it fails, or the SDK cannot serialize its factory, the write fails and
///   the coordinator is destroyed without Abort, so the destructor must also release what only
///   this write staged.
/// - Commit runs at most once, when every task succeeded. The SDK fails the write without calling
///   it when a task message cannot be decoded.
/// - Abort runs at most once, never after a successful Commit. It follows a failed Commit, or a
///   failed task, also when computing the data to write failed.
/// - The destructor runs exactly once, possibly without Commit or Abort.
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

  /// Whether the write supports WriteMode::kTruncate, for the save mode "overwrite", which adds the
  /// kBatchTruncate capability. A write sets it with `static constexpr bool kSupportsTruncate =
  /// true;`.
  static constexpr bool kSupportsTruncate = false;

  virtual ~BatchWrite() = default;

  /// Returns the configuration of the task writers, which the SDK serializes for them. Include a
  /// unique operation ID when needed; Spark supplies no batch query ID.
  virtual arrow::Result<Factory> NewWriterFactory() const = 0;

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
/// Spark creates a write for each micro-batch, with the calls of a BatchWrite, and passes the
/// epoch ID of the micro-batch to Commit and Abort, and to the task writers in
/// WriterContext::epoch_id. Its destruction is deferred, unlike that of a BatchWrite: Spark
/// destroys the write of a micro-batch only after it creates that of the next one, and the last
/// write of a query once it is no longer referenced, possibly long after the query stops and on
/// another thread. Release what a micro-batch holds, such as a lock on the destination, in Commit
/// and Abort rather than in the destructor.
///
/// Exactly-once output: Spark may commit an epoch again, with a new write, for example when the
/// query restarts after a failure: committing an epoch that is already committed must have no
/// effect. A restarted query may also abort an epoch that an earlier run committed, so Abort must
/// only remove what the task writers of its own write wrote. The ID of the query,
/// StreamingWriteInfo::query_id, and the epoch ID identify the output of a micro-batch, also when
/// other queries write to the same destination.
template <typename FactoryT>
class StreamingWrite {
 public:
  /// The configuration from which each task attempt opens its writer.
  using Factory = FactoryT;
  /// The message with which a task attempt reports its staged output to the driver.
  using Message = typename FactoryT::Message;

  /// Whether the write supports WriteMode::kTruncate, for the output mode "complete", which adds
  /// the kStreamingTruncate capability.
  static constexpr bool kSupportsTruncate = false;

  virtual ~StreamingWrite() = default;

  /// Returns the configuration of the task writers, which the SDK serializes for them.
  virtual arrow::Result<Factory> NewWriterFactory() const = 0;

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
