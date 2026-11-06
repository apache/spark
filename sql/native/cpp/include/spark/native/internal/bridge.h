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

// The objects behind the native handles: driver planners, streams, and writers, and the executor
// task writers, which are restored from serialized values only.
//
// Spark never calls a handle concurrently, and documents the order of the calls on each handle,
// so these objects have no locks, and do not guard against other orders. Calls on different
// handles run concurrently, and share no state.

#pragma once

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <arrow/array.h>
#include <arrow/c/bridge.h>
#include <arrow/record_batch.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/type.h>

#include "spark/native/internal/abi.h"
#include "spark/native/internal/arrow.h"
#include "spark/native/internal/serde.h"

namespace spark::native::internal {

/// A data source, with the capabilities that create returned.
template <typename S>
struct SourceHandle {
  S source;
  Capabilities capabilities;
};

/// The partitions of a scan or a micro-batch, and the state of its reader: its factory.
struct EncodedPlan {
  std::string state;
  std::vector<std::string> partitions;
};

/// Encodes the factory of a plan, as the state of its reader, and each of its partitions.
template <typename Plan>
arrow::Result<EncodedPlan> EncodePlan(const Plan& plan) {
  EncodedPlan encoded;
  ARROW_ASSIGN_OR_RAISE(encoded.state, Encode(plan.factory));
  for (const auto& partition : plan.partitions) {
    ARROW_ASSIGN_OR_RAISE(std::string bytes, Encode(partition));
    encoded.partitions.push_back(std::move(bytes));
  }
  return encoded;
}

/// The driver planner of a batch scan: it negotiates pushdown, then plans the scan, which consumes
/// the builder.
template <typename B>
class Planner {
 public:
  explicit Planner(B builder) : builder_(std::move(builder)) {}

  arrow::Result<bool> PushLimit(int64_t limit) {
    ARROW_ASSIGN_OR_RAISE(B * builder, Builder());
    ARROW_ASSIGN_OR_RAISE(LimitSupport support, Guard([&] { return builder->PushLimit(limit); }));
    return support == LimitSupport::kGlobal;
  }

  arrow::Status PruneColumns(const std::vector<std::string>& columns) {
    ARROW_ASSIGN_OR_RAISE(B * builder, Builder());
    return Guard([&] { return builder->PruneColumns(columns); });
  }

  arrow::Result<EncodedPlan> Plan() {
    ARROW_ASSIGN_OR_RAISE(B * builder, Builder());
    arrow::Result<EncodedPlan> result = Guard([&]() -> arrow::Result<EncodedPlan> {
      ARROW_ASSIGN_OR_RAISE(auto plan, builder->Plan());
      return EncodePlan(plan);
    });
    // Planning consumes the builder, whatever its result.
    builder_.reset();
    return result;
  }

 private:
  arrow::Result<B*> Builder() {
    if (!builder_) {
      return arrow::Status::Invalid("the scan has already been planned");
    }
    return &*builder_;
  }

  std::optional<B> builder_;
};

/// The driver state of the stream of a streaming query, which decodes and encodes its offsets.
template <typename M>
class StreamReader {
 public:
  using Offset = typename M::Offset;

  explicit StreamReader(M stream) : stream_(std::move(stream)) {}

  arrow::Result<std::string> InitialOffset() {
    return Guard([&]() -> arrow::Result<std::string> {
      ARROW_ASSIGN_OR_RAISE(Offset offset, stream_.InitialOffset());
      return EncodeOffset(offset);
    });
  }

  arrow::Result<ReadLimit> DefaultReadLimit() const {
    return Guard([&] { return stream_.DefaultReadLimit(); });
  }

  arrow::Result<std::string> LatestOffset(const LatestOffsetInfo<std::string>& request) {
    return Guard([&]() -> arrow::Result<std::string> {
      ARROW_ASSIGN_OR_RAISE(Offset start, DecodeOffset<Offset>(request.start));
      LatestOffsetInfo<Offset> info(std::move(start));
      info.limit = request.limit;
      if (request.run_end) {
        ARROW_ASSIGN_OR_RAISE(info.run_end, DecodeOffset<Offset>(*request.run_end));
      }
      ARROW_ASSIGN_OR_RAISE(Offset offset, stream_.LatestOffset(info));
      return EncodeOffset(offset);
    });
  }

  arrow::Result<EncodedPlan> Plan(std::string_view start, std::string_view end) {
    return Guard([&]() -> arrow::Result<EncodedPlan> {
      ARROW_ASSIGN_OR_RAISE(Offset from, DecodeOffset<Offset>(start));
      ARROW_ASSIGN_OR_RAISE(Offset to, DecodeOffset<Offset>(end));
      ARROW_ASSIGN_OR_RAISE(auto plan, stream_.Plan(from, to));
      return EncodePlan(plan);
    });
  }

  arrow::Status CommitOffset(std::string_view end) {
    return Guard([&]() -> arrow::Status {
      ARROW_ASSIGN_OR_RAISE(Offset offset, DecodeOffset<Offset>(end));
      return stream_.Commit(offset);
    });
  }

 private:
  M stream_;
};

/// The driver writer of a batch write, or of the write of a micro-batch, which share the writer
/// handles of the ABI. The driver keeps it until close, including after a failed commit.
class WriterHandle {
 public:
  virtual ~WriterHandle() = default;
  virtual arrow::Status Commit(int64_t epoch_id,
                               const std::vector<std::optional<std::string_view>>& messages) = 0;
  virtual arrow::Status Abort(int64_t epoch_id,
                              const std::vector<std::optional<std::string_view>>& messages) = 0;
};

/// A writer, with the state that its task writers need: its serialized writer factory.
struct CreatedWriter {
  std::unique_ptr<WriterHandle> writer;
  std::string state;
};

template <typename W, bool kStreaming>
class DriverWriter final : public WriterHandle {
 public:
  using Message = typename W::Message;

  /// Wraps a write, and serializes its writer factory. On failure, the write is destroyed.
  static arrow::Result<CreatedWriter> Make(W write) {
    auto writer = std::unique_ptr<DriverWriter>(new DriverWriter(std::move(write)));
    ARROW_ASSIGN_OR_RAISE(std::string state, Guard([&]() -> arrow::Result<std::string> {
                            ARROW_ASSIGN_OR_RAISE(auto factory,
                                                  writer->write_.CreateWriterFactory());
                            return Encode(factory);
                          }));
    return CreatedWriter{std::move(writer), std::move(state)};
  }

  arrow::Status Commit(int64_t epoch_id,
                       const std::vector<std::optional<std::string_view>>& messages) override {
    return Guard([&]() -> arrow::Status {
      std::vector<Message> decoded;
      decoded.reserve(messages.size());
      for (const auto& message : messages) {
        if (!message) {
          return arrow::Status::Invalid("missing committed task message");
        }
        ARROW_ASSIGN_OR_RAISE(Message value, Decode<Message>(*message));
        decoded.push_back(std::move(value));
      }
      if constexpr (kStreaming) {
        return write_.Commit(epoch_id, std::move(decoded));
      } else {
        return write_.Commit(std::move(decoded));
      }
    });
  }

  arrow::Status Abort(int64_t epoch_id,
                      const std::vector<std::optional<std::string_view>>& messages) override {
    return Guard([&]() -> arrow::Status {
      // Abort is the cleanup path: a message that cannot be decoded is passed as nothing instead
      // of preventing the coordinator from cleaning up.
      std::vector<std::optional<Message>> decoded;
      decoded.reserve(messages.size());
      for (size_t partition = 0; partition < messages.size(); ++partition) {
        if (!messages[partition]) {
          decoded.emplace_back(std::nullopt);
          continue;
        }
        arrow::Result<Message> message = Decode<Message>(*messages[partition]);
        if (message.ok()) {
          decoded.emplace_back(std::move(message).ValueUnsafe());
        } else {
          LogWarn("Aborting a write without the commit message of partition " +
                  std::to_string(partition) +
                  ", which cannot be decoded: " + message.status().ToString());
          decoded.emplace_back(std::nullopt);
        }
      }
      if constexpr (kStreaming) {
        return write_.Abort(epoch_id, std::move(decoded));
      } else {
        return write_.Abort(std::move(decoded));
      }
    });
  }

 private:
  explicit DriverWriter(W write) : write_(std::move(write)) {}

  W write_;
};

/// The writer of a task, which encodes its own commit messages, so that one handle type holds the
/// writers of batch and streaming writes, whatever their messages.
class TaskWriter {
 public:
  virtual ~TaskWriter() = default;

  /// Imports the array that Spark passes, which has the schema of the writer, moving it, and
  /// passes it to the writer.
  arrow::Status Write(struct ArrowArray* array) {
    return Guard([&]() -> arrow::Status {
      ARROW_RETURN_NOT_OK(RequireOutput(array));
      ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::RecordBatch> batch,
                            arrow::ImportRecordBatch(array, schema_));
      return WriteBatch(std::move(batch));
    });
  }

  /// Commits the task, and returns its serialized message.
  arrow::Result<std::string> Commit() {
    return Guard([&] { return CommitWriter(); });
  }

  /// Aborts the task, also after a failed write or commit.
  arrow::Status Abort() {
    return Guard([&] { return AbortWriter(); });
  }

 protected:
  explicit TaskWriter(std::shared_ptr<arrow::Schema> schema) : schema_(std::move(schema)) {}

  virtual arrow::Status WriteBatch(std::shared_ptr<arrow::RecordBatch> batch) = 0;
  virtual arrow::Result<std::string> CommitWriter() = 0;
  virtual arrow::Status AbortWriter() = 0;

 private:
  // The schema that create_data_writer got, which every array that write gets has.
  std::shared_ptr<arrow::Schema> schema_;
};

template <typename Message>
class TypedTaskWriter final : public TaskWriter {
 public:
  TypedTaskWriter(std::shared_ptr<arrow::Schema> schema,
                  std::unique_ptr<DataWriter<Message>> writer)
      : TaskWriter(std::move(schema)), writer_(std::move(writer)) {}

 protected:
  arrow::Status WriteBatch(std::shared_ptr<arrow::RecordBatch> batch) override {
    return writer_->Write(std::move(batch));
  }

  arrow::Result<std::string> CommitWriter() override {
    ARROW_ASSIGN_OR_RAISE(Message message, writer_->Commit());
    return Encode(message);
  }

  arrow::Status AbortWriter() override { return writer_->Abort(); }

 private:
  std::unique_ptr<DataWriter<Message>> writer_;
};

/// Opens the executor reader of a partition with the factory F, using only serialized values: no
/// source, planner, stream, or driver handle is created or consulted.
template <typename F>
arrow::Status OpenPartition(const SparkDsTaskContext* task, std::shared_ptr<arrow::Schema> schema,
                            std::string_view state, std::string_view partition,
                            struct ArrowArrayStream* out) {
  ARROW_RETURN_NOT_OK(ValidateSchema(*schema));
  ARROW_ASSIGN_OR_RAISE(F factory, Decode<F>(state));
  ARROW_ASSIGN_OR_RAISE(TaskContext context, ImportTaskContext(task));
  ARROW_ASSIGN_OR_RAISE(auto value, Decode<typename F::Partition>(partition));
  ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::RecordBatchReader> reader,
                        Guard([&] { return factory.Open(value, ReaderContext(schema, context)); }));
  if (reader == nullptr) {
    return arrow::Status::Invalid("PartitionReaderFactory::Open returned a null reader");
  }
  return ExportedStream::Export(std::move(reader), std::move(schema), out);
}

/// Opens the task writer of a write with the factory F, without consulting any driver state.
template <typename F>
arrow::Result<TaskWriter*> OpenTaskWriter(const SparkDsTaskContext* task,
                                          std::shared_ptr<arrow::Schema> schema,
                                          std::string_view state, std::optional<int64_t> epoch_id) {
  ARROW_RETURN_NOT_OK(ValidateSchema(*schema));
  ARROW_ASSIGN_OR_RAISE(F factory, Decode<F>(state));
  ARROW_ASSIGN_OR_RAISE(TaskContext context, ImportTaskContext(task));
  WriterContext writer_context(schema, context);
  writer_context.epoch_id = epoch_id;
  ARROW_ASSIGN_OR_RAISE(std::unique_ptr<DataWriter<typename F::Message>> writer,
                        Guard([&] { return factory.Open(writer_context); }));
  if (writer == nullptr) {
    return arrow::Status::Invalid("DataWriterFactory::Open returned a null writer");
  }
  return new TypedTaskWriter<typename F::Message>(std::move(schema), std::move(writer));
}

}  // namespace spark::native::internal
