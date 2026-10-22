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
// Spark never calls a handle concurrently, so these objects have no locks. Calls on different
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

/// A planned scan, which the native partitions and serialize_reader functions return.
struct EncodedPlan {
  std::string state;
  std::vector<std::string> partitions;
};

/// The driver planner of a batch scan: it negotiates pushdown, then plans the scan once, and
/// caches the plan or the first failure.
template <typename B>
class Planner {
 public:
  Planner(B builder, std::shared_ptr<arrow::Schema> schema)
      : builder_(std::move(builder)), schema_(std::move(schema)) {}

  /// Pushes parsed predicates. Nothing stands for one that did not parse, which the connector
  /// receives as kUnknown and which stays unsupported whatever it returns.
  arrow::Result<std::vector<FilterSupport>> PushPredicates(
      std::vector<std::optional<Predicate>> parsed) {
    std::vector<Predicate> predicates;
    predicates.reserve(parsed.size());
    for (auto& predicate : parsed) {
      predicates.push_back(predicate ? std::move(*predicate) : Predicate());
    }
    ARROW_ASSIGN_OR_RAISE(
        std::vector<FilterSupport> support,
        Negotiate([&](B& builder) -> arrow::Result<std::vector<FilterSupport>> {
          ARROW_ASSIGN_OR_RAISE(std::vector<FilterSupport> result, builder.PushFilters(predicates));
          if (result.size() != predicates.size()) {
            return arrow::Status::Invalid("PushFilters must return one result per predicate");
          }
          return result;
        }));
    for (size_t index = 0; index < support.size(); ++index) {
      if (!parsed[index]) {
        support[index] = FilterSupport::kUnsupported;
      }
      has_residual_ |= support[index] != FilterSupport::kExact;
    }
    return support;
  }

  arrow::Result<bool> PushLimit(int64_t limit) {
    bool has_residual = has_residual_;
    return Negotiate([&](B& builder) -> arrow::Result<bool> {
      if (has_residual) {
        return false;
      }
      ARROW_ASSIGN_OR_RAISE(LimitSupport support, builder.PushLimit(limit));
      return support == LimitSupport::kGlobal;
    });
  }

  arrow::Result<bool> PruneColumns(const std::vector<std::string>& columns) {
    arrow::FieldVector fields;
    for (const std::string& column : columns) {
      int index = schema_->GetFieldIndex(column);
      if (index < 0) {
        return arrow::Status::Invalid("Unable to get field index: '", column,
                                      "' is not a column of ", schema_->ToString());
      }
      fields.push_back(schema_->field(index));
    }
    auto schema = arrow::schema(std::move(fields), schema_->metadata());
    ARROW_ASSIGN_OR_RAISE(bool pruned,
                          Negotiate([&](B& builder) { return builder.PruneColumns(columns); }));
    if (pruned) {
      schema_ = std::move(schema);
    }
    return pruned;
  }

  arrow::Result<std::shared_ptr<const EncodedPlan>> Planned() {
    if (plan_ != nullptr) {
      return plan_;
    }
    ARROW_RETURN_NOT_OK(failure_);
    arrow::Result<std::shared_ptr<const EncodedPlan>> result =
        Guard([&]() -> arrow::Result<std::shared_ptr<const EncodedPlan>> {
          ARROW_ASSIGN_OR_RAISE(auto plan, builder_->Plan());
          auto encoded = std::make_shared<EncodedPlan>();
          ARROW_ASSIGN_OR_RAISE(encoded->state,
                                EncodeState(/*streaming=*/false, *schema_, plan.factory));
          for (const auto& partition : plan.partitions) {
            ARROW_ASSIGN_OR_RAISE(std::string bytes, Encode(partition));
            encoded->partitions.push_back(std::move(bytes));
          }
          return encoded;
        });
    // Planning consumes the builder, whatever its result.
    builder_.reset();
    if (!result.ok()) {
      failure_ = result.status();
      return failure_;
    }
    plan_ = *result;
    return plan_;
  }

 private:
  template <typename F>
  auto Negotiate(F&& action) -> decltype(action(std::declval<B&>())) {
    if (plan_ != nullptr) {
      return arrow::Status::Invalid("scan has already been planned");
    }
    ARROW_RETURN_NOT_OK(failure_);
    auto result = Guard([&] { return action(*builder_); });
    if (!result.ok()) {
      // A partially mutated builder must never be reused after a failure.
      failure_ = StatusOf(result);
      builder_.reset();
    }
    return result;
  }

  static arrow::Status StatusOf(const arrow::Status& status) { return status; }

  template <typename T>
  static arrow::Status StatusOf(const arrow::Result<T>& result) {
    return result.status();
  }

  std::optional<B> builder_;
  std::shared_ptr<const EncodedPlan> plan_;
  // The first failure, which every later call returns.
  arrow::Status failure_;
  std::shared_ptr<arrow::Schema> schema_;
  bool has_residual_ = false;
};

/// The driver state of the stream of a streaming query.
template <typename M>
class StreamReader {
 public:
  using Offset = typename M::Offset;

  StreamReader(M stream, std::shared_ptr<arrow::Schema> schema)
      : stream_(std::move(stream)), schema_(std::move(schema)) {}

  arrow::Result<std::string> InitialOffset() {
    return Call([&]() -> arrow::Result<std::string> {
      ARROW_ASSIGN_OR_RAISE(Offset offset, stream_.InitialOffset());
      return EncodeOffset(offset);
    });
  }

  arrow::Result<std::string> LatestOffset() {
    return Call([&]() -> arrow::Result<std::string> {
      ARROW_ASSIGN_OR_RAISE(Offset offset, stream_.LatestOffset());
      return EncodeOffset(offset);
    });
  }

  /// Plans a micro-batch, and keeps its encoded state for serialize_stream_reader.
  arrow::Result<std::vector<std::string>> Plan(std::string_view start, std::string_view end) {
    std::shared_ptr<const std::string> state;
    ARROW_ASSIGN_OR_RAISE(
        std::vector<std::string> partitions, Call([&]() -> arrow::Result<std::vector<std::string>> {
          ARROW_ASSIGN_OR_RAISE(Offset from, DecodeOffset<Offset>(start));
          ARROW_ASSIGN_OR_RAISE(Offset to, DecodeOffset<Offset>(end));
          ARROW_ASSIGN_OR_RAISE(auto plan, stream_.Plan(from, to));
          std::vector<std::string> encoded;
          for (const auto& partition : plan.partitions) {
            ARROW_ASSIGN_OR_RAISE(std::string bytes, Encode(partition));
            encoded.push_back(std::move(bytes));
          }
          ARROW_ASSIGN_OR_RAISE(std::string bytes,
                                EncodeState(/*streaming=*/true, *schema_, plan.factory));
          state = std::make_shared<const std::string>(std::move(bytes));
          return encoded;
        }));
    planned_ = std::move(state);
    return partitions;
  }

  arrow::Result<std::shared_ptr<const std::string>> Serialized() const {
    ARROW_RETURN_NOT_OK(failure_);
    if (planned_ == nullptr) {
      return arrow::Status::Invalid("no micro-batch has been planned");
    }
    return planned_;
  }

  arrow::Status CommitOffset(std::string_view end) {
    return Call([&]() -> arrow::Status {
      ARROW_ASSIGN_OR_RAISE(Offset offset, DecodeOffset<Offset>(end));
      return stream_.Commit(offset);
    });
  }

 private:
  /// Calls the stream, unless an earlier call failed, and keeps the first failure.
  template <typename F>
  auto Call(F&& action) -> decltype(action()) {
    ARROW_RETURN_NOT_OK(failure_);
    auto result = Guard(std::forward<F>(action));
    if (!result.ok()) {
      if constexpr (std::is_same_v<decltype(result), arrow::Status>) {
        failure_ = result;
      } else {
        failure_ = result.status();
      }
    }
    return result;
  }

  M stream_;
  std::shared_ptr<arrow::Schema> schema_;
  // The state of the last planned micro-batch, which serialize_stream_reader returns.
  std::shared_ptr<const std::string> planned_;
  // The first failure, which stops the query: every later call returns it.
  arrow::Status failure_;
};

/// Checks the epoch ID that Spark passes with a write: -1 for a batch write, and the ID of the
/// micro-batch, which is not negative, for a streaming write.
inline arrow::Status CheckEpoch(bool streaming, int64_t epoch_id) {
  if (streaming && epoch_id < 0) {
    return arrow::Status::Invalid("the write of a micro-batch requires its epoch ID, not ",
                                  epoch_id);
  }
  if (!streaming && epoch_id != -1) {
    return arrow::Status::Invalid("a batch write requires epoch ID -1, not ", epoch_id);
  }
  return arrow::Status::OK();
}

/// The phases of a driver or task writer, whose failures and results the SDK caches.
enum class Phase { kActive, kCommitted, kFailed, kAborted };

inline arrow::Status Active(Phase phase, const arrow::Status& failure) {
  switch (phase) {
    case Phase::kActive:
      return arrow::Status::OK();
    case Phase::kFailed:
      return failure;
    case Phase::kCommitted:
      return arrow::Status::Invalid("writer was already committed");
    case Phase::kAborted:
      break;
  }
  return arrow::Status::Invalid("writer was already aborted");
}

/// The driver writer of a batch write, or of the write of a micro-batch, which share the writer
/// handles of the ABI. The driver keeps it until close, including after a failed commit.
class WriterHandle {
 public:
  virtual ~WriterHandle() = default;
  virtual arrow::Result<std::shared_ptr<const std::string>> Serialized() = 0;
  virtual arrow::Status Commit(int64_t epoch_id,
                               const std::vector<std::optional<std::string_view>>& messages) = 0;
  virtual arrow::Status Abort(int64_t epoch_id,
                              const std::vector<std::optional<std::string_view>>& messages) = 0;
};

template <typename W, bool kStreaming>
class DriverWriter final : public WriterHandle {
 public:
  using Message = typename W::Message;

  DriverWriter(W write, std::shared_ptr<arrow::Schema> schema)
      : write_(std::move(write)), schema_(std::move(schema)) {}

  /// Creates and serializes the writer factory once, caching the result.
  arrow::Result<std::shared_ptr<const std::string>> Serialized() override {
    if (serialized_) {
      return *serialized_;
    }
    ARROW_RETURN_NOT_OK(Active(phase_, failure_));
    arrow::Result<std::shared_ptr<const std::string>> result =
        Guard([&]() -> arrow::Result<std::shared_ptr<const std::string>> {
          ARROW_RETURN_NOT_OK(ValidateSchema(*schema_));
          ARROW_ASSIGN_OR_RAISE(auto factory, write_.CreateWriterFactory());
          ARROW_ASSIGN_OR_RAISE(std::string state, EncodeState(kStreaming, *schema_, factory));
          return std::make_shared<const std::string>(std::move(state));
        });
    if (!result.ok()) {
      phase_ = Phase::kFailed;
      failure_ = result.status();
    }
    serialized_ = result;
    return result;
  }

  arrow::Status Commit(int64_t epoch_id,
                       const std::vector<std::optional<std::string_view>>& messages) override {
    ARROW_RETURN_NOT_OK(CheckEpoch(kStreaming, epoch_id));
    if (phase_ == Phase::kCommitted) {
      return arrow::Status::OK();
    }
    ARROW_RETURN_NOT_OK(Active(phase_, failure_));
    arrow::Status result = Guard([&]() -> arrow::Status {
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
    phase_ = result.ok() ? Phase::kCommitted : Phase::kFailed;
    failure_ = result;
    return result;
  }

  arrow::Status Abort(int64_t epoch_id,
                      const std::vector<std::optional<std::string_view>>& messages) override {
    ARROW_RETURN_NOT_OK(CheckEpoch(kStreaming, epoch_id));
    if (phase_ == Phase::kCommitted) {
      return arrow::Status::OK();
    }
    if (phase_ == Phase::kAborted) {
      return failure_;
    }
    arrow::Status result = Guard([&]() -> arrow::Status {
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
    phase_ = Phase::kAborted;
    failure_ = result;
    return result;
  }

 private:
  W write_;
  std::shared_ptr<arrow::Schema> schema_;
  std::optional<arrow::Result<std::shared_ptr<const std::string>>> serialized_;
  Phase phase_ = Phase::kActive;
  // The failure of a failed writer, or the result of the abort of an aborted one.
  arrow::Status failure_;
};

/// Imports the batch that Spark passes to write, moving its Arrow structs.
inline arrow::Result<std::shared_ptr<arrow::RecordBatch>> ImportBatch(
    struct ArrowArray* array, struct ArrowSchema* schema, const arrow::Schema& expected) {
  ARROW_RETURN_NOT_OK(RequireOutput(array));
  ARROW_RETURN_NOT_OK(RequireOutput(schema));
  ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Array> imported, arrow::ImportArray(array, schema));
  if (imported->type_id() != arrow::Type::STRUCT) {
    return arrow::Status::Invalid("write input must be an Arrow struct array");
  }
  if (imported->null_count() != 0) {
    return arrow::Status::Invalid("write input cannot contain null rows");
  }
  ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::RecordBatch> batch,
                        arrow::RecordBatch::FromStructArray(imported));
  ARROW_ASSIGN_OR_RAISE(bool same, SameSparkTypes(*batch->schema(), expected));
  if (!same) {
    return arrow::Status::Invalid("write batch schema does not match the planned schema");
  }
  return batch;
}

/// The writer of a task, which encodes its own commit messages, so that one handle type holds the
/// writers of batch and streaming writes, whatever their messages.
class TaskWriter {
 public:
  virtual ~TaskWriter() = default;

  /// Imports owned Arrow data and passes it to the writer.
  arrow::Status Write(struct ArrowArray* array, struct ArrowSchema* schema) {
    ARROW_RETURN_NOT_OK(Active(phase_, failure_));
    arrow::Status result = Guard([&]() -> arrow::Status {
      ARROW_ASSIGN_OR_RAISE(auto batch, ImportBatch(array, schema, *schema_));
      return WriteBatch(std::move(batch));
    });
    if (!result.ok()) {
      phase_ = Phase::kFailed;
      failure_ = result;
    }
    return result;
  }

  /// Commits the task once, and caches its serialized message, or its failure.
  arrow::Result<std::shared_ptr<const std::string>> Commit() {
    if (phase_ == Phase::kCommitted) {
      return message_;
    }
    ARROW_RETURN_NOT_OK(Active(phase_, failure_));
    arrow::Result<std::string> result = Guard([&] { return CommitWriter(); });
    if (!result.ok()) {
      phase_ = Phase::kFailed;
      failure_ = result.status();
      return failure_;
    }
    phase_ = Phase::kCommitted;
    message_ = std::make_shared<const std::string>(std::move(result).ValueUnsafe());
    return message_;
  }

  /// Aborts an unfinished task once, including after a failed write or commit.
  arrow::Status Abort() {
    if (phase_ == Phase::kCommitted) {
      return arrow::Status::OK();
    }
    if (phase_ == Phase::kAborted) {
      return failure_;
    }
    arrow::Status result = Guard([&] { return AbortWriter(); });
    phase_ = Phase::kAborted;
    failure_ = result;
    return result;
  }

 protected:
  explicit TaskWriter(std::shared_ptr<arrow::Schema> schema) : schema_(std::move(schema)) {}

  virtual arrow::Status WriteBatch(std::shared_ptr<arrow::RecordBatch> batch) = 0;
  virtual arrow::Result<std::string> CommitWriter() = 0;
  virtual arrow::Status AbortWriter() = 0;

 private:
  std::shared_ptr<arrow::Schema> schema_;
  Phase phase_ = Phase::kActive;
  std::shared_ptr<const std::string> message_;
  // The failure of a failed writer, or the result of the abort of an aborted one.
  arrow::Status failure_;
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

/// Restores the executor reader of a partition with the factory F, using only serialized values:
/// no source, planner, stream, or driver handle is created or consulted.
template <typename F>
arrow::Status OpenPartition(const SparkDsTaskContext* task, const State& state,
                            std::string_view partition, struct ArrowArrayStream* out) {
  ARROW_ASSIGN_OR_RAISE(F factory, FromJson<F>(state.factory));
  ARROW_RETURN_NOT_OK(ValidateSchema(*state.schema));
  ARROW_ASSIGN_OR_RAISE(TaskContext context, ImportTaskContext(task));
  ARROW_ASSIGN_OR_RAISE(auto value, Decode<typename F::Partition>(partition));
  ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::RecordBatchReader> reader, Guard([&] {
                          return factory.Open(value, ReaderContext(state.schema, context));
                        }));
  if (reader == nullptr) {
    return arrow::Status::Invalid("PartitionReaderFactory::Open returned a null reader");
  }
  return ExportedStream::Export(std::move(reader), state.schema, out);
}

/// Restores the task writer of a write with the factory F, without consulting any driver state.
template <typename F>
arrow::Result<TaskWriter*> OpenTaskWriter(const SparkDsTaskContext* task, const State& state,
                                          std::optional<int64_t> epoch_id) {
  ARROW_ASSIGN_OR_RAISE(F factory, FromJson<F>(state.factory));
  ARROW_RETURN_NOT_OK(ValidateSchema(*state.schema));
  ARROW_ASSIGN_OR_RAISE(TaskContext context, ImportTaskContext(task));
  WriterContext writer_context(state.schema, context);
  writer_context.epoch_id = epoch_id;
  ARROW_ASSIGN_OR_RAISE(std::unique_ptr<DataWriter<typename F::Message>> writer,
                        Guard([&] { return factory.Open(writer_context); }));
  if (writer == nullptr) {
    return arrow::Status::Invalid("DataWriterFactory::Open returned a null writer");
  }
  return new TypedTaskWriter<typename F::Message>(state.schema, std::move(writer));
}

}  // namespace spark::native::internal
