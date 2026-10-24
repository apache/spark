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

// A write-only JSONL sink demonstrating task staging, atomic manifest commits, and exactly-once
// streaming writes.
//
// This example requires one local or shared POSIX filesystem. It is not an object-store connector,
// a concurrent writer protocol, or a durable table format.

#pragma once

#include <cstdint>
#include <filesystem>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "spark/native/data_source.h"

namespace cpp_sdk_sink {

/// A published data file, relative to the root of the sink.
struct DataFile {
  std::string path;
  uint64_t rows = 0;
};
NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE(DataFile, path, rows)

/// The published state of the sink.
struct Manifest {
  int64_t version = 1;
  std::vector<DataFile> files;
  /// The last epoch that each streaming query committed, by the ID of the query.
  std::map<std::string, int64_t> epochs;
};

/// Reads the manifest of the sink at `root`, which is empty before the first write.
arrow::Result<Manifest> ReadManifest(const std::filesystem::path& root);

/// The columns that the sink accepts: id BIGINT and label STRING, in this order.
std::shared_ptr<arrow::Schema> SinkSchema();

/// The message with which a task attempt reports its staged file to the driver.
struct CommitMessage {
  std::string operation;
  std::string filename;
  int32_t partition = 0;
  uint64_t rows = 0;
};
NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE(CommitMessage, operation, filename, partition, rows)

/// Serializable values only: the driver lock and open files stay on their hosts.
struct JsonWriterFactory : spark::native::DataWriterFactory<CommitMessage> {
  std::string staging;
  std::string operation;

  arrow::Result<std::unique_ptr<spark::native::DataWriter<CommitMessage>>> Open(
      const spark::native::WriterContext& context) const override;
};
NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE(JsonWriterFactory, staging, operation)

/// The lock of a destination, which a write holds for its whole operation, including all task
/// attempts.
class WriteLock {
 public:
  static arrow::Result<WriteLock> Acquire(const std::filesystem::path& root);

  WriteLock(WriteLock&& other) noexcept : path_(std::exchange(other.path_, {})) {}
  WriteLock& operator=(WriteLock&& other) = delete;
  ~WriteLock();

 private:
  explicit WriteLock(std::filesystem::path path) : path_(std::move(path)) {}

  std::filesystem::path path_;
};

/// The files of one write operation: its staging directory, where task attempts create their
/// files, the directory of its published objects, and its temporary manifest.
struct Operation {
  std::filesystem::path root;
  std::string id;
  std::filesystem::path staging;
  std::filesystem::path objects;
  std::filesystem::path temporary_manifest;
  Manifest previous;
  bool truncate = false;
  bool committed = false;
  bool sealed = false;
  std::optional<WriteLock> lock;

  Operation() = default;
  Operation(const Operation&) = delete;
  Operation& operator=(const Operation&) = delete;
  // Spark can close a write without abort, for example when serializing its factory fails, so an
  // unfinished write also cleans up here.
  ~Operation();

  /// Publishes the winning attempts, and records the epoch of a streaming query.
  arrow::Status Publish(const std::vector<CommitMessage>& messages,
                        std::optional<std::pair<std::string, int64_t>> epoch);

  /// Publishes nothing, for an epoch whose output is already published: removes what this write
  /// staged, and releases the lock.
  arrow::Status Skip();

  /// Removes what this operation staged or sealed, but never published data.
  arrow::Status Cleanup() const;
};

// The SDK calls each method at most once, never after a successful commit or an abort, so the
// coordinators do not track their own phase.
class JsonWrite : public spark::native::BatchWrite<JsonWriterFactory> {
 public:
  // The sink also supports overwrites.
  static constexpr bool kSupportsTruncate = true;

  static arrow::Result<JsonWrite> Begin(const std::filesystem::path& root,
                                        spark::native::WriteMode mode);

  arrow::Result<JsonWriterFactory> CreateWriterFactory() const override;
  arrow::Status Commit(std::vector<CommitMessage> messages) override;
  arrow::Status Abort(std::vector<std::optional<CommitMessage>> messages) override;

  Operation& operation() const { return *operation_; }

 private:
  explicit JsonWrite(std::unique_ptr<Operation> operation) : operation_(std::move(operation)) {}

  std::unique_ptr<Operation> operation_;
};

/// The write of a micro-batch, exactly once: the manifest records the last epoch that each
/// streaming query committed, so an epoch that Spark commits again after a restart adds nothing.
class JsonStreamingWrite : public spark::native::StreamingWrite<JsonWriterFactory> {
 public:
  // The output mode complete replaces the output of the previous micro-batches.
  static constexpr bool kSupportsTruncate = true;

  JsonStreamingWrite(JsonWrite write, std::string query_id)
      : write_(std::move(write)), query_id_(std::move(query_id)) {}

  arrow::Result<JsonWriterFactory> CreateWriterFactory() const override;
  arrow::Status Commit(int64_t epoch_id, std::vector<CommitMessage> messages) override;
  arrow::Status Abort(int64_t epoch_id,
                      std::vector<std::optional<CommitMessage>> messages) override;

  Operation& operation() const { return write_.operation(); }

 private:
  JsonWrite write_;
  std::string query_id_;
};

/// Construction and capability discovery do not access the filesystem.
class JsonSink : public spark::native::DataSource {
 public:
  // A write-only sink: its reads stay Unsupported.
  using BatchWrite = JsonWrite;
  using StreamingWrite = JsonStreamingWrite;

  static arrow::Result<JsonSink> Make(const spark::native::CreateInfo& info);

  arrow::Result<std::shared_ptr<arrow::Schema>> InferSchema() const override;
  spark::native::Capabilities capabilities() const override;
  arrow::Result<JsonWrite> NewWrite(const spark::native::WriteInfo& info) const;
  arrow::Result<JsonStreamingWrite> NewStreamingWrite(
      const spark::native::StreamingWriteInfo& info) const;

 private:
  JsonSink(std::filesystem::path path, bool readonly)
      : path_(std::move(path)), readonly_(readonly) {}

  std::filesystem::path path_;
  bool readonly_;
};

}  // namespace cpp_sdk_sink
