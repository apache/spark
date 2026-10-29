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

#include "json_sink.h"

#include <fcntl.h>
#include <unistd.h>

#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <fstream>
#include <set>
#include <sstream>
#include <system_error>

#include <arrow/api.h>

namespace cpp_sdk_sink {

namespace fs = std::filesystem;
using spark::native::Capabilities;
using spark::native::CreateInfo;
using spark::native::WriteMode;

namespace {

std::atomic<uint64_t> next_id{0};

arrow::Status Error(const std::string& action, const fs::path& path, std::error_code error) {
  return arrow::Status::IOError(action, " ", path.string(), ": ", error.message());
}

arrow::Status ErrnoError(const std::string& action, const fs::path& path) {
  return Error(action, path, std::error_code(errno, std::generic_category()));
}

arrow::Status ValidateSchema(const arrow::Schema& schema) {
  if (schema.num_fields() != 2 || schema.field(0)->name() != "id" ||
      !schema.field(0)->type()->Equals(*arrow::int64()) || schema.field(1)->name() != "label" ||
      !schema.field(1)->type()->Equals(*arrow::utf8())) {
    return arrow::Status::Invalid("Expected columns id BIGINT, label STRING, in that order");
  }
  return arrow::Status::OK();
}

std::string UniqueId() {
  auto nanos = std::chrono::duration_cast<std::chrono::nanoseconds>(
                   std::chrono::system_clock::now().time_since_epoch())
                   .count();
  return std::to_string(::getpid()) + "-" + std::to_string(nanos) + "-" +
         std::to_string(next_id.fetch_add(1));
}

arrow::Result<fs::path> UniqueDirectory(const fs::path& parent) {
  for (int attempt = 0; attempt < 16; ++attempt) {
    fs::path path = parent / UniqueId();
    std::error_code error;
    if (fs::create_directory(path, error)) {
      return path;
    }
    if (error) {
      return Error("Cannot create", path, error);
    }
  }
  return arrow::Status::IOError("Could not create a unique write operation directory");
}

/// Whether `name` is the name of a file in a directory, without other path components.
bool IsFilename(const std::string& name) {
  fs::path path(name);
  return !name.empty() && name.find('/') == std::string::npos && name != "." && name != ".." &&
         path.filename() == path;
}

arrow::Status WriteAll(int descriptor, std::string_view data, const fs::path& path) {
  while (!data.empty()) {
    ssize_t written = ::write(descriptor, data.data(), data.size());
    if (written < 0) {
      if (errno == EINTR) {
        continue;
      }
      return ErrnoError("Cannot write", path);
    }
    data.remove_prefix(static_cast<size_t>(written));
  }
  return arrow::Status::OK();
}

/// Creates a file that does not exist yet with `contents`, and syncs it.
arrow::Status WriteNewFile(const fs::path& path, const std::string& contents) {
  int descriptor = ::open(path.c_str(), O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC, 0644);
  if (descriptor < 0) {
    return ErrnoError("Cannot create", path);
  }
  arrow::Status status = WriteAll(descriptor, contents, path);
  if (status.ok() && ::fsync(descriptor) != 0) {
    status = ErrnoError("Cannot sync", path);
  }
  if (::close(descriptor) != 0 && status.ok()) {
    status = ErrnoError("Cannot close", path);
  }
  return status;
}

nlohmann::json ManifestJson(const Manifest& manifest) {
  nlohmann::json json = {{"version", manifest.version}, {"files", manifest.files}};
  if (!manifest.epochs.empty()) {
    json["epochs"] = manifest.epochs;
  }
  return json;
}

/// The writer of one task attempt, which stages its rows as JSON lines in a file of its own.
class JsonTaskWriter : public spark::native::DataWriter<CommitMessage> {
 public:
  JsonTaskWriter(spark::native::TaskContext task, fs::path path, CommitMessage message,
                 int descriptor)
      : task_(task),
        path_(std::move(path)),
        message_(std::move(message)),
        descriptor_(descriptor) {}

  JsonTaskWriter(const JsonTaskWriter&) = delete;
  JsonTaskWriter& operator=(const JsonTaskWriter&) = delete;

  ~JsonTaskWriter() override {
    // A committed file belongs to the driver, which publishes or removes it.
    if (!committed_) {
      Discard();
      ::unlink(path_.c_str());
    }
  }

  // The SDK never writes after a failure, commit, or abort, commits at most once, and never
  // aborts a committed writer, so this writer does not track its phase.
  arrow::Status Write(std::shared_ptr<arrow::RecordBatch> batch) override {
    if (descriptor_ < 0) {
      return arrow::Status::Invalid("The task writer is closed");
    }
    // The factory checked the schema of the context, which every batch has.
    if (batch->column(0)->type_id() != arrow::Type::INT64) {
      return arrow::Status::Invalid("The id column must be an Arrow int64 array");
    }
    if (batch->column(1)->type_id() != arrow::Type::STRING) {
      return arrow::Status::Invalid("The label column must be an Arrow utf8 array");
    }
    const auto& ids = static_cast<const arrow::Int64Array&>(*batch->column(0));
    const auto& labels = static_cast<const arrow::StringArray&>(*batch->column(1));
    for (int64_t row = 0; row < batch->num_rows(); ++row) {
      if (row % 1024 == 0 && task_.IsInterrupted()) {
        return arrow::Status::Cancelled("Task cancelled while writing the sink");
      }
      nlohmann::json line = {
          {"id", ids.IsNull(row) ? nlohmann::json() : nlohmann::json(ids.Value(row))},
          {"label", labels.IsNull(row) ? nlohmann::json() : nlohmann::json(labels.GetView(row))},
      };
      buffer_ += line.dump();
      buffer_ += '\n';
      if (buffer_.size() >= kBufferSize) {
        ARROW_RETURN_NOT_OK(Flush());
      }
    }
    message_.rows += static_cast<uint64_t>(batch->num_rows());
    return arrow::Status::OK();
  }

  arrow::Result<CommitMessage> Commit() override {
    if (task_.IsInterrupted()) {
      return arrow::Status::Cancelled("Task cancelled before committing the sink");
    }
    if (descriptor_ < 0) {
      return arrow::Status::Invalid("The task writer is closed");
    }
    ARROW_RETURN_NOT_OK(Flush());
    if (::fsync(descriptor_) != 0) {
      return ErrnoError("Cannot sync", path_);
    }
    int descriptor = std::exchange(descriptor_, -1);
    if (::close(descriptor) != 0) {
      return ErrnoError("Cannot close", path_);
    }
    committed_ = true;
    return message_;
  }

  arrow::Status Abort() override {
    // Abort discards the rows that are still buffered.
    Discard();
    if (::unlink(path_.c_str()) != 0 && errno != ENOENT) {
      return ErrnoError("Cannot remove", path_);
    }
    return arrow::Status::OK();
  }

 private:
  static constexpr size_t kBufferSize = 64 * 1024;

  arrow::Status Flush() {
    ARROW_RETURN_NOT_OK(WriteAll(descriptor_, buffer_, path_));
    buffer_.clear();
    return arrow::Status::OK();
  }

  void Discard() {
    buffer_.clear();
    if (descriptor_ >= 0) {
      ::close(std::exchange(descriptor_, -1));
    }
  }

  spark::native::TaskContext task_;
  fs::path path_;
  CommitMessage message_;
  int descriptor_;
  std::string buffer_;
  bool committed_ = false;
};

}  // namespace

std::shared_ptr<arrow::Schema> SinkSchema() {
  return arrow::schema({arrow::field("id", arrow::int64()), arrow::field("label", arrow::utf8())});
}

arrow::Result<Manifest> ReadManifest(const fs::path& root) {
  std::ifstream file(root / "manifest.json");
  if (!file) {
    std::error_code error;
    if (!fs::exists(root / "manifest.json", error) && !error) {
      return Manifest();
    }
    return arrow::Status::IOError("Cannot read the sink manifest in ", root.string());
  }
  std::stringstream text;
  text << file.rdbuf();
  nlohmann::json json = nlohmann::json::parse(text.str(), nullptr, /*allow_exceptions=*/false);
  if (json.is_discarded() || !json.is_object()) {
    return arrow::Status::Invalid("The sink manifest is not a JSON object");
  }
  Manifest manifest;
  try {
    json.at("version").get_to(manifest.version);
    json.at("files").get_to(manifest.files);
    if (json.contains("epochs")) {
      json.at("epochs").get_to(manifest.epochs);
    }
  } catch (const std::exception& error) {
    return arrow::Status::Invalid("Invalid sink manifest: ", error.what());
  }
  if (manifest.version != 1) {
    return arrow::Status::Invalid("Unsupported sink manifest version");
  }
  for (const DataFile& data_file : manifest.files) {
    fs::path path(data_file.path);
    std::vector<fs::path> parts(path.begin(), path.end());
    bool valid = parts.size() == 3 && parts[0] == "objects";
    for (const fs::path& part : parts) {
      valid = valid && part != "." && part != ".." && !part.empty() && part != "/";
    }
    if (!valid) {
      return arrow::Status::Invalid("Invalid sink manifest data path");
    }
  }
  return manifest;
}

// WriteLock

arrow::Result<WriteLock> WriteLock::Acquire(const fs::path& root) {
  fs::path path = root / ".write-lock";
  std::error_code error;
  if (fs::create_directory(path, error)) {
    return WriteLock(std::move(path));
  }
  if (!error) {
    return arrow::Status::IOError(
        "Another write owns .write-lock; concurrent writes are unsupported");
  }
  return Error("Cannot create", path, error);
}

WriteLock::~WriteLock() {
  if (!path_.empty()) {
    std::error_code ignored;
    fs::remove(path_, ignored);
  }
}

// Operation

Operation::~Operation() {
  if (!committed) {
    (void)Cleanup();
  }
}

arrow::Status Operation::Publish(const std::vector<CommitMessage>& messages,
                                 std::optional<std::pair<std::string, int64_t>> epoch) {
  std::set<std::string> filenames;
  for (size_t index = 0; index < messages.size(); ++index) {
    const CommitMessage& message = messages[index];
    if (message.operation != id || !IsFilename(message.filename) ||
        !filenames.insert(message.filename).second) {
      return arrow::Status::Invalid("Invalid or duplicate task commit message");
    }
    // Spark passes the message of partition i at position i.
    if (message.partition < 0 || static_cast<size_t>(message.partition) != index) {
      return arrow::Status::Invalid("A commit message does not match its partition");
    }
    std::error_code error;
    if (!fs::is_regular_file(staging / message.filename, error)) {
      return arrow::Status::Invalid("A winning task's staged file is missing");
    }
  }

  // Seal the entire operation directory. New attempts can no longer open a staged file, and
  // existing losing files remain invisible to readers.
  std::error_code error;
  fs::rename(staging, objects, error);
  if (error) {
    return Error("Cannot seal", staging, error);
  }
  sealed = true;
  for (const auto& entry : fs::directory_iterator(objects, error)) {
    if (filenames.count(entry.path().filename().string()) == 0) {
      fs::remove(entry.path(), error);
      if (error) {
        return Error("Cannot remove", entry.path(), error);
      }
    }
  }
  if (error) {
    return Error("Cannot list", objects, error);
  }

  Manifest manifest;
  if (!truncate) {
    manifest.files = previous.files;
  }
  for (const CommitMessage& message : messages) {
    manifest.files.push_back(DataFile{"objects/" + id + "/" + message.filename, message.rows});
  }
  manifest.epochs = previous.epochs;
  if (epoch) {
    manifest.epochs[epoch->first] = epoch->second;
  }
  ARROW_RETURN_NOT_OK(WriteNewFile(temporary_manifest, ManifestJson(manifest).dump() + "\n"));

  // This single rename is the commit point. No existing data is deleted, including during
  // truncate. No fallible work follows publication.
  fs::rename(temporary_manifest, root / "manifest.json", error);
  if (error) {
    return Error("Cannot publish", temporary_manifest, error);
  }
  committed = true;
  lock.reset();
  return arrow::Status::OK();
}

arrow::Status Operation::Skip() {
  arrow::Status status = Cleanup();
  // Nothing is left for the destructor to clean.
  committed = true;
  lock.reset();
  return status;
}

arrow::Status Operation::Cleanup() const {
  // Do not rely on commit messages: failed tasks and losing attempts may have staged files without
  // returning a message to the driver.
  arrow::Status status;
  std::error_code error;
  fs::remove_all(staging, error);
  if (error && status.ok()) {
    status = Error("Cannot remove", staging, error);
  }
  if (sealed) {
    fs::remove_all(objects, error);
    if (error && status.ok()) {
      status = Error("Cannot remove", objects, error);
    }
  }
  fs::remove(temporary_manifest, error);
  if (error && status.ok()) {
    status = Error("Cannot remove", temporary_manifest, error);
  }
  return status;
}

// JsonWrite

arrow::Result<JsonWrite> JsonWrite::Begin(const fs::path& root, WriteMode mode) {
  auto operation = std::make_unique<Operation>();
  switch (mode) {
    case WriteMode::kAppend:
      operation->truncate = false;
      break;
    case WriteMode::kTruncate:
      operation->truncate = true;
      break;
    default:
      return arrow::Status::Invalid("Unsupported write mode");
  }
  std::error_code error;
  fs::create_directories(root, error);
  if (error) {
    return Error("Cannot create", root, error);
  }
  ARROW_ASSIGN_OR_RAISE(WriteLock lock, WriteLock::Acquire(root));
  ARROW_ASSIGN_OR_RAISE(operation->previous, ReadManifest(root));
  for (const char* directory : {".staging", "objects"}) {
    fs::create_directories(root / directory, error);
    if (error) {
      return Error("Cannot create", root / directory, error);
    }
  }
  ARROW_ASSIGN_OR_RAISE(operation->staging, UniqueDirectory(root / ".staging"));
  operation->root = root;
  operation->id = operation->staging.filename().string();
  operation->objects = root / "objects" / operation->id;
  operation->temporary_manifest = root / (".manifest-" + operation->id + ".json");
  operation->lock.emplace(std::move(lock));
  return JsonWrite(std::move(operation));
}

arrow::Result<JsonWriterFactory> JsonWrite::CreateWriterFactory() const {
  JsonWriterFactory factory;
  factory.staging = operation_->staging.string();
  factory.operation = operation_->id;
  return factory;
}

arrow::Status JsonWrite::Commit(std::vector<CommitMessage> messages) {
  return operation_->Publish(messages, std::nullopt);
}

arrow::Status JsonWrite::Abort(std::vector<std::optional<CommitMessage>> messages) {
  (void)messages;
  arrow::Status status = operation_->Cleanup();
  operation_->lock.reset();
  return status;
}

// JsonStreamingWrite

arrow::Result<JsonWriterFactory> JsonStreamingWrite::CreateWriterFactory() const {
  return write_.CreateWriterFactory();
}

arrow::Status JsonStreamingWrite::Commit(int64_t epoch_id, std::vector<CommitMessage> messages) {
  Operation& operation = write_.operation();
  // The write holds the lock, so the manifest that it read is still the latest one.
  auto committed = operation.previous.epochs.find(query_id_);
  if (committed != operation.previous.epochs.end() && committed->second >= epoch_id) {
    return operation.Skip();
  }
  return operation.Publish(messages, std::make_pair(query_id_, epoch_id));
}

arrow::Status JsonStreamingWrite::Abort(int64_t epoch_id,
                                        std::vector<std::optional<CommitMessage>> messages) {
  (void)epoch_id;
  return write_.Abort(std::move(messages));
}

// JsonWriterFactory

arrow::Result<std::unique_ptr<spark::native::DataWriter<CommitMessage>>> JsonWriterFactory::Open(
    const spark::native::WriterContext& context) const {
  // The SDK checks that each batch has this schema, so the writer checks it once.
  ARROW_RETURN_NOT_OK(ValidateSchema(*context.schema));
  spark::native::TaskContext task = context.task;
  if (task.IsInterrupted()) {
    return arrow::Status::Cancelled("Task cancelled before opening the sink");
  }
  // Do not recreate staging: its absence means the driver sealed or aborted this operation.
  // O_EXCL also separates speculative tasks.
  for (int attempt = 0; attempt < 16; ++attempt) {
    std::string filename = "part-" + std::to_string(task.partition_id()) + "-attempt-" +
                           std::to_string(task.task_attempt_id()) + "-" + UniqueId() + ".jsonl";
    fs::path path = fs::path(staging) / filename;
    int descriptor = ::open(path.c_str(), O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC, 0644);
    if (descriptor >= 0) {
      CommitMessage message{operation, filename, task.partition_id(), 0};
      return std::make_unique<JsonTaskWriter>(task, std::move(path), std::move(message),
                                              descriptor);
    }
    if (errno != EEXIST) {
      return ErrnoError("Cannot create the task file", path);
    }
  }
  return arrow::Status::IOError("Could not create a unique task attempt file");
}

// JsonSink

arrow::Result<JsonSink> JsonSink::Make(const CreateInfo& info) {
  spark::native::Options options = info.options;
  auto path = options.find("path");
  if (path == options.end()) {
    return arrow::Status::Invalid("The path option is required");
  }
  fs::path root(path->second);
  options.erase(path);
  if (!root.is_absolute()) {
    return arrow::Status::Invalid(
        "The path must be absolute and shared by the driver and executors");
  }
  bool readonly = false;
  if (auto option = options.find("readonly"); option != options.end()) {
    if (option->second != "true" && option->second != "false") {
      return arrow::Status::Invalid("The readonly option must be true or false");
    }
    readonly = option->second == "true";
    options.erase(option);
  }
  // Spark passes the checkpoint of a streaming query among the options of its sink.
  options.erase("checkpointlocation");
  if (!options.empty()) {
    std::string keys;
    for (const auto& [key, value] : options) {
      keys += (keys.empty() ? "" : ", ") + key;
    }
    return arrow::Status::Invalid("Unknown options: [", keys, "]");
  }
  return JsonSink(std::move(root), readonly);
}

arrow::Result<std::shared_ptr<arrow::Schema>> JsonSink::InferSchema() const { return SinkSchema(); }

Capabilities JsonSink::capabilities() const {
  if (readonly_) {
    // Also removes kTruncate, which requires a write.
    return Capabilities::All().Without(Capabilities::kBatchWrite | Capabilities::kStreamingWrite);
  }
  return Capabilities::All();
}

arrow::Result<JsonWrite> JsonSink::NewWrite(const spark::native::WriteInfo& info) const {
  if (readonly_) {
    return arrow::Status::Invalid("This sink is readonly");
  }
  ARROW_RETURN_NOT_OK(ValidateSchema(*info.schema));
  return JsonWrite::Begin(path_, info.mode);
}

arrow::Result<JsonStreamingWrite> JsonSink::NewStreamingWrite(
    const spark::native::StreamingWriteInfo& info) const {
  if (readonly_) {
    return arrow::Status::Invalid("This sink is readonly");
  }
  ARROW_RETURN_NOT_OK(ValidateSchema(*info.schema));
  ARROW_ASSIGN_OR_RAISE(JsonWrite write, JsonWrite::Begin(path_, info.mode));
  return JsonStreamingWrite(std::move(write), info.query_id);
}

}  // namespace cpp_sdk_sink

SPARK_EXPORT_DATA_SOURCE(cpp_sdk_sink::JsonSink);
