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

#include <unistd.h>

#include <filesystem>
#include <fstream>
#include <sstream>

#include <arrow/api.h>

#include "json_sink.h"
#include "test_util.h"

namespace fs = std::filesystem;
namespace sn = spark::native;
using cpp_sdk_sink::CommitMessage;
using cpp_sdk_sink::JsonSink;
using cpp_sdk_sink::JsonStreamingWrite;
using cpp_sdk_sink::JsonWrite;
using cpp_sdk_sink::JsonWriterFactory;
using cpp_sdk_sink::ReadManifest;
using cpp_sdk_sink::SinkSchema;
using spark::native::Capabilities;
using spark::native::WriteMode;

namespace {

using Options = std::vector<std::pair<std::string, std::string>>;

arrow::Result<JsonSink> MakeSink(const Options& options) {
  return JsonSink::Make(
      sn::CreateInfo("cpp_sdk_sink", sn::Options(options.begin(), options.end())));
}

class TempDirectory {
 public:
  TempDirectory() {
    static int counter = 0;
    path_ = fs::temp_directory_path() /
            ("cpp-sdk-sink-test-" + std::to_string(::getpid()) + "-" + std::to_string(counter++));
    fs::remove_all(path_);
    fs::create_directories(path_);
  }
  TempDirectory(const TempDirectory&) = delete;
  TempDirectory& operator=(const TempDirectory&) = delete;
  ~TempDirectory() {
    std::error_code ignored;
    fs::remove_all(path_, ignored);
  }

  const fs::path& path() const { return path_; }

  JsonSink Source() const {
    ASSIGN_OR_FAIL(JsonSink sink, MakeSink({{"path", path_.string()}}));
    return sink;
  }

 private:
  fs::path path_;
};

template <typename T>
T RoundTrip(const T& value) {
  return nlohmann::json::parse(nlohmann::json(value).dump()).get<T>();
}

std::pair<JsonWrite, JsonWriterFactory> NewWrite(const JsonSink& sink, WriteMode mode) {
  ASSIGN_OR_FAIL(JsonWrite write, sink.NewWrite(sn::WriteInfo(SinkSchema(), mode)));
  ASSIGN_OR_FAIL(JsonWriterFactory factory, write.CreateWriterFactory());
  return {std::move(write), factory};
}

std::pair<JsonStreamingWrite, JsonWriterFactory> NewStreamingWrite(const JsonSink& sink,
                                                                   const std::string& query_id,
                                                                   WriteMode mode) {
  ASSIGN_OR_FAIL(JsonStreamingWrite write,
                 sink.NewStreamingWrite(sn::StreamingWriteInfo(SinkSchema(), mode, query_id)));
  ASSIGN_OR_FAIL(JsonWriterFactory factory, write.CreateWriterFactory());
  return {std::move(write), factory};
}

arrow::Result<std::unique_ptr<sn::DataWriter<CommitMessage>>> TryOpen(
    const JsonWriterFactory& factory, int32_t partition) {
  sn::TaskContext task = sn::TaskContext::ForTesting().WithPartitionId(partition);
  return factory.Open(sn::WriterContext(SinkSchema(), task));
}

std::unique_ptr<sn::DataWriter<CommitMessage>> Open(const JsonWriterFactory& factory,
                                                    int32_t partition) {
  ASSIGN_OR_FAIL(auto writer, TryOpen(factory, partition));
  return writer;
}

std::shared_ptr<arrow::RecordBatch> Batch(const std::vector<std::optional<int64_t>>& ids) {
  arrow::Int64Builder id_builder;
  arrow::StringBuilder label_builder;
  for (const auto& id : ids) {
    if (id) {
      CHECK_OK(id_builder.Append(*id));
      CHECK_OK(label_builder.Append("row-" + std::to_string(*id)));
    } else {
      CHECK_OK(id_builder.AppendNull());
      CHECK_OK(label_builder.AppendNull());
    }
  }
  return arrow::RecordBatch::Make(
      SinkSchema(), static_cast<int64_t>(ids.size()),
      {id_builder.Finish().ValueOrDie(), label_builder.Finish().ValueOrDie()});
}

CommitMessage Stage(const JsonWriterFactory& factory, int32_t partition,
                    const std::vector<std::optional<int64_t>>& ids) {
  // Executors restore the factory without the source or the driver write.
  std::unique_ptr<sn::DataWriter<CommitMessage>> writer = Open(RoundTrip(factory), partition);
  CHECK_OK(writer->Write(Batch(ids)));
  ASSIGN_OR_FAIL(CommitMessage message, writer->Commit());
  return RoundTrip(message);
}

std::vector<nlohmann::json> CommittedRows(const fs::path& root) {
  ASSIGN_OR_FAIL(cpp_sdk_sink::Manifest manifest, ReadManifest(root));
  std::vector<nlohmann::json> rows;
  for (const auto& file : manifest.files) {
    std::ifstream input(root / file.path);
    std::string line;
    uint64_t count = 0;
    while (std::getline(input, line)) {
      rows.push_back(nlohmann::json::parse(line));
      ++count;
    }
    CHECK_EQ(count, file.rows);
  }
  return rows;
}

std::vector<nlohmann::json> Rows(const std::vector<int64_t>& ids) {
  std::vector<nlohmann::json> rows;
  for (int64_t id : ids) {
    rows.push_back({{"id", id}, {"label", "row-" + std::to_string(id)}});
  }
  return rows;
}

std::string Contents(const fs::path& path) {
  std::ifstream input(path);
  std::stringstream text;
  text << input.rdbuf();
  return text.str();
}

size_t Entries(const fs::path& directory) {
  return static_cast<size_t>(
      std::distance(fs::directory_iterator(directory), fs::directory_iterator()));
}

void CommitIds(const JsonSink& sink, WriteMode mode,
               const std::vector<std::optional<int64_t>>& ids) {
  auto [write, factory] = NewWrite(sink, mode);
  CHECK_OK(write.Commit({Stage(factory, 0, ids)}));
}

void CommitEpoch(const JsonSink& sink, const std::string& query_id, WriteMode mode,
                 int64_t epoch_id, const std::vector<std::optional<int64_t>>& ids) {
  auto [write, factory] = NewStreamingWrite(sink, query_id, mode);
  CHECK_OK(write.Commit(epoch_id, {Stage(factory, 0, ids)}));
}

}  // namespace

TEST(AppendPublishesAllCommittedRowsAndPreservesNulls) {
  TempDirectory directory;
  JsonSink sink = directory.Source();
  CommitIds(sink, WriteMode::kAppend, {1, std::nullopt});
  CommitIds(sink, WriteMode::kAppend, {2});
  CHECK(CommittedRows(directory.path()) ==
        std::vector<nlohmann::json>({{{"id", 1}, {"label", "row-1"}},
                                     {{"id", nullptr}, {"label", nullptr}},
                                     {{"id", 2}, {"label", "row-2"}}}));
  ASSIGN_OR_FAIL(auto manifest, ReadManifest(directory.path()));
  CHECK_EQ(manifest.files.size(), size_t{2});
  CHECK(!fs::exists(directory.path() / ".write-lock"));
}

TEST(TruncateReplacesTheManifestButRetainsPreviouslyPublishedFiles) {
  TempDirectory directory;
  JsonSink sink = directory.Source();
  CommitIds(sink, WriteMode::kAppend, {1});
  ASSIGN_OR_FAIL(auto previous, ReadManifest(directory.path()));
  CommitIds(sink, WriteMode::kTruncate, {2});
  CHECK(CommittedRows(directory.path()) == Rows({2}));
  CHECK(fs::is_regular_file(directory.path() / previous.files[0].path));
  CommitIds(sink, WriteMode::kTruncate, {});
  CHECK(CommittedRows(directory.path()).empty());
}

TEST(TheDriverPublishesOnlyWinningAttemptsAndRemovesUnreportedFiles) {
  TempDirectory directory;
  auto [write, factory] = NewWrite(directory.Source(), WriteMode::kAppend);
  CommitMessage winning = Stage(factory, 0, {1});
  Stage(factory, 0, {2});  // A losing attempt.
  Stage(factory, 1, {3});  // An attempt whose message never reached the driver.
  const cpp_sdk_sink::Operation& operation = write.operation();
  CHECK_EQ(Entries(operation.staging), size_t{3});
  CHECK_OK(write.Commit({winning}));
  CHECK(!fs::exists(operation.staging));
  CHECK_EQ(Entries(operation.objects), size_t{1});
  CHECK(CommittedRows(directory.path()) == Rows({1}));
  CHECK(!TryOpen(factory, 0).ok());
}

TEST(CommitRejectsAMessageAtThePositionOfAnotherPartition) {
  TempDirectory directory;
  auto [write, factory] = NewWrite(directory.Source(), WriteMode::kAppend);
  CommitMessage message = Stage(factory, 1, {1});
  CHECK(!write.Commit({message}).ok());
  CHECK(!fs::exists(directory.path() / "manifest.json"));
}

TEST(AbortCleansTheOperationEvenWithMissingMessages) {
  TempDirectory directory;
  JsonSink sink = directory.Source();
  CommitIds(sink, WriteMode::kAppend, {1});
  std::string before = Contents(directory.path() / "manifest.json");
  auto [write, factory] = NewWrite(sink, WriteMode::kTruncate);
  CommitMessage message = Stage(factory, 0, {2});
  Stage(factory, 1, {3});
  std::unique_ptr<sn::DataWriter<CommitMessage>> incomplete = Open(factory, 2);
  CHECK_OK(incomplete->Write(Batch({4})));
  CHECK_OK(write.Abort({message, std::nullopt}));
  const cpp_sdk_sink::Operation& operation = write.operation();
  CHECK(!fs::exists(operation.staging));
  CHECK(!fs::exists(operation.objects));
  CHECK(!fs::exists(directory.path() / ".write-lock"));
  CHECK_EQ(Contents(directory.path() / "manifest.json"), before);
  CHECK_EQ(CommittedRows(directory.path()).size(), size_t{1});
  CHECK(!TryOpen(factory, 3).ok());
}

TEST(FailedPublicationKeepsThePreviousManifestAndAbortRemovesSealedFiles) {
  TempDirectory directory;
  JsonSink sink = directory.Source();
  CommitIds(sink, WriteMode::kAppend, {1});
  std::string before = Contents(directory.path() / "manifest.json");
  auto [write, factory] = NewWrite(sink, WriteMode::kTruncate);
  CommitMessage message = Stage(factory, 0, {2});
  const cpp_sdk_sink::Operation& operation = write.operation();
  // Make the exclusive creation of the manifest fail after the operation directory is sealed.
  std::ofstream(operation.temporary_manifest) << "publication failure";
  CHECK(!write.Commit({message}).ok());
  CHECK(fs::exists(operation.objects));
  CHECK_EQ(Contents(directory.path() / "manifest.json"), before);
  CHECK_OK(write.Abort({std::nullopt}));
  CHECK(!fs::exists(operation.objects));
  CHECK(!fs::exists(operation.temporary_manifest));
  CHECK_EQ(CommittedRows(directory.path()).size(), size_t{1});
}

TEST(AConcurrentWriteIsRejectedAndDestructionReleasesStagingAndTheLock) {
  TempDirectory directory;
  JsonSink sink = directory.Source();
  fs::path staging;
  {
    auto [write, factory] = NewWrite(sink, WriteMode::kAppend);
    Stage(factory, 0, {1});
    staging = write.operation().staging;
    CHECK(!sink.NewWrite(sn::WriteInfo(SinkSchema(), WriteMode::kAppend)).ok());
  }
  CHECK(!fs::exists(staging));
  CHECK(!fs::exists(directory.path() / ".write-lock"));
  CHECK(sink.NewWrite(sn::WriteInfo(SinkSchema(), WriteMode::kAppend)).ok());
}

TEST(StreamingWritesPublishEachEpochOfAQueryOnce) {
  TempDirectory directory;
  JsonSink sink = directory.Source();
  CommitEpoch(sink, "query", WriteMode::kAppend, 0, {1});
  CommitEpoch(sink, "query", WriteMode::kAppend, 1, {2});
  {
    // A restarted query commits its last epoch again, which publishes nothing and removes what
    // the new attempt staged.
    auto [write, factory] = NewStreamingWrite(sink, "query", WriteMode::kAppend);
    CommitMessage message = Stage(factory, 0, {3});
    fs::path staging = write.operation().staging;
    CHECK_OK(write.Commit(1, {message}));
    CHECK(!fs::exists(staging));
    CHECK(!fs::exists(directory.path() / ".write-lock"));
  }
  CHECK(CommittedRows(directory.path()) == Rows({1, 2}));

  // Another query has epochs of its own.
  CommitEpoch(sink, "other", WriteMode::kAppend, 0, {4});
  CHECK(CommittedRows(directory.path()) == Rows({1, 2, 4}));
  ASSIGN_OR_FAIL(auto manifest, ReadManifest(directory.path()));
  using Epochs = std::map<std::string, int64_t>;
  CHECK(manifest.epochs == Epochs({{"other", 0}, {"query", 1}}));

  {
    // An aborted micro-batch leaves the output unchanged.
    auto [write, factory] = NewStreamingWrite(sink, "query", WriteMode::kAppend);
    CommitMessage message = Stage(factory, 0, {5});
    CHECK_OK(write.Abort(2, {message}));
  }
  CHECK(CommittedRows(directory.path()) == Rows({1, 2, 4}));

  // The output mode complete replaces the output of the previous micro-batches.
  CommitEpoch(sink, "query", WriteMode::kTruncate, 2, {6});
  CHECK(CommittedRows(directory.path()) == Rows({6}));
  ASSIGN_OR_FAIL(auto truncated, ReadManifest(directory.path()));
  CHECK_EQ(truncated.epochs.at("query"), int64_t{2});
}

TEST(StreamingQueriesPassTheirCheckpointAmongTheOptions) {
  TempDirectory directory;
  std::string path = directory.path().string();
  CHECK(MakeSink({{"path", path}, {"checkpointlocation", "/tmp/c"}}).ok());
  CHECK(!MakeSink({{"path", path}, {"unknown", "1"}}).ok());
  CHECK(!MakeSink({{"path", "relative/path"}}).ok());
  CHECK(!MakeSink({}).ok());
}

TEST(ReadonlyRemovesAllWriteCapabilitiesWithoutCreatingFiles) {
  TempDirectory directory;
  fs::path path = directory.path() / "not-created";
  ASSIGN_OR_FAIL(JsonSink sink, MakeSink({{"path", path.string()}, {"readonly", "true"}}));
  Capabilities capabilities = sink.capabilities();
  CHECK(!capabilities.Contains(Capabilities::kBatchWrite));
  CHECK(!capabilities.Contains(Capabilities::kStreamingWrite));
  CHECK(!capabilities.Contains(Capabilities::kTruncate));
  CHECK(!sink.NewWrite(sn::WriteInfo(SinkSchema(), WriteMode::kAppend)).ok());
  CHECK(!sink.NewStreamingWrite(sn::StreamingWriteInfo(SinkSchema(), WriteMode::kAppend, "query"))
             .ok());
  CHECK(!fs::exists(path));
  CHECK(!MakeSink({{"path", path.string()}, {"readonly", "yes"}}).ok());
}

TEST(TheSinkDeclaresOnlyWrites) {
  using Traits = sn::internal::SourceTraits<JsonSink>;
  CHECK(Traits::kCapabilities ==
        (Capabilities::kBatchWrite | Capabilities::kStreamingWrite | Capabilities::kTruncate));
  const SparkDsLibrary* library = sn::internal::Exports<JsonSink>::Library();
  CHECK(library->create_reader == nullptr && library->read == nullptr);
  CHECK(library->create_writer != nullptr && library->schema != nullptr);
}
