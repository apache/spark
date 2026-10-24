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

// Micro-batch streaming reads and streaming writes through the function table that the library
// exports.

#include <atomic>
#include <mutex>
#include <stdexcept>

#include <arrow/api.h>
#include <arrow/c/bridge.h>

#include "spark/native/data_source.h"
#include "test_util.h"

namespace sn = spark::native;
using spark::native::Capabilities;
using spark::native::testing::Collect;
using spark::native::testing::CreateArguments;
using spark::native::testing::LastError;

namespace {

std::mutex mutex;
std::vector<std::string> events;
// The position of the latest data of the streams.
std::atomic<uint64_t> latest{0};

void Record(std::string event) {
  std::lock_guard<std::mutex> lock(mutex);
  events.push_back(std::move(event));
}

std::vector<std::string> Events() {
  std::lock_guard<std::mutex> lock(mutex);
  return events;
}

void ClearEvents() {
  std::lock_guard<std::mutex> lock(mutex);
  events.clear();
}

std::shared_ptr<arrow::Schema> Schema() {
  return arrow::schema({arrow::field("value", arrow::int64(), false)});
}

arrow::Result<std::shared_ptr<arrow::RecordBatchReader>> ReaderOf(int64_t value) {
  arrow::Int64Builder builder;
  ARROW_RETURN_NOT_OK(builder.Append(value));
  ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Array> values, builder.Finish());
  return arrow::RecordBatchReader::Make({arrow::RecordBatch::Make(Schema(), 1, {values})},
                                        Schema());
}

/// Reads `base + partition` from each partition of a batch scan.
struct BatchFactory : sn::PartitionReaderFactory<int64_t> {
  int64_t base = 0;

  arrow::Result<std::shared_ptr<arrow::RecordBatchReader>> Open(
      const int64_t& partition, const sn::ReaderContext&) const override {
    return ReaderOf(base + partition);
  }
};
NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE(BatchFactory, base)

struct Scan : sn::ScanBuilder<BatchFactory> {
  arrow::Result<sn::ScanPlan<BatchFactory>> Plan() override {
    BatchFactory factory;
    factory.base = 100;
    return sn::ScanPlan<BatchFactory>(factory, {5});
  }
};

/// Reads `scale * position` from the partition of each position of a micro-batch.
struct StreamFactory : sn::PartitionReaderFactory<uint64_t> {
  int64_t scale = 0;

  arrow::Result<std::shared_ptr<arrow::RecordBatchReader>> Open(
      const uint64_t& position, const sn::ReaderContext&) const override {
    return ReaderOf(scale * static_cast<int64_t>(position));
  }
};
NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE(StreamFactory, scale)

/// The note checks that offsets stay on one line, as Spark requires.
struct Offset {
  uint64_t position = 0;
  std::string note = "two\nlines";
};
NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE(Offset, position, note)

class Stream : public sn::MicroBatchStream<Offset, StreamFactory> {
 public:
  explicit Stream(std::string failure) : failure_(std::move(failure)) {}

  arrow::Result<Offset> InitialOffset() override { return Offset(); }

  arrow::Result<Offset> LatestOffset() override {
    if (failure_ == "latest_throw") {
      throw std::runtime_error("finding the latest offset threw");
    }
    Offset offset;
    offset.position = latest.load();
    return offset;
  }

  arrow::Result<sn::ScanPlan<StreamFactory>> Plan(const Offset& start, const Offset& end) override {
    std::vector<uint64_t> positions;
    for (uint64_t position = start.position; position < end.position; ++position) {
      positions.push_back(position);
    }
    StreamFactory factory;
    factory.scale = 10;
    return sn::ScanPlan<StreamFactory>(factory, std::move(positions));
  }

  arrow::Status Commit(const Offset& end) override {
    Record("OffsetCommitted(" + std::to_string(end.position) + ")");
    return arrow::Status::OK();
  }

 private:
  std::string failure_;
};

struct Message {
  std::optional<int64_t> epoch_id;
  std::vector<int64_t> values;
};

void to_json(nlohmann::json& json, const Message& message) {
  json = {{"epoch_id", message.epoch_id ? nlohmann::json(*message.epoch_id) : nlohmann::json()},
          {"values", message.values}};
}

void from_json(const nlohmann::json& json, Message& message) {
  const nlohmann::json& epoch_id = json.at("epoch_id");
  message.epoch_id = epoch_id.is_null() ? std::nullopt : std::optional(epoch_id.get<int64_t>());
  json.at("values").get_to(message.values);
}

std::string Describe(const std::vector<std::optional<Message>>& messages) {
  nlohmann::json json = nlohmann::json::array();
  for (const auto& message : messages) {
    json.push_back(message ? nlohmann::json(*message) : nlohmann::json());
  }
  return json.dump();
}

class Writer : public sn::DataWriter<Message> {
 public:
  explicit Writer(std::optional<int64_t> epoch_id) { message_.epoch_id = epoch_id; }

  arrow::Status Write(std::shared_ptr<arrow::RecordBatch> batch) override {
    auto values = std::static_pointer_cast<arrow::Int64Array>(batch->column(0));
    message_.values.insert(message_.values.end(), values->raw_values(),
                           values->raw_values() + values->length());
    return arrow::Status::OK();
  }

  arrow::Result<Message> Commit() override { return message_; }

  arrow::Status Abort() override { return arrow::Status::OK(); }

 private:
  Message message_;
};

struct WriteFactory : sn::DataWriterFactory<Message> {
  int64_t unused = 0;

  arrow::Result<std::unique_ptr<sn::DataWriter<Message>>> Open(
      const sn::WriterContext& context) const override {
    return std::make_unique<Writer>(context.epoch_id);
  }
};
NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE(WriteFactory, unused)

struct Write : sn::StreamingWrite<WriteFactory> {
  static constexpr bool kSupportsTruncate = true;

  arrow::Result<WriteFactory> CreateWriterFactory() const override { return WriteFactory(); }

  arrow::Status Commit(int64_t epoch_id, std::vector<Message> messages) override {
    Record("EpochCommitted(" + std::to_string(epoch_id) + ", " +
           Describe(std::vector<std::optional<Message>>(messages.begin(), messages.end())) + ")");
    return arrow::Status::OK();
  }

  arrow::Status Abort(int64_t epoch_id, std::vector<std::optional<Message>> messages) override {
    Record("EpochAborted(" + std::to_string(epoch_id) + ", " + Describe(messages) + ")");
    return arrow::Status::OK();
  }
};

/// Batch and micro-batch reads, whose reader factories have different types, and streaming writes
/// without batch writes.
class Source : public sn::DataSource {
 public:
  using ScanBuilder = Scan;
  using MicroBatchStream = Stream;
  using StreamingWrite = Write;

  static arrow::Result<Source> Make(const sn::CreateInfo& info) {
    Source source;
    auto option = [&](const std::string& key) {
      auto value = info.options.find(key);
      return value == info.options.end() ? std::string() : value->second;
    };
    source.failure_ = option("failure");
    source.capabilities_ = option("capabilities");
    return source;
  }

  arrow::Result<std::shared_ptr<arrow::Schema>> InferSchema() const override { return Schema(); }

  Capabilities capabilities() const override {
    if (capabilities_ == "no_streaming_write") {
      return Capabilities::All().Without(Capabilities::kStreamingWrite);
    }
    return Capabilities::All();
  }

  arrow::Result<Scan> NewScan(const sn::ScanInfo&) const { return Scan(); }

  arrow::Result<Stream> NewMicroBatchStream(const sn::MicroBatchStreamInfo& info) const {
    if (!info.schema->Equals(*Schema())) {
      return arrow::Status::Invalid("unsupported stream schema");
    }
    Record("StreamCreated");
    return Stream(failure_);
  }

  arrow::Result<Write> NewStreamingWrite(const sn::StreamingWriteInfo& info) const {
    Record("WriteCreated(" + info.query_id + ", " +
           (info.mode == sn::WriteMode::kTruncate ? "truncate" : "append") + ")");
    return Write();
  }

 private:
  std::string failure_;
  std::string capabilities_;
};

const SparkDsLibrary& Library() {
  static const SparkDsLibrary& library = sn::testing::InitializeLibrary();
  return library;
}

void* CreateSource(const std::string& failure, const std::string& capabilities,
                   uint64_t* mask = nullptr) {
  CreateArguments arguments("streaming_test",
                            {{"failure", failure}, {"capabilities", capabilities}});
  uint64_t declared = 0;
  void* source = nullptr;
  CHECK_SUCCESS(Library(), Library().create(arguments.info(), &declared, &source));
  if (mask != nullptr) {
    *mask = declared;
  }
  return source;
}

void* StreamReader(const std::string& failure) {
  void* source = CreateSource(failure, "");
  struct ArrowSchema schema{};
  CHECK_OK(arrow::ExportSchema(*Schema(), &schema));
  void* reader = nullptr;
  CHECK_SUCCESS(Library(), Library().create_stream_reader(source, &schema, &reader));
  // A stream reader outlives its source, which Spark closes right after creating it.
  Library().close(source);
  return reader;
}

/// Calls a function of the table that passes its results to a sink.
template <typename F>
std::pair<int, std::vector<std::string>> Emitted(F&& call) {
  std::vector<std::string> output;
  int code = call(static_cast<void*>(&output));
  if (code != 0) {
    CHECK(output.empty());
  }
  return {code, output};
}

template <typename F>
std::string Single(F&& call) {
  auto [code, output] = Emitted(std::forward<F>(call));
  CHECK_SUCCESS(Library(), code);
  CHECK(output.size() == 1);
  return output[0];
}

std::pair<int, std::vector<std::string>> Latest(void* reader) {
  return Emitted([&](void* sink) { return Library().latest_offset(reader, &Collect, sink); });
}

/// Reads a partition with the read function that batch scans and micro-batches share.
std::vector<int64_t> Read(const std::string& state, const std::string& partition) {
  sn::testing::HostTask task(0, 10);
  struct ArrowArrayStream stream{};
  CHECK_SUCCESS(Library(),
                Library().read(&task.context, reinterpret_cast<const uint8_t*>(state.data()),
                               static_cast<int64_t>(state.size()),
                               reinterpret_cast<const uint8_t*>(partition.data()),
                               static_cast<int64_t>(partition.size()), &stream));
  ASSIGN_OR_FAIL(std::shared_ptr<arrow::RecordBatchReader> reader,
                 arrow::ImportRecordBatchReader(&stream));
  std::vector<int64_t> values;
  while (true) {
    ASSIGN_OR_FAIL(std::shared_ptr<arrow::RecordBatch> batch, reader->Next());
    if (batch == nullptr) {
      break;
    }
    auto column = std::static_pointer_cast<arrow::Int64Array>(batch->column(0));
    values.insert(values.end(), column->raw_values(), column->raw_values() + column->length());
  }
  return values;
}

/// Creates a writer, or returns the error of the library.
arrow::Result<void*> CreateWriter(int32_t mode, const char* query_id) {
  void* source = CreateSource("", "");
  struct ArrowSchema input{};
  CHECK_OK(arrow::ExportSchema(*Schema(), &input));
  SparkDsWriteInfo info{sizeof(SparkDsWriteInfo), mode, query_id};
  void* writer = nullptr;
  int code = Library().create_writer(source, &input, &info, &writer);
  if (input.release != nullptr) {
    input.release(&input);
  }
  arrow::Result<void*> result = writer;
  if (code != 0) {
    CHECK(writer == nullptr);
    result = arrow::Status::Invalid(LastError(Library()));
  }
  Library().close(source);
  return result;
}

int Finish(void* writer, bool abort, int64_t epoch_id,
           const std::vector<std::optional<std::string>>& messages) {
  std::vector<const uint8_t*> pointers;
  std::vector<int64_t> lengths;
  for (const auto& message : messages) {
    pointers.push_back(message ? reinterpret_cast<const uint8_t*>(message->data()) : nullptr);
    lengths.push_back(message ? static_cast<int64_t>(message->size()) : 0);
  }
  auto finish = abort ? Library().abort : Library().commit;
  return finish(writer, epoch_id, pointers.data(), lengths.data(),
                static_cast<int64_t>(messages.size()));
}

}  // namespace

SPARK_EXPORT_DATA_SOURCE(Source);

TEST(StreamingOperationsDeclareTheirCapabilitiesAndCallbacks) {
  const SparkDsLibrary& library = Library();
  CHECK(library.create_reader != nullptr && library.create_stream_reader != nullptr);
  CHECK(library.initial_offset != nullptr && library.latest_offset != nullptr);
  CHECK(library.stream_partitions != nullptr && library.serialize_stream_reader != nullptr);
  CHECK(library.commit_offset != nullptr && library.close_stream_reader != nullptr);
  CHECK(library.read != nullptr && library.create_writer != nullptr);
  CHECK(library.create_data_writer != nullptr && library.close_data_writer != nullptr);

  uint64_t mask = 0;
  library.close(CreateSource("", "", &mask));
  CHECK_EQ(mask, uint64_t{SPARK_DS_BATCH_READ | SPARK_DS_MICRO_BATCH_READ |
                          SPARK_DS_STREAMING_WRITE | SPARK_DS_TRUNCATE});
  // Truncation goes with the only write that supports it.
  library.close(CreateSource("", "no_streaming_write", &mask));
  CHECK_EQ(mask, uint64_t{SPARK_DS_BATCH_READ | SPARK_DS_MICRO_BATCH_READ});
}

TEST(StreamsPlanMicroBatchesBetweenJsonOffsets) {
  ClearEvents();
  const SparkDsLibrary& library = Library();
  void* reader = StreamReader("");
  CHECK(Events() == std::vector<std::string>({"StreamCreated"}));
  auto [code, state_before] =
      Emitted([&](void* sink) { return library.serialize_stream_reader(reader, &Collect, sink); });
  // A stream has no state before its first micro-batch.
  CHECK(code != 0);
  CHECK_CONTAINS(LastError(library), "no micro-batch");

  std::string initial =
      Single([&](void* sink) { return library.initial_offset(reader, &Collect, sink); });
  CHECK_EQ(initial, std::string(R"({"note":"two\nlines","position":0})"));
  latest = 3;
  auto [latest_code, offsets] = Latest(reader);
  CHECK_SUCCESS(library, latest_code);
  std::string end = offsets.at(0);
  // An offset must be on one line.
  CHECK(end.find('\n') == std::string::npos);

  auto [plan_code, partitions] = Emitted([&](void* sink) {
    return library.stream_partitions(reader, initial.c_str(), end.c_str(), &Collect, sink);
  });
  CHECK_SUCCESS(library, plan_code);
  std::string state =
      Single([&](void* sink) { return library.serialize_stream_reader(reader, &Collect, sink); });
  std::vector<int64_t> values;
  for (const std::string& partition : partitions) {
    std::vector<int64_t> read = Read(state, partition);
    values.insert(values.end(), read.begin(), read.end());
  }
  CHECK(values == std::vector<int64_t>({0, 10, 20}));
  CHECK_SUCCESS(library, library.commit_offset(reader, end.c_str()));
  CHECK(Events() == std::vector<std::string>({"StreamCreated", "OffsetCommitted(3)"}));

  // A batch scan of the same source reads its own partitions through the same function.
  void* source = CreateSource("", "");
  struct ArrowSchema read_schema{};
  CHECK_OK(arrow::ExportSchema(*Schema(), &read_schema));
  void* planner = nullptr;
  CHECK_SUCCESS(library, library.create_reader(source, &read_schema, &planner));
  library.close(source);
  auto [partitions_code, batch_partitions] =
      Emitted([&](void* sink) { return library.partitions(planner, &Collect, sink); });
  CHECK_SUCCESS(library, partitions_code);
  std::string batch_state =
      Single([&](void* sink) { return library.serialize_reader(planner, &Collect, sink); });
  library.close_reader(planner);
  CHECK(Read(batch_state, batch_partitions.at(0)) == std::vector<int64_t>({105}));

  // An offset that does not decode fails the stream, and so every later call.
  auto [invalid_code, invalid_partitions] = Emitted([&](void* sink) {
    return library.stream_partitions(reader, initial.c_str(), "not an offset", &Collect, sink);
  });
  CHECK(invalid_code != 0);
  std::string failure = LastError(library);
  CHECK_CONTAINS(failure, "cannot decode the offset not an offset");
  auto [failed_code, failed_offsets] = Latest(reader);
  CHECK(failed_code != 0);
  CHECK_EQ(LastError(library), failure);
  library.close_stream_reader(reader);
}

TEST(AnExceptionFailsTheStreamForEveryLaterCall) {
  const SparkDsLibrary& library = Library();
  void* reader = StreamReader("latest_throw");
  auto [code, offsets] = Latest(reader);
  CHECK(code != 0);
  std::string failure = LastError(library);
  CHECK_CONTAINS(failure, "finding the latest offset threw");
  auto [initial_code, initial] =
      Emitted([&](void* sink) { return library.initial_offset(reader, &Collect, sink); });
  CHECK(initial_code != 0);
  CHECK_EQ(LastError(library), failure);
  library.close_stream_reader(reader);
}

TEST(StreamingWritesPassTheQueryAndTheEpochToTheWriteAndItsTasks) {
  ClearEvents();
  const SparkDsLibrary& library = Library();
  ASSIGN_OR_FAIL(void* writer, CreateWriter(SPARK_DS_WRITE_TRUNCATE, "query-1"));
  std::string state =
      Single([&](void* sink) { return library.serialize_writer(writer, &Collect, sink); });

  // Each task gets the epoch of its micro-batch, which must not be that of a batch write.
  sn::testing::HostTask task(0, 10);
  auto create = [&](int64_t epoch_id, void** data_writer) {
    return library.create_data_writer(&task.context, reinterpret_cast<const uint8_t*>(state.data()),
                                      static_cast<int64_t>(state.size()), epoch_id, data_writer);
  };
  void* data_writer = nullptr;
  CHECK(create(-1, &data_writer) != 0);
  CHECK_CONTAINS(LastError(library), "epoch");
  CHECK(data_writer == nullptr);
  CHECK_SUCCESS(library, create(7, &data_writer));
  arrow::Int64Builder builder;
  CHECK_OK(builder.AppendValues({4, 2}));
  auto batch = arrow::RecordBatch::Make(Schema(), 2, {builder.Finish().ValueOrDie()});
  struct ArrowArray array{};
  struct ArrowSchema array_schema{};
  CHECK_OK(arrow::ExportRecordBatch(*batch, &array, &array_schema));
  CHECK_SUCCESS(library, library.write(data_writer, &array, &array_schema));
  std::string message =
      Single([&](void* sink) { return library.commit_data_writer(data_writer, &Collect, sink); });
  library.close_data_writer(data_writer);

  CHECK(Finish(writer, false, -1, {message}) != 0);
  CHECK_CONTAINS(LastError(library), "epoch");
  CHECK_SUCCESS(library, Finish(writer, false, 7, {message}));
  library.close_writer(writer);

  ASSIGN_OR_FAIL(void* appending, CreateWriter(SPARK_DS_WRITE_APPEND, "query-1"));
  CHECK_SUCCESS(library, Finish(appending, true, 8, {std::nullopt}));
  library.close_writer(appending);

  // The source has no batch writes.
  arrow::Result<void*> batch_write = CreateWriter(SPARK_DS_WRITE_APPEND, nullptr);
  CHECK(!batch_write.ok());
  CHECK_CONTAINS(batch_write.status().message(), "does not support batch writes");

  Message committed;
  committed.epoch_id = 7;
  committed.values = {4, 2};
  CHECK(Events() == std::vector<std::string>({
                        "WriteCreated(query-1, truncate)",
                        "EpochCommitted(7, " + Describe({committed}) + ")",
                        "WriteCreated(query-1, append)",
                        "EpochAborted(8, [null])",
                    }));
}
