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
using spark::native::ReadLimit;
using spark::native::testing::Collect;
using spark::native::testing::CreateArguments;
using spark::native::testing::LastError;
using spark::native::testing::Plan;

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

std::string DescribeLimit(const ReadLimit& limit) {
  return limit.kind() == ReadLimit::Kind::kMaxRows ? std::to_string(limit.max_rows()) : "all";
}

class Stream : public sn::MicroBatchStream<Offset, StreamFactory> {
 public:
  Stream(std::string failure, std::optional<int64_t> max_rows)
      : failure_(std::move(failure)), max_rows_(max_rows) {}

  arrow::Result<Offset> InitialOffset() override { return Offset(); }

  arrow::Result<ReadLimit> DefaultReadLimit() const override {
    return max_rows_ ? ReadLimit::MaxRows(*max_rows_) : ReadLimit::AllAvailable();
  }

  arrow::Result<Offset> LatestOffset(const sn::LatestOffsetInfo<Offset>& info) override {
    if (failure_ == "latest_throw") {
      throw std::runtime_error("finding the latest offset threw");
    }
    uint64_t start = info.start.position;
    Record("LatestOffset(" + std::to_string(start) + ", " + DescribeLimit(info.limit) + ", " +
           (info.run_end ? std::to_string(info.run_end->position) : "none") + ")");
    uint64_t end = latest.load();
    if (info.limit.kind() == ReadLimit::Kind::kMaxRows) {
      end = std::min(end, start + static_cast<uint64_t>(info.limit.max_rows()));
    }
    if (info.run_end) {
      end = std::min(end, info.run_end->position);
    }
    Offset offset;
    offset.position = end;
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
  std::optional<int64_t> max_rows_;
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
    if (!option("maxrows").empty()) {
      source.max_rows_ = std::stoll(option("maxrows"));
    }
    return source;
  }

  arrow::Result<std::shared_ptr<arrow::Schema>> InferSchema() const override { return Schema(); }

  Capabilities capabilities() const override {
    if (capabilities_ == "no_streaming_write") {
      return Capabilities::All().Without(Capabilities::kStreamingWrite);
    }
    if (capabilities_ == "append_only") {
      return Capabilities::All().Without(Capabilities::kStreamingTruncate);
    }
    return Capabilities::All();
  }

  arrow::Result<Scan> NewScan(const sn::ScanInfo&) const { return Scan(); }

  arrow::Result<Stream> NewMicroBatchStream(const sn::MicroBatchStreamInfo& info) const {
    if (!info.schema->Equals(*Schema())) {
      return arrow::Status::Invalid("unsupported stream schema");
    }
    Record("StreamCreated(" + info.checkpoint_location + ")");
    return Stream(failure_, max_rows_);
  }

  arrow::Result<Write> NewStreamingWrite(const sn::StreamingWriteInfo& info) const {
    Record("WriteCreated(" + info.query_id + ", " +
           (info.mode == sn::WriteMode::kTruncate ? "truncate" : "append") + ")");
    return Write();
  }

 private:
  std::string failure_;
  std::string capabilities_;
  std::optional<int64_t> max_rows_;
};

const SparkDsLibrary& Library() {
  static const SparkDsLibrary& library = sn::testing::InitializeLibrary();
  return library;
}

using OptionList = std::vector<std::pair<std::string, std::string>>;

void* CreateSource(OptionList options, uint64_t* mask = nullptr) {
  CreateArguments arguments("streaming_test", std::move(options));
  uint64_t declared = 0;
  void* source = nullptr;
  CHECK_SUCCESS(Library(), Library().create(arguments.info(), &declared, &source));
  if (mask != nullptr) {
    *mask = declared;
  }
  return source;
}

const char* const kCheckpoint = "file:/tmp/checkpoint/sources/0";

void* StreamReader(OptionList options) {
  void* source = CreateSource(std::move(options));
  struct ArrowSchema schema{};
  CHECK_OK(arrow::ExportSchema(*Schema(), &schema));
  SparkDsStreamReaderInfo info{sizeof(SparkDsStreamReaderInfo), kCheckpoint};
  void* reader = nullptr;
  CHECK_SUCCESS(Library(), Library().create_stream_reader(source, &schema, &info, &reader));
  if (schema.release != nullptr) {
    schema.release(&schema);
  }
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

SparkDsReadLimit Limit(int32_t kind, int64_t max_rows) {
  return SparkDsReadLimit{sizeof(SparkDsReadLimit), kind, max_rows};
}

std::pair<int, std::vector<std::string>> Latest(void* reader, const std::string& start,
                                                const SparkDsReadLimit& limit,
                                                const char* run_end = nullptr) {
  SparkDsLatestOffsetInfo info{sizeof(SparkDsLatestOffsetInfo), start.c_str(), &limit, run_end};
  return Emitted(
      [&](void* sink) { return Library().latest_offset(reader, &info, &Collect, sink); });
}

std::pair<int, Plan> StreamPartitions(void* reader, const std::string& start,
                                      const std::string& end) {
  SparkDsStreamPartitionsInfo info{sizeof(SparkDsStreamPartitionsInfo), start.c_str(), end.c_str()};
  Plan plan;
  int code =
      Library().stream_partitions(reader, &info, &Plan::AddPartition, &Plan::SetState, &plan);
  if (code != 0) {
    CHECK(plan.partitions.empty() && plan.states.empty());
  }
  return {code, plan};
}

/// Reads a partition with the read function that batch scans and micro-batches share, or returns
/// the error of the library.
arrow::Result<std::vector<int64_t>> Read(int32_t kind, const std::string& state,
                                         const std::string& partition) {
  sn::testing::HostTask task(0, 10);
  struct ArrowSchema schema{};
  CHECK_OK(arrow::ExportSchema(*Schema(), &schema));
  SparkDsReadInfo info{sizeof(SparkDsReadInfo),
                       kind,
                       &schema,
                       reinterpret_cast<const uint8_t*>(state.data()),
                       static_cast<int64_t>(state.size()),
                       reinterpret_cast<const uint8_t*>(partition.data()),
                       static_cast<int64_t>(partition.size())};
  struct ArrowArrayStream stream{};
  int code = Library().read(&task.context, &info, &stream);
  if (schema.release != nullptr) {
    schema.release(&schema);
  }
  if (code != 0) {
    CHECK(stream.release == nullptr);
    return arrow::Status::Invalid(LastError(Library()));
  }
  ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::RecordBatchReader> reader,
                        arrow::ImportRecordBatchReader(&stream));
  std::vector<int64_t> values;
  while (true) {
    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::RecordBatch> batch, reader->Next());
    if (batch == nullptr) {
      break;
    }
    auto column = std::static_pointer_cast<arrow::Int64Array>(batch->column(0));
    values.insert(values.end(), column->raw_values(), column->raw_values() + column->length());
  }
  return values;
}

/// A writer, with the state of its data writers.
struct CreatedWriter {
  void* writer;
  std::string state;
};

/// Creates a writer, or returns the error of the library.
arrow::Result<CreatedWriter> CreateWriter(OptionList options, int32_t mode, const char* query_id) {
  void* source = CreateSource(std::move(options));
  struct ArrowSchema input{};
  CHECK_OK(arrow::ExportSchema(*Schema(), &input));
  SparkDsWriteInfo info{sizeof(SparkDsWriteInfo), mode, query_id};
  void* writer = nullptr;
  std::vector<std::string> states;
  int code = Library().create_writer(source, &input, &info, &Collect, &states, &writer);
  if (input.release != nullptr) {
    input.release(&input);
  }
  arrow::Result<CreatedWriter> result = arrow::Status::Invalid("");
  if (code == 0) {
    CHECK(states.size() == 1);
    result = CreatedWriter{writer, states[0]};
  } else {
    CHECK(writer == nullptr && states.empty());
    result = arrow::Status::Invalid(LastError(Library()));
  }
  Library().close(source);
  return result;
}

int Finish(void* writer, bool abort, int64_t epoch_id,
           const std::vector<std::optional<std::string>>& messages, bool rejected = false) {
  std::vector<const uint8_t*> pointers;
  std::vector<int64_t> lengths;
  for (const auto& message : messages) {
    pointers.push_back(message ? reinterpret_cast<const uint8_t*>(message->data()) : nullptr);
    lengths.push_back(message ? static_cast<int64_t>(message->size()) : 0);
  }
  SparkDsFinishWriteInfo info{
      sizeof(SparkDsFinishWriteInfo),        epoch_id,        pointers.data(), lengths.data(),
      static_cast<int64_t>(messages.size()), rejected ? 1 : 0};
  auto finish = abort ? Library().abort : Library().commit;
  return finish(writer, &info);
}

}  // namespace

SPARK_EXPORT_DATA_SOURCE(Source);

TEST(StreamingOperationsDeclareTheirCapabilitiesAndCallbacks) {
  const SparkDsLibrary& library = Library();
  CHECK(library.create_reader != nullptr && library.create_stream_reader != nullptr);
  CHECK(library.initial_offset != nullptr && library.latest_offset != nullptr);
  CHECK(library.stream_partitions != nullptr && library.default_read_limit != nullptr);
  CHECK(library.commit_offset != nullptr && library.close_stream_reader != nullptr);
  CHECK(library.read != nullptr && library.create_writer != nullptr);
  CHECK(library.create_data_writer != nullptr && library.close_data_writer != nullptr);

  // Every stream supports admission control, and only streaming writes truncate.
  uint64_t reads = SPARK_DS_BATCH_READ | SPARK_DS_MICRO_BATCH_READ | SPARK_DS_ADMISSION_CONTROL;
  for (const auto& [capabilities, expected] : std::vector<std::pair<std::string, uint64_t>>{
           {"", reads | SPARK_DS_STREAMING_WRITE | SPARK_DS_STREAMING_TRUNCATE},
           {"append_only", reads | SPARK_DS_STREAMING_WRITE},
           // Truncation goes with the only write that supports it.
           {"no_streaming_write", reads}}) {
    uint64_t mask = 0;
    library.close(CreateSource({{"capabilities", capabilities}}, &mask));
    CHECK_EQ(mask, expected);
  }
}

TEST(StreamsPlanMicroBatchesBetweenJsonOffsets) {
  ClearEvents();
  const SparkDsLibrary& library = Library();
  void* reader = StreamReader({});
  CHECK(Events() == std::vector<std::string>({std::string("StreamCreated(") + kCheckpoint + ")"}));

  SparkDsReadLimit limit = Limit(SPARK_DS_READ_MAX_ROWS, 99);
  CHECK_SUCCESS(library, library.default_read_limit(reader, &limit));
  // No limit by default.
  CHECK_EQ(limit.kind, SPARK_DS_READ_ALL_AVAILABLE);

  SparkDsInitialOffsetInfo initial_info{sizeof(SparkDsInitialOffsetInfo)};
  std::string initial = Single(
      [&](void* sink) { return library.initial_offset(reader, &initial_info, &Collect, sink); });
  CHECK_EQ(initial, std::string(R"({"note":"two\nlines","position":0})"));
  latest = 3;
  auto [latest_code, offsets] = Latest(reader, initial, Limit(SPARK_DS_READ_ALL_AVAILABLE, 0));
  CHECK_SUCCESS(library, latest_code);
  std::string end = offsets.at(0);
  // An offset must be on one line.
  CHECK(end.find('\n') == std::string::npos);

  auto [plan_code, plan] = StreamPartitions(reader, initial, end);
  CHECK_SUCCESS(library, plan_code);
  CHECK(plan.states.size() == 1);
  std::vector<int64_t> values;
  for (const std::string& partition : plan.partitions) {
    ASSIGN_OR_FAIL(std::vector<int64_t> read,
                   Read(SPARK_DS_READ_MICRO_BATCH, plan.states[0], partition));
    values.insert(values.end(), read.begin(), read.end());
  }
  CHECK(values == std::vector<int64_t>({0, 10, 20}));
  SparkDsCommitOffsetInfo commit{sizeof(SparkDsCommitOffsetInfo), end.c_str()};
  CHECK_SUCCESS(library, library.commit_offset(reader, &commit));
  CHECK(Events() == std::vector<std::string>({std::string("StreamCreated(") + kCheckpoint + ")",
                                              "LatestOffset(0, all, none)", "OffsetCommitted(3)"}));

  // A batch scan of the same source reads its own partitions through the same function.
  void* source = CreateSource({});
  struct ArrowSchema read_schema{};
  CHECK_OK(arrow::ExportSchema(*Schema(), &read_schema));
  SparkDsReaderInfo reader_info{sizeof(SparkDsReaderInfo)};
  void* planner = nullptr;
  CHECK_SUCCESS(library, library.create_reader(source, &read_schema, &reader_info, &planner));
  if (read_schema.release != nullptr) {
    read_schema.release(&read_schema);
  }
  library.close(source);
  SparkDsPartitionsInfo partitions_info{sizeof(SparkDsPartitionsInfo)};
  Plan batch_plan;
  CHECK_SUCCESS(library, library.partitions(planner, &partitions_info, &Plan::AddPartition,
                                            &Plan::SetState, &batch_plan));
  library.close_reader(planner);
  CHECK(batch_plan.states.size() == 1);
  const std::string& batch_state = batch_plan.states[0];
  ASSIGN_OR_FAIL(std::vector<int64_t> batch_values,
                 Read(SPARK_DS_READ_BATCH, batch_state, batch_plan.partitions.at(0)));
  CHECK(batch_values == std::vector<int64_t>({105}));
  // The kind of the read selects the factory: a batch state is not that of a micro-batch.
  CHECK(!Read(SPARK_DS_READ_MICRO_BATCH, batch_state, batch_plan.partitions.at(0)).ok());
  CHECK_CONTAINS(Read(7, batch_state, batch_plan.partitions.at(0)).status().message(),
                 "unsupported kind of read: 7");

  // An offset that does not decode fails the call, after which Spark closes the reader.
  auto [invalid_code, invalid_plan] = StreamPartitions(reader, initial, "not an offset");
  CHECK(invalid_code != 0);
  CHECK_CONTAINS(LastError(library), "cannot decode the offset not an offset");
  library.close_stream_reader(reader);
}

TEST(StreamsLimitTheirMicroBatchesAndRunsWithAdmissionControl) {
  ClearEvents();
  const SparkDsLibrary& library = Library();
  void* reader = StreamReader({{"maxrows", "2"}});
  SparkDsReadLimit limit = Limit(SPARK_DS_READ_ALL_AVAILABLE, 0);
  CHECK_SUCCESS(library, library.default_read_limit(reader, &limit));
  CHECK_EQ(limit.kind, SPARK_DS_READ_MAX_ROWS);
  CHECK_EQ(limit.max_rows, int64_t{2});

  latest = 5;
  std::string start = nlohmann::json(Offset{1}).dump();
  std::string run_end = nlohmann::json(Offset{2}).dump();
  // The micro-batch of a limit, then that of a limit within the end of a run.
  for (const auto& [end, expected] :
       std::vector<std::pair<const char*, uint64_t>>{{nullptr, 3}, {run_end.c_str(), 2}}) {
    auto [code, offsets] = Latest(reader, start, limit, end);
    CHECK_SUCCESS(library, code);
    CHECK_EQ(nlohmann::json::parse(offsets.at(0)).get<Offset>().position, expected);
  }
  auto [unknown_code, unknown] = Latest(reader, start, Limit(9, 0));
  CHECK(unknown_code != 0);
  CHECK_CONTAINS(LastError(library), "unsupported kind of read limit: 9");
  std::vector<std::string> recorded = Events();
  CHECK(std::vector(recorded.begin() + 1, recorded.end()) ==
        std::vector<std::string>({"LatestOffset(1, 2, none)", "LatestOffset(1, 2, 2)"}));
  library.close_stream_reader(reader);
}

TEST(AnExceptionIsTheFailureOfItsCall) {
  const SparkDsLibrary& library = Library();
  void* reader = StreamReader({{"failure", "latest_throw"}});
  auto [code, offsets] =
      Latest(reader, nlohmann::json(Offset()).dump(), Limit(SPARK_DS_READ_ALL_AVAILABLE, 0));
  CHECK(code != 0);
  CHECK_CONTAINS(LastError(library), "finding the latest offset threw");
  library.close_stream_reader(reader);
}

TEST(StreamingWritesPassTheQueryAndTheEpochToTheWriteAndItsTasks) {
  ClearEvents();
  const SparkDsLibrary& library = Library();
  ASSIGN_OR_FAIL(CreatedWriter created, CreateWriter({}, SPARK_DS_WRITE_TRUNCATE, "query-1"));

  // Each task gets the epoch of its micro-batch, and the schema of the data.
  sn::testing::HostTask task(0, 10);
  auto create = [&](int64_t epoch_id, void** data_writer) {
    struct ArrowSchema input{};
    CHECK_OK(arrow::ExportSchema(*Schema(), &input));
    SparkDsDataWriterInfo info{sizeof(SparkDsDataWriterInfo), &input,
                               reinterpret_cast<const uint8_t*>(created.state.data()),
                               static_cast<int64_t>(created.state.size()), epoch_id};
    int code = library.create_data_writer(&task.context, &info, data_writer);
    if (input.release != nullptr) {
      input.release(&input);
    }
    return code;
  };
  void* data_writer = nullptr;
  CHECK(create(-2, &data_writer) != 0);
  CHECK_CONTAINS(LastError(library), "invalid epoch ID -2");
  // -1 is the epoch of a batch write, which this source does not have.
  CHECK(create(-1, &data_writer) != 0);
  CHECK(data_writer == nullptr);
  CHECK_SUCCESS(library, create(7, &data_writer));
  arrow::Int64Builder builder;
  CHECK_OK(builder.AppendValues({4, 2}));
  auto batch = arrow::RecordBatch::Make(Schema(), 2, {builder.Finish().ValueOrDie()});
  struct ArrowArray array{};
  CHECK_OK(arrow::ExportRecordBatch(*batch, &array));
  CHECK_SUCCESS(library, library.write(data_writer, &array));
  SparkDsFinishDataWriterInfo finish_task{sizeof(SparkDsFinishDataWriterInfo)};
  std::string message = Single([&](void* sink) {
    return library.commit_data_writer(data_writer, &finish_task, &Collect, sink);
  });
  library.close_data_writer(data_writer);

  CHECK_SUCCESS(library, Finish(created.writer, false, 7, {message}));
  library.close_writer(created.writer);

  ASSIGN_OR_FAIL(CreatedWriter appending, CreateWriter({}, SPARK_DS_WRITE_APPEND, "query-1"));
  CHECK_SUCCESS(library, Finish(appending.writer, true, 8, {std::nullopt}));
  library.close_writer(appending.writer);

  // A rejected writer is destroyed without its abort, as when its creation fails.
  ASSIGN_OR_FAIL(CreatedWriter rejected, CreateWriter({}, SPARK_DS_WRITE_APPEND, "query-1"));
  CHECK_SUCCESS(library, Finish(rejected.writer, true, -1, {}, /*rejected=*/true));
  library.close_writer(rejected.writer);

  // The source has no batch writes, and an append-only source does not truncate.
  arrow::Result<CreatedWriter> batch_write = CreateWriter({}, SPARK_DS_WRITE_APPEND, nullptr);
  CHECK(!batch_write.ok());
  CHECK_CONTAINS(batch_write.status().message(), "does not support batch writes");
  arrow::Result<CreatedWriter> complete =
      CreateWriter({{"capabilities", "append_only"}}, SPARK_DS_WRITE_TRUNCATE, "query-2");
  CHECK(!complete.ok());
  CHECK_CONTAINS(complete.status().message(), "does not support the output mode complete");

  Message committed;
  committed.epoch_id = 7;
  committed.values = {4, 2};
  CHECK(Events() == std::vector<std::string>({
                        "WriteCreated(query-1, truncate)",
                        "EpochCommitted(7, " + Describe({committed}) + ")",
                        "WriteCreated(query-1, append)",
                        "EpochAborted(8, [null])",
                        "WriteCreated(query-1, append)",
                    }));
}
