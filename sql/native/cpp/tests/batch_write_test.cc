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

// Batch writes through the function table that the library exports: driver coordinators, task
// writers restored from serialized state, and the lifecycle that the SDK enforces.

#include <algorithm>
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
using spark::native::testing::HostTask;
using spark::native::testing::LastError;

namespace {

std::mutex mutex;
std::vector<std::string> events;

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

size_t Count(const std::string& event) {
  std::vector<std::string> recorded = Events();
  return static_cast<size_t>(std::count(recorded.begin(), recorded.end(), event));
}

std::shared_ptr<arrow::Schema> Schema() {
  return arrow::schema({arrow::field("value", arrow::int64(), false)});
}

std::string Flag(bool value) { return value ? "true" : "false"; }

struct Message {
  int32_t partition = 0;
  int64_t attempt = 0;
  std::vector<int64_t> values;
  int64_t batches = 0;
  std::string failure;
};

void to_json(nlohmann::json& json, const Message& message) {
  if (message.failure == "message_serialization") {
    throw std::runtime_error("message serialization failed");
  }
  json = {{"partition", message.partition},
          {"attempt", message.attempt},
          {"values", message.values},
          {"batches", message.batches},
          {"failure", message.failure}};
}

void from_json(const nlohmann::json& json, Message& message) {
  json.at("partition").get_to(message.partition);
  json.at("attempt").get_to(message.attempt);
  json.at("values").get_to(message.values);
  json.at("batches").get_to(message.batches);
  json.at("failure").get_to(message.failure);
}

std::string Describe(const std::vector<std::optional<Message>>& messages) {
  nlohmann::json json = nlohmann::json::array();
  for (const auto& message : messages) {
    json.push_back(message ? nlohmann::json(*message) : nlohmann::json());
  }
  return json.dump();
}

std::string Describe(const std::vector<Message>& messages) {
  return Describe(std::vector<std::optional<Message>>(messages.begin(), messages.end()));
}

class Writer : public sn::DataWriter<Message> {
 public:
  Writer(sn::TaskContext task, std::string failure) : task_(task), failure_(std::move(failure)) {}

  ~Writer() override { Record("TaskDestroyed(" + Flag(task_.IsInterrupted()) + ")"); }

  arrow::Status Write(std::shared_ptr<arrow::RecordBatch> batch) override {
    Record("Batch(" + std::to_string(batch->num_rows()) + ")");
    // Retain the imported buffers after the ABI call returns and its inputs are released.
    batches_.push_back(std::move(batch));
    if (failure_ == "write_error") return arrow::Status::Invalid("writing batch failed");
    if (failure_ == "write_throw") throw std::runtime_error("writing batch threw");
    return arrow::Status::OK();
  }

  arrow::Result<Message> Commit() override {
    Record("TaskCommit");
    if (task_.IsInterrupted()) return arrow::Status::Cancelled("task cancelled during commit");
    if (failure_ == "task_commit_error") return arrow::Status::Invalid("task commit failed");
    if (failure_ == "task_commit_throw") throw std::runtime_error("task commit threw");
    Message message;
    message.partition = task_.partition_id();
    message.attempt = task_.task_attempt_id();
    for (const auto& batch : batches_) {
      auto values = std::static_pointer_cast<arrow::Int64Array>(batch->column(0));
      message.values.insert(message.values.end(), values->raw_values(),
                            values->raw_values() + values->length());
    }
    message.batches = static_cast<int64_t>(batches_.size());
    message.failure = failure_;
    return message;
  }

  arrow::Status Abort() override {
    Record("TaskAbort(" + Flag(task_.IsInterrupted()) + ")");
    batches_.clear();
    return arrow::Status::OK();
  }

 private:
  sn::TaskContext task_;
  std::string failure_;
  std::vector<std::shared_ptr<arrow::RecordBatch>> batches_;
};

struct Factory : sn::DataWriterFactory<Message> {
  std::string failure;

  arrow::Result<std::unique_ptr<sn::DataWriter<Message>>> Open(
      const sn::WriterContext& context) const override {
    if (!context.schema->Equals(*Schema()) || context.epoch_id.has_value()) {
      return arrow::Status::Invalid("the writer context lost the input schema");
    }
    return std::make_unique<Writer>(context.task, failure);
  }
};

void to_json(nlohmann::json& json, const Factory& factory) {
  if (factory.failure == "factory_serialization") {
    throw std::runtime_error("factory serialization failed");
  }
  json = {{"failure", factory.failure}};
}

void from_json(const nlohmann::json& json, Factory& factory) {
  json.at("failure").get_to(factory.failure);
}

class Driver : public sn::BatchWrite<Factory> {
 public:
  static constexpr bool kSupportsTruncate = true;

  explicit Driver(std::string failure) : failure_(std::move(failure)) {}

  arrow::Result<Factory> CreateWriterFactory() const override {
    Record("FactoryCreated");
    if (failure_ == "writer_factory_error") {
      return arrow::Status::Invalid("creating the writer factory failed");
    }
    Factory factory;
    factory.failure = failure_;
    return factory;
  }

  arrow::Status Commit(std::vector<Message> messages) override {
    Record("DriverCommit(" + Describe(messages) + ")");
    if (failure_ == "driver_commit_error") return arrow::Status::Invalid("driver commit failed");
    if (failure_ == "driver_commit_throw") throw std::runtime_error("driver commit threw");
    return arrow::Status::OK();
  }

  arrow::Status Abort(std::vector<std::optional<Message>> messages) override {
    Record("DriverAbort(" + Describe(messages) + ")");
    return arrow::Status::OK();
  }

 private:
  std::string failure_;
};

class Source : public sn::DataSource {
 public:
  using BatchWrite = Driver;

  // A sink of any input: Spark requires a user-specified schema to read it.
  static constexpr bool kInfersSchema = false;

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

  Capabilities capabilities() const override {
    if (capabilities_ == "none") return Capabilities::Empty();
    if (capabilities_ == "append") return Capabilities::All().Without(Capabilities::kTruncate);
    return Capabilities::All();
  }

  arrow::Result<Driver> NewWrite(const sn::WriteInfo& info) const {
    if (!info.schema->Equals(*Schema())) {
      return arrow::Status::Invalid("unsupported input schema");
    }
    Record("WriteCreated(" + Flag(info.mode == sn::WriteMode::kTruncate) + ")");
    return Driver(failure_);
  }

 private:
  std::string failure_;
  std::string capabilities_;
};

const SparkDsLibrary& Library() {
  static const SparkDsLibrary& library = sn::testing::InitializeLibrary();
  return library;
}

/// A handle of the library, which closes it.
class Handle {
 public:
  Handle(void* pointer, void (*close)(void*)) : pointer_(pointer), close_(close) {}
  Handle(const Handle&) = delete;
  Handle& operator=(const Handle&) = delete;
  ~Handle() {
    if (pointer_ != nullptr) {
      close_(pointer_);
    }
  }

  void* get() const { return pointer_; }

 private:
  void* pointer_;
  void (*close_)(void*);
};

std::unique_ptr<Handle> CreateSource(const std::string& failure, const std::string& capabilities,
                                     uint64_t* mask = nullptr) {
  CreateArguments arguments("write_test", {{"failure", failure}, {"capabilities", capabilities}});
  uint64_t declared = 0;
  void* source = nullptr;
  CHECK_SUCCESS(Library(), Library().create(arguments.info(), &declared, &source));
  CHECK(source != nullptr);
  if (mask != nullptr) {
    *mask = declared;
  }
  return std::make_unique<Handle>(source, Library().close);
}

int CreateDriver(const Handle& source, int32_t mode, const char* query_id, void** writer) {
  struct ArrowSchema input{};
  CHECK_OK(arrow::ExportSchema(*Schema(), &input));
  SparkDsWriteInfo info{sizeof(SparkDsWriteInfo), mode, query_id};
  *writer = nullptr;
  int code = Library().create_writer(source.get(), &input, &info, writer);
  if (input.release != nullptr) {
    input.release(&input);
  }
  return code;
}

std::unique_ptr<Handle> OpenDriver(const Handle& source, int32_t mode) {
  void* writer = nullptr;
  CHECK_SUCCESS(Library(), CreateDriver(source, mode, nullptr, &writer));
  CHECK(writer != nullptr);
  return std::make_unique<Handle>(writer, Library().close_writer);
}

int Serialize(const Handle& driver, std::vector<std::string>* output) {
  return Library().serialize_writer(driver.get(), &Collect, output);
}

std::string State(const Handle& driver) {
  std::vector<std::string> output;
  CHECK_SUCCESS(Library(), Serialize(driver, &output));
  CHECK(output.size() == 1);
  return output[0];
}

/// The data writer of a task, which closes it before its host context expires.
struct Task {
  std::unique_ptr<HostTask> host;
  std::unique_ptr<Handle> writer;
};

std::unique_ptr<Task> OpenTask(const std::string& state, int32_t partition) {
  auto task = std::make_unique<Task>();
  task->host = std::make_unique<HostTask>(partition, 100 + partition);
  void* writer = nullptr;
  CHECK_SUCCESS(Library(), Library().create_data_writer(
                               &task->host->context, reinterpret_cast<const uint8_t*>(state.data()),
                               static_cast<int64_t>(state.size()), -1, &writer));
  CHECK(writer != nullptr);
  task->writer = std::make_unique<Handle>(writer, Library().close_data_writer);
  return task;
}

std::shared_ptr<arrow::RecordBatch> Batch(const std::vector<int64_t>& values) {
  arrow::Int64Builder builder;
  CHECK_OK(builder.AppendValues(values));
  return arrow::RecordBatch::Make(Schema(), static_cast<int64_t>(values.size()),
                                  {builder.Finish().ValueOrDie()});
}

int Write(const Task& task, const std::shared_ptr<arrow::RecordBatch>& batch) {
  struct ArrowArray array{};
  struct ArrowSchema schema{};
  CHECK_OK(arrow::ExportRecordBatch(*batch, &array, &schema));
  int code = Library().write(task.writer->get(), &array, &schema);
  if (code == 0) {
    // Retained input must be moved from the caller.
    CHECK(array.release == nullptr && schema.release == nullptr);
  }
  if (array.release != nullptr) array.release(&array);
  if (schema.release != nullptr) schema.release(&schema);
  return code;
}

int TaskCommit(const Task& task, std::vector<std::string>* messages) {
  return Library().commit_data_writer(task.writer->get(), &Collect, messages);
}

std::string TaskMessage(const Task& task) {
  std::vector<std::string> messages;
  CHECK_SUCCESS(Library(), TaskCommit(task, &messages));
  CHECK(messages.size() == 1);
  return messages[0];
}

int Finish(const Handle& driver, bool abort,
           const std::vector<std::optional<std::string>>& messages, int64_t epoch_id = -1) {
  std::vector<const uint8_t*> pointers;
  std::vector<int64_t> lengths;
  for (const auto& message : messages) {
    pointers.push_back(message ? reinterpret_cast<const uint8_t*>(message->data()) : nullptr);
    lengths.push_back(message ? static_cast<int64_t>(message->size()) : 0);
  }
  auto finish = abort ? Library().abort : Library().commit;
  return finish(driver.get(), epoch_id, pointers.data(), lengths.data(),
                static_cast<int64_t>(messages.size()));
}

Message Expected(int32_t partition, std::vector<int64_t> values, int64_t batches,
                 std::string failure) {
  Message message;
  message.partition = partition;
  message.attempt = 100 + partition;
  message.values = std::move(values);
  message.batches = batches;
  message.failure = std::move(failure);
  return message;
}

}  // namespace

SPARK_EXPORT_DATA_SOURCE(Source);

TEST(WriteTypesAndOptionsControlCapabilities) {
  ClearEvents();
  const SparkDsLibrary& library = Library();
  // kInfersSchema = false leaves schema inference to Spark's error.
  CHECK(library.schema == nullptr);
  CHECK(library.create_reader == nullptr && library.read == nullptr);
  CHECK(library.create_stream_reader == nullptr);
  CHECK(library.create_writer != nullptr && library.write != nullptr);

  for (const auto& [capabilities, expected] :
       std::vector<std::pair<std::string, uint64_t>>{{"", SPARK_DS_BATCH_WRITE | SPARK_DS_TRUNCATE},
                                                     {"append", SPARK_DS_BATCH_WRITE},
                                                     {"none", 0}}) {
    uint64_t mask = 0;
    std::unique_ptr<Handle> source = CreateSource("", capabilities, &mask);
    CHECK_EQ(mask, expected);
    for (int32_t mode : {SPARK_DS_WRITE_APPEND, SPARK_DS_WRITE_TRUNCATE}) {
      void* writer = nullptr;
      int code = CreateDriver(*source, mode, nullptr, &writer);
      bool supported = (expected & SPARK_DS_BATCH_WRITE) != 0 &&
                       (mode == SPARK_DS_WRITE_APPEND || (expected & SPARK_DS_TRUNCATE) != 0);
      if (supported) {
        CHECK_SUCCESS(library, code);
        library.close_writer(writer);
      } else {
        CHECK(code != 0);
        CHECK(writer == nullptr);
        CHECK_CONTAINS(LastError(library), "support");
      }
    }
  }
  CHECK(Events() == std::vector<std::string>(
                        {"WriteCreated(false)", "WriteCreated(true)", "WriteCreated(false)"}));

  std::unique_ptr<Handle> source = CreateSource("", "");
  void* writer = nullptr;
  CHECK(CreateDriver(*source, 99, nullptr, &writer) != 0);
  CHECK(writer == nullptr);
  CHECK(CreateDriver(*source, SPARK_DS_WRITE_APPEND, "streaming-query", &writer) != 0);
  CHECK_CONTAINS(LastError(library), "does not support streaming writes");
  CHECK(writer == nullptr);
}

TEST(SerializedFactoryAndMessagesRoundTripWithoutTheDriverSourceOrInputBuffers) {
  ClearEvents();
  std::unique_ptr<Handle> source = CreateSource("", "");
  std::unique_ptr<Handle> driver = OpenDriver(*source, SPARK_DS_WRITE_TRUNCATE);
  source.reset();
  std::string state = State(*driver);
  CHECK(state == State(*driver));

  std::unique_ptr<Task> first = OpenTask(state, 7);
  std::shared_ptr<arrow::RecordBatch> input = Batch({2, 5, 9});
  std::shared_ptr<arrow::Buffer> buffer = input->column(0)->data()->buffers[1];
  CHECK_SUCCESS(Library(), Write(*first, input));
  input.reset();
  // The writer owns the retained Arrow buffers.
  CHECK(buffer.use_count() > 1);
  CHECK_SUCCESS(Library(), Write(*first, Batch({})));
  std::string first_message = TaskMessage(*first);
  CHECK(first_message == TaskMessage(*first));
  first.reset();
  // Closing the writer releases the retained Arrow buffers.
  CHECK_EQ(buffer.use_count(), long{1});

  std::unique_ptr<Task> second = OpenTask(state, 8);
  std::string second_message = TaskMessage(*second);
  second.reset();
  CHECK_SUCCESS(Library(), Finish(*driver, false, {first_message, second_message}));
  CHECK_SUCCESS(Library(), Finish(*driver, false, {first_message, second_message}));
  CHECK(Events() == std::vector<std::string>({
                        "WriteCreated(true)",
                        "FactoryCreated",
                        "Batch(3)",
                        "Batch(0)",
                        "TaskCommit",
                        "TaskDestroyed(false)",
                        "TaskCommit",
                        "TaskDestroyed(false)",
                        "DriverCommit(" +
                            Describe(std::vector<Message>{Expected(7, {2, 5, 9}, 2, ""),
                                                          Expected(8, {}, 0, "")}) +
                            ")",
                    }));
}

TEST(TheInputSchemaAndLiveTaskCancellationAreChecked) {
  for (bool cancelled : {false, true}) {
    ClearEvents();
    std::unique_ptr<Handle> source = CreateSource("", "");
    std::unique_ptr<Handle> driver = OpenDriver(*source, SPARK_DS_WRITE_APPEND);
    std::unique_ptr<Task> task = OpenTask(State(*driver), 0);
    if (cancelled) {
      CHECK_SUCCESS(Library(), Write(*task, Batch({1})));
      task->host->cancelled = true;
      std::vector<std::string> messages;
      CHECK(TaskCommit(*task, &messages) != 0);
      CHECK(messages.empty());
      CHECK_CONTAINS(LastError(Library()), "cancelled");
    } else {
      auto wrong_schema = arrow::schema({arrow::field("value", arrow::int32(), false)});
      arrow::Int32Builder builder;
      CHECK_OK(builder.Append(1));
      auto input = arrow::RecordBatch::Make(wrong_schema, 1, {builder.Finish().ValueOrDie()});
      CHECK(Write(*task, input) != 0);
      CHECK_CONTAINS(LastError(Library()), "schema");
      CHECK_EQ(Count("Batch(1)"), size_t{0});
    }
    CHECK_SUCCESS(Library(), Library().abort_data_writer(task->writer->get()));
    task.reset();
    CHECK_EQ(Count("TaskAbort(" + Flag(cancelled) + ")"), size_t{1});
    CHECK_EQ(Count("TaskDestroyed(" + Flag(cancelled) + ")"), size_t{1});
    CHECK_SUCCESS(Library(), Finish(*driver, true, {std::nullopt}));
  }
}

TEST(TaskFailuresAndMessageSerializationFailuresRemainAbortable) {
  for (const std::string failure : {"write_error", "write_throw", "task_commit_error",
                                    "task_commit_throw", "message_serialization"}) {
    ClearEvents();
    std::unique_ptr<Handle> source = CreateSource(failure, "");
    std::unique_ptr<Handle> driver = OpenDriver(*source, SPARK_DS_WRITE_APPEND);
    std::unique_ptr<Task> task = OpenTask(State(*driver), 0);
    bool write_failed = failure.rfind("write_", 0) == 0;
    CHECK_EQ(Write(*task, Batch({4})) != 0, write_failed);
    std::vector<std::string> messages;
    CHECK(TaskCommit(*task, &messages) != 0);
    CHECK(messages.empty());
    std::string message = LastError(Library());
    CHECK(message.find("failed") != std::string::npos ||
          message.find("threw") != std::string::npos);
    std::vector<std::string> before_retry = Events();
    CHECK(TaskCommit(*task, &messages) != 0);
    CHECK(messages.empty());
    // A failed commit cannot run the commit of the provider twice.
    CHECK(Events() == before_retry);
    // A commit whose message cannot be serialized counts as failed, so abort follows a provider
    // commit that returned a message.
    CHECK_SUCCESS(Library(), Library().abort_data_writer(task->writer->get()));
    CHECK_SUCCESS(Library(), Library().abort_data_writer(task->writer->get()));
    task.reset();
    CHECK_EQ(Count("TaskAbort(false)"), size_t{1});
    CHECK_EQ(Count("TaskCommit"), write_failed ? size_t{0} : size_t{1});
    CHECK_SUCCESS(Library(), Finish(*driver, true, {std::nullopt}));
  }
}

TEST(DriverFailuresPreserveMessagesAndMissingTaskPositionsForAbort) {
  for (const std::string failure : {"driver_commit_error", "driver_commit_throw"}) {
    ClearEvents();
    std::unique_ptr<Handle> source = CreateSource(failure, "");
    std::unique_ptr<Handle> driver = OpenDriver(*source, SPARK_DS_WRITE_APPEND);
    std::unique_ptr<Task> task = OpenTask(State(*driver), 1);
    CHECK_SUCCESS(Library(), Write(*task, Batch({42})));
    std::string message = TaskMessage(*task);
    task.reset();
    CHECK(Finish(*driver, false, {message}) != 0);
    CHECK_CONTAINS(LastError(Library()), "driver commit");
    std::vector<std::string> before_retry = Events();
    CHECK(Finish(*driver, false, {message}) != 0);
    CHECK(Events() == before_retry);
    std::vector<std::optional<std::string>> aborted = {std::nullopt, message, std::nullopt};
    CHECK_SUCCESS(Library(), Finish(*driver, true, aborted));
    CHECK_SUCCESS(Library(), Finish(*driver, true, aborted));
    Message expected = Expected(1, {42}, 1, failure);
    CHECK_EQ(Count("DriverCommit(" + Describe(std::vector<Message>{expected}) + ")"), size_t{1});
    CHECK_EQ(Count("DriverAbort(" + Describe({std::nullopt, expected, std::nullopt}) + ")"),
             size_t{1});
    size_t aborts = 0;
    for (const std::string& event : Events()) {
      aborts += event.rfind("DriverAbort", 0) == 0 ? 1 : 0;
    }
    CHECK_EQ(aborts, size_t{1});
  }
}

TEST(WriterFactoryFailuresAreCachedAndTheDriverCanAbort) {
  for (const auto& [failure, expected] : std::vector<std::pair<std::string, std::string>>{
           {"writer_factory_error", "creating the writer factory failed"},
           {"factory_serialization", "factory serialization failed"}}) {
    ClearEvents();
    std::unique_ptr<Handle> source = CreateSource(failure, "");
    std::unique_ptr<Handle> driver = OpenDriver(*source, SPARK_DS_WRITE_APPEND);
    for (int attempt = 0; attempt < 2; ++attempt) {
      std::vector<std::string> output;
      CHECK(Serialize(*driver, &output) != 0);
      CHECK(output.empty());
      CHECK_CONTAINS(LastError(Library()), expected);
    }
    // Commit never follows a failed call.
    CHECK(Finish(*driver, false, {}) != 0);
    CHECK_SUCCESS(Library(), Finish(*driver, true, {}));
    CHECK(Events() ==
          std::vector<std::string>({"WriteCreated(false)", "FactoryCreated", "DriverAbort([])"}));
  }
}

TEST(BatchWritesRejectTheEpochsOfMicroBatches) {
  ClearEvents();
  std::unique_ptr<Handle> source = CreateSource("", "");
  std::unique_ptr<Handle> driver = OpenDriver(*source, SPARK_DS_WRITE_APPEND);
  std::string state = State(*driver);
  HostTask host(0, 100);
  void* writer = nullptr;
  CHECK(Library().create_data_writer(&host.context, reinterpret_cast<const uint8_t*>(state.data()),
                                     static_cast<int64_t>(state.size()), 0, &writer) != 0);
  CHECK_CONTAINS(LastError(Library()), "epoch ID -1");
  CHECK(writer == nullptr);
  CHECK(Finish(*driver, false, {}, /*epoch_id=*/3) != 0);
  CHECK_CONTAINS(LastError(Library()), "epoch ID -1");
  // The rejected call did not change the write, which can still abort.
  CHECK_SUCCESS(Library(), Finish(*driver, true, {}));
  CHECK(Events() ==
        std::vector<std::string>({"WriteCreated(false)", "FactoryCreated", "DriverAbort([])"}));
}

TEST(UndecodableMessagesFailCommitButAbortStillCleansUp) {
  ClearEvents();
  std::unique_ptr<Handle> source = CreateSource("", "");
  std::unique_ptr<Handle> driver = OpenDriver(*source, SPARK_DS_WRITE_APPEND);
  std::unique_ptr<Task> task = OpenTask(State(*driver), 2);
  CHECK_SUCCESS(Library(), Write(*task, Batch({6})));
  std::string message = TaskMessage(*task);
  task.reset();

  std::string garbage = "not a commit message";
  CHECK(Finish(*driver, false, {garbage}) != 0);
  for (const std::string& event : Events()) {
    CHECK(event.rfind("DriverCommit", 0) != 0);
  }
  CHECK_SUCCESS(Library(), Finish(*driver, true, {garbage, message, std::nullopt}));
  Message expected = Expected(2, {6}, 1, "");
  CHECK_EQ(Count("DriverAbort(" + Describe({std::nullopt, expected, std::nullopt}) + ")"),
           size_t{1});
}
