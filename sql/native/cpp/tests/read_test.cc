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

// The driver planners of batch scans and the executor reads of their partitions, through the
// function table that the library exports.

#include <atomic>
#include <stdexcept>

#include <arrow/api.h>
#include <arrow/c/bridge.h>

#include "spark/native/data_source.h"
#include "test_util.h"

namespace sn = spark::native;
using spark::native::Capabilities;
using spark::native::LimitSupport;
using spark::native::testing::CreateArguments;
using spark::native::testing::LastError;
using spark::native::testing::Plan;

namespace {

std::atomic<int> builds{0};
std::atomic<int> limits{0};
std::atomic<int> sources_destroyed{0};
std::atomic<int> builders_destroyed{0};

void ResetCounters() {
  builds = 0;
  limits = 0;
  sources_destroyed = 0;
  builders_destroyed = 0;
}

arrow::Status Fail(const std::string& failure, const std::string& message) {
  if (failure == "error") {
    return arrow::Status::Invalid(message);
  }
  if (failure == "throw") {
    throw std::runtime_error(message);
  }
  return arrow::Status::OK();
}

std::shared_ptr<arrow::Schema> IdSchema() {
  return arrow::schema({arrow::field("id", arrow::int64(), false)});
}

/// Reads one row, `value + partition`, ignoring the schema to read of its context, to test that the
/// SDK checks it.
struct Factory : sn::PartitionReaderFactory<int64_t> {
  int64_t value = 0;

  arrow::Result<std::shared_ptr<arrow::RecordBatchReader>> Open(
      const int64_t& partition, const sn::ReaderContext&) const override {
    arrow::Int64Builder builder;
    ARROW_RETURN_NOT_OK(builder.Append(value + partition));
    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Array> ids, builder.Finish());
    return arrow::RecordBatchReader::Make({arrow::RecordBatch::Make(IdSchema(), 1, {ids})},
                                          IdSchema());
  }
};
NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE(Factory, value)

class Builder : public sn::ScanBuilder<Factory> {
 public:
  Builder(LimitSupport limit, std::string failure) : limit_(limit), failure_(std::move(failure)) {}
  Builder(Builder&& other) noexcept
      : ScanBuilder(std::move(other)),
        limit_(other.limit_),
        failure_(std::move(other.failure_)),
        value_(other.value_),
        live_(std::exchange(other.live_, false)) {}
  ~Builder() override {
    if (live_) {
      ++builders_destroyed;
    }
  }

  arrow::Result<LimitSupport> PushLimit(int64_t limit) override {
    ++limits;
    value_ = limit;
    ARROW_RETURN_NOT_OK(Fail(failure_, "negotiation failed"));
    return limit_;
  }

  arrow::Result<sn::ScanPlan<Factory>> Plan() override {
    ++builds;
    ARROW_RETURN_NOT_OK(Fail(failure_, "planning failed"));
    Factory factory;
    factory.value = value_;
    return sn::ScanPlan<Factory>(factory, {2, 7});
  }

 private:
  LimitSupport limit_;
  std::string failure_;
  int64_t value_ = 42;
  bool live_ = true;
};

class Source : public sn::DataSource {
 public:
  using ScanBuilder = Builder;

  explicit Source(sn::Options options) : options_(std::move(options)) {}
  Source(Source&& other) noexcept
      : DataSource(std::move(other)),
        options_(std::move(other.options_)),
        live_(std::exchange(other.live_, false)) {}
  ~Source() override {
    if (live_) {
      ++sources_destroyed;
    }
  }

  static arrow::Result<Source> Make(const sn::CreateInfo& info) { return Source(info.options); }

  arrow::Result<std::shared_ptr<arrow::Schema>> InferSchema() const override { return IdSchema(); }

  Capabilities capabilities() const override {
    std::string mode = Option("capabilities");
    if (mode == "empty") return Capabilities::Empty();
    if (mode == "without_read") return Capabilities::All().Without(Capabilities::kBatchRead);
    return Capabilities::All();
  }

  arrow::Result<Builder> NewScan(const sn::ScanInfo&) const {
    LimitSupport limit =
        Option("limit") == "global" ? LimitSupport::kGlobal : LimitSupport::kUnsupported;
    return Builder(limit, Option("failure"));
  }

 private:
  std::string Option(const std::string& key) const {
    auto value = options_.find(key);
    return value == options_.end() ? "" : value->second;
  }

  sn::Options options_;
  bool live_ = true;
};

const SparkDsLibrary& Library() {
  static const SparkDsLibrary& library = sn::testing::InitializeLibrary();
  return library;
}

void* CreateSource(std::vector<std::pair<std::string, std::string>> options,
                   uint64_t* mask = nullptr) {
  const SparkDsLibrary& library = Library();
  CreateArguments arguments("read_test", std::move(options));
  uint64_t capabilities = 0;
  void* source = nullptr;
  CHECK_SUCCESS(library, library.create(arguments.info(), &capabilities, &source));
  if (mask != nullptr) {
    *mask = capabilities;
  }
  return source;
}

/// Creates a reader of the schema of the source.
void* CreateReader(void* source) {
  const SparkDsLibrary& library = Library();
  struct ArrowSchema exported{};
  CHECK_SUCCESS(library, library.schema(source, &exported));
  SparkDsReaderInfo info{sizeof(SparkDsReaderInfo)};
  void* reader = nullptr;
  int code = library.create_reader(source, &exported, &info, &reader);
  if (exported.release != nullptr) {
    exported.release(&exported);
  }
  CHECK_SUCCESS(library, code);
  CHECK(reader != nullptr);
  return reader;
}

arrow::Result<bool> PushLimit(void* reader, int32_t limit) {
  const SparkDsLibrary& library = Library();
  bool applied = false;
  if (library.push_limit(reader, limit, &applied) != 0) {
    return arrow::Status::Invalid(LastError(library));
  }
  return applied;
}

/// Plans a scan, whose partitions and state partitions returns in a single call.
arrow::Result<Plan> Planned(void* reader) {
  const SparkDsLibrary& library = Library();
  SparkDsPartitionsInfo info{sizeof(SparkDsPartitionsInfo)};
  Plan plan;
  if (library.partitions(reader, &info, &Plan::AddPartition, &Plan::SetState, &plan) != 0) {
    CHECK(plan.partitions.empty() && plan.states.empty());
    return arrow::Status::Invalid(LastError(library));
  }
  CHECK(plan.states.size() == 1);
  return plan;
}

/// The value of the factory in the serialized state of a planned scan.
int64_t FactoryValue(const Plan& plan) {
  ASSIGN_OR_FAIL(Factory factory, sn::internal::Decode<Factory>(plan.states[0]));
  return factory.value;
}

/// Reads a partition of a plan with `schema` as the schema to read, as Spark does.
arrow::Result<std::shared_ptr<arrow::RecordBatchReader>> Read(
    const Plan& plan, size_t index, const std::shared_ptr<arrow::Schema>& schema,
    const sn::testing::HostTask& task, int32_t kind = SPARK_DS_READ_BATCH) {
  const SparkDsLibrary& library = Library();
  struct ArrowSchema exported{};
  CHECK_OK(arrow::ExportSchema(*schema, &exported));
  const std::string& state = plan.states[0];
  const std::string& partition = plan.partitions[index];
  SparkDsReadInfo info{sizeof(SparkDsReadInfo),
                       kind,
                       &exported,
                       reinterpret_cast<const uint8_t*>(state.data()),
                       static_cast<int64_t>(state.size()),
                       reinterpret_cast<const uint8_t*>(partition.data()),
                       static_cast<int64_t>(partition.size())};
  struct ArrowArrayStream stream{};
  int code = library.read(&task.context, &info, &stream);
  // The library may move the schema; otherwise Spark releases it after the call.
  if (exported.release != nullptr) {
    exported.release(&exported);
  }
  if (code != 0) {
    CHECK(stream.release == nullptr);
    return arrow::Status::Invalid(LastError(library));
  }
  return arrow::ImportRecordBatchReader(&stream);
}

}  // namespace

SPARK_EXPORT_DATA_SOURCE(Source);

TEST(SourceCanCloseBeforePlanningAndAScanIsPlannedOnce) {
  ResetCounters();
  const SparkDsLibrary& library = Library();
  void* source = CreateSource({});
  void* reader = CreateReader(source);
  library.close(source);
  CHECK_EQ(sources_destroyed.load(), 1);

  ASSIGN_OR_FAIL(Plan plan, Planned(reader));
  CHECK(plan.partitions.size() == 2);
  ASSIGN_OR_FAIL(int64_t first, sn::internal::Decode<int64_t>(plan.partitions[0]));
  ASSIGN_OR_FAIL(int64_t second, sn::internal::Decode<int64_t>(plan.partitions[1]));
  CHECK_EQ(first, int64_t{2});
  CHECK_EQ(second, int64_t{7});
  CHECK_EQ(FactoryValue(plan), int64_t{42});
  CHECK_EQ(builds.load(), 1);
  // Planning consumed the builder: no other call reaches it.
  CHECK_EQ(builders_destroyed.load(), 1);
  CHECK_CONTAINS(Planned(reader).status().message(), "already been planned");
  CHECK(library.prune_columns(reader, nullptr, 0) != 0);
  CHECK_CONTAINS(LastError(library), "already been planned");
  library.close_reader(reader);
}

TEST(OnlyGlobalLimitsReturnTrueToTheAbi) {
  for (const std::string& limit : {"unsupported", "global"}) {
    ResetCounters();
    void* source = CreateSource({{"limit", limit}});
    void* reader = CreateReader(source);
    Library().close(source);
    ASSIGN_OR_FAIL(bool applied, PushLimit(reader, 4));
    CHECK_EQ(applied, limit == "global");
    CHECK_EQ(limits.load(), 1);
    CHECK(!PushLimit(reader, -1).ok());
    Library().close_reader(reader);
  }
}

TEST(NegotiationAndPlanningErrorsAndExceptionsAreReported) {
  for (const auto& [failure, prefix] : std::vector<std::pair<std::string, std::string>>{
           {"error", ""}, {"throw", "native provider threw an exception: "}}) {
    ResetCounters();
    void* source = CreateSource({{"failure", failure}});
    void* negotiated = CreateReader(source);
    void* planned = CreateReader(source);
    Library().close(source);
    CHECK_EQ(PushLimit(negotiated, 5).status().message(), prefix + "negotiation failed");
    CHECK_EQ(Planned(planned).status().message(), prefix + "planning failed");
    CHECK_EQ(limits.load(), 1);
    CHECK_EQ(builds.load(), 1);
    // The failed plan consumed its builder; Spark only closes the other scan.
    CHECK_EQ(builders_destroyed.load(), 1);
    Library().close_reader(negotiated);
    Library().close_reader(planned);
    CHECK_EQ(builders_destroyed.load(), 2);
  }
}

TEST(ScansOfOneSourceKeepPushdownAndPlansIndependent) {
  ResetCounters();
  void* source = CreateSource({{"limit", "global"}});
  void* first = CreateReader(source);
  void* second = CreateReader(source);
  Library().close(source);

  ASSIGN_OR_FAIL(bool first_limited, PushLimit(first, 5));
  CHECK(first_limited);
  ASSIGN_OR_FAIL(Plan first_plan, Planned(first));
  ASSIGN_OR_FAIL(Plan second_plan, Planned(second));
  CHECK_EQ(FactoryValue(first_plan), int64_t{5});
  CHECK_EQ(FactoryValue(second_plan), int64_t{42});
  CHECK_EQ(builds.load(), 2);
  CHECK_EQ(limits.load(), 1);
  Library().close_reader(first);
  Library().close_reader(second);
}

TEST(ExecutorRetriesUseOnlySerializedValuesAfterTheDriverIsClosed) {
  ResetCounters();
  const SparkDsLibrary& library = Library();
  void* source = CreateSource({});
  void* reader = CreateReader(source);
  library.close(source);
  // Spark copies the factory and the partitions out of the library before closing the planner.
  ASSIGN_OR_FAIL(Plan plan, Planned(reader));
  library.close_reader(reader);
  CHECK_EQ(builds.load(), 1);
  CHECK_EQ(sources_destroyed.load(), 1);
  CHECK_EQ(builders_destroyed.load(), 1);

  for (int32_t attempt = 0; attempt < 2; ++attempt) {
    for (size_t index = 0; index < plan.partitions.size(); ++index) {
      sn::testing::HostTask task(static_cast<int32_t>(index), attempt * 2 + int64_t(index));
      ASSIGN_OR_FAIL(auto batches, Read(plan, index, IdSchema(), task));
      ASSIGN_OR_FAIL(auto batch, batches->Next());
      CHECK(batch != nullptr);
      int64_t expected = 42 + (index == 0 ? 2 : 7);
      CHECK_EQ(std::static_pointer_cast<arrow::Int64Array>(batch->column(0))->Value(0), expected);
      ASSIGN_OR_FAIL(auto end, batches->Next());
      CHECK(end == nullptr);
    }
  }
}

TEST(ExecutorRejectsAReaderWithoutTheSchemaToRead) {
  const SparkDsLibrary& library = Library();
  void* source = CreateSource({});
  void* reader = CreateReader(source);
  ASSIGN_OR_FAIL(Plan plan, Planned(reader));
  library.close_reader(reader);
  library.close(source);
  // The factory returns a bigint id column, but Spark reads a string id or a label.
  sn::testing::HostTask task(0, 1);
  auto string_id = arrow::schema({arrow::field("id", arrow::utf8(), false)});
  CHECK_CONTAINS(Read(plan, 0, string_id, task).status().message(),
                 "does not match the schema to read");
  auto label = arrow::schema({arrow::field("label", arrow::int64(), false)});
  CHECK_CONTAINS(Read(plan, 0, label, task).status().message(), "does not return the column label");
}

TEST(TheKindOfTheReadSelectsTheFactory) {
  const SparkDsLibrary& library = Library();
  CHECK(library.create_stream_reader == nullptr);
  void* source = CreateSource({});
  void* reader = CreateReader(source);
  ASSIGN_OR_FAIL(Plan plan, Planned(reader));
  library.close_reader(reader);
  library.close(source);
  sn::testing::HostTask task(0, 1);
  CHECK_CONTAINS(Read(plan, 0, IdSchema(), task, SPARK_DS_READ_MICRO_BATCH).status().message(),
                 "does not support micro-batch streaming reads");
  CHECK_CONTAINS(Read(plan, 0, IdSchema(), task, 7).status().message(),
                 "unsupported kind of read: 7");
}

TEST(CapabilitiesKeepOnlyImplementedOperations) {
  for (const auto& [mode, expected] : std::vector<std::pair<std::string, uint64_t>>{
           {"all", SPARK_DS_BATCH_READ}, {"empty", 0}, {"without_read", 0}}) {
    uint64_t mask = 99;
    void* source = CreateSource({{"capabilities", mode}}, &mask);
    CHECK_EQ(mask, expected);
    Library().close(source);
  }
}
