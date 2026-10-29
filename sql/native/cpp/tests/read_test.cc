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
using spark::native::FilterSupport;
using spark::native::LimitSupport;
using spark::native::testing::Collect;
using spark::native::testing::CreateArguments;
using spark::native::testing::LastError;

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

/// Reads one row, `value + partition`, ignoring the negotiated schema of its context, to test that
/// the SDK checks it.
struct Factory : sn::PartitionReaderFactory<int64_t> {
  int64_t value = 0;

  arrow::Result<std::shared_ptr<arrow::RecordBatchReader>> Open(
      const int64_t& partition, const sn::ReaderContext&) const override {
    auto schema = arrow::schema({arrow::field("id", arrow::int64(), false)});
    arrow::Int64Builder builder;
    ARROW_RETURN_NOT_OK(builder.Append(value + partition));
    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Array> ids, builder.Finish());
    return arrow::RecordBatchReader::Make({arrow::RecordBatch::Make(schema, 1, {ids})}, schema);
  }
};
NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE(Factory, value)

class Builder : public sn::ScanBuilder<Factory> {
 public:
  Builder(FilterSupport filters, LimitSupport limit, std::string failure)
      : filters_(filters), limit_(limit), failure_(std::move(failure)) {}
  Builder(Builder&& other) noexcept
      : ScanBuilder(std::move(other)),
        filters_(other.filters_),
        limit_(other.limit_),
        failure_(std::move(other.failure_)),
        value_(other.value_),
        live_(std::exchange(other.live_, false)) {}
  ~Builder() override {
    if (live_) {
      ++builders_destroyed;
    }
  }

  arrow::Result<std::vector<FilterSupport>> PushFilters(
      const std::vector<sn::Predicate>& filters) override {
    ARROW_RETURN_NOT_OK(Fail(failure_, "negotiation failed"));
    if (filters_ == FilterSupport::kExact && failure_ == "wrong_count") {
      return std::vector<FilterSupport>();
    }
    return std::vector<FilterSupport>(filters.size(), filters_);
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
  FilterSupport filters_;
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

  arrow::Result<std::shared_ptr<arrow::Schema>> InferSchema() const override {
    return arrow::schema(arrow::FieldVector{});
  }

  Capabilities capabilities() const override {
    std::string mode = Option("capabilities");
    if (mode == "empty") return Capabilities::Empty();
    if (mode == "without_read") return Capabilities::All().Without(Capabilities::kBatchRead);
    return Capabilities::All();
  }

  arrow::Result<Builder> NewScan(const sn::ScanInfo&) const {
    std::string filters = Option("filters");
    FilterSupport support = filters == "exact"     ? FilterSupport::kExact
                            : filters == "pruning" ? FilterSupport::kPruningOnly
                                                   : FilterSupport::kUnsupported;
    LimitSupport limit =
        Option("limit") == "global" ? LimitSupport::kGlobal : LimitSupport::kUnsupported;
    return Builder(support, limit, Option("failure"));
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

/// Creates a reader of `schema`, or of the schema of the source.
void* CreateReader(void* source, std::shared_ptr<arrow::Schema> schema = nullptr) {
  const SparkDsLibrary& library = Library();
  struct ArrowSchema exported{};
  if (schema == nullptr) {
    CHECK_SUCCESS(library, library.schema(source, &exported));
  } else {
    CHECK_OK(arrow::ExportSchema(*schema, &exported));
  }
  void* reader = nullptr;
  int code = library.create_reader(source, &exported, &reader);
  if (exported.release != nullptr) {
    exported.release(&exported);
  }
  CHECK_SUCCESS(library, code);
  CHECK(reader != nullptr);
  return reader;
}

/// Pushes predicates, and returns how the reader uses them, or the error of the call.
arrow::Result<std::vector<int32_t>> PushPredicates(void* reader,
                                                   const std::vector<std::string>& predicates) {
  const SparkDsLibrary& library = Library();
  std::vector<const char*> pointers;
  for (const std::string& predicate : predicates) {
    pointers.push_back(predicate.c_str());
  }
  std::vector<int32_t> uses(predicates.size(), SPARK_DS_PREDICATE_UNUSED);
  if (library.push_predicates(reader, pointers.data(), static_cast<int64_t>(pointers.size()),
                              uses.data()) != 0) {
    return arrow::Status::Invalid(LastError(library));
  }
  return uses;
}

arrow::Result<bool> PushLimit(void* reader, int32_t limit) {
  const SparkDsLibrary& library = Library();
  bool applied = false;
  if (library.push_limit(reader, limit, &applied) != 0) {
    return arrow::Status::Invalid(LastError(library));
  }
  return applied;
}

struct Plan {
  std::string state;
  std::vector<std::string> partitions;
};

arrow::Result<Plan> Planned(void* reader) {
  const SparkDsLibrary& library = Library();
  Plan plan;
  if (library.partitions(reader, &Collect, &plan.partitions) != 0) {
    return arrow::Status::Invalid(LastError(library));
  }
  std::vector<std::string> state;
  if (library.serialize_reader(reader, &Collect, &state) != 0) {
    return arrow::Status::Invalid(LastError(library));
  }
  CHECK(state.size() == 1);
  plan.state = state[0];
  return plan;
}

/// The value of the factory in the serialized state of a planned scan.
int64_t FactoryValue(const std::string& state) {
  ASSIGN_OR_FAIL(sn::internal::State decoded, sn::internal::DecodeState(state));
  CHECK(!decoded.streaming);
  return decoded.factory.get<Factory>().value;
}

const std::string kUnknownPredicate = R"({"type":"future_expression"})";

}  // namespace

SPARK_EXPORT_DATA_SOURCE(Source);

TEST(SourceCanCloseBeforePlanningAndThePlanIsBuiltOnce) {
  ResetCounters();
  const SparkDsLibrary& library = Library();
  void* source = CreateSource({});
  void* reader = CreateReader(source);
  library.close(source);
  CHECK_EQ(sources_destroyed.load(), 1);

  ASSIGN_OR_FAIL(Plan plan, Planned(reader));
  ASSIGN_OR_FAIL(Plan again, Planned(reader));
  CHECK(plan.state == again.state && plan.partitions == again.partitions);
  CHECK(plan.partitions.size() == 2);
  ASSIGN_OR_FAIL(int64_t first, sn::internal::Decode<int64_t>(plan.partitions[0]));
  ASSIGN_OR_FAIL(int64_t second, sn::internal::Decode<int64_t>(plan.partitions[1]));
  CHECK_EQ(first, int64_t{2});
  CHECK_EQ(second, int64_t{7});
  CHECK_EQ(FactoryValue(plan.state), int64_t{42});
  CHECK_EQ(builds.load(), 1);
  CHECK_EQ(builders_destroyed.load(), 1);
  bool pruned = false;
  CHECK(library.prune_columns(reader, nullptr, 0, &pruned) != 0);
  CHECK_CONTAINS(LastError(library), "already been planned");
  library.close_reader(reader);
}

TEST(ResidualFiltersPreventCallingTheLimitOfTheProvider) {
  for (const auto& [filters, use] : std::vector<std::pair<std::string, int32_t>>{
           {"pruning", SPARK_DS_PREDICATE_PARTIAL}, {"unsupported", SPARK_DS_PREDICATE_UNUSED}}) {
    ResetCounters();
    void* source = CreateSource({{"filters", filters}, {"limit", "global"}});
    void* reader = CreateReader(source);
    Library().close(source);
    ASSIGN_OR_FAIL(std::vector<int32_t> uses, PushPredicates(reader, {kUnknownPredicate}));
    CHECK(uses == std::vector<int32_t>({use}));
    ASSIGN_OR_FAIL(bool applied, PushLimit(reader, 1));
    CHECK(!applied);
    CHECK_EQ(limits.load(), 0);
    Library().close_reader(reader);
  }
}

TEST(PredicatesThatDoNotParseStayUnsupportedAndResidual) {
  ResetCounters();
  void* source = CreateSource({{"filters", "exact"}, {"limit", "global"}});
  void* reader = CreateReader(source);
  Library().close(source);
  // A column name must be an array of parts.
  std::string malformed =
      R"({"type":"function","name":"IS_NULL","children":[{"type":"column","name":"id"}]})";
  std::string valid =
      R"({"type":"function","name":"IS_NULL","children":[{"type":"column","name":["id"]}]})";
  // The connector reports every predicate as exact, including the kUnknown placeholder.
  ASSIGN_OR_FAIL(std::vector<int32_t> uses, PushPredicates(reader, {malformed, valid}));
  CHECK(uses == std::vector<int32_t>({SPARK_DS_PREDICATE_UNUSED, SPARK_DS_PREDICATE_COMPLETE}));
  ASSIGN_OR_FAIL(bool applied, PushLimit(reader, 1));
  CHECK(!applied);
  CHECK_EQ(limits.load(), 0);
  Library().close_reader(reader);
}

TEST(PushFiltersMustReturnOneResultPerPredicate) {
  ResetCounters();
  void* source = CreateSource({{"filters", "exact"}, {"failure", "wrong_count"}});
  void* reader = CreateReader(source);
  Library().close(source);
  arrow::Result<std::vector<int32_t>> uses = PushPredicates(reader, {kUnknownPredicate});
  CHECK(!uses.ok());
  CHECK_CONTAINS(uses.status().message(), "one result per predicate");
  Library().close_reader(reader);
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

TEST(PlanningErrorsAndExceptionsPreserveTheFirstFailureWithoutRetrying) {
  for (const auto& [failure, expected] : std::vector<std::pair<std::string, std::string>>{
           {"error", "planning failed"},
           {"throw", "native provider threw an exception: planning failed"}}) {
    ResetCounters();
    void* source = CreateSource({{"failure", failure}});
    void* reader = CreateReader(source);
    Library().close(source);
    arrow::Result<Plan> first = Planned(reader);
    arrow::Result<Plan> second = Planned(reader);
    CHECK(!first.ok() && !second.ok());
    CHECK_EQ(first.status().message(), expected);
    CHECK_EQ(second.status().message(), expected);
    CHECK_EQ(PushLimit(reader, 1).status().message(), expected);
    CHECK_EQ(builds.load(), 1);
    CHECK_EQ(builders_destroyed.load(), 1);
    Library().close_reader(reader);
    CHECK_EQ(builders_destroyed.load(), 1);
  }
}

TEST(FailedNegotiationDiscardsThePartiallyMutatedBuilder) {
  for (const auto& [failure, expected] : std::vector<std::pair<std::string, std::string>>{
           {"error", "negotiation failed"},
           {"throw", "native provider threw an exception: negotiation failed"}}) {
    ResetCounters();
    void* source = CreateSource({{"failure", failure}});
    void* reader = CreateReader(source);
    CHECK_EQ(PushLimit(reader, 5).status().message(), expected);
    CHECK_EQ(PushLimit(reader, 1).status().message(), expected);
    CHECK_EQ(PushPredicates(reader, {}).status().message(), expected);
    CHECK_EQ(Planned(reader).status().message(), expected);
    CHECK_EQ(limits.load(), 1);
    CHECK_EQ(builds.load(), 0);
    CHECK_EQ(builders_destroyed.load(), 1);
    Library().close_reader(reader);
    Library().close(source);
  }
}

TEST(ScansOfOneSourceKeepPushdownAndCachedPlansIndependent) {
  ResetCounters();
  void* source = CreateSource({{"filters", "pruning"}, {"limit", "global"}});
  void* first = CreateReader(source);
  void* second = CreateReader(source);
  Library().close(source);

  CHECK_OK(PushPredicates(second, {kUnknownPredicate}));
  ASSIGN_OR_FAIL(bool first_limited, PushLimit(first, 5));
  ASSIGN_OR_FAIL(bool second_limited, PushLimit(second, 1));
  CHECK(first_limited);
  CHECK(!second_limited);
  ASSIGN_OR_FAIL(Plan first_plan, Planned(first));
  ASSIGN_OR_FAIL(Plan second_plan, Planned(second));
  CHECK_EQ(FactoryValue(first_plan.state), int64_t{5});
  CHECK_EQ(FactoryValue(second_plan.state), int64_t{42});
  CHECK_EQ(builds.load(), 2);
  CHECK_EQ(limits.load(), 1);
  Library().close_reader(first);
  ASSIGN_OR_FAIL(Plan again, Planned(second));
  CHECK(again.state == second_plan.state);
  Library().close_reader(second);
}

TEST(ExecutorRetriesUseOnlySerializedValuesAfterTheDriverIsClosed) {
  ResetCounters();
  const SparkDsLibrary& library = Library();
  auto schema = arrow::schema({arrow::field("id", arrow::int64(), false)});
  void* source = CreateSource({});
  void* reader = CreateReader(source, schema);
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
      const std::string& partition = plan.partitions[index];
      struct ArrowArrayStream stream{};
      CHECK_SUCCESS(library,
                    library.read(&task.context, reinterpret_cast<const uint8_t*>(plan.state.data()),
                                 static_cast<int64_t>(plan.state.size()),
                                 reinterpret_cast<const uint8_t*>(partition.data()),
                                 static_cast<int64_t>(partition.size()), &stream));
      ASSIGN_OR_FAIL(auto batches, arrow::ImportRecordBatchReader(&stream));
      ASSIGN_OR_FAIL(auto batch, batches->Next());
      CHECK(batch != nullptr);
      int64_t expected = 42 + (index == 0 ? 2 : 7);
      CHECK_EQ(std::static_pointer_cast<arrow::Int64Array>(batch->column(0))->Value(0), expected);
      ASSIGN_OR_FAIL(auto end, batches->Next());
      CHECK(end == nullptr);
    }
  }
}

TEST(ExecutorRejectsAFactorySchemaDifferentFromTheNegotiatedSchema) {
  const SparkDsLibrary& library = Library();
  void* source = CreateSource({});
  // The factory returns one id column, but the driver negotiates the empty schema of the source.
  void* reader = CreateReader(source);
  ASSIGN_OR_FAIL(Plan plan, Planned(reader));
  library.close_reader(reader);
  library.close(source);
  sn::testing::HostTask task(0, 1);
  struct ArrowArrayStream stream{};
  CHECK(library.read(&task.context, reinterpret_cast<const uint8_t*>(plan.state.data()),
                     static_cast<int64_t>(plan.state.size()),
                     reinterpret_cast<const uint8_t*>(plan.partitions[0].data()),
                     static_cast<int64_t>(plan.partitions[0].size()), &stream) != 0);
  CHECK_CONTAINS(LastError(library), "negotiated schema");
  CHECK(stream.release == nullptr);
}

TEST(TheReadOfAMicroBatchFailsForASourceWithoutStreamingReads) {
  const SparkDsLibrary& library = Library();
  CHECK(library.create_stream_reader == nullptr);
  ASSIGN_OR_FAIL(std::string state,
                 sn::internal::EncodeState(/*streaming=*/true, *arrow::schema(arrow::FieldVector{}),
                                           Factory()));
  ASSIGN_OR_FAIL(std::string partition, sn::internal::Encode(int64_t{0}));
  sn::testing::HostTask task(0, 1);
  struct ArrowArrayStream stream{};
  CHECK(library.read(&task.context, reinterpret_cast<const uint8_t*>(state.data()),
                     static_cast<int64_t>(state.size()),
                     reinterpret_cast<const uint8_t*>(partition.data()),
                     static_cast<int64_t>(partition.size()), &stream) != 0);
  CHECK_CONTAINS(LastError(library), "does not support micro-batch streaming reads");
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
