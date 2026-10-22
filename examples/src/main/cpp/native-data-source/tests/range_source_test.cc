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

#include <limits>

#include <arrow/api.h>

#include "range_source.h"
#include "test_util.h"

namespace sn = spark::native;
using cpp_sdk_range::OutputSchema;
using cpp_sdk_range::RangeOffset;
using cpp_sdk_range::RangePartition;
using cpp_sdk_range::RangeReaderFactory;
using cpp_sdk_range::RangeScan;
using cpp_sdk_range::RangeSource;
using spark::native::FilterSupport;
using spark::native::LimitSupport;
using spark::native::Predicate;

namespace {

constexpr int64_t kMin = std::numeric_limits<int64_t>::min();
constexpr int64_t kMax = std::numeric_limits<int64_t>::max();

using Options = std::vector<std::pair<std::string, std::string>>;

arrow::Result<RangeSource> MakeSource(const Options& options) {
  return RangeSource::Make(
      sn::CreateInfo("cpp_sdk_range", sn::Options(options.begin(), options.end())));
}

RangeSource Source(const Options& options) {
  ASSIGN_OR_FAIL(RangeSource source, MakeSource(options));
  return source;
}

RangeScan Scan(const RangeSource& source) {
  ASSIGN_OR_FAIL(std::shared_ptr<arrow::Schema> schema, source.InferSchema());
  ASSIGN_OR_FAIL(RangeScan scan, source.NewScan(sn::ScanInfo(schema)));
  return scan;
}

Predicate Comparison(const std::string& name, int64_t value, bool reversed = false) {
  std::vector<Predicate> children = {Predicate::Column({"id"}),
                                     Predicate::Literal("bigint", value)};
  if (reversed) {
    std::swap(children[0], children[1]);
  }
  return Predicate::Function(name, std::move(children));
}

using Batches = std::vector<std::shared_ptr<arrow::RecordBatch>>;

/// Reads every partition with the schema that the SDK negotiated for `columns`.
Batches Read(const sn::ScanPlan<RangeReaderFactory>& plan,
             const std::vector<std::string>& columns) {
  ASSIGN_OR_FAIL(std::shared_ptr<arrow::Schema> schema, OutputSchema(columns));
  Batches batches;
  for (size_t index = 0; index < plan.partitions.size(); ++index) {
    sn::TaskContext task = sn::TaskContext::ForTesting().WithPartitionId(static_cast<int>(index));
    ASSIGN_OR_FAIL(auto reader,
                   plan.factory.Open(plan.partitions[index], sn::ReaderContext(schema, task)));
    while (true) {
      ASSIGN_OR_FAIL(std::shared_ptr<arrow::RecordBatch> batch, reader->Next());
      if (batch == nullptr) {
        break;
      }
      batches.push_back(batch);
    }
  }
  return batches;
}

const std::vector<std::string> kAll = {"id", "label"};

std::vector<int64_t> Ids(const Batches& batches) {
  std::vector<int64_t> ids;
  for (const auto& batch : batches) {
    auto column = std::static_pointer_cast<arrow::Int64Array>(batch->GetColumnByName("id"));
    ids.insert(ids.end(), column->raw_values(), column->raw_values() + column->length());
  }
  return ids;
}

std::vector<int64_t> Sequence(int64_t start, int64_t end) {
  std::vector<int64_t> values;
  for (int64_t value = start; value < end; ++value) {
    values.push_back(value);
  }
  return values;
}

sn::ScanPlan<RangeReaderFactory> Plan(RangeScan& scan) {
  ASSIGN_OR_FAIL(sn::ScanPlan<RangeReaderFactory> plan, scan.Plan());
  return plan;
}

}  // namespace

TEST(FiltersBeforeTheGlobalLimitAndPreservesTheProjectedOrder) {
  RangeSource source =
      Source({{"start", "10"}, {"end", "25"}, {"numpartitions", "3"}, {"batchsize", "2"}});
  RangeScan scan = Scan(source);
  ASSIGN_OR_FAIL(auto support, scan.PushFilters({Comparison(">=", 13), Comparison("<", 22)}));
  CHECK(support == std::vector<FilterSupport>(2, FilterSupport::kExact));
  ASSIGN_OR_FAIL(LimitSupport limit, scan.PushLimit(5));
  CHECK(limit == LimitSupport::kGlobal);
  ASSIGN_OR_FAIL(bool pruned, scan.PruneColumns({"label", "id"}));
  CHECK(pruned);
  sn::ScanPlan<RangeReaderFactory> plan = Plan(scan);
  CHECK_EQ(plan.partitions.size(), size_t{3});
  Batches batches = Read(plan, {"label", "id"});
  CHECK(Ids(batches) == Sequence(13, 18));
  for (const auto& batch : batches) {
    CHECK(batch->num_rows() <= 2);
    CHECK_EQ(batch->schema()->field(0)->name(), std::string("label"));
    auto labels = std::static_pointer_cast<arrow::StringArray>(batch->column(0));
    std::vector<int64_t> ids = Ids({batch});
    for (size_t row = 0; row < ids.size(); ++row) {
      CHECK_EQ(labels->GetString(static_cast<int64_t>(row)), "row-" + std::to_string(ids[row]));
    }
  }
}

TEST(ComparisonsHandleReversedOperandsAndInt64Boundaries) {
  for (const auto& [start, end] :
       std::vector<std::pair<int64_t, int64_t>>{{-2, 3}, {kMin, kMin + 3}, {kMax - 3, kMax}}) {
    RangeSource source = Source({{"start", std::to_string(start)}, {"end", std::to_string(end)}});
    for (int64_t literal : {kMin, int64_t{-1}, int64_t{0}, int64_t{1}, kMax}) {
      for (const std::string op : {"=", "<=>", ">", ">=", "<", "<="}) {
        for (bool reversed : {false, true}) {
          RangeScan scan = Scan(source);
          ASSIGN_OR_FAIL(auto support, scan.PushFilters({Comparison(op, literal, reversed)}));
          CHECK(support == std::vector<FilterSupport>({FilterSupport::kExact}));
          std::vector<int64_t> expected;
          for (int64_t id : Sequence(start, end)) {
            int64_t left = reversed ? literal : id;
            int64_t right = reversed ? id : literal;
            bool matches = op == ">"    ? left > right
                           : op == ">=" ? left >= right
                           : op == "<"  ? left < right
                           : op == "<=" ? left <= right
                                        : left == right;
            if (matches) {
              expected.push_back(id);
            }
          }
          CHECK(Ids(Read(Plan(scan), kAll)) == expected);
        }
      }
    }
  }
}

TEST(UnsupportedFiltersRemainResidualWithoutMutatingTheScan) {
  RangeSource source = Source({{"end", "10"}});
  RangeScan scan = Scan(source);
  Predicate unsupported_and = Predicate::Function("AND", {Comparison(">", 5), Predicate()});
  ASSIGN_OR_FAIL(Predicate null, Predicate::Parse(R"({"type":"function","name":"<=>","children":[
      {"type":"column","name":["id"]},{"type":"literal","dataType":"bigint","value":null}]})"));
  ASSIGN_OR_FAIL(Predicate floating, Predicate::Parse(R"({"type":"function","name":"=","children":[
      {"type":"column","name":["id"]},{"type":"literal","dataType":"double","value":1.5}]})"));
  ASSIGN_OR_FAIL(auto support, scan.PushFilters({unsupported_and, null, floating,
                                                 Comparison("FUTURE_COMPARISON", 0)}));
  CHECK(support == std::vector<FilterSupport>(4, FilterSupport::kUnsupported));
  CHECK(Ids(Read(Plan(scan), kAll)) == Sequence(0, 10));
}

TEST(ConjunctionsAndContradictoryPredicatesFormExactIntersections) {
  RangeSource source = Source({{"end", "10"}});
  RangeScan first = Scan(source);
  Predicate conjunction = Predicate::Function("AND", {Comparison(">=", 3), Comparison("<=", 6)});
  ASSIGN_OR_FAIL(auto support, first.PushFilters({conjunction}));
  CHECK(support == std::vector<FilterSupport>({FilterSupport::kExact}));
  CHECK(Ids(Read(Plan(first), kAll)) == std::vector<int64_t>({3, 4, 5, 6}));
  RangeScan empty = Scan(source);
  CHECK_OK(empty.PushFilters({Comparison(">", 5), Comparison("<", 3)}));
  CHECK(Plan(empty).partitions.empty());
}

TEST(SerializedPlansPreserveZeroColumnRowCountsAndBatchSizes) {
  RangeSource source = Source({{"end", "7"}, {"numpartitions", "1"}, {"batchsize", "3"}});
  RangeScan scan = Scan(source);
  CHECK_OK(scan.PruneColumns({}));
  sn::ScanPlan<RangeReaderFactory> plan = Plan(scan);
  auto factory =
      nlohmann::json::parse(nlohmann::json(plan.factory).dump()).get<RangeReaderFactory>();
  std::vector<RangePartition> partitions;
  for (const auto& partition : plan.partitions) {
    partitions.push_back(
        nlohmann::json::parse(nlohmann::json(partition).dump()).get<RangePartition>());
  }
  Batches batches = Read(sn::ScanPlan<RangeReaderFactory>(factory, partitions), {});
  std::vector<int64_t> rows;
  for (const auto& batch : batches) {
    rows.push_back(batch->num_rows());
    CHECK_EQ(batch->num_columns(), 0);
  }
  CHECK(rows == std::vector<int64_t>({3, 3, 1}));
}

TEST(ZeroLimitEmptyRangeAndRepeatedLimitsDoNotAddRows) {
  RangeSource source = Source({{"end", "10"}, {"numpartitions", "20"}});
  RangeScan zero = Scan(source);
  CHECK_OK(zero.PushLimit(0));
  CHECK(Plan(zero).partitions.empty());
  RangeScan limited = Scan(source);
  CHECK_OK(limited.PushLimit(2));
  CHECK_OK(limited.PushLimit(5));
  sn::ScanPlan<RangeReaderFactory> plan = Plan(limited);
  CHECK_EQ(plan.partitions.size(), size_t{2});
  CHECK(Ids(Read(plan, kAll)) == std::vector<int64_t>({0, 1}));
  RangeSource empty = Source({{"start", "5"}, {"end", "5"}});
  RangeScan scan = Scan(empty);
  CHECK(Plan(scan).partitions.empty());
}

TEST(RejectsInvalidOptionsAndIncompatibleUserSchemas) {
  for (const auto& option : Options{{"batchsize", "0"},
                                    {"batchsize", "-1"},
                                    {"numpartitions", "0"},
                                    {"end", "-1"},
                                    {"start", "not-an-integer"},
                                    {"unknown", "1"}}) {
    CHECK(!MakeSource({option}).ok());
  }
  RangeSource source = Source({});
  auto invalid = arrow::schema({arrow::field("id", arrow::int32(), false)});
  CHECK(!source.NewScan(sn::ScanInfo(invalid)).ok());
  ASSIGN_OR_FAIL(auto labels, OutputSchema({"label"}));
  ASSIGN_OR_FAIL(RangeScan scan, source.NewScan(sn::ScanInfo(labels)));
  CHECK(!scan.PruneColumns({"id"}).ok());
  CHECK(!OutputSchema({"id", "id"}).ok());
}

TEST(AStreamPlansMicroBatchesBetweenJsonOffsets) {
  RangeSource source =
      Source({{"start", "10"}, {"end", "25"}, {"numpartitions", "2"}, {"batchsize", "4"}});
  ASSIGN_OR_FAIL(auto schema, source.InferSchema());
  ASSIGN_OR_FAIL(auto stream, source.NewMicroBatchStream(sn::MicroBatchStreamInfo(schema)));
  ASSIGN_OR_FAIL(RangeOffset initial, stream.InitialOffset());
  ASSIGN_OR_FAIL(RangeOffset latest, stream.LatestOffset());
  CHECK_EQ(nlohmann::json(initial).dump(), std::string(R"({"next":10})"));
  CHECK_EQ(nlohmann::json(latest).dump(), std::string(R"({"next":25})"));
  // As if an earlier query had planned a micro-batch up to an offset of its checkpoint.
  auto middle = nlohmann::json::parse(R"({"next":13})").get<RangeOffset>();
  ASSIGN_OR_FAIL(auto first, stream.Plan(initial, middle));
  CHECK_EQ(first.partitions.size(), size_t{2});
  CHECK(Ids(Read(first, kAll)) == Sequence(10, 13));
  ASSIGN_OR_FAIL(auto second, stream.Plan(middle, latest));
  CHECK(Ids(Read(second, kAll)) == Sequence(13, 25));
  ASSIGN_OR_FAIL(auto none, stream.Plan(latest, latest));
  CHECK(none.partitions.empty());
  CHECK(!stream.Plan(initial, RangeOffset{30}).ok());
  CHECK(!stream.Plan(latest, initial).ok());
  auto invalid = arrow::schema({arrow::field("id", arrow::int32(), false)});
  CHECK(!source.NewMicroBatchStream(sn::MicroBatchStreamInfo(invalid)).ok());
}

TEST(OpensAHugePartitionLazilyWithoutMaterializingIt) {
  RangeSource source = Source({{"start", std::to_string(kMin)},
                               {"end", std::to_string(kMax)},
                               {"numpartitions", "1"},
                               {"batchsize", "2"}});
  RangeScan scan = Scan(source);
  sn::ScanPlan<RangeReaderFactory> plan = Plan(scan);
  ASSIGN_OR_FAIL(auto schema, source.InferSchema());
  ASSIGN_OR_FAIL(auto reader,
                 plan.factory.Open(plan.partitions[0],
                                   sn::ReaderContext(schema, sn::TaskContext::ForTesting())));
  ASSIGN_OR_FAIL(auto first, reader->Next());
  ASSIGN_OR_FAIL(auto second, reader->Next());
  CHECK(Ids({first}) == std::vector<int64_t>({kMin, kMin + 1}));
  CHECK(Ids({second}) == std::vector<int64_t>({kMin + 2, kMin + 3}));
}
