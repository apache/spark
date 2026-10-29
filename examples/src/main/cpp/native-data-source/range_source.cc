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

#include "range_source.h"

#include <algorithm>
#include <charconv>
#include <limits>
#include <set>
#include <tuple>
#include <utility>

#include <arrow/api.h>
#include <arrow/util/iterator.h>

namespace cpp_sdk_range {

using spark::native::FilterSupport;
using spark::native::LimitSupport;
using spark::native::LiteralValue;
using spark::native::Predicate;
using spark::native::ScanPlan;

namespace {

constexpr int64_t kMax = std::numeric_limits<int64_t>::max();

/// The number of ids in [lower, upper), which may exceed the range of int64_t.
uint64_t Rows(int64_t lower, int64_t upper) {
  return upper <= lower ? 0 : static_cast<uint64_t>(upper) - static_cast<uint64_t>(lower);
}

/// The id `rows` after `id`, which the caller keeps within the range.
int64_t Advance(int64_t id, uint64_t rows) {
  return static_cast<int64_t>(static_cast<uint64_t>(id) + rows);
}

/// Splits the ids in [lower, upper) into at most `num_partitions` disjoint partitions.
std::vector<RangePartition> Split(int64_t lower, int64_t upper, int64_t num_partitions) {
  std::vector<RangePartition> partitions;
  uint64_t rows = Rows(lower, upper);
  uint64_t count = std::min(static_cast<uint64_t>(num_partitions), rows);
  if (count == 0) {
    return partitions;
  }
  uint64_t width = rows / count;
  uint64_t extra = rows % count;
  int64_t start = lower;
  for (uint64_t index = 0; index < count; ++index) {
    int64_t end = Advance(start, width + (index < extra ? 1 : 0));
    partitions.push_back(RangePartition{start, end});
    start = end;
  }
  return partitions;
}

/// The columns of a schema to read, which must have the types of the range.
arrow::Result<std::vector<std::string>> ReadColumns(const arrow::Schema& schema) {
  std::vector<std::string> columns;
  for (const auto& field : schema.fields()) {
    columns.push_back(field->name());
  }
  ARROW_ASSIGN_OR_RAISE(auto expected, OutputSchema(columns));
  for (int index = 0; index < schema.num_fields(); ++index) {
    if (!schema.field(index)->type()->Equals(*expected->field(index)->type())) {
      return arrow::Status::Invalid("Unsupported type for ", schema.field(index)->name(), ": ",
                                    schema.field(index)->type()->ToString());
    }
  }
  return columns;
}

arrow::Result<int64_t> Option(const spark::native::Options& options, const std::string& name,
                              int64_t default_value) {
  auto option = options.find(name);
  if (option == options.end()) {
    return default_value;
  }
  const std::string& text = option->second;
  int64_t value;
  auto [end, error] = std::from_chars(text.data(), text.data() + text.size(), value);
  if (error != std::errc() || end != text.data() + text.size()) {
    return arrow::Status::Invalid("Invalid ", name, ": ", text, " is not a 64-bit integer");
  }
  return value;
}

bool IsId(const Predicate& predicate) {
  return predicate.kind == Predicate::Kind::kColumn && predicate.column.size() == 1 &&
         predicate.column[0] == "id";
}

/// A non-null tinyint, smallint, int, or bigint literal, decoded by the SDK.
std::optional<int64_t> IntegerLiteral(const Predicate& predicate) {
  std::optional<LiteralValue> value = predicate.literal_value();
  if (!value || !std::holds_alternative<int64_t>(*value)) {
    return std::nullopt;
  }
  return std::get<int64_t>(*value);
}

/// The ids in [lower, upper) for which `predicate` is true, or nothing for a predicate that the
/// range does not evaluate.
std::optional<std::pair<int64_t, int64_t>> PredicateBounds(const Predicate& predicate,
                                                           int64_t lower, int64_t upper) {
  if (predicate.kind != Predicate::Kind::kFunction || predicate.children.size() != 2) {
    return std::nullopt;
  }
  if (predicate.name == "AND") {
    // Do not narrow the scan when only one side of an AND is supported.
    auto left = PredicateBounds(predicate.children[0], lower, upper);
    if (!left) {
      return std::nullopt;
    }
    return PredicateBounds(predicate.children[1], left->first, left->second);
  }
  std::string name = predicate.name;
  std::optional<int64_t> value;
  if (IsId(predicate.children[0])) {
    value = IntegerLiteral(predicate.children[1]);
  } else if (IsId(predicate.children[1])) {
    value = IntegerLiteral(predicate.children[0]);
    // `value op id` is `id reversed(op) value`.
    if (name == ">") {
      name = "<";
    } else if (name == ">=") {
      name = "<=";
    } else if (name == "<") {
      name = ">";
    } else if (name == "<=") {
      name = ">=";
    }
  }
  if (!value) {
    return std::nullopt;
  }
  // The bounds never exceed those of the range, whose end is at most the largest int64_t.
  auto at_least = [&](int64_t id) { lower = std::max(lower, id); };
  auto below = [&](int64_t id) { upper = std::min(upper, id); };
  if (name == "=" || name == "<=>") {
    at_least(*value);
    if (*value < kMax) below(*value + 1);
  } else if (name == ">") {
    if (*value < kMax) {
      at_least(*value + 1);
    } else {
      below(lower);
    }
  } else if (name == ">=") {
    at_least(*value);
  } else if (name == "<") {
    below(*value);
  } else if (name == "<=") {
    if (*value < kMax) below(*value + 1);
  } else {
    return std::nullopt;
  }
  return std::make_pair(lower, upper);
}

}  // namespace

arrow::Result<std::shared_ptr<arrow::Schema>> OutputSchema(
    const std::vector<std::string>& columns) {
  std::set<std::string> seen;
  arrow::FieldVector fields;
  for (const std::string& name : columns) {
    if (!seen.insert(name).second) {
      return arrow::Status::Invalid("Duplicate column: ", name);
    }
    if (name == "id") {
      fields.push_back(arrow::field(name, arrow::int64(), /*nullable=*/false));
    } else if (name == "label") {
      fields.push_back(arrow::field(name, arrow::utf8(), /*nullable=*/false));
    } else {
      return arrow::Status::Invalid("Unknown column: ", name);
    }
  }
  return arrow::schema(std::move(fields));
}

// RangeSource

RangeSource::RangeSource(int64_t start, int64_t end, int64_t num_partitions, int64_t batch_size)
    : start_(start), end_(end), num_partitions_(num_partitions), batch_size_(batch_size) {}

arrow::Result<RangeSource> RangeSource::Make(const spark::native::CreateInfo& info) {
  for (const auto& [key, value] : info.options) {
    if (key != "start" && key != "end" && key != "numpartitions" && key != "batchsize") {
      return arrow::Status::Invalid("Unknown range option: ", key);
    }
  }
  ARROW_ASSIGN_OR_RAISE(int64_t start, Option(info.options, "start", 0));
  ARROW_ASSIGN_OR_RAISE(int64_t end, Option(info.options, "end", 100));
  ARROW_ASSIGN_OR_RAISE(int64_t num_partitions, Option(info.options, "numpartitions", 2));
  ARROW_ASSIGN_OR_RAISE(int64_t batch_size, Option(info.options, "batchsize", 1024));
  if (end < start) {
    return arrow::Status::Invalid("end must be greater than or equal to start");
  }
  if (num_partitions <= 0 || batch_size <= 0) {
    return arrow::Status::Invalid("numPartitions and batchSize must be positive");
  }
  return RangeSource(start, end, num_partitions, batch_size);
}

arrow::Result<std::shared_ptr<arrow::Schema>> RangeSource::InferSchema() const {
  return OutputSchema({"id", "label"});
}

arrow::Result<RangeScan> RangeSource::NewScan(const spark::native::ScanInfo& info) const {
  ARROW_ASSIGN_OR_RAISE(std::vector<std::string> columns, ReadColumns(*info.schema));
  RangeReaderFactory factory;
  factory.batch_size = batch_size_;
  return RangeScan(start_, end_, num_partitions_, std::move(columns), factory);
}

arrow::Result<RangeStream> RangeSource::NewMicroBatchStream(
    const spark::native::MicroBatchStreamInfo& info) const {
  ARROW_RETURN_NOT_OK(ReadColumns(*info.schema));
  RangeReaderFactory factory;
  factory.batch_size = batch_size_;
  return RangeStream(start_, end_, num_partitions_, factory);
}

// RangeReaderFactory

arrow::Result<std::shared_ptr<arrow::RecordBatchReader>> RangeReaderFactory::Open(
    const RangePartition& partition, const spark::native::ReaderContext& context) const {
  if (batch_size <= 0 || partition.start > partition.end) {
    return arrow::Status::Invalid("Invalid serialized range plan");
  }
  // The negotiated schema has the requested columns in order, with the types that the scan
  // validated.
  std::shared_ptr<arrow::Schema> schema = context.schema;
  std::vector<std::string> columns;
  for (const auto& field : schema->fields()) {
    columns.push_back(field->name());
  }
  ARROW_RETURN_NOT_OK(OutputSchema(columns));
  spark::native::TaskContext task = context.task;
  int64_t end = partition.end;
  uint64_t rows_per_batch = static_cast<uint64_t>(batch_size);
  auto next =
      [=,
       current = partition.start]() mutable -> arrow::Result<std::shared_ptr<arrow::RecordBatch>> {
    if (current == end) {
      return arrow::IterationEnd<std::shared_ptr<arrow::RecordBatch>>();
    }
    if (task.IsInterrupted()) {
      current = end;
      return arrow::Status::Cancelled("Range read was cancelled");
    }
    uint64_t rows = std::min(rows_per_batch, Rows(current, end));
    int64_t last = Advance(current, rows);
    arrow::ArrayVector arrays;
    for (const std::string& column : columns) {
      std::shared_ptr<arrow::Array> array;
      if (column == "id") {
        arrow::Int64Builder ids;
        ARROW_RETURN_NOT_OK(ids.Reserve(static_cast<int64_t>(rows)));
        for (int64_t id = current; id < last; ++id) {
          ids.UnsafeAppend(id);
        }
        ARROW_ASSIGN_OR_RAISE(array, ids.Finish());
      } else {
        arrow::StringBuilder labels;
        for (int64_t id = current; id < last; ++id) {
          ARROW_RETURN_NOT_OK(labels.Append("row-" + std::to_string(id)));
        }
        ARROW_ASSIGN_OR_RAISE(array, labels.Finish());
      }
      arrays.push_back(std::move(array));
    }
    current = last;
    // Explicit row counts also preserve COUNT(*) with a zero-column projection.
    return arrow::RecordBatch::Make(schema, static_cast<int64_t>(rows), std::move(arrays));
  };
  return arrow::RecordBatchReader::MakeFromIterator(arrow::MakeFunctionIterator(std::move(next)),
                                                    schema);
}

// RangeScan

RangeScan::RangeScan(int64_t start, int64_t end, int64_t num_partitions,
                     std::vector<std::string> columns, RangeReaderFactory factory)
    : lower_(start),
      upper_(end),
      num_partitions_(num_partitions),
      columns_(std::move(columns)),
      factory_(factory) {}

arrow::Result<std::vector<FilterSupport>> RangeScan::PushFilters(
    const std::vector<Predicate>& filters) {
  std::vector<FilterSupport> support;
  for (const Predicate& filter : filters) {
    if (auto bounds = PredicateBounds(filter, lower_, upper_)) {
      std::tie(lower_, upper_) = *bounds;
      support.push_back(FilterSupport::kExact);
    } else {
      support.push_back(FilterSupport::kUnsupported);
    }
  }
  return support;
}

arrow::Result<LimitSupport> RangeScan::PushLimit(int64_t limit) {
  limit_ = limit_ ? std::min(*limit_, limit) : limit;
  return LimitSupport::kGlobal;
}

arrow::Result<bool> RangeScan::PruneColumns(const std::vector<std::string>& columns) {
  for (const std::string& column : columns) {
    if (std::find(columns_.begin(), columns_.end(), column) == columns_.end()) {
      return arrow::Status::Invalid("Projection contains a column outside the read schema");
    }
  }
  ARROW_RETURN_NOT_OK(OutputSchema(columns));
  columns_ = columns;
  return true;
}

arrow::Result<ScanPlan<RangeReaderFactory>> RangeScan::Plan() {
  // Filter before applying a global limit, then split the selected interval.
  int64_t upper = upper_;
  if (limit_ && Rows(lower_, upper) > static_cast<uint64_t>(*limit_)) {
    upper = Advance(lower_, static_cast<uint64_t>(*limit_));
  }
  return ScanPlan<RangeReaderFactory>(factory_, Split(lower_, upper, num_partitions_));
}

// RangeStream

RangeStream::RangeStream(int64_t start, int64_t end, int64_t num_partitions,
                         RangeReaderFactory factory)
    : start_(start), end_(end), num_partitions_(num_partitions), factory_(factory) {}

arrow::Result<RangeOffset> RangeStream::InitialOffset() { return RangeOffset{start_}; }

arrow::Result<RangeOffset> RangeStream::LatestOffset() { return RangeOffset{end_}; }

arrow::Result<ScanPlan<RangeReaderFactory>> RangeStream::Plan(const RangeOffset& start,
                                                              const RangeOffset& end) {
  // The offsets of a checkpoint may come from a query with other options.
  if (start.next < start_ || end.next > end_ || start.next > end.next) {
    return arrow::Status::Invalid("The offsets ", start.next, " and ", end.next,
                                  " do not delimit rows of the range [", start_, ", ", end_, ")");
  }
  // Micro-batches are read as batch scans are.
  return ScanPlan<RangeReaderFactory>(factory_, Split(start.next, end.next, num_partitions_));
}

}  // namespace cpp_sdk_range

SPARK_EXPORT_DATA_SOURCE(cpp_sdk_range::RangeSource);
