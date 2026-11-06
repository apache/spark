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
#include <set>
#include <utility>

#include <arrow/api.h>
#include <arrow/util/iterator.h>

namespace cpp_sdk_range {

using spark::native::LatestOffsetInfo;
using spark::native::LimitSupport;
using spark::native::ReadLimit;
using spark::native::ScanPlan;

namespace {

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

RangeSource::RangeSource(int64_t start, int64_t end, int64_t num_partitions, int64_t batch_size,
                         std::optional<int64_t> max_rows_per_trigger)
    : start_(start),
      end_(end),
      num_partitions_(num_partitions),
      batch_size_(batch_size),
      max_rows_per_trigger_(max_rows_per_trigger) {}

arrow::Result<RangeSource> RangeSource::Make(const spark::native::CreateInfo& info) {
  for (const auto& [key, value] : info.options) {
    if (key != "start" && key != "end" && key != "numpartitions" && key != "batchsize" &&
        key != "maxrowspertrigger") {
      return arrow::Status::Invalid("Unknown range option: ", key);
    }
  }
  ARROW_ASSIGN_OR_RAISE(int64_t start, Option(info.options, "start", 0));
  ARROW_ASSIGN_OR_RAISE(int64_t end, Option(info.options, "end", 100));
  ARROW_ASSIGN_OR_RAISE(int64_t num_partitions, Option(info.options, "numpartitions", 2));
  ARROW_ASSIGN_OR_RAISE(int64_t batch_size, Option(info.options, "batchsize", 1024));
  std::optional<int64_t> max_rows_per_trigger;
  if (info.options.count("maxrowspertrigger") != 0) {
    ARROW_ASSIGN_OR_RAISE(max_rows_per_trigger, Option(info.options, "maxrowspertrigger", 0));
  }
  if (end < start) {
    return arrow::Status::Invalid("end must be greater than or equal to start");
  }
  if (num_partitions <= 0 || batch_size <= 0 ||
      (max_rows_per_trigger && *max_rows_per_trigger <= 0)) {
    return arrow::Status::Invalid(
        "numPartitions, batchSize and maxRowsPerTrigger must be positive");
  }
  return RangeSource(start, end, num_partitions, batch_size, max_rows_per_trigger);
}

arrow::Result<std::shared_ptr<arrow::Schema>> RangeSource::InferSchema() const {
  return OutputSchema({"id", "label"});
}

arrow::Result<RangeScan> RangeSource::NewScan(const spark::native::ScanInfo& info) const {
  ARROW_RETURN_NOT_OK(ReadColumns(*info.schema));
  RangeReaderFactory factory;
  factory.batch_size = batch_size_;
  return RangeScan(start_, end_, num_partitions_, factory);
}

arrow::Result<RangeStream> RangeSource::NewMicroBatchStream(
    const spark::native::MicroBatchStreamInfo& info) const {
  ARROW_RETURN_NOT_OK(ReadColumns(*info.schema));
  RangeReaderFactory factory;
  factory.batch_size = batch_size_;
  return RangeStream(start_, end_, num_partitions_, max_rows_per_trigger_, factory);
}

// RangeReaderFactory

arrow::Result<std::shared_ptr<arrow::RecordBatchReader>> RangeReaderFactory::Open(
    const RangePartition& partition, const spark::native::ReaderContext& context) const {
  if (batch_size <= 0 || partition.start > partition.end) {
    return arrow::Status::Invalid("Invalid serialized range plan");
  }
  // The schema to read has the columns that the query needs, in order, with the types that the
  // scan validated.
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

RangeScan::RangeScan(int64_t start, int64_t end, int64_t num_partitions, RangeReaderFactory factory)
    : start_(start), end_(end), num_partitions_(num_partitions), factory_(factory) {}

arrow::Result<LimitSupport> RangeScan::PushLimit(int64_t limit) {
  limit_ = limit;
  return LimitSupport::kGlobal;
}

arrow::Result<ScanPlan<RangeReaderFactory>> RangeScan::Plan() {
  // Apply a global limit by shortening the range, then split it.
  int64_t end = end_;
  if (limit_ && Rows(start_, end) > static_cast<uint64_t>(*limit_)) {
    end = Advance(start_, static_cast<uint64_t>(*limit_));
  }
  return ScanPlan<RangeReaderFactory>(factory_, Split(start_, end, num_partitions_));
}

// RangeStream

RangeStream::RangeStream(int64_t start, int64_t end, int64_t num_partitions,
                         std::optional<int64_t> max_rows_per_trigger, RangeReaderFactory factory)
    : start_(start),
      end_(end),
      num_partitions_(num_partitions),
      max_rows_per_trigger_(max_rows_per_trigger),
      factory_(factory) {}

arrow::Result<RangeOffset> RangeStream::InitialOffset() { return RangeOffset{start_}; }

arrow::Result<ReadLimit> RangeStream::DefaultReadLimit() const {
  return max_rows_per_trigger_ ? ReadLimit::MaxRows(*max_rows_per_trigger_)
                               : ReadLimit::AllAvailable();
}

arrow::Result<RangeOffset> RangeStream::LatestOffset(const LatestOffsetInfo<RangeOffset>& info) {
  // All the rows of the range are available, within the limit and the end of the run.
  int64_t next = end_;
  if (info.limit.kind() == ReadLimit::Kind::kMaxRows && info.start.next < end_ &&
      Rows(info.start.next, end_) > static_cast<uint64_t>(info.limit.max_rows())) {
    next = Advance(info.start.next, static_cast<uint64_t>(info.limit.max_rows()));
  }
  if (info.run_end) {
    next = std::min(next, info.run_end->next);
  }
  return RangeOffset{next};
}

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
