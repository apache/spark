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

// A deterministic range source: `id` in [start, end), and `label = "row-{id}"`. All C ABI and
// Arrow C stream plumbing is supplied by the SDK's SPARK_EXPORT_DATA_SOURCE.

#pragma once

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "spark/native/data_source.h"

namespace cpp_sdk_range {

/// The rows of a partition: the ids in [start, end).
struct RangePartition {
  int64_t start = 0;
  int64_t end = 0;
};
NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE(RangePartition, start, end)

/// Reader configuration travels to each executor; it contains no live objects. The SDK passes the
/// negotiated columns to the reader, so the factory omits them.
struct RangeReaderFactory : spark::native::PartitionReaderFactory<RangePartition> {
  int64_t batch_size = 1024;

  arrow::Result<std::shared_ptr<arrow::RecordBatchReader>> Open(
      const RangePartition& partition, const spark::native::ReaderContext& context) const override;
};
NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE(RangeReaderFactory, batch_size)

/// The offset of a streaming read: the next row to read. Spark stores it in the checkpoint as
/// {"next":13}.
struct RangeOffset {
  int64_t next = 0;
};
NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE(RangeOffset, next)

class RangeScan : public spark::native::ScanBuilder<RangeReaderFactory> {
 public:
  RangeScan(int64_t start, int64_t end, int64_t num_partitions, std::vector<std::string> columns,
            RangeReaderFactory factory);

  arrow::Result<std::vector<spark::native::FilterSupport>> PushFilters(
      const std::vector<spark::native::Predicate>& filters) override;
  arrow::Result<spark::native::LimitSupport> PushLimit(int64_t limit) override;
  arrow::Result<bool> PruneColumns(const std::vector<std::string>& columns) override;
  arrow::Result<spark::native::ScanPlan<RangeReaderFactory>> Plan() override;

 private:
  // The selected ids, [lower_, upper_), which stay within the range, so that their arithmetic
  // never overflows. The selection is empty when lower_ >= upper_.
  int64_t lower_;
  int64_t upper_;
  int64_t num_partitions_;
  std::optional<int64_t> limit_;
  std::vector<std::string> columns_;
  RangeReaderFactory factory_;
};

/// A streaming read of the range. The range is fixed, so all its rows are available when the query
/// starts: the first micro-batch reads them, and later ones find no new data.
class RangeStream : public spark::native::MicroBatchStream<RangeOffset, RangeReaderFactory> {
 public:
  RangeStream(int64_t start, int64_t end, int64_t num_partitions, RangeReaderFactory factory);

  arrow::Result<RangeOffset> InitialOffset() override;
  arrow::Result<RangeOffset> LatestOffset() override;
  arrow::Result<spark::native::ScanPlan<RangeReaderFactory>> Plan(const RangeOffset& start,
                                                                  const RangeOffset& end) override;

 private:
  int64_t start_;
  int64_t end_;
  int64_t num_partitions_;
  RangeReaderFactory factory_;
};

class RangeSource : public spark::native::DataSource {
 public:
  using ScanBuilder = RangeScan;
  using MicroBatchStream = RangeStream;
  // A read-only source: its writes stay Unsupported.

  static arrow::Result<RangeSource> Make(const spark::native::CreateInfo& info);

  arrow::Result<std::shared_ptr<arrow::Schema>> InferSchema() const override;
  arrow::Result<RangeScan> NewScan(const spark::native::ScanInfo& info) const;
  arrow::Result<RangeStream> NewMicroBatchStream(
      const spark::native::MicroBatchStreamInfo& info) const;

 private:
  RangeSource(int64_t start, int64_t end, int64_t num_partitions, int64_t batch_size);

  int64_t start_;
  int64_t end_;
  int64_t num_partitions_;
  int64_t batch_size_;
};

/// The schema of the columns of the range, in the order of `columns`.
arrow::Result<std::shared_ptr<arrow::Schema>> OutputSchema(const std::vector<std::string>& columns);

}  // namespace cpp_sdk_range
