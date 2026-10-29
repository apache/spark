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

#include "spark/native/data_source.h"
#include "test_util.h"

using spark::native::Capabilities;

TEST(TruncateNeverRemainsWithoutWrites) {
  Capabilities all = Capabilities::All();
  // Truncation remains while either write does.
  CHECK(all.Without(Capabilities::kBatchWrite).Contains(Capabilities::kTruncate));
  Capabilities reads =
      all.Without(Capabilities::kBatchWrite).Without(Capabilities::kStreamingWrite);
  CHECK(reads.Contains(Capabilities::kBatchRead));
  CHECK(reads.Contains(Capabilities::kMicroBatchRead));
  CHECK(!reads.Contains(Capabilities::kTruncate));
  CHECK(all.Without(Capabilities::kBatchWrite | Capabilities::kStreamingWrite) == reads);
  // A set of truncation alone has no write to truncate.
  CHECK((Capabilities::kTruncate | Capabilities::Empty()) == Capabilities::Empty());
  CHECK((Capabilities::kTruncate & all) == Capabilities::Empty());
  Capabilities append_only = all.Without(Capabilities::kTruncate);
  CHECK(append_only.Contains(Capabilities::kBatchWrite));
  CHECK(!append_only.Contains(Capabilities::kTruncate));
  CHECK(all.Contains(Capabilities::Empty()));
  CHECK_EQ(all.bits(),
           uint64_t{SPARK_DS_BATCH_READ | SPARK_DS_MICRO_BATCH_READ | SPARK_DS_BATCH_WRITE |
                    SPARK_DS_STREAMING_WRITE | SPARK_DS_TRUNCATE});
}

TEST(ToStringListsOperationNames) {
  CHECK_EQ(Capabilities::All().ToString(),
           std::string("{BATCH_READ, MICRO_BATCH_READ, BATCH_WRITE, STREAMING_WRITE, TRUNCATE}"));
  CHECK_EQ(Capabilities::Empty().ToString(), std::string("{}"));
}

// The capabilities of a source are those of its types, derived when the library compiles.

namespace {

struct Partition {
  int64_t value = 0;
};
NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE(Partition, value)

struct Factory : spark::native::PartitionReaderFactory<Partition> {
  int64_t unused = 0;

  arrow::Result<std::shared_ptr<arrow::RecordBatchReader>> Open(
      const Partition&, const spark::native::ReaderContext&) const override {
    return arrow::Status::NotImplemented("not read");
  }
};
NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE(Factory, unused)

struct Scan : spark::native::ScanBuilder<Factory> {
  arrow::Result<spark::native::ScanPlan<Factory>> Plan() override {
    return spark::native::ScanPlan<Factory>(Factory(), {});
  }
};

struct Message {
  int64_t rows = 0;
};
NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE(Message, rows)

struct WriterFactory : spark::native::DataWriterFactory<Message> {
  int64_t unused = 0;

  arrow::Result<std::unique_ptr<spark::native::DataWriter<Message>>> Open(
      const spark::native::WriterContext&) const override {
    return arrow::Status::NotImplemented("not written");
  }
};
NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE(WriterFactory, unused)

template <bool kTruncate>
struct Write : spark::native::StreamingWrite<WriterFactory> {
  static constexpr bool kSupportsTruncate = kTruncate;

  arrow::Result<WriterFactory> CreateWriterFactory() const override { return WriterFactory(); }
  arrow::Status Commit(int64_t, std::vector<Message>) override { return arrow::Status::OK(); }
  arrow::Status Abort(int64_t, std::vector<std::optional<Message>>) override {
    return arrow::Status::OK();
  }
};

class ReadOnly : public spark::native::DataSource {
 public:
  using ScanBuilder = Scan;

  static arrow::Result<ReadOnly> Make(const spark::native::CreateInfo&) { return ReadOnly(); }
  arrow::Result<Scan> NewScan(const spark::native::ScanInfo&) const { return Scan(); }
};

template <bool kTruncate>
class StreamingSink : public spark::native::DataSource {
 public:
  using StreamingWrite = Write<kTruncate>;

  static arrow::Result<StreamingSink> Make(const spark::native::CreateInfo&) {
    return StreamingSink();
  }
  arrow::Result<StreamingWrite> NewStreamingWrite(const spark::native::StreamingWriteInfo&) const {
    return StreamingWrite();
  }
};

template <typename S>
using Traits = spark::native::internal::SourceTraits<S>;

}  // namespace

TEST(TheTypesOfASourceDetermineItsOperations) {
  CHECK(Traits<ReadOnly>::kCapabilities == Capabilities::kBatchRead);
  CHECK(Traits<StreamingSink<true>>::kCapabilities ==
        (Capabilities::kStreamingWrite | Capabilities::kTruncate));
  CHECK(Traits<StreamingSink<false>>::kCapabilities == Capabilities::kStreamingWrite);
  // A declaration can only narrow the implemented operations.
  CHECK(Traits<ReadOnly>::Effective(Capabilities::All()) == Capabilities::kBatchRead);
  CHECK(Traits<ReadOnly>::Effective(Capabilities::kMicroBatchRead) == Capabilities::Empty());
  CHECK(Traits<StreamingSink<false>>::Effective(Capabilities::All()) ==
        Capabilities::kStreamingWrite);
  CHECK(Traits<StreamingSink<true>>::Effective(Capabilities::kStreamingWrite) ==
        Capabilities::kStreamingWrite);
  CHECK(Traits<StreamingSink<true>>::Effective(Capabilities::kTruncate) == Capabilities::Empty());

  const SparkDsLibrary* read_only = spark::native::internal::Exports<ReadOnly>::Library();
  CHECK(read_only->create_reader != nullptr && read_only->read != nullptr);
  CHECK(read_only->create_stream_reader == nullptr && read_only->create_writer == nullptr);
  const SparkDsLibrary* sink = spark::native::internal::Exports<StreamingSink<true>>::Library();
  CHECK(sink->create_reader == nullptr && sink->read == nullptr);
  CHECK(sink->create_writer != nullptr && sink->create_data_writer != nullptr);
}
