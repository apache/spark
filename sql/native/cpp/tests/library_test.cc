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

// The initialization of a library, its logging, and the capabilities of its sources, through the
// function table that it exports.

#include <atomic>
#include <mutex>
#include <stdexcept>

#include <arrow/api.h>

#include "spark/native/data_source.h"
#include "test_util.h"

namespace sn = spark::native;
using spark::native::Capabilities;
using spark::native::testing::CreateArguments;
using spark::native::testing::LastError;

namespace {

std::atomic<int> declarations{0};
std::atomic<int> schemas{0};
std::atomic<int> scans{0};
std::atomic<int> destroyed{0};
std::mutex mutex;
std::vector<std::optional<std::string>> initialized;
std::vector<std::pair<int32_t, std::string>> logs;

void RecordLog(int32_t level, const char* message) {
  std::lock_guard<std::mutex> lock(mutex);
  logs.emplace_back(level, message);
}

struct Factory : sn::PartitionReaderFactory<int64_t> {
  int64_t unused = 0;

  arrow::Result<std::shared_ptr<arrow::RecordBatchReader>> Open(
      const int64_t&, const sn::ReaderContext&) const override {
    return arrow::Status::Invalid("capability tests do not open partition readers");
  }
};
NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE(Factory, unused)

struct Builder : sn::ScanBuilder<Factory> {
  arrow::Result<sn::ScanPlan<Factory>> Plan() override {
    return sn::ScanPlan<Factory>(Factory(), {});
  }
};

class Source : public sn::DataSource {
 public:
  using ScanBuilder = Builder;

  explicit Source(std::string mode) : mode_(std::move(mode)) {}
  Source(Source&& other) noexcept
      : DataSource(std::move(other)),
        mode_(std::move(other.mode_)),
        live_(std::exchange(other.live_, false)) {}
  ~Source() override {
    if (live_) {
      ++destroyed;
    }
  }

  static arrow::Status Init(const sn::HostInfo& host) {
    {
      std::lock_guard<std::mutex> lock(mutex);
      initialized.push_back(host.spark_version);
    }
    if (host.spark_version == "unsupported") {
      return arrow::Status::Invalid("this library does not support the Spark version");
    }
    sn::LogInfo("capability test library initialized");
    return arrow::Status::OK();
  }

  static arrow::Result<Source> Make(const sn::CreateInfo& info) {
    if (info.name != "capability_test") {
      return arrow::Status::Invalid("unexpected source name ", info.name);
    }
    return Source(info.options.at("capabilities"));
  }

  Capabilities capabilities() const override {
    ++declarations;
    if (mode_ == "empty") return Capabilities::Empty();
    if (mode_ == "read") return Capabilities::kBatchRead;
    if (mode_ == "without_read") return Capabilities::All().Without(Capabilities::kBatchRead);
    // Operations that the source does not implement are dropped.
    if (mode_ == "all") return Capabilities::All();
    throw std::runtime_error("capability discovery failed");
  }

  arrow::Result<std::shared_ptr<arrow::Schema>> InferSchema() const override {
    ++schemas;
    return arrow::schema(arrow::FieldVector{});
  }

  arrow::Result<Builder> NewScan(const sn::ScanInfo&) const {
    ++scans;
    return Builder();
  }

 private:
  std::string mode_;
  bool live_ = true;
};

struct Created {
  int code;
  uint64_t mask;
  void* source;
};

Created Create(const SparkDsLibrary& library, const std::string& name, const std::string& mode) {
  CreateArguments arguments(name, {{"capabilities", mode}});
  Created created{0, 0, nullptr};
  created.code = library.create(arguments.info(), &created.mask, &created.source);
  return created;
}

}  // namespace

SPARK_EXPORT_DATA_SOURCE(Source);

TEST(InitializationLoggingAndCapabilitiesFollowTheSourceLifecycle) {
  const SparkDsLibrary* table = nullptr;
  CHECK(spark_ds_init(sn::testing::LeakedHost(&RecordLog, "unsupported"), &table) != 0);
  CHECK(table == nullptr);
  {
    std::lock_guard<std::mutex> lock(mutex);
    CHECK(logs == decltype(logs)({{1, "this library does not support the Spark version"}}));
  }
  CHECK(spark_ds_init(sn::testing::LeakedHost(&RecordLog, "4.4.0-test"), &table) == 0);
  CHECK(table != nullptr);
  const SparkDsLibrary& library = *table;
  CHECK(library.size == sizeof(SparkDsLibrary));
  CHECK(library.abi_version == SPARK_DS_ABI_VERSION);
  {
    std::lock_guard<std::mutex> lock(mutex);
    CHECK(initialized == decltype(initialized)({"unsupported", "4.4.0-test"}));
  }
  sn::LogWarn(std::string("a message with a \0 byte", 23));
  // The default level drops debug messages, until the library raises it.
  CHECK(!sn::IsLogEnabled(sn::LogLevel::kDebug));
  sn::LogDebug("a dropped debug message");
  sn::SetMaxLogLevel(sn::LogLevel::kDebug);
  sn::LogDebug("a debug message");
  {
    std::lock_guard<std::mutex> lock(mutex);
    CHECK(std::vector(logs.begin() + 1, logs.end()) ==
          decltype(logs)({{3, "capability test library initialized"},
                          {2, "a message with a \\0 byte"},
                          {4, "a debug message"}}));
  }

  // Analysis can ask only for capabilities, then close without schema or scan discovery.
  Created probe = Create(library, "capability_test", "all");
  CHECK_SUCCESS(library, probe.code);
  CHECK_EQ(probe.mask, uint64_t{SPARK_DS_BATCH_READ});
  CHECK(probe.source != nullptr);
  library.close(probe.source);
  CHECK_EQ(declarations.load(), 1);
  CHECK_EQ(schemas.load(), 0);
  CHECK_EQ(scans.load(), 0);
  CHECK_EQ(destroyed.load(), 1);

  // Keep all instances alive together: the last declaration must not affect earlier sources.
  std::vector<std::pair<uint64_t, void*>> sources;
  for (const auto& [mode, expected] : std::vector<std::pair<std::string, uint64_t>>{
           {"empty", 0}, {"read", 1}, {"all", 1}, {"without_read", 0}}) {
    Created created = Create(library, "capability_test", mode);
    CHECK_SUCCESS(library, created.code);
    CHECK_EQ(created.mask, expected);
    CHECK(created.source != nullptr);
    sources.emplace_back(expected, created.source);
  }
  CHECK_EQ(declarations.load(), 5);

  for (const auto& [mask, source] : sources) {
    struct ArrowSchema schema{};
    CHECK_SUCCESS(library, library.schema(source, &schema));
    int scans_before = scans.load();
    void* reader = nullptr;
    int code = library.create_reader(source, &schema, &reader);
    if (schema.release != nullptr) {
      schema.release(&schema);
    }
    if (mask == 0) {
      CHECK(code != 0);
      CHECK_EQ(LastError(library), std::string("source does not support batch reads"));
      CHECK(reader == nullptr);
      CHECK_EQ(scans.load(), scans_before);
    } else {
      CHECK_SUCCESS(library, code);
      CHECK(reader != nullptr);
      CHECK_EQ(scans.load(), scans_before + 1);
      library.close_reader(reader);
    }
    CHECK_EQ(declarations.load(), 5);
    library.close(source);
  }
  CHECK_EQ(schemas.load(), 4);
  CHECK_EQ(scans.load(), 2);
  CHECK_EQ(destroyed.load(), 5);

  // The source receives the name that Spark passes, so it can reject other names.
  Created other = Create(library, "other_source", "all");
  CHECK(other.code != 0);
  CHECK_EQ(LastError(library), std::string("unexpected source name other_source"));
  CHECK_EQ(other.mask, uint64_t{0});
  CHECK(other.source == nullptr);

  Created failing = Create(library, "capability_test", "throw");
  CHECK(failing.code != 0);
  CHECK_EQ(LastError(library),
           std::string("native provider threw an exception: capability discovery failed"));
  CHECK_EQ(failing.mask, uint64_t{0});
  CHECK(failing.source == nullptr);
  CHECK_EQ(destroyed.load(), 6);
  CHECK_EQ(schemas.load(), 4);
  CHECK_EQ(scans.load(), 2);
}
