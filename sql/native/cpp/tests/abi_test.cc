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

#include <stdexcept>
#include <thread>

#include <arrow/api.h>

#include "spark/native/data_source.h"
#include "test_util.h"

namespace internal = spark::native::internal;

TEST(ErrorsAndExceptionsAreThreadLocalCStrings) {
  CHECK(internal::Call(
            [] { return arrow::Status::Invalid(std::string("failed\0operation", 16)); }) != 0);
  std::string error = internal::LastError();
  CHECK_EQ(error, std::string("failed\\0operation"));
  // Another thread has errors of its own. Checks run after the thread, which cannot throw them.
  bool cleared = false;
  std::string thrown;
  std::string unknown;
  std::thread([&] {
    cleared = internal::LastError() == nullptr;
    if (internal::Call([]() -> arrow::Status { throw std::runtime_error("provider failure"); })) {
      thrown = internal::LastError();
    }
    if (internal::Call([]() -> arrow::Status { throw 42; })) {
      unknown = internal::LastError();
    }
  }).join();
  CHECK(cleared);
  CHECK_EQ(thrown, std::string("native provider threw an exception: provider failure"));
  CHECK_CONTAINS(unknown, "not a std::exception");
  CHECK_EQ(std::string(internal::LastError()), error);
  CHECK(internal::Call([] { return arrow::Status::OK(); }) == 0);
  CHECK(internal::LastError() == nullptr);
}

namespace {

class ErrnoDetail : public arrow::StatusDetail {
 public:
  const char* type_id() const override { return "test-errno"; }
  std::string ToString() const override { return "[errno 2] No such file or directory"; }
};

}  // namespace

TEST(ErrorMessagesKeepTheDetailOfArrowErrors) {
  arrow::Status status =
      arrow::Status::IOError("cannot open the table").WithDetail(std::make_shared<ErrnoDetail>());
  CHECK_EQ(internal::ErrorMessage(status),
           std::string("cannot open the table. Detail: [errno 2] No such file or directory"));
  CHECK_EQ(internal::ErrorMessage(arrow::Status::Invalid("plain")), std::string("plain"));
}

TEST(EmptyArraysAcceptNullAndTruncatedStructuresFail) {
  ASSIGN_OR_FAIL(std::vector<std::string> strings, internal::Strings(nullptr, 0));
  CHECK(strings.empty());
  CHECK(!internal::Strings(nullptr, 1).ok());
  CHECK(!internal::Strings(nullptr, -1).ok());
  int64_t prefix = sizeof(int64_t);
  arrow::Result<spark::native::CreateInfo> truncated =
      internal::ReadCreateInfo(reinterpret_cast<const SparkDsCreateInfo*>(&prefix));
  CHECK(!truncated.ok());
  CHECK_EQ(truncated.status().message(),
           std::string("SparkDsCreateInfo has 8 bytes, fewer than in version 1 of the native ABI"));
  CHECK(!internal::Input<SparkDsReadInfo>(nullptr, "SparkDsReadInfo").ok());
}

TEST(CreateInfoCarriesTheSourceNameAndOptions) {
  const char* keys[] = {"path"};
  const char* values[] = {"/tmp/data"};
  SparkDsCreateInfo info{sizeof(SparkDsCreateInfo), "my_source", keys, values, 1};
  ASSIGN_OR_FAIL(spark::native::CreateInfo created, internal::ReadCreateInfo(&info));
  CHECK_EQ(created.name, std::string("my_source"));
  CHECK(created.options == spark::native::Options({{"path", "/tmp/data"}}));
  info.name = nullptr;
  CHECK(!internal::ReadCreateInfo(&info).ok());
}

TEST(WriteInfoCarriesTheModeAndTheQueryOfAStreamingWrite) {
  int64_t prefix = sizeof(int64_t);
  CHECK(!internal::ReadWriteInfo(reinterpret_cast<const SparkDsWriteInfo*>(&prefix)).ok());
  SparkDsWriteInfo info{sizeof(SparkDsWriteInfo), SPARK_DS_WRITE_APPEND, nullptr};
  ASSIGN_OR_FAIL(internal::WriteRequest append, internal::ReadWriteInfo(&info));
  CHECK(append.mode == spark::native::WriteMode::kAppend);
  CHECK(!append.query_id.has_value());
  info.mode = SPARK_DS_WRITE_TRUNCATE;
  info.query_id = "stream";
  ASSIGN_OR_FAIL(internal::WriteRequest truncate, internal::ReadWriteInfo(&info));
  CHECK(truncate.mode == spark::native::WriteMode::kTruncate);
  CHECK(truncate.query_id == std::optional<std::string>("stream"));
  info.mode = 2;
  CHECK(!internal::ReadWriteInfo(&info).ok());
}

TEST(AbortMessagesDistinguishMissingAndEmptyMessages) {
  const uint8_t bytes[] = {7, 8};
  const uint8_t* pointers[] = {nullptr, bytes, bytes};
  const int64_t lengths[] = {0, 0, 2};
  ASSIGN_OR_FAIL(auto messages, internal::Messages(pointers, lengths, 3));
  CHECK(messages.size() == 3);
  CHECK(!messages[0].has_value());
  CHECK(messages[1] == std::optional<std::string_view>(""));
  CHECK(messages[2] == std::optional<std::string_view>("\x07\x08"));
  ASSIGN_OR_FAIL(auto empty, internal::Messages(nullptr, nullptr, 0));
  CHECK(empty.empty());
  CHECK(!internal::Messages(nullptr, nullptr, -1).ok());
  const int64_t null_with_length[] = {1, 0, 2};
  CHECK(!internal::Messages(pointers, null_with_length, 3).ok());
  const int64_t negative[] = {0, -1, 2};
  CHECK(!internal::Messages(pointers, negative, 3).ok());
}

TEST(EncodedValuesRoundTripAndRejectAnotherWireVersion) {
  ASSIGN_OR_FAIL(std::string encoded, internal::Encode(std::vector<int64_t>{2, 7}));
  ASSIGN_OR_FAIL(std::vector<int64_t> decoded, internal::Decode<std::vector<int64_t>>(encoded));
  CHECK(decoded == std::vector<int64_t>({2, 7}));
  std::string other =
      "{\"version\":" + std::to_string(internal::kStateVersion + 1) + ",\"value\":42}";
  arrow::Result<int64_t> rejected = internal::Decode<int64_t>(other);
  CHECK(!rejected.ok());
  CHECK_CONTAINS(rejected.status().message(), "unsupported native state version");
  CHECK(!internal::Decode<int64_t>("not json").ok());
  std::string text =
      "{\"version\":" + std::to_string(internal::kStateVersion) + ",\"value\":\"text\"}";
  CHECK(!internal::Decode<int64_t>(text).ok());
}

TEST(LatestOffsetInfoCarriesTheStartTheLimitAndTheEndOfTheRun) {
  SparkDsReadLimit limit{sizeof(SparkDsReadLimit), SPARK_DS_READ_ALL_AVAILABLE, 0};
  SparkDsLatestOffsetInfo info{sizeof(SparkDsLatestOffsetInfo), R"({"next":3})", &limit, nullptr};
  ASSIGN_OR_FAIL(auto request, internal::ReadLatestOffsetInfo(&info));
  CHECK_EQ(request.start, std::string(R"({"next":3})"));
  CHECK(request.limit == spark::native::ReadLimit::AllAvailable());
  CHECK(!request.run_end.has_value());

  limit.kind = SPARK_DS_READ_MAX_ROWS;
  limit.max_rows = 5;
  info.run_end = R"({"next":9})";
  ASSIGN_OR_FAIL(auto limited, internal::ReadLatestOffsetInfo(&info));
  CHECK(limited.limit == spark::native::ReadLimit::MaxRows(5));
  CHECK(limited.run_end == std::optional<std::string>(R"({"next":9})"));

  limit.kind = 7;
  arrow::Result<spark::native::LatestOffsetInfo<std::string>> unknown =
      internal::ReadLatestOffsetInfo(&info);
  CHECK(!unknown.ok());
  CHECK_CONTAINS(unknown.status().message(), "unsupported kind of read limit: 7");
}

TEST(DefaultReadLimitsSetTheKindAndTheRows) {
  SparkDsReadLimit limit{sizeof(SparkDsReadLimit), SPARK_DS_READ_ALL_AVAILABLE, 0};
  CHECK_OK(internal::SetReadLimit(&limit, spark::native::ReadLimit::MaxRows(4)));
  CHECK_EQ(limit.kind, SPARK_DS_READ_MAX_ROWS);
  CHECK_EQ(limit.max_rows, int64_t{4});
  CHECK_OK(internal::SetReadLimit(&limit, spark::native::ReadLimit::AllAvailable()));
  CHECK_EQ(limit.kind, SPARK_DS_READ_ALL_AVAILABLE);
  limit.size = sizeof(int64_t);
  CHECK(!internal::SetReadLimit(&limit, spark::native::ReadLimit::AllAvailable()).ok());
}

TEST(OffsetsAreCompactJsonOnOneLine) {
  nlohmann::json offset = {{"position", 3}, {"note", "two\nlines"}};
  ASSIGN_OR_FAIL(std::string json, internal::EncodeOffset(offset));
  CHECK_EQ(json, std::string(R"({"note":"two\nlines","position":3})"));
  ASSIGN_OR_FAIL(nlohmann::json decoded, internal::DecodeOffset<nlohmann::json>(json));
  CHECK(decoded == offset);
  arrow::Result<int64_t> invalid = internal::DecodeOffset<int64_t>("not an offset");
  CHECK(!invalid.ok());
  CHECK_CONTAINS(invalid.status().message(), "cannot decode the offset not an offset");
}

TEST(EmitRequiresASink) {
  std::vector<std::string> values;
  CHECK_OK(internal::Emit(&spark::native::testing::Collect, &values, "value"));
  CHECK(values == std::vector<std::string>({"value"}));
  CHECK(!internal::Emit(nullptr, &values, "value").ok());
}
