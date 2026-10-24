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

#include "spark/native/data_source.h"
#include "test_util.h"

using spark::native::BinaryLiteral;
using spark::native::DateLiteral;
using spark::native::DecimalLiteral;
using spark::native::LiteralValue;
using spark::native::NullLiteral;
using spark::native::Predicate;
using spark::native::TimestampLiteral;
using spark::native::TimestampNtzLiteral;

namespace {

std::optional<LiteralValue> Literal(const std::string& data_type, nlohmann::json value) {
  return Predicate::Literal(data_type, std::move(value)).literal_value();
}

std::optional<LiteralValue> Some(LiteralValue value) { return value; }

std::optional<LiteralValue> Decimal(int64_t unscaled, int32_t precision, int32_t scale) {
  return DecimalLiteral{arrow::Decimal128(unscaled), precision, scale};
}

}  // namespace

TEST(PreservesInt64LiteralsAndNestedColumnPaths) {
  ASSIGN_OR_FAIL(Predicate predicate, Predicate::Parse(R"({"type":"function","name":">=",
      "children":[{"type":"column","name":["outer","id"]},
                  {"type":"literal","dataType":"bigint","value":9223372036854775807}]})"));
  CHECK(predicate.kind == Predicate::Kind::kFunction);
  CHECK_EQ(predicate.name, std::string(">="));
  CHECK(predicate.children.size() == 2);
  CHECK(predicate.children[0] == Predicate::Column({"outer", "id"}));
  CHECK(predicate.children[1].literal_value() == Some(std::numeric_limits<int64_t>::max()));
  CHECK(!predicate.children[0].literal_value().has_value());
}

TEST(PreservesUnknownExpressionsForConservativeFallback) {
  ASSIGN_OR_FAIL(Predicate unknown, Predicate::Parse(R"({"type":"future_expression","value":1})"));
  CHECK(unknown.kind == Predicate::Kind::kUnknown);
  ASSIGN_OR_FAIL(Predicate function,
                 Predicate::Parse(R"({"type":"function","name":"FUTURE_FUNCTION","children":[]})"));
  CHECK(function.kind == Predicate::Kind::kFunction);
  CHECK_EQ(function.name, std::string("FUTURE_FUNCTION"));
}

TEST(MalformedExpressionsDoNotParse) {
  for (const char* text : {
           R"({"type":"function","name":"IS_NULL","children":[{"type":"column","name":"id"}]})",
           R"({"type":"column","name":["a",1]})",
           R"({"type":"literal","dataType":"int"})",
           R"({"type":"function","name":"NOT"})",
           R"({"name":"id"})",
           R"([1])",
           "not json",
       }) {
    CHECK(!Predicate::Parse(text).ok());
  }
  CHECK(Predicate::Parse(
            R"({"type":"function","name":"IS_NULL","children":[{"type":"column","name":["id"]}]})")
            .ok());
}

TEST(DecodesLiteralsByTheirDataTypeAndRange) {
  CHECK(Literal("int", nullptr) == Some(NullLiteral{}));
  CHECK(Literal("void", nullptr) == Some(NullLiteral{}));
  CHECK(Literal("boolean", true) == Some(true));
  CHECK(Literal("tinyint", -128) == Some(int64_t{-128}));
  CHECK(!Literal("tinyint", 128));
  CHECK(Literal("smallint", 32767) == Some(int64_t{32767}));
  CHECK(!Literal("smallint", 32768));
  CHECK(Literal("int", std::numeric_limits<int32_t>::min()) == Some(int64_t{-(int64_t{1} << 31)}));
  CHECK(!Literal("int", int64_t{1} << 31));
  CHECK(!Literal("bigint", 1.5));
  CHECK(!Literal("bigint", uint64_t{1} << 63));
  CHECK(Literal("double", 1.5) == Some(1.5));
  CHECK(Literal("float", static_cast<double>(0.1f)) == Some(static_cast<double>(0.1f)));
  // 0.1 is not the value of a float.
  CHECK(!Literal("float", 0.1));
  CHECK(!Literal("float", 1e300));
  CHECK(Literal("string", "row-1") == Some(std::string("row-1")));
  CHECK(!Literal("string", 1));
  CHECK(Literal("date", 19000) == Some(DateLiteral{19000}));
  CHECK(Literal("timestamp", -1) == Some(TimestampLiteral{-1}));
  CHECK(Literal("timestamp_ntz", std::numeric_limits<int64_t>::max()) ==
        Some(TimestampNtzLiteral{std::numeric_limits<int64_t>::max()}));
  CHECK(!Literal("interval day", 1));
}

TEST(DecodesBinaryLiteralsAsPaddedBase64) {
  std::vector<std::pair<std::string, std::vector<uint8_t>>> cases = {
      {"", {}},
      {"Zg==", {'f'}},
      {"Zm8=", {'f', 'o'}},
      {"Zm9v", {'f', 'o', 'o'}},
      {"Zm9vYg==", {'f', 'o', 'o', 'b'}},
      {"AP8+/w==", {0x00, 0xff, 0x3e, 0xff}},
  };
  for (const auto& [text, bytes] : cases) {
    CHECK(Literal("binary", text) == Some(BinaryLiteral{bytes}));
    CHECK_EQ(spark::native::internal::Base64Encode(std::string(bytes.begin(), bytes.end())), text);
  }
  for (const char* invalid : {"Zg", "Zg=", "Z===", "Zg==Zg==", "Zm9-", "Zm 9"}) {
    CHECK(!Literal("binary", invalid));
  }
}

TEST(DecodesPlainDecimalStringsAtTheirScale) {
  CHECK(Literal("decimal(10,2)", "123.45") == Decimal(12345, 10, 2));
  CHECK(Literal("decimal(10,2)", "-0.05") == Decimal(-5, 10, 2));
  CHECK(Literal("decimal(10,2)", "1.00") == Decimal(100, 10, 2));
  CHECK(Literal("decimal(10,2)", "1") == Decimal(100, 10, 2));
  CHECK(Literal("decimal(5,0)", "12345") == Decimal(12345, 5, 0));
  CHECK(Literal("decimal(3,-2)", "12300") == Decimal(123, 3, -2));
  ASSIGN_OR_FAIL(arrow::Decimal128 max, arrow::Decimal128::FromString(std::string(38, '9')));
  CHECK(Literal("decimal(38,0)", std::string(38, '9')) == Some(DecimalLiteral{max, 38, 0}));
  for (const auto& [data_type, text] : std::vector<std::pair<std::string, std::string>>{
           {"decimal(4,2)", "123.45"},
           {"decimal(10,2)", "1.234"},
           {"decimal(3,-2)", "12345"},
           {"decimal(10,2)", "1."},
           {"decimal(10,2)", ".5"},
           {"decimal(10,2)", "1e3"},
           {"decimal(10,2)", "+1.00"},
           {"decimal(39,0)", "1"},
           {"decimal(2,3)", "0.001"},
       }) {
    CHECK(!Literal(data_type, text));
  }
  CHECK(!Literal("decimal(10,2)", 1.5));
}
