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

#pragma once

#include <cfloat>
#include <charconv>
#include <cmath>
#include <cstdint>
#include <limits>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>

#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/util/decimal.h>
#include <nlohmann/json.hpp>

#include "spark/native/internal/base64.h"

namespace spark::native {

/// A null literal, of any data type.
struct NullLiteral {
  bool operator==(const NullLiteral& other) const = default;
};

/// A binary literal.
struct BinaryLiteral {
  std::vector<uint8_t> bytes;
  bool operator==(const BinaryLiteral& other) const = default;
};

/// A decimal(precision, scale) literal, whose value is unscaled * 10^-scale.
struct DecimalLiteral {
  arrow::Decimal128 unscaled;
  int32_t precision = 0;
  int32_t scale = 0;
  bool operator==(const DecimalLiteral& other) const = default;
};

/// A date literal, as days since 1970-01-01.
struct DateLiteral {
  int32_t days = 0;
  bool operator==(const DateLiteral& other) const = default;
};

/// A timestamp literal, as microseconds since the epoch.
struct TimestampLiteral {
  int64_t micros = 0;
  bool operator==(const TimestampLiteral& other) const = default;
};

/// A timestamp_ntz literal, as microseconds since 1970-01-01 00:00:00 without a time zone.
struct TimestampNtzLiteral {
  int64_t micros = 0;
  bool operator==(const TimestampNtzLiteral& other) const = default;
};

/// The value of a literal, decoded by its Spark SQL data type:
///
/// - bool for a boolean;
/// - int64_t for a tinyint, smallint, int, or bigint, within the range of its type;
/// - double for a float or double, where a float is the exact value of the float;
/// - std::string for a string, which Spark compares by its UTF-8 bytes;
/// - and the structs above for a null of any type, binary, decimal, date, timestamp, and
///   timestamp_ntz.
using LiteralValue =
    std::variant<NullLiteral, bool, int64_t, double, std::string, BinaryLiteral, DecimalLiteral,
                 DateLiteral, TimestampLiteral, TimestampNtzLiteral>;

/// The expression tree of a pushed predicate, as documented by push_predicates in
/// spark_datasource.h.
///
/// Literal values keep their JSON representation, so that a connector can reject unsupported
/// types without lossy numeric conversions: in particular, decimal values are strings and
/// timestamps are integer microseconds. literal_value() decodes them by their data type. Function
/// names are extensible: a predicate with a name that a connector does not know must stay
/// unsupported.
struct Predicate {
  enum class Kind {
    /// A column, with the parts of its name in `column`.
    kColumn,
    /// A literal, with its type in `data_type` and its JSON value in `value`.
    kLiteral,
    /// A function, such as a comparison, with its `name` and its `children`.
    kFunction,
    /// An expression of a type that this SDK does not know, or that it could not parse. A
    /// connector must leave a predicate that contains one unsupported.
    kUnknown,
  };

  Kind kind = Kind::kUnknown;
  /// The name of a column, with a part for each level of a nested field.
  std::vector<std::string> column;
  /// The Spark SQL type name of a literal, such as "int", "string", or "decimal(10,2)".
  std::string data_type;
  /// The value of a literal, as Spark writes it in JSON.
  nlohmann::json value;
  /// The name of a function, such as ">=", "IN", or "AND".
  std::string name;
  /// The arguments of a function.
  std::vector<Predicate> children;

  static Predicate Column(std::vector<std::string> name) {
    Predicate predicate;
    predicate.kind = Kind::kColumn;
    predicate.column = std::move(name);
    return predicate;
  }

  static Predicate Literal(std::string data_type, nlohmann::json value) {
    Predicate predicate;
    predicate.kind = Kind::kLiteral;
    predicate.data_type = std::move(data_type);
    predicate.value = std::move(value);
    return predicate;
  }

  static Predicate Function(std::string name, std::vector<Predicate> children) {
    Predicate predicate;
    predicate.kind = Kind::kFunction;
    predicate.name = std::move(name);
    predicate.children = std::move(children);
    return predicate;
  }

  /// Parses an expression. An expression of a type that this SDK does not know is kUnknown, but a
  /// malformed one, such as a column whose name is not an array of strings, is an error.
  static arrow::Result<Predicate> FromJson(const nlohmann::json& json);

  /// Parses the JSON text of an expression, as FromJson.
  static arrow::Result<Predicate> Parse(std::string_view text) {
    nlohmann::json json = nlohmann::json::parse(text, nullptr, /*allow_exceptions=*/false);
    if (json.is_discarded()) {
      return arrow::Status::Invalid("a predicate is not valid JSON");
    }
    return FromJson(json);
  }

  /// Decodes a literal by its data type. Returns nothing for other expressions, and for data types
  /// or values that this SDK does not decode: keep such predicates unsupported.
  std::optional<LiteralValue> literal_value() const;

  bool operator==(const Predicate& other) const = default;
};

namespace internal {

inline std::optional<int64_t> JsonInt64(const nlohmann::json& value) {
  if (value.is_number_unsigned()) {
    uint64_t number = value.get<uint64_t>();
    if (number > static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
      return std::nullopt;
    }
    return static_cast<int64_t>(number);
  }
  if (value.is_number_integer()) {
    return value.get<int64_t>();
  }
  return std::nullopt;
}

inline std::optional<int64_t> JsonInteger(const nlohmann::json& value, int64_t min, int64_t max) {
  std::optional<int64_t> number = JsonInt64(value);
  if (!number || *number < min || *number > max) {
    return std::nullopt;
  }
  return number;
}

inline std::string_view Trim(std::string_view text) {
  while (!text.empty() && text.front() == ' ') text.remove_prefix(1);
  while (!text.empty() && text.back() == ' ') text.remove_suffix(1);
  return text;
}

template <typename T>
std::optional<T> ParseNumber(std::string_view text) {
  T number;
  auto [end, error] = std::from_chars(text.data(), text.data() + text.size(), number);
  if (error != std::errc() || end != text.data() + text.size()) {
    return std::nullopt;
  }
  return number;
}

/// Decodes a decimal(p,s) literal, which Spark writes as a plain decimal string.
inline std::optional<LiteralValue> DecodeDecimal(std::string_view data_type,
                                                 std::string_view text) {
  constexpr std::string_view kPrefix = "decimal(";
  if (data_type.substr(0, kPrefix.size()) != kPrefix || data_type.empty() ||
      data_type.back() != ')') {
    return std::nullopt;
  }
  std::string_view arguments =
      data_type.substr(kPrefix.size(), data_type.size() - kPrefix.size() - 1);
  size_t comma = arguments.find(',');
  if (comma == std::string_view::npos) {
    return std::nullopt;
  }
  std::optional<uint8_t> precision = ParseNumber<uint8_t>(Trim(arguments.substr(0, comma)));
  std::optional<int8_t> scale = ParseNumber<int8_t>(Trim(arguments.substr(comma + 1)));
  if (!precision || !scale || *precision < 1 || *precision > 38 || *scale > *precision) {
    return std::nullopt;
  }
  bool negative = !text.empty() && text.front() == '-';
  std::string_view digits = negative ? text.substr(1) : text;
  std::string_view integer = digits;
  std::string_view fraction;
  if (size_t point = digits.find('.'); point != std::string_view::npos) {
    integer = digits.substr(0, point);
    fraction = digits.substr(point + 1);
    if (fraction.empty()) {
      return std::nullopt;
    }
  }
  auto all_digits = [](std::string_view part) {
    for (char c : part) {
      if (c < '0' || c > '9') return false;
    }
    return true;
  };
  if (integer.empty() || !all_digits(integer) || !all_digits(fraction)) {
    return std::nullopt;
  }
  std::string unscaled = std::string(integer) + std::string(fraction);
  // The digits have fraction.size() decimal places: rescale them to `scale` exactly.
  int64_t shift = int64_t{*scale} - static_cast<int64_t>(fraction.size());
  if (shift > 38 || shift < -38) {
    return std::nullopt;
  }
  if (shift >= 0) {
    unscaled.append(static_cast<size_t>(shift), '0');
  } else {
    size_t dropped = static_cast<size_t>(-shift);
    size_t kept = unscaled.size() > dropped ? unscaled.size() - dropped : 0;
    if (unscaled.find_first_not_of('0', kept) != std::string::npos) {
      return std::nullopt;
    }
    unscaled.resize(kept);
  }
  size_t first = unscaled.find_first_not_of('0');
  unscaled = first == std::string::npos ? "" : unscaled.substr(first);
  // The value must be below 10^precision, which also keeps it within 128 bits.
  if (unscaled.size() > *precision) {
    return std::nullopt;
  }
  arrow::Decimal128 value(0);
  for (char digit : unscaled) {
    value *= arrow::Decimal128(10);
    value += arrow::Decimal128(digit - '0');
  }
  if (negative) {
    value.Negate();
  }
  return DecimalLiteral{value, *precision, *scale};
}

}  // namespace internal

inline arrow::Result<Predicate> Predicate::FromJson(const nlohmann::json& json) {
  if (!json.is_object()) {
    return arrow::Status::Invalid("a predicate expression must be a JSON object");
  }
  auto type = json.find("type");
  if (type == json.end() || !type->is_string()) {
    return arrow::Status::Invalid("a predicate expression must have a string type");
  }
  const auto& kind = type->get_ref<const std::string&>();
  if (kind == "column") {
    auto name = json.find("name");
    if (name == json.end() || !name->is_array()) {
      return arrow::Status::Invalid("the name of a column must be an array of its parts");
    }
    std::vector<std::string> parts;
    for (const auto& part : *name) {
      if (!part.is_string()) {
        return arrow::Status::Invalid("the name of a column must be an array of strings");
      }
      parts.push_back(part.get<std::string>());
    }
    return Column(std::move(parts));
  }
  if (kind == "literal") {
    auto data_type = json.find("dataType");
    auto value = json.find("value");
    if (data_type == json.end() || !data_type->is_string() || value == json.end()) {
      return arrow::Status::Invalid("a literal must have a string dataType and a value");
    }
    return Literal(data_type->get<std::string>(), *value);
  }
  if (kind == "function") {
    auto name = json.find("name");
    auto children = json.find("children");
    if (name == json.end() || !name->is_string() || children == json.end() ||
        !children->is_array()) {
      return arrow::Status::Invalid("a function must have a string name and an array of children");
    }
    std::vector<Predicate> arguments;
    for (const auto& child : *children) {
      ARROW_ASSIGN_OR_RAISE(Predicate argument, FromJson(child));
      arguments.push_back(std::move(argument));
    }
    return Function(name->get<std::string>(), std::move(arguments));
  }
  return Predicate();
}

inline std::optional<LiteralValue> Predicate::literal_value() const {
  if (kind != Kind::kLiteral) {
    return std::nullopt;
  }
  if (value.is_null()) {
    return NullLiteral{};
  }
  auto integer = [this](int64_t min, int64_t max) -> std::optional<LiteralValue> {
    std::optional<int64_t> number = internal::JsonInteger(value, min, max);
    if (!number) return std::nullopt;
    return *number;
  };
  if (data_type == "boolean") {
    if (!value.is_boolean()) return std::nullopt;
    return value.get<bool>();
  }
  if (data_type == "tinyint") return integer(INT8_MIN, INT8_MAX);
  if (data_type == "smallint") return integer(INT16_MIN, INT16_MAX);
  if (data_type == "int") return integer(INT32_MIN, INT32_MAX);
  if (data_type == "bigint") return integer(INT64_MIN, INT64_MAX);
  if (data_type == "float" || data_type == "double") {
    if (!value.is_number()) return std::nullopt;
    double number = value.get<double>();
    // Spark writes a float as the double of the same value.
    if (data_type == "float" && (std::fabs(number) > FLT_MAX ||
                                 static_cast<double>(static_cast<float>(number)) != number)) {
      return std::nullopt;
    }
    return number;
  }
  if (data_type == "string") {
    if (!value.is_string()) return std::nullopt;
    return value.get<std::string>();
  }
  if (data_type == "binary") {
    if (!value.is_string()) return std::nullopt;
    std::optional<std::string> bytes = internal::Base64Decode(value.get_ref<const std::string&>());
    if (!bytes) return std::nullopt;
    return BinaryLiteral{std::vector<uint8_t>(bytes->begin(), bytes->end())};
  }
  if (data_type == "date") {
    std::optional<int64_t> days = internal::JsonInteger(value, INT32_MIN, INT32_MAX);
    if (!days) return std::nullopt;
    return DateLiteral{static_cast<int32_t>(*days)};
  }
  if (data_type == "timestamp" || data_type == "timestamp_ntz") {
    std::optional<int64_t> micros = internal::JsonInt64(value);
    if (!micros) return std::nullopt;
    if (data_type == "timestamp") return TimestampLiteral{*micros};
    return TimestampNtzLiteral{*micros};
  }
  if (!value.is_string()) {
    return std::nullopt;
  }
  return internal::DecodeDecimal(data_type, value.get_ref<const std::string&>());
}

}  // namespace spark::native
