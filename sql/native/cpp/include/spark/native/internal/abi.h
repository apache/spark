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

// Adapters between the C conventions of spark_datasource.h and the C++ API: errors, sized
// structures, strings, byte arrays, and sinks.

#pragma once

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <exception>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <arrow/c/bridge.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/type.h>

#include "spark/native/data_source.h"
#include "spark_datasource.h"

namespace spark::native::internal {

/// Calls connector code, and reports an exception that it throws as the error of the call. The
/// action returns an arrow::Status or an arrow::Result.
template <typename F>
auto Guard(F&& action) noexcept -> decltype(action()) {
  try {
    return action();
  } catch (const std::exception& error) {
    return arrow::Status::UnknownError("native provider threw an exception: ", error.what());
  } catch (...) {
    return arrow::Status::UnknownError(
        "native provider threw an exception that is not a std::exception");
  }
}

/// The message of an error as Spark reports it, with the detail of an Arrow error, such as the
/// errno of an I/O error, but without the name of its code. It is a C string, which a NUL would
/// truncate.
inline std::string ErrorMessage(const arrow::Status& status) {
  std::string message = status.message();
  if (status.detail() != nullptr) {
    message += ". Detail: " + status.detail()->ToString();
  }
  std::string text;
  text.reserve(message.size());
  for (char c : message) {
    if (c == '\0') {
      text += "\\0";
    } else {
      text += c;
    }
  }
  return text;
}

// The message of the last error on each thread, which last_error returns until the next call on
// that thread.
inline thread_local std::optional<std::string> last_error;

/// Returns the message of the last error on the calling thread, or NULL if the last call
/// succeeded. The message stays valid until the next call on that thread.
inline const char* LastError() noexcept { return last_error ? last_error->c_str() : nullptr; }

/// Converts the result of a callback into the C error convention: 0 on success, or 1 after setting
/// the message that last_error returns.
template <typename F>
int Call(F&& action) noexcept {
  last_error.reset();
  arrow::Status status = Guard(std::forward<F>(action));
  if (status.ok()) {
    return 0;
  }
  try {
    last_error = ErrorMessage(status);
  } catch (...) {
    last_error.reset();
  }
  return 1;
}

inline arrow::Status RequireOutput(const void* pointer) {
  if (pointer == nullptr) {
    return arrow::Status::Invalid("null output pointer");
  }
  return arrow::Status::OK();
}

/// The size of a structure that starts with its size, as the structures of the ABI do.
inline arrow::Result<int64_t> StructSize(const void* pointer) {
  if (pointer == nullptr) {
    return arrow::Status::Invalid("null sized structure");
  }
  int64_t size;
  std::memcpy(&size, pointer, sizeof(size));
  if (size < static_cast<int64_t>(sizeof(int64_t))) {
    return arrow::Status::Invalid("structure is smaller than its size field");
  }
  return size;
}

/// Reads a field of a sized structure, only if it fits in the size of the structure.
template <typename T>
std::optional<T> FieldOf(const void* pointer, int64_t size, size_t offset) {
  if (size < 0 || static_cast<uint64_t>(size) < offset + sizeof(T)) {
    return std::nullopt;
  }
  T value;
  std::memcpy(&value, static_cast<const char*>(pointer) + offset, sizeof(T));
  return value;
}

inline arrow::Result<std::string> String(const char* pointer) {
  if (pointer == nullptr) {
    return arrow::Status::Invalid("null string");
  }
  return std::string(pointer);
}

inline arrow::Status CheckArray(const void* data, int64_t length, size_t element_size) {
  if (length < 0 || (length > 0 && data == nullptr) ||
      static_cast<uint64_t>(length) > static_cast<uint64_t>(PTRDIFF_MAX) / element_size) {
    return arrow::Status::Invalid("invalid array pointer or length");
  }
  return arrow::Status::OK();
}

inline arrow::Result<std::vector<std::string>> Strings(const char* const* data, int64_t length) {
  ARROW_RETURN_NOT_OK(CheckArray(data, length, sizeof(const char*)));
  std::vector<std::string> strings;
  strings.reserve(static_cast<size_t>(length));
  for (int64_t index = 0; index < length; ++index) {
    ARROW_ASSIGN_OR_RAISE(std::string string, String(data[index]));
    strings.push_back(std::move(string));
  }
  return strings;
}

/// The bytes that Spark passes, valid during the call.
inline arrow::Result<std::string_view> Bytes(const uint8_t* data, int64_t length) {
  ARROW_RETURN_NOT_OK(CheckArray(data, length, 1));
  if (length == 0) {
    return std::string_view();
  }
  return std::string_view(reinterpret_cast<const char*>(data), static_cast<size_t>(length));
}

/// The commit messages of the tasks of a write. Nothing stands for a task that did not commit.
inline arrow::Result<std::vector<std::optional<std::string_view>>> Messages(
    const uint8_t* const* pointers, const int64_t* lengths, int64_t count) {
  ARROW_RETURN_NOT_OK(CheckArray(pointers, count, sizeof(const uint8_t*)));
  ARROW_RETURN_NOT_OK(CheckArray(lengths, count, sizeof(int64_t)));
  std::vector<std::optional<std::string_view>> messages;
  messages.reserve(static_cast<size_t>(count));
  for (int64_t index = 0; index < count; ++index) {
    if (pointers[index] == nullptr) {
      if (lengths[index] != 0) {
        return arrow::Status::Invalid("null commit message must have zero length");
      }
      messages.emplace_back(std::nullopt);
    } else {
      ARROW_ASSIGN_OR_RAISE(std::string_view message, Bytes(pointers[index], lengths[index]));
      messages.emplace_back(message);
    }
  }
  return messages;
}

/// Passes a string or a byte array to a sink, which copies it during the call.
inline arrow::Status Emit(SparkDsSink sink, void* data, std::string_view bytes) {
  if (sink == nullptr) {
    return arrow::Status::Invalid("null result sink");
  }
  sink(data, reinterpret_cast<const uint8_t*>(bytes.data()), static_cast<int64_t>(bytes.size()));
  return arrow::Status::OK();
}

/// Moves a schema that Spark passes. Arrow releases the input struct, even on failure.
inline arrow::Result<std::shared_ptr<arrow::Schema>> ImportSchema(struct ArrowSchema* schema) {
  ARROW_RETURN_NOT_OK(RequireOutput(schema));
  return arrow::ImportSchema(schema);
}

inline arrow::Result<CreateInfo> ReadCreateInfo(const SparkDsCreateInfo* info) {
  ARROW_ASSIGN_OR_RAISE(int64_t size, StructSize(info));
  auto name = FieldOf<const char*>(info, size, offsetof(SparkDsCreateInfo, name));
  if (!name) {
    return arrow::Status::Invalid("create info does not provide a name");
  }
  if (*name == nullptr) {
    return arrow::Status::Invalid("create info has a null name");
  }
  auto keys = FieldOf<const char* const*>(info, size, offsetof(SparkDsCreateInfo, option_keys));
  auto values = FieldOf<const char* const*>(info, size, offsetof(SparkDsCreateInfo, option_values));
  auto count = FieldOf<int64_t>(info, size, offsetof(SparkDsCreateInfo, num_options));
  if (!keys || !values || !count) {
    return arrow::Status::Invalid("create info does not provide its options");
  }
  ARROW_ASSIGN_OR_RAISE(std::vector<std::string> option_keys, Strings(*keys, *count));
  ARROW_ASSIGN_OR_RAISE(std::vector<std::string> option_values, Strings(*values, *count));
  Options options;
  for (size_t index = 0; index < option_keys.size(); ++index) {
    options[std::move(option_keys[index])] = std::move(option_values[index]);
  }
  return CreateInfo(std::string(*name), std::move(options));
}

/// The mode of a write, and the ID of the query of a streaming write.
struct WriteRequest {
  WriteMode mode;
  std::optional<std::string> query_id;
};

inline arrow::Result<WriteRequest> ReadWriteInfo(const SparkDsWriteInfo* info) {
  ARROW_ASSIGN_OR_RAISE(int64_t size, StructSize(info));
  auto mode = FieldOf<int32_t>(info, size, offsetof(SparkDsWriteInfo, mode));
  if (!mode) {
    return arrow::Status::Invalid("write info does not provide a mode");
  }
  auto query_id = FieldOf<const char*>(info, size, offsetof(SparkDsWriteInfo, query_id));
  if (!query_id) {
    return arrow::Status::Invalid("write info does not provide a query ID");
  }
  WriteRequest request{WriteMode::kAppend, std::nullopt};
  switch (*mode) {
    case SPARK_DS_WRITE_APPEND:
      request.mode = WriteMode::kAppend;
      break;
    case SPARK_DS_WRITE_TRUNCATE:
      request.mode = WriteMode::kTruncate;
      break;
    default:
      return arrow::Status::Invalid("unsupported native write mode: ", *mode);
  }
  if (*query_id != nullptr) {
    request.query_id = std::string(*query_id);
  }
  return request;
}

inline int32_t PredicateUse(FilterSupport support) {
  switch (support) {
    case FilterSupport::kExact:
      return SPARK_DS_PREDICATE_COMPLETE;
    case FilterSupport::kPruningOnly:
      return SPARK_DS_PREDICATE_PARTIAL;
    case FilterSupport::kUnsupported:
      break;
  }
  return SPARK_DS_PREDICATE_UNUSED;
}

/// Parses each predicate on its own. One that does not parse, for example because a later Spark
/// encodes it differently, stays unused instead of failing the scan.
inline std::vector<std::optional<Predicate>> ParsePredicates(
    const std::vector<std::string>& predicates) {
  std::vector<std::optional<Predicate>> parsed;
  parsed.reserve(predicates.size());
  for (const std::string& text : predicates) {
    arrow::Result<Predicate> predicate = Predicate::Parse(text);
    if (predicate.ok()) {
      parsed.emplace_back(std::move(predicate).ValueUnsafe());
    } else {
      LogWarn("Leaving unused a predicate that cannot be parsed (" + predicate.status().message() +
              "): " + text);
      parsed.emplace_back(std::nullopt);
    }
  }
  return parsed;
}

}  // namespace spark::native::internal
