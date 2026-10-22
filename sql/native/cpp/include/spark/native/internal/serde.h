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

// The serialization of the values that the executors restore, and of offsets.

#pragma once

#include <memory>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>

#include <arrow/buffer.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/dictionary.h>
#include <arrow/ipc/reader.h>
#include <arrow/ipc/writer.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/type.h>
#include <nlohmann/json.hpp>

#include "spark/native/internal/abi.h"
#include "spark/native/internal/base64.h"

namespace spark::native::internal {

// The version of the format of the values that the SDK serializes for the executors: the state of
// readers and writers, partitions, and commit messages. It is an implementation detail of the
// SDK, so the driver and the executors must use the same build of a library.
inline constexpr int kStateVersion = 1;

/// Whether nlohmann::json serializes T in both directions, as the SDK requires of factories,
/// partitions, offsets, and commit messages.
template <typename T>
concept Serializable =
    std::is_default_constructible_v<T> && requires(const T& value, const nlohmann::json& json) {
      nlohmann::json(value);
      json.template get<T>();
    };

template <typename T>
arrow::Result<T> FromJson(const nlohmann::json& json) {
  try {
    return json.get<T>();
  } catch (const std::exception& error) {
    return arrow::Status::Invalid("cannot decode a native value: ", error.what());
  } catch (...) {
    return arrow::Status::Invalid("cannot decode a native value");
  }
}

/// Parses an envelope of a value that the SDK serialized, and checks its version.
inline arrow::Result<nlohmann::json> Unwrap(std::string_view bytes) {
  nlohmann::json envelope = nlohmann::json::parse(bytes, nullptr, /*allow_exceptions=*/false);
  if (envelope.is_discarded() || !envelope.is_object()) {
    return arrow::Status::Invalid("cannot decode a native value: it is not a JSON object");
  }
  auto version = envelope.find("version");
  if (version == envelope.end() || !version->is_number_integer()) {
    return arrow::Status::Invalid("cannot decode a native value without a version");
  }
  if (*version != kStateVersion) {
    return arrow::Status::Invalid("unsupported native state version: ", version->dump());
  }
  return envelope;
}

/// Serializes a partition or a commit message.
template <typename T>
arrow::Result<std::string> Encode(const T& value) {
  return Guard([&]() -> arrow::Result<std::string> {
    nlohmann::json envelope = nlohmann::json::object();
    envelope["version"] = kStateVersion;
    envelope["value"] = value;
    return envelope.dump();
  });
}

template <typename T>
arrow::Result<T> Decode(std::string_view bytes) {
  ARROW_ASSIGN_OR_RAISE(nlohmann::json envelope, Unwrap(bytes));
  auto value = envelope.find("value");
  if (value == envelope.end()) {
    return arrow::Status::Invalid("cannot decode a native value without a value");
  }
  return FromJson<T>(*value);
}

/// The state of a reader or a writer, which the executors restore. Batch and streaming operations
/// share the executor entry points of the ABI, read and create_data_writer, so the state tells
/// them apart.
struct State {
  bool streaming = false;
  std::shared_ptr<arrow::Schema> schema;
  nlohmann::json factory;
};

template <typename F>
arrow::Result<std::string> EncodeState(bool streaming, const arrow::Schema& schema,
                                       const F& factory) {
  ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Buffer> ipc, arrow::ipc::SerializeSchema(schema));
  std::string_view schema_bytes(reinterpret_cast<const char*>(ipc->data()),
                                static_cast<size_t>(ipc->size()));
  return Guard([&]() -> arrow::Result<std::string> {
    nlohmann::json envelope = nlohmann::json::object();
    envelope["version"] = kStateVersion;
    envelope["kind"] = streaming ? "streaming" : "batch";
    envelope["schema"] = Base64Encode(schema_bytes);
    envelope["factory"] = factory;
    return envelope.dump();
  });
}

inline arrow::Result<State> DecodeState(std::string_view bytes) {
  ARROW_ASSIGN_OR_RAISE(nlohmann::json envelope, Unwrap(bytes));
  auto kind = envelope.find("kind");
  auto schema = envelope.find("schema");
  auto factory = envelope.find("factory");
  if (kind == envelope.end() || !kind->is_string() || schema == envelope.end() ||
      !schema->is_string() || factory == envelope.end()) {
    return arrow::Status::Invalid("cannot decode the native state of a reader or a writer");
  }
  State state;
  if (*kind == "batch") {
    state.streaming = false;
  } else if (*kind == "streaming") {
    state.streaming = true;
  } else {
    return arrow::Status::Invalid("unknown kind of native state: ", kind->dump());
  }
  std::optional<std::string> schema_bytes = Base64Decode(schema->get_ref<const std::string&>());
  if (!schema_bytes) {
    return arrow::Status::Invalid("cannot decode the schema of a native state");
  }
  arrow::io::BufferReader reader(arrow::Buffer::FromString(std::move(*schema_bytes)));
  arrow::ipc::DictionaryMemo dictionaries;
  ARROW_ASSIGN_OR_RAISE(state.schema, arrow::ipc::ReadSchema(&reader, &dictionaries));
  state.factory = std::move(*factory);
  return state;
}

/// Offsets are compact JSON on a single line, which Spark stores in the checkpoint as they are,
/// so they have no envelope: the library owns their format.
template <typename T>
arrow::Result<std::string> EncodeOffset(const T& offset) {
  return Guard([&]() -> arrow::Result<std::string> { return nlohmann::json(offset).dump(); });
}

template <typename T>
arrow::Result<T> DecodeOffset(std::string_view json) {
  nlohmann::json value = nlohmann::json::parse(json, nullptr, /*allow_exceptions=*/false);
  if (value.is_discarded()) {
    return arrow::Status::Invalid("cannot decode the offset ", json, ": it is not valid JSON");
  }
  try {
    return value.get<T>();
  } catch (const std::exception& error) {
    return arrow::Status::Invalid("cannot decode the offset ", json, ": ", error.what());
  } catch (...) {
    return arrow::Status::Invalid("cannot decode the offset ", json);
  }
}

}  // namespace spark::native::internal
