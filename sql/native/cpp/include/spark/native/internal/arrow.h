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

// The Arrow data that the library exchanges with Spark: the types of version 1 of the ABI, the
// checks of the batches that readers return, and the Arrow C stream of a reader.

#pragma once

#include <cerrno>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <arrow/array.h>
#include <arrow/c/bridge.h>
#include <arrow/record_batch.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/type.h>

#include "spark/native/internal/abi.h"

namespace spark::native::internal {

arrow::Result<arrow::FieldVector> CanonicalFields(const arrow::FieldVector& fields);

// Compares the Spark logical types of version 1 of the ABI, ignoring Arrow nullability and
// metadata and the interchangeable physical encodings of strings and binary data. Spark also
// distinguishes only the presence of a time zone, not its spelling. Nullability is checked
// against the data instead, in CheckNulls.
inline arrow::Result<std::shared_ptr<arrow::DataType>> CanonicalType(const arrow::DataType& type) {
  switch (type.id()) {
    case arrow::Type::BOOL:
      return arrow::boolean();
    case arrow::Type::INT8:
      return arrow::int8();
    case arrow::Type::INT16:
      return arrow::int16();
    case arrow::Type::INT32:
      return arrow::int32();
    case arrow::Type::INT64:
      return arrow::int64();
    case arrow::Type::FLOAT:
      return arrow::float32();
    case arrow::Type::DOUBLE:
      return arrow::float64();
    case arrow::Type::DATE32:
      return arrow::date32();
    case arrow::Type::STRING:
    case arrow::Type::LARGE_STRING:
    case arrow::Type::STRING_VIEW:
      return arrow::utf8();
    case arrow::Type::BINARY:
    case arrow::Type::LARGE_BINARY:
    case arrow::Type::BINARY_VIEW:
      return arrow::binary();
    case arrow::Type::DECIMAL128: {
      const auto& decimal = static_cast<const arrow::Decimal128Type&>(type);
      if (decimal.precision() > 0 && decimal.precision() <= 38) {
        return arrow::decimal128(decimal.precision(), decimal.scale());
      }
      break;
    }
    case arrow::Type::TIMESTAMP: {
      const auto& timestamp = static_cast<const arrow::TimestampType&>(type);
      if (timestamp.unit() == arrow::TimeUnit::MICRO) {
        return arrow::timestamp(arrow::TimeUnit::MICRO, timestamp.timezone().empty() ? "" : "UTC");
      }
      break;
    }
    case arrow::Type::STRUCT: {
      ARROW_ASSIGN_OR_RAISE(arrow::FieldVector fields, CanonicalFields(type.fields()));
      return arrow::struct_(std::move(fields));
    }
    case arrow::Type::LIST: {
      const auto& list = static_cast<const arrow::ListType&>(type);
      ARROW_ASSIGN_OR_RAISE(auto element, CanonicalType(*list.value_type()));
      return arrow::list(arrow::field("item", std::move(element), /*nullable=*/false));
    }
    case arrow::Type::MAP: {
      const auto& map = static_cast<const arrow::MapType&>(type);
      ARROW_ASSIGN_OR_RAISE(auto key, CanonicalType(*map.key_type()));
      ARROW_ASSIGN_OR_RAISE(auto value, CanonicalType(*map.item_type()));
      return std::make_shared<arrow::MapType>(
          std::move(key), arrow::field("value", std::move(value), /*nullable=*/false),
          /*keys_sorted=*/false);
    }
    default:
      break;
  }
  return arrow::Status::Invalid("unsupported Spark Arrow type: ", type.ToString());
}

inline arrow::Result<arrow::FieldVector> CanonicalFields(const arrow::FieldVector& fields) {
  arrow::FieldVector canonical;
  canonical.reserve(fields.size());
  for (const auto& field : fields) {
    ARROW_ASSIGN_OR_RAISE(auto type, CanonicalType(*field->type()));
    canonical.push_back(arrow::field(field->name(), std::move(type), /*nullable=*/false));
  }
  return canonical;
}

/// Whether two schemas have the same columns, with the same Spark types.
inline arrow::Result<bool> SameSparkTypes(const arrow::Schema& left, const arrow::Schema& right) {
  ARROW_ASSIGN_OR_RAISE(arrow::FieldVector left_fields, CanonicalFields(left.fields()));
  ARROW_ASSIGN_OR_RAISE(arrow::FieldVector right_fields, CanonicalFields(right.fields()));
  if (left_fields.size() != right_fields.size()) {
    return false;
  }
  for (size_t index = 0; index < left_fields.size(); ++index) {
    if (!left_fields[index]->Equals(*right_fields[index], /*check_metadata=*/false)) {
      return false;
    }
  }
  return true;
}

/// Rejects the types that version 1 of the ABI does not support.
inline arrow::Status ValidateSchema(const arrow::Schema& schema) {
  return CanonicalFields(schema.fields()).status();
}

/// Which values of a list or map belong to a value that `valid` accepts.
template <typename Valid>
arrow::Result<std::vector<bool>> Elements(const arrow::ListArray& list, Valid valid) {
  std::vector<bool> mask(static_cast<size_t>(list.values()->length()), false);
  for (int64_t index = 0; index < list.length(); ++index) {
    if (!valid(index)) {
      continue;
    }
    int32_t start = list.value_offset(index);
    int32_t end = list.value_offset(index + 1);
    if (start < 0 || end < start || static_cast<size_t>(end) > mask.size()) {
      return arrow::Status::Invalid("Arrow offsets exceed the length of their values");
    }
    std::fill(mask.begin() + start, mask.begin() + end, true);
  }
  return mask;
}

/// Rejects a null where the negotiated schema declares a column, a field, an element, or a map
/// value not nullable, unless its parent is null, as Spark does after reading. `referenced` tells
/// which values belong to a parent value that is not null.
inline arrow::Status CheckNulls(const arrow::Array& array, const arrow::Field& field,
                                const std::string& column, const std::vector<bool>* referenced) {
  auto is_referenced = [referenced](int64_t index) {
    return referenced == nullptr || (*referenced)[static_cast<size_t>(index)];
  };
  if (!field.nullable() && array.null_count() > 0) {
    for (int64_t index = 0; index < array.length(); ++index) {
      if (array.IsNull(index) && is_referenced(index)) {
        return arrow::Status::Invalid("The column ", column,
                                      " has a null, but the schema to read declares it not "
                                      "nullable");
      }
    }
  }
  auto valid = [&](int64_t index) { return array.IsValid(index) && is_referenced(index); };
  switch (field.type()->id()) {
    case arrow::Type::STRUCT: {
      const auto& structs = static_cast<const arrow::StructArray&>(array);
      std::vector<bool> mask(static_cast<size_t>(array.length()));
      for (int64_t index = 0; index < array.length(); ++index) {
        mask[static_cast<size_t>(index)] = valid(index);
      }
      for (int child = 0; child < field.type()->num_fields(); ++child) {
        const auto& child_field = *field.type()->field(child);
        ARROW_RETURN_NOT_OK(CheckNulls(*structs.field(child), child_field,
                                       column + "." + child_field.name(), &mask));
      }
      break;
    }
    case arrow::Type::LIST: {
      const auto& lists = static_cast<const arrow::ListArray&>(array);
      ARROW_ASSIGN_OR_RAISE(std::vector<bool> mask, Elements(lists, valid));
      const auto& element = *static_cast<const arrow::ListType&>(*field.type()).value_field();
      ARROW_RETURN_NOT_OK(CheckNulls(*lists.values(), element, column + ".element", &mask));
      break;
    }
    case arrow::Type::MAP: {
      const auto& maps = static_cast<const arrow::MapArray&>(array);
      ARROW_ASSIGN_OR_RAISE(std::vector<bool> mask, Elements(maps, valid));
      const auto& type = static_cast<const arrow::MapType&>(*field.type());
      // Spark does not allow null keys.
      ARROW_RETURN_NOT_OK(
          CheckNulls(*maps.keys(), *type.key_field()->WithNullable(false), column + ".key", &mask));
      ARROW_RETURN_NOT_OK(CheckNulls(*maps.items(), *type.item_field(), column + ".value", &mask));
      break;
    }
    default:
      break;
  }
  return arrow::Status::OK();
}

/// Arrow Java ignores the offsets of the arrays that it imports, so Spark rejects an array with an
/// offset other than 0, at any level.
inline arrow::Status ValidateLayout(const arrow::ArrayData& data) {
  if (data.offset != 0) {
    return arrow::Status::Invalid(
        "Spark requires zero Arrow array offsets: ", data.type->ToString(), " has offset ",
        data.offset, "; normalize sliced arrays");
  }
  for (const auto& child : data.child_data) {
    ARROW_RETURN_NOT_OK(ValidateLayout(*child));
  }
  return arrow::Status::OK();
}

/// The Arrow C stream of a partition reader. Its callbacks can be called from any thread, never
/// concurrently, and keep their errors in the stream, not in a thread-local.
class ExportedStream {
 public:
  /// Exports `reader`, whose schema must have the Spark types of `expected`, the negotiated schema.
  /// On failure, `out` is left unchanged.
  static arrow::Status Export(std::shared_ptr<arrow::RecordBatchReader> reader,
                              std::shared_ptr<arrow::Schema> expected,
                              struct ArrowArrayStream* out) {
    ARROW_RETURN_NOT_OK(RequireOutput(out));
    std::shared_ptr<arrow::Schema> schema = reader->schema();
    if (schema == nullptr) {
      return arrow::Status::Invalid("the partition reader has no schema");
    }
    ARROW_ASSIGN_OR_RAISE(bool same, SameSparkTypes(*schema, *expected));
    if (!same) {
      return arrow::Status::Invalid("executor reader schema does not match the negotiated schema: ",
                                    schema->ToString(), " instead of ", expected->ToString());
    }
    auto state = std::unique_ptr<ExportedStream>(
        new ExportedStream(std::move(reader), std::move(schema), std::move(expected)));
    out->get_schema = &GetSchema;
    out->get_next = &GetNext;
    out->get_last_error = &GetLastError;
    out->release = &Release;
    out->private_data = state.release();
    return arrow::Status::OK();
  }

 private:
  ExportedStream(std::shared_ptr<arrow::RecordBatchReader> reader,
                 std::shared_ptr<arrow::Schema> schema, std::shared_ptr<arrow::Schema> expected)
      : reader_(std::move(reader)), schema_(std::move(schema)), expected_(std::move(expected)) {}

  static ExportedStream& Of(struct ArrowArrayStream* stream) {
    return *static_cast<ExportedStream*>(stream->private_data);
  }

  template <typename F>
  int Callback(F&& action) noexcept {
    arrow::Status status = Guard(std::forward<F>(action));
    if (status.ok()) {
      return 0;
    }
    // A failed stream has no more batches.
    finished_ = true;
    try {
      error_ = ErrorMessage(status);
    } catch (...) {
      error_ = "native stream error";
    }
    // EIO: get_last_error supplies the actual message.
    return EIO;
  }

  static int GetSchema(struct ArrowArrayStream* stream, struct ArrowSchema* out) noexcept {
    ExportedStream& state = Of(stream);
    return state.Callback([&] { return arrow::ExportSchema(*state.schema_, out); });
  }

  static int GetNext(struct ArrowArrayStream* stream, struct ArrowArray* out) noexcept {
    ExportedStream& state = Of(stream);
    return state.Callback([&]() -> arrow::Status {
      std::shared_ptr<arrow::RecordBatch> batch;
      if (!state.finished_) {
        ARROW_RETURN_NOT_OK(state.reader_->ReadNext(&batch));
      }
      if (batch == nullptr) {
        // A released array marks the end of the stream, also for data without columns.
        state.finished_ = true;
        out->release = nullptr;
        return arrow::Status::OK();
      }
      ARROW_RETURN_NOT_OK(state.Check(*batch));
      return arrow::ExportRecordBatch(*batch, out);
    });
  }

  static const char* GetLastError(struct ArrowArrayStream* stream) noexcept {
    ExportedStream& state = Of(stream);
    return state.error_.empty() ? nullptr : state.error_.c_str();
  }

  static void Release(struct ArrowArrayStream* stream) noexcept {
    auto* state = static_cast<ExportedStream*>(stream->private_data);
    // Mark the stream released before destroying it. The consumer owns the stream structure.
    stream->release = nullptr;
    stream->private_data = nullptr;
    delete state;
  }

  arrow::Status Check(const arrow::RecordBatch& batch) const {
    if (!batch.schema()->Equals(*schema_, /*check_metadata=*/false)) {
      return arrow::Status::Invalid(
          "reader returned a batch with a different schema: ", batch.schema()->ToString(),
          " instead of ", schema_->ToString());
    }
    // Spark cannot check the sizes of the buffers, so check that they hold their values.
    ARROW_RETURN_NOT_OK(batch.Validate());
    for (int index = 0; index < batch.num_columns(); ++index) {
      const auto& column = batch.column(index);
      ARROW_RETURN_NOT_OK(ValidateLayout(*column->data()));
      const auto& field = *expected_->field(index);
      ARROW_RETURN_NOT_OK(CheckNulls(*column, field, field.name(), nullptr));
    }
    return arrow::Status::OK();
  }

  std::shared_ptr<arrow::RecordBatchReader> reader_;
  // The schema of the stream, the schema of the reader, which every batch must have.
  std::shared_ptr<arrow::Schema> schema_;
  // The negotiated schema, whose Spark types and nullability each batch must respect.
  std::shared_ptr<arrow::Schema> expected_;
  std::string error_;
  bool finished_ = false;
};

}  // namespace spark::native::internal
