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

#include <atomic>
#include <cstdint>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

#include <arrow/api.h>
#include <arrow/c/bridge.h>

#include "spark/native/data_source.h"
#include "test_util.h"

namespace internal = spark::native::internal;

namespace {

using BatchResult = arrow::Result<std::shared_ptr<arrow::RecordBatch>>;

class TestReader : public arrow::RecordBatchReader {
 public:
  TestReader(std::shared_ptr<arrow::Schema> schema, std::vector<BatchResult> batches,
             std::shared_ptr<std::atomic<int>> destroyed)
      : schema_(std::move(schema)),
        batches_(std::move(batches)),
        destroyed_(std::move(destroyed)) {}

  ~TestReader() override { ++*destroyed_; }

  std::shared_ptr<arrow::Schema> schema() const override { return schema_; }

  arrow::Status ReadNext(std::shared_ptr<arrow::RecordBatch>* batch) override {
    if (throw_next) {
      throw std::runtime_error("iterator failed");
    }
    if (index_ == batches_.size()) {
      *batch = nullptr;
      return arrow::Status::OK();
    }
    BatchResult& next = batches_[index_++];
    ARROW_RETURN_NOT_OK(next.status());
    *batch = *next;
    return arrow::Status::OK();
  }

  bool throw_next = false;

 private:
  std::shared_ptr<arrow::Schema> schema_;
  std::vector<BatchResult> batches_;
  size_t index_ = 0;
  std::shared_ptr<std::atomic<int>> destroyed_;
};

std::shared_ptr<arrow::Schema> IdSchema(bool nullable = false) {
  return arrow::schema({arrow::field("id", arrow::int64(), nullable)});
}

std::shared_ptr<arrow::Array> Int64s(const std::vector<std::optional<int64_t>>& values) {
  arrow::Int64Builder builder;
  for (const auto& value : values) {
    if (value) {
      (void)builder.Append(*value);
    } else {
      (void)builder.AppendNull();
    }
  }
  return builder.Finish().ValueOrDie();
}

std::shared_ptr<arrow::Array> Int32s(const std::vector<std::optional<int32_t>>& values) {
  arrow::Int32Builder builder;
  for (const auto& value : values) {
    if (value) {
      (void)builder.Append(*value);
    } else {
      (void)builder.AppendNull();
    }
  }
  return builder.Finish().ValueOrDie();
}

/// A string or binary array of `type` with the given offsets over `data`, which may be invalid.
template <typename Offset>
std::shared_ptr<arrow::Array> Strings(std::shared_ptr<arrow::DataType> type,
                                      std::vector<Offset> offsets, std::string data) {
  auto length = static_cast<int64_t>(offsets.size()) - 1;
  return arrow::MakeArray(arrow::ArrayData::Make(
      std::move(type), length,
      {nullptr, arrow::Buffer::FromVector(std::move(offsets)),
       arrow::Buffer::FromString(std::move(data))},
      0));
}

std::shared_ptr<arrow::RecordBatch> Ids(const std::vector<std::optional<int64_t>>& values,
                                        bool nullable = false) {
  return arrow::RecordBatch::Make(IdSchema(nullable), static_cast<int64_t>(values.size()),
                                  {Int64s(values)});
}

struct Exported {
  struct ArrowArrayStream stream{};
  std::shared_ptr<std::atomic<int>> destroyed = std::make_shared<std::atomic<int>>(0);
  TestReader* reader = nullptr;

  Exported() = default;
  Exported(const Exported&) = delete;
  Exported& operator=(const Exported&) = delete;

  ~Exported() {
    if (stream.release != nullptr) {
      stream.release(&stream);
    }
  }
};

/// Exports a reader of `batches`, with the schema `schema`, for the schema to read `expected`.
void Export(Exported& exported, std::vector<BatchResult> batches,
            std::shared_ptr<arrow::Schema> schema, std::shared_ptr<arrow::Schema> expected) {
  auto reader =
      std::make_shared<TestReader>(std::move(schema), std::move(batches), exported.destroyed);
  exported.reader = reader.get();
  CHECK_OK(internal::ExportedStream::Export(reader, std::move(expected), &exported.stream));
}

/// Calls get_next, and returns its error, if any.
std::optional<std::string> NextError(struct ArrowArrayStream& stream) {
  struct ArrowArray array{};
  if (stream.get_next(&stream, &array) == 0) {
    if (array.release != nullptr) {
      array.release(&array);
    }
    return std::nullopt;
  }
  const char* error = stream.get_last_error(&stream);
  return std::string(error == nullptr ? "(no message)" : error);
}

/// The error of the first batch of a reader of `batch`, whose schema to read is `expected`.
std::optional<std::string> BatchError(std::shared_ptr<arrow::RecordBatch> batch,
                                      std::shared_ptr<arrow::Schema> expected) {
  Exported exported;
  std::shared_ptr<arrow::Schema> schema = batch->schema();
  Export(exported, {batch}, schema, std::move(expected));
  return NextError(exported.stream);
}

bool AtEnd(struct ArrowArrayStream& stream) {
  struct ArrowArray array{};
  return stream.get_next(&stream, &array) == 0 && array.release == nullptr;
}

std::shared_ptr<arrow::RecordBatchReader> Import(Exported& exported) {
  auto reader = arrow::ImportRecordBatchReader(&exported.stream).ValueOrDie();
  CHECK(exported.stream.release == nullptr);
  return reader;
}

}  // namespace

TEST(ImportedBatchSurvivesEarlyStreamRelease) {
  Exported exported;
  Export(exported, {Ids({3, 5})}, IdSchema(), IdSchema());
  std::shared_ptr<arrow::RecordBatchReader> reader = Import(exported);
  ASSIGN_OR_FAIL(std::shared_ptr<arrow::RecordBatch> batch, reader->Next());
  reader.reset();
  CHECK_EQ(exported.destroyed->load(), 1);
  CHECK_EQ(batch->num_rows(), int64_t{2});
  CHECK(batch->column(0)->Equals(*Int64s({3, 5})));
}

TEST(EmptyBatchIsNotTheEndOfTheStream) {
  Exported exported;
  Export(exported, {Ids({}), Ids({3, 5})}, IdSchema(), IdSchema());
  std::shared_ptr<arrow::RecordBatchReader> reader = Import(exported);
  ASSIGN_OR_FAIL(auto empty, reader->Next());
  CHECK(empty != nullptr && empty->num_rows() == 0);
  ASSIGN_OR_FAIL(auto batch, reader->Next());
  CHECK(batch != nullptr && batch->num_rows() == 2);
  ASSIGN_OR_FAIL(auto end, reader->Next());
  CHECK(end == nullptr);
}

TEST(AReaderWithoutBatchesHasItsSchema) {
  Exported exported;
  Export(exported, {}, IdSchema(), IdSchema());
  std::shared_ptr<arrow::RecordBatchReader> reader = Import(exported);
  CHECK(reader->schema()->Equals(*IdSchema()));
  ASSIGN_OR_FAIL(auto end, reader->Next());
  CHECK(end == nullptr);
}

TEST(TheSchemaOfTheReaderIsTheSchemaOfTheStream) {
  // The schema to read has a utf8 string, which the reader returns as large_utf8.
  auto expected = arrow::schema({arrow::field("label", arrow::utf8())});
  auto actual = arrow::schema({arrow::field("label", arrow::large_utf8())});
  arrow::LargeStringBuilder builder;
  CHECK_OK(builder.AppendValues({"a", "b"}));
  ASSIGN_OR_FAIL(std::shared_ptr<arrow::Array> labels, builder.Finish());
  Exported exported;
  Export(exported, {arrow::RecordBatch::Make(actual, 2, {labels})}, actual, expected);
  std::shared_ptr<arrow::RecordBatchReader> reader = Import(exported);
  CHECK(reader->schema()->Equals(*actual));
  ASSIGN_OR_FAIL(auto batch, reader->Next());
  CHECK_EQ(std::static_pointer_cast<arrow::LargeStringArray>(batch->column(0))->GetString(1),
           std::string("b"));
}

TEST(ReaderErrorsAndExceptionsBecomeStreamErrors) {
  Exported throwing;
  Export(throwing, {Ids({1})}, IdSchema(), IdSchema());
  throwing.reader->throw_next = true;
  CHECK_CONTAINS(NextError(throwing.stream).value_or(""), "iterator failed");
  CHECK(AtEnd(throwing.stream));

  Exported failing;
  Export(failing, {arrow::Status::Invalid(std::string("bad\0value", 9))}, IdSchema(), IdSchema());
  CHECK_CONTAINS(NextError(failing.stream).value_or(""), "bad\\0value");

  // The error of a later batch is reported by a later call.
  Exported late;
  Export(late, {Ids({1, 2}), arrow::Status::Invalid("late failure")}, IdSchema(), IdSchema());
  CHECK(!NextError(late.stream).has_value());
  CHECK_CONTAINS(NextError(late.stream).value_or(""), "late failure");
  CHECK(AtEnd(late.stream));
}

TEST(AFailedStreamReleasesItsReader) {
  std::shared_ptr<std::atomic<int>> destroyed;
  {
    Exported exported;
    destroyed = exported.destroyed;
    Export(exported, {Ids({1})}, IdSchema(), IdSchema());
    exported.reader->throw_next = true;
    CHECK(NextError(exported.stream).has_value());
    CHECK_EQ(destroyed->load(), 0);
  }
  CHECK_EQ(destroyed->load(), 1);
}

TEST(ASchemaMismatchIsRejectedBeforeTheStreamIsExported) {
  auto destroyed = std::make_shared<std::atomic<int>>(0);
  struct ArrowArrayStream stream{};
  for (auto [expected, error] :
       {std::make_pair(arrow::schema({arrow::field("id", arrow::utf8(), false)}),
                       "does not match the schema to read"),
        std::make_pair(arrow::schema({arrow::field("label", arrow::utf8(), false)}),
                       "does not return the column label")}) {
    auto reader = std::make_shared<TestReader>(IdSchema(), std::vector<BatchResult>{}, destroyed);
    arrow::Status status = internal::ExportedStream::Export(reader, expected, &stream);
    CHECK(!status.ok());
    CHECK_CONTAINS(status.message(), error);
  }
  CHECK(stream.release == nullptr);
  CHECK_EQ(destroyed->load(), 2);
}

TEST(TheSdkKeepsTheColumnsOfTheSchemaToReadByName) {
  // The reader ignores pruning and returns all the columns of the source, in its order.
  auto all = arrow::schema(
      {arrow::field("id", arrow::int64(), false), arrow::field("flag", arrow::boolean(), false)});
  arrow::BooleanBuilder flags;
  CHECK_OK(flags.AppendValues(std::vector<bool>{true, false}));
  ASSIGN_OR_FAIL(std::shared_ptr<arrow::Array> flag_values, flags.Finish());
  auto batch = arrow::RecordBatch::Make(all, 2, {Int64s({3, 5}), flag_values});
  for (const auto& expected :
       {arrow::schema({all->field(1), all->field(0)}), arrow::schema(arrow::FieldVector{})}) {
    Exported exported;
    Export(exported, {batch}, all, expected);
    std::shared_ptr<arrow::RecordBatchReader> reader = Import(exported);
    CHECK(reader->schema()->Equals(*expected));
    ASSIGN_OR_FAIL(auto read, reader->Next());
    CHECK(read->schema()->Equals(*expected));
    CHECK_EQ(read->num_rows(), int64_t{2});
    ASSIGN_OR_FAIL(auto end, reader->Next());
    CHECK(end == nullptr);
  }
}

TEST(LogicalSchemaComparisonAcceptsEquivalentArrowRepresentations) {
  auto actual = arrow::schema({
      arrow::field("text", arrow::large_utf8()),
      arrow::field("bytes", arrow::binary_view()),
      arrow::field("time", arrow::timestamp(arrow::TimeUnit::MICRO, "Europe/Paris")),
      arrow::field("nested", arrow::struct_({arrow::field("text", arrow::utf8_view())})),
      arrow::field("list", arrow::list(arrow::field("element", arrow::large_binary()))),
      arrow::field("map", arrow::map(arrow::utf8(), arrow::large_utf8())),
  });
  auto expected = arrow::schema({
      arrow::field("text", arrow::utf8(), false),
      arrow::field("bytes", arrow::binary(), false),
      arrow::field("time", arrow::timestamp(arrow::TimeUnit::MICRO, "UTC"), false),
      arrow::field("nested", arrow::struct_({arrow::field("text", arrow::utf8(), false)}), false),
      arrow::field("list", arrow::list(arrow::field("item", arrow::binary(), false))),
      arrow::field("map", arrow::map(arrow::large_utf8(), arrow::utf8())),
  });
  ASSIGN_OR_FAIL(bool same, internal::SameSparkTypes(*actual, *expected));
  CHECK(same);
  // A timestamp without a time zone remains a distinct Spark logical type.
  auto ntz = arrow::schema({arrow::field("time", arrow::timestamp(arrow::TimeUnit::MICRO))});
  auto ltz = arrow::schema({arrow::field("time", arrow::timestamp(arrow::TimeUnit::MICRO, "UTC"))});
  ASSIGN_OR_FAIL(bool zones, internal::SameSparkTypes(*ntz, *ltz));
  CHECK(!zones);
  // So do column names and their order.
  auto renamed = arrow::schema({arrow::field("other", arrow::int64())});
  ASSIGN_OR_FAIL(bool names, internal::SameSparkTypes(*renamed, *IdSchema()));
  CHECK(!names);
}

TEST(UnsupportedTypesAreRejectedInsideNestedFields) {
  for (const auto& type : std::vector<std::shared_ptr<arrow::DataType>>{
           arrow::list(arrow::field("item", arrow::uint64(), false)),
           arrow::struct_({arrow::field("dictionary",
                                        arrow::dictionary(arrow::int32(), arrow::utf8()), false)}),
           arrow::map(arrow::utf8(), arrow::decimal256(40, 2)),
           arrow::large_list(arrow::int64()),
           arrow::fixed_size_list(arrow::int64(), 2),
           arrow::timestamp(arrow::TimeUnit::NANO),
       }) {
    arrow::Status status = internal::ValidateSchema(*arrow::schema({arrow::field("nested", type)}));
    CHECK_CONTAINS(status.message(), "unsupported Spark Arrow type");
  }
}

TEST(IntervalsAndTheNullTypeAreNotVersion1Types) {
  for (const auto& type : std::vector<std::shared_ptr<arrow::DataType>>{
           arrow::null(), arrow::duration(arrow::TimeUnit::MICRO), arrow::month_interval(),
           arrow::month_day_nano_interval()}) {
    CHECK(!internal::ValidateSchema(*arrow::schema({arrow::field("value", type)})).ok());
  }
}

TEST(AValidSlicedBooleanInsideAStructIsRejectedBeforeExport) {
  arrow::BooleanBuilder builder;
  CHECK_OK(builder.AppendValues(std::vector<bool>{true, false, true}));
  ASSIGN_OR_FAIL(std::shared_ptr<arrow::Array> values, builder.Finish());
  std::shared_ptr<arrow::Array> sliced = values->Slice(1, 2);
  CHECK_EQ(sliced->offset(), int64_t{1});
  arrow::FieldVector fields = {arrow::field("flag", arrow::boolean(), false)};
  ASSIGN_OR_FAIL(std::shared_ptr<arrow::Array> nested, arrow::StructArray::Make({sliced}, fields));
  auto schema = arrow::schema({arrow::field("nested", arrow::struct_(fields), false)});
  Exported exported;
  Export(exported, {arrow::RecordBatch::Make(schema, 2, {nested})}, schema, schema);
  std::string error = NextError(exported.stream).value_or("");
  CHECK_CONTAINS(error, "zero Arrow array offsets");
  CHECK_CONTAINS(error, "offset 1");
  CHECK(AtEnd(exported.stream));
}

TEST(AColumnShorterThanItsBatchIsRejected) {
  // The C data interface does not carry the sizes of buffers, so Spark could read past them.
  auto batch = arrow::RecordBatch::Make(IdSchema(), 3, {Int64s({1, 2})});
  CHECK(BatchError(batch, IdSchema()).has_value());
}

TEST(NullsMustRespectTheNullabilityOfTheSchemaToRead) {
  auto batch = Ids({1, std::nullopt}, /*nullable=*/true);
  CHECK_CONTAINS(BatchError(batch, IdSchema(false)).value_or(""), "The column id has a null");
  CHECK(!BatchError(batch, IdSchema(true)).has_value());
}

TEST(NestedNullsAreAllowedOnlyUnderANullParent) {
  auto required = arrow::struct_({arrow::field("flag", arrow::boolean(), false)});
  auto nullable = arrow::struct_({arrow::field("flag", arrow::boolean(), true)});
  for (bool parent_is_valid : {false, true}) {
    auto flags = std::make_shared<arrow::BooleanBuilder>();
    arrow::StructBuilder builder(nullable, arrow::default_memory_pool(), {flags});
    CHECK_OK(flags->Append(true));
    CHECK_OK(builder.Append(true));
    CHECK_OK(flags->AppendNull());
    CHECK_OK(builder.Append(parent_is_valid));
    ASSIGN_OR_FAIL(std::shared_ptr<arrow::Array> nested, builder.Finish());
    auto batch =
        arrow::RecordBatch::Make(arrow::schema({arrow::field("nested", nullable)}), 2, {nested});
    std::optional<std::string> error =
        BatchError(batch, arrow::schema({arrow::field("nested", required)}));
    CHECK_EQ(error.has_value(), parent_is_valid);
    if (error) {
      CHECK_CONTAINS(*error, "The column nested.flag has a null");
    }
  }
}

TEST(ListElementsAndMapValuesRespectTheirNullability) {
  auto list_schema = [](bool nullable) {
    return arrow::schema(
        {arrow::field("values", arrow::list(arrow::field("item", arrow::int32(), nullable)))});
  };
  for (bool first_is_null : {false, true}) {
    auto elements = std::make_shared<arrow::Int32Builder>();
    arrow::ListBuilder builder(arrow::default_memory_pool(), elements);
    CHECK_OK(builder.Append());
    CHECK_OK(first_is_null ? elements->AppendNull() : elements->Append(1));
    // The second list is null, so its null element does not count.
    CHECK_OK(builder.AppendNull());
    CHECK_OK(elements->AppendNull());
    ASSIGN_OR_FAIL(std::shared_ptr<arrow::Array> lists, builder.Finish());
    auto batch = arrow::RecordBatch::Make(list_schema(true), 2, {lists});
    std::optional<std::string> error = BatchError(batch, list_schema(false));
    CHECK_EQ(error.has_value(), first_is_null);
    if (error) {
      CHECK_CONTAINS(*error, "The column values.element has a null");
    }
  }

  auto keys = std::make_shared<arrow::StringBuilder>();
  auto items = std::make_shared<arrow::Int32Builder>();
  arrow::MapBuilder builder(arrow::default_memory_pool(), keys, items);
  CHECK_OK(builder.Append());
  CHECK_OK(keys->Append("a"));
  CHECK_OK(items->AppendNull());
  ASSIGN_OR_FAIL(std::shared_ptr<arrow::Array> map, builder.Finish());
  auto actual = arrow::schema({arrow::field("map", map->type())});
  auto required = arrow::schema(
      {arrow::field("map", std::make_shared<arrow::MapType>(
                               arrow::utf8(), arrow::field("value", arrow::int32(), false)))});
  auto batch = arrow::RecordBatch::Make(actual, 1, {map});
  CHECK_CONTAINS(BatchError(batch, required).value_or(""), "The column map.value has a null");
  CHECK(!BatchError(batch, actual).has_value());
}

TEST(MapEntriesShorterThanTheirKeysAndItemsAreCheckedByTheirLength) {
  // Arrow accepts struct children longer than their struct: the null item is past the entries.
  auto type = arrow::map(arrow::utf8(), arrow::int32());
  arrow::StringBuilder keys;
  CHECK_OK(keys.AppendValues({"a", "b", "c"}));
  ASSIGN_OR_FAIL(std::shared_ptr<arrow::Array> key_values, keys.Finish());
  auto entries = std::make_shared<arrow::StructArray>(
      static_cast<const arrow::MapType&>(*type).value_type(), 2,
      arrow::ArrayVector{key_values, Int32s({1, 2, std::nullopt})});
  auto map = std::make_shared<arrow::MapArray>(
      type, 1, arrow::Buffer::FromVector(std::vector<int32_t>{0, 2}), entries);
  auto actual = arrow::schema({arrow::field("map", type)});
  auto required = arrow::schema(
      {arrow::field("map", std::make_shared<arrow::MapType>(
                               arrow::utf8(), arrow::field("value", arrow::int32(), false)))});
  CHECK(!BatchError(arrow::RecordBatch::Make(actual, 1, {map}), required).has_value());
}

TEST(StringOffsetsMustNotDecreaseInsideTheirData) {
  // RecordBatch::Validate() accepts these offsets, which only end within the data.
  std::vector<std::shared_ptr<arrow::Array>> columns = {
      Strings<int32_t>(arrow::utf8(), {0, 100, 3}, "abc"),
      Strings<int32_t>(arrow::binary(), {0, 100, 3}, "abc"),
      Strings<int64_t>(arrow::large_utf8(), {0, 100, 3}, "abc"),
      Strings<int64_t>(arrow::large_binary(), {0, 100, 3}, "abc"),
  };
  for (const auto& column : columns) {
    auto field = arrow::field("text", column->type());
    CHECK_CONTAINS(BatchError(arrow::RecordBatch::Make(arrow::schema({field}), 2, {column}),
                              arrow::schema({field}))
                       .value_or(""),
                   "decreasing offsets");
    ASSIGN_OR_FAIL(std::shared_ptr<arrow::Array> nested,
                   arrow::StructArray::Make({column}, arrow::FieldVector{field}));
    auto nested_schema = arrow::schema({arrow::field("nested", nested->type())});
    CHECK_CONTAINS(BatchError(arrow::RecordBatch::Make(nested_schema, 2, {nested}), nested_schema)
                       .value_or(""),
                   "decreasing offsets");
    auto list = std::make_shared<arrow::ListArray>(
        arrow::list(field), 1, arrow::Buffer::FromVector(std::vector<int32_t>{0, 2}), column);
    auto list_schema = arrow::schema({arrow::field("list", list->type())});
    CHECK_CONTAINS(
        BatchError(arrow::RecordBatch::Make(list_schema, 1, {list}), list_schema).value_or(""),
        "decreasing offsets");
  }
  // Offsets that do not start at 0 are valid.
  auto shifted = Strings<int32_t>(arrow::utf8(), {1, 2, 3}, "abc");
  auto schema = arrow::schema({arrow::field("text", arrow::utf8())});
  CHECK(!BatchError(arrow::RecordBatch::Make(schema, 2, {shifted}), schema).has_value());
}

TEST(StringViewsMustNameARangeOfAnExistingDataBuffer) {
  for (const auto& type : {arrow::utf8_view(), arrow::binary_view()}) {
    auto field = arrow::field("text", type);
    auto schema = arrow::schema({field});
    for (auto [buffer_index, view_offset] :
         {std::make_pair(0, 0), std::make_pair(0, 8), std::make_pair(1, 0)}) {
      arrow::BinaryViewType::c_type view{};
      view.ref.size = 16;
      view.ref.buffer_index = buffer_index;
      view.ref.offset = view_offset;
      // The data buffer has 16 bytes, so only the view at offset 0 of buffer 0 is valid.
      auto column = arrow::MakeArray(arrow::ArrayData::Make(
          type, 1,
          {nullptr, arrow::Buffer::FromVector(std::vector<arrow::BinaryViewType::c_type>{view}),
           arrow::Buffer::FromString(std::string(16, 'x'))},
          0));
      std::optional<std::string> error =
          BatchError(arrow::RecordBatch::Make(schema, 1, {column}), schema);
      CHECK_EQ(error.has_value(), buffer_index != 0 || view_offset != 0);
      if (error) {
        CHECK_CONTAINS(*error, "data buffer");
      }
    }
  }
  // Inline and out-of-line views of a builder export, with their values.
  arrow::StringViewBuilder builder;
  CHECK_OK(builder.Append(std::string_view("short")));
  CHECK_OK(builder.Append(std::string_view("a value longer than twelve bytes")));
  ASSIGN_OR_FAIL(std::shared_ptr<arrow::Array> views, builder.Finish());
  auto schema = arrow::schema({arrow::field("text", arrow::utf8_view())});
  Exported exported;
  Export(exported, {arrow::RecordBatch::Make(schema, 2, {views})}, schema, schema);
  std::shared_ptr<arrow::RecordBatchReader> reader = Import(exported);
  ASSIGN_OR_FAIL(auto batch, reader->Next());
  CHECK(batch->column(0)->Equals(*views));
}

TEST(ABatchCannotChangeTheSchemaOfTheStream) {
  auto changed_schema = arrow::schema({arrow::field("id", arrow::int32(), false)});
  arrow::Int32Builder builder;
  CHECK_OK(builder.AppendValues({7, 9}));
  ASSIGN_OR_FAIL(std::shared_ptr<arrow::Array> values, builder.Finish());
  Exported exported;
  Export(exported, {Ids({1, 2}), arrow::RecordBatch::Make(changed_schema, 2, {values})}, IdSchema(),
         IdSchema());
  CHECK(!NextError(exported.stream).has_value());
  CHECK_CONTAINS(NextError(exported.stream).value_or(""), "different schema");
  CHECK(AtEnd(exported.stream));
}

TEST(BatchesWithoutColumnsKeepTheirRowCounts) {
  auto empty = arrow::schema(arrow::FieldVector{});
  Exported exported;
  Export(exported, {arrow::RecordBatch::Make(empty, 3, arrow::ArrayVector{})}, empty, empty);
  struct ArrowArray array{};
  CHECK(exported.stream.get_next(&exported.stream, &array) == 0);
  CHECK(array.release != nullptr);
  CHECK_EQ(array.length, int64_t{3});
  CHECK_EQ(array.n_children, int64_t{0});
  array.release(&array);
  CHECK(AtEnd(exported.stream));
}
