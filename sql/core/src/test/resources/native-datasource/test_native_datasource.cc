/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// A native data source library used by NativeDataSourceSuite. It implements the C interface in
// spark_datasource.h without any other dependency, building the Arrow C data interface structs by
// hand. It provides three data sources:
//
//   native_range    Reads rows [0, end) with columns of several types. Supports predicate,
//                   column and limit pushdown. With the option "claim_predicates", it reports
//                   all the predicates as evaluated completely, but ignores them. With the
//                   option "extra", the schema has one more column: "map" adds a map whose
//                   entry fields are named "keys" and "values", as arrow-rs names them, "struct"
//                   adds a struct, "decimal256" adds a decimal256, "listview" adds a list view,
//                   "largelist" adds a large list, and "dictionary" adds a dictionary-encoded
//                   string column, whose data Spark rejects before reading it. With the option
//                   "slice", the columns of the batches have an offset of 1. With the option
//                   "short", an array of the batches has one value fewer than its parent refers
//                   to, although its buffers hold them all: "id" shortens the column id,
//                   "entries" the entries of the map and their fields, and "values" the values
//                   of the entries.
//   native_sink     Writes the rows as CSV files under the directory in the option "path".
//   native_counter  Streams the rows [0, max_offset) with a single column "id", making "step"
//                   more rows available each time Spark asks for the latest offset. With the
//                   option "bad_offset", the latest offset is "none", "multiline" JSON or
//                   "invalid" JSON. With the option "closed_path", closing the stream reader
//                   creates that file.
//
// With the option "fail", a step fails with an error. With the option "zero", a function that
// creates a handle succeeds without setting it: "spark_ds_create", "spark_ds_create_reader",
// "spark_ds_create_stream_reader", "spark_ds_create_writer", "spark_ds_create_stream_writer" or
// "spark_ds_create_data_writer".
//
// The suite compiles it with these macros to get several distinct libraries:
//
//   NAME_PREFIX     A prefix for the names of the data sources, "" by default.
//   ID_SIGN         Multiplies the ids produced by native_range, 1 by default.
//   READ_ONLY       Leaves out the functions for writes, streaming reads and pushdown.
//   ABI_VERSION     The version returned by spark_ds_abi_version, 1 by default.
//   NO_LAST_ERROR   Leaves out spark_ds_last_error.

#include "spark_datasource.h"

#include <algorithm>
#include <cctype>
#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <map>
#include <memory>
#include <sstream>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include <dirent.h>
#include <sys/stat.h>
#include <unistd.h>

#ifndef NAME_PREFIX
#define NAME_PREFIX ""
#endif
#ifndef ID_SIGN
#define ID_SIGN 1
#endif
#ifndef ABI_VERSION
#define ABI_VERSION 1
#endif

namespace {

// ---------------------------------------------------------------------------------------------
// Columns
// ---------------------------------------------------------------------------------------------

struct ColumnType {
  const char* name;
  const char* format;
};

// The columns of native_range, with their Arrow formats. native_counter only has "id".
const ColumnType kColumns[] = {
    {"id", "l"},       // bigint
    {"mod", "i"},      // int
    {"name", "u"},     // string, null when id is a multiple of 5
    {"value", "g"},    // double
    {"flag", "b"},     // boolean
    {"day", "tdD"},    // date
    {"ts", "tsu:UTC"}  // timestamp
};

// The columns that native_range adds with the option "extra", by its value.
const std::map<std::string, ColumnType> kExtraColumns = {
    {"map", {"m", "+m"}},                   // map<string, bigint>: {"k<id>": id}
    {"struct", {"st", "+s"}},               // struct<a: bigint, b: string>: {id, "s<id>"}
    {"decimal256", {"dec", "d:10,2,256"}},  // decimal256(10, 2): id
    {"listview", {"lv", "+vl"}},            // list_view<bigint>: [id]
    {"largelist", {"ll", "+L"}},            // large_list<bigint>, without data
    {"dictionary", {"dict", "i"}}           // dictionary<int32, string>, without data
};

const ColumnType* findColumn(const std::string& name) {
  for (const auto& column : kColumns) {
    if (name == column.name) return &column;
  }
  for (const auto& extra : kExtraColumns) {
    if (name == extra.second.name) return &extra.second;
  }
  throw std::runtime_error("unknown column " + name);
}

std::vector<std::string> allColumns(bool counter, const std::string& extra = "") {
  if (counter) return {"id"};
  std::vector<std::string> names;
  for (const auto& column : kColumns) names.push_back(column.name);
  if (!extra.empty()) {
    auto it = kExtraColumns.find(extra);
    if (it == kExtraColumns.end()) throw std::runtime_error("unknown extra column " + extra);
    names.push_back(it->second.name);
  }
  return names;
}

// ---------------------------------------------------------------------------------------------
// Exporting schemas
// ---------------------------------------------------------------------------------------------

struct SchemaData {
  std::string format;
  std::string name;
  std::vector<std::unique_ptr<ArrowSchema>> children;
  std::vector<ArrowSchema*> child_pointers;
  std::unique_ptr<ArrowSchema> dictionary;
};

void releaseSchema(ArrowSchema* schema) {
  auto* data = static_cast<SchemaData*>(schema->private_data);
  for (auto& child : data->children) {
    if (child->release != nullptr) child->release(child.get());
  }
  if (data->dictionary != nullptr && data->dictionary->release != nullptr) {
    data->dictionary->release(data->dictionary.get());
  }
  delete data;
  schema->release = nullptr;
}

void initSchema(ArrowSchema* out, const std::string& format, const std::string& name,
                int64_t flags) {
  auto* data = new SchemaData{format, name, {}, {}, nullptr};
  out->format = data->format.c_str();
  out->name = data->name.c_str();
  out->metadata = nullptr;
  out->flags = flags;
  out->n_children = 0;
  out->children = nullptr;
  out->dictionary = nullptr;
  out->release = releaseSchema;
  out->private_data = data;
}

// Adds a child to a schema initialized by initSchema, and returns it.
ArrowSchema* addChild(ArrowSchema* parent, const std::string& format, const std::string& name,
                      int64_t flags) {
  auto* data = static_cast<SchemaData*>(parent->private_data);
  auto child = std::make_unique<ArrowSchema>();
  initSchema(child.get(), format, name, flags);
  data->child_pointers.push_back(child.get());
  data->children.push_back(std::move(child));
  parent->n_children = static_cast<int64_t>(data->children.size());
  parent->children = data->child_pointers.data();
  return data->children.back().get();
}

// Exports a struct type with the given columns.
void exportSchema(const std::vector<std::string>& columns, ArrowSchema* out) {
  initSchema(out, "+s", "", 0);
  for (const auto& name : columns) {
    ArrowSchema* column = addChild(out, findColumn(name)->format, name, ARROW_FLAG_NULLABLE);
    if (name == "m") {
      ArrowSchema* entries = addChild(column, "+s", "entries", 0);
      addChild(entries, "u", "keys", 0);
      addChild(entries, "l", "values", ARROW_FLAG_NULLABLE);
    } else if (name == "st") {
      addChild(column, "l", "a", ARROW_FLAG_NULLABLE);
      addChild(column, "u", "b", ARROW_FLAG_NULLABLE);
    } else if (name == "lv" || name == "ll") {
      addChild(column, "l", "item", ARROW_FLAG_NULLABLE);
    } else if (name == "dict") {
      auto* data = static_cast<SchemaData*>(column->private_data);
      data->dictionary = std::make_unique<ArrowSchema>();
      initSchema(data->dictionary.get(), "u", "", ARROW_FLAG_NULLABLE);
      column->dictionary = data->dictionary.get();
    }
  }
}

// Reads the column names of a struct schema exported by Spark, and releases it.
std::vector<std::string> importColumns(ArrowSchema* schema) {
  std::vector<std::string> columns;
  for (int64_t i = 0; i < schema->n_children; i++) {
    columns.push_back(schema->children[i]->name);
  }
  schema->release(schema);
  return columns;
}

// ---------------------------------------------------------------------------------------------
// Exporting arrays
// ---------------------------------------------------------------------------------------------

struct ArrayData {
  std::vector<std::vector<uint8_t>> buffers;
  std::vector<const void*> buffer_pointers;
  std::vector<std::unique_ptr<ArrowArray>> children;
  std::vector<ArrowArray*> child_pointers;
};

void releaseArray(ArrowArray* array) {
  auto* data = static_cast<ArrayData*>(array->private_data);
  for (auto& child : data->children) {
    if (child->release != nullptr) child->release(child.get());
  }
  delete data;
  array->release = nullptr;
}

// Initializes an array that owns `buffers`. An empty buffer is exported as null.
void initArray(ArrowArray* out, int64_t length, int64_t null_count,
               std::vector<std::vector<uint8_t>> buffers) {
  auto* data = new ArrayData{std::move(buffers), {}, {}, {}};
  for (auto& buffer : data->buffers) {
    data->buffer_pointers.push_back(buffer.empty() ? nullptr : buffer.data());
  }
  out->length = length;
  out->null_count = null_count;
  out->offset = 0;
  out->n_buffers = static_cast<int64_t>(data->buffers.size());
  out->n_children = 0;
  out->buffers = data->buffer_pointers.data();
  out->children = nullptr;
  out->dictionary = nullptr;
  out->release = releaseArray;
  out->private_data = data;
}

template <typename T>
std::vector<uint8_t> toBytes(const std::vector<T>& values) {
  std::vector<uint8_t> bytes(values.size() * sizeof(T));
  if (!bytes.empty()) std::memcpy(bytes.data(), values.data(), bytes.size());
  return bytes;
}

void setBit(std::vector<uint8_t>& bitmap, int64_t i) {
  bitmap[i / 8] |= static_cast<uint8_t>(1 << (i % 8));
}

// Adds a child to an array initialized by initArray, and returns it to be initialized.
ArrowArray* addChild(ArrowArray* parent) {
  auto* data = static_cast<ArrayData*>(parent->private_data);
  data->children.push_back(std::make_unique<ArrowArray>());
  data->child_pointers.push_back(data->children.back().get());
  parent->n_children = static_cast<int64_t>(data->children.size());
  parent->children = data->child_pointers.data();
  return data->children.back().get();
}

// Exports the values of a column for the ids [lo, hi).
void exportColumn(const std::string& name, int64_t lo, int64_t hi, ArrowArray* out) {
  int64_t n = hi - lo;
  std::vector<uint8_t> no_validity;
  if (name == "id") {
    std::vector<int64_t> values;
    for (int64_t id = lo; id < hi; id++) values.push_back(id * ID_SIGN);
    initArray(out, n, 0, {no_validity, toBytes(values)});
  } else if (name == "mod") {
    std::vector<int32_t> values;
    for (int64_t id = lo; id < hi; id++) values.push_back(static_cast<int32_t>(id % 3));
    initArray(out, n, 0, {no_validity, toBytes(values)});
  } else if (name == "name") {
    std::vector<uint8_t> validity((n + 7) / 8, 0);
    std::vector<int32_t> offsets{0};
    std::string chars;
    int64_t null_count = 0;
    for (int64_t id = lo; id < hi; id++) {
      if (id % 5 == 0) {
        null_count++;
      } else {
        setBit(validity, id - lo);
        chars += "n" + std::to_string(id);
      }
      offsets.push_back(static_cast<int32_t>(chars.size()));
    }
    std::vector<uint8_t> chars_bytes(chars.begin(), chars.end());
    initArray(out, n, null_count, {validity, toBytes(offsets), chars_bytes});
  } else if (name == "value") {
    std::vector<double> values;
    for (int64_t id = lo; id < hi; id++) values.push_back(static_cast<double>(id) * 0.5);
    initArray(out, n, 0, {no_validity, toBytes(values)});
  } else if (name == "flag") {
    std::vector<uint8_t> bits((n + 7) / 8, 0);
    for (int64_t id = lo; id < hi; id++) {
      if (id % 2 == 0) setBit(bits, id - lo);
    }
    initArray(out, n, 0, {no_validity, bits});
  } else if (name == "day") {
    std::vector<int32_t> values;
    for (int64_t id = lo; id < hi; id++) values.push_back(static_cast<int32_t>(id));
    initArray(out, n, 0, {no_validity, toBytes(values)});
  } else if (name == "ts") {
    std::vector<int64_t> values;
    for (int64_t id = lo; id < hi; id++) values.push_back(id * 1000000);
    initArray(out, n, 0, {no_validity, toBytes(values)});
  } else if (name == "m") {
    // A single entry for each row.
    std::vector<int32_t> offsets;
    for (int64_t i = 0; i <= n; i++) offsets.push_back(static_cast<int32_t>(i));
    initArray(out, n, 0, {no_validity, toBytes(offsets)});
    ArrowArray* entries = addChild(out);
    initArray(entries, n, 0, {no_validity});
    std::vector<int32_t> key_offsets{0};
    std::string keys;
    std::vector<int64_t> values;
    for (int64_t id = lo; id < hi; id++) {
      keys += "k" + std::to_string(id);
      key_offsets.push_back(static_cast<int32_t>(keys.size()));
      values.push_back(id);
    }
    initArray(addChild(entries), n, 0,
              {no_validity, toBytes(key_offsets), std::vector<uint8_t>(keys.begin(), keys.end())});
    initArray(addChild(entries), n, 0, {no_validity, toBytes(values)});
  } else if (name == "st") {
    initArray(out, n, 0, {no_validity});
    std::vector<int64_t> ids;
    std::vector<int32_t> offsets{0};
    std::string chars;
    for (int64_t id = lo; id < hi; id++) {
      ids.push_back(id);
      chars += "s" + std::to_string(id);
      offsets.push_back(static_cast<int32_t>(chars.size()));
    }
    initArray(addChild(out), n, 0, {no_validity, toBytes(ids)});
    initArray(addChild(out), n, 0,
              {no_validity, toBytes(offsets), std::vector<uint8_t>(chars.begin(), chars.end())});
  } else if (name == "dec") {
    // 256-bit little-endian unscaled values of the scale 2.
    std::vector<int64_t> words;
    for (int64_t id = lo; id < hi; id++) {
      int64_t unscaled = id * 100;
      int64_t sign = unscaled < 0 ? -1 : 0;
      words.insert(words.end(), {unscaled, sign, sign, sign});
    }
    initArray(out, n, 0, {no_validity, toBytes(words)});
  } else if (name == "lv") {
    // A list of a single element for each row.
    std::vector<int32_t> offsets;
    std::vector<int32_t> sizes;
    std::vector<int64_t> values;
    for (int64_t id = lo; id < hi; id++) {
      offsets.push_back(static_cast<int32_t>(id - lo));
      sizes.push_back(1);
      values.push_back(id);
    }
    initArray(out, n, 0, {no_validity, toBytes(offsets), toBytes(sizes)});
    initArray(addChild(out), n, 0, {no_validity, toBytes(values)});
  } else {
    // Including "ll" and "dict", which Spark rejects from the schema, before reading data.
    throw std::runtime_error("no data for the column " + name);
  }
}

// Exports a struct array with the given columns for the ids [lo, hi). A sliced batch exports
// the columns for [lo - 1, hi), with an offset of 1, like a zero-copy slice. `short_array` names
// the array to export with one value fewer than its parent refers to, as the option "short".
void exportBatch(const std::vector<std::string>& columns, int64_t lo, int64_t hi, bool slice,
                 const std::string& short_array, ArrowArray* out) {
  initArray(out, hi - lo, 0, {std::vector<uint8_t>()});
  for (const auto& name : columns) {
    ArrowArray* column = addChild(out);
    exportColumn(name, slice ? lo - 1 : lo, hi, column);
    if (slice) {
      column->offset = 1;
      column->length = hi - lo;
      column->null_count = -1;  // Unknown.
    }
    if (short_array == "id" && name == "id") {
      column->length--;
    } else if (short_array == "entries" && name == "m") {
      // The last offset of the map refers to one entry more than there are.
      ArrowArray* entries = column->children[0];
      entries->length--;
      for (int64_t i = 0; i < entries->n_children; i++) entries->children[i]->length--;
    } else if (short_array == "values" && name == "m") {
      column->children[0]->children[1]->length--;
    }
  }
}

// ---------------------------------------------------------------------------------------------
// A minimal JSON parser, for the predicates and the offsets
// ---------------------------------------------------------------------------------------------

struct Json {
  enum Kind { kNull, kBool, kNumber, kString, kArray, kObject } kind = kNull;
  bool boolean = false;
  double number = 0;
  std::string text;
  std::vector<Json> items;
  std::map<std::string, Json> fields;

  const Json& operator[](const std::string& key) const {
    static const Json missing;
    auto it = fields.find(key);
    return it == fields.end() ? missing : it->second;
  }
};

class JsonParser {
 public:
  explicit JsonParser(const std::string& text) : text_(text) {}

  Json parse() {
    Json value = parseValue();
    skipSpaces();
    if (pos_ != text_.size()) fail();
    return value;
  }

 private:
  [[noreturn]] void fail() { throw std::runtime_error("invalid JSON: " + text_); }

  void skipSpaces() {
    while (pos_ < text_.size() && std::isspace(static_cast<unsigned char>(text_[pos_]))) pos_++;
  }

  bool consume(char c) {
    skipSpaces();
    if (pos_ < text_.size() && text_[pos_] == c) {
      pos_++;
      return true;
    }
    return false;
  }

  void expect(char c) {
    if (!consume(c)) fail();
  }

  bool consumeWord(const char* word) {
    size_t length = std::strlen(word);
    if (text_.compare(pos_, length, word) == 0) {
      pos_ += length;
      return true;
    }
    return false;
  }

  std::string parseString() {
    expect('"');
    std::string result;
    while (pos_ < text_.size() && text_[pos_] != '"') {
      char c = text_[pos_++];
      if (c == '\\') {
        if (pos_ >= text_.size()) fail();
        char escaped = text_[pos_++];
        switch (escaped) {
          case 'n': result += '\n'; break;
          case 't': result += '\t'; break;
          case 'u': result += '?'; pos_ += 4; break;
          default: result += escaped;
        }
      } else {
        result += c;
      }
    }
    expect('"');
    return result;
  }

  Json parseValue() {
    skipSpaces();
    Json value;
    if (pos_ >= text_.size()) fail();
    char c = text_[pos_];
    if (c == '{') {
      value.kind = Json::kObject;
      pos_++;
      if (!consume('}')) {
        do {
          skipSpaces();
          std::string key = parseString();
          expect(':');
          value.fields[key] = parseValue();
        } while (consume(','));
        expect('}');
      }
    } else if (c == '[') {
      value.kind = Json::kArray;
      pos_++;
      if (!consume(']')) {
        do {
          value.items.push_back(parseValue());
        } while (consume(','));
        expect(']');
      }
    } else if (c == '"') {
      value.kind = Json::kString;
      value.text = parseString();
    } else if (consumeWord("true")) {
      value.kind = Json::kBool;
      value.boolean = true;
    } else if (consumeWord("false")) {
      value.kind = Json::kBool;
    } else if (consumeWord("null")) {
      value.kind = Json::kNull;
    } else {
      value.kind = Json::kNumber;
      char* end = nullptr;
      value.number = std::strtod(text_.c_str() + pos_, &end);
      if (end == text_.c_str() + pos_) fail();
      pos_ = end - text_.c_str();
    }
    return value;
  }

  const std::string& text_;
  size_t pos_ = 0;
};

// ---------------------------------------------------------------------------------------------
// State, serialized as "key=value" lines
// ---------------------------------------------------------------------------------------------

using Options = std::map<std::string, std::string>;

std::string serialize(const Options& options) {
  std::string result;
  for (const auto& entry : options) result += entry.first + "=" + entry.second + "\n";
  return result;
}

Options deserialize(const std::string& text) {
  Options options;
  std::istringstream lines(text);
  std::string line;
  while (std::getline(lines, line)) {
    size_t eq = line.find('=');
    if (eq != std::string::npos) options[line.substr(0, eq)] = line.substr(eq + 1);
  }
  return options;
}

std::string get(const Options& options, const std::string& key, const std::string& fallback) {
  auto it = options.find(key);
  return it == options.end() ? fallback : it->second;
}

int64_t getLong(const Options& options, const std::string& key, int64_t fallback) {
  auto it = options.find(key);
  return it == options.end() ? fallback : std::stoll(it->second);
}

std::string join(const std::vector<std::string>& values) {
  std::string result;
  for (size_t i = 0; i < values.size(); i++) result += (i > 0 ? "," : "") + values[i];
  return result;
}

std::vector<std::string> split(const std::string& text) {
  std::vector<std::string> values;
  std::string value;
  std::istringstream stream(text);
  while (std::getline(stream, value, ',')) values.push_back(value);
  return values;
}

// ---------------------------------------------------------------------------------------------
// Helpers for the C interface
// ---------------------------------------------------------------------------------------------

thread_local std::string last_error;

// Runs `f`, and turns a C++ exception into an error: none may cross the boundary.
template <typename F>
int guarded(F&& f) {
  try {
    f();
    return 0;
  } catch (const std::exception& e) {
    last_error = e.what();
    return 1;
  }
}

std::vector<std::string> toStrings(const char* const* values, int64_t count) {
  return std::vector<std::string>(values, values + count);
}

std::string toBytes(const uint8_t* data, int64_t length) {
  return std::string(reinterpret_cast<const char*>(data), static_cast<size_t>(length));
}

void sendBytes(SparkDsSink sink_function, void* sink, const std::string& bytes) {
  sink_function(sink, reinterpret_cast<const uint8_t*>(bytes.data()),
                static_cast<int64_t>(bytes.size()));
}

[[maybe_unused]]
std::vector<std::string> toByteStrings(const uint8_t* const* messages, const int64_t* lengths,
                                       int64_t count, bool* has_null) {
  std::vector<std::string> result;
  for (int64_t i = 0; i < count; i++) {
    if (messages[i] == nullptr) {
      *has_null = true;
    } else {
      result.push_back(toBytes(messages[i], lengths[i]));
    }
  }
  return result;
}

template <typename T>
T* fromHandle(void* handle) {
  return static_cast<T*>(handle);
}

// ---------------------------------------------------------------------------------------------
// Data sources
// ---------------------------------------------------------------------------------------------

std::string name(const std::string& base) { return std::string(NAME_PREFIX) + base; }

struct DataSource {
  std::string name;
  Options options;
};

void maybeFail(const Options& options, const std::string& step) {
  if (get(options, "fail", "") == step) throw std::runtime_error("injected failure in " + step);
}

// Whether the given function succeeds without setting its handle, with the option "zero".
bool returnsZero(const Options& options, const std::string& function) {
  return get(options, "zero", "") == function;
}

// The driver-side reader of native_range and native_counter.
struct Reader {
  Options state;  // Serialized to the executors.
};

// Reads the ids [lo, hi) of the columns in the state, in batches.
struct Stream {
  std::vector<std::string> columns;
  int64_t next;
  int64_t end;
  int64_t batch_size;
  bool fail;
  bool slice;
  std::string short_array;
  std::string last_error;
};

int streamGetSchema(ArrowArrayStream* stream, ArrowSchema* out) {
  auto* state = static_cast<Stream*>(stream->private_data);
  exportSchema(state->columns, out);
  return 0;
}

int streamGetNext(ArrowArrayStream* stream, ArrowArray* out) {
  auto* state = static_cast<Stream*>(stream->private_data);
  if (state->fail) {
    state->last_error = "injected failure in get_next";
    return EIO;
  }
  if (state->next >= state->end) {
    out->release = nullptr;  // The end of the stream.
    return 0;
  }
  int64_t hi = std::min(state->end, state->next + state->batch_size);
  exportBatch(state->columns, state->next, hi, state->slice, state->short_array, out);
  state->next = hi;
  return 0;
}

const char* streamGetLastError(ArrowArrayStream* stream) {
  return static_cast<Stream*>(stream->private_data)->last_error.c_str();
}

void streamRelease(ArrowArrayStream* stream) {
  delete static_cast<Stream*>(stream->private_data);
  stream->release = nullptr;
}

// The driver-side writer of native_sink.
struct Writer {
  Options state;  // path, overwrite, fail.
  // The file that closeWriter creates, so that the tests can check that the writer of a committed
  // or aborted write is closed: _CLOSED for a batch write, _CLOSED_<epoch> for a streaming one.
  std::string closed_marker;
};

[[maybe_unused]] std::string closedMarker(int64_t epoch_id) {
  return epoch_id < 0 ? "/_CLOSED" : "/_CLOSED_" + std::to_string(epoch_id);
}

// Writes the rows of a task as CSV lines to a temporary file, renamed on commit.
struct DataWriter {
  std::string temp_path;
  FILE* file;
  Options state;
};

[[maybe_unused]] void makeDirs(const std::string& path) {
  for (size_t pos = path.find('/', 1); pos != std::string::npos; pos = path.find('/', pos + 1)) {
    mkdir(path.substr(0, pos).c_str(), 0755);
  }
  mkdir(path.c_str(), 0755);
}

[[maybe_unused]] bool isValid(const ArrowArray* array, int64_t i) {
  if (array->null_count == 0 || array->buffers[0] == nullptr) return true;
  int64_t bit = array->offset + i;
  return (static_cast<const uint8_t*>(array->buffers[0])[bit / 8] >> (bit % 8)) & 1;
}

// Formats the value at row `i` of a column, or "null". The helpers below are only used by
// writes, which READ_ONLY leaves out.
[[maybe_unused]]
std::string formatValue(const ArrowSchema* schema, const ArrowArray* array, int64_t i) {
  if (!isValid(array, i)) return "null";
  std::string format = schema->format;
  int64_t row = array->offset + i;
  if (format == "l" || format.rfind("ts", 0) == 0) {
    return std::to_string(static_cast<const int64_t*>(array->buffers[1])[row]);
  } else if (format == "i" || format == "tdD") {
    return std::to_string(static_cast<const int32_t*>(array->buffers[1])[row]);
  } else if (format == "g") {
    std::ostringstream out;
    out << static_cast<const double*>(array->buffers[1])[row];
    return out.str();
  } else if (format == "b") {
    return ((static_cast<const uint8_t*>(array->buffers[1])[row / 8] >> (row % 8)) & 1)
               ? "true" : "false";
  } else if (format == "u") {
    const int32_t* offsets = static_cast<const int32_t*>(array->buffers[1]);
    const char* chars = static_cast<const char*>(array->buffers[2]);
    return std::string(chars + offsets[row], static_cast<size_t>(offsets[row + 1] - offsets[row]));
  }
  throw std::runtime_error("unsupported Arrow format " + format);
}

[[maybe_unused]]
std::vector<std::string> listFiles(const std::string& dir, const std::string& prefix) {
  std::vector<std::string> files;
  DIR* handle = opendir(dir.c_str());
  if (handle == nullptr) return files;
  while (dirent* entry = readdir(handle)) {
    std::string file = entry->d_name;
    if (file.rfind(prefix, 0) == 0) files.push_back(file);
  }
  closedir(handle);
  return files;
}

[[maybe_unused]] void writeFile(const std::string& path, const std::string& content) {
  FILE* file = std::fopen(path.c_str(), "w");
  if (file == nullptr) throw std::runtime_error("cannot write " + path);
  std::fputs(content.c_str(), file);
  std::fclose(file);
}

}  // namespace


// ---------------------------------------------------------------------------------------------
// spark_datasource.h
// ---------------------------------------------------------------------------------------------

extern "C" {

#ifndef NO_LAST_ERROR
const char* spark_ds_last_error(void) { return last_error.c_str(); }
#endif

int32_t spark_ds_abi_version(void) { return ABI_VERSION; }

int spark_ds_create(const char* name_value, const char* const* option_keys,
                    const char* const* option_values, int64_t num_options, void** handle) {
  return guarded([&] {
    auto source = std::make_unique<DataSource>(DataSource{name_value, {}});
    auto keys = toStrings(option_keys, num_options);
    auto values = toStrings(option_values, num_options);
    for (size_t i = 0; i < keys.size(); i++) source->options[keys[i]] = values[i];
    maybeFail(source->options, "create");
    if (source->name != name("native_range") && source->name != name("native_sink") &&
        source->name != name("native_counter")) {
      throw std::runtime_error("unknown data source " + source->name);
    }
    if (returnsZero(source->options, "spark_ds_create")) return;
    *handle = source.release();
  });
}

int spark_ds_schema(void* handle, struct ArrowSchema* out) {
  return guarded([&] {
    auto* source = fromHandle<DataSource>(handle);
    maybeFail(source->options, "schema");
    if (source->name == name("native_sink")) {
      throw std::runtime_error("native_sink has no schema; it can only be written");
    }
    bool counter = source->name == name("native_counter");
    exportSchema(allColumns(counter, get(source->options, "extra", "")), out);
  });
}

int spark_ds_create_reader(void* handle, struct ArrowSchema* schema, void** out) {
  return guarded([&] {
    auto* source = fromHandle<DataSource>(handle);
    if (source->name != name("native_range")) {
      throw std::runtime_error(source->name + " does not support batch reads");
    }
    auto columns = importColumns(schema);
    for (const auto& column : columns) findColumn(column);
    if (returnsZero(source->options, "spark_ds_create_reader")) return;
    auto* reader = new Reader();
    reader->state["columns"] = join(columns);
    reader->state["lo"] = "0";
    reader->state["hi"] = get(source->options, "end", "10");
    reader->state["partitions"] = get(source->options, "partitions", "2");
    reader->state["batch_size"] = get(source->options, "batch_size", "1000");
    reader->state["fail"] = get(source->options, "fail", "");
    reader->state["slice"] = get(source->options, "slice", "");
    reader->state["short"] = get(source->options, "short", "");
    reader->state["claim_predicates"] = get(source->options, "claim_predicates", "");
    *out = reader;
  });
}

#ifndef READ_ONLY

int spark_ds_create_stream_reader(void* handle, struct ArrowSchema* schema, void** out) {
  return guarded([&] {
    auto* source = fromHandle<DataSource>(handle);
    if (source->name != name("native_counter")) {
      throw std::runtime_error(source->name + " does not support streaming reads");
    }
    auto columns = join(importColumns(schema));
    if (returnsZero(source->options, "spark_ds_create_stream_reader")) return;
    auto* reader = new Reader();
    reader->state["columns"] = columns;
    reader->state["max_offset"] = get(source->options, "max_offset", "10");
    reader->state["step"] = get(source->options, "step", reader->state["max_offset"]);
    reader->state["current"] = "0";
    reader->state["batch_size"] = get(source->options, "batch_size", "1000");
    reader->state["commit_path"] = get(source->options, "commit_path", "");
    reader->state["bad_offset"] = get(source->options, "bad_offset", "");
    reader->state["closed_path"] = get(source->options, "closed_path", "");
    *out = reader;
  });
}

#endif  // READ_ONLY

void spark_ds_close(void* handle) { delete fromHandle<DataSource>(handle); }

#ifndef READ_ONLY

int spark_ds_push_predicates(void* handle, const char* const* predicates, int64_t count,
                             bool* evaluated) {
  return guarded([&] {
    auto* reader = fromHandle<Reader>(handle);
    auto json = toStrings(predicates, count);
    int64_t lo = getLong(reader->state, "lo", 0);
    int64_t hi = getLong(reader->state, "hi", 0);
    // With the option "claim_predicates", reports every predicate as evaluated completely, but
    // evaluates none of them.
    if (reader->state["claim_predicates"] == "true") {
      for (size_t i = 0; i < json.size(); i++) evaluated[i] = true;
      return;
    }
    for (size_t i = 0; i < json.size(); i++) {
      // Accepts "id <op> <integer literal>" for the comparison operators.
      Json predicate = JsonParser(json[i]).parse();
      const Json& children = predicate["children"];
      std::string op = predicate["name"].text;
      bool comparison = op == ">" || op == ">=" || op == "<" || op == "<=" || op == "=";
      bool ok = ID_SIGN == 1 && predicate["type"].text == "function" && comparison &&
                children.items.size() == 2 && children.items[0]["type"].text == "column" &&
                children.items[0]["name"].items.size() == 1 &&
                children.items[0]["name"].items[0].text == "id" &&
                children.items[1]["type"].text == "literal" &&
                children.items[1]["value"].kind == Json::kNumber;
      if (ok) {
        auto value = static_cast<int64_t>(children.items[1]["value"].number);
        if (op == ">") lo = std::max(lo, value + 1);
        if (op == ">=") lo = std::max(lo, value);
        if (op == "<") hi = std::min(hi, value);
        if (op == "<=") hi = std::min(hi, value + 1);
        if (op == "=") {
          lo = std::max(lo, value);
          hi = std::min(hi, value + 1);
        }
      }
      evaluated[i] = ok;
    }
    reader->state["lo"] = std::to_string(lo);
    reader->state["hi"] = std::to_string(std::max(lo, hi));
    reader->state["predicates"] = std::to_string(json.size());
  });
}

int spark_ds_push_limit(void* handle, int32_t limit, bool* applied) {
  return guarded([&] {
    auto* reader = fromHandle<Reader>(handle);
    // Spark calls spark_ds_push_limit at most once.
    if (reader->state.count("limit") > 0) throw std::runtime_error("pushLimit was called twice");
    reader->state["limit"] = std::to_string(limit);
    *applied = true;
  });
}

int spark_ds_prune_columns(void* handle, const char* const* columns, int64_t count,
                           bool* pruned) {
  return guarded([&] {
    auto names = toStrings(columns, count);
    for (const auto& column : names) findColumn(column);
    fromHandle<Reader>(handle)->state["columns"] = join(names);
    *pruned = true;
  });
}

#endif  // READ_ONLY

int spark_ds_partitions(void* handle, SparkDsSink add_partition, void* sink) {
  return guarded([&] {
    auto* reader = fromHandle<Reader>(handle);
    maybeFail(reader->state, "plan");
    int64_t lo = getLong(reader->state, "lo", 0);
    int64_t hi = getLong(reader->state, "hi", 0);
    int64_t count = getLong(reader->state, "partitions", 1);
    if (reader->state.count("limit") > 0) {
      // A pushed limit is applied by reading a single partition with at most `limit` rows.
      hi = std::min(hi, lo + getLong(reader->state, "limit", 0));
      count = 1;
    }
    int64_t step = std::max<int64_t>(1, (hi - lo + count - 1) / count);
    for (int64_t start = lo; start < hi; start += step) {
      int64_t stop = std::min(hi, start + step);
      sendBytes(add_partition, sink, std::to_string(start) + "," + std::to_string(stop));
    }
  });
}

int spark_ds_serialize_reader(void* handle, SparkDsSink set_state, void* sink) {
  return guarded([&] { sendBytes(set_state, sink, serialize(fromHandle<Reader>(handle)->state)); });
}

void spark_ds_close_reader(void* handle) { delete fromHandle<Reader>(handle); }

#ifndef READ_ONLY

int spark_ds_initial_offset(void*, SparkDsSink set_offset, void* sink) {
  return guarded([&] { sendBytes(set_offset, sink, "{\"offset\":0}"); });
}

// Each call makes `step` more rows available, up to `max_offset`.
int spark_ds_latest_offset(void* handle, SparkDsSink set_offset, void* sink) {
  return guarded([&] {
    Options& state = fromHandle<Reader>(handle)->state;
    int64_t latest = std::min(getLong(state, "max_offset", 10),
                              getLong(state, "current", 0) + getLong(state, "step", 10));
    state["current"] = std::to_string(latest);
    std::string bad_offset = get(state, "bad_offset", "");
    if (bad_offset == "none") return;
    if (bad_offset == "multiline") {
      // Pretty-printed JSON, as serde_json::to_string_pretty writes it.
      sendBytes(set_offset, sink, "{\n  \"offset\": " + std::to_string(latest) + "\n}");
    } else if (bad_offset == "invalid") {
      sendBytes(set_offset, sink, "offset " + std::to_string(latest));
    } else {
      sendBytes(set_offset, sink, "{\"offset\":" + std::to_string(latest) + "}");
    }
  });
}

int spark_ds_stream_partitions(void*, const char* start, const char* end,
                               SparkDsSink add_partition, void* sink) {
  return guarded([&] {
    auto lo = static_cast<int64_t>(JsonParser(start).parse()["offset"].number);
    auto hi = static_cast<int64_t>(JsonParser(end).parse()["offset"].number);
    if (hi > lo) sendBytes(add_partition, sink, std::to_string(lo) + "," + std::to_string(hi));
  });
}

int spark_ds_serialize_stream_reader(void* handle, SparkDsSink set_state, void* sink) {
  return guarded([&] { sendBytes(set_state, sink, serialize(fromHandle<Reader>(handle)->state)); });
}

int spark_ds_commit_offset(void* handle, const char* end) {
  return guarded([&] {
    std::string path = get(fromHandle<Reader>(handle)->state, "commit_path", "");
    if (!path.empty()) writeFile(path, end);
  });
}

void spark_ds_close_stream_reader(void* handle) {
  std::unique_ptr<Reader> reader(fromHandle<Reader>(handle));
  guarded([&] {
    std::string path = get(reader->state, "closed_path", "");
    if (!path.empty()) writeFile(path, "");
  });
}

#endif  // READ_ONLY

int spark_ds_read(const uint8_t* reader_state, int64_t reader_state_length,
                  const uint8_t* partition, int64_t partition_length,
                  struct ArrowArrayStream* stream) {
  return guarded([&] {
    Options state = deserialize(toBytes(reader_state, reader_state_length));
    maybeFail(state, "open");
    // Returns success without initializing the stream, which Spark must reject.
    if (get(state, "fail", "") == "uninitialized") return;
    auto bounds = split(toBytes(partition, partition_length));
    if (bounds.size() != 2) throw std::runtime_error("invalid partition");
    stream->get_schema = streamGetSchema;
    stream->get_next = streamGetNext;
    stream->get_last_error = streamGetLastError;
    stream->release = streamRelease;
    stream->private_data = new Stream{split(get(state, "columns", "")), std::stoll(bounds[0]),
                                      std::stoll(bounds[1]), getLong(state, "batch_size", 1000),
                                      get(state, "fail", "") == "read",
                                      get(state, "slice", "") == "true", get(state, "short", ""),
                                      ""};
  });
}

#ifndef READ_ONLY

}  // extern "C"

namespace {

// Creates the writer of a batch write, or of a streaming write with the ID of its query.
int createAnyWriter(const char* function, void* handle, struct ArrowSchema* schema,
                    bool overwrite, const char* query_id, void** out) {
  return guarded([&] {
    auto* source = fromHandle<DataSource>(handle);
    if (source->name != name("native_sink")) {
      throw std::runtime_error(source->name + " does not support writes");
    }
    // Spark releases the schema after the call, since it is not moved here.
    if (schema->n_children == 0) throw std::runtime_error("no columns to write");
    // The location of a table in a catalog is passed as a URI.
    std::string path = get(source->options, "path", "");
    if (path.rfind("file://", 0) == 0) {
      path = path.substr(7);
    } else if (path.rfind("file:", 0) == 0) {
      path = path.substr(5);
    }
    if (path.empty()) throw std::runtime_error("the option 'path' is required");
    if (returnsZero(source->options, function)) return;
    auto* writer = new Writer();
    writer->state["path"] = path;
    writer->state["overwrite"] = overwrite ? "true" : "false";
    writer->state["fail"] = get(source->options, "fail", "");
    writer->state["zero"] = get(source->options, "zero", "");
    // Records that a writer was created, which Spark then commits or aborts, and the ID of the
    // streaming query.
    makeDirs(path);
    writeFile(path + "/_CREATED", "");
    if (query_id != nullptr) writeFile(path + "/_QUERY_ID", query_id);
    *out = writer;
  });
}

}  // namespace

extern "C" {

int spark_ds_create_writer(void* handle, struct ArrowSchema* schema, bool overwrite,
                           void** writer) {
  return createAnyWriter("spark_ds_create_writer", handle, schema, overwrite, nullptr, writer);
}

int spark_ds_create_stream_writer(void* handle, struct ArrowSchema* schema, bool overwrite,
                                  const char* query_id, void** writer) {
  return createAnyWriter("spark_ds_create_stream_writer", handle, schema, overwrite, query_id,
                         writer);
}

int spark_ds_serialize_writer(void* handle, SparkDsSink set_state, void* sink) {
  return guarded([&] { sendBytes(set_state, sink, serialize(fromHandle<Writer>(handle)->state)); });
}

int spark_ds_commit(void* handle, int64_t epoch_id, const uint8_t* const* messages,
                    const int64_t* message_lengths, int64_t num_messages) {
  return guarded([&] {
    auto* writer = fromHandle<Writer>(handle);
    std::string path = writer->state["path"];
    maybeFail(writer->state, "commit");
    bool has_null = false;
    auto temp_files = toByteStrings(messages, message_lengths, num_messages, &has_null);
    if (has_null) throw std::runtime_error("commit got a null message");
    if (writer->state["overwrite"] == "true") {
      for (const auto& file : listFiles(path, "part-")) unlink((path + "/" + file).c_str());
    }
    for (const auto& temp_file : temp_files) {
      std::string file_name = temp_file.substr(temp_file.rfind('/') + 1);
      std::rename(temp_file.c_str(), (path + "/part-" + file_name).c_str());
    }
    writeFile(path + (epoch_id < 0 ? "/_SUCCESS" : "/_EPOCH_" + std::to_string(epoch_id)),
              std::to_string(temp_files.size()));
    writer->closed_marker = closedMarker(epoch_id);
  });
}

// Records the aborted write in _ABORTED for a batch write, or _ABORTED_<epoch> for a streaming
// one, with the number of tasks that did not commit, whose messages are NULL.
int spark_ds_abort(void* handle, int64_t epoch_id, const uint8_t* const* messages,
                   const int64_t* message_lengths, int64_t num_messages) {
  return guarded([&] {
    auto* writer = fromHandle<Writer>(handle);
    writer->closed_marker = closedMarker(epoch_id);
    bool has_null = false;
    for (const auto& temp_file :
         toByteStrings(messages, message_lengths, num_messages, &has_null)) {
      unlink(temp_file.c_str());
    }
    int64_t null_messages = 0;
    for (int64_t i = 0; i < num_messages; i++) {
      if (messages[i] == nullptr) null_messages++;
    }
    writeFile(writer->state["path"] +
                  (epoch_id < 0 ? "/_ABORTED" : "/_ABORTED_" + std::to_string(epoch_id)),
              std::to_string(null_messages));
  });
}

void spark_ds_close_writer(void* handle) {
  std::unique_ptr<Writer> writer(fromHandle<Writer>(handle));
  guarded([&] {
    if (!writer->closed_marker.empty()) {
      writeFile(writer->state["path"] + writer->closed_marker, "");
    }
  });
}

int spark_ds_create_data_writer(const uint8_t* writer_state, int64_t writer_state_length,
                                int32_t partition_id, int64_t task_id, int64_t epoch_id,
                                void** out) {
  return guarded([&] {
    Options state = deserialize(toBytes(writer_state, writer_state_length));
    if (returnsZero(state, "spark_ds_create_data_writer")) return;
    std::string dir = state["path"] + "/_temporary";
    makeDirs(dir);
    std::string temp_path = dir + "/" + std::to_string(epoch_id) + "-" +
                            std::to_string(partition_id) + "-" + std::to_string(task_id) + ".csv";
    FILE* file = std::fopen(temp_path.c_str(), "w");
    if (file == nullptr) throw std::runtime_error("cannot write " + temp_path);
    *out = new DataWriter{temp_path, file, state};
  });
}

int spark_ds_write(void* handle, struct ArrowArray* array, struct ArrowSchema* schema) {
  return guarded([&] {
    auto* writer = fromHandle<DataWriter>(handle);
    // The batch is released here, so Spark does not release it after the call.
    std::unique_ptr<ArrowArray, void (*)(ArrowArray*)> array_owner(
        array, [](ArrowArray* a) { a->release(a); });
    std::unique_ptr<ArrowSchema, void (*)(ArrowSchema*)> schema_owner(
        schema, [](ArrowSchema* s) { s->release(s); });
    maybeFail(writer->state, "write");
    for (int64_t i = 0; i < array->length; i++) {
      std::string line;
      for (int64_t c = 0; c < array->n_children; c++) {
        if (c > 0) line += ",";
        line += formatValue(schema->children[c], array->children[c], array->offset + i);
      }
      line += "\n";
      std::fputs(line.c_str(), writer->file);
    }
  });
}

int spark_ds_commit_data_writer(void* handle, SparkDsSink set_message, void* sink) {
  return guarded([&] {
    auto* writer = fromHandle<DataWriter>(handle);
    // If the commit fails, the data writer stays valid: Spark aborts it with
    // spark_ds_abort_data_writer.
    maybeFail(writer->state, "commitDataWriter");
    sendBytes(set_message, sink, writer->temp_path);
    std::fclose(writer->file);
    delete writer;
  });
}

int spark_ds_abort_data_writer(void* handle) {
  std::unique_ptr<DataWriter> writer(fromHandle<DataWriter>(handle));
  std::fclose(writer->file);
  unlink(writer->temp_path.c_str());
  return 0;
}

#endif  // READ_ONLY

}  // extern "C"
