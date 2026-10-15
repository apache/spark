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
// spark_datasource.h without any other dependency, building the Arrow C data interface structs and
// the Protocol Buffers messages of spark_datasource.proto by hand. It provides three data sources:
//
//   native_range    Reads rows [0, end) with columns of several types. Supports predicate,
//                   column and limit pushdown. With the option "claim_predicates", it reports
//                   all the predicates as evaluated completely, but ignores them. With the
//                   option "predicate_use", it reports the predicates that it evaluates with
//                   another value: "partial" for PREDICATE_USE_PARTIAL, as a reader that
//                   reads the blocks of 10 rows that may match, or a number. With the option
//                   "extra", the schema has one more column: "map" adds a map whose entry fields
//                   are named "keys" and "values", as arrow-rs names them, "struct" adds a
//                   struct, "decimal256" adds a decimal256, "listview" adds a list view,
//                   "largelist" adds a large list, and "dictionary" adds a dictionary-encoded
//                   string column, whose data Spark rejects before reading it. With the option
//                   "slice", the columns of the batches have an offset of 1. With the option
//                   "short", an array of the batches has one value fewer than its parent refers
//                   to, although its buffers hold them all: "id" shortens the column id,
//                   "entries" the entries of the map and their fields, and "values" the values
//                   of the entries. With the option "wait_for_interrupt", the stream of a
//                   partition creates the file <wait_for_interrupt>.waiting, waits until its task
//                   is killed, and then creates the file <wait_for_interrupt> and fails.
//   native_sink     Writes the rows as CSV files under the directory in the option "path". With
//                   the option "commit_messages", a data writer passes its commit message that
//                   many times. Creating and closing a data writer create a file named after
//                   it in _created_data_writers and _closed_data_writers.
//   native_counter  Streams the rows [0, max_offset) with a single column "id", making "step"
//                   more rows available each time Spark asks for the latest offset. With the
//                   option "bad_offset", the latest offset is "none", "multiline" JSON or
//                   "invalid" JSON. With the option "closed_path", closing the stream reader
//                   creates that file.
//
// native_range supports batch reads, native_counter micro-batch streaming reads and native_sink
// batch and streaming writes, which may replace the existing data. With the option
// "capabilities", a data source declares these capabilities instead, as the numbers of the enum
// Capability separated by commas. With the option "task_path", the reads and the data writers of
// native_range and native_sink record their task in a file named after its partition, in that
// directory. With the option "log", opening a data source logs its value as a warning, and with
// the option "spark_version", it fails unless Spark has that version. With the option
// "unknown_fields", every reply has a field that Spark does not know.
//
// With the option "fail", a step fails with an error. With the option "zero", opening an object
// succeeds without setting its handle: "create" for a data source, "create_reader",
// "create_stream_reader", "create_writer" or "create_data_writer".
//
// The suite compiles it with these macros to get several distinct libraries:
//
//   NAME_PREFIX     A prefix for the names of the data sources, "" by default.
//   ID_SIGN         Multiplies the ids produced by native_range, 1 by default.
//   READ_ONLY       Leaves out writes, streaming reads and pushdown: the replies to the pushdown
//                   requests are empty.
//   ABI_VERSION     The value of spark_ds_abi_version, 1 by default.
//   NO_LAST_ERROR   Leaves out spark_ds_last_error.
//   NO_OPEN         Leaves out spark_ds_open, which makes the library invalid.

#include "spark_datasource.h"

#include <algorithm>
#include <cctype>
#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <map>
#include <memory>
#include <mutex>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
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

// Spark, as spark_ds_open got it.
const SparkDsHost* host = nullptr;

// Logs with the log of Spark. A library must only use the fields that fit in the size of the
// struct that Spark passes, which may be of another version of the header.
void hostLog(int32_t level, const std::string& message) {
  if (host->size >= static_cast<int64_t>(offsetof(SparkDsHost, log) + sizeof(host->log))) {
    host->log(level, message.c_str());
  }
}

// Whether the task with the given ID was killed.
bool isInterrupted(int64_t task_attempt_id) {
  return host->size >= static_cast<int64_t>(offsetof(SparkDsHost, is_interrupted) +
                                            sizeof(host->is_interrupted)) &&
         host->is_interrupted(task_attempt_id);
}

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

std::string toBytes(const uint8_t* data, int64_t length) {
  return std::string(reinterpret_cast<const char*>(data), static_cast<size_t>(length));
}

void sendBytes(SparkDsSink sink_function, void* sink, const std::string& bytes) {
  sink_function(sink, reinterpret_cast<const uint8_t*>(bytes.data()),
                static_cast<int64_t>(bytes.size()));
}

// ---------------------------------------------------------------------------------------------
// Data sources
// ---------------------------------------------------------------------------------------------

std::string name(const std::string& base) { return std::string(NAME_PREFIX) + base; }

void maybeFail(const Options& options, const std::string& step) {
  if (get(options, "fail", "") == step) throw std::runtime_error("injected failure in " + step);
}

// Whether the given function succeeds without setting its handle, with the option "zero".
bool returnsZero(const Options& options, const std::string& function) {
  return get(options, "zero", "") == function;
}

void writeFile(const std::string& path, const std::string& content) {
  FILE* file = std::fopen(path.c_str(), "w");
  if (file == nullptr) throw std::runtime_error("cannot write " + path);
  std::fputs(content.c_str(), file);
  std::fclose(file);
}

void makeDirs(const std::string& path) {
  for (size_t pos = path.find('/', 1); pos != std::string::npos; pos = path.find('/', pos + 1)) {
    mkdir(path.substr(0, pos).c_str(), 0755);
  }
  mkdir(path.c_str(), 0755);
}

// Reads the ids [lo, hi) of the columns in the state, in batches.
struct Stream {
  std::vector<std::string> columns;
  int64_t next;
  int64_t end;
  int64_t batch_size;
  bool fail;
  bool slice;
  std::string short_array;
  int64_t task_attempt_id;
  std::string wait_for_interrupt;
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
  if (!state->wait_for_interrupt.empty()) {
    // Waits, as for data that does not come, until the task is killed, for at most a minute.
    writeFile(state->wait_for_interrupt + ".waiting", "");
    for (int i = 0; i < 6000 && !isInterrupted(state->task_attempt_id); i++) {
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    bool interrupted = isInterrupted(state->task_attempt_id);
    writeFile(state->wait_for_interrupt, interrupted ? "interrupted" : "timed out");
    state->last_error = "the task was killed";
    return ECANCELED;
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

// ---------------------------------------------------------------------------------------------
// Messages
// ---------------------------------------------------------------------------------------------

// A minimal Protocol Buffers codec for the messages of spark_datasource.proto, so that the test
// library needs nothing but a C++ compiler. A library would rather use the code that a Protocol
// Buffers compiler generates from spark_datasource.proto.
class ProtoReader {
 public:
  // Keeps the bytes of the message, which are often a temporary string.
  explicit ProtoReader(std::string bytes)
      : bytes_(std::move(bytes)),
        p_(reinterpret_cast<const uint8_t*>(bytes_.data())),
        end_(p_ + bytes_.size()) {}

  // Moves to the next field, and returns false at the end of the message.
  bool next() {
    if (p_ == end_) return false;
    uint64_t key = varint();
    field_ = static_cast<int>(key >> 3);
    wire_type_ = static_cast<int>(key & 7);
    return true;
  }

  int field() const { return field_; }

  uint64_t varint() {
    uint64_t value = 0;
    for (int shift = 0; shift < 64; shift += 7) {
      if (p_ == end_) throw std::runtime_error("truncated message");
      uint8_t byte = *p_++;
      value |= static_cast<uint64_t>(byte & 0x7f) << shift;
      if ((byte & 0x80) == 0) return value;
    }
    throw std::runtime_error("invalid varint");
  }

  int64_t int64() { return static_cast<int64_t>(varint()); }

  // A string, bytes or a message.
  std::string bytes() {
    uint64_t length = varint();
    if (length > static_cast<uint64_t>(end_ - p_)) throw std::runtime_error("truncated message");
    std::string value(reinterpret_cast<const char*>(p_), static_cast<size_t>(length));
    p_ += length;
    return value;
  }

  // Skips a field that the library does not know, as Protocol Buffers requires.
  void skip() {
    switch (wire_type_) {
      case 0: varint(); break;
      case 1: advance(8); break;
      case 2: bytes(); break;
      case 5: advance(4); break;
      default: throw std::runtime_error("unsupported wire type " + std::to_string(wire_type_));
    }
  }

 private:
  void advance(size_t n) {
    if (n > static_cast<size_t>(end_ - p_)) throw std::runtime_error("truncated message");
    p_ += n;
  }

  std::string bytes_;
  const uint8_t* p_;
  const uint8_t* end_;
  int field_ = 0;
  int wire_type_ = 0;
};

class ProtoWriter {
 public:
  ProtoWriter& varint(int field, uint64_t value) {
    key(field, 0);
    raw(value);
    return *this;
  }

  ProtoWriter& bytes(int field, const std::string& value) {
    key(field, 2);
    raw(value.size());
    out_ += value;
    return *this;
  }

  ProtoWriter& message(int field, const ProtoWriter& value) { return bytes(field, value.out_); }

  // A packed repeated field of enums.
  ProtoWriter& packed(int field, const std::vector<int32_t>& values) {
    ProtoWriter packed;
    for (int32_t value : values) packed.raw(static_cast<uint64_t>(static_cast<int64_t>(value)));
    return bytes(field, packed.out_);
  }

  const std::string& str() const { return out_; }

 private:
  void key(int field, int wire_type) {
    raw((static_cast<uint64_t>(field) << 3) | static_cast<uint64_t>(wire_type));
  }

  void raw(uint64_t value) {
    while (value >= 0x80) {
      out_ += static_cast<char>((value & 0x7f) | 0x80);
      value >>= 7;
    }
    out_ += static_cast<char>(value);
  }

  std::string out_;
};

// The fields of the messages, by their number in spark_datasource.proto.
enum OpenField { kSparkVersion = 1, kDataSource = 10, kBatchReader, kStreamReader, kWriter,
                 kPartitionReader, kDataWriter };
enum CallField { kInferSchema = 1, kPushPredicates, kPushLimit, kPruneColumns, kPlanPartitions,
                 kSerialize, kInitialOffset, kLatestOffset, kPlanStreamPartitions, kCommitOffset,
                 kCommit, kAbort, kCommitDataWriter, kAbortDataWriter };
enum StreamField { kRead = 1, kWrite };
enum Capability { kBatchRead = 1, kMicroBatchRead, kBatchWrite, kStreamingWrite, kTruncate };
enum PredicateUse { kUnused = 0, kPartial, kComplete };
enum WriteMode { kAppend = 0, kTruncateMode };

// With the option "unknown_fields", every reply has a field that Spark does not know, which it
// must ignore.
std::string withUnknownField(const Options& options, const ProtoWriter& reply) {
  ProtoWriter result = reply;
  if (get(options, "unknown_fields", "") == "true") result.bytes(1000, "unknown");
  return result.str();
}

void sendReply(SparkDsSink reply, void* sink, const Options& options, const ProtoWriter& message) {
  sendBytes(reply, sink, withUnknownField(options, message));
}

// The fields of a message, by number: the last value of each field.
std::map<int, std::string> parseMessage(const std::string& bytes) {
  std::map<int, std::string> fields;
  ProtoReader reader(bytes);
  while (reader.next()) {
    // The fields that the library reads are strings, bytes and messages, or varints, which are
    // returned as decimal numbers.
    int field = reader.field();
    try {
      fields[field] = reader.bytes();
    } catch (const std::exception&) {
      throw std::runtime_error("unexpected field " + std::to_string(field));
    }
  }
  return fields;
}

// The varints of a message, by number.
std::map<int, uint64_t> parseVarints(const std::string& bytes) {
  std::map<int, uint64_t> fields;
  ProtoReader reader(bytes);
  while (reader.next()) {
    int field = reader.field();
    fields[field] = reader.varint();
  }
  return fields;
}

// A TaskContext message.
struct TaskInfo {
  int64_t stage_id = 0;
  int64_t stage_attempt_number = 0;
  int64_t partition_id = 0;
  int64_t attempt_number = 0;
  int64_t task_attempt_id = 0;
};

TaskInfo parseTask(const std::string& bytes) {
  auto fields = parseVarints(bytes);
  auto field = [&](int number) {
    return static_cast<int64_t>(static_cast<int32_t>(fields.count(number) ? fields[number] : 0));
  };
  TaskInfo task;
  task.stage_id = field(1);
  task.stage_attempt_number = field(2);
  task.partition_id = field(3);
  task.attempt_number = field(4);
  task.task_attempt_id = fields.count(5) ? static_cast<int64_t>(fields[5]) : 0;
  return task;
}

// The names of the columns of a StructType message.
std::vector<std::string> parseColumns(const std::string& struct_type) {
  std::vector<std::string> columns;
  ProtoReader fields(struct_type);
  while (fields.next()) {
    if (fields.field() != 1) {
      fields.skip();
      continue;
    }
    ProtoReader field(fields.bytes());
    std::string name;
    while (field.next()) {
      if (field.field() == 1) {
        name = field.bytes();
      } else {
        field.skip();
      }
    }
    columns.push_back(name);
  }
  return columns;
}

// A DataType message of a type without parameters, by the number of its kind.
ProtoWriter simpleType(int kind) { return ProtoWriter().message(kind, ProtoWriter()); }

// The DataType message of a column, as Spark infers it from the Arrow type: Spark cannot express
// some of the Arrow types of the extra columns, whose data it rejects.
ProtoWriter columnType(const std::string& name) {
  enum { kInteger = 5, kLong, kDouble = 8, kDecimal, kString, kBoolean = 2, kDate = 12,
         kTimestamp, kArray = 18, kMap, kStruct };
  auto field = [](const std::string& field_name, const ProtoWriter& type) {
    return ProtoWriter().bytes(1, field_name).message(2, type).varint(3, 1);
  };
  std::string format = findColumn(name)->format;
  if (name == "m") {
    return ProtoWriter().message(kMap, ProtoWriter()
        .message(1, ProtoWriter().message(kString, ProtoWriter()))
        .message(2, simpleType(kLong))
        .varint(3, 1));
  } else if (name == "st") {
    return ProtoWriter().message(kStruct, ProtoWriter()
        .message(1, field("a", simpleType(kLong)))
        .message(1, field("b", ProtoWriter().message(kString, ProtoWriter()))));
  } else if (name == "lv" || name == "ll") {
    return ProtoWriter().message(kArray,
        ProtoWriter().message(1, simpleType(kLong)).varint(2, 1));
  } else if (name == "dict" || format == "u") {
    return ProtoWriter().message(kString, ProtoWriter());
  } else if (format == "l") {
    return simpleType(kLong);
  } else if (format == "i") {
    return simpleType(kInteger);
  } else if (format == "g") {
    return simpleType(kDouble);
  } else if (format == "b") {
    return simpleType(kBoolean);
  } else if (format == "tdD") {
    return simpleType(kDate);
  } else if (format.rfind("ts", 0) == 0) {
    return simpleType(kTimestamp);
  } else if (format.rfind("d:", 0) == 0) {
    return ProtoWriter().message(kDecimal, ProtoWriter().varint(1, 10).varint(2, 2));
  }
  throw std::runtime_error("no Spark type for the column " + name);
}

// The StructType message of the given columns.
ProtoWriter structType(const std::vector<std::string>& columns) {
  ProtoWriter schema;
  for (const auto& column : columns) {
    schema.message(1, ProtoWriter().bytes(1, column).message(2, columnType(column)).varint(3, 1));
  }
  return schema;
}

// ---------------------------------------------------------------------------------------------
// Objects
// ---------------------------------------------------------------------------------------------

// What a handle refers to.
struct Object {
  virtual ~Object() = default;
};

struct DataSource : Object {
  std::string name;
  Options options;
};

// The batch or stream reader of native_range or native_counter.
struct Reader : Object {
  Options state;  // Serialized to the executors.
};

// The reader of one partition, on an executor.
struct PartitionReader : Object {
  Options state;
  std::string partition;
  TaskInfo task;
};

// The writer of native_sink.
struct Writer : Object {
  Options state;  // path, overwrite, fail.
  // The file that closing the writer creates, so that the tests can check that the writer of a
  // committed or aborted write is closed: _CLOSED for a batch write, _CLOSED_<epoch> for a
  // streaming one.
  std::string closed_marker;
};

[[maybe_unused]] std::string closedMarker(int64_t epoch_id) {
  return epoch_id < 0 ? "/_CLOSED" : "/_CLOSED_" + std::to_string(epoch_id);
}

// Writes the rows of a task as CSV lines to a temporary file, renamed on commit. The commit records
// in _streams the number of streams and of batches that the data writer got.
struct DataWriter : Object {
  std::string temp_path;
  FILE* file = nullptr;
  Options state;
  bool committed = false;
  int64_t streams = 0;
  int64_t batches = 0;
};

// The data writers that Spark closed, with the path of their write, so that using one of them
// afterwards, which the header forbids, is recorded in the file _DATA_WRITER_USED_AFTER_CLOSE
// rather than crashing.
std::mutex closed_data_writers_lock;
std::map<void*, std::string> closed_data_writers;

// Whether Spark uses a data writer that it closed, which the header forbids.
bool usedAfterClose(void* handle) {
  std::lock_guard<std::mutex> guard(closed_data_writers_lock);
  auto closed = closed_data_writers.find(handle);
  if (closed == closed_data_writers.end()) return false;
  writeFile(closed->second + "/_DATA_WRITER_USED_AFTER_CLOSE", "");
  return true;
}

// Returns a new object as a handle. The allocator may reuse the address of a closed data writer.
void* newHandle(Object* object) {
  std::lock_guard<std::mutex> guard(closed_data_writers_lock);
  closed_data_writers.erase(object);
  return object;
}

template <typename T>
T* as(void* handle, const char* request) {
  auto* object = dynamic_cast<T*>(static_cast<Object*>(handle));
  if (object == nullptr) throw std::runtime_error(std::string(request) + " on another object");
  return object;
}

// With the option "task_path", records the task of a read or a data writer in a file named after
// its partition: its fields.
void recordTask(const Options& options, const TaskInfo& task) {
  std::string dir = get(options, "task_path", "");
  if (dir.empty()) return;
  makeDirs(dir);
  writeFile(dir + "/" + std::to_string(task.partition_id),
            std::to_string(task.stage_id) + "," + std::to_string(task.stage_attempt_number) +
                "," + std::to_string(task.partition_id) + "," +
                std::to_string(task.attempt_number) + "," +
                std::to_string(task.task_attempt_id));
}

DataSource parseDataSource(const std::string& bytes) {
  DataSource source;
  ProtoReader reader(bytes);
  while (reader.next()) {
    if (reader.field() == 1) {
      source.name = reader.bytes();
    } else if (reader.field() == 2) {
      // A map is a repeated message of entries, with the key and the value.
      ProtoReader entry(reader.bytes());
      std::string key;
      std::string value;
      while (entry.next()) {
        if (entry.field() == 1) {
          key = entry.bytes();
        } else if (entry.field() == 2) {
          value = entry.bytes();
        } else {
          entry.skip();
        }
      }
      source.options[key] = value;
    } else {
      reader.skip();
    }
  }
  return source;
}

// The capabilities of a data source, unless the option "capabilities" declares others: their
// numbers, separated by commas.
std::vector<int32_t> capabilitiesOf(const DataSource& source) {
  auto declared = source.options.find("capabilities");
  if (declared != source.options.end()) {
    std::vector<int32_t> capabilities;
    for (const auto& value : split(declared->second)) capabilities.push_back(std::stoi(value));
    return capabilities;
  }
  if (source.name == name("native_range")) return {kBatchRead};
#ifndef READ_ONLY
  if (source.name == name("native_counter")) return {kMicroBatchRead};
  if (source.name == name("native_sink")) return {kBatchWrite, kStreamingWrite, kTruncate};
#endif
  return {};
}

// ---------------------------------------------------------------------------------------------
// spark_ds_open
// ---------------------------------------------------------------------------------------------

void* openDataSource(const std::string& message, const std::string& spark_version,
                     SparkDsSink reply, void* sink) {
  auto source = std::make_unique<DataSource>(parseDataSource(message));
  maybeFail(source->options, "create");
  if (source->name != name("native_range") && source->name != name("native_sink") &&
      source->name != name("native_counter")) {
    throw std::runtime_error("unknown data source " + source->name);
  }
  std::string log = get(source->options, "log", "");
  if (!log.empty()) hostLog(SPARK_DS_LOG_WARN, log);
  std::string version = get(source->options, "spark_version", "");
  if (!version.empty() && version != spark_version) {
    throw std::runtime_error("the version of Spark is " + spark_version);
  }
  sendReply(reply, sink, source->options, ProtoWriter().packed(1, capabilitiesOf(*source)));
  if (returnsZero(source->options, "create")) return nullptr;
  return newHandle(source.release());
}

void* openBatchReader(const std::string& message) {
  auto fields = parseMessage(message);
  DataSource source = parseDataSource(fields[1]);
  if (source.name != name("native_range")) {
    throw std::runtime_error(source.name + " does not support batch reads");
  }
  auto columns = parseColumns(fields[2]);
  for (const auto& column : columns) findColumn(column);
  if (returnsZero(source.options, "create_reader")) return nullptr;
  auto reader = std::make_unique<Reader>();
  reader->state["columns"] = join(columns);
  reader->state["lo"] = "0";
  reader->state["hi"] = get(source.options, "end", "10");
  reader->state["partitions"] = get(source.options, "partitions", "2");
  reader->state["batch_size"] = get(source.options, "batch_size", "1000");
  for (const char* option : {"fail", "slice", "short", "claim_predicates", "predicate_use",
                             "wait_for_interrupt", "task_path", "unknown_fields"}) {
    reader->state[option] = get(source.options, option, "");
  }
  return newHandle(reader.release());
}

#ifndef READ_ONLY

void* openStreamReader(const std::string& message) {
  auto fields = parseMessage(message);
  DataSource source = parseDataSource(fields[1]);
  if (source.name != name("native_counter")) {
    throw std::runtime_error(source.name + " does not support streaming reads");
  }
  auto columns = join(parseColumns(fields[2]));
  if (returnsZero(source.options, "create_stream_reader")) return nullptr;
  auto reader = std::make_unique<Reader>();
  reader->state["columns"] = columns;
  reader->state["max_offset"] = get(source.options, "max_offset", "10");
  reader->state["step"] = get(source.options, "step", reader->state["max_offset"]);
  reader->state["current"] = "0";
  reader->state["batch_size"] = get(source.options, "batch_size", "1000");
  reader->state["commit_path"] = get(source.options, "commit_path", "");
  reader->state["bad_offset"] = get(source.options, "bad_offset", "");
  reader->state["closed_path"] = get(source.options, "closed_path", "");
  reader->state["stream"] = "true";
  return newHandle(reader.release());
}

void* openWriter(const std::string& message) {
  ProtoReader reader(message);
  DataSource source;
  std::vector<std::string> columns;
  uint64_t mode = kAppend;
  std::string query_id;
  while (reader.next()) {
    switch (reader.field()) {
      case 1: source = parseDataSource(reader.bytes()); break;
      case 2: columns = parseColumns(reader.bytes()); break;
      case 3: mode = reader.varint(); break;
      case 4: query_id = reader.bytes(); break;
      default: reader.skip();
    }
  }
  if (source.name != name("native_sink")) {
    throw std::runtime_error(source.name + " does not support writes");
  }
  if (mode != kAppend && mode != kTruncateMode) {
    throw std::runtime_error("unknown mode " + std::to_string(mode));
  }
  if (columns.empty()) throw std::runtime_error("no columns to write");
  // The location of a table in a catalog is passed as a URI.
  std::string path = get(source.options, "path", "");
  if (path.rfind("file://", 0) == 0) {
    path = path.substr(7);
  } else if (path.rfind("file:", 0) == 0) {
    path = path.substr(5);
  }
  if (path.empty()) throw std::runtime_error("the option 'path' is required");
  if (returnsZero(source.options, "create_writer")) return nullptr;
  auto writer = std::make_unique<Writer>();
  writer->state["path"] = path;
  writer->state["overwrite"] = mode == kTruncateMode ? "true" : "false";
  for (const char* option : {"fail", "zero", "task_path", "unknown_fields"}) {
    writer->state[option] = get(source.options, option, "");
  }
  writer->state["commit_messages"] = get(source.options, "commit_messages", "1");
  // Records that a writer was created, which Spark then commits or aborts, and the ID of the
  // streaming query.
  makeDirs(path);
  writeFile(path + "/_CREATED", "");
  if (!query_id.empty()) writeFile(path + "/_QUERY_ID", query_id);
  return newHandle(writer.release());
}

void* openDataWriter(const std::string& message) {
  ProtoReader reader(message);
  TaskInfo task;
  Options state;
  int64_t epoch_id = 0;
  while (reader.next()) {
    switch (reader.field()) {
      case 1: task = parseTask(reader.bytes()); break;
      case 2: state = deserialize(reader.bytes()); break;
      case 3: epoch_id = reader.int64(); break;
      default: reader.skip();
    }
  }
  if (returnsZero(state, "create_data_writer")) return nullptr;
  recordTask(state, task);
  std::string dir = state["path"] + "/_temporary";
  makeDirs(dir);
  std::string temp_path = dir + "/" + std::to_string(epoch_id) + "-" +
                          std::to_string(task.partition_id) + "-" +
                          std::to_string(task.task_attempt_id) + ".csv";
  FILE* file = std::fopen(temp_path.c_str(), "w");
  if (file == nullptr) throw std::runtime_error("cannot write " + temp_path);
  makeDirs(state["path"] + "/_created_data_writers");
  writeFile(state["path"] + "/_created_data_writers/" + temp_path.substr(dir.size() + 1), "");
  auto writer = std::make_unique<DataWriter>();
  writer->temp_path = temp_path;
  writer->file = file;
  writer->state = state;
  return newHandle(writer.release());
}

#endif  // READ_ONLY

void* openPartitionReader(const std::string& message) {
  ProtoReader reader(message);
  auto partition = std::make_unique<PartitionReader>();
  while (reader.next()) {
    switch (reader.field()) {
      case 1: partition->task = parseTask(reader.bytes()); break;
      case 2: partition->state = deserialize(reader.bytes()); break;
      case 3: partition->partition = reader.bytes(); break;
      default: reader.skip();
    }
  }
  maybeFail(partition->state, "open");
  recordTask(partition->state, partition->task);
  return newHandle(partition.release());
}

// ---------------------------------------------------------------------------------------------
// spark_ds_call
// ---------------------------------------------------------------------------------------------

void inferSchema(DataSource* source, SparkDsSink reply, void* sink) {
  maybeFail(source->options, "schema");
  if (source->name == name("native_sink")) {
    throw std::runtime_error("native_sink has no schema; it can only be written");
  }
  bool counter = source->name == name("native_counter");
  auto columns = allColumns(counter, get(source->options, "extra", ""));
  sendReply(reply, sink, source->options,
            ProtoWriter().message(kInferSchema, ProtoWriter().message(1, structType(columns))));
}

#ifndef READ_ONLY

void pushPredicates(Reader* reader, const std::string& message, SparkDsSink reply, void* sink) {
  std::vector<std::string> json;
  ProtoReader request(message);
  while (request.next()) {
    if (request.field() == 1) {
      json.push_back(request.bytes());
    } else {
      request.skip();
    }
  }
  int64_t end = getLong(reader->state, "hi", 0);
  int64_t lo = getLong(reader->state, "lo", 0);
  int64_t hi = end;
  std::vector<int32_t> uses(json.size(), kUnused);
  // With the option "claim_predicates", reports every predicate as evaluated completely, but
  // evaluates none of them.
  if (reader->state["claim_predicates"] == "true") {
    std::fill(uses.begin(), uses.end(), kComplete);
  } else {
    std::string predicate_use = reader->state["predicate_use"];
    bool partial = predicate_use == "partial";
    int32_t use = predicate_use.empty() ? kComplete
                  : partial            ? kPartial
                                       : std::stoi(predicate_use);
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
      uses[i] = ok ? use : kUnused;
    }
    if (partial) {
      // Reads the blocks of 10 rows that contain matching rows, with all their rows.
      lo = lo / 10 * 10;
      hi = std::min(end, (hi + 9) / 10 * 10);
    }
    reader->state["lo"] = std::to_string(lo);
    reader->state["hi"] = std::to_string(std::max(lo, hi));
  }
  reader->state["predicates"] = std::to_string(json.size());
  sendReply(reply, sink, reader->state,
            ProtoWriter().message(kPushPredicates, ProtoWriter().packed(1, uses)));
}

void pushLimit(Reader* reader, const std::string& message, SparkDsSink reply, void* sink) {
  // Spark pushes a limit at most once.
  if (reader->state.count("limit") > 0) throw std::runtime_error("PushLimit was called twice");
  auto fields = parseVarints(message);
  reader->state["limit"] = std::to_string(static_cast<int32_t>(fields[1]));
  sendReply(reply, sink, reader->state,
            ProtoWriter().message(kPushLimit, ProtoWriter().varint(1, 1)));
}

void pruneColumns(Reader* reader, const std::string& message, SparkDsSink reply, void* sink) {
  std::vector<std::string> names;
  ProtoReader request(message);
  while (request.next()) {
    if (request.field() == 1) {
      names.push_back(request.bytes());
    } else {
      request.skip();
    }
  }
  for (const auto& column : names) findColumn(column);
  reader->state["columns"] = join(names);
  sendReply(reply, sink, reader->state,
            ProtoWriter().message(kPruneColumns, ProtoWriter().varint(1, 1)));
}

#endif  // READ_ONLY

void planPartitions(Reader* reader, SparkDsSink reply, void* sink) {
  maybeFail(reader->state, "plan");
  int64_t lo = getLong(reader->state, "lo", 0);
  int64_t hi = getLong(reader->state, "hi", 0);
  int64_t count = getLong(reader->state, "partitions", 1);
  if (reader->state.count("limit") > 0) {
    // A pushed limit is applied by reading a single partition with at most `limit` rows.
    hi = std::min(hi, lo + getLong(reader->state, "limit", 0));
    count = 1;
  }
  ProtoWriter partitions;
  int64_t step = std::max<int64_t>(1, (hi - lo + count - 1) / count);
  for (int64_t start = lo; start < hi; start += step) {
    int64_t stop = std::min(hi, start + step);
    partitions.bytes(1, std::to_string(start) + "," + std::to_string(stop));
  }
  sendReply(reply, sink, reader->state, ProtoWriter().message(kPlanPartitions, partitions));
}

void serializeState(const Options& state, SparkDsSink reply, void* sink) {
  sendReply(reply, sink, state,
            ProtoWriter().message(kSerialize, ProtoWriter().bytes(1, serialize(state))));
}

#ifndef READ_ONLY

void initialOffset(Reader* reader, SparkDsSink reply, void* sink) {
  sendReply(reply, sink, reader->state,
            ProtoWriter().message(kInitialOffset, ProtoWriter().bytes(1, "{\"offset\":0}")));
}

// Each call makes `step` more rows available, up to `max_offset`.
void latestOffset(Reader* reader, SparkDsSink reply, void* sink) {
  Options& state = reader->state;
  int64_t latest = std::min(getLong(state, "max_offset", 10),
                            getLong(state, "current", 0) + getLong(state, "step", 10));
  state["current"] = std::to_string(latest);
  std::string bad_offset = get(state, "bad_offset", "");
  if (bad_offset == "none") return;
  std::string offset = "{\"offset\":" + std::to_string(latest) + "}";
  if (bad_offset == "multiline") {
    // Pretty-printed JSON, as serde_json::to_string_pretty writes it.
    offset = "{\n  \"offset\": " + std::to_string(latest) + "\n}";
  } else if (bad_offset == "invalid") {
    offset = "offset " + std::to_string(latest);
  }
  sendReply(reply, sink, state,
            ProtoWriter().message(kLatestOffset, ProtoWriter().bytes(1, offset)));
}

void planStreamPartitions(Reader* reader, const std::string& message, SparkDsSink reply,
                          void* sink) {
  auto fields = parseMessage(message);
  auto lo = static_cast<int64_t>(JsonParser(fields[1]).parse()["offset"].number);
  auto hi = static_cast<int64_t>(JsonParser(fields[2]).parse()["offset"].number);
  ProtoWriter partitions;
  if (hi > lo) partitions.bytes(1, std::to_string(lo) + "," + std::to_string(hi));
  sendReply(reply, sink, reader->state,
            ProtoWriter().message(kPlanStreamPartitions, partitions));
}

void commitOffset(Reader* reader, const std::string& message) {
  std::string path = get(reader->state, "commit_path", "");
  if (!path.empty()) writeFile(path, parseMessage(message)[1]);
}

// The epoch ID and the messages of a Commit or an Abort, with whether a task did not commit.
int64_t parseTaskMessages(const std::string& message, std::vector<std::string>* messages,
                          int64_t* uncommitted) {
  int64_t epoch_id = 0;
  ProtoReader request(message);
  while (request.next()) {
    if (request.field() == 1) {
      epoch_id = request.int64();
    } else if (request.field() == 2) {
      ProtoReader task(request.bytes());
      bool committed = false;
      std::string bytes;
      while (task.next()) {
        if (task.field() == 1) {
          committed = task.varint() != 0;
        } else if (task.field() == 2) {
          bytes = task.bytes();
        } else {
          task.skip();
        }
      }
      if (committed) {
        messages->push_back(bytes);
      } else {
        (*uncommitted)++;
      }
    } else {
      request.skip();
    }
  }
  return epoch_id;
}

void commitWrite(Writer* writer, const std::string& message) {
  std::string path = writer->state["path"];
  maybeFail(writer->state, "commit");
  std::vector<std::string> temp_files;
  int64_t uncommitted = 0;
  int64_t epoch_id = parseTaskMessages(message, &temp_files, &uncommitted);
  if (uncommitted > 0) throw std::runtime_error("commit got a task that did not commit");
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
}

// Records the aborted write in _ABORTED for a batch write, or _ABORTED_<epoch> for a streaming
// one, with the number of tasks that did not commit.
void abortWrite(Writer* writer, const std::string& message) {
  std::vector<std::string> temp_files;
  int64_t uncommitted = 0;
  int64_t epoch_id = parseTaskMessages(message, &temp_files, &uncommitted);
  writer->closed_marker = closedMarker(epoch_id);
  for (const auto& temp_file : temp_files) unlink(temp_file.c_str());
  writeFile(writer->state["path"] +
                (epoch_id < 0 ? "/_ABORTED" : "/_ABORTED_" + std::to_string(epoch_id)),
            std::to_string(uncommitted));
}

void commitDataWriter(DataWriter* writer, SparkDsSink reply, void* sink) {
  // If the commit fails, Spark aborts the data writer.
  maybeFail(writer->state, "commitDataWriter");
  // With the option "commit_messages", the reply is passed that many times.
  int64_t messages = getLong(writer->state, "commit_messages", 1);
  for (int64_t i = 0; i < messages; i++) {
    sendReply(reply, sink, writer->state, ProtoWriter().message(
        kCommitDataWriter, ProtoWriter().bytes(1, writer->temp_path)));
  }
  std::fclose(writer->file);
  writer->file = nullptr;
  writer->committed = true;
  std::string dir = writer->state["path"] + "/_streams";
  makeDirs(dir);
  writeFile(dir + "/" + writer->temp_path.substr(writer->temp_path.rfind('/') + 1),
            std::to_string(writer->streams) + "," + std::to_string(writer->batches));
}

void abortDataWriter(DataWriter* writer) {
  // Spark does not abort a data writer that committed: records it if it does.
  if (writer->committed) {
    writeFile(writer->state["path"] + "/_ABORTED_COMMITTED_DATA_WRITER", "");
    return;
  }
  if (writer->file != nullptr) {
    std::fclose(writer->file);
    writer->file = nullptr;
  }
  unlink(writer->temp_path.c_str());
}

#endif  // READ_ONLY

void call(void* handle, const std::string& request, SparkDsSink reply, void* sink) {
  ProtoReader message(request);
  while (message.next()) {
    int field = message.field();
    if (field < kInferSchema || field > kAbortDataWriter) {
      // A request that the library does not know, which Spark does not send to it.
      message.skip();
      continue;
    }
    std::string body = message.bytes();
    switch (field) {
      case kInferSchema: return inferSchema(as<DataSource>(handle, "InferSchema"), reply, sink);
      case kPlanPartitions:
        return planPartitions(as<Reader>(handle, "PlanPartitions"), reply, sink);
      case kSerialize: {
        auto* object = static_cast<Object*>(handle);
        if (auto* reader = dynamic_cast<Reader*>(object)) {
          return serializeState(reader->state, reply, sink);
        }
        return serializeState(as<Writer>(handle, "Serialize")->state, reply, sink);
      }
#ifdef READ_ONLY
      // A library without pushdown replies to it with empty replies: Spark pushes nothing.
      case kPushPredicates:
      case kPushLimit:
      case kPruneColumns:
        return;
      default:
        throw std::runtime_error("unsupported request " + std::to_string(field));
#else
      case kPushPredicates:
        return pushPredicates(as<Reader>(handle, "PushPredicates"), body, reply, sink);
      case kPushLimit: return pushLimit(as<Reader>(handle, "PushLimit"), body, reply, sink);
      case kPruneColumns:
        return pruneColumns(as<Reader>(handle, "PruneColumns"), body, reply, sink);
      case kInitialOffset:
        return initialOffset(as<Reader>(handle, "InitialOffset"), reply, sink);
      case kLatestOffset: return latestOffset(as<Reader>(handle, "LatestOffset"), reply, sink);
      case kPlanStreamPartitions:
        return planStreamPartitions(as<Reader>(handle, "PlanStreamPartitions"), body, reply, sink);
      case kCommitOffset: return commitOffset(as<Reader>(handle, "CommitOffset"), body);
      case kCommit: return commitWrite(as<Writer>(handle, "Commit"), body);
      case kAbort: return abortWrite(as<Writer>(handle, "Abort"), body);
      case kCommitDataWriter:
        return commitDataWriter(as<DataWriter>(handle, "CommitDataWriter"), reply, sink);
      case kAbortDataWriter:
        return abortDataWriter(as<DataWriter>(handle, "AbortDataWriter"));
#endif
    }
  }
}

// ---------------------------------------------------------------------------------------------
// spark_ds_stream
// ---------------------------------------------------------------------------------------------

void read(PartitionReader* partition, struct ArrowArrayStream* stream) {
  const Options& state = partition->state;
  // Returns success without initializing the stream, which Spark must reject.
  if (get(state, "fail", "") == "uninitialized") return;
  auto bounds = split(partition->partition);
  if (bounds.size() != 2) throw std::runtime_error("invalid partition");
  stream->get_schema = streamGetSchema;
  stream->get_next = streamGetNext;
  stream->get_last_error = streamGetLastError;
  stream->release = streamRelease;
  stream->private_data = new Stream{split(get(state, "columns", "")), std::stoll(bounds[0]),
                                    std::stoll(bounds[1]), getLong(state, "batch_size", 1000),
                                    get(state, "fail", "") == "read",
                                    get(state, "slice", "") == "true", get(state, "short", ""),
                                    partition->task.task_attempt_id,
                                    get(state, "wait_for_interrupt", ""), ""};
}

#ifndef READ_ONLY

// Writes the batches of the stream. Spark releases the stream after the call, since it is not
// moved here, but the schema and the arrays that the stream returns are the library's to release.
void write(DataWriter* writer, struct ArrowArrayStream* stream) {
  maybeFail(writer->state, "write");
  writer->streams++;
  ArrowSchema schema;
  if (stream->get_schema(stream, &schema) != 0) {
    throw std::runtime_error(std::string("get_schema failed: ") + stream->get_last_error(stream));
  }
  std::unique_ptr<ArrowSchema, void (*)(ArrowSchema*)> schema_owner(
      &schema, [](ArrowSchema* s) { s->release(s); });
  while (true) {
    ArrowArray array;
    if (stream->get_next(stream, &array) != 0) {
      throw std::runtime_error(std::string("get_next failed: ") + stream->get_last_error(stream));
    }
    if (array.release == nullptr) break;  // The end of the stream.
    std::unique_ptr<ArrowArray, void (*)(ArrowArray*)> array_owner(
        &array, [](ArrowArray* a) { a->release(a); });
    writer->batches++;
    for (int64_t i = 0; i < array.length; i++) {
      std::string line;
      for (int64_t c = 0; c < array.n_children; c++) {
        if (c > 0) line += ",";
        line += formatValue(schema.children[c], array.children[c], array.offset + i);
      }
      line += "\n";
      std::fputs(line.c_str(), writer->file);
    }
  }
}

#endif  // READ_ONLY

// ---------------------------------------------------------------------------------------------
// spark_ds_close
// ---------------------------------------------------------------------------------------------

void close(void* handle) {
  std::unique_ptr<Object> object(static_cast<Object*>(handle));
  guarded([&] {
#ifndef READ_ONLY
    if (auto* reader = dynamic_cast<Reader*>(object.get())) {
      std::string path = get(reader->state, "closed_path", "");
      if (!path.empty()) writeFile(path, "");
    } else if (auto* writer = dynamic_cast<Writer*>(object.get())) {
      if (!writer->closed_marker.empty()) {
        writeFile(writer->state["path"] + writer->closed_marker, "");
      }
    } else if (auto* data_writer = dynamic_cast<DataWriter*>(object.get())) {
      if (data_writer->file != nullptr) std::fclose(data_writer->file);
      std::string path = data_writer->state["path"];
      std::string dir = path + "/_closed_data_writers";
      makeDirs(dir);
      std::string temp_path = data_writer->temp_path;
      writeFile(dir + "/" + temp_path.substr(temp_path.rfind('/') + 1), "");
      std::lock_guard<std::mutex> guard(closed_data_writers_lock);
      closed_data_writers[handle] = path;
    }
#endif
  });
}

// spark_ds_open, which NO_OPEN does not export.
[[maybe_unused]] int open(const SparkDsHost* spark, const uint8_t* request, int64_t length,
                          SparkDsSink reply, void* sink, void** handle) {
  return guarded([&] {
    host = spark;
    // The fields may come in any order, so they are all read before the object is opened.
    std::string spark_version;
    int object = 0;
    std::string message;
    ProtoReader reader(toBytes(request, length));
    while (reader.next()) {
      if (reader.field() == kSparkVersion) {
        spark_version = reader.bytes();
      } else if (reader.field() >= kDataSource && reader.field() <= kDataWriter) {
        object = reader.field();
        message = reader.bytes();
      } else {
        reader.skip();
      }
    }
    switch (object) {
      case kDataSource: *handle = openDataSource(message, spark_version, reply, sink); break;
      case kBatchReader: *handle = openBatchReader(message); break;
      case kPartitionReader: *handle = openPartitionReader(message); break;
#ifndef READ_ONLY
      case kStreamReader: *handle = openStreamReader(message); break;
      case kWriter: *handle = openWriter(message); break;
      case kDataWriter: *handle = openDataWriter(message); break;
#endif
      default: {
        const char* kinds[] = {"data source", "batch reader", "stream reader", "writer",
                               "partition reader", "data writer"};
        bool known = object >= kDataSource && object <= kDataWriter;
        throw std::runtime_error(std::string("the library cannot open a ") +
                                 (known ? kinds[object - kDataSource] : "new kind of object"));
      }
    }
  });
}

}  // namespace

// ---------------------------------------------------------------------------------------------
// spark_datasource.h
// ---------------------------------------------------------------------------------------------

int32_t spark_ds_abi_version(void) { return ABI_VERSION; }

#ifndef NO_LAST_ERROR
const char* spark_ds_last_error(void) { return last_error.c_str(); }
#endif

#ifndef NO_OPEN

int spark_ds_open(const SparkDsHost* spark, const uint8_t* request, int64_t length,
                  SparkDsSink reply, void* sink, void** handle) {
  return open(spark, request, length, reply, sink, handle);
}

#endif  // NO_OPEN

int spark_ds_call(void* handle, const uint8_t* request, int64_t length, SparkDsSink reply,
                  void* sink) {
  if (usedAfterClose(handle)) return 0;
  return guarded([&] { call(handle, toBytes(request, length), reply, sink); });
}

int spark_ds_stream(void* handle, const uint8_t* request, int64_t length,
                    [[maybe_unused]] struct ArrowArrayStream* in, struct ArrowArrayStream* out) {
  if (usedAfterClose(handle)) return 0;
  return guarded([&] {
    ProtoReader message(toBytes(request, length));
    while (message.next()) {
      int field = message.field();
      message.skip();
      if (field == kRead) return read(as<PartitionReader>(handle, "Read"), out);
#ifndef READ_ONLY
      if (field == kWrite) return write(as<DataWriter>(handle, "Write"), in);
#endif
    }
    throw std::runtime_error("unsupported stream request");
  });
}

void spark_ds_close(void* handle) {
  if (usedAfterClose(handle)) return;
  close(handle);
}
