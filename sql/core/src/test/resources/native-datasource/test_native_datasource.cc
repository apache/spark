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

// A native data source library used by NativeDataSourceSuite. It implements the JNI functions of
// org.apache.spark.sql.datasource.NativeBridge without any dependency, building the Arrow C data
// interface structs by hand. It provides three data sources:
//
//   native_range    Reads rows [0, end) with columns of several types. Supports predicate,
//                   column and limit pushdown.
//   native_sink     Writes the rows as CSV files under the directory in the option "path".
//   native_counter  Streams the rows [0, max_offset) with a single column "id", making "step"
//                   more rows available each time Spark asks for the latest offset.
//
// The suite compiles it with these macros to get several distinct libraries:
//
//   NAME_PREFIX     A prefix for the names of the data sources, "" by default.
//   ID_SIGN         Multiplies the ids produced by native_range, 1 by default.
//   READ_ONLY       Leaves out the functions for writes, streaming reads and pushdown.
//   ABI_VERSION     The version returned by abiVersion(), 1 by default.

#include <jni.h>

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

#define SPARK_JNI(ret, method) \
  extern "C" JNIEXPORT ret JNICALL Java_org_apache_spark_sql_datasource_NativeBridge_##method

// ---------------------------------------------------------------------------------------------
// The Arrow C data interface: https://arrow.apache.org/docs/format/CDataInterface.html
// ---------------------------------------------------------------------------------------------

#ifndef ARROW_C_DATA_INTERFACE
#define ARROW_C_DATA_INTERFACE

#define ARROW_FLAG_NULLABLE 2

struct ArrowSchema {
  const char* format;
  const char* name;
  const char* metadata;
  int64_t flags;
  int64_t n_children;
  struct ArrowSchema** children;
  struct ArrowSchema* dictionary;
  void (*release)(struct ArrowSchema*);
  void* private_data;
};

struct ArrowArray {
  int64_t length;
  int64_t null_count;
  int64_t offset;
  int64_t n_buffers;
  int64_t n_children;
  const void** buffers;
  struct ArrowArray** children;
  struct ArrowArray* dictionary;
  void (*release)(struct ArrowArray*);
  void* private_data;
};

#endif  // ARROW_C_DATA_INTERFACE

#ifndef ARROW_C_STREAM_INTERFACE
#define ARROW_C_STREAM_INTERFACE

struct ArrowArrayStream {
  int (*get_schema)(struct ArrowArrayStream*, struct ArrowSchema* out);
  int (*get_next)(struct ArrowArrayStream*, struct ArrowArray* out);
  const char* (*get_last_error)(struct ArrowArrayStream*);
  void (*release)(struct ArrowArrayStream*);
  void* private_data;
};

#endif  // ARROW_C_STREAM_INTERFACE

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

const ColumnType* findColumn(const std::string& name) {
  for (const auto& column : kColumns) {
    if (name == column.name) return &column;
  }
  throw std::runtime_error("unknown column " + name);
}

std::vector<std::string> allColumns(bool counter) {
  if (counter) return {"id"};
  std::vector<std::string> names;
  for (const auto& column : kColumns) names.push_back(column.name);
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
};

void releaseSchema(ArrowSchema* schema) {
  auto* data = static_cast<SchemaData*>(schema->private_data);
  for (auto& child : data->children) {
    if (child->release != nullptr) child->release(child.get());
  }
  delete data;
  schema->release = nullptr;
}

void initSchema(ArrowSchema* out, const std::string& format, const std::string& name,
                int64_t flags) {
  auto* data = new SchemaData{format, name, {}, {}};
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

// Exports a struct type with the given columns.
void exportSchema(const std::vector<std::string>& columns, ArrowSchema* out) {
  initSchema(out, "+s", "", 0);
  auto* data = static_cast<SchemaData*>(out->private_data);
  for (const auto& name : columns) {
    auto child = std::make_unique<ArrowSchema>();
    initSchema(child.get(), findColumn(name)->format, name, ARROW_FLAG_NULLABLE);
    data->child_pointers.push_back(child.get());
    data->children.push_back(std::move(child));
  }
  out->n_children = static_cast<int64_t>(data->children.size());
  out->children = data->child_pointers.data();
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
  } else {
    throw std::runtime_error("unknown column " + name);
  }
}

// Exports a struct array with the given columns for the ids [lo, hi).
void exportBatch(const std::vector<std::string>& columns, int64_t lo, int64_t hi,
                 ArrowArray* out) {
  initArray(out, hi - lo, 0, {std::vector<uint8_t>()});
  auto* data = static_cast<ArrayData*>(out->private_data);
  for (const auto& name : columns) {
    auto child = std::make_unique<ArrowArray>();
    exportColumn(name, lo, hi, child.get());
    data->child_pointers.push_back(child.get());
    data->children.push_back(std::move(child));
  }
  out->n_children = static_cast<int64_t>(data->children.size());
  out->children = data->child_pointers.data();
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
// JNI helpers
// ---------------------------------------------------------------------------------------------

void throwJava(JNIEnv* env, const std::string& message) {
  jclass cls = env->FindClass("java/lang/RuntimeException");
  if (cls != nullptr) env->ThrowNew(cls, message.c_str());
}

// Runs `f`, and turns a C++ exception into a Java exception: none may cross the boundary.
template <typename T, typename F>
T guarded(JNIEnv* env, T fallback, F&& f) {
  try {
    return f();
  } catch (const std::exception& e) {
    throwJava(env, e.what());
    return fallback;
  }
}

template <typename F>
void guardedVoid(JNIEnv* env, F&& f) {
  try {
    f();
  } catch (const std::exception& e) {
    throwJava(env, e.what());
  }
}

std::string toString(JNIEnv* env, jstring value) {
  if (value == nullptr) return "";
  const char* chars = env->GetStringUTFChars(value, nullptr);
  std::string result(chars);
  env->ReleaseStringUTFChars(value, chars);
  return result;
}

std::vector<std::string> toStrings(JNIEnv* env, jobjectArray values) {
  std::vector<std::string> result;
  jsize n = env->GetArrayLength(values);
  for (jsize i = 0; i < n; i++) {
    auto value = static_cast<jstring>(env->GetObjectArrayElement(values, i));
    result.push_back(toString(env, value));
    env->DeleteLocalRef(value);
  }
  return result;
}

std::string toBytes(JNIEnv* env, jbyteArray bytes) {
  if (bytes == nullptr) return "";
  jsize n = env->GetArrayLength(bytes);
  std::string result(static_cast<size_t>(n), '\0');
  if (n > 0) env->GetByteArrayRegion(bytes, 0, n, reinterpret_cast<jbyte*>(&result[0]));
  return result;
}

jbyteArray newBytes(JNIEnv* env, const std::string& bytes) {
  jbyteArray result = env->NewByteArray(static_cast<jsize>(bytes.size()));
  env->SetByteArrayRegion(result, 0, static_cast<jsize>(bytes.size()),
                          reinterpret_cast<const jbyte*>(bytes.data()));
  return result;
}

jobjectArray newByteArrays(JNIEnv* env, const std::vector<std::string>& values) {
  jobjectArray result =
      env->NewObjectArray(static_cast<jsize>(values.size()), env->FindClass("[B"), nullptr);
  for (size_t i = 0; i < values.size(); i++) {
    jbyteArray value = newBytes(env, values[i]);
    env->SetObjectArrayElement(result, static_cast<jsize>(i), value);
    env->DeleteLocalRef(value);
  }
  return result;
}

[[maybe_unused]]
std::vector<std::string> toByteStrings(JNIEnv* env, jobjectArray values, bool* has_null) {
  std::vector<std::string> result;
  jsize n = env->GetArrayLength(values);
  for (jsize i = 0; i < n; i++) {
    auto value = static_cast<jbyteArray>(env->GetObjectArrayElement(values, i));
    if (value == nullptr) {
      *has_null = true;
    } else {
      result.push_back(toBytes(env, value));
      env->DeleteLocalRef(value);
    }
  }
  return result;
}

template <typename T>
jlong toHandle(T* object) {
  return reinterpret_cast<jlong>(object);
}

template <typename T>
T* fromHandle(jlong handle) {
  return reinterpret_cast<T*>(handle);
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
  exportBatch(state->columns, state->next, hi, out);
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

[[maybe_unused]] std::string closedMarker(jlong epoch_id) {
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
// NativeBridge
// ---------------------------------------------------------------------------------------------

SPARK_JNI(jint, abiVersion)(JNIEnv*, jclass) { return ABI_VERSION; }

SPARK_JNI(jlong, createDataSource)(JNIEnv* env, jclass, jstring name_value, jobjectArray keys,
                                   jobjectArray values) {
  return guarded<jlong>(env, 0, [&] {
    auto* source = new DataSource{toString(env, name_value), {}};
    std::unique_ptr<DataSource> owner(source);
    auto key_strings = toStrings(env, keys);
    auto value_strings = toStrings(env, values);
    for (size_t i = 0; i < key_strings.size(); i++) {
      source->options[key_strings[i]] = value_strings[i];
    }
    maybeFail(source->options, "create");
    if (source->name != name("native_range") && source->name != name("native_sink") &&
        source->name != name("native_counter")) {
      throw std::runtime_error("unknown data source " + source->name);
    }
    return toHandle(owner.release());
  });
}

SPARK_JNI(void, schema)(JNIEnv* env, jclass, jlong handle, jlong schema_address) {
  guardedVoid(env, [&] {
    auto* source = fromHandle<DataSource>(handle);
    maybeFail(source->options, "schema");
    if (source->name == name("native_sink")) {
      throw std::runtime_error("native_sink has no schema; it can only be written");
    }
    exportSchema(allColumns(source->name == name("native_counter")),
                 reinterpret_cast<ArrowSchema*>(schema_address));
  });
}

SPARK_JNI(jlong, createReader)(JNIEnv* env, jclass, jlong handle, jlong schema_address) {
  return guarded<jlong>(env, 0, [&] {
    auto* source = fromHandle<DataSource>(handle);
    if (source->name != name("native_range")) {
      throw std::runtime_error(source->name + " does not support batch reads");
    }
    auto columns = importColumns(reinterpret_cast<ArrowSchema*>(schema_address));
    for (const auto& column : columns) findColumn(column);
    auto* reader = new Reader();
    reader->state["columns"] = join(columns);
    reader->state["lo"] = "0";
    reader->state["hi"] = get(source->options, "end", "10");
    reader->state["partitions"] = get(source->options, "partitions", "2");
    reader->state["batch_size"] = get(source->options, "batch_size", "1000");
    reader->state["fail"] = get(source->options, "fail", "");
    return toHandle(reader);
  });
}

#ifndef READ_ONLY

SPARK_JNI(jlong, createStreamReader)(JNIEnv* env, jclass, jlong handle, jlong schema_address) {
  return guarded<jlong>(env, 0, [&] {
    auto* source = fromHandle<DataSource>(handle);
    if (source->name != name("native_counter")) {
      throw std::runtime_error(source->name + " does not support streaming reads");
    }
    auto* reader = new Reader();
    reader->state["columns"] = join(importColumns(reinterpret_cast<ArrowSchema*>(schema_address)));
    reader->state["max_offset"] = get(source->options, "max_offset", "10");
    reader->state["step"] = get(source->options, "step", reader->state["max_offset"]);
    reader->state["current"] = "0";
    reader->state["batch_size"] = get(source->options, "batch_size", "1000");
    reader->state["commit_path"] = get(source->options, "commit_path", "");
    return toHandle(reader);
  });
}

#endif  // READ_ONLY

SPARK_JNI(void, closeDataSource)(JNIEnv*, jclass, jlong handle) {
  delete fromHandle<DataSource>(handle);
}

#ifndef READ_ONLY

SPARK_JNI(jbooleanArray, pushPredicates)(JNIEnv* env, jclass, jlong handle,
                                         jobjectArray predicates) {
  return guarded<jbooleanArray>(env, nullptr, [&] {
    auto* reader = fromHandle<Reader>(handle);
    auto json = toStrings(env, predicates);
    std::vector<jboolean> accepted;
    int64_t lo = getLong(reader->state, "lo", 0);
    int64_t hi = getLong(reader->state, "hi", 0);
    for (const auto& text : json) {
      // Accepts "id <op> <integer literal>" for the comparison operators.
      Json predicate = JsonParser(text).parse();
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
      accepted.push_back(ok ? JNI_TRUE : JNI_FALSE);
    }
    reader->state["lo"] = std::to_string(lo);
    reader->state["hi"] = std::to_string(std::max(lo, hi));
    reader->state["predicates"] = std::to_string(json.size());
    jbooleanArray result = env->NewBooleanArray(static_cast<jsize>(accepted.size()));
    env->SetBooleanArrayRegion(result, 0, static_cast<jsize>(accepted.size()), accepted.data());
    return result;
  });
}

SPARK_JNI(jboolean, pushLimit)(JNIEnv* env, jclass, jlong handle, jint limit) {
  return guarded<jboolean>(env, JNI_FALSE, [&] {
    fromHandle<Reader>(handle)->state["limit"] = std::to_string(limit);
    return JNI_TRUE;
  });
}

SPARK_JNI(jboolean, pruneColumns)(JNIEnv* env, jclass, jlong handle, jobjectArray columns) {
  return guarded<jboolean>(env, JNI_FALSE, [&] {
    auto names = toStrings(env, columns);
    for (const auto& column : names) findColumn(column);
    fromHandle<Reader>(handle)->state["columns"] = join(names);
    return JNI_TRUE;
  });
}

#endif  // READ_ONLY

SPARK_JNI(jobjectArray, partitions)(JNIEnv* env, jclass, jlong handle) {
  return guarded<jobjectArray>(env, nullptr, [&] {
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
    std::vector<std::string> partitions;
    int64_t step = std::max<int64_t>(1, (hi - lo + count - 1) / count);
    for (int64_t start = lo; start < hi; start += step) {
      int64_t stop = std::min(hi, start + step);
      partitions.push_back(std::to_string(start) + "," + std::to_string(stop));
    }
    return newByteArrays(env, partitions);
  });
}

SPARK_JNI(jbyteArray, serializeReader)(JNIEnv* env, jclass, jlong handle) {
  return guarded<jbyteArray>(env, nullptr, [&] {
    return newBytes(env, serialize(fromHandle<Reader>(handle)->state));
  });
}

SPARK_JNI(void, closeReader)(JNIEnv*, jclass, jlong handle) { delete fromHandle<Reader>(handle); }

#ifndef READ_ONLY

SPARK_JNI(jstring, initialOffset)(JNIEnv* env, jclass, jlong) {
  return env->NewStringUTF("{\"offset\":0}");
}

// Each call makes `step` more rows available, up to `max_offset`.
SPARK_JNI(jstring, latestOffset)(JNIEnv* env, jclass, jlong handle) {
  return guarded<jstring>(env, nullptr, [&] {
    Options& state = fromHandle<Reader>(handle)->state;
    int64_t latest = std::min(getLong(state, "max_offset", 10),
                              getLong(state, "current", 0) + getLong(state, "step", 10));
    state["current"] = std::to_string(latest);
    return env->NewStringUTF(("{\"offset\":" + std::to_string(latest) + "}").c_str());
  });
}

SPARK_JNI(jobjectArray, streamPartitions)(JNIEnv* env, jclass, jlong, jstring start,
                                          jstring end) {
  return guarded<jobjectArray>(env, nullptr, [&] {
    auto lo = static_cast<int64_t>(JsonParser(toString(env, start)).parse()["offset"].number);
    auto hi = static_cast<int64_t>(JsonParser(toString(env, end)).parse()["offset"].number);
    std::vector<std::string> partitions;
    if (hi > lo) partitions.push_back(std::to_string(lo) + "," + std::to_string(hi));
    return newByteArrays(env, partitions);
  });
}

SPARK_JNI(jbyteArray, serializeStreamReader)(JNIEnv* env, jclass, jlong handle) {
  return guarded<jbyteArray>(env, nullptr, [&] {
    return newBytes(env, serialize(fromHandle<Reader>(handle)->state));
  });
}

SPARK_JNI(void, commitOffset)(JNIEnv* env, jclass, jlong handle, jstring end) {
  guardedVoid(env, [&] {
    std::string path = get(fromHandle<Reader>(handle)->state, "commit_path", "");
    if (!path.empty()) writeFile(path, toString(env, end));
  });
}

SPARK_JNI(void, closeStreamReader)(JNIEnv*, jclass, jlong handle) {
  delete fromHandle<Reader>(handle);
}

#endif  // READ_ONLY

SPARK_JNI(void, read)(JNIEnv* env, jclass, jbyteArray reader_state, jbyteArray partition,
                      jlong stream_address) {
  guardedVoid(env, [&] {
    Options state = deserialize(toBytes(env, reader_state));
    maybeFail(state, "open");
    auto bounds = split(toBytes(env, partition));
    if (bounds.size() != 2) throw std::runtime_error("invalid partition");
    auto* stream = reinterpret_cast<ArrowArrayStream*>(stream_address);
    stream->get_schema = streamGetSchema;
    stream->get_next = streamGetNext;
    stream->get_last_error = streamGetLastError;
    stream->release = streamRelease;
    stream->private_data = new Stream{split(get(state, "columns", "")), std::stoll(bounds[0]),
                                      std::stoll(bounds[1]), getLong(state, "batch_size", 1000),
                                      get(state, "fail", "") == "read", ""};
  });
}

#ifndef READ_ONLY

namespace {

jlong createAnyWriter(JNIEnv* env, jlong handle, jlong schema_address, jboolean overwrite) {
  return guarded<jlong>(env, 0, [&] {
    auto* source = fromHandle<DataSource>(handle);
    if (source->name != name("native_sink")) {
      throw std::runtime_error(source->name + " does not support writes");
    }
    // Spark releases the schema after the call, since it is not moved here.
    auto* schema = reinterpret_cast<ArrowSchema*>(schema_address);
    if (schema->n_children == 0) throw std::runtime_error("no columns to write");
    auto* writer = new Writer();
    // The location of a table in a catalog is passed as a URI.
    std::string path = get(source->options, "path", "");
    if (path.rfind("file://", 0) == 0) {
      path = path.substr(7);
    } else if (path.rfind("file:", 0) == 0) {
      path = path.substr(5);
    }
    writer->state["path"] = path;
    writer->state["overwrite"] = overwrite ? "true" : "false";
    writer->state["fail"] = get(source->options, "fail", "");
    if (writer->state["path"].empty()) {
      delete writer;
      throw std::runtime_error("the option 'path' is required");
    }
    return toHandle(writer);
  });
}

}  // namespace

SPARK_JNI(jlong, createWriter)(JNIEnv* env, jclass, jlong handle, jlong schema_address,
                               jboolean overwrite) {
  return createAnyWriter(env, handle, schema_address, overwrite);
}

SPARK_JNI(jlong, createStreamWriter)(JNIEnv* env, jclass, jlong handle, jlong schema_address,
                                     jboolean overwrite) {
  return createAnyWriter(env, handle, schema_address, overwrite);
}

SPARK_JNI(jbyteArray, serializeWriter)(JNIEnv* env, jclass, jlong handle) {
  return guarded<jbyteArray>(env, nullptr, [&] {
    return newBytes(env, serialize(fromHandle<Writer>(handle)->state));
  });
}

SPARK_JNI(void, commit)(JNIEnv* env, jclass, jlong handle, jlong epoch_id,
                        jobjectArray messages) {
  guardedVoid(env, [&] {
    auto* writer = fromHandle<Writer>(handle);
    std::string path = writer->state["path"];
    maybeFail(writer->state, "commit");
    bool has_null = false;
    auto temp_files = toByteStrings(env, messages, &has_null);
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

SPARK_JNI(void, abort)(JNIEnv* env, jclass, jlong handle, jlong epoch_id,
                       jobjectArray messages) {
  guardedVoid(env, [&] {
    auto* writer = fromHandle<Writer>(handle);
    writer->closed_marker = closedMarker(epoch_id);
    bool has_null = false;
    for (const auto& temp_file : toByteStrings(env, messages, &has_null)) {
      unlink(temp_file.c_str());
    }
    writeFile(writer->state["path"] + "/_ABORTED", "");
  });
}

SPARK_JNI(void, closeWriter)(JNIEnv* env, jclass, jlong handle) {
  std::unique_ptr<Writer> writer(fromHandle<Writer>(handle));
  guardedVoid(env, [&] {
    if (!writer->closed_marker.empty()) {
      writeFile(writer->state["path"] + writer->closed_marker, "");
    }
  });
}

SPARK_JNI(jlong, createDataWriter)(JNIEnv* env, jclass, jbyteArray writer_state,
                                   jint partition_id, jlong task_id, jlong epoch_id) {
  return guarded<jlong>(env, 0, [&] {
    Options state = deserialize(toBytes(env, writer_state));
    std::string dir = state["path"] + "/_temporary";
    makeDirs(dir);
    std::string temp_path = dir + "/" + std::to_string(epoch_id) + "-" +
                            std::to_string(partition_id) + "-" + std::to_string(task_id) + ".csv";
    FILE* file = std::fopen(temp_path.c_str(), "w");
    if (file == nullptr) throw std::runtime_error("cannot write " + temp_path);
    return toHandle(new DataWriter{temp_path, file, state});
  });
}

SPARK_JNI(void, write)(JNIEnv* env, jclass, jlong handle, jlong array_address,
                       jlong schema_address) {
  guardedVoid(env, [&] {
    auto* writer = fromHandle<DataWriter>(handle);
    auto* array = reinterpret_cast<ArrowArray*>(array_address);
    auto* schema = reinterpret_cast<ArrowSchema*>(schema_address);
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

SPARK_JNI(jbyteArray, commitDataWriter)(JNIEnv* env, jclass, jlong handle) {
  return guarded<jbyteArray>(env, nullptr, [&] {
    auto* writer = fromHandle<DataWriter>(handle);
    // If the commit fails, the data writer stays valid: Spark aborts it with abortDataWriter.
    maybeFail(writer->state, "commitDataWriter");
    jbyteArray message = newBytes(env, writer->temp_path);
    std::fclose(writer->file);
    delete writer;
    return message;
  });
}

SPARK_JNI(void, abortDataWriter)(JNIEnv*, jclass, jlong handle) {
  std::unique_ptr<DataWriter> writer(fromHandle<DataWriter>(handle));
  std::fclose(writer->file);
  unlink(writer->temp_path.c_str());
}

#endif  // READ_ONLY
