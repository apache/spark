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

// A native function library used by the suites of native functions. It implements the C interface
// in spark_udf.h without any other dependency, building the Arrow C data interface structs by
// hand. Its scalar functions, by the names that a manifest lists for them:
//
//   identity(x)          Returns its argument, of any type of version 1, copied to arrays with an
//                        offset of 0, with its null values.
//   add_one(bigint)      x + 1, wrapping around, and null for null. The result has no validity
//                        buffer when no value is null.
//   half_if_even(bigint) x / 2 for an even x, and null for an odd x or null.
//   shout(string)        The string with its ASCII letters in upper case, followed by "!".
//   edit_distance(string, string)
//                        The Levenshtein distance between the code points of the two strings, as an
//                        int, or null if one is null.
//   describe(...)        For each row, the formats of the arguments and of the result that create
//                        got, at every level, with "?" after a nullable field, and then the time
//                        zone of the session, such as "l,u?->u?;UTC".
//   offset(x)            For each row, the offset of the array of its argument plus the offset of
//                        the batch.
//   rows_before(x)       For each row, the number of rows that its state evaluated before it.
//   task_attempt_id()    For each row, task_attempt_id of the task context that create got, read
//                        in eval.
//   fail_on(bigint, bigint)
//                        Returns its first argument, but fails with a message on a batch in which
//                        it equals the second argument.
//   fail_create(...)     create fails with a message.
//   null_state(...)      create succeeds without setting the state.
//   wait_for_interrupt(string)
//                        Creates the file <path>.waiting, where path is the argument of the first
//                        row, waits until is_interrupted is true, then creates the file <path> and
//                        fails. It fails without creating <path> after a minute.
//   bad_result(bigint, string)
//                        Returns a result that breaks a rule of the header, chosen by the second
//                        argument of the first row: "offset", an offset of 1; "short", one value
//                        fewer than the rows; "long", one value more; "null_count", a null_count of
//                        1 without a validity buffer; "children", a child array; "buffers", a NULL
//                        pointer to its buffers; "unset", out left unset.
//   bad_nested_result(bigint, string)
//                        Returns, for each row, an array<bigint> of its first argument, in a
//                        result whose child array breaks a rule of the header, chosen as in
//                        bad_result: "child_offset", an offset of 1; "short_child", one value fewer
//                        than its parent refers to; "null_child", a NULL child; "child_dictionary",
//                        a dictionary.
//   bad_map_result(bigint, string)
//                        Returns, for each row, a map from its first argument to itself, whose key
//                        breaks the type of the result, chosen as in bad_result: "null_key", a null
//                        key in the map of the first row.
//   keep_input(bigint)   Moves the input and keeps it until close, and returns its argument in an
//                        array that shares the buffers of the input when they have an offset of 0.
//                        close records an order violation if the kept buffers changed.
//
// create fails for an unknown name, for a kind other than SPARK_UDF_SCALAR, and unless the types
// of the arguments and of the result are those of the function. The suites compile it with these
// macros to get several distinct libraries:
//
//   ABI_VERSION     The version of the table, 1 by default.
//   NO_LAST_ERROR   Leaves out last_error.
//   TABLE_SIZE      The size of the table, sizeof(SparkUdfLibrary) by default: Spark must not read
//                   the functions after it.
//   INIT_FAILURE    1 to make spark_udf_init fail, after logging why, 2 to make it succeed without
//                   a table.
//   NO_INIT         Leaves out spark_udf_init, which makes the library invalid.
//   ORDER_VIOLATIONS_PATH
//                   A file to which the calls that the header rules out are appended, as lines: a
//                   call on a state that is closed or was never created, two calls on a state at
//                   the same time, a call after eval failed, an input without rows, with null rows
//                   or with another number of children than of arguments, a result released after
//                   its state was closed, and kept input buffers that changed.

#include "spark_udf.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <memory>
#include <mutex>
#include <set>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#ifndef ABI_VERSION
#define ABI_VERSION 1
#endif
#ifndef TABLE_SIZE
#define TABLE_SIZE sizeof(SparkUdfLibrary)
#endif
#ifndef INIT_FAILURE
#define INIT_FAILURE 0
#endif

namespace {

// ---------------------------------------------------------------------------------------------
// Helpers for the C interface
// ---------------------------------------------------------------------------------------------

thread_local std::string last_error;

const SparkUdfHost* host = nullptr;

[[maybe_unused]] void hostLog(int32_t level, const std::string& message) {
  auto fits = [](size_t end) { return host->size >= static_cast<int64_t>(end); };
  if (fits(offsetof(SparkUdfHost, is_log_enabled) + sizeof(host->is_log_enabled)) &&
      !host->is_log_enabled(level)) {
    return;
  }
  if (fits(offsetof(SparkUdfHost, log) + sizeof(host->log))) host->log(level, message.c_str());
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

[[maybe_unused]] const char* lastError(void) { return last_error.c_str(); }

void writeFile(const std::string& path, const std::string& content) {
  FILE* file = std::fopen(path.c_str(), "w");
  if (file == nullptr) throw std::runtime_error("cannot write " + path);
  std::fputs(content.c_str(), file);
  std::fclose(file);
}

// A failure to record is ignored: the checks that call it run before `guarded`, and must not throw.
std::mutex order_violations_lock;
void orderViolation(const std::string& call) {
#ifdef ORDER_VIOLATIONS_PATH
  std::lock_guard<std::mutex> guard(order_violations_lock);
  FILE* file = std::fopen(ORDER_VIOLATIONS_PATH, "a");
  if (file == nullptr) return;
  std::fputs((call + "\n").c_str(), file);
  std::fclose(file);
#else
  (void)call;
#endif
}

void checkSize(const char* struct_name, int64_t actual, size_t size) {
  if (actual < static_cast<int64_t>(size)) {
    throw std::runtime_error(std::string(struct_name) + " has a size of " + std::to_string(actual));
  }
}

// ---------------------------------------------------------------------------------------------
// Reading arrays
// ---------------------------------------------------------------------------------------------

bool bitSet(const void* bits, int64_t index) {
  return (static_cast<const uint8_t*>(bits)[index >> 3] >> (index & 7)) & 1;
}

void setBit(std::vector<uint8_t>& bits, int64_t index) {
  bits[static_cast<size_t>(index >> 3)] |= static_cast<uint8_t>(1 << (index & 7));
}

const void* validityOf(const ArrowArray* array) {
  if (array->null_count == 0 || array->n_buffers == 0) return nullptr;
  return array->buffers[0];
}

bool isValid(const ArrowArray* array, int64_t physical) {
  const void* validity = validityOf(array);
  return validity == nullptr || bitSet(validity, physical);
}

// An argument of a batch: row i of the batch is at the physical index base + i of the array.
struct Column {
  const ArrowArray* array;
  int64_t base;

  bool valid(int64_t row) const { return isValid(array, base + row); }

  int64_t int64(int64_t row) const {
    return static_cast<const int64_t*>(array->buffers[1])[base + row];
  }

  std::string string(int64_t row) const {
    const int32_t* offsets = static_cast<const int32_t*>(array->buffers[1]);
    const char* data = static_cast<const char*>(array->buffers[2]);
    int32_t start = offsets[base + row];
    return std::string(data + start, static_cast<size_t>(offsets[base + row + 1] - start));
  }
};

Column column(const ArrowArray* input, int64_t index) {
  const ArrowArray* child = input->children[index];
  return {child, input->offset + child->offset};
}

// The width in bytes of the values of a fixed-width format of version 1, 0 for bool, which has
// bits, and -1 for the other formats.
int64_t byteWidth(const std::string& format) {
  if (format == "c") return 1;
  if (format == "s") return 2;
  if (format == "i" || format == "f" || format == "tdD") return 4;
  if (format == "l" || format == "g" || format.rfind("tsu:", 0) == 0) return 8;
  if (format.rfind("d:", 0) == 0) {
    // decimal128 only: a third number in the format is the bit width.
    size_t comma = format.find(',');
    if (comma != std::string::npos && format.find(',', comma + 1) != std::string::npos &&
        format.substr(format.rfind(',') + 1) != "128") {
      return -1;
    }
    return 16;
  }
  if (format == "b") return 0;
  return -1;
}

// ---------------------------------------------------------------------------------------------
// Exporting arrays
// ---------------------------------------------------------------------------------------------

std::mutex states_lock;
// The IDs of the states that were closed, to find the results released after their close.
std::set<int64_t> closed_state_ids;

bool isClosed(int64_t state_id) {
  std::lock_guard<std::mutex> guard(states_lock);
  return closed_state_ids.count(state_id) > 0;
}

struct ArrayData {
  std::vector<std::vector<uint8_t>> owned;
  std::vector<const void*> buffers;
  std::vector<std::unique_ptr<ArrowArray>> children;
  std::vector<ArrowArray*> child_pointers;
  std::unique_ptr<ArrowArray> dictionary;
  // The state of the eval that returned the array, or 0 for a child array.
  int64_t state_id = 0;
};

void releaseArray(ArrowArray* array) {
  auto* data = static_cast<ArrayData*>(array->private_data);
  if (data->state_id != 0 && isClosed(data->state_id)) {
    orderViolation("release of a result after close of its state");
  }
  for (auto& child : data->children) {
    if (child->release != nullptr) child->release(child.get());
  }
  if (data->dictionary != nullptr && data->dictionary->release != nullptr) {
    data->dictionary->release(data->dictionary.get());
  }
  delete data;
  array->release = nullptr;
}

// The validity buffer is NULL when null_count is 0. The non-NULL entries of `shared` replace the
// owned buffers at their positions, and are not owned.
void exportArray(ArrowArray* out, int64_t length, int64_t null_count,
                 std::vector<std::vector<uint8_t>> owned, std::vector<ArrowArray> children = {},
                 std::vector<const void*> shared = {}) {
  auto* data = new ArrayData();
  data->owned = std::move(owned);
  for (size_t i = 0; i < data->owned.size(); i++) {
    // A buffer without bytes, such as the data of empty strings, is still a valid pointer.
    if (i > 0 && data->owned[i].empty()) data->owned[i].push_back(0);
    const void* buffer = i == 0 && null_count == 0 ? nullptr : data->owned[i].data();
    if (i < shared.size() && shared[i] != nullptr) buffer = shared[i];
    data->buffers.push_back(buffer);
  }
  for (auto& child : children) {
    data->children.push_back(std::make_unique<ArrowArray>(child));
    data->child_pointers.push_back(data->children.back().get());
  }
  out->length = length;
  out->null_count = null_count;
  out->offset = 0;
  out->n_buffers = static_cast<int64_t>(data->buffers.size());
  out->n_children = static_cast<int64_t>(data->children.size());
  out->buffers = data->buffers.empty() ? nullptr : data->buffers.data();
  out->children = data->child_pointers.empty() ? nullptr : data->child_pointers.data();
  out->dictionary = nullptr;
  out->release = releaseArray;
  out->private_data = data;
}

template <typename T>
std::vector<uint8_t> bytesOf(const std::vector<T>& values) {
  std::vector<uint8_t> bytes(values.size() * sizeof(T));
  if (!bytes.empty()) std::memcpy(bytes.data(), values.data(), bytes.size());
  return bytes;
}

std::vector<uint8_t> validityBuffer(const std::vector<bool>& valid, int64_t* null_count) {
  std::vector<uint8_t> bits((valid.size() + 7) / 8, 0);
  *null_count = 0;
  for (size_t i = 0; i < valid.size(); i++) {
    if (valid[i]) {
      setBit(bits, static_cast<int64_t>(i));
    } else {
      (*null_count)++;
    }
  }
  return bits;
}

template <typename T>
void exportValues(ArrowArray* out, const std::vector<T>& values, const std::vector<bool>& valid) {
  int64_t null_count = 0;
  std::vector<uint8_t> validity = validityBuffer(valid, &null_count);
  exportArray(out, static_cast<int64_t>(values.size()), null_count,
              {std::move(validity), bytesOf(values)});
}

void exportStrings(ArrowArray* out, const std::vector<std::string>& values,
                   const std::vector<bool>& valid) {
  int64_t null_count = 0;
  std::vector<uint8_t> validity = validityBuffer(valid, &null_count);
  std::vector<int32_t> offsets{0};
  std::vector<uint8_t> data;
  for (const auto& value : values) {
    data.insert(data.end(), value.begin(), value.end());
    offsets.push_back(static_cast<int32_t>(data.size()));
  }
  exportArray(out, static_cast<int64_t>(values.size()), null_count,
              {std::move(validity), bytesOf(offsets), std::move(data)});
}

// Copies `length` values of `array`, of the type of `field`, from its logical index `start`, to
// `out`, with an offset of 0 at every level. The children of a struct are read at the physical
// index of the struct, and those of a list or a map through its offsets.
void copyArray(const ArrowSchema* field, const ArrowArray* array, int64_t start, int64_t length,
               ArrowArray* out) {
  const std::string format = field->format;
  const int64_t base = array->offset + start;
  std::vector<uint8_t> validity((static_cast<size_t>(length) + 7) / 8, 0);
  int64_t null_count = 0;
  for (int64_t i = 0; i < length; i++) {
    if (isValid(array, base + i)) {
      setBit(validity, i);
    } else {
      null_count++;
    }
  }
  int64_t width = byteWidth(format);
  if (width == 0) {
    std::vector<uint8_t> values(validity.size(), 0);
    for (int64_t i = 0; i < length; i++) {
      if (bitSet(array->buffers[1], base + i)) setBit(values, i);
    }
    exportArray(out, length, null_count, {std::move(validity), std::move(values)});
  } else if (width > 0) {
    const uint8_t* values = static_cast<const uint8_t*>(array->buffers[1]) + base * width;
    exportArray(out, length, null_count,
                {std::move(validity), std::vector<uint8_t>(values, values + length * width)});
  } else if (format == "u" || format == "z") {
    const int32_t* offsets = static_cast<const int32_t*>(array->buffers[1]) + base;
    const uint8_t* data = static_cast<const uint8_t*>(array->buffers[2]);
    std::vector<int32_t> copied(static_cast<size_t>(length) + 1);
    for (int64_t i = 0; i <= length; i++) copied[i] = offsets[i] - offsets[0];
    exportArray(out, length, null_count,
                {std::move(validity), bytesOf(copied),
                 std::vector<uint8_t>(data + offsets[0], data + offsets[length])});
  } else if (format == "+l" || format == "+m") {
    const int32_t* offsets = static_cast<const int32_t*>(array->buffers[1]) + base;
    std::vector<int32_t> copied(static_cast<size_t>(length) + 1);
    for (int64_t i = 0; i <= length; i++) copied[i] = offsets[i] - offsets[0];
    ArrowArray child{};
    copyArray(field->children[0], array->children[0], offsets[0], offsets[length] - offsets[0],
              &child);
    exportArray(out, length, null_count, {std::move(validity), bytesOf(copied)}, {child});
  } else if (format == "+s") {
    std::vector<ArrowArray> children;
    for (int64_t c = 0; c < field->n_children; c++) {
      children.emplace_back();
      copyArray(field->children[c], array->children[c], base, length, &children.back());
    }
    exportArray(out, length, null_count, {std::move(validity)}, children);
  } else {
    throw std::runtime_error("unsupported format " + format);
  }
}

// The type of a field, as its format and those of its children, such as "+m(+s(u,l))".
std::string typeOf(const ArrowSchema* field) {
  std::string type = field->format;
  if (field->n_children > 0) {
    type += "(";
    for (int64_t i = 0; i < field->n_children; i++) {
      if (i > 0) type += ",";
      type += typeOf(field->children[i]);
    }
    type += ")";
  }
  return type;
}

// Like typeOf, with "?" after each nullable field.
std::string describeField(const ArrowSchema* field) {
  std::string description = field->format;
  if (field->flags & ARROW_FLAG_NULLABLE) description += "?";
  if (field->n_children > 0) {
    description += "(";
    for (int64_t i = 0; i < field->n_children; i++) {
      if (i > 0) description += ",";
      description += describeField(field->children[i]);
    }
    description += ")";
  }
  return description;
}

// ---------------------------------------------------------------------------------------------
// States
// ---------------------------------------------------------------------------------------------

std::atomic<int64_t> next_state_id{1};

struct State {
  int64_t id;
  std::string name;
  std::string description;
  // The schema of the arguments, moved from create, which identity reads in each eval.
  ArrowSchema arguments{};
  const SparkUdfTaskContext* task;
  int64_t rows = 0;
  bool failed = false;
  std::atomic<bool> in_call{false};
  // The inputs that keep_input moved, with a checksum of their values when it moved them.
  std::vector<ArrowArray> kept;
  std::vector<uint64_t> kept_checksums;
};

// The states that create returned and that are not closed. A call on another state is an order
// violation, which must not use the state.
std::set<void*> live_states;

State* liveState(void* handle, const char* call) {
  {
    std::lock_guard<std::mutex> guard(states_lock);
    if (live_states.count(handle) > 0) return static_cast<State*>(handle);
  }
  orderViolation(std::string(call) + " on a state that is closed or was never created");
  return nullptr;
}

class InCall {
 public:
  explicit InCall(State* state, const char* call) : state_(state) {
    if (state_->in_call.exchange(true)) orderViolation(std::string(call) + " during a call");
  }
  ~InCall() { state_->in_call.store(false); }

 private:
  State* state_;
};

uint64_t checksum(const ArrowArray* input) {
  Column x = column(input, 0);
  uint64_t hash = 1469598103934665603ULL;
  for (int64_t i = 0; i < input->length; i++) {
    uint64_t value = x.valid(i) ? static_cast<uint64_t>(x.int64(i)) : 0x5bd1e995ULL;
    hash = (hash ^ value) * 1099511628211ULL;
  }
  return hash;
}

// ---------------------------------------------------------------------------------------------
// The functions
// ---------------------------------------------------------------------------------------------

struct Function {
  const char* name;
  // The types of the arguments and of the result, as typeOf gives them, or null to accept any.
  const char* arguments;
  const char* result;
};

const Function kFunctions[] = {
    {"identity", nullptr, nullptr},
    {"add_one", "l", "l"},
    {"half_if_even", "l", "l"},
    {"shout", "u", "u"},
    {"edit_distance", "u,u", "i"},
    {"describe", nullptr, "u"},
    {"offset", nullptr, "l"},
    {"rows_before", nullptr, "l"},
    {"task_attempt_id", "", "l"},
    {"fail_on", "l,l", "l"},
    {"fail_create", nullptr, nullptr},
    {"null_state", nullptr, nullptr},
    {"wait_for_interrupt", "u", "l"},
    {"bad_result", "l,u", "l"},
    {"bad_nested_result", "l,u", "+l(l)"},
    {"bad_map_result", "l,u", "+m(+s(l,l))"},
    {"keep_input", "l", "l"},
};

std::string argumentTypes(const ArrowSchema* arguments) {
  std::string types;
  for (int64_t i = 0; i < arguments->n_children; i++) {
    if (i > 0) types += ",";
    types += typeOf(arguments->children[i]);
  }
  return types;
}

void checkTypes(const SparkUdfCreateInfo* info) {
  const Function* function = nullptr;
  for (const auto& candidate : kFunctions) {
    if (std::strcmp(candidate.name, info->name) == 0) function = &candidate;
  }
  if (function == nullptr) throw std::runtime_error(std::string("unknown function ") + info->name);
  std::string arguments = argumentTypes(info->arguments);
  std::string result = typeOf(info->result);
  bool one_argument = info->arguments->n_children == 1;
  bool same = function->name == std::string("identity")
                  ? one_argument && arguments == result
                  : (function->arguments == nullptr || arguments == function->arguments) &&
                        (function->result == nullptr || result == function->result);
  bool any_one = std::string(function->name) == "offset" ||
                 std::string(function->name) == "rows_before";
  if (!same || (any_one && !one_argument)) {
    throw std::runtime_error(std::string(info->name) + ": unsupported types (" + arguments +
                             ") -> " + result);
  }
}

int createState(const SparkUdfCreateInfo* info, void** out) {
  return guarded([&] {
    checkSize("SparkUdfCreateInfo", info->size, sizeof(SparkUdfCreateInfo));
    checkSize("SparkUdfTaskContext", info->task->size, sizeof(SparkUdfTaskContext));
    if (info->kind != SPARK_UDF_SCALAR) {
      throw std::runtime_error("unsupported kind " + std::to_string(info->kind));
    }
    checkTypes(info);
    std::string name = info->name;
    if (name == "fail_create") throw std::runtime_error("fail_create: injected failure");
    if (name == "null_state") return;
    auto state = std::make_unique<State>();
    state->id = next_state_id++;
    state->name = name;
    std::string arguments;
    for (int64_t i = 0; i < info->arguments->n_children; i++) {
      if (i > 0) arguments += ",";
      arguments += describeField(info->arguments->children[i]);
    }
    state->description =
        arguments + "->" + describeField(info->result) + ";" + info->session_time_zone;
    state->task = info->task;
    state->arguments = *info->arguments;
    info->arguments->release = nullptr;
    std::lock_guard<std::mutex> guard(states_lock);
    live_states.insert(state.get());
    *out = state.release();
  });
}

void addOne(const ArrowArray* input, ArrowArray* out) {
  Column x = column(input, 0);
  std::vector<int64_t> values(static_cast<size_t>(input->length));
  std::vector<bool> valid(values.size());
  bool nulls = false;
  for (int64_t i = 0; i < input->length; i++) {
    valid[i] = x.valid(i);
    nulls = nulls || !valid[i];
    if (valid[i]) values[i] = static_cast<int64_t>(static_cast<uint64_t>(x.int64(i)) + 1);
  }
  if (nulls) {
    exportValues(out, values, valid);
  } else {
    exportArray(out, input->length, 0, {{}, bytesOf(values)});
  }
}

void halfIfEven(const ArrowArray* input, ArrowArray* out) {
  Column x = column(input, 0);
  std::vector<int64_t> values(static_cast<size_t>(input->length));
  std::vector<bool> valid(values.size());
  for (int64_t i = 0; i < input->length; i++) {
    valid[i] = x.valid(i) && x.int64(i) % 2 == 0;
    if (valid[i]) values[i] = x.int64(i) / 2;
  }
  exportValues(out, values, valid);
}

void shout(const ArrowArray* input, ArrowArray* out) {
  Column s = column(input, 0);
  std::vector<std::string> values(static_cast<size_t>(input->length));
  std::vector<bool> valid(values.size());
  for (int64_t i = 0; i < input->length; i++) {
    valid[i] = s.valid(i);
    if (!valid[i]) continue;
    std::string value = s.string(i);
    for (char& c : value) {
      if (c >= 'a' && c <= 'z') c = static_cast<char>(c - 'a' + 'A');
    }
    values[i] = value + "!";
  }
  exportStrings(out, values, valid);
}

// A byte that does not start a valid UTF-8 sequence counts as one code point.
std::vector<uint32_t> codePoints(const std::string& value) {
  std::vector<uint32_t> points;
  size_t i = 0;
  while (i < value.size()) {
    unsigned char c = static_cast<unsigned char>(value[i]);
    size_t extra = c >= 0xF0 ? 3 : c >= 0xE0 ? 2 : c >= 0xC0 ? 1 : 0;
    if (i + extra >= value.size()) extra = 0;
    uint32_t point = extra == 0 ? c : c & (0x3F >> extra);
    for (size_t k = 1; k <= extra; k++) {
      point = (point << 6) | (static_cast<unsigned char>(value[i + k]) & 0x3F);
    }
    points.push_back(point);
    i += extra + 1;
  }
  return points;
}

int32_t levenshtein(const std::vector<uint32_t>& a, const std::vector<uint32_t>& b) {
  std::vector<int32_t> previous(b.size() + 1);
  std::vector<int32_t> current(b.size() + 1);
  for (size_t j = 0; j <= b.size(); j++) previous[j] = static_cast<int32_t>(j);
  for (size_t i = 1; i <= a.size(); i++) {
    current[0] = static_cast<int32_t>(i);
    for (size_t j = 1; j <= b.size(); j++) {
      int32_t substitution = previous[j - 1] + (a[i - 1] == b[j - 1] ? 0 : 1);
      current[j] = std::min(std::min(previous[j] + 1, current[j - 1] + 1), substitution);
    }
    std::swap(previous, current);
  }
  return previous[b.size()];
}

void editDistance(const ArrowArray* input, ArrowArray* out) {
  Column a = column(input, 0);
  Column b = column(input, 1);
  std::vector<int32_t> values(static_cast<size_t>(input->length));
  std::vector<bool> valid(values.size());
  for (int64_t i = 0; i < input->length; i++) {
    valid[i] = a.valid(i) && b.valid(i);
    if (valid[i]) values[i] = levenshtein(codePoints(a.string(i)), codePoints(b.string(i)));
  }
  exportValues(out, values, valid);
}

void constantStrings(const ArrowArray* input, const std::string& value, ArrowArray* out) {
  std::vector<std::string> values(static_cast<size_t>(input->length), value);
  exportStrings(out, values, std::vector<bool>(values.size(), true));
}

void exportFirst(const ArrowArray* input, ArrowArray* out) {
  Column x = column(input, 0);
  std::vector<int64_t> values(static_cast<size_t>(input->length));
  std::vector<bool> valid(values.size());
  for (int64_t i = 0; i < input->length; i++) {
    valid[i] = x.valid(i);
    if (valid[i]) values[i] = x.int64(i);
  }
  exportValues(out, values, valid);
}

void constantLongs(const ArrowArray* input, int64_t value, ArrowArray* out) {
  std::vector<int64_t> values(static_cast<size_t>(input->length), value);
  exportArray(out, input->length, 0, {{}, bytesOf(values)});
}

void failOn(const ArrowArray* input, ArrowArray* out) {
  Column x = column(input, 0);
  Column failing = column(input, 1);
  for (int64_t i = 0; i < input->length; i++) {
    if (x.valid(i) && failing.valid(i) && x.int64(i) == failing.int64(i)) {
      throw std::runtime_error("fail_on: got " + std::to_string(x.int64(i)) + " in row " +
                               std::to_string(i) + " of the batch");
    }
  }
  exportFirst(input, out);
}

void waitForInterrupt(State* state, const ArrowArray* input) {
  std::string path = column(input, 0).string(0);
  writeFile(path + ".waiting", "");
  for (int i = 0; i < 6000; i++) {
    if (state->task->is_interrupted(state->task)) {
      writeFile(path, "interrupted");
      throw std::runtime_error("wait_for_interrupt: interrupted");
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  throw std::runtime_error("wait_for_interrupt: not interrupted after a minute");
}

// Succeeds without setting out for "unset".
void badResult(const ArrowArray* input, ArrowArray* out) {
  Column x = column(input, 0);
  std::string how = column(input, 1).string(0);
  std::vector<int64_t> values;
  for (int64_t i = 0; i < input->length; i++) values.push_back(x.valid(i) ? x.int64(i) : 0);
  if (how == "offset") {
    values.insert(values.begin(), 0);
    exportArray(out, input->length, 0, {{}, bytesOf(values)});
    out->offset = 1;
  } else if (how == "short" || how == "long") {
    if (how == "short") {
      values.pop_back();
    } else {
      values.push_back(0);
    }
    exportArray(out, static_cast<int64_t>(values.size()), 0, {{}, bytesOf(values)});
  } else if (how == "null_count") {
    exportArray(out, input->length, 0, {{}, bytesOf(values)});
    out->null_count = 1;
  } else if (how == "children") {
    ArrowArray child{};
    exportArray(&child, input->length, 0, {{}, bytesOf(values)});
    exportArray(out, input->length, 0, {{}, bytesOf(values)}, {child});
  } else if (how == "buffers") {
    exportArray(out, input->length, 0, {{}, bytesOf(values)});
    out->buffers = nullptr;
  } else if (how != "unset") {
    throw std::runtime_error("bad_result: unknown kind " + how);
  }
}

void badNestedResult(const ArrowArray* input, ArrowArray* out) {
  Column x = column(input, 0);
  std::string how = column(input, 1).string(0);
  std::vector<int64_t> values;
  std::vector<int32_t> offsets{0};
  for (int64_t i = 0; i < input->length; i++) {
    values.push_back(x.valid(i) ? x.int64(i) : 0);
    offsets.push_back(static_cast<int32_t>(i + 1));
  }
  ArrowArray child{};
  if (how == "child_offset") {
    values.insert(values.begin(), 0);
    exportArray(&child, input->length, 0, {{}, bytesOf(values)});
    child.offset = 1;
  } else if (how == "short_child") {
    values.pop_back();
    exportArray(&child, static_cast<int64_t>(values.size()), 0, {{}, bytesOf(values)});
  } else if (how == "null_child" || how == "child_dictionary") {
    exportArray(&child, input->length, 0, {{}, bytesOf(values)});
  } else {
    throw std::runtime_error("bad_nested_result: unknown kind " + how);
  }
  exportArray(out, input->length, 0, {{}, bytesOf(offsets)}, {child});
  auto* data = static_cast<ArrayData*>(out->private_data);
  if (how == "null_child") {
    data->child_pointers[0] = nullptr;
  } else if (how == "child_dictionary") {
    ArrowArray* exported = data->children[0].get();
    auto* child_data = static_cast<ArrayData*>(exported->private_data);
    child_data->dictionary = std::make_unique<ArrowArray>();
    exportArray(child_data->dictionary.get(), 1, 0, {{}, bytesOf(std::vector<int64_t>{0})});
    exported->dictionary = child_data->dictionary.get();
  }
}

void badMapResult(const ArrowArray* input, ArrowArray* out) {
  Column x = column(input, 0);
  std::string how = column(input, 1).string(0);
  if (how != "null_key") throw std::runtime_error("bad_map_result: unknown kind " + how);
  std::vector<int64_t> values;
  std::vector<int32_t> offsets{0};
  std::vector<uint8_t> valid_keys(static_cast<size_t>((input->length + 7) / 8), 0);
  for (int64_t i = 0; i < input->length; i++) {
    values.push_back(x.valid(i) ? x.int64(i) : 0);
    offsets.push_back(static_cast<int32_t>(i + 1));
    if (i > 0) setBit(valid_keys, i);
  }
  ArrowArray keys{};
  exportArray(&keys, input->length, 1, {std::move(valid_keys), bytesOf(values)});
  ArrowArray items{};
  exportArray(&items, input->length, 0, {{}, bytesOf(values)});
  ArrowArray entries{};
  exportArray(&entries, input->length, 0, {{}}, {keys, items});
  exportArray(out, input->length, 0, {{}, bytesOf(offsets)}, {entries});
}

void keepInput(State* state, ArrowArray* input, ArrowArray* out) {
  state->kept.push_back(*input);
  input->release = nullptr;
  const ArrowArray* kept = &state->kept.back();
  state->kept_checksums.push_back(checksum(kept));
  Column x = column(kept, 0);
  if (x.base == 0) {
    exportArray(out, kept->length, x.array->null_count, {{}, {}}, {},
                {validityOf(x.array), x.array->buffers[1]});
  } else {
    exportFirst(kept, out);
  }
}

bool checkInput(const State* state, const ArrowArray* input) {
  std::string problem;
  if (input->length < 1) {
    problem = "an input without rows";
  } else if (input->null_count != 0) {
    problem = "an input with null rows";
  } else if (input->n_children != state->arguments.n_children) {
    problem = "an input with " + std::to_string(input->n_children) + " children for " +
              std::to_string(state->arguments.n_children) + " arguments";
  }
  if (problem.empty()) return true;
  orderViolation("eval of " + state->name + " with " + problem);
  last_error = state->name + ": " + problem;
  return false;
}

int evalBatch(void* handle, const SparkUdfEvalInfo* info, ArrowArray* input, ArrowArray* out) {
  State* state = liveState(handle, "eval");
  if (state == nullptr) {
    last_error = "eval on an unknown state";
    return 1;
  }
  InCall in_call(state, "eval");
  if (state->failed) orderViolation("eval of " + state->name + " after eval failed");
  int status = 1;
  if (checkInput(state, input)) {
    const std::string& name = state->name;
    status = guarded([&] {
      checkSize("SparkUdfEvalInfo", info->size, sizeof(SparkUdfEvalInfo));
      if (name == "identity") {
        copyArray(state->arguments.children[0], input->children[0], input->offset, input->length,
                  out);
      } else if (name == "add_one") {
        addOne(input, out);
      } else if (name == "half_if_even") {
        halfIfEven(input, out);
      } else if (name == "shout") {
        shout(input, out);
      } else if (name == "edit_distance") {
        editDistance(input, out);
      } else if (name == "describe") {
        constantStrings(input, state->description, out);
      } else if (name == "offset") {
        constantLongs(input, column(input, 0).base, out);
      } else if (name == "rows_before") {
        std::vector<int64_t> values;
        for (int64_t i = 0; i < input->length; i++) values.push_back(state->rows + i);
        exportArray(out, input->length, 0, {{}, bytesOf(values)});
      } else if (name == "task_attempt_id") {
        constantLongs(input, state->task->task_attempt_id, out);
      } else if (name == "fail_on") {
        failOn(input, out);
      } else if (name == "wait_for_interrupt") {
        waitForInterrupt(state, input);
      } else if (name == "bad_result") {
        badResult(input, out);
      } else if (name == "bad_nested_result") {
        badNestedResult(input, out);
      } else if (name == "bad_map_result") {
        badMapResult(input, out);
      } else if (name == "keep_input") {
        keepInput(state, input, out);
      }
    });
  }
  if (status == 0) {
    state->rows += input->length;
    if (out->release != nullptr) static_cast<ArrayData*>(out->private_data)->state_id = state->id;
  } else {
    state->failed = true;
  }
  return status;
}

void closeState(void* handle) {
  State* state = liveState(handle, "close");
  if (state == nullptr) return;
  {
    InCall in_call(state, "close");
    std::lock_guard<std::mutex> guard(states_lock);
    live_states.erase(handle);
    closed_state_ids.insert(state->id);
  }
  for (size_t i = 0; i < state->kept.size(); i++) {
    if (checksum(&state->kept[i]) != state->kept_checksums[i]) {
      orderViolation("the buffers of an input that keep_input kept changed before close");
    }
    state->kept[i].release(&state->kept[i]);
  }
  if (state->arguments.release != nullptr) state->arguments.release(&state->arguments);
  delete state;
}

// The table, which NO_INIT leaves unused.
[[maybe_unused]] SparkUdfLibrary makeTable() {
  SparkUdfLibrary table{};
  table.size = TABLE_SIZE;
  table.abi_version = ABI_VERSION;
#ifndef NO_LAST_ERROR
  table.last_error = lastError;
#endif
  table.create = createState;
  table.eval = evalBatch;
  table.close = closeState;
  return table;
}

}  // namespace

// ---------------------------------------------------------------------------------------------
// spark_udf.h
// ---------------------------------------------------------------------------------------------

#ifndef NO_INIT

int spark_udf_init(const SparkUdfHost* spark, const SparkUdfLibrary** library) {
  host = spark;
  if (INIT_FAILURE == 1) {
    hostLog(SPARK_UDF_LOG_ERROR, "injected failure in spark_udf_init");
    return 1;
  }
  // Succeeds without returning a table, which Spark must reject.
  if (INIT_FAILURE == 2) return 0;
  static const SparkUdfLibrary table = makeTable();
  *library = &table;
  return 0;
}

#endif  // NO_INIT
