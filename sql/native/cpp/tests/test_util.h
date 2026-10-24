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

// A minimal test harness, so that the tests need no test framework, and helpers to call the
// function table of a library as Spark does.

#pragma once

#include <atomic>
#include <cstdint>
#include <cstdio>
#include <deque>
#include <ostream>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <arrow/result.h>
#include <arrow/status.h>

#include "spark_datasource.h"

namespace spark::native::testing {

struct TestCase {
  const char* name;
  void (*body)();
};

inline std::vector<TestCase>& Registry() {
  static std::vector<TestCase> tests;
  return tests;
}

struct Registration {
  Registration(const char* name, void (*body)()) { Registry().push_back({name, body}); }
};

/// A failed check, which ends its test.
struct Failure : std::runtime_error {
  using std::runtime_error::runtime_error;
};

[[noreturn]] inline void Fail(const char* file, int line, const std::string& message) {
  throw Failure(std::string(file) + ":" + std::to_string(line) + ": " + message);
}

template <typename T>
std::string Describe(const T& value) {
  if constexpr (requires(std::ostream& out) { out << value; }) {
    std::ostringstream out;
    out << value;
    return out.str();
  } else {
    return "(a value that cannot be printed)";
  }
}

inline arrow::Status ToStatus(const arrow::Status& status) { return status; }

template <typename T>
arrow::Status ToStatus(const arrow::Result<T>& result) {
  return result.status();
}

inline int RunAllTests() {
  int failures = 0;
  for (const TestCase& test : Registry()) {
    try {
      test.body();
      std::printf("[ OK ] %s\n", test.name);
    } catch (const std::exception& error) {
      ++failures;
      std::printf("[FAIL] %s\n       %s\n", test.name, error.what());
    }
  }
  std::printf("%zu tests, %d failed\n", Registry().size(), failures);
  return failures == 0 ? 0 : 1;
}

// Calls of a function table, as Spark makes them.

/// A sink that collects the values that the library returns.
inline void Collect(void* sink, const uint8_t* data, int64_t length) {
  auto& values = *static_cast<std::vector<std::string>*>(sink);
  values.emplace_back(reinterpret_cast<const char*>(data), static_cast<size_t>(length));
}

/// The message of the last error of the calling thread.
inline std::string LastError(const SparkDsLibrary& library) {
  const char* message = library.last_error();
  return message == nullptr ? std::string("(no error)") : std::string(message);
}

/// A SparkDsCreateInfo for a name and options, which keeps the strings that it points to.
class CreateArguments {
 public:
  CreateArguments(std::string name, std::vector<std::pair<std::string, std::string>> options)
      : name_(std::move(name)), options_(std::move(options)) {
    for (const auto& [key, value] : options_) {
      keys_.push_back(key.c_str());
      values_.push_back(value.c_str());
    }
    info_.size = sizeof(SparkDsCreateInfo);
    info_.name = name_.c_str();
    info_.option_keys = keys_.data();
    info_.option_values = values_.data();
    info_.num_options = static_cast<int64_t>(keys_.size());
  }

  CreateArguments(const CreateArguments&) = delete;
  CreateArguments& operator=(const CreateArguments&) = delete;

  const SparkDsCreateInfo* info() const { return &info_; }

 private:
  std::string name_;
  std::vector<std::pair<std::string, std::string>> options_;
  std::vector<const char*> keys_;
  std::vector<const char*> values_;
  SparkDsCreateInfo info_{};
};

using HostLogFunction = void (*)(int32_t level, const char* message);

/// A host that, like Spark's, stays valid until the process exits.
inline const SparkDsHost* LeakedHost(HostLogFunction log, const char* spark_version) {
  static std::deque<SparkDsHost> hosts;
  return &hosts.emplace_back(
      SparkDsHost{sizeof(SparkDsHost), SPARK_DS_ABI_VERSION, spark_version, log});
}

/// Initializes the data source that the test executable exports.
inline const SparkDsLibrary& InitializeLibrary(HostLogFunction log = nullptr) {
  const SparkDsLibrary* library = nullptr;
  if (spark_ds_init(LeakedHost(log, "test"), &library) != 0 || library == nullptr) {
    Fail(__FILE__, __LINE__, "spark_ds_init failed");
  }
  return *library;
}

/// A task context of Spark whose cancellation a test controls.
struct HostTask {
  SparkDsTaskContext context;
  std::atomic<bool> cancelled{false};

  explicit HostTask(int32_t partition_id, int64_t task_attempt_id) {
    context.size = sizeof(SparkDsTaskContext);
    context.stage_id = 3;
    context.stage_attempt_number = 1;
    context.partition_id = partition_id;
    context.attempt_number = 2;
    context.task_attempt_id = task_attempt_id;
    context.is_interrupted = &IsInterrupted;
  }

  HostTask(const HostTask&) = delete;
  HostTask& operator=(const HostTask&) = delete;

  static bool IsInterrupted(const SparkDsTaskContext* task) {
    // The context is the first member, so the callback can find the test's flag.
    return reinterpret_cast<const HostTask*>(task)->cancelled;
  }
};

}  // namespace spark::native::testing

#define TEST(name)                                                                       \
  static void name();                                                                    \
  static const ::spark::native::testing::Registration name##_registration(#name, &name); \
  static void name()

#define CHECK(condition)                                                                  \
  do {                                                                                    \
    if (!(condition)) {                                                                   \
      ::spark::native::testing::Fail(__FILE__, __LINE__, "CHECK(" #condition ") failed"); \
    }                                                                                     \
  } while (0)

#define CHECK_EQ(left, right)                                                               \
  do {                                                                                      \
    const auto check_left_ = (left);                                                        \
    const auto check_right_ = (right);                                                      \
    if (!(check_left_ == check_right_)) {                                                   \
      ::spark::native::testing::Fail(__FILE__, __LINE__,                                    \
                                     "CHECK_EQ(" #left ", " #right ") failed: " +           \
                                         ::spark::native::testing::Describe(check_left_) +  \
                                         " is not " +                                       \
                                         ::spark::native::testing::Describe(check_right_)); \
    }                                                                                       \
  } while (0)

#define CHECK_CONTAINS(text, part)                                                               \
  do {                                                                                           \
    const std::string check_text_ = (text);                                                      \
    if (check_text_.find(part) == std::string::npos) {                                           \
      ::spark::native::testing::Fail(__FILE__, __LINE__,                                         \
                                     "'" + check_text_ + "' does not contain '" + (part) + "'"); \
    }                                                                                            \
  } while (0)

#define CHECK_OK(expression)                                                              \
  do {                                                                                    \
    const ::arrow::Status check_status_ = ::spark::native::testing::ToStatus(expression); \
    if (!check_status_.ok()) {                                                            \
      ::spark::native::testing::Fail(__FILE__, __LINE__,                                  \
                                     #expression " failed: " + check_status_.ToString()); \
    }                                                                                     \
  } while (0)

/// Checks that a call of a function table succeeds, and shows the error of the library if not.
#define CHECK_SUCCESS(library, call)                                                             \
  do {                                                                                           \
    const int check_code_ = (call);                                                              \
    if (check_code_ != 0) {                                                                      \
      ::spark::native::testing::Fail(                                                            \
          __FILE__, __LINE__, #call " failed: " + ::spark::native::testing::LastError(library)); \
    }                                                                                            \
  } while (0)

#define SPARK_TEST_CONCAT_(left, right) left##right
#define SPARK_TEST_CONCAT(left, right) SPARK_TEST_CONCAT_(left, right)

#define ASSIGN_OR_FAIL(lhs, expression) \
  ASSIGN_OR_FAIL_IMPL(SPARK_TEST_CONCAT(assign_result_, __LINE__), lhs, expression)

#define ASSIGN_OR_FAIL_IMPL(result, lhs, expression)                                      \
  auto&& result = (expression);                                                           \
  if (!result.ok()) {                                                                     \
    ::spark::native::testing::Fail(__FILE__, __LINE__,                                    \
                                   #expression " failed: " + result.status().ToString()); \
  }                                                                                       \
  lhs = std::move(result).ValueUnsafe()
