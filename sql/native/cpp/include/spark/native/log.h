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

// The Spark process that loaded the library: its version and its log.

#pragma once

#include <atomic>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>

#include "spark_datasource.h"

namespace spark::native {

/// The levels of the messages that Log passes to the log of the Spark driver or executor.
enum class LogLevel : int32_t {
  kError = SPARK_DS_LOG_ERROR,
  kWarn = SPARK_DS_LOG_WARN,
  kInfo = SPARK_DS_LOG_INFO,
  kDebug = SPARK_DS_LOG_DEBUG,
  kTrace = SPARK_DS_LOG_TRACE,
};

/// The Spark process that loaded the library, as DataSource::Init receives it.
struct HostInfo {
  /// A host that supports the ABI version of this SDK, for testing outside Spark.
  HostInfo() = default;

  /// The newest version of the native data source ABI that Spark supports.
  int32_t abi_version = SPARK_DS_ABI_VERSION;
  /// The version of Spark, such as "4.4.0", when Spark provides it.
  std::optional<std::string> spark_version;
};

namespace internal {

using HostLog = void (*)(int32_t level, const char* message);

// Copied from the host when Spark initializes the library, so that logging never reads the host
// structure afterwards. Messages are dropped until then, for example in unit tests.
inline std::atomic<HostLog> host_log{nullptr};
inline std::atomic<int32_t> max_log_level{static_cast<int32_t>(LogLevel::kInfo)};

}  // namespace internal

/// Passes the messages up to `level`, which is LogLevel::kInfo by default. Call it, for example in
/// DataSource::Init, to pass more.
inline void SetMaxLogLevel(LogLevel level) {
  internal::max_log_level.store(static_cast<int32_t>(level), std::memory_order_relaxed);
}

/// The most verbose level of the messages that reach Spark.
inline LogLevel MaxLogLevel() {
  return static_cast<LogLevel>(internal::max_log_level.load(std::memory_order_relaxed));
}

/// Whether a message of `level` reaches the log of Spark, which requires Spark to have initialized
/// the library.
inline bool IsLogEnabled(LogLevel level) {
  return internal::host_log.load(std::memory_order_acquire) != nullptr &&
         static_cast<int32_t>(level) <= internal::max_log_level.load(std::memory_order_relaxed);
}

/// Writes a message to the log of the Spark driver or executor, with the path of the library, once
/// Spark initializes the library. Any thread can call it. Outside Spark, for example in unit tests,
/// messages are dropped.
inline void Log(LogLevel level, std::string_view message) {
  internal::HostLog log = internal::host_log.load(std::memory_order_acquire);
  if (log == nullptr ||
      static_cast<int32_t>(level) > internal::max_log_level.load(std::memory_order_relaxed)) {
    return;
  }
  // The message is a C string, so a NUL in it would truncate it.
  std::string text;
  text.reserve(message.size());
  for (char c : message) {
    if (c == '\0') {
      text += "\\0";
    } else {
      text += c;
    }
  }
  log(static_cast<int32_t>(level), text.c_str());
}

inline void LogError(std::string_view message) { Log(LogLevel::kError, message); }
inline void LogWarn(std::string_view message) { Log(LogLevel::kWarn, message); }
inline void LogInfo(std::string_view message) { Log(LogLevel::kInfo, message); }
inline void LogDebug(std::string_view message) { Log(LogLevel::kDebug, message); }
inline void LogTrace(std::string_view message) { Log(LogLevel::kTrace, message); }

}  // namespace spark::native
