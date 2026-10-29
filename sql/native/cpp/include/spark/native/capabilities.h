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

// The native operations of a source instance.

#pragma once

#include <cstdint>
#include <string>

#include "spark_datasource.h"

namespace spark::native {

/// The native operations that a source instance supports with its effective options.
///
/// A source implements the operations whose types in DataSource are not Unsupported. The SDK
/// keeps only those of DataSource::capabilities(), which can therefore remove some, but cannot
/// enable an operation that the source does not implement.
///
/// These are native capabilities, not Spark's Data Source V2 routing flags or per-scan pushdown
/// results.
class Capabilities {
 public:
  static const Capabilities kBatchRead;
  static const Capabilities kMicroBatchRead;
  static const Capabilities kBatchWrite;
  static const Capabilities kStreamingWrite;
  /// Writes that replace the existing data: batch writes with the save mode "overwrite", and
  /// micro-batches with the output mode "complete". Requires kBatchWrite or kStreamingWrite: the
  /// SDK removes it from a set without them.
  static const Capabilities kTruncate;

  /// No operations. A source without capabilities still supports schema discovery.
  static constexpr Capabilities Empty() { return Capabilities(0); }

  /// Every operation of this SDK, which DataSource::capabilities() returns by default. The SDK
  /// keeps only the operations that the source implements.
  static constexpr Capabilities All() {
    return Capabilities(SPARK_DS_BATCH_READ | SPARK_DS_MICRO_BATCH_READ | SPARK_DS_BATCH_WRITE |
                        SPARK_DS_STREAMING_WRITE | SPARK_DS_TRUNCATE);
  }

  /// Whether this set contains every operation of `other`.
  constexpr bool Contains(Capabilities other) const { return (bits_ & other.bits_) == other.bits_; }

  /// Removes the operations of `other`, and also kTruncate when no write remains.
  constexpr Capabilities Without(Capabilities other) const {
    return FromBits(bits_ & ~other.bits_);
  }

  /// The operations of both sets, as in `Capabilities::kBatchWrite |
  /// Capabilities::kStreamingWrite`.
  constexpr Capabilities operator|(Capabilities other) const {
    return FromBits(bits_ | other.bits_);
  }

  /// The operations in both sets.
  constexpr Capabilities operator&(Capabilities other) const {
    return FromBits(bits_ & other.bits_);
  }

  constexpr bool operator==(const Capabilities& other) const = default;

  /// The native capability mask, a combination of the SPARK_DS_* flags.
  constexpr uint64_t bits() const { return bits_; }

  /// The names of the operations, such as "{BATCH_READ, MICRO_BATCH_READ}".
  std::string ToString() const {
    static constexpr struct {
      uint64_t bit;
      const char* name;
    } kNames[] = {
        {SPARK_DS_BATCH_READ, "BATCH_READ"},   {SPARK_DS_MICRO_BATCH_READ, "MICRO_BATCH_READ"},
        {SPARK_DS_BATCH_WRITE, "BATCH_WRITE"}, {SPARK_DS_STREAMING_WRITE, "STREAMING_WRITE"},
        {SPARK_DS_TRUNCATE, "TRUNCATE"},
    };
    std::string names;
    for (const auto& entry : kNames) {
      if ((bits_ & entry.bit) != 0) {
        names += names.empty() ? "" : ", ";
        names += entry.name;
      }
    }
    return "{" + names + "}";
  }

 private:
  constexpr explicit Capabilities(uint64_t bits) : bits_(bits) {}

  // Truncation requires a write, so a set never keeps it without one.
  static constexpr Capabilities FromBits(uint64_t bits) {
    constexpr uint64_t kWrites = SPARK_DS_BATCH_WRITE | SPARK_DS_STREAMING_WRITE;
    return Capabilities((bits & kWrites) == 0 ? bits & ~uint64_t{SPARK_DS_TRUNCATE} : bits);
  }

  uint64_t bits_;
};

inline constexpr Capabilities Capabilities::kBatchRead{SPARK_DS_BATCH_READ};
inline constexpr Capabilities Capabilities::kMicroBatchRead{SPARK_DS_MICRO_BATCH_READ};
inline constexpr Capabilities Capabilities::kBatchWrite{SPARK_DS_BATCH_WRITE};
inline constexpr Capabilities Capabilities::kStreamingWrite{SPARK_DS_STREAMING_WRITE};
inline constexpr Capabilities Capabilities::kTruncate{SPARK_DS_TRUNCATE};

}  // namespace spark::native
