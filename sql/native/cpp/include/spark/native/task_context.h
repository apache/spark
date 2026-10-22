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

#pragma once

#include <atomic>
#include <cstdint>
#include <cstring>

#include <arrow/result.h>
#include <arrow/status.h>

#include "spark_datasource.h"

namespace spark::native {

class TaskContext;

namespace internal {
inline arrow::Result<TaskContext> ImportTaskContext(const SparkDsTaskContext* task);
}  // namespace internal

/// The IDs of a task, and its cooperative cancellation.
///
/// A context refers to the task of Spark, which stays valid until Spark releases the stream of
/// the reader or closes the data writer that received the context. A reader or writer may keep the
/// context and check it during long operations, but must not use it afterwards, including in
/// background work. Blocking I/O still needs its own timeouts.
class TaskContext {
 public:
  /// A context for testing a reader or writer outside Spark. Its IDs are 0 until set with the
  /// With* methods, and it is never interrupted unless it is given a flag with InterruptedBy.
  static TaskContext ForTesting() { return TaskContext(); }

  TaskContext WithStageId(int32_t stage_id) const {
    TaskContext context = *this;
    context.stage_id_ = stage_id;
    return context;
  }

  TaskContext WithStageAttemptNumber(int32_t stage_attempt_number) const {
    TaskContext context = *this;
    context.stage_attempt_number_ = stage_attempt_number;
    return context;
  }

  TaskContext WithPartitionId(int32_t partition_id) const {
    TaskContext context = *this;
    context.partition_id_ = partition_id;
    return context;
  }

  TaskContext WithAttemptNumber(int32_t attempt_number) const {
    TaskContext context = *this;
    context.attempt_number_ = attempt_number;
    return context;
  }

  TaskContext WithTaskAttemptId(int64_t task_attempt_id) const {
    TaskContext context = *this;
    context.task_attempt_id_ = task_attempt_id;
    return context;
  }

  /// Reports the task as interrupted whenever `flag` is true. The context refers to the flag,
  /// which must outlive it.
  TaskContext InterruptedBy(const std::atomic<bool>& flag) const {
    TaskContext context = *this;
    context.host_ = nullptr;
    context.is_interrupted_ = nullptr;
    context.flag_ = &flag;
    return context;
  }

  /// The ID of the stage of the task.
  int32_t stage_id() const { return stage_id_; }

  /// The attempt number of the stage of the task.
  int32_t stage_attempt_number() const { return stage_attempt_number_; }

  /// The partition that the task reads or writes.
  int32_t partition_id() const { return partition_id_; }

  /// The attempt number of the task.
  int32_t attempt_number() const { return attempt_number_; }

  /// The ID of the task attempt, unique within the Spark application but not across applications.
  int64_t task_attempt_id() const { return task_attempt_id_; }

  /// Whether Spark killed this task, read from Spark each time. Any thread can call it.
  bool IsInterrupted() const {
    if (is_interrupted_ != nullptr) {
      return is_interrupted_(host_);
    }
    return flag_ != nullptr && flag_->load();
  }

 private:
  friend arrow::Result<TaskContext> internal::ImportTaskContext(const SparkDsTaskContext* task);

  TaskContext() = default;

  int32_t stage_id_ = 0;
  int32_t stage_attempt_number_ = 0;
  int32_t partition_id_ = 0;
  int32_t attempt_number_ = 0;
  int64_t task_attempt_id_ = 0;
  // Where the task learns that Spark killed it: the callback of the host, a flag, or neither.
  const SparkDsTaskContext* host_ = nullptr;
  bool (*is_interrupted_)(const SparkDsTaskContext* task) = nullptr;
  const std::atomic<bool>* flag_ = nullptr;
};

namespace internal {

/// Wraps the context that Spark passes, which has every field of version 1 of the ABI. Fields are
/// only appended to the structure, so every host has them.
inline arrow::Result<TaskContext> ImportTaskContext(const SparkDsTaskContext* task) {
  if (task == nullptr) {
    return arrow::Status::Invalid("null task context");
  }
  int64_t size;
  std::memcpy(&size, task, sizeof(size));
  if (size < static_cast<int64_t>(sizeof(SparkDsTaskContext))) {
    return arrow::Status::Invalid("the task context has ", size,
                                  " bytes, fewer than in version 1 of the native ABI");
  }
  SparkDsTaskContext copy;
  std::memcpy(&copy, task, sizeof(copy));
  TaskContext context;
  context.stage_id_ = copy.stage_id;
  context.stage_attempt_number_ = copy.stage_attempt_number;
  context.partition_id_ = copy.partition_id;
  context.attempt_number_ = copy.attempt_number;
  context.task_attempt_id_ = copy.task_attempt_id;
  if (copy.is_interrupted != nullptr) {
    context.host_ = task;
    context.is_interrupted_ = copy.is_interrupted;
  }
  return context;
}

}  // namespace internal

}  // namespace spark::native
