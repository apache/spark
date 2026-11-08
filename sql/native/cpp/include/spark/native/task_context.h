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

#include <algorithm>
#include <atomic>
#include <cstddef>
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
    context.task_.stage_id = stage_id;
    return context;
  }

  TaskContext WithStageAttemptNumber(int32_t stage_attempt_number) const {
    TaskContext context = *this;
    context.task_.stage_attempt_number = stage_attempt_number;
    return context;
  }

  TaskContext WithPartitionId(int32_t partition_id) const {
    TaskContext context = *this;
    context.task_.partition_id = partition_id;
    return context;
  }

  TaskContext WithAttemptNumber(int32_t attempt_number) const {
    TaskContext context = *this;
    context.task_.attempt_number = attempt_number;
    return context;
  }

  TaskContext WithTaskAttemptId(int64_t task_attempt_id) const {
    TaskContext context = *this;
    context.task_.task_attempt_id = task_attempt_id;
    return context;
  }

  /// Reports the task as interrupted whenever `flag` is true. The context refers to the flag,
  /// which must outlive it.
  TaskContext InterruptedBy(const std::atomic<bool>& flag) const {
    TaskContext context = *this;
    context.host_ = nullptr;
    context.task_.is_interrupted = nullptr;
    context.flag_ = &flag;
    return context;
  }

  /// The ID of the stage of the task.
  int32_t stage_id() const { return task_.stage_id; }

  /// The attempt number of the stage of the task.
  int32_t stage_attempt_number() const { return task_.stage_attempt_number; }

  /// For a reader, the index of its partition in the plan of its scan or micro-batch, which is not
  /// the partition of the task when a task reads several partitions, for example after a coalesce
  /// or a union. For a writer, the partition of the task.
  int32_t partition_id() const { return task_.partition_id; }

  /// The attempt number of the task.
  int32_t attempt_number() const { return task_.attempt_number; }

  /// The ID of the task attempt, unique within the Spark application but not across applications.
  int64_t task_attempt_id() const { return task_.task_attempt_id; }

  /// Whether Spark killed this task, read from Spark each time. For a reader, also whether the task
  /// failed or completed: a native engine may read a stream after its task ended. Any thread can
  /// call it.
  bool IsInterrupted() const {
    if (task_.is_interrupted != nullptr) {
      return task_.is_interrupted(host_);
    }
    return flag_ != nullptr && flag_->load();
  }

 private:
  friend arrow::Result<TaskContext> internal::ImportTaskContext(const SparkDsTaskContext* task);

  TaskContext() = default;

  // A copy of the context of the host, with its IDs and its callback, or zeros for testing.
  SparkDsTaskContext task_{};
  // Where the task learns that Spark killed it: the callback of the host, which gets the context
  // of the host, a flag, or neither.
  const SparkDsTaskContext* host_ = nullptr;
  const std::atomic<bool>* flag_ = nullptr;
};

namespace internal {

/// Wraps the context that Spark passes, which has every field of version 1 of the ABI. Fields are
/// only appended to the structure, so every host has them, and a later version 1 header can have
/// more than an earlier release of Spark passes: those read as zero.
inline arrow::Result<TaskContext> ImportTaskContext(const SparkDsTaskContext* task) {
  constexpr size_t kVersion1Size =
      offsetof(SparkDsTaskContext, is_interrupted) + sizeof(SparkDsTaskContext::is_interrupted);
  if (task == nullptr) {
    return arrow::Status::Invalid("null task context");
  }
  int64_t size;
  std::memcpy(&size, task, sizeof(size));
  if (size < 0 || static_cast<uint64_t>(size) < kVersion1Size) {
    return arrow::Status::Invalid("the task context has ", size,
                                  " bytes, fewer than in version 1 of the native ABI");
  }
  TaskContext context;
  std::memcpy(&context.task_, task, std::min(static_cast<size_t>(size), sizeof(context.task_)));
  context.host_ = task;
  return context;
}

}  // namespace internal

}  // namespace spark::native
