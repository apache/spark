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
#include <thread>

#include "spark/native/data_source.h"
#include "test_util.h"

using spark::native::TaskContext;
using spark::native::internal::ImportTaskContext;
using spark::native::testing::HostTask;

TEST(ReadsIdsAndLiveCancellationFromTheHostContext) {
  HostTask host(5, 7);
  host.context.stage_id = 3;
  host.context.stage_attempt_number = 4;
  host.context.attempt_number = 6;
  ASSIGN_OR_FAIL(TaskContext context, ImportTaskContext(&host.context));
  CHECK_EQ(context.stage_id(), 3);
  CHECK_EQ(context.stage_attempt_number(), 4);
  CHECK_EQ(context.partition_id(), 5);
  CHECK_EQ(context.attempt_number(), 6);
  CHECK_EQ(context.task_attempt_id(), int64_t{7});
  CHECK(!context.IsInterrupted());
  host.cancelled = true;
  bool interrupted = false;
  std::thread([&] { interrupted = context.IsInterrupted(); }).join();
  CHECK(interrupted);
}

TEST(RejectsAContextWithoutEveryFieldOfVersion1) {
  HostTask host(0, 0);
  host.context.size = sizeof(int64_t);
  arrow::Result<TaskContext> context = ImportTaskContext(&host.context);
  CHECK(!context.ok());
  CHECK_CONTAINS(context.status().message(), "version 1");
  CHECK(!ImportTaskContext(nullptr).ok());
}

TEST(TestContextsCarryIdsAndAnOptionalInterruptionFlag) {
  TaskContext context = TaskContext::ForTesting()
                            .WithStageId(1)
                            .WithStageAttemptNumber(2)
                            .WithPartitionId(3)
                            .WithAttemptNumber(4)
                            .WithTaskAttemptId(5);
  CHECK_EQ(context.stage_id(), 1);
  CHECK_EQ(context.stage_attempt_number(), 2);
  CHECK_EQ(context.partition_id(), 3);
  CHECK_EQ(context.attempt_number(), 4);
  CHECK_EQ(context.task_attempt_id(), int64_t{5});
  CHECK(!context.IsInterrupted());
  std::atomic<bool> flag{false};
  TaskContext interruptible = TaskContext::ForTesting().WithPartitionId(8).InterruptedBy(flag);
  CHECK_EQ(interruptible.partition_id(), 8);
  CHECK(!interruptible.IsInterrupted());
  flag = true;
  CHECK(interruptible.IsInterrupted());
}
