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

package org.apache.spark.memory

import org.apache.spark.{SparkEnv, TaskContextImpl, TaskContext}

/**
 * Helper methods for mocking out memory-management-related classes in tests.
 */
object MemoryTestingUtils {
  def fakeTaskContext(env: SparkEnv): TaskContext = {
    val taskMemoryManager = new TaskMemoryManager(env.memoryManager, 0)
    new TaskContextImpl(
      stageId = 0,
      partitionId = 0,
      taskAttemptId = 0,
      attemptNumber = 0,
      taskMemoryManager = taskMemoryManager,
      metricsSystem = env.metricsSystem,
      internalAccumulators = Seq.empty)
  }
}
