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

import java.util.Properties

import org.apache.spark.{SparkEnv, TaskContext, TaskContextImpl}

/**
 * Helper methods for mocking out memory-management-related classes in tests.
 */
object MemoryTestingUtils {
  /** Admit optional bytes after a storage test has registered its owner. */
  def tryAcquireOptionalMemory(
      memoryManager: MemoryManager,
      taskAttemptId: Long,
      numBytes: Long,
      memoryMode: MemoryMode): Long = {
    memoryManager.tryAcquireExecutionMemory(numBytes, taskAttemptId, memoryMode)
  }

  /** Release exactly the optional bytes owned by a storage test's callback. */
  def releaseOptionalMemory(
      memoryManager: MemoryManager,
      taskAttemptId: Long,
      numBytes: Long,
      memoryMode: MemoryMode): Unit = {
    memoryManager.releaseExecutionMemory(numBytes, taskAttemptId, memoryMode)
  }

  /** Exercise storage callers with a real optional reservation, releasing it after the test. */
  def withOptionalMemoryReclaimer(
      memoryManager: MemoryManager,
      taskAttemptId: Long,
      numBytes: Long,
      memoryMode: MemoryMode,
      reclaimer: Runnable)(body: => Unit): Unit = {
    val unregister = memoryManager.registerOptionalMemoryReclaimer(
      taskAttemptId, memoryMode, reclaimer)
    try {
      assert(
        memoryManager.tryAcquireExecutionMemory(numBytes, taskAttemptId, memoryMode) == numBytes)
      body
    } finally {
      memoryManager.releaseAllExecutionMemoryForTask(taskAttemptId)
      unregister.run()
    }
  }

  def fakeTaskContext(env: SparkEnv): TaskContext = {
    val taskMemoryManager = new TaskMemoryManager(env.memoryManager, 0)
    new TaskContextImpl(
      stageId = 0,
      stageAttemptNumber = 0,
      partitionId = 0,
      taskAttemptId = 0,
      attemptNumber = 0,
      numPartitions = 1,
      taskMemoryManager = taskMemoryManager,
      localProperties = new Properties,
      metricsSystem = env.metricsSystem)
  }
}
