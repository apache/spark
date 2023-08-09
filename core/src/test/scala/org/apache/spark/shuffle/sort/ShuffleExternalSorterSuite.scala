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

package org.apache.spark.shuffle.sort

import java.lang.{Long => JLong}

import org.mockito.Mockito.when
import org.scalatestplus.mockito.MockitoSugar

import org.apache.spark._
import org.apache.spark.executor.{ShuffleWriteMetrics, TaskMetrics}
import org.apache.spark.internal.config.MEMORY_FRACTION
import org.apache.spark.internal.config.Tests._
import org.apache.spark.memory._
import org.apache.spark.unsafe.Platform

class ShuffleExternalSorterSuite extends SparkFunSuite with LocalSparkContext with MockitoSugar {

  test("nested spill should be no-op") {
    val conf = new SparkConf()
      .setMaster("local[1]")
      .setAppName("ShuffleExternalSorterSuite")
      .set(IS_TESTING, true)
      .set(TEST_MEMORY, 1600L)
      .set(MEMORY_FRACTION, 0.9999)

    sc = new SparkContext(conf)

    val memoryManager = UnifiedMemoryManager(conf, 1)

    var shouldAllocate = false

    // Mock `TaskMemoryManager` to allocate free memory when `shouldAllocate` is true.
    // This will trigger a nested spill and expose issues if we don't handle this case properly.
    val taskMemoryManager = new TaskMemoryManager(memoryManager, 0) {
      override def acquireExecutionMemory(required: Long, consumer: MemoryConsumer): Long = {
        // ExecutionMemoryPool.acquireMemory will wait until there are 400 bytes for a task to use.
        // So we leave 400 bytes for the task.
        if (shouldAllocate &&
          memoryManager.maxHeapMemory - memoryManager.executionMemoryUsed > 400) {
          val acquireExecutionMemoryMethod =
            memoryManager.getClass.getMethods.filter(_.getName == "acquireExecutionMemory").head
          acquireExecutionMemoryMethod.invoke(
            memoryManager,
            JLong.valueOf(
              memoryManager.maxHeapMemory - memoryManager.executionMemoryUsed - 400),
            JLong.valueOf(1L), // taskAttemptId
            MemoryMode.ON_HEAP
          ).asInstanceOf[java.lang.Long]
        }
        super.acquireExecutionMemory(required, consumer)
      }
    }
    val taskContext = mock[TaskContext]
    val taskMetrics = new TaskMetrics
    when(taskContext.taskMetrics()).thenReturn(taskMetrics)
    val sorter = new ShuffleExternalSorter(
      taskMemoryManager,
      sc.env.blockManager,
      taskContext,
      100, // initialSize - This will require ShuffleInMemorySorter to acquire at least 800 bytes
      1, // numPartitions
      conf,
      new ShuffleWriteMetrics)
    val inMemSorter = {
      val field = sorter.getClass.getDeclaredField("inMemSorter")
      field.setAccessible(true)
      field.get(sorter).asInstanceOf[ShuffleInMemorySorter]
    }
    // Allocate memory to make the next "insertRecord" call triggers a spill.
    val bytes = new Array[Byte](1)
    while (inMemSorter.hasSpaceForAnotherRecord) {
      sorter.insertRecord(bytes, Platform.BYTE_ARRAY_OFFSET, 1, 0)
    }

    // This flag will make the mocked TaskMemoryManager acquire free memory released by spill to
    // trigger a nested spill.
    shouldAllocate = true

    // Should throw `SparkOutOfMemoryError` as there is no enough memory: `ShuffleInMemorySorter`
    // will try to acquire 800 bytes but there are only 400 bytes available.
    //
    // Before the fix, a nested spill may use a released page and this causes two tasks access the
    // same memory page. When a task reads memory written by another task, many types of failures
    // may happen. Here are some examples we have seen:
    //
    // - JVM crash. (This is easy to reproduce in the unit test as we fill newly allocated and
    //   deallocated memory with 0xa5 and 0x5a bytes which usually points to an invalid memory
    //   address)
    // - java.lang.IllegalArgumentException: Comparison method violates its general contract!
    // - java.lang.NullPointerException
    //     at org.apache.spark.memory.TaskMemoryManager.getPage(TaskMemoryManager.java:384)
    // - java.lang.UnsupportedOperationException: Cannot grow BufferHolder by size -536870912
    //     because the size after growing exceeds size limitation 2147483632
    checkError(
      exception = intercept[SparkOutOfMemoryError] {
        sorter.insertRecord(bytes, Platform.BYTE_ARRAY_OFFSET, 1, 0)
      },
      errorClass = "UNABLE_TO_ACQUIRE_MEMORY",
      parameters = Map("requestedBytes" -> "800", "receivedBytes" -> "400"))
  }
}
