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

import java.util.concurrent.atomic.AtomicLong

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}

import org.mockito.Matchers.{any, anyLong}
import org.mockito.Mockito.{mock, when}
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatest.time.SpanSugar._

import org.apache.spark.SparkFunSuite
import org.apache.spark.storage.MemoryStore


/**
 * Helper trait for sharing code among [[MemoryManager]] tests.
 */
private[memory] trait MemoryManagerSuite extends SparkFunSuite {

  import MemoryManagerSuite.DEFAULT_ENSURE_FREE_SPACE_CALLED

  // Note: Mockito's verify mechanism does not provide a way to reset method call counts
  // without also resetting stubbed methods. Since our test code relies on the latter,
  // we need to use our own variable to track invocations of `ensureFreeSpace`.

  /**
   * The amount of free space requested in the last call to [[MemoryStore.ensureFreeSpace]]
   *
   * This set whenever [[MemoryStore.ensureFreeSpace]] is called, and cleared when the test
   * code makes explicit assertions on this variable through [[assertEnsureFreeSpaceCalled]].
   */
  private val ensureFreeSpaceCalled = new AtomicLong(DEFAULT_ENSURE_FREE_SPACE_CALLED)

  /**
   * Make a mocked [[MemoryStore]] whose [[MemoryStore.ensureFreeSpace]] method is stubbed.
   *
   * This allows our test code to release storage memory when [[MemoryStore.ensureFreeSpace]]
   * is called without relying on [[org.apache.spark.storage.BlockManager]] and all of its
   * dependencies.
   */
  protected def makeMemoryStore(mm: MemoryManager): MemoryStore = {
    val ms = mock(classOf[MemoryStore])
    when(ms.ensureFreeSpace(anyLong(), any())).thenAnswer(ensureFreeSpaceAnswer(mm, 0))
    when(ms.ensureFreeSpace(any(), anyLong(), any())).thenAnswer(ensureFreeSpaceAnswer(mm, 1))
    mm.setMemoryStore(ms)
    ms
  }

  /**
   * Make an [[Answer]] that stubs [[MemoryStore.ensureFreeSpace]] with the right arguments.
   */
  private def ensureFreeSpaceAnswer(mm: MemoryManager, numBytesPos: Int): Answer[Boolean] = {
    new Answer[Boolean] {
      override def answer(invocation: InvocationOnMock): Boolean = {
        val args = invocation.getArguments
        require(args.size > numBytesPos, s"bad test: expected >$numBytesPos arguments " +
          s"in ensureFreeSpace, found ${args.size}")
        require(args(numBytesPos).isInstanceOf[Long], s"bad test: expected ensureFreeSpace " +
          s"argument at index $numBytesPos to be a Long: ${args.mkString(", ")}")
        val numBytes = args(numBytesPos).asInstanceOf[Long]
        mockEnsureFreeSpace(mm, numBytes)
      }
    }
  }

  /**
   * Simulate the part of [[MemoryStore.ensureFreeSpace]] that releases storage memory.
   *
   * This is a significant simplification of the real method, which actually drops existing
   * blocks based on the size of each block. Instead, here we simply release as many bytes
   * as needed to ensure the requested amount of free space. This allows us to set up the
   * test without relying on the [[org.apache.spark.storage.BlockManager]], which brings in
   * many other dependencies.
   *
   * Every call to this method will set a global variable, [[ensureFreeSpaceCalled]], that
   * records the number of bytes this is called with. This variable is expected to be cleared
   * by the test code later through [[assertEnsureFreeSpaceCalled]].
   */
  private def mockEnsureFreeSpace(mm: MemoryManager, numBytes: Long): Boolean = mm.synchronized {
    require(ensureFreeSpaceCalled.get() === DEFAULT_ENSURE_FREE_SPACE_CALLED,
      "bad test: ensure free space variable was not reset")
    // Record the number of bytes we freed this call
    ensureFreeSpaceCalled.set(numBytes)
    if (numBytes <= mm.maxStorageMemory) {
      def freeMemory = mm.maxStorageMemory - mm.storageMemoryUsed
      val spaceToRelease = numBytes - freeMemory
      if (spaceToRelease > 0) {
        mm.releaseStorageMemory(spaceToRelease)
      }
      freeMemory >= numBytes
    } else {
      // We attempted to free more bytes than our max allowable memory
      false
    }
  }

  /**
   * Assert that [[MemoryStore.ensureFreeSpace]] is called with the given parameters.
   */
  protected def assertEnsureFreeSpaceCalled(ms: MemoryStore, numBytes: Long): Unit = {
    assert(ensureFreeSpaceCalled.get() === numBytes,
      s"expected ensure free space to be called with $numBytes")
    ensureFreeSpaceCalled.set(DEFAULT_ENSURE_FREE_SPACE_CALLED)
  }

  /**
   * Assert that [[MemoryStore.ensureFreeSpace]] is NOT called.
   */
  protected def assertEnsureFreeSpaceNotCalled[T](ms: MemoryStore): Unit = {
    assert(ensureFreeSpaceCalled.get() === DEFAULT_ENSURE_FREE_SPACE_CALLED,
      "ensure free space should not have been called!")
  }

  /**
   * Create a MemoryManager with the specified execution memory limit and no storage memory.
   */
  protected def createMemoryManager(maxExecutionMemory: Long): MemoryManager

  // -- Tests of sharing of execution memory between tasks ----------------------------------------
  // Prior to Spark 1.6, these tests were part of ShuffleMemoryManagerSuite.

  implicit val ec = ExecutionContext.global

  test("single task requesting execution memory") {
    val manager = createMemoryManager(1000L)
    val taskMemoryManager = new TaskMemoryManager(manager, 0)

    assert(taskMemoryManager.acquireExecutionMemory(100L, null) === 100L)
    assert(taskMemoryManager.acquireExecutionMemory(400L, null) === 400L)
    assert(taskMemoryManager.acquireExecutionMemory(400L, null) === 400L)
    assert(taskMemoryManager.acquireExecutionMemory(200L, null) === 100L)
    assert(taskMemoryManager.acquireExecutionMemory(100L, null) === 0L)
    assert(taskMemoryManager.acquireExecutionMemory(100L, null) === 0L)

    taskMemoryManager.releaseExecutionMemory(500L, null)
    assert(taskMemoryManager.acquireExecutionMemory(300L, null) === 300L)
    assert(taskMemoryManager.acquireExecutionMemory(300L, null) === 200L)

    taskMemoryManager.cleanUpAllAllocatedMemory()
    assert(taskMemoryManager.acquireExecutionMemory(1000L, null) === 1000L)
    assert(taskMemoryManager.acquireExecutionMemory(100L, null) === 0L)
  }

  test("two tasks requesting full execution memory") {
    val memoryManager = createMemoryManager(1000L)
    val t1MemManager = new TaskMemoryManager(memoryManager, 1)
    val t2MemManager = new TaskMemoryManager(memoryManager, 2)
    val futureTimeout: Duration = 20.seconds

    // Have both tasks request 500 bytes, then wait until both requests have been granted:
    val t1Result1 = Future { t1MemManager.acquireExecutionMemory(500L, null) }
    val t2Result1 = Future { t2MemManager.acquireExecutionMemory(500L, null) }
    assert(Await.result(t1Result1, futureTimeout) === 500L)
    assert(Await.result(t2Result1, futureTimeout) === 500L)

    // Have both tasks each request 500 bytes more; both should immediately return 0 as they are
    // both now at 1 / N
    val t1Result2 = Future { t1MemManager.acquireExecutionMemory(500L, null) }
    val t2Result2 = Future { t2MemManager.acquireExecutionMemory(500L, null) }
    assert(Await.result(t1Result2, 200.millis) === 0L)
    assert(Await.result(t2Result2, 200.millis) === 0L)
  }

  test("two tasks cannot grow past 1 / N of execution memory") {
    val memoryManager = createMemoryManager(1000L)
    val t1MemManager = new TaskMemoryManager(memoryManager, 1)
    val t2MemManager = new TaskMemoryManager(memoryManager, 2)
    val futureTimeout: Duration = 20.seconds

    // Have both tasks request 250 bytes, then wait until both requests have been granted:
    val t1Result1 = Future { t1MemManager.acquireExecutionMemory(250L, null) }
    val t2Result1 = Future { t2MemManager.acquireExecutionMemory(250L, null) }
    assert(Await.result(t1Result1, futureTimeout) === 250L)
    assert(Await.result(t2Result1, futureTimeout) === 250L)

    // Have both tasks each request 500 bytes more.
    // We should only grant 250 bytes to each of them on this second request
    val t1Result2 = Future { t1MemManager.acquireExecutionMemory(500L, null) }
    val t2Result2 = Future { t2MemManager.acquireExecutionMemory(500L, null) }
    assert(Await.result(t1Result2, futureTimeout) === 250L)
    assert(Await.result(t2Result2, futureTimeout) === 250L)
  }

  test("tasks can block to get at least 1 / 2N of execution memory") {
    val memoryManager = createMemoryManager(1000L)
    val t1MemManager = new TaskMemoryManager(memoryManager, 1)
    val t2MemManager = new TaskMemoryManager(memoryManager, 2)
    val futureTimeout: Duration = 20.seconds

    // t1 grabs 1000 bytes and then waits until t2 is ready to make a request.
    val t1Result1 = Future { t1MemManager.acquireExecutionMemory(1000L, null) }
    assert(Await.result(t1Result1, futureTimeout) === 1000L)
    val t2Result1 = Future { t2MemManager.acquireExecutionMemory(250L, null) }
    // Make sure that t2 didn't grab the memory right away. This is hacky but it would be difficult
    // to make sure the other thread blocks for some time otherwise.
    Thread.sleep(300)
    t1MemManager.releaseExecutionMemory(250L, null)
    // The memory freed from t1 should now be granted to t2.
    assert(Await.result(t2Result1, futureTimeout) === 250L)
    // Further requests by t2 should be denied immediately because it now has 1 / 2N of the memory.
    val t2Result2 = Future { t2MemManager.acquireExecutionMemory(100L, null) }
    assert(Await.result(t2Result2, 200.millis) === 0L)
  }

  test("TaskMemoryManager.cleanUpAllAllocatedMemory") {
    val memoryManager = createMemoryManager(1000L)
    val t1MemManager = new TaskMemoryManager(memoryManager, 1)
    val t2MemManager = new TaskMemoryManager(memoryManager, 2)
    val futureTimeout: Duration = 20.seconds

    // t1 grabs 1000 bytes and then waits until t2 is ready to make a request.
    val t1Result1 = Future { t1MemManager.acquireExecutionMemory(1000L, null) }
    assert(Await.result(t1Result1, futureTimeout) === 1000L)
    val t2Result1 = Future { t2MemManager.acquireExecutionMemory(500L, null) }
    // Make sure that t2 didn't grab the memory right away. This is hacky but it would be difficult
    // to make sure the other thread blocks for some time otherwise.
    Thread.sleep(300)
    // t1 releases all of its memory, so t2 should be able to grab all of the memory
    t1MemManager.cleanUpAllAllocatedMemory()
    assert(Await.result(t2Result1, futureTimeout) === 500L)
    val t2Result2 = Future { t2MemManager.acquireExecutionMemory(500L, null) }
    assert(Await.result(t2Result2, futureTimeout) === 500L)
    val t2Result3 = Future { t2MemManager.acquireExecutionMemory(500L, null) }
    assert(Await.result(t2Result3, 200.millis) === 0L)
  }

  test("tasks should not be granted a negative amount of execution memory") {
    // This is a regression test for SPARK-4715.
    val memoryManager = createMemoryManager(1000L)
    val t1MemManager = new TaskMemoryManager(memoryManager, 1)
    val t2MemManager = new TaskMemoryManager(memoryManager, 2)
    val futureTimeout: Duration = 20.seconds

    val t1Result1 = Future { t1MemManager.acquireExecutionMemory(700L, null) }
    assert(Await.result(t1Result1, futureTimeout) === 700L)

    val t2Result1 = Future { t2MemManager.acquireExecutionMemory(300L, null) }
    assert(Await.result(t2Result1, futureTimeout) === 300L)

    val t1Result2 = Future { t1MemManager.acquireExecutionMemory(300L, null) }
    assert(Await.result(t1Result2, 200.millis) === 0L)
  }
}

private object MemoryManagerSuite {
  private val DEFAULT_ENSURE_FREE_SPACE_CALLED = -1L
}
