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

import scala.collection.mutable
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}

import org.mockito.Matchers.{any, anyLong}
import org.mockito.Mockito.{mock, when, RETURNS_SMART_NULLS}
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatest.BeforeAndAfterEach
import org.scalatest.time.SpanSugar._

import org.apache.spark.SparkFunSuite
import org.apache.spark.storage.{BlockId, BlockStatus, MemoryStore, StorageLevel}


/**
 * Helper trait for sharing code among [[MemoryManager]] tests.
 */
private[memory] trait MemoryManagerSuite extends SparkFunSuite with BeforeAndAfterEach {

  protected val evictedBlocks = new mutable.ArrayBuffer[(BlockId, BlockStatus)]

  import MemoryManagerSuite.DEFAULT_EVICT_BLOCKS_TO_FREE_SPACE_CALLED

  // Note: Mockito's verify mechanism does not provide a way to reset method call counts
  // without also resetting stubbed methods. Since our test code relies on the latter,
  // we need to use our own variable to track invocations of `evictBlocksToFreeSpace`.

  /**
   * The amount of space requested in the last call to [[MemoryStore.evictBlocksToFreeSpace]].
   *
   * This set whenever [[MemoryStore.evictBlocksToFreeSpace]] is called, and cleared when the test
   * code makes explicit assertions on this variable through
   * [[assertEvictBlocksToFreeSpaceCalled]].
   */
  private val evictBlocksToFreeSpaceCalled = new AtomicLong(0)

  override def beforeEach(): Unit = {
    super.beforeEach()
    evictedBlocks.clear()
    evictBlocksToFreeSpaceCalled.set(DEFAULT_EVICT_BLOCKS_TO_FREE_SPACE_CALLED)
  }

  /**
   * Make a mocked [[MemoryStore]] whose [[MemoryStore.evictBlocksToFreeSpace]] method is stubbed.
   *
   * This allows our test code to release storage memory when these methods are called
   * without relying on [[org.apache.spark.storage.BlockManager]] and all of its dependencies.
   */
  protected def makeMemoryStore(mm: MemoryManager): MemoryStore = {
    val ms = mock(classOf[MemoryStore], RETURNS_SMART_NULLS)
    when(ms.evictBlocksToFreeSpace(any(), anyLong(), any()))
      .thenAnswer(evictBlocksToFreeSpaceAnswer(mm))
    mm.setMemoryStore(ms)
    ms
  }

  /**
    * Simulate the part of [[MemoryStore.evictBlocksToFreeSpace]] that releases storage memory.
    *
    * This is a significant simplification of the real method, which actually drops existing
    * blocks based on the size of each block. Instead, here we simply release as many bytes
    * as needed to ensure the requested amount of free space. This allows us to set up the
    * test without relying on the [[org.apache.spark.storage.BlockManager]], which brings in
    * many other dependencies.
    *
    * Every call to this method will set a global variable, [[evictBlocksToFreeSpaceCalled]], that
    * records the number of bytes this is called with. This variable is expected to be cleared
    * by the test code later through [[assertEvictBlocksToFreeSpaceCalled]].
    */
  private def evictBlocksToFreeSpaceAnswer(mm: MemoryManager): Answer[Boolean] = {
    new Answer[Boolean] {
      override def answer(invocation: InvocationOnMock): Boolean = {
        val args = invocation.getArguments
        val numBytesToFree = args(1).asInstanceOf[Long]
        assert(numBytesToFree > 0)
        require(evictBlocksToFreeSpaceCalled.get() === DEFAULT_EVICT_BLOCKS_TO_FREE_SPACE_CALLED,
          "bad test: evictBlocksToFreeSpace() variable was not reset")
        evictBlocksToFreeSpaceCalled.set(numBytesToFree)
        if (numBytesToFree <= mm.storageMemoryUsed) {
          // We can evict enough blocks to fulfill the request for space
          mm.releaseStorageMemory(numBytesToFree)
          args.last.asInstanceOf[mutable.Buffer[(BlockId, BlockStatus)]].append(
            (null, BlockStatus(StorageLevel.MEMORY_ONLY, numBytesToFree, 0L, 0L)))
          // We need to add this call so that that the suite-level `evictedBlocks` is updated when
          // execution evicts storage; in that case, args.last will not be equal to evictedBlocks
          // because it will be a temporary buffer created inside of the MemoryManager rather than
          // being passed in by the test code.
          if (!(evictedBlocks eq args.last)) {
            evictedBlocks.append(
              (null, BlockStatus(StorageLevel.MEMORY_ONLY, numBytesToFree, 0L, 0L)))
          }
          true
        } else {
          // No blocks were evicted because eviction would not free enough space.
          false
        }
      }
    }
  }

  /**
   * Assert that [[MemoryStore.evictBlocksToFreeSpace]] is called with the given parameters.
   */
  protected def assertEvictBlocksToFreeSpaceCalled(ms: MemoryStore, numBytes: Long): Unit = {
    assert(evictBlocksToFreeSpaceCalled.get() === numBytes,
      s"expected evictBlocksToFreeSpace() to be called with $numBytes")
    evictBlocksToFreeSpaceCalled.set(DEFAULT_EVICT_BLOCKS_TO_FREE_SPACE_CALLED)
  }

  /**
   * Assert that [[MemoryStore.evictBlocksToFreeSpace]] is NOT called.
   */
  protected def assertEvictBlocksToFreeSpaceNotCalled[T](ms: MemoryStore): Unit = {
    assert(evictBlocksToFreeSpaceCalled.get() === DEFAULT_EVICT_BLOCKS_TO_FREE_SPACE_CALLED,
      "evictBlocksToFreeSpace() should not have been called!")
    assert(evictedBlocks.isEmpty)
  }

  /**
   * Create a MemoryManager with the specified execution memory limits and no storage memory.
   */
  protected def createMemoryManager(
     maxOnHeapExecutionMemory: Long,
     maxOffHeapExecutionMemory: Long = 0L): MemoryManager

  // -- Tests of sharing of execution memory between tasks ----------------------------------------
  // Prior to Spark 1.6, these tests were part of ShuffleMemoryManagerSuite.

  implicit val ec = ExecutionContext.global

  test("single task requesting on-heap execution memory") {
    val manager = createMemoryManager(1000L)
    val taskMemoryManager = new TaskMemoryManager(manager, 0)

    assert(taskMemoryManager.acquireExecutionMemory(100L, MemoryMode.ON_HEAP, null) === 100L)
    assert(taskMemoryManager.acquireExecutionMemory(400L, MemoryMode.ON_HEAP, null) === 400L)
    assert(taskMemoryManager.acquireExecutionMemory(400L, MemoryMode.ON_HEAP, null) === 400L)
    assert(taskMemoryManager.acquireExecutionMemory(200L, MemoryMode.ON_HEAP, null) === 100L)
    assert(taskMemoryManager.acquireExecutionMemory(100L, MemoryMode.ON_HEAP, null) === 0L)
    assert(taskMemoryManager.acquireExecutionMemory(100L, MemoryMode.ON_HEAP, null) === 0L)

    taskMemoryManager.releaseExecutionMemory(500L, MemoryMode.ON_HEAP, null)
    assert(taskMemoryManager.acquireExecutionMemory(300L, MemoryMode.ON_HEAP, null) === 300L)
    assert(taskMemoryManager.acquireExecutionMemory(300L, MemoryMode.ON_HEAP, null) === 200L)

    taskMemoryManager.cleanUpAllAllocatedMemory()
    assert(taskMemoryManager.acquireExecutionMemory(1000L, MemoryMode.ON_HEAP, null) === 1000L)
    assert(taskMemoryManager.acquireExecutionMemory(100L, MemoryMode.ON_HEAP, null) === 0L)
  }

  test("two tasks requesting full on-heap execution memory") {
    val memoryManager = createMemoryManager(1000L)
    val t1MemManager = new TaskMemoryManager(memoryManager, 1)
    val t2MemManager = new TaskMemoryManager(memoryManager, 2)
    val futureTimeout: Duration = 20.seconds

    // Have both tasks request 500 bytes, then wait until both requests have been granted:
    val t1Result1 = Future { t1MemManager.acquireExecutionMemory(500L, MemoryMode.ON_HEAP, null) }
    val t2Result1 = Future { t2MemManager.acquireExecutionMemory(500L, MemoryMode.ON_HEAP, null) }
    assert(Await.result(t1Result1, futureTimeout) === 500L)
    assert(Await.result(t2Result1, futureTimeout) === 500L)

    // Have both tasks each request 500 bytes more; both should immediately return 0 as they are
    // both now at 1 / N
    val t1Result2 = Future { t1MemManager.acquireExecutionMemory(500L, MemoryMode.ON_HEAP, null) }
    val t2Result2 = Future { t2MemManager.acquireExecutionMemory(500L, MemoryMode.ON_HEAP, null) }
    assert(Await.result(t1Result2, 200.millis) === 0L)
    assert(Await.result(t2Result2, 200.millis) === 0L)
  }

  test("two tasks cannot grow past 1 / N of on-heap execution memory") {
    val memoryManager = createMemoryManager(1000L)
    val t1MemManager = new TaskMemoryManager(memoryManager, 1)
    val t2MemManager = new TaskMemoryManager(memoryManager, 2)
    val futureTimeout: Duration = 20.seconds

    // Have both tasks request 250 bytes, then wait until both requests have been granted:
    val t1Result1 = Future { t1MemManager.acquireExecutionMemory(250L, MemoryMode.ON_HEAP, null) }
    val t2Result1 = Future { t2MemManager.acquireExecutionMemory(250L, MemoryMode.ON_HEAP, null) }
    assert(Await.result(t1Result1, futureTimeout) === 250L)
    assert(Await.result(t2Result1, futureTimeout) === 250L)

    // Have both tasks each request 500 bytes more.
    // We should only grant 250 bytes to each of them on this second request
    val t1Result2 = Future { t1MemManager.acquireExecutionMemory(500L, MemoryMode.ON_HEAP, null) }
    val t2Result2 = Future { t2MemManager.acquireExecutionMemory(500L, MemoryMode.ON_HEAP, null) }
    assert(Await.result(t1Result2, futureTimeout) === 250L)
    assert(Await.result(t2Result2, futureTimeout) === 250L)
  }

  test("tasks can block to get at least 1 / 2N of on-heap execution memory") {
    val memoryManager = createMemoryManager(1000L)
    val t1MemManager = new TaskMemoryManager(memoryManager, 1)
    val t2MemManager = new TaskMemoryManager(memoryManager, 2)
    val futureTimeout: Duration = 20.seconds

    // t1 grabs 1000 bytes and then waits until t2 is ready to make a request.
    val t1Result1 = Future { t1MemManager.acquireExecutionMemory(1000L, MemoryMode.ON_HEAP, null) }
    assert(Await.result(t1Result1, futureTimeout) === 1000L)
    val t2Result1 = Future { t2MemManager.acquireExecutionMemory(250L, MemoryMode.ON_HEAP, null) }
    // Make sure that t2 didn't grab the memory right away. This is hacky but it would be difficult
    // to make sure the other thread blocks for some time otherwise.
    Thread.sleep(300)
    t1MemManager.releaseExecutionMemory(250L, MemoryMode.ON_HEAP, null)
    // The memory freed from t1 should now be granted to t2.
    assert(Await.result(t2Result1, futureTimeout) === 250L)
    // Further requests by t2 should be denied immediately because it now has 1 / 2N of the memory.
    val t2Result2 = Future { t2MemManager.acquireExecutionMemory(100L, MemoryMode.ON_HEAP, null) }
    assert(Await.result(t2Result2, 200.millis) === 0L)
  }

  test("TaskMemoryManager.cleanUpAllAllocatedMemory") {
    val memoryManager = createMemoryManager(1000L)
    val t1MemManager = new TaskMemoryManager(memoryManager, 1)
    val t2MemManager = new TaskMemoryManager(memoryManager, 2)
    val futureTimeout: Duration = 20.seconds

    // t1 grabs 1000 bytes and then waits until t2 is ready to make a request.
    val t1Result1 = Future { t1MemManager.acquireExecutionMemory(1000L, MemoryMode.ON_HEAP, null) }
    assert(Await.result(t1Result1, futureTimeout) === 1000L)
    val t2Result1 = Future { t2MemManager.acquireExecutionMemory(500L, MemoryMode.ON_HEAP, null) }
    // Make sure that t2 didn't grab the memory right away. This is hacky but it would be difficult
    // to make sure the other thread blocks for some time otherwise.
    Thread.sleep(300)
    // t1 releases all of its memory, so t2 should be able to grab all of the memory
    t1MemManager.cleanUpAllAllocatedMemory()
    assert(Await.result(t2Result1, futureTimeout) === 500L)
    val t2Result2 = Future { t2MemManager.acquireExecutionMemory(500L, MemoryMode.ON_HEAP, null) }
    assert(Await.result(t2Result2, futureTimeout) === 500L)
    val t2Result3 = Future { t2MemManager.acquireExecutionMemory(500L, MemoryMode.ON_HEAP, null) }
    assert(Await.result(t2Result3, 200.millis) === 0L)
  }

  test("tasks should not be granted a negative amount of execution memory") {
    // This is a regression test for SPARK-4715.
    val memoryManager = createMemoryManager(1000L)
    val t1MemManager = new TaskMemoryManager(memoryManager, 1)
    val t2MemManager = new TaskMemoryManager(memoryManager, 2)
    val futureTimeout: Duration = 20.seconds

    val t1Result1 = Future { t1MemManager.acquireExecutionMemory(700L, MemoryMode.ON_HEAP, null) }
    assert(Await.result(t1Result1, futureTimeout) === 700L)

    val t2Result1 = Future { t2MemManager.acquireExecutionMemory(300L, MemoryMode.ON_HEAP, null) }
    assert(Await.result(t2Result1, futureTimeout) === 300L)

    val t1Result2 = Future { t1MemManager.acquireExecutionMemory(300L, MemoryMode.ON_HEAP, null) }
    assert(Await.result(t1Result2, 200.millis) === 0L)
  }

  test("off-heap execution allocations cannot exceed limit") {
    val memoryManager = createMemoryManager(
      maxOnHeapExecutionMemory = 0L,
      maxOffHeapExecutionMemory = 1000L)

    val tMemManager = new TaskMemoryManager(memoryManager, 1)
    val result1 = Future { tMemManager.acquireExecutionMemory(1000L, MemoryMode.OFF_HEAP, null) }
    assert(Await.result(result1, 200.millis) === 1000L)
    assert(tMemManager.getMemoryConsumptionForThisTask === 1000L)

    val result2 = Future { tMemManager.acquireExecutionMemory(300L, MemoryMode.OFF_HEAP, null) }
    assert(Await.result(result2, 200.millis) === 0L)

    assert(tMemManager.getMemoryConsumptionForThisTask === 1000L)
    tMemManager.releaseExecutionMemory(500L, MemoryMode.OFF_HEAP, null)
    assert(tMemManager.getMemoryConsumptionForThisTask === 500L)
    tMemManager.releaseExecutionMemory(500L, MemoryMode.OFF_HEAP, null)
    assert(tMemManager.getMemoryConsumptionForThisTask === 0L)
  }
}

private object MemoryManagerSuite {
  private val DEFAULT_EVICT_BLOCKS_TO_FREE_SPACE_CALLED = -1L
}
