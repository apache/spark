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

import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}

import scala.concurrent.{blocking, Future}
import scala.concurrent.duration._

import org.mockito.ArgumentMatchers.{any, anyLong}
import org.mockito.Mockito.doAnswer
import org.mockito.invocation.InvocationOnMock
import org.scalatest.PrivateMethodTester

import org.apache.spark.{SparkConf, SparkIllegalArgumentException}
import org.apache.spark.internal.config._
import org.apache.spark.internal.config.Tests._
import org.apache.spark.storage.{BlockId, TestBlockId}
import org.apache.spark.storage.memory.MemoryStore
import org.apache.spark.util.{ThreadUtils, Utils}

class UnifiedMemoryManagerSuite extends MemoryManagerSuite with PrivateMethodTester {
  private val dummyBlock = TestBlockId("--")

  private val storageFraction: Double = 0.5

  /**
   * Make a [[UnifiedMemoryManager]] and a [[MemoryStore]] with limited class dependencies.
   */
  private def makeThings(maxMemory: Long): (UnifiedMemoryManager, MemoryStore) = {
    val mm = createMemoryManager(maxMemory)
    val ms = makeMemoryStore(mm)
    (mm, ms)
  }

  override protected def createMemoryManager(
      maxOnHeapExecutionMemory: Long,
      maxOffHeapExecutionMemory: Long): UnifiedMemoryManager = {
    val conf = new SparkConf()
      .set(MEMORY_FRACTION, 1.0)
      .set(TEST_MEMORY, maxOnHeapExecutionMemory)
      .set(MEMORY_OFFHEAP_SIZE, maxOffHeapExecutionMemory)
      .set(MEMORY_STORAGE_FRACTION, storageFraction)
    UnifiedMemoryManager(conf, numCores = 1)
  }

  test("basic execution memory") {
    val maxMemory = 1000L
    val taskAttemptId = 0L
    val (mm, _) = makeThings(maxMemory)
    val memoryMode = MemoryMode.ON_HEAP
    assert(mm.executionMemoryUsed === 0L)
    assert(mm.acquireExecutionMemory(10L, taskAttemptId, memoryMode) === 10L)
    assert(mm.executionMemoryUsed === 10L)
    assert(mm.acquireExecutionMemory(100L, taskAttemptId, memoryMode) === 100L)
    // Acquire up to the max
    assert(mm.acquireExecutionMemory(1000L, taskAttemptId, memoryMode) === 890L)
    assert(mm.executionMemoryUsed === maxMemory)
    assert(mm.acquireExecutionMemory(1L, taskAttemptId, memoryMode) === 0L)
    assert(mm.executionMemoryUsed === maxMemory)
    mm.releaseExecutionMemory(800L, taskAttemptId, memoryMode)
    assert(mm.executionMemoryUsed === 200L)
    // Acquire after release
    assert(mm.acquireExecutionMemory(1L, taskAttemptId, memoryMode) === 1L)
    assert(mm.executionMemoryUsed === 201L)
    // Release beyond what was acquired
    mm.releaseExecutionMemory(maxMemory, taskAttemptId, memoryMode)
    assert(mm.executionMemoryUsed === 0L)
  }

  test("optional admission does not borrow free storage or partially reserve") {
    val (mm, ms) = makeThings(1000L)
    val mode = MemoryMode.ON_HEAP
    assert(mm.tryAcquireExecutionMemory(501L, 1L, mode) === 0L)
    assert(mm.executionMemoryUsed === 0L)
    assert(mm.tryAcquireExecutionMemory(500L, 1L, mode) === 500L)
    assert(mm.tryAcquireExecutionMemory(1L, 1L, mode) === 0L)
    assert(mm.storageMemoryUsed === 0L)
    assertEvictBlocksToFreeSpaceNotCalled(ms)
    assert(mm.releaseAllExecutionMemoryForTask(1L) === 500L)
    assert(mm.tryAcquireExecutionMemory(501L, 1L, mode) === 0L)
  }

  test("direct storage pressure reclaims only matching optional owners before eviction") {
    val (mm, _) = makeThings(1000L)
    val mode = MemoryMode.ON_HEAP
    var held = 0L
    var callbacks = 0
    val unregister = mm.registerOptionalMemoryReclaimer(1L, mode, () => {
      assert(!Thread.holdsLock(mm))
      callbacks += 1
      mm.releaseExecutionMemory(held, 1L, mode)
      held = 0L
    })
    held = mm.tryAcquireExecutionMemory(100L, 1L, mode)
    assert(held === 100L)
    assert(mm.acquireStorageMemory(dummyBlock, 800L, mode))
    assert(callbacks === 0)
    assert(held === 100L)
    mm.releaseStorageMemory(800L, mode)
    assert(mm.acquireStorageMemory(dummyBlock, 950L, mode))
    assert(callbacks === 1)
    assert(held === 0L)
    assert(mm.executionMemoryUsed === 0L)
    mm.releaseStorageMemory(950L, mode)
    unregister.run()
  }

  test("outer storage marker drains before inherited monitor and excludes new optional admission") {
    val (mm, _) = makeThings(1000L)
    val mode = MemoryMode.ON_HEAP
    var held = 0L
    var callbacks = 0
    val unregister = mm.registerOptionalMemoryReclaimer(1L, mode, () => {
      assert(!Thread.holdsLock(mm))
      callbacks += 1
      assert(mm.tryAcquireExecutionMemory(1L, 3L, mode) === 0L)
      mm.releaseExecutionMemory(held, 1L, mode)
      held = 0L
    })
    held = mm.tryAcquireExecutionMemory(100L, 1L, mode)
    mm.withMemoryReclamation {
      mm.synchronized {
        assert(callbacks === 1)
        assert(mm.acquireStorageMemory(dummyBlock, 950L, mode))
        assert(callbacks === 1)
      }
    }
    assert(mm.executionMemoryUsed === 0L)
    mm.releaseStorageMemory(950L, mode)
    unregister.run()
  }

  test("ordinary reclamation drains other owners before propagating callback failure") {
    val (mm, _) = makeThings(1000L)
    val mode = MemoryMode.ON_HEAP
    var held = 0L
    val failed = mm.registerOptionalMemoryReclaimer(1L, mode, () => {
      throw new IllegalStateException("injected release failure")
    })
    val released = mm.registerOptionalMemoryReclaimer(2L, mode, () => {
      assert(!Thread.holdsLock(mm))
      mm.releaseExecutionMemory(held, 2L, mode)
      held = 0L
    })
    assert(mm.tryAcquireExecutionMemory(100L, 1L, mode) === 100L)
    held = mm.tryAcquireExecutionMemory(100L, 2L, mode)
    assert(held === 100L)
    intercept[IllegalStateException] {
      mm.acquireExecutionMemory(700L, 3L, mode)
    }
    assert(held === 0L)
    assert(mm.getExecutionMemoryUsageForTask(3L) === 0L)
    assert(mm.executionMemoryUsed === 100L)
    mm.releaseExecutionMemory(100L, 1L, mode)
    failed.run()
    released.run()
    assert(mm.acquireExecutionMemory(700L, 3L, mode) === 700L)
    mm.releaseAllExecutionMemoryForTask(3L)
  }

  test("concurrent demand reclaims shared optional ownership exactly once") {
    for (mode <- Seq(MemoryMode.ON_HEAP, MemoryMode.OFF_HEAP)) {
      val mm = createMemoryManager(1000L, 1000L)
      val ownerLock = new Object
      val bothReclaiming = new CountDownLatch(2)
      val calls = new AtomicInteger()
      var held = 0L
      var released = 0L
      val unregister = mm.registerOptionalMemoryReclaimer(1L, mode, () => {
        assert(!Thread.holdsLock(mm))
        calls.incrementAndGet()
        // Force overlapping callbacks before either releases the optional reservation.
        // This scheduling barrier is outside the owner's short release-only critical section.
        bothReclaiming.countDown()
        assert(blocking { bothReclaiming.await(5, TimeUnit.SECONDS) })
        ownerLock.synchronized {
          if (held != 0L) {
            mm.releaseExecutionMemory(held, 1L, mode)
            released += held
            held = 0L
          }
        }
      })
      ownerLock.synchronized {
        held = mm.tryAcquireExecutionMemory(400L, 1L, mode)
        assert(held === 400L)
      }
      // Ordinary allocation must not retain the owner lock: it can reclaim another task.
      val demands = Seq(2L, 3L).map { taskId =>
        Future {
          try {
            val granted = mm.acquireExecutionMemory(700L, taskId, mode)
            assert(granted > 0L && granted <= 700L)
          } finally {
            mm.releaseAllExecutionMemoryForTask(taskId)
          }
        }
      }
      try {
        demands.foreach(ThreadUtils.awaitResult(_, 10.seconds))
        // A repeated invocation must also be harmless after both demand requests have finished.
        mm.withMemoryReclamation { () }
        assert(calls.get() === 3)
        assert(held === 0L)
        assert(released === 400L)
        assert(mm.executionMemoryUsed === 0L)
      } finally {
        mm.releaseAllExecutionMemoryForTask(1L)
        unregister.run()
      }
    }
  }

  test("storage cleanup tolerates drain failure but preserves body failures and memory charges") {
    val (mm, _) = makeThings(1000L)
    val mode = MemoryMode.ON_HEAP
    val drainFailure = new IllegalStateException("injected drain failure")
    val bodyFailure = new IllegalArgumentException("injected cleanup failure")
    val unregister = mm.registerOptionalMemoryReclaimer(1L, mode, () => throw drainFailure)
    assert(mm.tryAcquireExecutionMemory(100L, 1L, mode) === 100L)
    try {
      // Ordinary operations still stop before their body if reclamation fails.
      assert(intercept[IllegalStateException] {
        mm.withMemoryReclamation { fail("body must not run") }
      } eq drainFailure)
      assert(intercept[IllegalArgumentException] {
        mm.withMemoryReclamation({
          assert(mm.tryAcquireExecutionMemory(1L, 2L, mode) === 0L)
          throw bodyFailure
        }, releaseOnly = true)
      } eq bodyFailure)
      assert(mm.executionMemoryUsed === 100L)
      // The gate is released even when the cleanup body throws.
      assert(mm.tryAcquireExecutionMemory(1L, 2L, mode) === 1L)
    } finally {
      mm.releaseAllExecutionMemoryForTask(1L)
      mm.releaseAllExecutionMemoryForTask(2L)
      unregister.run()
    }
  }

  test("ordinary pressure reclaims only the requested memory mode") {
    val mm = createMemoryManager(1000L, 1000L)
    makeMemoryStore(mm)
    var onHeapHeld = 0L
    var offHeapHeld = 0L
    val onHeap = mm.registerOptionalMemoryReclaimer(1L, MemoryMode.ON_HEAP, () => {
      mm.releaseExecutionMemory(onHeapHeld, 1L, MemoryMode.ON_HEAP)
      onHeapHeld = 0L
    })
    val offHeap = mm.registerOptionalMemoryReclaimer(1L, MemoryMode.OFF_HEAP, () => {
      mm.releaseExecutionMemory(offHeapHeld, 1L, MemoryMode.OFF_HEAP)
      offHeapHeld = 0L
    })
    onHeapHeld = mm.tryAcquireExecutionMemory(100L, 1L, MemoryMode.ON_HEAP)
    offHeapHeld = mm.tryAcquireExecutionMemory(100L, 1L, MemoryMode.OFF_HEAP)
    assert(mm.acquireExecutionMemory(700L, 2L, MemoryMode.OFF_HEAP) === 700L)
    assert(offHeapHeld === 0L)
    assert(onHeapHeld === 100L)
    mm.releaseExecutionMemory(onHeapHeld, 1L, MemoryMode.ON_HEAP)
    mm.releaseExecutionMemory(700L, 2L, MemoryMode.OFF_HEAP)
    onHeap.run()
    offHeap.run()
    assert(mm.executionMemoryUsed === 0L)
  }

  test("optional admission does not evict storage borrowed from execution") {
    val (mm, ms) = makeThings(1000L)
    val mode = MemoryMode.ON_HEAP
    assert(mm.acquireStorageMemory(dummyBlock, 800L, mode))
    assertEvictBlocksToFreeSpaceNotCalled(ms)
    assert(mm.tryAcquireExecutionMemory(201L, 1L, mode) === 0L)
    assert(mm.storageMemoryUsed === 800L)
    assert(mm.executionMemoryUsed === 0L)
    assertEvictBlocksToFreeSpaceNotCalled(ms)
    assert(mm.tryAcquireExecutionMemory(200L, 1L, mode) === 200L)
    assert(mm.releaseAllExecutionMemoryForTask(1L) === 200L)
  }

  test("optional admission preserves fair shares and task release in both memory modes") {
    for (mode <- Seq(MemoryMode.ON_HEAP, MemoryMode.OFF_HEAP)) {
      val mm = createMemoryManager(1000L, 1000L)
      // Ordinary admission borrows storage, making the execution pool 1000 bytes.
      assert(mm.acquireExecutionMemory(1000L, 1L, mode) === 1000L)
      mm.releaseExecutionMemory(1000L, 1L, mode)
      assert(mm.tryAcquireExecutionMemory(250L, 1L, mode) === 250L)
      assert(mm.tryAcquireExecutionMemory(501L, 2L, mode) === 0L)
      assert(mm.tryAcquireExecutionMemory(500L, 2L, mode) === 500L)
      assert(mm.tryAcquireExecutionMemory(251L, 1L, mode) === 0L)
      assert(mm.tryAcquireExecutionMemory(250L, 1L, mode) === 250L)
      assert(mm.executionMemoryUsed === 1000L)
      assert(mm.releaseAllExecutionMemoryForTask(2L) === 500L)
      assert(mm.tryAcquireExecutionMemory(500L, 1L, mode) === 500L)
      assert(mm.releaseAllExecutionMemoryForTask(1L) === 1000L)
      assert(mm.executionMemoryUsed === 0L)
    }
  }

  test("zero and denied optional requests do not create phantom fair-share participants") {
    val mm = createMemoryManager(1000L)
    val mode = MemoryMode.ON_HEAP
    assert(mm.acquireExecutionMemory(1000L, 1L, mode) === 1000L)
    assert(mm.tryAcquireExecutionMemory(0L, 2L, mode) === 0L)
    assert(mm.tryAcquireExecutionMemory(1L, 2L, mode) === 0L)
    intercept[IllegalArgumentException] {
      mm.tryAcquireExecutionMemory(-1L, 3L, mode)
    }
    mm.releaseExecutionMemory(100L, 1L, mode)
    assert(mm.tryAcquireExecutionMemory(100L, 1L, mode) === 100L)
    assert(mm.releaseAllExecutionMemoryForTask(1L) === 1000L)
  }

  test("optional admission refuses stalled eviction before and after first registration") {
    for (storageRequest <- Seq(false, true)) {
      val (mm, ms) = makeThings(1000L)
      val mode = MemoryMode.ON_HEAP
      assert(mm.acquireStorageMemory(dummyBlock, 800L, mode))
      val evicting = new CountDownLatch(1)
      val finishEviction = new CountDownLatch(1)
      doAnswer(
        (invocation: InvocationOnMock) => {
          evicting.countDown()
          assert(finishEviction.await(10, TimeUnit.SECONDS))
          val bytes = invocation.getArguments()(1).asInstanceOf[Long]
          mm.releaseStorageMemory(bytes, mode)
          bytes
        }).when(ms).evictBlocksToFreeSpace(any(), anyLong(), any())
      // No owner exists when this request takes the fast path and starts eviction.
      val ordinary = Future {
        if (storageRequest) {
          assert(mm.acquireStorageMemory(TestBlockId("replacement"), 900L, mode))
        } else {
          assert(mm.acquireExecutionMemory(300L, 1L, mode) === 300L)
        }
      }
      var unregister: Option[Runnable] = None
      try {
        assert(evicting.await(5, TimeUnit.SECONDS))
        val beforeRegistration = Future { mm.tryAcquireExecutionMemory(1L, 2L, mode) }
        assert(ThreadUtils.awaitResult(beforeRegistration, 5.seconds) === 0L)
        unregister = Some(mm.registerOptionalMemoryReclaimer(2L, mode, () => {
          fail("the first empty owner must not be reclaimed by an in-progress fast path")
        }))
        val afterRegistration = Future { mm.tryAcquireExecutionMemory(100L, 2L, mode) }
        assert(ThreadUtils.awaitResult(afterRegistration, 5.seconds) === 0L)
        finishEviction.countDown()
        ThreadUtils.awaitResult(ordinary, 5.seconds)
        if (storageRequest) mm.releaseStorageMemory(900L, mode)
        else mm.releaseExecutionMemory(300L, 1L, mode)
        // Storage may own the whole pool; only ordinary admission can borrow free storage back.
        assert(mm.acquireExecutionMemory(100L, 2L, mode) === 100L)
        mm.releaseExecutionMemory(100L, 2L, mode)
        assert(mm.tryAcquireExecutionMemory(100L, 2L, mode) === 100L)
        assert(mm.releaseAllExecutionMemoryForTask(2L) === 100L)
      } finally {
        finishEviction.countDown()
        ThreadUtils.awaitResult(ordinary, 5.seconds)
        unregister.foreach(_.run())
        mm.releaseAllExecutionMemoryForTask(1L)
        mm.releaseAllExecutionMemoryForTask(2L)
        mm.releaseAllStorageMemory()
      }
    }
  }

  test("optional admission marker does not serialize ordinary operations") {
    val mm = createMemoryManager(1000L)
    val marked = new CountDownLatch(1)
    val finish = new CountDownLatch(1)
    val markedOperation = Future {
      mm.withMemoryReclamation {
        marked.countDown()
        assert(finish.await(10, TimeUnit.SECONDS))
      }
    }
    try {
      assert(marked.await(5, TimeUnit.SECONDS))
      assert(mm.tryAcquireExecutionMemory(1L, 1L, MemoryMode.ON_HEAP) === 0L)
      val ordinary = Future { mm.acquireExecutionMemory(1L, 1L, MemoryMode.ON_HEAP) }
      assert(ThreadUtils.awaitResult(ordinary, 5.seconds) === 1L)
      assert(mm.releaseAllExecutionMemoryForTask(1L) === 1L)
    } finally {
      finish.countDown()
      ThreadUtils.awaitResult(markedOperation, 5.seconds)
    }
    assert(mm.tryAcquireExecutionMemory(1L, 1L, MemoryMode.ON_HEAP) === 1L)
    assert(mm.releaseAllExecutionMemoryForTask(1L) === 1L)
  }

  test("direct unroll admission acquires the reclamation marker before the monitor") {
    val enteredStorage = new CountDownLatch(1)
    val continueStorage = new CountDownLatch(1)
    val abortStorage = new AtomicBoolean(false)
    val mode = MemoryMode.ON_HEAP
    val mm = new UnifiedMemoryManager(new SparkConf(), 1000L, 500L, 1) {
      /**
       * Pause before ordinary storage admission so optional admission can take its gate first.
       * On test failure, return without entering that gate to let both futures finish safely.
       */
      override def acquireStorageMemory(
          blockId: BlockId,
          numBytes: Long,
          memoryMode: MemoryMode): Boolean = {
        enteredStorage.countDown()
        assert(continueStorage.await(10, TimeUnit.SECONDS))
        if (abortStorage.get()) {
          false
        } else {
          super.acquireStorageMemory(blockId, numBytes, memoryMode)
        }
      }
    }
    val unroll = Future { mm.acquireUnrollMemory(dummyBlock, 1L, mode) }
    val optional = Future {
      assert(enteredStorage.await(5, TimeUnit.SECONDS))
      mm.tryAcquireExecutionMemory(1L, 1L, mode)
    }
    try {
      assert(ThreadUtils.awaitResult(optional, 5.seconds) === 1L)
      continueStorage.countDown()
      assert(ThreadUtils.awaitResult(unroll, 5.seconds))
      assert(mm.storageMemoryUsed === 1L)
    } finally {
      abortStorage.set(true)
      continueStorage.countDown()
      ThreadUtils.awaitResult(unroll, 5.seconds)
      ThreadUtils.awaitResult(optional, 5.seconds)
      mm.releaseAllExecutionMemoryForTask(1L)
      mm.releaseAllStorageMemory()
    }
    assert(mm.executionMemoryUsed === 0L)
    assert(mm.storageMemoryUsed === 0L)
  }

  test("basic storage memory") {
    val maxMemory = 1000L
    val (mm, ms) = makeThings(maxMemory)
    val memoryMode = MemoryMode.ON_HEAP
    assert(mm.storageMemoryUsed === 0L)
    assert(mm.acquireStorageMemory(dummyBlock, 10L, memoryMode))
    assertEvictBlocksToFreeSpaceNotCalled(ms)
    assert(mm.storageMemoryUsed === 10L)

    assert(mm.acquireStorageMemory(dummyBlock, 100L, memoryMode))
    assertEvictBlocksToFreeSpaceNotCalled(ms)
    assert(mm.storageMemoryUsed === 110L)
    // Acquire more than the max, not granted
    assert(!mm.acquireStorageMemory(dummyBlock, maxMemory + 1L, memoryMode))
    assertEvictBlocksToFreeSpaceNotCalled(ms)
    assert(mm.storageMemoryUsed === 110L)
    // Acquire up to the max, requests after this are still granted due to LRU eviction
    assert(mm.acquireStorageMemory(dummyBlock, maxMemory, memoryMode))
    assertEvictBlocksToFreeSpaceCalled(ms, 110L)
    assert(mm.storageMemoryUsed === 1000L)
    assert(evictedBlocks.nonEmpty)
    evictedBlocks.clear()
    assert(mm.acquireStorageMemory(dummyBlock, 1L, memoryMode))
    assertEvictBlocksToFreeSpaceCalled(ms, 1L)
    assert(evictedBlocks.nonEmpty)
    evictedBlocks.clear()
    // Note: We evicted 1 byte to put another 1-byte block in, so the storage memory used remains at
    // 1000 bytes. This is different from real behavior, where the 1-byte block would have evicted
    // the 1000-byte block entirely. This is set up differently so we can write finer-grained tests.
    assert(mm.storageMemoryUsed === 1000L)
    mm.releaseStorageMemory(800L, memoryMode)
    assert(mm.storageMemoryUsed === 200L)
    // Acquire after release
    assert(mm.acquireStorageMemory(dummyBlock, 1L, memoryMode))
    assertEvictBlocksToFreeSpaceNotCalled(ms)
    assert(mm.storageMemoryUsed === 201L)
    mm.releaseAllStorageMemory()
    assert(mm.storageMemoryUsed === 0L)
    assert(mm.acquireStorageMemory(dummyBlock, 1L, memoryMode))
    assertEvictBlocksToFreeSpaceNotCalled(ms)
    assert(mm.storageMemoryUsed === 1L)
    // Release beyond what was acquired
    mm.releaseStorageMemory(100L, memoryMode)
    assert(mm.storageMemoryUsed === 0L)
  }

  test("execution evicts storage") {
    val maxMemory = 1000L
    val taskAttemptId = 0L
    val (mm, ms) = makeThings(maxMemory)
    val memoryMode = MemoryMode.ON_HEAP
    // Acquire enough storage memory to exceed the storage region
    assert(mm.acquireStorageMemory(dummyBlock, 750L, memoryMode))
    assertEvictBlocksToFreeSpaceNotCalled(ms)
    assert(mm.executionMemoryUsed === 0L)
    assert(mm.storageMemoryUsed === 750L)
    // Execution needs to request 250 bytes to evict storage memory
    assert(mm.acquireExecutionMemory(100L, taskAttemptId, memoryMode) === 100L)
    assert(mm.executionMemoryUsed === 100L)
    assert(mm.storageMemoryUsed === 750L)
    assertEvictBlocksToFreeSpaceNotCalled(ms)
    // Execution wants 200 bytes but only 150 are free, so storage is evicted
    assert(mm.acquireExecutionMemory(200L, taskAttemptId, memoryMode) === 200L)
    assert(mm.executionMemoryUsed === 300L)
    assert(mm.storageMemoryUsed === 700L)
    assertEvictBlocksToFreeSpaceCalled(ms, 50L)
    assert(evictedBlocks.nonEmpty)
    evictedBlocks.clear()
    mm.releaseAllStorageMemory()
    require(mm.executionMemoryUsed === 300L)
    require(mm.storageMemoryUsed === 0, "bad test: all storage memory should have been released")
    // Acquire some storage memory again, but this time keep it within the storage region
    assert(mm.acquireStorageMemory(dummyBlock, 400L, memoryMode))
    assertEvictBlocksToFreeSpaceNotCalled(ms)
    assert(mm.storageMemoryUsed === 400L)
    assert(mm.executionMemoryUsed === 300L)
    // Execution cannot evict storage because the latter is within the storage fraction,
    // so grant only what's remaining without evicting anything, i.e. 1000 - 300 - 400 = 300
    assert(mm.acquireExecutionMemory(400L, taskAttemptId, memoryMode) === 300L)
    assert(mm.executionMemoryUsed === 600L)
    assert(mm.storageMemoryUsed === 400L)
    assertEvictBlocksToFreeSpaceNotCalled(ms)
  }

  test("execution memory requests smaller than free memory should evict storage (SPARK-12165)") {
    val maxMemory = 1000L
    val taskAttemptId = 0L
    val (mm, ms) = makeThings(maxMemory)
    val memoryMode = MemoryMode.ON_HEAP
    // Acquire enough storage memory to exceed the storage region size
    assert(mm.acquireStorageMemory(dummyBlock, 700L, memoryMode))
    assertEvictBlocksToFreeSpaceNotCalled(ms)
    assert(mm.executionMemoryUsed === 0L)
    assert(mm.storageMemoryUsed === 700L)
    // SPARK-12165: previously, MemoryStore would not evict anything because it would
    // mistakenly think that the 300 bytes of free space was still available even after
    // using it to expand the execution pool. Consequently, no storage memory was released
    // and the following call granted only 300 bytes to execution.
    assert(mm.acquireExecutionMemory(500L, taskAttemptId, memoryMode) === 500L)
    assertEvictBlocksToFreeSpaceCalled(ms, 200L)
    assert(mm.storageMemoryUsed === 500L)
    assert(mm.executionMemoryUsed === 500L)
    assert(evictedBlocks.nonEmpty)
  }

  test("storage does not evict execution") {
    val maxMemory = 1000L
    val taskAttemptId = 0L
    val (mm, ms) = makeThings(maxMemory)
    val memoryMode = MemoryMode.ON_HEAP
    // Acquire enough execution memory to exceed the execution region
    assert(mm.acquireExecutionMemory(800L, taskAttemptId, memoryMode) === 800L)
    assert(mm.executionMemoryUsed === 800L)
    assert(mm.storageMemoryUsed === 0L)
    assertEvictBlocksToFreeSpaceNotCalled(ms)
    // Storage should not be able to evict execution
    assert(mm.acquireStorageMemory(dummyBlock, 100L, memoryMode))
    assert(mm.executionMemoryUsed === 800L)
    assert(mm.storageMemoryUsed === 100L)
    assertEvictBlocksToFreeSpaceNotCalled(ms)
    assert(!mm.acquireStorageMemory(dummyBlock, 250L, memoryMode))
    assert(mm.executionMemoryUsed === 800L)
    assert(mm.storageMemoryUsed === 100L)
    // Do not attempt to evict blocks, since evicting will not free enough memory:
    assertEvictBlocksToFreeSpaceNotCalled(ms)
    mm.releaseExecutionMemory(maxMemory, taskAttemptId, memoryMode)
    mm.releaseStorageMemory(maxMemory, memoryMode)
    // Acquire some execution memory again, but this time keep it within the execution region
    assert(mm.acquireExecutionMemory(200L, taskAttemptId, memoryMode) === 200L)
    assert(mm.executionMemoryUsed === 200L)
    assert(mm.storageMemoryUsed === 0L)
    assertEvictBlocksToFreeSpaceNotCalled(ms)
    // Storage should still not be able to evict execution
    assert(mm.acquireStorageMemory(dummyBlock, 750L, memoryMode))
    assert(mm.executionMemoryUsed === 200L)
    assert(mm.storageMemoryUsed === 750L)
    assertEvictBlocksToFreeSpaceNotCalled(ms) // since there were 800 bytes free
    assert(!mm.acquireStorageMemory(dummyBlock, 850L, memoryMode))
    assert(mm.executionMemoryUsed === 200L)
    assert(mm.storageMemoryUsed === 750L)
    // Do not attempt to evict blocks, since evicting will not free enough memory:
    assertEvictBlocksToFreeSpaceNotCalled(ms)
  }

  test("small heap") {
    val systemMemory = 1024L * 1024
    val reservedMemory = 300L * 1024
    val memoryFraction = 0.8
    val conf = new SparkConf()
      .set(MEMORY_FRACTION, memoryFraction)
      .set(TEST_MEMORY, systemMemory)
      .set(TEST_RESERVED_MEMORY, reservedMemory)

    val mm = UnifiedMemoryManager(conf, numCores = 1)
    val expectedMaxMemory = ((systemMemory - reservedMemory) * memoryFraction).toLong
    assert(mm.maxHeapMemory === expectedMaxMemory)

    // Try using a system memory that's too small
    val conf2 = conf.clone().set(TEST_MEMORY, reservedMemory / 2)
    val exception = intercept[SparkIllegalArgumentException] {
      UnifiedMemoryManager(conf2, numCores = 1)
    }
    checkError(
      exception,
      condition = "INVALID_DRIVER_MEMORY.SYSTEM_MEMORY",
      parameters = Map(
        "systemMemory" -> (reservedMemory / 2).toString,
        "minSystemMemory" -> (reservedMemory * 1.5).ceil.toLong.toString,
        "config" -> DRIVER_MEMORY.key))
  }

  test("insufficient executor memory") {
    val systemMemory = 1024L * 1024
    val reservedMemory = 300L * 1024
    val memoryFraction = 0.8
    val conf = new SparkConf()
      .set(MEMORY_FRACTION, memoryFraction)
      .set(TEST_MEMORY, systemMemory)
      .set(TEST_RESERVED_MEMORY, reservedMemory)

    val mm = UnifiedMemoryManager(conf, numCores = 1)

    // Try using an executor memory that's too small
    val conf2 = conf.clone().set(EXECUTOR_MEMORY.key, (reservedMemory / 2).toString)
    val exception = intercept[SparkIllegalArgumentException] {
      UnifiedMemoryManager(conf2, numCores = 1)
    }
    checkError(
      exception,
      condition = "INVALID_EXECUTOR_MEMORY.CONFIG_MEMORY",
      parameters = Map(
        "executorMemory" -> (reservedMemory / 2).toString,
        "minSystemMemory" -> (reservedMemory * 1.5).ceil.toLong.toString,
        "config" -> EXECUTOR_MEMORY.key))
  }

  test("SPARK-58513: executor validates executor heap") {
    val systemMemory = 400L * 1024
    val reservedMemory = 300L * 1024
    val memoryFraction = 0.8
    val conf = new SparkConf()
      .set(MEMORY_FRACTION, memoryFraction)
      .set(TEST_MEMORY, systemMemory)
      .set(TEST_RESERVED_MEMORY, reservedMemory)
      .set(EXECUTOR_MEMORY.key, (500L * 1024).toString)

    val exception = intercept[SparkIllegalArgumentException] {
      UnifiedMemoryManager(conf, numCores = 1, isDriver = false)
    }
    checkError(
      exception,
      condition = "INVALID_EXECUTOR_MEMORY.SYSTEM_MEMORY",
      parameters = Map(
        "systemMemory" -> systemMemory.toString,
        "minSystemMemory" -> (reservedMemory * 1.5).ceil.toLong.toString,
        "config" -> EXECUTOR_MEMORY.key))
  }

  test("execution can evict cached blocks when there are multiple active tasks (SPARK-12155)") {
    val conf = new SparkConf()
      .set(MEMORY_FRACTION, 1.0)
      .set(MEMORY_STORAGE_FRACTION, 0.0)
      .set(TEST_MEMORY, 1000L)

    val mm = UnifiedMemoryManager(conf, numCores = 2)
    val ms = makeMemoryStore(mm)
    val memoryMode = MemoryMode.ON_HEAP
    assert(mm.maxHeapMemory === 1000)
    // Have two tasks each acquire some execution memory so that the memory pool registers that
    // there are two active tasks:
    assert(mm.acquireExecutionMemory(100L, 0, memoryMode) === 100L)
    assert(mm.acquireExecutionMemory(100L, 1, memoryMode) === 100L)
    // Fill up all of the remaining memory with storage.
    assert(mm.acquireStorageMemory(dummyBlock, 800L, memoryMode))
    assertEvictBlocksToFreeSpaceNotCalled(ms)
    assert(mm.storageMemoryUsed === 800)
    assert(mm.executionMemoryUsed === 200)
    // A task should still be able to allocate 100 bytes execution memory by evicting blocks
    assert(mm.acquireExecutionMemory(100L, 0, memoryMode) === 100L)
    assertEvictBlocksToFreeSpaceCalled(ms, 100L)
    assert(mm.executionMemoryUsed === 300)
    assert(mm.storageMemoryUsed === 700)
    assert(evictedBlocks.nonEmpty)
  }

  test("SPARK-15260: atomically resize memory pools") {
    val conf = new SparkConf()
      .set(MEMORY_FRACTION, 1.0)
      .set(MEMORY_STORAGE_FRACTION, 0.0)
      .set(TEST_MEMORY, 1000L)

    val mm = UnifiedMemoryManager(conf, numCores = 2)
    makeBadMemoryStore(mm)
    val memoryMode = MemoryMode.ON_HEAP
    // Acquire 1000 then release 600 bytes of storage memory, leaving the
    // storage memory pool at 1000 bytes but only 400 bytes of which are used.
    assert(mm.acquireStorageMemory(dummyBlock, 1000L, memoryMode))
    mm.releaseStorageMemory(600L, memoryMode)
    // Before the fix for SPARK-15260, we would first shrink the storage pool by the amount of
    // unused storage memory (600 bytes), try to evict blocks, then enlarge the execution pool
    // by the same amount. If the eviction threw an exception, then we would shrink one pool
    // without enlarging the other, resulting in an assertion failure.
    intercept[RuntimeException] {
      mm.acquireExecutionMemory(1000L, 0, memoryMode)
    }
    val assertInvariants = PrivateMethod[Unit](Symbol("assertInvariants"))
    mm.invokePrivate[Unit](assertInvariants())
  }

  test("not enough free memory in the storage pool --OFF_HEAP") {
    val conf = new SparkConf()
      .set(MEMORY_OFFHEAP_SIZE, 1000L)
      .set(TEST_MEMORY, 1000L)
      .set(MEMORY_OFFHEAP_ENABLED, true)
    val taskAttemptId = 0L
    val mm = UnifiedMemoryManager(conf, numCores = 1)
    val ms = makeMemoryStore(mm)
    val memoryMode = MemoryMode.OFF_HEAP

    assert(mm.acquireExecutionMemory(400L, taskAttemptId, memoryMode) === 400L)
    assert(mm.storageMemoryUsed === 0L)
    assert(mm.executionMemoryUsed === 400L)

    // Fail fast
    assert(!mm.acquireStorageMemory(dummyBlock, 700L, memoryMode))
    assert(mm.storageMemoryUsed === 0L)

    assert(mm.acquireStorageMemory(dummyBlock, 100L, memoryMode))
    assert(mm.storageMemoryUsed === 100L)
    assertEvictBlocksToFreeSpaceNotCalled(ms)

    // Borrow 50 from execution memory
    assert(mm.acquireStorageMemory(dummyBlock, 450L, memoryMode))
    assertEvictBlocksToFreeSpaceNotCalled(ms)
    assert(mm.storageMemoryUsed === 550L)

    // Borrow 50 from execution memory and evict 50 to free space
    assert(mm.acquireStorageMemory(dummyBlock, 100L, memoryMode))
    assertEvictBlocksToFreeSpaceCalled(ms, 50)
    assert(mm.storageMemoryUsed === 600L)
    UnifiedMemoryManager.shutdownUnmanagedMemoryPoller()
  }

  test("optional memory admission and reclamation account for unmanaged memory") {
    Seq(MemoryMode.ON_HEAP, MemoryMode.OFF_HEAP).foreach { mode =>
      val conf = new SparkConf()
        .set(MEMORY_FRACTION, 1.0)
        .set(TEST_MEMORY, 1000L)
        .set(MEMORY_OFFHEAP_SIZE, 1000L)
        .set(MEMORY_STORAGE_FRACTION, storageFraction)
        .set(UNMANAGED_MEMORY_POLLING_INTERVAL, 100L)
      val mm = UnifiedMemoryManager(conf, numCores = 1)
      makeMemoryStore(mm)
      // Poll explicitly so admission observes a fixed unmanaged usage without sleeps.
      UnifiedMemoryManager.shutdownUnmanagedMemoryPoller()
      val consumer = new UnmanagedMemoryConsumer {
        override def unmanagedMemoryConsumerId: UnmanagedMemoryConsumerId =
          UnmanagedMemoryConsumerId("OptionalMemoryTest", mode.toString)
        override def memoryMode: MemoryMode = mode
        override def getMemBytesUsed: Long = 800L
      }
      var held = 0L
      var callbacks = 0
      val unregister = mm.registerOptionalMemoryReclaimer(1L, mode, () => {
        callbacks += 1
        mm.releaseExecutionMemory(held, 1L, mode)
        held = 0L
      })
      try {
        UnifiedMemoryManager.registerUnmanagedMemoryConsumer(consumer)
        val poll = PrivateMethod[Unit](Symbol("pollUnmanagedMemoryUsers"))
        UnifiedMemoryManager invokePrivate poll()
        assert(mm.tryAcquireExecutionMemory(201L, 1L, mode) === 0L)
        held = mm.tryAcquireExecutionMemory(100L, 1L, mode)
        assert(held === 100L)
        assert(mm.acquireExecutionMemory(100L, 2L, mode) === 100L)
        assert(callbacks === 0)
        mm.releaseAllExecutionMemoryForTask(2L)
        held += mm.tryAcquireExecutionMemory(100L, 1L, mode)
        assert(held === 200L)

        // The prospective fair share is 100 bytes until the optional task releases its charge.
        assert(mm.acquireExecutionMemory(150L, 2L, mode) === 150L)
        assert(callbacks === 1)
        mm.releaseAllExecutionMemoryForTask(2L)

        held = mm.tryAcquireExecutionMemory(200L, 1L, mode)
        assert(held === 200L)
        assert(mm.acquireStorageMemory(dummyBlock, 150L, mode))
        assert(callbacks === 2)
        mm.releaseStorageMemory(150L, mode)

        assert(!mm.acquireStorageMemory(dummyBlock, 1001L, mode))
        assert(callbacks === 2)
      } finally {
        mm.releaseAllExecutionMemoryForTask(1L)
        mm.releaseAllExecutionMemoryForTask(2L)
        unregister.run()
        UnifiedMemoryManager.clearUnmanagedMemoryUsers()
      }
    }
  }

  test("storage preflight and immediate grant use the same unmanaged memory sample") {
    val mode = MemoryMode.ON_HEAP
    val conf = new SparkConf()
      .set(MEMORY_OFFHEAP_SIZE, 0L)
      .set(UNMANAGED_MEMORY_POLLING_INTERVAL, 100L)
    val poll = PrivateMethod[Unit](Symbol("pollUnmanagedMemoryUsers"))
    var unmanagedMemory = 0L
    var storageLimitReads = 0
    val mm = new UnifiedMemoryManager(conf, 1000L, 500L, 1) {
      override def maxOnHeapStorageMemory: Long = {
        val available = super.maxOnHeapStorageMemory
        storageLimitReads += 1
        if (storageLimitReads == 2) {
          // A poll may complete after preflight even while the requesting thread holds the monitor.
          unmanagedMemory = 800L
          UnifiedMemoryManager invokePrivate poll()
        }
        available
      }
    }
    makeMemoryStore(mm)
    UnifiedMemoryManager.shutdownUnmanagedMemoryPoller()
    val consumer = new UnmanagedMemoryConsumer {
      override def unmanagedMemoryConsumerId: UnmanagedMemoryConsumerId =
        UnmanagedMemoryConsumerId("OptionalMemoryTest", "poll-during-storage-admission")
      override def memoryMode: MemoryMode = mode
      override def getMemBytesUsed: Long = unmanagedMemory
    }
    var callbacks = 0
    val unregister = mm.registerOptionalMemoryReclaimer(1L, mode, () => { callbacks += 1 })
    try {
      UnifiedMemoryManager.registerUnmanagedMemoryConsumer(consumer)
      UnifiedMemoryManager invokePrivate poll()
      assert(mm.tryAcquireExecutionMemory(200L, 1L, mode) === 200L)
      assert(mm.acquireStorageMemory(dummyBlock, 150L, mode))
      assert(storageLimitReads === 2)
      assert(unmanagedMemory === 800L)
      assert(callbacks === 0)
      assert(mm.executionMemoryUsed === 200L)
    } finally {
      mm.releaseAllStorageMemory()
      mm.releaseAllExecutionMemoryForTask(1L)
      unregister.run()
      UnifiedMemoryManager.clearUnmanagedMemoryUsers()
    }
  }

  test("unmanaged memory tracking with memory mode separation") {
    val maxMemory = 1000L
    val taskAttemptId = 0L
    val conf = new SparkConf()
      .set(MEMORY_FRACTION, 1.0)
      .set(TEST_MEMORY, maxMemory)
      .set(MEMORY_OFFHEAP_ENABLED, false)
      .set(MEMORY_STORAGE_FRACTION, storageFraction)
      .set(UNMANAGED_MEMORY_POLLING_INTERVAL, 100L) // 100ms polling
    val mm = UnifiedMemoryManager(conf, numCores = 1)
    val memoryMode = MemoryMode.ON_HEAP

    // Mock unmanaged memory consumer for ON_HEAP
    class MockOnHeapMemoryConsumer(var memoryUsed: Long) extends UnmanagedMemoryConsumer {
      override def unmanagedMemoryConsumerId: UnmanagedMemoryConsumerId =
        UnmanagedMemoryConsumerId("TestOnHeap", "test-instance")
      override def memoryMode: MemoryMode = MemoryMode.ON_HEAP
      override def getMemBytesUsed: Long = memoryUsed
    }

    // Mock unmanaged memory consumer for OFF_HEAP
    class MockOffHeapMemoryConsumer(var memoryUsed: Long) extends UnmanagedMemoryConsumer {
      override def unmanagedMemoryConsumerId: UnmanagedMemoryConsumerId =
        UnmanagedMemoryConsumerId("TestOffHeap", "test-instance")
      override def memoryMode: MemoryMode = MemoryMode.OFF_HEAP
      override def getMemBytesUsed: Long = memoryUsed
    }

    val onHeapConsumer = new MockOnHeapMemoryConsumer(0L)
    val offHeapConsumer = new MockOffHeapMemoryConsumer(0L)

    try {
      // Register both consumers
      UnifiedMemoryManager.registerUnmanagedMemoryConsumer(onHeapConsumer)
      UnifiedMemoryManager.registerUnmanagedMemoryConsumer(offHeapConsumer)

      // Initially no unmanaged memory usage
      assert(UnifiedMemoryManager.getMemoryByComponentType("TestOnHeap") === 0L)
      assert(UnifiedMemoryManager.getMemoryByComponentType("TestOffHeap") === 0L)

      // Set off-heap memory usage - this should NOT affect on-heap allocations
      offHeapConsumer.memoryUsed = 200L

      // Wait for polling to pick up the change
      Thread.sleep(200)

      // Test that off-heap unmanaged memory doesn't affect on-heap execution memory allocation
      val acquiredMemory = mm.acquireExecutionMemory(1000L, taskAttemptId, memoryMode)
      // Should get full 1000 bytes since off-heap unmanaged memory doesn't affect on-heap pool
      assert(acquiredMemory == 1000L)

      // Release execution memory
      mm.releaseExecutionMemory(acquiredMemory, taskAttemptId, memoryMode)

      // Now set on-heap memory usage - this SHOULD affect on-heap allocations
      onHeapConsumer.memoryUsed = 200L
      Thread.sleep(200)

      // Test that on-heap unmanaged memory affects on-heap execution memory allocation
      val acquiredMemory2 = mm.acquireExecutionMemory(900L, taskAttemptId, memoryMode)
      // Should only get 800 bytes due to 200 bytes of on-heap unmanaged memory usage
      assert(acquiredMemory2 == 800L)

      // Release execution memory to test storage allocation
      mm.releaseExecutionMemory(acquiredMemory2, taskAttemptId, memoryMode)

      // Test storage memory with on-heap unmanaged memory consideration
      onHeapConsumer.memoryUsed = 300L
      Thread.sleep(200)

      // Storage should fail when block size + unmanaged memory > max memory
      assert(!mm.acquireStorageMemory(dummyBlock, 800L, memoryMode))

      // But smaller storage requests should succeed with unmanaged memory factored in
      // With 300L on-heap unmanaged memory, effective max is 700L
      assert(mm.acquireStorageMemory(dummyBlock, 600L, memoryMode))

    } finally {
      UnifiedMemoryManager.shutdownUnmanagedMemoryPoller()
      UnifiedMemoryManager.clearUnmanagedMemoryUsers()
    }
  }

  test("unmanaged memory consumer registration and unregistration") {
    val conf = new SparkConf()
      .set(MEMORY_FRACTION, 1.0)
      .set(TEST_MEMORY, 1000L)
      .set(MEMORY_OFFHEAP_ENABLED, false)
      .set(UNMANAGED_MEMORY_POLLING_INTERVAL, 100L)

    val mm = UnifiedMemoryManager(conf, numCores = 1)

    class MockMemoryConsumer(
        var memoryUsed: Long,
        instanceId: String,
        mode: MemoryMode = MemoryMode.ON_HEAP) extends UnmanagedMemoryConsumer {
      override def unmanagedMemoryConsumerId: UnmanagedMemoryConsumerId =
        UnmanagedMemoryConsumerId("Test", instanceId)
      override def memoryMode: MemoryMode = mode
      override def getMemBytesUsed: Long = memoryUsed
    }

    val consumer1 = new MockMemoryConsumer(100L, "test-instance-1")
    val consumer2 = new MockMemoryConsumer(200L, "test-instance-2")

    try {
      // Register consumers
      UnifiedMemoryManager.registerUnmanagedMemoryConsumer(consumer1)
      UnifiedMemoryManager.registerUnmanagedMemoryConsumer(consumer2)

      Thread.sleep(200)
      assert(UnifiedMemoryManager.getMemoryByComponentType("Test") === 300L)

      // Unregister one consumer
      UnifiedMemoryManager.unregisterUnmanagedMemoryConsumer(consumer1)

      Thread.sleep(200)
      assert(UnifiedMemoryManager.getMemoryByComponentType("Test") === 200L)

      // Unregister second consumer
      UnifiedMemoryManager.unregisterUnmanagedMemoryConsumer(consumer2)

      Thread.sleep(200)
      assert(UnifiedMemoryManager.getMemoryByComponentType("Test") === 0L)

    } finally {
      UnifiedMemoryManager.shutdownUnmanagedMemoryPoller()
      UnifiedMemoryManager.clearUnmanagedMemoryUsers()
    }
  }

  test("unmanaged memory consumer auto-removal when returning -1") {
    val conf = new SparkConf()
      .set(MEMORY_FRACTION, 1.0)
      .set(TEST_MEMORY, 1000L)
      .set(MEMORY_OFFHEAP_ENABLED, false)
      .set(UNMANAGED_MEMORY_POLLING_INTERVAL, 100L)

    val mm = UnifiedMemoryManager(conf, numCores = 1)

    class MockMemoryConsumer(var memoryUsed: Long) extends UnmanagedMemoryConsumer {
      override def unmanagedMemoryConsumerId: UnmanagedMemoryConsumerId =
        UnmanagedMemoryConsumerId("Test", s"test-instance-${this.hashCode()}")
      override def memoryMode: MemoryMode = MemoryMode.ON_HEAP
      override def getMemBytesUsed: Long = memoryUsed
    }

    val consumer1 = new MockMemoryConsumer(100L)
    val consumer2 = new MockMemoryConsumer(200L)

    try {
      // Register consumers
      UnifiedMemoryManager.registerUnmanagedMemoryConsumer(consumer1)
      UnifiedMemoryManager.registerUnmanagedMemoryConsumer(consumer2)

      Thread.sleep(200)
      assert(UnifiedMemoryManager.getMemoryByComponentType("Test") === 300L)

      // Mark consumer1 as inactive
      consumer1.memoryUsed = -1L

      // Wait for polling to detect and remove the inactive consumer
      Thread.sleep(200)
      assert(UnifiedMemoryManager.getMemoryByComponentType("Test") === 200L)

      // Mark consumer2 as inactive as well
      consumer2.memoryUsed = -1L

      Thread.sleep(200)
      assert(UnifiedMemoryManager.getMemoryByComponentType("Test") === 0L)

    } finally {
      UnifiedMemoryManager.shutdownUnmanagedMemoryPoller()
      UnifiedMemoryManager.clearUnmanagedMemoryUsers()
    }
  }

  test("unmanaged memory polling disabled when interval is zero") {
    val conf = new SparkConf()
      .set(MEMORY_FRACTION, 1.0)
      .set(TEST_MEMORY, 1000L)
      .set(MEMORY_OFFHEAP_ENABLED, false)
      .set(MEMORY_STORAGE_FRACTION, storageFraction)
      .set(UNMANAGED_MEMORY_POLLING_INTERVAL, 0L) // Disabled

    val mm = UnifiedMemoryManager(conf, numCores = 1)

    // When polling is disabled, unmanaged memory should not affect allocations
    class MockUnmanagedMemoryConsumer(var memoryUsed: Long) extends UnmanagedMemoryConsumer {
      override def unmanagedMemoryConsumerId: UnmanagedMemoryConsumerId =
        UnmanagedMemoryConsumerId("Test", "test-instance")
      override def memoryMode: MemoryMode = MemoryMode.ON_HEAP
      override def getMemBytesUsed: Long = memoryUsed
    }

    val consumer = new MockUnmanagedMemoryConsumer(500L)

    try {
      UnifiedMemoryManager.registerUnmanagedMemoryConsumer(consumer)

      // Since polling is disabled, should be able to allocate full memory
      val acquiredMemory = mm.acquireExecutionMemory(1000L, 0L, MemoryMode.ON_HEAP)
      assert(acquiredMemory === 1000L)

    } finally {
      UnifiedMemoryManager.shutdownUnmanagedMemoryPoller()
      UnifiedMemoryManager.clearUnmanagedMemoryUsers()
    }
  }

  test("unmanaged memory tracking with off-heap memory enabled") {
    assume(!Utils.isMacOnAppleSilicon)
    val maxOnHeapMemory = 1000L
    val maxOffHeapMemory = 1500L
    val taskAttemptId = 0L
    val conf = new SparkConf()
      .set(MEMORY_FRACTION, 1.0)
      .set(TEST_MEMORY, maxOnHeapMemory)
      .set(MEMORY_OFFHEAP_ENABLED, true)
      .set(MEMORY_OFFHEAP_SIZE, maxOffHeapMemory)
      .set(MEMORY_STORAGE_FRACTION, storageFraction)
      .set(UNMANAGED_MEMORY_POLLING_INTERVAL, 100L)
    val mm = UnifiedMemoryManager(conf, numCores = 1)

    // Mock unmanaged memory consumer
    class MockUnmanagedMemoryConsumer(var memoryUsed: Long) extends UnmanagedMemoryConsumer {
      override def unmanagedMemoryConsumerId: UnmanagedMemoryConsumerId =
        UnmanagedMemoryConsumerId("ExternalLib", "test-instance")

      override def memoryMode: MemoryMode = MemoryMode.OFF_HEAP

      override def getMemBytesUsed: Long = memoryUsed
    }

    val unmanagedConsumer = new MockUnmanagedMemoryConsumer(0L)

    try {
      // Register the unmanaged memory consumer
      UnifiedMemoryManager.registerUnmanagedMemoryConsumer(unmanagedConsumer)

      // Test off-heap memory allocation with unmanaged memory
      unmanagedConsumer.memoryUsed = 300L
      Thread.sleep(200)

      // Test off-heap execution memory
      // With 300 bytes of unmanaged memory, effective off-heap memory should be reduced
      val offHeapAcquired = mm.acquireExecutionMemory(1400L, taskAttemptId, MemoryMode.OFF_HEAP)
      assert(offHeapAcquired <= 1200L, "Off-heap memory should be reduced by unmanaged usage")
      mm.releaseExecutionMemory(offHeapAcquired, taskAttemptId, MemoryMode.OFF_HEAP)

      // Test off-heap storage memory
      unmanagedConsumer.memoryUsed = 500L
      Thread.sleep(200)

      // Storage should fail when block size + unmanaged memory > max off-heap memory
      assert(!mm.acquireStorageMemory(dummyBlock, 1100L, MemoryMode.OFF_HEAP))

      // But smaller off-heap storage requests should succeed
      assert(mm.acquireStorageMemory(dummyBlock, 900L, MemoryMode.OFF_HEAP))
      mm.releaseStorageMemory(900L, MemoryMode.OFF_HEAP)

      // Test that on-heap is NOT affected by off-heap unmanaged memory
      val onHeapAcquired = mm.acquireExecutionMemory(600L, taskAttemptId, MemoryMode.ON_HEAP)
      assert(onHeapAcquired == 600L,
        "On-heap memory should not be reduced by off-heap unmanaged usage")
      mm.releaseExecutionMemory(onHeapAcquired, taskAttemptId, MemoryMode.ON_HEAP)

      // Test with mixed memory modes
      unmanagedConsumer.memoryUsed = 200L
      Thread.sleep(200)

      // Allocate some on-heap and off-heap memory
      val onHeap = mm.acquireExecutionMemory(400L, taskAttemptId, MemoryMode.ON_HEAP)
      val offHeap = mm.acquireExecutionMemory(1000L, taskAttemptId, MemoryMode.OFF_HEAP)

      assert(onHeap == 400L && offHeap <= 1300L,
        "Off-heap memory pool should respect unmanaged memory usage, on-heap should not")

    } finally {
      UnifiedMemoryManager.shutdownUnmanagedMemoryPoller()
      UnifiedMemoryManager.clearUnmanagedMemoryUsers()
    }
  }
}
