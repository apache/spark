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

import java.util.concurrent.{CompletableFuture, ExecutionException, TimeUnit}
import java.util.concurrent.atomic.AtomicBoolean

import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal

import org.scalatest.concurrent.Eventually
import org.scalatest.time.SpanSugar._

import org.apache.spark.{SparkConf, SparkFunSuite}

class ExecutionMemoryPoolSuite extends SparkFunSuite with Eventually {
  private class WaitingAcquire(acquire: () => Long) {
    val result = new CompletableFuture[Long]()
    val thread = new Thread("execution-memory-waiter") {
      override def run(): Unit = {
        try {
          result.complete(acquire())
        } catch {
          case e: InterruptedException => result.completeExceptionally(e)
          case NonFatal(e) => result.completeExceptionally(e)
        }
      }
    }
    thread.setDaemon(true)

    def awaitWaiting(): Unit = eventually(timeout(10.seconds)) {
      assert(!result.isDone, "acquisition completed instead of waiting")
      assert(thread.getState == Thread.State.WAITING)
      assert(thread.getStackTrace.exists { frame =>
        frame.getClassName == classOf[ExecutionMemoryPool].getName &&
          frame.getMethodName == "acquireMemory"
      })
    }

    def acquired(): Long = result.get(10, TimeUnit.SECONDS)

    def failure(): Throwable = {
      val error = intercept[ExecutionException] {
        result.get(10, TimeUnit.SECONDS)
      }
      error.getCause
    }

    def interrupt(): Unit = {
      thread.interrupt()
      assert(failure().isInstanceOf[InterruptedException])
    }
  }

  private val waiters = new ArrayBuffer[WaitingAcquire]()

  override protected def afterEach(): Unit = {
    try {
      waiters.foreach(_.thread.interrupt())
      waiters.foreach(_.thread.join(10000))
      assert(waiters.forall(!_.thread.isAlive), "memory acquisition thread did not terminate")
    } finally {
      waiters.clear()
      super.afterEach()
    }
  }

  private def acquireAsync(
      pool: ExecutionMemoryPool,
      bytes: Long,
      maybeGrowPool: Long => Unit = _ => ()): WaitingAcquire = {
    acquireAsync(pool.acquireMemory(bytes, 1L, maybeGrowPool))
  }

  private def acquireAsync(acquire: => Long): WaitingAcquire = {
    val waiter = new WaitingAcquire(() => acquire)
    waiters += waiter
    waiter.thread.start()
    waiter.awaitWaiting()
    waiter
  }

  private def newPool(mode: MemoryMode): ExecutionMemoryPool = {
    val pool = new ExecutionMemoryPool(new Object, mode)
    pool.incrementPoolSize(1000L)
    assert(pool.acquireMemory(900L, 2L) == 900L)
    pool
  }

  for (mode <- Seq(MemoryMode.ON_HEAP, MemoryMode.OFF_HEAP)) {
    test(s"retain task registration across two consumers of one TaskMemoryManager ($mode)") {
      val conf = new SparkConf(false)
        .set("spark.memory.offHeap.enabled", "true")
        .set("spark.memory.offHeap.size", "1000")
      val memory = new UnifiedMemoryManager(conf, 1000L, 500L, 1)
      val task = new TaskMemoryManager(memory, 1L)
      val peerTask = new TaskMemoryManager(memory, 2L)
      val owner = new TestMemoryConsumer(task, mode)
      val requester = new TestMemoryConsumer(task, mode)
      val peer = new TestMemoryConsumer(peerTask, mode)
      assert(peer.acquireMemory(900L) == 900L)
      assert(owner.acquireMemory(100L) == 100L)
      val waiter = acquireAsync(requester.acquireMemory(300L))

      // Release through another consumer of the waiting task, not through the pool directly.
      owner.freeMemory(100L)
      peer.freeMemory(300L)
      assert(waiter.acquired() == 300L)
      assert(owner.getUsed() == 0L)
      assert(requester.getUsed() == 300L)
      assert(task.getMemoryConsumptionForThisTask() == 300L)
      assert(peerTask.getMemoryConsumptionForThisTask() == 600L)
      assert(memory.executionMemoryUsed == 900L)

      requester.freeMemory(300L)
      peer.freeMemory(600L)
      assert(task.cleanUpAllAllocatedMemory() == 0L)
      assert(peerTask.cleanUpAllAllocatedMemory() == 0L)
      assert(memory.executionMemoryUsed == 0L)
    }

    for (releaseAll <- Seq(false, true)) {
      test(s"retain a waiting task after its last release ($mode, releaseAll=$releaseAll)") {
        val pool = newPool(mode)
        assert(pool.acquireMemory(100L, 1L) == 100L)
        val waiter = acquireAsync(pool, 300L)

        if (releaseAll) {
          assert(pool.releaseAllMemoryForTask(1L) == 100L)
        } else {
          pool.releaseMemory(100L, 1L)
        }
        pool.releaseMemory(300L, 2L)

        assert(waiter.acquired() == 300L)
        assert(pool.getMemoryUsageForTask(1L) == 300L)
        assert(pool.memoryUsed == 900L)
      }
    }

    test(s"retain a task until all its waiting acquisitions complete ($mode)") {
      val pool = newPool(mode)
      assert(pool.acquireMemory(100L, 1L) == 100L)
      val first = acquireAsync(pool, 200L)
      val second = acquireAsync(pool, 200L)
      pool.releaseMemory(100L, 1L)
      pool.releaseMemory(100L, 2L)

      eventually(timeout(10.seconds)) {
        assert(first.result.isDone || second.result.isDone)
      }
      val (completed, remaining) = if (first.result.isDone) (first, second) else (second, first)
      assert(completed.acquired() == 200L)
      remaining.awaitWaiting()
      pool.releaseMemory(200L, 1L)

      assert(remaining.acquired() == 200L)
      assert(pool.getMemoryUsageForTask(1L) == 200L)
      assert(pool.memoryUsed == 1000L)
    }

    test(s"preserve a waiting task's remaining allocation after a partial release ($mode)") {
      val pool = newPool(mode)
      assert(pool.acquireMemory(100L, 1L) == 100L)
      val waiter = acquireAsync(pool, 300L)
      pool.releaseMemory(40L, 1L)
      pool.releaseMemory(300L, 2L)

      assert(waiter.acquired() == 300L)
      assert(pool.getMemoryUsageForTask(1L) == 360L)
      assert(pool.memoryUsed == 960L)
    }

    for (previousAllocation <- Seq(false, true)) {
      test(s"remove an interrupted zero-byte task ($mode, previous=$previousAllocation)") {
        val pool = newPool(mode)
        if (previousAllocation) {
          assert(pool.acquireMemory(100L, 1L) == 100L)
        }
        val waiter = acquireAsync(pool, 300L)
        if (previousAllocation) {
          pool.releaseMemory(100L, 1L)
        }
        waiter.interrupt()

        // The interrupted task must no longer reduce the remaining task's fair share.
        assert(pool.acquireMemory(100L, 2L) == 100L)
        assert(pool.getMemoryUsageForTask(1L) == 0L)
        assert(pool.releaseAllMemoryForTask(2L) == 1000L)
        assert(pool.memoryUsed == 0L)
      }
    }

    test(s"remove a zero-byte task if the pool-growth callback fails after waiting ($mode)") {
      val pool = newPool(mode)
      val failAfterWaiting = new AtomicBoolean(false)
      val error = new IllegalStateException("pool-growth failure")
      val waiter = acquireAsync(pool, 300L, _ => {
        if (failAfterWaiting.get()) {
          throw error
        }
      })
      failAfterWaiting.set(true)
      pool.releaseMemory(0L, 2L) // Wake the waiter to run the callback again.

      assert(waiter.failure() eq error)
      assert(pool.acquireMemory(100L, 2L) == 100L)
      assert(pool.releaseAllMemoryForTask(2L) == 1000L)
      assert(pool.memoryUsed == 0L)
    }

    test(s"interrupting one acquisition must retain the same task's other waiter ($mode)") {
      val pool = newPool(mode)
      assert(pool.acquireMemory(100L, 1L) == 100L)
      val interrupted = acquireAsync(pool, 300L)
      val remaining = acquireAsync(pool, 300L)
      pool.releaseMemory(100L, 1L)
      interrupted.interrupt()
      remaining.awaitWaiting()
      pool.releaseMemory(300L, 2L)

      assert(remaining.acquired() == 300L)
      assert(pool.getMemoryUsageForTask(1L) == 300L)
      assert(pool.memoryUsed == 900L)
    }
  }
}
