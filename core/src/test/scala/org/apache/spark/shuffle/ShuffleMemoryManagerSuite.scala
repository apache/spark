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

package org.apache.spark.shuffle

import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicInteger

import org.mockito.Mockito._
import org.scalatest.concurrent.Timeouts
import org.scalatest.time.SpanSugar._

import org.apache.spark.{SparkConf, SparkFunSuite, TaskContext}

class ShuffleMemoryManagerSuite extends SparkFunSuite with Timeouts {

  val nextTaskAttemptId = new AtomicInteger()

  /** Launch a thread with the given body block and return it. */
  private def startThread(name: String)(body: => Unit): Thread = {
    val thread = new Thread("ShuffleMemorySuite " + name) {
      override def run() {
        try {
          val taskAttemptId = nextTaskAttemptId.getAndIncrement
          val mockTaskContext = mock(classOf[TaskContext], RETURNS_SMART_NULLS)
          when(mockTaskContext.taskAttemptId()).thenReturn(taskAttemptId)
          TaskContext.setTaskContext(mockTaskContext)
          body
        } finally {
          TaskContext.unset()
        }
      }
    }
    thread.start()
    thread
  }

  test("single task requesting memory") {
    val manager = ShuffleMemoryManager.createForTesting(maxMemory = 1000L)

    assert(manager.tryToAcquire(100L) === 100L)
    assert(manager.tryToAcquire(400L) === 400L)
    assert(manager.tryToAcquire(400L) === 400L)
    assert(manager.tryToAcquire(200L) === 100L)
    assert(manager.tryToAcquire(100L) === 0L)
    assert(manager.tryToAcquire(100L) === 0L)

    manager.release(500L)
    assert(manager.tryToAcquire(300L) === 300L)
    assert(manager.tryToAcquire(300L) === 200L)

    manager.releaseMemoryForThisTask()
    assert(manager.tryToAcquire(1000L) === 1000L)
    assert(manager.tryToAcquire(100L) === 0L)
  }

  test("two threads requesting full memory") {
    // Two threads request 500 bytes first, wait for each other to get it, and then request
    // 500 more; we should immediately return 0 as both are now at 1 / N

    val manager = ShuffleMemoryManager.createForTesting(maxMemory = 1000L)

    class State {
      var t1Result1 = -1L
      var t2Result1 = -1L
      var t1Result2 = -1L
      var t2Result2 = -1L
    }
    val state = new State

    val t1 = startThread("t1") {
      val r1 = manager.tryToAcquire(500L)
      state.synchronized {
        state.t1Result1 = r1
        state.notifyAll()
        while (state.t2Result1 === -1L) {
          state.wait()
        }
      }
      val r2 = manager.tryToAcquire(500L)
      state.synchronized { state.t1Result2 = r2 }
    }

    val t2 = startThread("t2") {
      val r1 = manager.tryToAcquire(500L)
      state.synchronized {
        state.t2Result1 = r1
        state.notifyAll()
        while (state.t1Result1 === -1L) {
          state.wait()
        }
      }
      val r2 = manager.tryToAcquire(500L)
      state.synchronized { state.t2Result2 = r2 }
    }

    failAfter(20 seconds) {
      t1.join()
      t2.join()
    }

    assert(state.t1Result1 === 500L)
    assert(state.t2Result1 === 500L)
    assert(state.t1Result2 === 0L)
    assert(state.t2Result2 === 0L)
  }


  test("tasks cannot grow past 1 / N") {
    // Two tasks request 250 bytes first, wait for each other to get it, and then request
    // 500 more; we should only grant 250 bytes to each of them on this second request

    val manager = ShuffleMemoryManager.createForTesting(maxMemory = 1000L)

    class State {
      var t1Result1 = -1L
      var t2Result1 = -1L
      var t1Result2 = -1L
      var t2Result2 = -1L
    }
    val state = new State

    val t1 = startThread("t1") {
      val r1 = manager.tryToAcquire(250L)
      state.synchronized {
        state.t1Result1 = r1
        state.notifyAll()
        while (state.t2Result1 === -1L) {
          state.wait()
        }
      }
      val r2 = manager.tryToAcquire(500L)
      state.synchronized { state.t1Result2 = r2 }
    }

    val t2 = startThread("t2") {
      val r1 = manager.tryToAcquire(250L)
      state.synchronized {
        state.t2Result1 = r1
        state.notifyAll()
        while (state.t1Result1 === -1L) {
          state.wait()
        }
      }
      val r2 = manager.tryToAcquire(500L)
      state.synchronized { state.t2Result2 = r2 }
    }

    failAfter(20 seconds) {
      t1.join()
      t2.join()
    }

    assert(state.t1Result1 === 250L)
    assert(state.t2Result1 === 250L)
    assert(state.t1Result2 === 250L)
    assert(state.t2Result2 === 250L)
  }

  test("tasks can block to get at least 1 / 2N memory") {
    // t1 grabs 1000 bytes and then waits until t2 is ready to make a request. It sleeps
    // for a bit and releases 250 bytes, which should then be granted to t2. Further requests
    // by t2 will return false right away because it now has 1 / 2N of the memory.

    val manager = ShuffleMemoryManager.createForTesting(maxMemory = 1000L)

    class State {
      var t1Requested = false
      var t2Requested = false
      var t1Result = -1L
      var t2Result = -1L
      var t2Result2 = -1L
      var t2WaitTime = 0L
    }
    val state = new State

    val t1 = startThread("t1") {
      state.synchronized {
        state.t1Result = manager.tryToAcquire(1000L)
        state.t1Requested = true
        state.notifyAll()
        while (!state.t2Requested) {
          state.wait()
        }
      }
      // Sleep a bit before releasing our memory; this is hacky but it would be difficult to make
      // sure the other thread blocks for some time otherwise
      Thread.sleep(300)
      manager.release(250L)
    }

    val t2 = startThread("t2") {
      state.synchronized {
        while (!state.t1Requested) {
          state.wait()
        }
        state.t2Requested = true
        state.notifyAll()
      }
      val startTime = System.currentTimeMillis()
      val result = manager.tryToAcquire(250L)
      val endTime = System.currentTimeMillis()
      state.synchronized {
        state.t2Result = result
        // A second call should return 0 because we're now already at 1 / 2N
        state.t2Result2 = manager.tryToAcquire(100L)
        state.t2WaitTime = endTime - startTime
      }
    }

    failAfter(20 seconds) {
      t1.join()
      t2.join()
    }

    // Both threads should've been able to acquire their memory; the second one will have waited
    // until the first one acquired 1000 bytes and then released 250
    state.synchronized {
      assert(state.t1Result === 1000L, "t1 could not allocate memory")
      assert(state.t2Result === 250L, "t2 could not allocate memory")
      assert(state.t2WaitTime > 200, s"t2 waited less than 200 ms (${state.t2WaitTime})")
      assert(state.t2Result2 === 0L, "t1 got extra memory the second time")
    }
  }

  test("releaseMemoryForThisTask") {
    // t1 grabs 1000 bytes and then waits until t2 is ready to make a request. It sleeps
    // for a bit and releases all its memory. t2 should now be able to grab all the memory.

    val manager = ShuffleMemoryManager.createForTesting(maxMemory = 1000L)

    class State {
      var t1Requested = false
      var t2Requested = false
      var t1Result = -1L
      var t2Result1 = -1L
      var t2Result2 = -1L
      var t2Result3 = -1L
      var t2WaitTime = 0L
    }
    val state = new State

    val t1 = startThread("t1") {
      state.synchronized {
        state.t1Result = manager.tryToAcquire(1000L)
        state.t1Requested = true
        state.notifyAll()
        while (!state.t2Requested) {
          state.wait()
        }
      }
      // Sleep a bit before releasing our memory; this is hacky but it would be difficult to make
      // sure the other task blocks for some time otherwise
      Thread.sleep(300)
      manager.releaseMemoryForThisTask()
    }

    val t2 = startThread("t2") {
      state.synchronized {
        while (!state.t1Requested) {
          state.wait()
        }
        state.t2Requested = true
        state.notifyAll()
      }
      val startTime = System.currentTimeMillis()
      val r1 = manager.tryToAcquire(500L)
      val endTime = System.currentTimeMillis()
      val r2 = manager.tryToAcquire(500L)
      val r3 = manager.tryToAcquire(500L)
      state.synchronized {
        state.t2Result1 = r1
        state.t2Result2 = r2
        state.t2Result3 = r3
        state.t2WaitTime = endTime - startTime
      }
    }

    failAfter(20 seconds) {
      t1.join()
      t2.join()
    }

    // Both tasks should've been able to acquire their memory; the second one will have waited
    // until the first one acquired 1000 bytes and then released all of it
    state.synchronized {
      assert(state.t1Result === 1000L, "t1 could not allocate memory")
      assert(state.t2Result1 === 500L, "t2 didn't get 500 bytes the first time")
      assert(state.t2Result2 === 500L, "t2 didn't get 500 bytes the second time")
      assert(state.t2Result3 === 0L, s"t2 got more bytes a third time (${state.t2Result3})")
      assert(state.t2WaitTime > 200, s"t2 waited less than 200 ms (${state.t2WaitTime})")
    }
  }

  test("tasks should not be granted a negative size") {
    val manager = ShuffleMemoryManager.createForTesting(maxMemory = 1000L)
    manager.tryToAcquire(700L)

    val latch = new CountDownLatch(1)
    startThread("t1") {
      manager.tryToAcquire(300L)
      latch.countDown()
    }
    latch.await() // Wait until `t1` calls `tryToAcquire`

    val granted = manager.tryToAcquire(300L)
    assert(0 === granted, "granted is negative")
  }
}
