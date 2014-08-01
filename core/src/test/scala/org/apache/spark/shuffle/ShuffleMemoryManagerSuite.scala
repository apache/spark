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

import org.scalatest.FunSuite
import org.scalatest.concurrent.Timeouts
import org.scalatest.time.SpanSugar._
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.CountDownLatch

class ShuffleMemoryManagerSuite extends FunSuite with Timeouts {
  /** Launch a thread with the given body block and return it. */
  private def startThread(name: String)(body: => Unit): Thread = {
    val thread = new Thread("ShuffleMemorySuite " + name) {
      override def run() {
        body
      }
    }
    thread.start()
    thread
  }

  test("single thread requesting memory") {
    val manager = new ShuffleMemoryManager(1000L)

    assert(manager.tryToAcquire(100L) === true)
    assert(manager.tryToAcquire(400L) === true)
    assert(manager.tryToAcquire(400L) === true)
    assert(manager.tryToAcquire(200L) === false)
    assert(manager.tryToAcquire(100L) === true)
    assert(manager.tryToAcquire(100L) === false)

    manager.release(500L)
    assert(manager.tryToAcquire(300L) === true)
    assert(manager.tryToAcquire(300L) === false)

    manager.releaseMemoryForThisThread()
    assert(manager.tryToAcquire(1000L) === true)
    assert(manager.tryToAcquire(100L) === false)
  }

  test("two threads requesting full memory") {
    val manager = new ShuffleMemoryManager(1000L)
    val t1Succeeded = new AtomicBoolean(false)
    val t2Succeeded = new AtomicBoolean(false)

    val t1 = startThread("t1") {
      t1Succeeded.set(manager.tryToAcquire(1000L))
    }

    val t2 = startThread("t2") {
      t2Succeeded.set(manager.tryToAcquire(1000L))
    }

    failAfter(20 seconds) {
      t1.join()
      t2.join()
    }

    // Only one thread should've been able to grab memory: once it got it, the other one's request
    // should not have been granted because it's asking for more than 1 / N (and N = 2)
    assert(t1Succeeded.get() || t2Succeeded.get(), "neither thread succeeded")
    assert(!t1Succeeded.get() || !t2Succeeded.get(), "both threads succeeded")
  }

  test("threads can block to get at least 1 / 2N memory") {
    // t1 grabs 1000 bytes and then waits until t2 is ready to make a request. It sleeps
    // for a bit and releases 250 bytes, which should then be grabbed by t2. Further requests
    // by t2 will return false right away because it now has 1 / 2N of the memory.

    val manager = new ShuffleMemoryManager(1000L)

    class State {
      var t1Requested = false
      var t2Requested = false
      var t1Succeeded = false
      var t2Succeeded = false
      var t2SucceededSecondTime = false
      var t2WaitTime = 0L
    }
    val state = new State

    val t1 = startThread("t1") {
      state.synchronized {
        state.t1Succeeded = manager.tryToAcquire(1000L)
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
      val success = manager.tryToAcquire(250L)
      val endTime = System.currentTimeMillis()
      state.synchronized {
        state.t2Succeeded = success
        // A second call should not succeed because we're now already at 1 / 2N
        state.t2SucceededSecondTime = manager.tryToAcquire(100L)
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
      assert(state.t1Succeeded, "t1 could not allocate memory")
      assert(state.t2Succeeded, "t2 could not allocate memory")
      assert(state.t2WaitTime > 200, s"t2 waited less than 200 ms (${state.t2WaitTime})")
      assert(!state.t2SucceededSecondTime, "t1 could allocate memory a second time")
    }
  }


  test("releaseMemoryForThisThread") {
    // t1 grabs 1000 bytes and then waits until t2 is ready to make a request. It sleeps
    // for a bit and releases all its memory. t2 starts by requesting 500 bytes, which is too
    // much to grab right away (more than 1 / 2N), so that fails. It then requests 250, which
    // should block and work. Finally it requests 750, which will also work as t1 is all done.

    val manager = new ShuffleMemoryManager(1000L)

    class State {
      var t1Requested = false
      var t2Requested = false
      var t1Succeeded = false
      var t2SucceededGrabbing500 = false
      var t2SucceededGrabbing250 = false
      var t2SucceededGrabbingAllSecondTime = false
      var t2WaitTime = 0L
    }
    val state = new State

    val t1 = startThread("t1") {
      state.synchronized {
        state.t1Succeeded = manager.tryToAcquire(1000L)
        state.t1Requested = true
        state.notifyAll()
        while (!state.t2Requested) {
          state.wait()
        }
      }
      // Sleep a bit before releasing our memory; this is hacky but it would be difficult to make
      // sure the other thread blocks for some time otherwise
      Thread.sleep(300)
      manager.releaseMemoryForThisThread()
    }

    val t2 = startThread("t2") {
      state.synchronized {
        while (!state.t1Requested) {
          state.wait()
        }
        state.t2SucceededGrabbing500 = manager.tryToAcquire(500L)
        state.t2Requested = true
        state.notifyAll()
      }
      val startTime = System.currentTimeMillis()
      val success = manager.tryToAcquire(250L)
      val endTime = System.currentTimeMillis()
      state.synchronized {
        state.t2SucceededGrabbing250 = success
        // Since t1 released its all memory, we should now be able to grab the 750 extra too
        state.t2SucceededGrabbingAllSecondTime = manager.tryToAcquire(750L)
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
      assert(state.t1Succeeded, "t1 could not allocate memory")
      assert(!state.t2SucceededGrabbing500, "t2 grabbed 500 bytes the first time")
      assert(state.t2SucceededGrabbing250, "t2 could not grab 250 bytes")
      assert(state.t2SucceededGrabbingAllSecondTime, "t2 could not grab everything second time")
      assert(state.t2WaitTime > 200, s"t2 waited less than 200 ms (${state.t2WaitTime})")
    }
  }
}
