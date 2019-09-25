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

package org.apache.spark.util

import java.util.concurrent.{CountDownLatch, TimeoutException, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.duration._

import org.scalatest.concurrent.{ThreadSignaler, TimeLimits}

import org.apache.spark.SparkFunSuite

class KeyLockSuite extends SparkFunSuite with TimeLimits {

  // Necessary to make ScalaTest 3.x interrupt a thread on the JVM like ScalaTest 2.2.x
  private implicit val defaultSignaler = ThreadSignaler

  private val foreverMs = 60 * 1000L

  test("The same key should wait when its lock is held") {
    val keyLock = new KeyLock[Object]
    val numThreads = 10
    // Create different objects that are equal
    val keys = List.fill(numThreads)(List(1))
    require(keys.tail.forall(_ ne keys.head) && keys.tail.forall(_ == keys.head))

    // A latch to make `withLock` be called almost at the same time
    val latch = new CountDownLatch(1)
    // Track how many threads get the lock at the same time
    val numThreadsHoldingLock = new AtomicInteger(0)
    // Track how many functions get called
    val numFuncCalled = new AtomicInteger(0)
    @volatile var e: Throwable = null
    val threads = (0 until numThreads).map { i =>
      new Thread() {
        override def run(): Unit = {
          latch.await(foreverMs, TimeUnit.MILLISECONDS)
          keyLock.withLock(keys(i)) {
            var cur = numThreadsHoldingLock.get()
            if (cur != 0) {
              e = new AssertionError(s"numThreadsHoldingLock is not 0: $cur")
            }
            cur = numThreadsHoldingLock.incrementAndGet()
            if (cur != 1) {
              e = new AssertionError(s"numThreadsHoldingLock is not 1: $cur")
            }
            cur = numThreadsHoldingLock.decrementAndGet()
            if (cur != 0) {
              e = new AssertionError(s"numThreadsHoldingLock is not 0: $cur")
            }
            numFuncCalled.incrementAndGet()
          }
        }
      }
    }
    threads.foreach(_.start())
    latch.countDown()
    threads.foreach(_.join())
    if (e != null) {
      throw e
    }
    assert(numFuncCalled.get === numThreads)
  }

  test("A different key should not be locked") {
    val keyLock = new KeyLock[Object]
    val k1 = new Object
    val k2 = new Object

    // Start a thread to hold the lock for `k1` forever
    val latch = new CountDownLatch(1)
    val t = new Thread() {
      override def run(): Unit = try {
        keyLock.withLock(k1) {
          latch.countDown()
          Thread.sleep(foreverMs)
        }
      } catch {
        case _: InterruptedException => // Ignore it as it's the exit signal
      }
    }
    t.start()
    try {
      // Wait until the thread gets the lock for `k1`
      if (!latch.await(foreverMs, TimeUnit.MILLISECONDS)) {
        throw new TimeoutException("thread didn't get the lock")
      }

      var funcCalled = false
      // Verify we can acquire the lock for `k2` and call `func`
      failAfter(foreverMs.millis) {
        keyLock.withLock(k2) {
          funcCalled = true
        }
      }
      assert(funcCalled, "func is not called")
    } finally {
      t.interrupt()
      t.join()
    }
  }
}
