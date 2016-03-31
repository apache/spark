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

import java.util.concurrent.{CountDownLatch, TimeUnit}

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.util.Random

import org.scalatest.concurrent.Eventually._

import org.apache.spark.SparkFunSuite

class ThreadUtilsSuite extends SparkFunSuite {

  test("newDaemonSingleThreadExecutor") {
    val executor = ThreadUtils.newDaemonSingleThreadExecutor("this-is-a-thread-name")
    @volatile var threadName = ""
    executor.submit(new Runnable {
      override def run(): Unit = {
        threadName = Thread.currentThread().getName()
      }
    })
    executor.shutdown()
    executor.awaitTermination(10, TimeUnit.SECONDS)
    assert(threadName === "this-is-a-thread-name")
  }

  test("newDaemonSingleThreadScheduledExecutor") {
    val executor = ThreadUtils.newDaemonSingleThreadScheduledExecutor("this-is-a-thread-name")
    try {
      val latch = new CountDownLatch(1)
      @volatile var threadName = ""
      executor.schedule(new Runnable {
        override def run(): Unit = {
          threadName = Thread.currentThread().getName()
          latch.countDown()
        }
      }, 1, TimeUnit.MILLISECONDS)
      latch.await(10, TimeUnit.SECONDS)
      assert(threadName === "this-is-a-thread-name")
    } finally {
      executor.shutdownNow()
    }
  }

  test("newDaemonCachedThreadPool") {
    val maxThreadNumber = 10
    val startThreadsLatch = new CountDownLatch(maxThreadNumber)
    val latch = new CountDownLatch(1)
    val cachedThreadPool = ThreadUtils.newDaemonCachedThreadPool(
      "ThreadUtilsSuite-newDaemonCachedThreadPool",
      maxThreadNumber,
      keepAliveSeconds = 2)
    try {
      for (_ <- 1 to maxThreadNumber) {
        cachedThreadPool.execute(new Runnable {
          override def run(): Unit = {
            startThreadsLatch.countDown()
            latch.await(10, TimeUnit.SECONDS)
          }
        })
      }
      startThreadsLatch.await(10, TimeUnit.SECONDS)
      assert(cachedThreadPool.getActiveCount === maxThreadNumber)
      assert(cachedThreadPool.getQueue.size === 0)

      // Submit a new task and it should be put into the queue since the thread number reaches the
      // limitation
      cachedThreadPool.execute(new Runnable {
        override def run(): Unit = {
          latch.await(10, TimeUnit.SECONDS)
        }
      })

      assert(cachedThreadPool.getActiveCount === maxThreadNumber)
      assert(cachedThreadPool.getQueue.size === 1)

      latch.countDown()
      eventually(timeout(10.seconds)) {
        // All threads should be stopped after keepAliveSeconds
        assert(cachedThreadPool.getActiveCount === 0)
        assert(cachedThreadPool.getPoolSize === 0)
      }
    } finally {
      cachedThreadPool.shutdownNow()
    }
  }

  test("sameThread") {
    val callerThreadName = Thread.currentThread().getName()
    val f = Future {
      Thread.currentThread().getName()
    }(ThreadUtils.sameThread)
    val futureThreadName = Await.result(f, 10.seconds)
    assert(futureThreadName === callerThreadName)
  }

  test("runInNewThread") {
    import ThreadUtils._
    assert(runInNewThread("thread-name") { Thread.currentThread().getName } === "thread-name")
    assert(runInNewThread("thread-name") { Thread.currentThread().isDaemon } === true)
    assert(
      runInNewThread("thread-name", isDaemon = false) { Thread.currentThread().isDaemon } === false
    )
    val uniqueExceptionMessage = "test" + Random.nextInt()
    val exception = intercept[IllegalArgumentException] {
      runInNewThread("thread-name") { throw new IllegalArgumentException(uniqueExceptionMessage) }
    }
    assert(exception.asInstanceOf[IllegalArgumentException].getMessage === uniqueExceptionMessage)
    assert(exception.getStackTrace.mkString("\n").contains(
      "... run in separate thread using org.apache.spark.util.ThreadUtils ...") === true,
      "stack trace does not contain expected place holder"
    )
    assert(exception.getStackTrace.mkString("\n").contains("ThreadUtils.scala") === false,
      "stack trace contains unexpected references to ThreadUtils"
    )
  }
}
