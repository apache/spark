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

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._

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

  test("sameThread") {
    val callerThreadName = Thread.currentThread().getName()
    val f = Future {
      Thread.currentThread().getName()
    }(ThreadUtils.sameThread)
    val futureThreadName = Await.result(f, 10.seconds)
    assert(futureThreadName === callerThreadName)
  }
}
