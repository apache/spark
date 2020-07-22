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

import org.apache.spark.SparkFunSuite

class UninterruptibleThreadRunnerSuite extends SparkFunSuite {
  private var latch: CountDownLatch = null
  private var runner: UninterruptibleThreadRunner = null

  override def beforeEach(): Unit = {
    latch = new CountDownLatch(1)
    runner = new UninterruptibleThreadRunner("ThreadName")
  }

  override def afterEach(): Unit = {
    runner.close()
  }

  test("runUninterruptibly should switch to UninterruptibleThread") {
    assert(!Thread.currentThread().isInstanceOf[UninterruptibleThread])
    runner.runUninterruptibly {
      assert(Thread.currentThread().isInstanceOf[UninterruptibleThread])
      latch.countDown()
    }
    assert(latch.await(10, TimeUnit.SECONDS), "await timeout")
  }

  test("runUninterruptibly should not add new UninterruptibleThread") {
    val t = new UninterruptibleThread("test") {
      override def run(): Unit = {
        runUninterruptibly {
          val initialThread = Thread.currentThread()
          assert(initialThread.isInstanceOf[UninterruptibleThread])
          runner.runUninterruptibly {
            val runnerThread = Thread.currentThread()
            assert(runnerThread.isInstanceOf[UninterruptibleThread])
            assert(runnerThread == initialThread)
            latch.countDown()
          }
        }
      }
    }
    t.start()
    assert(latch.await(10, TimeUnit.SECONDS), "await timeout")
  }
}
