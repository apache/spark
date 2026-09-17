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

package org.apache.spark.sql.execution.python

import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.AtomicBoolean

import org.apache.spark.SparkFunSuite

class InProcessPythonRuntimeSuite extends SparkFunSuite {
  override def afterEach(): Unit = {
    try { InProcessPythonRuntime.shutdown() } finally { super.afterEach() }
  }

  test("calls from different threads use the same interpreter owner thread") {
    val first = InProcessPythonRuntime.onInterpreterThread { Thread.currentThread() }
    @volatile var second: Thread = null
    val caller = new Thread(() => {
      second = InProcessPythonRuntime.onInterpreterThread { Thread.currentThread() }
    })
    caller.start()
    caller.join(10000)
    assert(!caller.isAlive)
    assert(first eq second)
    assert(first ne Thread.currentThread())
    InProcessPythonRuntime.shutdown()
    val restarted = InProcessPythonRuntime.onInterpreterThread { Thread.currentThread() }
    assert(restarted ne first)
  }

  test("interpreter exceptions retain their original cause") {
    val expected = new IllegalArgumentException("python failure")
    val actual = intercept[IllegalArgumentException] {
      InProcessPythonRuntime.onInterpreterThread { throw expected }
    }
    assert(actual eq expected)
  }

  test("interruption does not release caller resources before native work finishes") {
    val entered = new CountDownLatch(1)
    val finish = new CountDownLatch(1)
    val returned = new CountDownLatch(1)
    val interrupted = new AtomicBoolean(false)
    val caller = new Thread(() => {
      try {
        InProcessPythonRuntime.onInterpreterThread {
          entered.countDown()
          assert(finish.await(10, TimeUnit.SECONDS))
        }
        interrupted.set(Thread.currentThread().isInterrupted)
      } finally {
        returned.countDown()
      }
    })
    caller.start()
    try {
      assert(entered.await(10, TimeUnit.SECONDS))
      caller.interrupt()
      assert(!returned.await(100, TimeUnit.MILLISECONDS))
    } finally {
      finish.countDown()
      caller.join(10000)
    }
    assert(!caller.isAlive)
    assert(interrupted.get())
  }
}
