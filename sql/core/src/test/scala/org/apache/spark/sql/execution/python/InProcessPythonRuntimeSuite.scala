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

import org.apache.spark.{SparkFunSuite, TaskContext, TaskKilledException}

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

  gridTest("cancelled callers stop waiting without entering the interpreter")(
      Seq(true, false)) { interruptThread =>
    val entered = new CountDownLatch(1)
    val finish = new CountDownLatch(1)
    val waiting = new CountDownLatch(1)
    val returned = new CountDownLatch(1)
    val invoked = new AtomicBoolean(false)
    val cancelled = new AtomicBoolean(false)
    val context = TaskContext.empty()
    val owner = new Thread(() => {
      InProcessPythonRuntime.onInterpreterThread {
        entered.countDown()
        assert(finish.await(10, TimeUnit.SECONDS))
      }
    })
    val waiter = new Thread(() => {
      TaskContext.setTaskContext(context)
      try {
        waiting.countDown()
        InProcessPythonRuntime.onInterpreterThread { invoked.set(true) }
      } catch {
        case _: InterruptedException => cancelled.set(true)
        case _: TaskKilledException => cancelled.set(true)
      } finally {
        TaskContext.unset()
        returned.countDown()
      }
    })
    owner.start()
    try {
      assert(entered.await(10, TimeUnit.SECONDS))
      waiter.start()
      assert(waiting.await(10, TimeUnit.SECONDS))
      assert(!returned.await(100, TimeUnit.MILLISECONDS))
      context.markInterrupted("test cancellation")
      if (interruptThread) waiter.interrupt()
      assert(returned.await(5, TimeUnit.SECONDS))
      assert(cancelled.get())
      assert(!invoked.get())
    } finally {
      finish.countDown()
      owner.join(10000)
      waiter.join(10000)
    }
    assert(!owner.isAlive && !waiter.isAlive)
  }

  test("already cancelled tasks do not invoke the interpreter") {
    val context = TaskContext.empty()
    context.markInterrupted("test cancellation")
    TaskContext.setTaskContext(context)
    try {
      intercept[TaskKilledException] {
        InProcessPythonRuntime.onInterpreterThread { fail("must not invoke Python") }
      }
    } finally {
      TaskContext.unset()
    }
  }

  test("cancellation during native work is reported after the work finishes") {
    val context = TaskContext.empty()
    val finished = new AtomicBoolean(false)
    TaskContext.setTaskContext(context)
    try {
      intercept[TaskKilledException] {
        InProcessPythonRuntime.onInterpreterThread {
          context.markInterrupted("test cancellation")
          finished.set(true)
        }
      }
      assert(finished.get())
    } finally {
      TaskContext.unset()
    }
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
