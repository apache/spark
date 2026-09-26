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
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.types.StructType

class InProcessPythonRuntimeSuite extends SparkFunSuite {
  private var runtime: InProcessPythonRuntime.InterpreterSession = _

  override def beforeEach(): Unit = {
    super.beforeEach()
    runtime = new InProcessPythonRuntime.InterpreterSession()
  }

  override def afterEach(): Unit = {
    try { runtime.shutdown() } finally { super.afterEach() }
  }

  test("lifecycle errors distinguish configuration mismatch from stopping") {
    val mismatch = intercept[InProcessPythonRuntime.LifecycleException] {
      runtime.requireCompatible(Seq("different"))
    }
    assert(mismatch.getMessage.contains("different sitePackages"))
    runtime.shutdown()
    val stopping = intercept[InProcessPythonRuntime.LifecycleException] {
      runtime.requireCompatible(Seq.empty)
    }
    assert(stopping.getMessage.contains("still stopping"))
  }

  test("sub-millisecond invocations accumulate in processing metrics") {
    val metric = new SQLMetric("timing", 0L)
    val timer = new InProcessArrowEvalPythonEvaluatorFactory.NanosecondTimer(metric)
    (1 to 25).foreach(_ => timer.add(100000L))
    assert(metric.value == 2L)
    timer.add(500000L)
    assert(metric.value == 3L)
  }

  test("unused evaluator iterators do not charge Python total time") {
    val metrics = Seq("pythonInitTime", "pythonProcessingTime", "pythonTotalTime")
      .map(_ -> new SQLMetric("timing", 0L)).toMap
    val context = TaskContext.empty()
    class TestEvaluator extends InProcessArrowEvalPythonEvaluatorFactory(
        Seq.empty, Seq.empty, Seq.empty, 10, 0L, "UTC", false, false, false, false, metrics) {
      def createUnusedIterator(): Unit = {
        evaluate(Seq.empty, Array.empty, Iterator.empty, new StructType, context)
      }
    }
    new TestEvaluator().createUnusedIterator()
    Thread.sleep(20)
    context.markTaskCompleted(None)
    assert(metrics("pythonTotalTime").value == 0L)
  }

  test("calls from different threads use the same interpreter owner thread") {
    val first = runtime.onInterpreterThread { Thread.currentThread() }
    @volatile var second: Thread = null
    val caller = new Thread(() => {
      second = runtime.onInterpreterThread { Thread.currentThread() }
    })
    caller.start()
    caller.join(10000)
    assert(!caller.isAlive)
    assert(first eq second)
    assert(first ne Thread.currentThread())
    assert(first.isDaemon)
    assert(first.getName == "inprocess-python")
    runtime.shutdown()
    intercept[IllegalStateException] {
      runtime.onInterpreterThread { fail("stopped sessions must not restart") }
    }
  }

  test("interpreter timing excludes time queued behind another task") {
    val entered = new CountDownLatch(1)
    val finish = new CountDownLatch(1)
    val waiting = new CountDownLatch(1)
    @volatile var elapsed = -1L
    val owner = new Thread(() => runtime.onInterpreterThread {
      entered.countDown()
      assert(finish.await(10, TimeUnit.SECONDS))
    })
    val caller = new Thread(() => {
      waiting.countDown()
      elapsed = runtime.timedOnInterpreterThread { () }
    })
    owner.start()
    try {
      assert(entered.await(10, TimeUnit.SECONDS))
      caller.start()
      assert(waiting.await(10, TimeUnit.SECONDS))
      // The measured call cannot execute until the preceding task releases the owner.
      caller.join(500)
      assert(caller.isAlive)
    } finally {
      finish.countDown()
      owner.join(10000)
      caller.join(10000)
    }
    assert(!owner.isAlive && !caller.isAlive)
    assert(elapsed >= 0L && elapsed < TimeUnit.MILLISECONDS.toNanos(500))
  }

  test("interpreter exceptions retain their original cause") {
    val expected = new IllegalArgumentException("python failure")
    val actual = intercept[IllegalArgumentException] {
      runtime.onInterpreterThread { throw expected }
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
      runtime.onInterpreterThread {
        entered.countDown()
        assert(finish.await(10, TimeUnit.SECONDS))
      }
    })
    val waiter = new Thread(() => {
      TaskContext.setTaskContext(context)
      try {
        waiting.countDown()
        runtime.onInterpreterThread { invoked.set(true) }
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
        runtime.onInterpreterThread { fail("must not invoke Python") }
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
        runtime.onInterpreterThread {
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
        runtime.onInterpreterThread {
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
  test("cleanup and bounded shutdown do not wait behind a running invocation") {
    val entered = new CountDownLatch(1)
    val finish = new CountDownLatch(1)
    val caller = new Thread(() => runtime.onInterpreterThread {
      entered.countDown()
      assert(finish.await(10, TimeUnit.SECONDS))
    })
    caller.start()
    try {
      assert(entered.await(10, TimeUnit.SECONDS))
      val start = System.nanoTime()
      runtime.release(Seq.empty)
      runtime.release(Seq("partially-registered"))
      runtime.shutdown(waitMillis = 20)
      assert(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start) < 2000)
      assert(!runtime.isTerminated)
      intercept[IllegalStateException] {
        runtime.onInterpreterThread { fail("shutdown must reject new work") }
      }
      runtime.release(Seq("late-task"))
    } finally {
      finish.countDown()
      caller.join(10000)
      runtime.shutdown()
    }
    assert(runtime.isTerminated)
  }

}
