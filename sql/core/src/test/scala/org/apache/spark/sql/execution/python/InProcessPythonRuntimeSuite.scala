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

import java.util.Collections
import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}

import org.mockito.Mockito.{mock, when}

import org.apache.spark.{SparkConf, SparkFunSuite, SparkIllegalArgumentException, TaskContext, TaskKilledException}
import org.apache.spark.api.plugin.PluginContext
import org.apache.spark.api.python.{ChainedPythonFunctions, PythonEvalType, SimplePythonFunction}
import org.apache.spark.internal.config.Python.IN_PROCESS_SITE_PACKAGES
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.PythonUDF
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.types.{LongType, StructType}
import org.apache.spark.sql.util.ArrowUtils

class InProcessPythonRuntimeSuite extends SparkFunSuite {
  private var runtime: InProcessPythonRuntime.InterpreterSession = _

  override def beforeEach(): Unit = {
    super.beforeEach()
    runtime = new InProcessPythonRuntime.InterpreterSession()
  }

  override def afterEach(): Unit = {
    try { runtime.shutdown() } finally { super.afterEach() }
  }

  test("site-packages config validates JEP include paths") {
    val conf = new SparkConf(false)
    assert(conf.get(IN_PROCESS_SITE_PACKAGES).isEmpty)
    conf.set(IN_PROCESS_SITE_PACKAGES.key, " /opt/venv/lib, /opt/extra ")
    assert(conf.get(IN_PROCESS_SITE_PACKAGES) == Seq("/opt/venv/lib", "/opt/extra"))
    conf.set(IN_PROCESS_SITE_PACKAGES.key, "back\\slash")
    assert(conf.get(IN_PROCESS_SITE_PACKAGES) == Seq("back\\slash"))
    Seq("bad'path", "bad\npath", "bad\rpath", "bad\u0000path",
      "bad" + new String(Character.toChars(0x1f600)), "bad" + 0xd800.toChar,
      s"bad${java.io.File.pathSeparator}path")
      .foreach { path =>
        conf.set(IN_PROCESS_SITE_PACKAGES.key, path)
        intercept[IllegalArgumentException] { conf.get(IN_PROCESS_SITE_PACKAGES) }
        intercept[IllegalArgumentException] {
          InProcessPythonRuntime.InterpreterConfiguration.interpreterConfig(Seq(path))
        }
      }
  }

  test("registration failure frees its temporary native command buffer") {
    val before = ArrowUtils.rootAllocator.getAllocatedMemory
    val field = ArrowUtils.toArrowField("result", LongType, true, "UTC")
    intercept[NullPointerException] {
      // This session deliberately has no interpreter, so invocation fails after allocation.
      runtime.register(
        "failed", new Array[Byte](1024 * 1024), field, "3.12", false, false, false, true)
    }
    assert(ArrowUtils.rootAllocator.getAllocatedMemory == before)
    runtime.shutdown(waitMillis = 20)
    assert(!runtime.isTerminated)
    runtime.release(Seq("failed"))
    runtime.shutdown()
    assert(runtime.isTerminated)
  }

  test("plugin reports invalid sitePackages without the installation checklist") {
    val ctx = mock(classOf[PluginContext])
    when(ctx.conf()).thenReturn(new SparkConf().set(IN_PROCESS_SITE_PACKAGES.key, "/a'b"))
    val e = intercept[SparkIllegalArgumentException] {
      new InProcessPythonExecutorPlugin().init(ctx, Collections.emptyMap())
    }
    assert(e.getCondition == "INVALID_CONF_VALUE.REQUIREMENT")
    assert(e.getMessage.contains("single quotes") && !e.getMessage.contains("libjep"))
  }

  test("task-side calls after shutdown report the shutdown") {
    runtime.shutdown()
    val field = ArrowUtils.toArrowField("result", LongType, true, "UTC")
    Seq(
      () => runtime.onInterpreterThread(()),
      () => runtime.register("stopped", Array.emptyByteArray, field, "3.12",
        false, false, false, true)
    ).foreach { call =>
      val e = intercept[IllegalStateException] { call() }
      assert(e.getMessage.contains("has been stopped"))
    }
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
        Seq.empty, Seq.empty, Seq.empty, 10, 0L, "UTC", false, false, false, false, true, metrics) {
      override private[python] def runtimeSession: InProcessPythonRuntime.InterpreterSession =
        runtime

      def createUnusedIterator(): Unit = {
        evaluateBatches(Seq.empty, Array.empty, Iterator.empty, new StructType, context,
          InProcessArrowEvalPythonEvaluatorFactory.ReadBack)
      }
    }
    new TestEvaluator().createUnusedIterator()
    Thread.sleep(20)
    context.markTaskCompleted(None)
    assert(metrics("pythonTotalTime").value == 0L)
  }

  test("evaluators retain the generation captured before consuming any input") {
    val metrics = Seq("pythonInitTime", "pythonProcessingTime", "pythonTotalTime")
      .map(_ -> new SQLMetric("timing", 0L)).toMap
    val context = TaskContext.empty()
    val function = SimplePythonFunction(
      Seq.empty, Collections.emptyMap[String, String](), Collections.emptyList[String](),
      "", "3.12", Collections.emptyList(), null)
    val udf = PythonUDF("identity", function, LongType, Seq.empty,
      PythonEvalType.SQL_SCALAR_ARROW_INPROCESS_UDF, true)
    var lookups = 0
    class TestEvaluator extends InProcessArrowEvalPythonEvaluatorFactory(
        Seq.empty, Seq(udf), Seq.empty, 10, 0L, "UTC", false, false, false, false, true, metrics) {
      override private[python] def runtimeSession: InProcessPythonRuntime.InterpreterSession = {
        lookups += 1
        runtime
      }

      def iterator(): Iterator[InternalRow] = evaluateBatches(
        Seq((ChainedPythonFunctions(Seq(function)), 0L)), Array(Array.empty),
        Iterator.single(InternalRow.empty), new StructType, context,
        InProcessArrowEvalPythonEvaluatorFactory.ReadBack)
    }
    val iterator = new TestEvaluator().iterator()
    assert(lookups == 1)
    runtime.shutdown()
    runtime = new InProcessPythonRuntime.InterpreterSession()
    try {
      val error = intercept[IllegalStateException] { iterator.next() }
      assert(error.getMessage.contains("has been stopped"))
      assert(lookups == 1)
    } finally {
      context.markTaskCompleted(None)
    }
  }

  private class Releases {
    val taskMemory = new AtomicInteger()
    val others = new AtomicInteger()

    def resources(lockWaitMillis: Long = 10000L)
      : InProcessArrowEvalPythonEvaluatorFactory.IteratorResources =
      new InProcessArrowEvalPythonEvaluatorFactory.IteratorResources(
        () => taskMemory.incrementAndGet(), () => others.incrementAndGet(), lockWaitMillis)
  }

  private def thread(body: => Unit): Thread = {
    val t = new Thread(() => body)
    t.start()
    t
  }

  test("task completion waits for the consumer's lock and stops later calls") {
    val releases = new Releases
    val resources = releases.resources()
    val entered = new CountDownLatch(1)
    val finish = new CountDownLatch(1)
    val consumer = thread {
      assert(resources.enter())
      entered.countDown()
      finish.await()
      resources.exit()
    }
    assert(entered.await(10, TimeUnit.SECONDS))
    val closing = thread(resources.close())
    closing.join(200)
    // Nothing is released while the consumer reads input, the queue or Arrow vectors.
    assert(closing.isAlive && resources.isClosed && releases.taskMemory.get == 0)
    finish.countDown()
    closing.join(10000)
    consumer.join(10000)
    assert(!closing.isAlive && releases.taskMemory.get == 1 && releases.others.get == 1)
    assert(!resources.enter())
    assert(releases.taskMemory.get == 1 && releases.others.get == 1)
  }

  test("task completion releases task memory at once while Python runs") {
    val releases = new Releases
    val resources = releases.resources()
    val inPython = new CountDownLatch(1)
    val finish = new CountDownLatch(1)
    val closedAfterPython = new AtomicBoolean()
    val consumer = thread {
      assert(resources.enter())
      try {
        resources.withoutLock { inPython.countDown(); finish.await() }
        closedAfterPython.set(resources.isClosed)
      } finally {
        resources.exit()
      }
    }
    assert(inPython.await(10, TimeUnit.SECONDS))
    resources.close()
    // The listener does not wait for Python, but keeps the Arrow vectors Python may use.
    assert(releases.taskMemory.get == 1 && releases.others.get == 0)
    finish.countDown()
    consumer.join(10000)
    assert(closedAfterPython.get && releases.taskMemory.get == 1 && releases.others.get == 1)
  }

  test("task completion waits only briefly for a consumer blocked on its input") {
    val releases = new Releases
    val resources = releases.resources(lockWaitMillis = 50L)
    val entered = new CountDownLatch(1)
    val finish = new CountDownLatch(1)
    val consumer = thread {
      assert(resources.enter())
      entered.countDown()
      finish.await()
      resources.exit()
    }
    assert(entered.await(10, TimeUnit.SECONDS))
    resources.close()
    // The executor frees the task memory; the consumer releases the rest when it returns.
    assert(releases.taskMemory.get == 0 && releases.others.get == 0)
    finish.countDown()
    consumer.join(10000)
    assert(releases.taskMemory.get == 0 && releases.others.get == 1)
  }

  test("an interrupted completion listener still waits for the consumer's lock") {
    val releases = new Releases
    val resources = releases.resources()
    val entered = new CountDownLatch(1)
    val consumer = thread {
      assert(resources.enter())
      entered.countDown()
      Thread.sleep(300)
      resources.exit()
    }
    assert(entered.await(10, TimeUnit.SECONDS))
    val interrupted = new AtomicBoolean()
    val closing = thread {
      Thread.currentThread().interrupt()
      resources.close()
      interrupted.set(Thread.currentThread().isInterrupted)
    }
    closing.join(10000)
    consumer.join(10000)
    assert(releases.taskMemory.get == 1 && releases.others.get == 1 && interrupted.get)
  }

  test("exhausted iterators close within a call and return no more rows") {
    val releases = new Releases
    val resources = releases.resources()
    assert(resources.enter())
    resources.close()
    resources.exit()
    assert(!resources.enter())
    resources.close()
    assert(releases.taskMemory.get == 1 && releases.others.get == 1)
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
