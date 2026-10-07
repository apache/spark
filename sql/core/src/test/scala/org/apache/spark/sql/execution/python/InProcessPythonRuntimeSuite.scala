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
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicReference}

import org.mockito.Mockito.{mock, when}

import org.apache.spark.{SparkConf, SparkFunSuite, SparkIllegalArgumentException, TaskContext, TaskKilledException}
import org.apache.spark.api.plugin.PluginContext
import org.apache.spark.api.python.{ChainedPythonFunctions, PythonEvalType, SimplePythonFunction}
import org.apache.spark.internal.config.Python.{IN_PROCESS_PATH_RULE, IN_PROCESS_SITE_PACKAGES}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.PythonUDF
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.types.{LongType, StructType}
import org.apache.spark.sql.util.ArrowUtils

class InProcessPythonRuntimeSuite extends SparkFunSuite {
  import InProcessEvaluatorTestUtils._

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

  test("shutdown does not wait for registrations while no call is running") {
    val field = ArrowUtils.toArrowField("result", LongType, true, "UTC")
    intercept[NullPointerException] {
      runtime.register("idle", Array.emptyByteArray, field, "3.12", false, false, false, true)
    }
    val start = System.nanoTime()
    runtime.shutdown(waitMillis = 5000)
    assert(System.nanoTime() - start < TimeUnit.SECONDS.toNanos(2) && !runtime.isTerminated)
    // The last release finishes the shutdown.
    runtime.release(Seq("idle"))
    runtime.shutdown()
    assert(runtime.isTerminated)
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
    assert(e.getMessage.contains(IN_PROCESS_PATH_RULE) && !e.getMessage.contains("libjep"))
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
    val metrics = allMetrics()
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
    val metrics = allMetrics()
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
    val abandoned = new AtomicInteger()
    val others = new AtomicInteger()

    def resources(lockWaitMillis: Long = 10000L)
      : InProcessArrowEvalPythonEvaluatorFactory.IteratorResources =
      new InProcessArrowEvalPythonEvaluatorFactory.IteratorResources(
        () => taskMemory.incrementAndGet(),
        () => abandoned.incrementAndGet(),
        () => others.incrementAndGet(),
        lockWaitMillis)
  }

  /**
   * Runs `test` while a consumer on another thread is inside a call, optionally running
   * Python, until `test` returns. The consumer is released and joined even if `test` fails.
   */
  private def withConsumer(
      resources: InProcessArrowEvalPythonEvaluatorFactory.IteratorResources,
      inPython: Boolean = false,
      readingInput: Boolean = false)(test: => Unit): Boolean = {
    val entered = new CountDownLatch(1)
    val finish = new CountDownLatch(1)
    val closedAfterCall = new AtomicBoolean()
    val consumer = thread {
      assert(resources.enter())
      try {
        if (inPython) {
          resources.withoutLock { entered.countDown(); finish.await(10, TimeUnit.SECONDS) }
        } else if (readingInput) {
          resources.startReadingInput()
          try {
            entered.countDown()
            finish.await(10, TimeUnit.SECONDS)
          } finally {
            resources.endReadingInput()
          }
        } else {
          entered.countDown()
          finish.await(10, TimeUnit.SECONDS)
        }
        closedAfterCall.set(resources.isClosed)
      } finally {
        resources.exit()
      }
    }
    try {
      assert(entered.await(10, TimeUnit.SECONDS))
      test
    } finally {
      finish.countDown()
      consumer.join(10000)
    }
    assert(!consumer.isAlive)
    closedAfterCall.get
  }

  test("task completion waits for the consumer's lock and stops later calls") {
    val releases = new Releases
    val resources = releases.resources()
    var closing: Thread = null
    withConsumer(resources) {
      closing = thread(resources.close())
      closing.join(200)
      // Nothing is released while the consumer reads input, the queue or Arrow vectors.
      assert(closing.isAlive && resources.isClosed && releases.taskMemory.get == 0)
    }
    closing.join(10000)
    assert(!closing.isAlive && releases.taskMemory.get == 1 && releases.others.get == 1)
    assert(!resources.enter())
    assert(releases.taskMemory.get == 1 && releases.others.get == 1)
  }

  test("task completion releases task memory at once while Python runs") {
    val releases = new Releases
    val resources = releases.resources()
    val closedAfterPython = withConsumer(resources, inPython = true) {
      resources.close()
      // The listener does not wait for Python, but keeps the Arrow vectors Python may use.
      assert(releases.taskMemory.get == 1 && releases.others.get == 0)
    }
    assert(closedAfterPython && releases.taskMemory.get == 1 && releases.others.get == 1)
  }

  test("task completion waits only briefly for a consumer blocked on its input") {
    val releases = new Releases
    val resources = releases.resources(lockWaitMillis = 50L)
    withConsumer(resources, readingInput = true) {
      resources.close()
      // The executor frees the task memory, after the listener deletes what lives outside it.
      assert(releases.taskMemory.get == 0 && releases.abandoned.get == 1)
      assert(releases.others.get == 0)
    }
    assert(releases.taskMemory.get == 0 && releases.others.get == 1)
  }

  test("an interrupted completion listener still waits for the consumer's lock") {
    val releases = new Releases
    val resources = releases.resources()
    val interrupted = new AtomicBoolean()
    var closing: Thread = null
    withConsumer(resources) {
      closing = thread {
        Thread.currentThread().interrupt()
        resources.close()
        interrupted.set(Thread.currentThread().isInterrupted)
      }
      closing.join(200)
      assert(closing.isAlive && releases.taskMemory.get == 0)
    }
    closing.join(10000)
    assert(!closing.isAlive && interrupted.get)
    assert(releases.taskMemory.get == 1 && releases.others.get == 1)
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

  test("task completion waits for a consumer that is not reading input") {
    val releases = new Releases
    val resources = releases.resources(lockWaitMillis = 50L)
    var closing: Thread = null
    withConsumer(resources) {
      closing = thread(resources.close())
      closing.join(300)
      // The consumer may use the task memory, e.g. the queue, so the listener keeps waiting
      // past its 50 ms instead of leaving that memory to the executor.
      assert(closing.isAlive && releases.abandoned.get == 0 && releases.taskMemory.get == 0)
    }
    closing.join(10000)
    assert(!closing.isAlive && releases.abandoned.get == 0)
    assert(releases.taskMemory.get == 1 && releases.others.get == 1)
  }

  private def readBack(blockAt: Int, blockInNext: Boolean = false): BlockingInput =
    new BlockingInput(InProcessArrowEvalPythonEvaluatorFactory.ReadBack, TaskContext.empty(),
      runtime, blockAt = blockAt, blockInNext = blockInNext)

  /** Completes the task while the input blocks, then unblocks it and joins `consumer`. */
  private def completeWhileBlocked(input: BlockingInput, consumer: Thread): Unit = {
    try {
      assert(input.reached.await(10, TimeUnit.SECONDS))
      val closing = thread(input.context.markTaskCompleted(None))
      closing.join(200)
      assert(closing.isAlive)
      input.gate.countDown()
      closing.join(10000)
      assert(!closing.isAlive)
    } finally {
      input.gate.countDown()
      consumer.join(10000)
    }
  }

  Seq(false, true).foreach { blockInNext =>
    val where = if (blockInNext) "next" else "hasNext"
    test(s"task completion stops a batch fill within one input row (blocked in $where)") {
      val input = readBack(blockAt = 3, blockInNext = blockInNext)
      val iterator = input.iterator()
      val error = new AtomicReference[Throwable]()
      val consumer = thread {
        try iterator.next() catch { case t: Throwable => error.set(t) }
      }
      completeWhileBlocked(input, consumer)
      // A row read while completing is dropped, and no later row is read.
      assert(error.get.isInstanceOf[NoSuchElementException])
      assert(error.get.getMessage == "End of in-process UDF input")
      assert(input.pulled.get == (if (blockInNext) 4 else 3))
      assert(!iterator.hasNext)
    }
  }

  test("hasNext returns false when task completion happens while it reads input") {
    val input = readBack(blockAt = 0)
    val iterator = input.iterator()
    val available = new AtomicReference[java.lang.Boolean]()
    val consumer = thread(available.set(iterator.hasNext))
    completeWhileBlocked(input, consumer)
    assert(available.get == false && input.pulled.get == 0)
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
