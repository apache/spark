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

import java.io.File
import java.util.Properties
import java.util.concurrent.{CountDownLatch, LinkedBlockingQueue, TimeUnit}
import java.util.concurrent.atomic.AtomicBoolean

import scala.jdk.CollectionConverters._

import org.apache.spark.{SparkEnv, SparkException, TaskContextImpl}
import org.apache.spark.api.python.PythonEvalType
import org.apache.spark.internal.config.{BUFFER_PAGESIZE, PLUGINS}
import org.apache.spark.memory.{TaskMemoryManager, TestMemoryConsumer, TestMemoryManager}
import org.apache.spark.sql.{AnalysisException, Column, QueryTest}
import org.apache.spark.sql.api.python.PythonSQLUtils
import org.apache.spark.sql.catalyst.expressions.PythonUDF
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, ArrowEvalPython, Filter, LocalLimit}
import org.apache.spark.sql.execution.{GlobalLimitExec, ProjectExec, SortExec}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.LongType
import org.apache.spark.util.Utils

/**
 * Planning regressions, and evaluator tests that need no Python; runtime coverage lives in
 * the PySpark integration suite.
 */
class InProcessPythonUDFSuite extends QueryTest with SharedSparkSession {

  import InProcessEvaluatorTestUtils._
  import testImplicits._

  private val plugin = "org.apache.spark.sql.execution.python.InProcessPythonPlugin"

  override def beforeEach(): Unit = {
    super.beforeEach()
    // These tests plan queries without loading a native interpreter. Advertise the plugin
    // after context creation; actual plugin initialization is covered by integration tests.
    SparkEnv.get.conf.set(PLUGINS, Seq(plugin))
  }

  override def afterEach(): Unit = {
    try { SparkEnv.get.conf.remove(PLUGINS) } finally { super.afterEach() }
  }

  private def makeUDF(
      name: String,
      input: Column,
      deterministic: Boolean = true): Column = {
    // Each call creates fresh bytes, as Py4J does. Semantic equality must compare their contents.
    InProcessPythonUDFBuilder.build(
      name, Array[Byte](1, 2), LongType.json, Seq(input).asJava, deterministic, "3.11")
  }

  test("in-process UDFs use PythonUDF and ArrowEvalPython planning contracts") {
    val df = spark.range(10)
    val doubled = makeUDF("double", df("id"))
    val expr = doubled.expr.asInstanceOf[PythonUDF]
    assert(expr.evalType == PythonEvalType.SQL_SCALAR_ARROW_INPROCESS_UDF)
    assert(expr.expensive)
    assert(expr.semanticEquals(makeUDF("double", df("id")).expr))

    val query = df.select(doubled)
    val eval = query.queryExecution.optimizedPlan.collect { case p: ArrowEvalPython => p }
    assert(eval.size == 1)
    assert(eval.head.evalType == PythonEvalType.SQL_SCALAR_ARROW_INPROCESS_UDF)
    val physical = query.queryExecution.executedPlan.collect {
      case p: InProcessArrowEvalPythonExec => p
    }
    assert(physical.size == 1)
    assert(physical.head.producedAttributes ==
      (physical.head.outputSet -- physical.head.child.outputSet))
    assert(physical.head.missingInput.isEmpty)
  }

  test("a committed write that invalidates a cached in-process plan does not fail") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      spark.range(3).write.parquet(path)
      val cached = spark.read.parquet(path).select(makeUDF("identity", col("id")))
      cached.cache()
      try {
        assert(spark.sharedState.cacheManager.lookupCachedData(cached).nonEmpty)
        // Re-caching plans the entry in this session, which rejects in-process UDFs.
        withSQLConf(SQLConf.PYTHON_UDF_PROFILER.key -> "perf") {
          spark.range(3, 5).write.mode("append").parquet(path)
        }
        assert(spark.sharedState.cacheManager.lookupCachedData(cached).isEmpty)
        assert(spark.read.parquet(path).count() == 5)
      } finally {
        cached.unpersist()
      }
    }
  }

  test("unsupported configuration added after column creation fails before task submission") {
    val column = makeUDF("identity", col("id"))
    for (partitionEvaluator <- Seq("true", "false")) {
      withSQLConf(
          SQLConf.USE_PARTITION_EVALUATOR.key -> partitionEvaluator,
          SQLConf.PYTHON_UDF_PROFILER.key -> "perf") {
        val error = intercept[SparkException] {
          spark.range(1).select(column).queryExecution.executedPlan
        }
        checkError(
          exception = error,
          condition = "INVALID_SPARK_CONFIG.UNSUPPORTED_IN_PROCESS_PYTHON_UDF",
          parameters = Map("config" -> SQLConf.PYTHON_UDF_PROFILER.key))
      }
    }
  }

  test("legacy Python profilers are rejected") {
    for (key <- Seq("spark.python.profile", "spark.python.profile.memory")) {
      SparkEnv.get.conf.set(key, "true")
      try {
        checkError(
          exception = intercept[SparkException] {
            InProcessPythonUDFBuilder.checkConfiguration(spark.sessionState.conf)
          },
          condition = "INVALID_SPARK_CONFIG.UNSUPPORTED_IN_PROCESS_PYTHON_UDF",
          parameters = Map("config" -> key))
      } finally {
        SparkEnv.get.conf.remove(key)
      }
    }
  }

  test("missing executor plugin is rejected before task submission") {
    SparkEnv.get.conf.remove(PLUGINS)
    val column = makeUDF("identity", col("id"))
    val error = intercept[SparkException] {
      spark.range(1).select(column).queryExecution.executedPlan.execute()
    }
    checkError(
      exception = error,
      condition = "INVALID_SPARK_CONFIG.MISSING_IN_PROCESS_PYTHON_PLUGIN",
      parameters = Map("plugin" ->
        "org.apache.spark.sql.execution.python.InProcessPythonPlugin"))
  }

  test("a subclass of the executor plugin satisfies the plugin check") {
    SparkEnv.get.conf.set(PLUGINS, Seq(classOf[TunedInProcessPythonPlugin].getName))
    InProcessPythonUDFBuilder.checkConfiguration(spark.sessionState.conf)
    SparkEnv.get.conf.set(PLUGINS, Seq("com.example.MissingPlugin"))
    checkError(
      exception = intercept[SparkException] {
        InProcessPythonUDFBuilder.checkConfiguration(spark.sessionState.conf)
      },
      condition = "INVALID_SPARK_CONFIG.MISSING_IN_PROCESS_PYTHON_PLUGIN",
      parameters = Map("plugin" -> plugin))
  }

  test("AQE validates configuration while planning above a shuffle") {
    val df = spark.range(0, 10, 1, 2).selectExpr("id % 2 AS k", "id AS v")
    val query = df.groupBy("k").agg(sum("v").as("s"))
      .select(makeUDF("identity", col("s")))
    withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
        SQLConf.PYTHON_UDF_PROFILER.key -> "perf") {
      // Planning cannot submit a shuffle stage. Both collect() and explain() plan here.
      val error = intercept[SparkException] { query.queryExecution.executedPlan }
      checkError(
        exception = error,
        condition = "INVALID_SPARK_CONFIG.UNSUPPORTED_IN_PROCESS_PYTHON_UDF",
        parameters = Map("config" -> SQLConf.PYTHON_UDF_PROFILER.key))
    }
  }

  test("positional arguments after named arguments are rejected by the builder") {
    val named = PythonSQLUtils.namedArgumentExpression("x", col("id"))
    val error = intercept[AnalysisException] {
      InProcessPythonUDFBuilder.build(
        "f", Array[Byte](1), LongType.json, Seq(named, col("id")).asJava, true, "3.11")
    }
    assert(error.getCondition == "UNEXPECTED_POSITIONAL_ARGUMENT")
  }

  test("parallel calls fuse and deterministic duplicate calls are shared") {
    val df = spark.range(10)
    val plan = df.select(
      makeUDF("double", df("id")), makeUDF("triple", df("id")),
      makeUDF("double", df("id"))).queryExecution.optimizedPlan
    val eval = plan.collect { case p: ArrowEvalPython => p }
    assert(eval.size == 1)
    assert(eval.head.udfs.size == 2)
  }

  test("nested calls and collapsed projects produce separate evaluation nodes") {
    val df = spark.range(10)
    val nested = df.select(makeUDF("outer", makeUDF("inner", df("id"))))
    val separate = df.select(makeUDF("inner", df("id")).as("x"))
      .select(makeUDF("outer", col("x")))
    Seq(nested, separate).foreach { query =>
      val eval = query.queryExecution.optimizedPlan.collect { case p: ArrowEvalPython => p }
      assert(eval.size == 2)
      assert(eval.forall(_.udfs.forall(_.children.forall(!_.isInstanceOf[PythonUDF]))))
    }
  }

  test("UDFs over grouping keys, aggregate results and constants run after aggregation") {
    val df = spark.range(10).selectExpr("id % 2 AS k", "id AS v")
    val queries = Seq(
      df.groupBy("k").agg(makeUDF("f", col("k"))),
      df.groupBy("k").count().select(col("k"), makeUDF("f", col("k"))),
      df.groupBy("k").agg(sum("v").as("s")).select(makeUDF("f", col("s"))),
      df.agg(count(lit(1)), makeUDF("f", lit(1))))
    queries.foreach { query =>
      val plan = query.queryExecution.optimizedPlan
      val eval = plan.collect { case p: ArrowEvalPython => p }
      assert(eval.size == 1)
      assert(eval.head.child.exists(_.isInstanceOf[Aggregate]))
      assert(!plan.exists(_.missingInput.nonEmpty))
      assert(!query.queryExecution.executedPlan.exists(_.missingInput.nonEmpty))
    }
  }

  test("repeated UDFs in grouping keys and rebuilt queries are semantically equal") {
    val df = spark.range(10)
    val query = df.groupBy(makeUDF("f", col("id"))).agg(makeUDF("f", col("id")))
    assert(!query.queryExecution.optimizedPlan.exists(_.missingInput.nonEmpty))
    val first = df.select(makeUDF("f", col("id"))).queryExecution.optimizedPlan
    val second = df.select(makeUDF("f", col("id"))).queryExecution.optimizedPlan
    assert(first.sameResult(second))
  }

  test("nondeterministic calls work in grouping and sort expressions") {
    val df = spark.range(10)
    val nd = makeUDF("nd", col("id"), deterministic = false)
    Seq(df.groupBy(nd).count(), df.orderBy(nd)).foreach { query =>
      val plan = query.queryExecution.optimizedPlan
      assert(plan.exists(_.isInstanceOf[ArrowEvalPython]))
      assert(!plan.exists(_.missingInput.nonEmpty))
    }
  }

  test("ordinary filters and limits pass through in-process evaluation") {
    val df = spark.range(10)
    val plan = df.filter(col("id") =!= 0).filter(makeUDF("f", col("id")) > 1)
      .queryExecution.optimizedPlan
    val eval = plan.collectFirst { case p: ArrowEvalPython => p }.get
    assert(eval.child.isInstanceOf[Filter])
    val limited = df.select(makeUDF("f", col("id"))).limit(1).queryExecution.optimizedPlan
    val limitedEval = limited.collectFirst { case p: ArrowEvalPython => p }.get
    assert(limitedEval.child.isInstanceOf[LocalLimit])
  }

  test("in-process extraction cannot be disabled") {
    withSQLConf(SQLConf.OPTIMIZER_EXCLUDED_RULES.key -> ExtractPythonUDFs.ruleName) {
      val plan = spark.range(10).select(makeUDF("f", col("id"))).queryExecution.optimizedPlan
      assert(plan.exists(_.isInstanceOf[ArrowEvalPython]))
    }
  }

  test("planning does not parse scheduler CPU settings from SQLConf") {
    withSQLConf("spark.executor.cores" -> "4", "spark.task.cpus" -> "0.5") {
      val plan = spark.range(10).select(makeUDF("f", col("id"))).queryExecution.optimizedPlan
      assert(plan.exists(_.isInstanceOf[ArrowEvalPython]))
    }
  }

  test("inner join conditions using both sides use existing Python join extraction") {
    val left = spark.range(3).toDF("a")
    val right = spark.range(3).toDF("b")
    withSQLConf(SQLConf.CROSS_JOINS_ENABLED.key -> "true") {
      val plan = left.join(right, makeUDF("f", left("a") + right("b")) > 0)
        .queryExecution.optimizedPlan
      assert(plan.exists(_.isInstanceOf[ArrowEvalPython]))
      assert(!plan.exists(_.missingInput.nonEmpty))
    }
  }
  test("non-root limit and offset propagate ordering through the in-process physical node") {
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
        SQLConf.TOP_K_SORT_FALLBACK_THRESHOLD.key -> "1") {
      val df = spark.range(0, 100, 1, 4).orderBy("id")
      val projected = df.select(makeUDF("identity", col("id")))
      Seq(projected.limit(10), projected.offset(7).limit(10)).foreach { query =>
        val plan = query.distinct().queryExecution.executedPlan
        assert(plan.exists {
          case GlobalLimitExec(_, sort: SortExec, _) => !sort.global
          case GlobalLimitExec(_, ProjectExec(_, sort: SortExec), _) => !sort.global
          case _ => false
        })
        assert(plan.exists(_.isInstanceOf[InProcessArrowEvalPythonExec]))
      }
    }
  }

  // Evaluator tests without Python, for the Buffered path's queue and spill directory.

  /** Spill directories of in-process evaluators under every local root directory. */
  private def spillDirs(): Set[String] =
    Utils.getOrCreateLocalRootDirs(SparkEnv.get.conf).toSeq
      .flatMap(root => Option(new File(root).listFiles()).toSeq.flatten)
      .map(_.getAbsolutePath).filter(_.contains("inprocess-udf-")).toSet

  /** A task context whose memory manager has 1 MB pages and limits memory if `spill`. */
  private class BufferedTask(spill: Boolean) {
    val memory = new TestMemoryManager(SparkEnv.get.conf.clone.set(BUFFER_PAGESIZE, 1L << 20))
    if (spill) memory.limit(0)
    val taskMemory = new TaskMemoryManager(memory, 0)
    val context = new TaskContextImpl(0, 0, 0, 0, 0, 1, taskMemory, new Properties, null)
    val session = new InProcessPythonRuntime.InterpreterSession()

    def input(
        rowCount: Int = 25,
        blockAt: Int = -1,
        blockInNext: Boolean = false,
        batchSize: Int = 10): BlockingInput =
      new BlockingInput(InProcessArrowEvalPythonEvaluatorFactory.Buffered(None), context,
        session, rowCount, blockAt, blockInNext, batchSize)

    def close(): Unit = {
      try context.markTaskCompleted(None) finally session.shutdown()
    }
  }

  test("buffered rows create a spill directory only when they spill") {
    val before = spillDirs()
    val task = new BufferedTask(spill = false)
    try {
      val iterator = task.input().iterator()
      // The first batch is buffered in memory, while the queue is in use.
      assert(iterator.next().getLong(0) == 1L && spillDirs() == before)
      assert(iterator.map(_.getLong(0)).toSeq == (2L to 25L) && spillDirs() == before)
    } finally {
      task.close()
    }
  }

  test("buffered rows that spill delete their spill directory at the end of input") {
    val before = spillDirs()
    val task = new BufferedTask(spill = true)
    try {
      val iterator = task.input().iterator()
      assert(iterator.next().getLong(0) == 1L && (spillDirs() -- before).size == 1)
      assert(iterator.map(_.getLong(0)).toSeq == (2L to 25L) && spillDirs() == before)
    } finally {
      task.close()
    }
  }

  /**
   * Completes the task while the consumer of `input` blocks on its input, which makes the
   * listener give up on the lock after 1 s, and returns the consumer's failure.
   */
  private def abandonWhileBlocked(task: BufferedTask, input: BlockingInput)(
      whileAbandoned: => Unit): Throwable = {
    val (consumer, error) = input.nextOnAnotherThread()
    try {
      assert(input.reached.await(10, TimeUnit.SECONDS))
      task.context.markTaskCompleted(None)
      assert(consumer.isAlive)
      whileAbandoned
      task.taskMemory.cleanUpAllAllocatedMemory()
    } finally {
      input.gate.countDown()
      consumer.join(10000)
      task.session.shutdown()
    }
    error()
  }

  Seq(false, true).foreach { blockInNext =>
    val where = if (blockInNext) "next" else "hasNext"
    test(s"task completion deletes the spill directory of an abandoned queue ($where)") {
      val before = spillDirs()
      val task = new BufferedTask(spill = true)
      val input = task.input(blockAt = 5, blockInNext = blockInNext)
      val error = abandonWhileBlocked(task, input) {
        assert(spillDirs() == before)
      }
      // A row read while completing is dropped, without adding it to the abandoned queue.
      assert(error.isInstanceOf[NoSuchElementException])
      assert(error.getMessage == "End of in-process UDF input")
      assert(input.pulled.get == (if (blockInNext) 6 else 5) && spillDirs() == before)
    }
  }

  test("task completion waits for a consumer reading its queue instead of freeing it") {
    val taskMemory = new TaskMemoryManager(SparkEnv.get.memoryManager, 0)
    val stall = new AtomicBoolean(false)
    val stalled = new CountDownLatch(1)
    val context = new TaskContextImpl(0, 0, 0, 0, 0, 1, taskMemory, new Properties, null) {
      // A pause after the consumer takes the lock and before it reads the queue, as a GC
      // pause or a slow read of a spilled queue would cause.
      override private[spark] def killTaskIfInterrupted(): Unit = {
        if (stall.compareAndSet(true, false)) {
          stalled.countDown()
          Thread.sleep(2000)
        }
        super.killTaskIfInterrupted()
      }
    }
    val session = new InProcessPythonRuntime.InterpreterSession()
    try {
      val iterator = new BlockingInput(InProcessArrowEvalPythonEvaluatorFactory.Buffered(None),
        context, session, rowCount = 25).iterator()
      val results = new LinkedBlockingQueue[Any]()
      val consumer = thread {
        try {
          results.put(iterator.next().getLong(0))
          stall.set(true)
          results.put(iterator.next().getLong(0))
        } catch {
          case t: Throwable => results.put(t)
        }
      }
      assert(results.poll(30, TimeUnit.SECONDS) == 1L)
      assert(stalled.await(30, TimeUnit.SECONDS))
      val closing = thread(context.markTaskCompleted(None))
      closing.join(30000)
      assert(!closing.isAlive)
      // What the executor does after the task and its listeners: nothing is left to free.
      assert(taskMemory.cleanUpAllAllocatedMemory() == 0L)
      assert(results.poll(30, TimeUnit.SECONDS) == 2L)
      consumer.join(30000)
    } finally {
      session.shutdown()
    }
  }

  test("an abandoned queue does not spill for other consumers") {
    val before = spillDirs()
    val task = new BufferedTask(spill = false)
    // Fill several in-memory pages of 1 MB, so that the queue could spill all but the last.
    val input = task.input(rowCount = 300000, blockAt = 200000, batchSize = 0)
    abandonWhileBlocked(task, input) {
      task.memory.limit(0)
      val other = new TestMemoryConsumer(task.taskMemory)
      other.use(1L << 20)
      assert(spillDirs() == before && other.getUsed == 0L)
    }
    assert(spillDirs() == before)
  }
}

class TunedInProcessPythonPlugin extends InProcessPythonPlugin
