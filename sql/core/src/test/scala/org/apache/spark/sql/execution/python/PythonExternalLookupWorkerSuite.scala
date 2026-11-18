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
import java.net.{InetSocketAddress, StandardSocketOptions}
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag
import scala.util.Try

import org.apache.spark.{SparkConf, SparkEnv, SparkException, TaskContext}
import org.apache.spark.api.python.{BasePythonRunner, PythonException, SimplePythonFunction}
import org.apache.spark.internal.config.Python.{PYTHON_AUTH_SOCKET_TIMEOUT, PYTHON_TASK_KILL_TIMEOUT, PYTHON_WORKER_REUSE}
import org.apache.spark.resource.ResourceProfile
import org.apache.spark.sql.{AnalysisException, Encoders}
import org.apache.spark.sql.IntegratedUDFTestUtils.{createUserDefinedPythonDataSource, shouldTestPandasUDFs}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.plans.logical.PreparedExternalLookupJoin
import org.apache.spark.sql.connector.read.{ExternalLookupReader, ExternalLookupResultCursor}
import org.apache.spark.sql.execution.SQLExecution
import org.apache.spark.sql.execution.datasources.v2.python.PythonExternalLookupReaderFactory
import org.apache.spark.sql.execution.streaming.runtime.MemoryStream
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.util.{ThreadUtils, Utils}

/**
 * Tests of the executor side of Python data source external lookups: the reader, its cursor and
 * its dedicated Python worker. Each test plans a lookup on the driver, takes the reader factory
 * of the plan, and drives readers in tasks, as the lookup join does.
 *
 * The lookup source has the schema `id BIGINT, pid INT, partition INT, match INT`. For each
 * parameter row with a non-negative `id`, it returns `matches` rows, numbered by `match`, with
 * the process id of the Python worker and the partition of the task; a negative `id` is a miss.
 * With the option `recordBatches`, it returns the same rows as `pyarrow.RecordBatch`es of
 * `batchRows` rows, by default 10, with the ordinal first.
 * It appends its events, with the process id, to the file `events` in `markerDir`.
 */
abstract class PythonExternalLookupTestBase extends PythonDataSourceSuiteBase {
  // Each test uses its own pool of Python workers, through an environment variable of the
  // session, so that the process ids of one test do not depend on another.
  private val poolId = new AtomicInteger(0)

  // The tests of timeouts and of worker reuse assume these values, which are the defaults.
  override protected def sparkConf: SparkConf = super.sparkConf
    .set(PYTHON_TASK_KILL_TIMEOUT.key, "2s")
    .set(PYTHON_WORKER_REUSE.key, "true")

  private def lookupSourceScript: String =
    s"""
       |import logging
       |import os
       |import time
       |from pyspark import TaskContext
       |from pyspark.sql.datasource import (
       |    DataSource, DataSourceLookupReader)
       |
       |def mark(options, event):
       |    with open(os.path.join(options["markerDir"], "events"), "a") as f:
       |        f.write(f"{os.getpid()} {event}\\n")
       |
       |class TestLookupReader(DataSourceLookupReader):
       |    def __init__(self, options):
       |        self.options = options
       |
       |    def __setstate__(self, state):
       |        # Runs when the worker loads the planned reader, as it starts.
       |        self.__dict__.update(state)
       |        if "startSeconds" in self.options:
       |            mark(self.options, "start")
       |            time.sleep(float(self.options["startSeconds"]))
       |
       |    def selectLookupPredicates(self, candidates):
       |        return [i for i, c in enumerate(candidates) if c.attribute == ("id",)][:1]
       |
       |    def lookup(self, parameters):
       |        pairs = self.pairs(parameters)
       |        if self.options.get("recordBatches") != "true":
       |            return pairs
       |        return self.record_batches(pairs)
       |
       |    def record_batches(self, pairs):
       |        import pyarrow as pa
       |        from itertools import islice
       |        types = [pa.int64(), pa.int32(), pa.int32(), pa.int32()]
       |        try:
       |            while True:
       |                chunk = list(islice(pairs, int(self.options.get("batchRows", "10"))))
       |                if not chunk:
       |                    return
       |                yield pa.RecordBatch.from_arrays(
       |                    [pa.array([o for o, _ in chunk], pa.int64())] +
       |                    [pa.array([r[i] for _, r in chunk], t) for i, t in enumerate(types)],
       |                    names=["ordinal", "id", "pid", "partition", "match"])
       |        finally:
       |            pairs.close()
       |
       |    def pairs(self, parameters):
       |        matches = int(self.options.get("matches", "1"))
       |        behavior = self.options.get("behavior", "")
       |        pid = os.getpid()
       |        partition = TaskContext.get().partitionId()
       |        if behavior in ("hang", "trickle"):
       |            mark(self.options, behavior)
       |        if behavior == "hang":
       |            time.sleep(float(self.options.get("seconds", "30")))
       |        if behavior == "log":
       |            logging.getLogger("lookup_test").warning(f"lookup of {len(parameters)} rows")
       |        if behavior == "rlimit":
       |            import resource
       |            mark(self.options, f"rlimit {resource.getrlimit(resource.RLIMIT_AS)[0]}")
       |        yielded = 0
       |        try:
       |            for ordinal, (key,) in enumerate(parameters):
       |                if key < 0:
       |                    continue
       |                for i in range(matches):
       |                    if behavior == "trickle":
       |                        time.sleep(0.05)
       |                    if behavior == "block" and yielded == 1:
       |                        time.sleep(float(self.options.get("seconds", "30")))
       |                    if behavior == "sleep" and yielded == 0:
       |                        time.sleep(float(self.options["seconds"]))
       |                    if behavior == "slow":
       |                        time.sleep(float(self.options["seconds"]))
       |                    if behavior == "fail" and yielded == 1:
       |                        raise ValueError("lookup failed on purpose")
       |                    yield ordinal, (key, pid, partition, i)
       |                    yielded += 1
       |        finally:
       |            mark(self.options, f"finally {yielded}")
       |
       |    def close(self):
       |        mark(self.options, "close")
       |        behavior = self.options.get("closeBehavior", "")
       |        if behavior == "block":
       |            time.sleep(float(self.options.get("seconds", "30")))
       |        if behavior == "fail":
       |            raise ValueError("close failed on purpose")
       |        if behavior == "exit":
       |            os._exit(1)
       |
       |class $dataSourceName(DataSource):
       |    def schema(self):
       |        return "id BIGINT, pid INT, partition INT, match INT"
       |
       |    def lookupReader(self, schema):
       |        return TestLookupReader(self.options)
       |""".stripMargin

  /** The events that the Python lookup readers wrote, as (process id, event). */
  protected def events(markerDir: File): Seq[(Int, String)] = {
    val file = new File(markerDir, "events")
    if (!file.exists()) Seq.empty else {
      Files.readAllLines(file.toPath, StandardCharsets.UTF_8).asScala.toSeq.map { line =>
        val Array(pid, event) = line.split(" ", 2)
        pid.toInt -> event
      }
    }
  }

  /**
   * Plans a lookup of the test source with the options, and runs `f` with its reader factory and
   * a new directory for the events, with the session configurations and a new worker pool.
   */
  protected def withLookup(options: (String, String)*)(confs: (String, String)*)(
      f: (PythonExternalLookupReaderFactory, File) => Unit): Unit = {
    assume(shouldTestPandasUDFs)
    spark.dataSource.registerPython(
      dataSourceName, createUserDefinedPythonDataSource(dataSourceName, lookupSourceScript))
    withTempDir { markerDir =>
      withSQLConf(Seq(
          SQLConf.PYTHON_EXTERNAL_LOOKUP_ENABLED.key -> "true",
          SQLConf.UNSUPPORTED_OPERATION_CHECK_ENABLED.key -> "false",
          s"${PythonWorkerEnvironment.confPrefix}LOOKUP_TEST_POOL" ->
            s"${getClass.getSimpleName}-${poolId.incrementAndGet()}") ++ confs: _*) {
        val lookup = spark.read.format(dataSourceName)
          .options(options.toMap + ("markerDir" -> markerDir.getAbsolutePath)).load()
        val stream = MemoryStream[Long](1)(Encoders.scalaLong, spark.sqlContext).toDF()
        val factory = stream.join(lookup, stream("value") === lookup("id"))
          .queryExecution.optimizedPlan.collectFirst {
            case join: PreparedExternalLookupJoin => join.readerFactory
          }.get.asInstanceOf[PythonExternalLookupReaderFactory]
        f(factory, markerDir)
      }
    }
  }

  /** Runs `f` with `spark.python.task.killTimeout` at `value`, which readers read when created. */
  protected def withKillTimeout(value: String)(f: => Unit): Unit = {
    val conf = SparkEnv.get.conf
    val previous = conf.get(PYTHON_TASK_KILL_TIMEOUT.key)
    conf.set(PYTHON_TASK_KILL_TIMEOUT.key, value)
    try f finally conf.set(PYTHON_TASK_KILL_TIMEOUT.key, previous)
  }

  /** Runs `f` in a task of partition `partition`, with the session configurations. */
  protected def inTask[T: ClassTag](partition: Int = 0)(f: TaskContext => T): T = {
    SQLExecution.withSQLConfPropagated(spark) {
      spark.sparkContext.parallelize(0 to partition, partition + 1)
        .mapPartitionsWithIndex { (index, _) =>
          if (index == partition) Iterator(f(TaskContext.get())) else Iterator.empty
        }.collect().head
    }
  }
}

class PythonExternalLookupWorkerSuite extends PythonExternalLookupTestBase {
  import PythonExternalLookupWorkerSuite._

  test("one Python worker serves every request of a reader") {
    withLookup("matches" -> "2")(SQLConf.ARROW_EXECUTION_MAX_RECORDS_PER_BATCH.key -> "3") {
      (factory, markerDir) =>
        val (results, metrics) = inTask(partition = 1) { _ =>
          val reader = factory.createReader()
          val results = Seq(Seq(1L), Seq(2L, -1L, 3L), Seq(-5L), Seq(4L, 5L)).map { keys =>
            readAll(reader.lookupBatch(parameterRows(keys)))
          }
          val metrics = reader.currentMetricsValues().map(m => m.name() -> m.value()).toMap
          reader.close()
          (results, metrics)
        }
        val pids = results.flatten.map(_.pid).distinct
        assert(pids.length === 1)
        def expected(keys: Long*): Seq[(Int, Long, Int)] = keys.zipWithIndex.flatMap {
          case (key, ordinal) if key >= 0 => Seq((ordinal, key, 0), (ordinal, key, 1))
          case _ => Seq.empty
        }
        assert(results.map(_.map(r => (r.ordinal, r.id, r.matchIndex))) === Seq(
          expected(1L), expected(2L, -1L, 3L), Seq.empty, expected(4L, 5L)))
        assert(results.flatten.forall(_.partition == 1))
        // The bytes sent to and received from the worker.
        assert(metrics.keySet === Set("pythonDataSent", "pythonDataReceived"))
        assert(metrics("pythonDataSent") > 0 && metrics("pythonDataReceived") > 0)
        // The graceful close runs the reader's close() on the same worker.
        assert(events(markerDir).filter(_._2 == "close") === Seq(pids.head -> "close"))
    }
  }

  test("a reader's worker is reused by later tasks only with worker reuse on") {
    def pidsOfTwoTasks(factory: PythonExternalLookupReaderFactory): Seq[Int] = (0 to 1).map { _ =>
      inTask() { _ =>
        val reader = factory.createReader()
        try readAll(reader.lookupBatch(parameterRows(Seq(1L)))).head.pid finally reader.close()
      }
    }
    withLookup()() { (factory, _) =>
      val pids = pidsOfTwoTasks(factory)
      assert(pids.distinct.length === 1)
    }
    val conf = SparkEnv.get.conf
    val reuse = conf.get(PYTHON_WORKER_REUSE)
    conf.set(PYTHON_WORKER_REUSE, false)
    try {
      withLookup()() { (factory, _) =>
        val pids = pidsOfTwoTasks(factory)
        assert(pids.distinct.length === 2)
      }
    } finally {
      conf.set(PYTHON_WORKER_REUSE, reuse)
    }
  }

  test("an early close of a cursor cancels the response and keeps the worker") {
    withLookup("matches" -> "100000")(SQLConf.ARROW_EXECUTION_MAX_RECORDS_PER_BATCH.key -> "1") {
      (factory, markerDir) =>
        val (firstPid, second) = inTask() { _ =>
          val reader = factory.createReader()
          try {
            val cursor = reader.lookupBatch(parameterRows(Seq(1L)))
            assert(cursor.next())
            val firstPid = cursor.get().getInt(1)
            cursor.close()
            val next = reader.lookupBatch(parameterRows(Seq(-1L, 2L)))
            assert(next.next())
            val second = (next.inputOrdinal(), next.get().getLong(0), next.get().getInt(1))
            next.close()
            (firstPid, second)
          } finally {
            reader.close()
          }
        }
        // The same worker serves the next request.
        assert(second === ((1, 2L, firstPid)))
        // The cancelled response stopped long before its end, and ran the generator's finally.
        val yielded = events(markerDir).collect {
          case (pid, event) if event.startsWith("finally") && pid == firstPid =>
            event.stripPrefix("finally ").toInt
        }
        assert(yielded.length === 2)
        assert(yielded.forall(_ < 100000), s"the cancelled responses yielded $yielded rows")
    }
  }

  test("a lookup that does not stop after a cancel is killed after the kill timeout") {
    withLookup("matches" -> "2", "behavior" -> "block", "seconds" -> "30")(
        SQLConf.ARROW_EXECUTION_MAX_RECORDS_PER_BATCH.key -> "1") { (factory, markerDir) =>
        val (firstPid, closeMillis) = inTask() { _ =>
          val reader = factory.createReader()
          try {
            val cursor = reader.lookupBatch(parameterRows(Seq(1L)))
            assert(cursor.next())
            val firstPid = cursor.get().getInt(1)
            val before = metricValues(reader)
            val start = System.nanoTime()
            cursor.close()
            val closeMillis = (System.nanoTime() - start) / 1000000
            // The next request starts a new worker.
            assert(readAll(reader.lookupBatch(parameterRows(Seq(-1L)))).isEmpty)
            // The metrics keep the bytes of the replaced worker, and add those of the new one.
            val after = metricValues(reader)
            val replacement = currentWorker(reader)
            assert(after("pythonDataSent") >= before("pythonDataSent") + replacement.bytesSent)
            assert(after("pythonDataReceived") >=
              before("pythonDataReceived") + replacement.bytesReceived)
            (firstPid, closeMillis)
          } finally {
            reader.close()
          }
        }
        // spark.python.task.killTimeout is 2 seconds in the suite.
        assert(closeMillis >= 1500 && closeMillis < 20000, s"the close took $closeMillis ms")
        // The killed worker ran no finally; a new worker served the next request and the close.
        assert(eventuallyDead(firstPid))
        val newPids = events(markerDir).map(_._1).distinct
        assert(events(markerDir).map(_._2) === Seq("finally 0", "close"))
        assert(newPids.length === 1 && newPids.head != firstPid)
    }
  }

  test("a close() that blocks ends with its worker after the kill timeout") {
    withLookup("closeBehavior" -> "block", "seconds" -> "30")() { (factory, markerDir) =>
      val (pid, failure, closeMillis) = inTask() { _ =>
        val reader = factory.createReader()
        val pid = readAll(reader.lookupBatch(parameterRows(Seq(1L)))).head.pid
        val start = System.nanoTime()
        val failure = caught(reader.close())
        (pid, failure, (System.nanoTime() - start) / 1000000)
      }
      // spark.python.task.killTimeout is 2 seconds in the suite.
      assert(closeMillis >= 1500 && closeMillis < 20000, s"the close took $closeMillis ms")
      checkError(
        exception = failure.asInstanceOf[AnalysisException],
        condition = "PYTHON_DATA_SOURCE_ERROR",
        parameters = Map(
          "action" -> "close",
          "type" -> "lookup reader",
          "msg" -> ("close() did not return within 2000 ms, the value of " +
            "spark.python.task.killTimeout, or its default when the value is not positive.")))
      assert(events(markerDir).map(_._2) === Seq("finally 1", "close"))
      assert(eventuallyDead(pid))
    }
  }

  test("a kill timeout of 0 does not fail a graceful close or a cancel") {
    withKillTimeout("0") {
      withLookup("matches" -> "100000")(SQLConf.ARROW_EXECUTION_MAX_RECORDS_PER_BATCH.key -> "1") {
        (factory, markerDir) =>
          val pids = inTask() { _ =>
            val reader = factory.createReader()
            val cursor = reader.lookupBatch(parameterRows(Seq(1L)))
            assert(cursor.next())
            val firstPid = cursor.get().getInt(1)
            // The cancel reaches Python, and the response ends.
            cursor.close()
            val next = reader.lookupBatch(parameterRows(Seq(-1L, 2L)))
            assert(next.next())
            val secondPid = next.get().getInt(1)
            next.close()
            // Python runs the reader's close().
            reader.close()
            Seq(firstPid, secondPid)
          }
          // The same worker served both requests and the close.
          assert(pids.distinct.length === 1)
          assert(events(markerDir).map(_._1).distinct === pids.distinct)
          val finished = events(markerDir).map(_._2)
          assert(finished.length === 3 && finished.last === "close", finished)
          assert(finished.init.forall(_.startsWith("finally ")), finished)
      }
    }
  }

  test("a negative kill timeout still bounds a close() that blocks") {
    withKillTimeout("-1") {
      withLookup("closeBehavior" -> "block", "seconds" -> "30")() { (factory, markerDir) =>
        val (pid, failure, closeMillis) = inTask() { _ =>
          val reader = factory.createReader()
          val pid = readAll(reader.lookupBatch(parameterRows(Seq(1L)))).head.pid
          val start = System.nanoTime()
          val failure = Try(reader.close()).failed.toOption
          (pid, failure, (System.nanoTime() - start) / 1000000)
        }
        // The default of spark.python.task.killTimeout, 2 seconds, bounds the close.
        assert(closeMillis >= 1500 && closeMillis < 20000, s"the close took $closeMillis ms")
        checkError(
          exception = failure.get.asInstanceOf[AnalysisException],
          condition = "PYTHON_DATA_SOURCE_ERROR",
          parameters = Map(
            "action" -> "close",
            "type" -> "lookup reader",
            "msg" -> ("close() did not return within 2000 ms, the value of " +
              "spark.python.task.killTimeout, or its default when the value is not positive.")))
        assert(events(markerDir).map(_._2) === Seq("finally 1", "close"))
        assert(eventuallyDead(pid))
      }
    }
  }

  test("closing a cursor that has not read its response cancels before the first batch") {
    // Each row takes 1.2 seconds, and batches hold one row.
    withLookup("matches" -> "3", "behavior" -> "slow", "seconds" -> "1.2")(
        SQLConf.ARROW_EXECUTION_MAX_RECORDS_PER_BATCH.key -> "1") { (factory, markerDir) =>
      val closeMillis = inTask() { _ =>
        val reader = factory.createReader()
        try {
          val cursor = reader.lookupBatch(parameterRows(Seq(1L)))
          val start = System.nanoTime()
          cursor.close()
          val closeMillis = (System.nanoTime() - start) / 1000000
          assert(readAll(reader.lookupBatch(parameterRows(Seq(-1L)))).isEmpty)
          closeMillis
        } finally {
          reader.close()
        }
      }
      // The generator stopped at its first row, within the kill timeout of 2 seconds, and the
      // same worker served the next request and the close. A generator closed at a `yield` has
      // not counted the row of that `yield`.
      assert(closeMillis < 2000, s"the close took $closeMillis ms")
      assert(events(markerDir).map(_._2) === Seq("finally 0", "finally 0", "close"))
      assert(events(markerDir).map(_._1).distinct.length === 1)
    }
  }

  test("a close() that raises fails the close") {
    withLookup("closeBehavior" -> "fail")() { (factory, markerDir) =>
      val (pid, failure) = inTask() { _ =>
        val reader = factory.createReader()
        val pid = readAll(reader.lookupBatch(parameterRows(Seq(1L)))).head.pid
        (pid, caught(reader.close()))
      }
      assert(failure.isInstanceOf[PythonException], failure)
      assert(failure.getMessage.contains("ValueError: close failed on purpose"), failure)
      assert(events(markerDir).map(_._2) === Seq("finally 1", "close"))
      assert(eventuallyDead(pid))
    }
  }

  test("a worker that exits during the graceful close fails the close as a crash") {
    withLookup("closeBehavior" -> "exit")() { (factory, markerDir) =>
      val failure = inTask() { _ =>
        val reader = factory.createReader()
        readAll(reader.lookupBatch(parameterRows(Seq(1L))))
        caught(reader.close())
      }
      assert(failure.isInstanceOf[SparkException], failure)
      assert(failure.getMessage.contains("Python worker exited unexpectedly (crashed)"), failure)
      assert(events(markerDir).map(_._2) === Seq("finally 1", "close"))
    }
  }

  test("a close() that fails after a failure of the task is suppressed by that failure") {
    Seq(
      "fail" -> "ValueError: close failed on purpose",
      "block" -> "close() did not return within 2000 ms").foreach { case (behavior, closeError) =>
      withLookup("closeBehavior" -> behavior, "seconds" -> "30")() { (factory, markerDir) =>
        val start = System.nanoTime()
        val failure = intercept[SparkException] {
          inTask() { _ =>
            val reader = factory.createReader()
            val cursor = reader.lookupBatch(parameterRows(Seq(1L)))
            assert(cursor.next())
            // As when the join fails on a result: the cursor and the reader are still open, and
            // the completion listener of the reader closes them.
            throw new IllegalStateException("the task failed on purpose")
          }
        }
        val taskMillis = (System.nanoTime() - start) / 1000000
        assert(taskMillis < 20000, s"the task took $taskMillis ms")
        // The task's error is the earlier failure, which carries the failure of close().
        val error = failure.getCause
        assert(error.isInstanceOf[IllegalStateException], failure)
        assert(error.getMessage === "the task failed on purpose")
        def messages(e: Throwable): Seq[String] =
          e.getSuppressed.toSeq.flatMap(s => Option(s.getMessage).toSeq ++ messages(s))
        assert(messages(error).exists(_.contains(closeError)), messages(error))
        assert(events(markerDir).map(_._2) === Seq("finally 1", "close"), behavior)
      }
    }
  }

  test("a close from another thread does not wait for a request in progress") {
    withLookup("matches" -> "2", "behavior" -> "block", "seconds" -> "30")(
        SQLConf.ARROW_EXECUTION_MAX_RECORDS_PER_BATCH.key -> "1") { (factory, markerDir) =>
        val (pid, closeMillis, failure, afterClose) = inTask() { _ =>
          val reader = factory.createReader()
          val cursor = reader.lookupBatch(parameterRows(Seq(1L)))
          assert(cursor.next())
          val pid = cursor.get().getInt(1)
          // The next row blocks in Python while another thread closes the reader.
          val closing = Future {
            Thread.sleep(500)
            val start = System.nanoTime()
            reader.close()
            (System.nanoTime() - start) / 1000000
          }(ExecutionContext.global)
          val failure = caught(cursor.next())
          val closeMillis = ThreadUtils.awaitResult(closing, 60.seconds)
          val afterClose = caught(reader.lookupBatch(parameterRows(Seq(1L))))
          (pid, closeMillis, failure, afterClose.getMessage)
        }
        assert(closeMillis < 5000, s"the close took $closeMillis ms")
        // The blocked read failed once the worker was destroyed.
        assert(failure.isInstanceOf[java.io.IOException], failure)
        assert(afterClose.contains("used after it was closed"))
        // The worker was destroyed: Python ran neither the generator's finally nor close().
        assert(events(markerDir).isEmpty)
        assert(eventuallyDead(pid))
    }
  }

  test("a close of an idle reader from another thread destroys the worker") {
    withLookup()() { (factory, markerDir) =>
      val pid = inTask() { _ =>
        val reader = factory.createReader()
        val pid = readAll(reader.lookupBatch(parameterRows(Seq(1L)))).head.pid
        val closing = Future(reader.close())(ExecutionContext.global)
        ThreadUtils.awaitResult(closing, 60.seconds)
        pid
      }
      assert(events(markerDir).map(_._2) === Seq("finally 1"))
      assert(eventuallyDead(pid))
    }
  }

  test("the last row of a cursor stays readable after a close from another thread") {
    withLookup("matches" -> "3")() { (factory, _) =>
      val (values, ordinal, memoryAfterClose) = inTask() { _ =>
        val before = ArrowUtils.rootAllocator.getAllocatedMemory
        val reader = factory.createReader()
        val cursor = reader.lookupBatch(parameterRows(Seq(-1L, 7L)))
        assert(cursor.next())
        val row = cursor.get()
        // Another thread closes the reader, which frees the Arrow memory of the response.
        ThreadUtils.awaitResult(Future(reader.close())(ExecutionContext.global), 60.seconds)
        val memoryAfterClose = ArrowUtils.rootAllocator.getAllocatedMemory - before
        // Spark may still read the row that get() returned last, and its ordinal, after the close.
        ((row.getLong(0), row.getInt(3)), cursor.inputOrdinal(), memoryAfterClose)
      }
      assert(values === ((7L, 0)))
      assert(ordinal === 1)
      assert(memoryAfterClose === 0)
    }
  }

  test("a cursor closed on another thread is read to its end before the next request") {
    withLookup("matches" -> "3")(SQLConf.ARROW_EXECUTION_MAX_RECORDS_PER_BATCH.key -> "1") {
      (factory, _) =>
        val next = inTask() { _ =>
          val reader = factory.createReader()
          try {
            val cursor = reader.lookupBatch(parameterRows(Seq(1L)))
            assert(cursor.next())
            // Another thread only marks the cursor closed: the rest of its response is unread.
            ThreadUtils.awaitResult(Future(cursor.close())(ExecutionContext.global), 60.seconds)
            readAll(reader.lookupBatch(parameterRows(Seq(-1L, 2L))))
          } finally {
            reader.close()
          }
        }
        assert(next.map(r => (r.ordinal, r.id, r.matchIndex)) ===
          Seq((1, 2L, 0), (1, 2L, 1), (1, 2L, 2)))
    }
  }

  test("a close from another thread while the worker starts destroys that worker") {
    withLookup("startSeconds" -> "3")() { (factory, markerDir) =>
      inTask() { _ =>
        val reader = factory.createReader()
        // The worker loads the reader for 3 seconds, and another thread closes the reader then.
        val closing = Future {
          Thread.sleep(1000)
          reader.close()
        }(ExecutionContext.global)
        Try(reader.lookupBatch(parameterRows(Seq(1L))))
        ThreadUtils.awaitResult(closing, 60.seconds)
      }
      val pid = events(markerDir).collectFirst { case (pid, "start") => pid }.get
      assert(eventuallyDead(pid))
    }
  }

  test("a close from another thread while a cancelled response drains frees the memory") {
    withLookup("matches" -> "2", "behavior" -> "block", "seconds" -> "30")(
        SQLConf.ARROW_EXECUTION_MAX_RECORDS_PER_BATCH.key -> "1") { (factory, _) =>
      val memoryAfter = inTask() { _ =>
        val before = ArrowUtils.rootAllocator.getAllocatedMemory
        val reader = factory.createReader()
        val cursor = reader.lookupBatch(parameterRows(Seq(1L)))
        assert(cursor.next())
        // The close of the cursor waits for the rest of the response, which blocks in Python,
        // and another thread closes the reader meanwhile, which ends that wait.
        val closing = Future {
          Thread.sleep(500)
          reader.close()
        }(ExecutionContext.global)
        Try(cursor.close())
        ThreadUtils.awaitResult(closing, 60.seconds)
        ArrowUtils.rootAllocator.getAllocatedMemory - before
      }
      assert(memoryAfter === 0)
    }
  }

  test("a reader registers one interrupt and one completion listener") {
    withLookup()() { (factory, _) =>
      val counts = inTask() { context =>
        val before = listenerCounts(context)
        val reader = factory.createReader()
        val afterCreate = listenerCounts(context)
        (1 to 20).foreach(key => readAll(reader.lookupBatch(parameterRows(Seq(key.toLong)))))
        val afterRequests = listenerCounts(context)
        reader.close()
        (before, afterCreate, afterRequests)
      }
      val (before, afterCreate, afterRequests) = counts
      assert(afterCreate === ((before._1 + 1, before._2 + 1)))
      assert(afterRequests === afterCreate)
    }
  }

  test("a task killed during a lookup ends with its worker, without the reader's close()") {
    // A lookup that blocks in Python, killed with and without interrupting the task thread, and
    // one that keeps sending rows, killed without the interrupt.
    Seq(("hang", true), ("hang", false), ("trickle", false)).foreach { case (behavior, interrupt) =>
      withLookup("behavior" -> behavior, "seconds" -> "60", "matches" -> "100000")(
          SQLConf.ARROW_EXECUTION_MAX_RECORDS_PER_BATCH.key -> "1") { (factory, markerDir) =>
        val sc = spark.sparkContext
        val group = s"python-lookup-kill-$behavior-$interrupt"
        val job = Future {
          sc.setJobGroup(group, group, interruptOnCancel = interrupt)
          try {
            inTask() { _ =>
              val reader = factory.createReader()
              readAll(reader.lookupBatch(parameterRows(Seq(1L))))
            }
          } finally {
            sc.clearJobGroup()
          }
        }(ExecutionContext.global)
        eventually(timeout(60.seconds), interval(50.millis)) {
          assert(events(markerDir).exists(_._2 == behavior))
        }
        val Seq((pid, _)) = events(markerDir)
        val start = System.nanoTime()
        sc.cancelJobGroup(group)
        val failure = caught(ThreadUtils.awaitResult(job, 60.seconds))
        assert(Utils.stackTraceToString(failure).contains("cancelled"), failure)
        // The job fails at once; the task ends with its worker.
        assert(eventuallyDead(pid), interrupt)
        val millis = (System.nanoTime() - start) / 1000000
        if (interrupt || behavior == "trickle") {
          // The interrupt ends the wait for the worker at once, and the next read of a response
          // finds the task killed.
          assert(millis < 1500, s"the worker of the killed task ended after $millis ms")
        } else {
          // The interrupt listener destroys the worker after the kill timeout, 2 seconds.
          assert(millis >= 1500 && millis < 20000,
            s"the worker of the killed task ended after $millis ms")
        }
        // Python ran neither the generator's finally nor close().
        assert(events(markerDir) === Seq(pid -> behavior), (behavior, interrupt))
      }
    }
  }

  test("a task killed while its worker starts ends with that worker") {
    // The worker loads the planned reader for 10 seconds, and the kill does not interrupt the
    // task thread, so only the interrupt listener can end the start.
    withLookup("startSeconds" -> "10")() { (factory, markerDir) =>
      val sc = spark.sparkContext
      val group = "python-lookup-kill-during-start"
      val job = Future {
        sc.setJobGroup(group, group, interruptOnCancel = false)
        try {
          inTask() { _ =>
            val reader = factory.createReader()
            readAll(reader.lookupBatch(parameterRows(Seq(1L))))
          }
        } finally {
          sc.clearJobGroup()
        }
      }(ExecutionContext.global)
      eventually(timeout(60.seconds), interval(50.millis)) {
        assert(events(markerDir).map(_._2) === Seq("start"))
      }
      val Seq((pid, _)) = events(markerDir)
      val start = System.nanoTime()
      sc.cancelJobGroup(group)
      caught(ThreadUtils.awaitResult(job, 60.seconds))
      assert(eventuallyDead(pid))
      // The interrupt listener destroyed the worker after the kill timeout, 2 seconds, long
      // before the reader would have loaded.
      val millis = (System.nanoTime() - start) / 1000000
      assert(millis < 6000, s"the worker of the killed task ended after $millis ms")
      // Python ran neither a lookup nor close().
      assert(events(markerDir) === Seq(pid -> "start"))
    }
  }

  test("a task killed between lookups ends its worker without the reader's close()") {
    withLookup()() { (factory, markerDir) =>
      val sc = spark.sparkContext
      val group = "python-lookup-kill-between-lookups"
      val job = Future {
        sc.setJobGroup(group, group, interruptOnCancel = true)
        try {
          inTask() { _ =>
            val reader = factory.createReader()
            readAll(reader.lookupBatch(parameterRows(Seq(1L))))
            // The kill interrupts the task here, outside a call of the reader.
            Thread.sleep(60000)
          }
        } finally {
          sc.clearJobGroup()
        }
      }(ExecutionContext.global)
      eventually(timeout(60.seconds), interval(50.millis)) {
        assert(events(markerDir).map(_._2) === Seq("finally 1"))
      }
      val Seq((pid, _)) = events(markerDir)
      sc.cancelJobGroup(group)
      caught(ThreadUtils.awaitResult(job, 60.seconds))
      assert(eventuallyDead(pid))
      // The completion listener destroyed the worker: Python ran no close().
      assert(events(markerDir).map(_._2) === Seq("finally 1"))
    }
  }

  test("the RecordBatch form returns the rows of its batches") {
    def results(options: (String, String)*): Seq[(Int, Long, Int)] = {
      var rows: Seq[(Int, Long, Int)] = Nil
      withLookup(options :+ ("matches" -> "3"): _*)() { (factory, _) =>
        rows = inTask() { _ =>
          val reader = factory.createReader()
          try {
            readAll(reader.lookupBatch(parameterRows(Seq(7L, -1L, 9L)))).map { r =>
              (r.ordinal, r.id, r.matchIndex)
            }
          } finally {
            reader.close()
          }
        }
      }
      rows
    }
    val expected = Seq(7L, 9L).zip(Seq(0, 2)).flatMap { case (key, ordinal) =>
      (0 until 3).map(i => (ordinal, key, i))
    }
    assert(results() === expected)
    // Batches of two rows, and of more rows than a request returns.
    assert(results("recordBatches" -> "true", "batchRows" -> "2") === expected)
    assert(results("recordBatches" -> "true", "batchRows" -> "100") === expected)
  }

  // The tuple form keeps the name of its test from before the RecordBatch form.
  Seq(false, true).foreach { recordBatches =>
    val form = if (recordBatches) ", in the RecordBatch form" else ""
    test(s"the Arrow memory of a reader returns to the baseline$form") {
      withLookup("matches" -> "1000", "recordBatches" -> recordBatches.toString)(
          SQLConf.ARROW_EXECUTION_MAX_RECORDS_PER_BATCH.key -> "10") { (factory, _) =>
        val (before, during, after) = inTask() { _ =>
          val before = ArrowUtils.rootAllocator.getAllocatedMemory
          val reader = factory.createReader()
          readAll(reader.lookupBatch(parameterRows(Seq(1L, 2L))))
          val cursor = reader.lookupBatch(parameterRows(Seq(3L)))
          cursor.next()
          val during = ArrowUtils.rootAllocator.getAllocatedMemory
          // The cursor is closed by the reader's close.
          reader.close()
          (before, during, ArrowUtils.rootAllocator.getAllocatedMemory)
        }
        assert(during > before)
        assert(after === before)
      }
    }
  }

  test("the JVM side of a lookup worker over TCP turns off Nagle's algorithm") {
    withLookup()() { (factory, _) =>
      val noDelays = inTask() { _ =>
        // A new worker, and then the same worker from the pool.
        (1 to 2).map { _ =>
          val reader = factory.createReader()
          try {
            readAll(reader.lookupBatch(parameterRows(Seq(1L))))
            val channel = currentWorker(reader).worker.channel
            channel.getLocalAddress match {
              case _: InetSocketAddress =>
                Some(channel.getOption(StandardSocketOptions.TCP_NODELAY).booleanValue())
              // A Unix domain socket has no Nagle's algorithm.
              case _ => None
            }
          } finally {
            reader.close()
          }
        }
      }
      assert(noDelays.forall(_.forall(identity)), noDelays)
    }
  }

  test("the logs of a lookup reader reach the Python worker logs of its session") {
    withLookup("behavior" -> "log")(SQLConf.PYTHON_WORKER_LOGGING_ENABLED.key -> "true") {
      (factory, _) =>
        inTask() { _ =>
          val reader = factory.createReader()
          try readAll(reader.lookupBatch(parameterRows(Seq(1L, 2L)))) finally reader.close()
        }
        // The logs are collected from the worker's output asynchronously.
        eventually(timeout(30.seconds), interval(100.millis)) {
          val logs = spark.tvf.python_worker_logs()
            .where("logger = 'lookup_test'").select("level", "msg").collect()
          assert(logs.map(r => (r.getString(0), r.getString(1))).toSeq ===
            Seq(("WARNING", "lookup of 2 rows")))
        }
    }
  }

  test("a lookup worker gets the memory limit of the task's Python workers") {
    assume(!Utils.isWindows && !Utils.isMac)
    withLookup("behavior" -> "rlimit")() { (factory, markerDir) =>
      val sc = spark.sparkContext
      // The scheduler sets this for a task when spark.executor.pyspark.memory is set, and the
      // number of tasks per executor that the memory is split by, always.
      sc.setLocalProperty(ResourceProfile.PYSPARK_MEMORY_LOCAL_PROPERTY, "32768")
      val maxTasks = try {
        inTask() { context =>
          val reader = factory.createReader()
          try readAll(reader.lookupBatch(parameterRows(Seq(1L)))) finally reader.close()
          context.getLocalProperty(ResourceProfile.MAX_TASKS_PER_EXECUTOR_LOCAL_PROPERTY).toInt
        }
      } finally {
        sc.setLocalProperty(ResourceProfile.PYSPARK_MEMORY_LOCAL_PROPERTY, null)
      }
      val limits = events(markerDir).collect {
        case (_, event) if event.startsWith("rlimit ") => event.stripPrefix("rlimit ").toLong
      }
      // The limit of a Python UDF worker of the same task, as BasePythonRunner computes it.
      val expectedMb = BasePythonRunner.getWorkerMemoryMb(Some(32768L), maxTasks).get
      assert(limits === Seq(expectedMb * 1024 * 1024))
    }
  }

  test("a failure of the Python reader during a response fails the request") {
    withLookup("matches" -> "3", "behavior" -> "fail")(
        SQLConf.ARROW_EXECUTION_MAX_RECORDS_PER_BATCH.key -> "1") { (factory, markerDir) =>
      val (rowsBefore, failure, memoryAfter) = inTask() { _ =>
        val before = ArrowUtils.rootAllocator.getAllocatedMemory
        val reader = factory.createReader()
        val cursor = reader.lookupBatch(parameterRows(Seq(1L)))
        var rows = 0
        val error = caught {
          while (cursor.next()) rows += 1
        }
        cursor.close()
        reader.close()
        (rows, error, ArrowUtils.rootAllocator.getAllocatedMemory - before)
      }
      val errorClass = failure.getClass.getName
      val error = failure.getMessage
      // The row before the failure reached the JVM, and the failure followed the response.
      assert(rowsBefore === 1)
      assert(errorClass === classOf[PythonException].getName)
      assert(error.contains("ValueError: lookup failed on purpose"), error)
      assert(memoryAfter === 0)
      // The generator was closed on the failure.
      assert(events(markerDir).map(_._2) === Seq("finally 1"))
    }
  }

  test("a planned reader that is not a lookup reader fails the initialization") {
    withLookup()() { (factory, _) =>
      // The pickle of the int 42 instead of a lookup reader.
      val notReader = SimplePythonFunction(
        command = Seq[Byte](0x80.toByte, 2, 'K'.toByte, 42, '.'.toByte),
        envVars = factory.reader.envVars,
        pythonIncludes = factory.reader.pythonIncludes,
        pythonExec = factory.reader.pythonExec,
        pythonVer = factory.reader.pythonVer,
        broadcastVars = factory.reader.broadcastVars,
        accumulator = factory.reader.accumulator)
      val badFactory = new PythonExternalLookupReaderFactory(
        notReader, factory.parameterTypes, factory.readSchema,
        factory.maxRecordsPerBatch, factory.pythonRunnerConf, factory.jobArtifactUUID,
        factory.sessionUUID)
      val error = inTask() { _ =>
        val reader = badFactory.createReader()
        try {
          caught(reader.lookupBatch(parameterRows(Seq(1L))))
        } finally {
          reader.close()
        }
      }
      checkError(
        exception = error.asInstanceOf[AnalysisException],
        condition = "PYTHON_DATA_SOURCE_ERROR",
        parameters = Map(
          "action" -> "initialize",
          "type" -> "lookup reader",
          "msg" -> "(?s).*DATA_SOURCE_TYPE_MISMATCH.*DataSourceLookupReader.*'int'.*"),
        matchPVals = true)
    }
  }

  test("the idle timeout counts only while a response is awaited") {
    withLookup("behavior" -> "sleep", "seconds" -> "4")(
        SQLConf.PYTHON_UDF_WORKER_IDLE_TIMEOUT_SECONDS.key -> "1",
        SQLConf.PYTHON_UDF_WORKER_KILL_ON_IDLE_TIMEOUT.key -> "true") { (factory, _) =>
      val error = inTask() { _ =>
        val reader = factory.createReader()
        try {
          // No request is in progress, so a gap longer than the idle timeout is no timeout.
          readAll(reader.lookupBatch(parameterRows(Seq(-1L))))
          Thread.sleep(2500)
          readAll(reader.lookupBatch(parameterRows(Seq(-1L))))
          // A lookup that sends nothing for longer than the idle timeout is killed.
          caught(readAll(reader.lookupBatch(parameterRows(Seq(1L)))))
        } finally {
          reader.close()
        }
      }
      assert(Utils.stackTraceToString(error).contains("terminated due to idle timeout"))
    }
  }
}

/** Tests of Python data source external lookups whose workers run without the daemon. */
class PythonExternalLookupSimpleWorkerSuite extends PythonExternalLookupTestBase {
  import PythonExternalLookupWorkerSuite._

  override protected def sparkConf: SparkConf =
    super.sparkConf.set("spark.python.use.daemon", "false")

  test("one simple Python worker serves every request of a reader") {
    withLookup()() { (factory, markerDir) =>
      val pids = inTask() { _ =>
        val reader = factory.createReader()
        try {
          (1 to 3).flatMap { key =>
            readAll(reader.lookupBatch(parameterRows(Seq(key.toLong)))).map(_.pid)
          }
        } finally {
          reader.close()
        }
      }
      assert(pids.length === 3 && pids.distinct.length === 1)
      // The simple worker served the graceful close, and then exited.
      assert(events(markerDir).filter(_._2 == "close") === Seq(pids.head -> "close"))
      assert(eventuallyDead(pids.head))
    }
  }

  test("a simple Python worker waits longer than the authentication timeout for requests") {
    val conf = SparkEnv.get.conf
    val authTimeout = conf.get(PYTHON_AUTH_SOCKET_TIMEOUT)
    conf.set(PYTHON_AUTH_SOCKET_TIMEOUT.key, "1s")
    try {
      withLookup()() { (factory, markerDir) =>
        val ids = inTask() { _ =>
          val reader = factory.createReader()
          try {
            // Each request, and the close, comes after a pause longer than the timeout.
            Seq(1L, 2L).flatMap { key =>
              Thread.sleep(2500)
              readAll(reader.lookupBatch(parameterRows(Seq(key)))).map(_.id)
            }
          } finally {
            Thread.sleep(2500)
            reader.close()
          }
        }
        assert(ids === Seq(1L, 2L))
        assert(events(markerDir).map(_._2) === Seq("finally 1", "finally 1", "close"))
      }
    } finally {
      conf.set(PYTHON_AUTH_SOCKET_TIMEOUT.key, s"${authTimeout}s")
    }
  }
}

object PythonExternalLookupWorkerSuite {
  /** A result row of the test source, with the ordinal of the parameter row that it answers. */
  case class LookupResult(ordinal: Int, id: Long, pid: Int, partition: Int, matchIndex: Int)

  def parameterRows(keys: Seq[Long]): Array[InternalRow] = keys.map(InternalRow(_)).toArray

  /** The failure of `f`, which must fail. A task cannot use the suite's `intercept`. */
  def caught(f: => Any): Throwable = {
    try {
      f
    } catch {
      case e: Throwable => return e
    }
    throw new IllegalStateException("Expected a failure")
  }

  /** Reads a cursor to its end, and closes it. */
  def readAll(cursor: ExternalLookupResultCursor): Seq[LookupResult] = {
    val results = mutable.ArrayBuffer.empty[LookupResult]
    try {
      while (cursor.next()) {
        val row = cursor.get()
        results += LookupResult(
          cursor.inputOrdinal(), row.getLong(0), row.getInt(1), row.getInt(2), row.getInt(3))
      }
    } finally {
      cursor.close()
    }
    results.toSeq
  }

  /** The numbers of completion and interrupt listeners of the task. */
  def listenerCounts(context: TaskContext): (Int, Int) = {
    def size(field: String): Int = {
      val f = context.getClass.getDeclaredFields.find(_.getName.endsWith(field)).get
      f.setAccessible(true)
      f.get(context).asInstanceOf[java.util.Stack[_]].size()
    }
    (size("onCompleteCallbacks"), size("onInterruptCallbacks"))
  }

  /** The current metrics of a reader, by name. */
  def metricValues(reader: ExternalLookupReader): Map[String, Long] =
    reader.currentMetricsValues().map(m => m.name() -> m.value()).toMap

  /**
   * The current worker of a reader, which is private to its package, read by reflection, as
   * `listenerCounts` reads the listeners of a task.
   */
  def currentWorker(reader: ExternalLookupReader): PythonExternalLookupWorker = {
    val field = reader.getClass.getDeclaredFields.find(_.getName.endsWith("worker")).get
    field.setAccessible(true)
    field.get(reader).asInstanceOf[PythonExternalLookupWorker]
  }

  /** Whether the process exits within 30 seconds. */
  def eventuallyDead(pid: Int): Boolean = {
    val deadline = System.nanoTime() + 30.seconds.toNanos
    while (ProcessHandle.of(pid).map(_.isAlive).orElse(false) && System.nanoTime() < deadline) {
      Thread.sleep(50)
    }
    !ProcessHandle.of(pid).map(_.isAlive).orElse(false)
  }
}
