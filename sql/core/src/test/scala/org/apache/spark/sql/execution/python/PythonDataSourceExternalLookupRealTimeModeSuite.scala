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
import java.nio.charset.StandardCharsets
import java.nio.file.Files

import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

import org.apache.spark.SparkThrowable
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.IntegratedUDFTestUtils.{createUserDefinedPythonDataSource, shouldTestPandasUDFs}
import org.apache.spark.sql.connector.CountingKeyUDF
import org.apache.spark.sql.execution.datasources.v2.LowLatencyClock
import org.apache.spark.sql.execution.streaming.runtime.StreamingQueryWrapper
import org.apache.spark.sql.execution.streaming.sources.LowLatencyMemoryStream
import org.apache.spark.sql.functions.{coalesce, concat, lit, udf}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.{ResultsCollector, StreamingQuery, StreamRealTimeModeE2ESuiteBase}
import org.apache.spark.sql.streaming.util.GlobalSingletonManualClock

/**
 * Tests of lookup joins with Python data sources in real-time mode.
 *
 * The lookup source stores two rows for key 1 and one row for key 2, with the schema
 * `id INT, department STRING, pid INT`, where `pid` is the process id of the Python worker that
 * returns the row. It allows `maxRows` parameter rows per request, and appends `request <n>
 * <pid>` to the file `events` in `markerDir` for each request of `n` parameter rows.
 */
class PythonDataSourceExternalLookupRealTimeModeSuite extends StreamRealTimeModeE2ESuiteBase {
  import testImplicits._

  private val dataSourceName = "test_python_lookup_rtm"

  private val lookupSourceScript =
    s"""
       |import os
       |from pyspark.sql.datasource import DataSource, DataSourceLookupReader, EqualTo
       |
       |DEPARTMENTS = {1: ["eng", "ops"], 2: ["sales"]}
       |
       |class TestLookupReader(DataSourceLookupReader):
       |    def __init__(self, options):
       |        self.options = options
       |
       |    def selectLookupPredicates(self, candidates):
       |        return [i for i, c in enumerate(candidates) if isinstance(c, EqualTo)]
       |
       |    def maxParameterRowsPerRequest(self):
       |        return int(self.options.get("maxRows", "1000"))
       |
       |    def lookup(self, parameters):
       |        pid = os.getpid()
       |        with open(os.path.join(self.options["markerDir"], "events"), "a") as f:
       |            f.write(f"request {len(parameters)} {pid}\\n")
       |        for ordinal, (key,) in enumerate(parameters):
       |            for department in DEPARTMENTS.get(key, []):
       |                yield ordinal, (key, department, pid)
       |
       |class $dataSourceName(DataSource):
       |    def schema(self):
       |        return "id INT, department STRING, pid INT"
       |
       |    def lookupReader(self, schema):
       |        return TestLookupReader(self.options)
       |""".stripMargin

  /** The output rows of the inner join for stream rows `ids`, as `id:department`. */
  private def joinOutputs(ids: Int*): Seq[String] = ids.flatMap {
    case 1 => Seq("1:eng", "1:ops")
    case 2 => Seq("2:sales")
    case _ => Nil
  }

  /** The requests of the lookup readers, as (number of parameter rows, process id). */
  private def requests(markerDir: File): Seq[(Int, Int)] = {
    val file = new File(markerDir, "events")
    if (!file.exists()) Seq.empty else {
      Files.readAllLines(file.toPath, StandardCharsets.UTF_8).asScala.toSeq.map { line =>
        val Array(_, rows, pid) = line.split(" ")
        rows.toInt -> pid.toInt
      }
    }
  }

  /** A clock for the real-time batches, so that a batch ends only when a test advances it. */
  private def useManualClock(): GlobalSingletonManualClock = {
    GlobalSingletonManualClock.reset()
    val clock = new GlobalSingletonManualClock()
    LowLatencyClock.setClock(clock)
    clock
  }

  /**
   * Runs `f` with the inner join of a single-partition real-time stream of ids with the lookup
   * source, on the key that `streamKey` computes from the id, and the directory of its events,
   * with the lookups of Python data sources turned on in real-time mode.
   */
  private def withJoin(
      queryName: String,
      options: (String, String)*)(
      streamKey: org.apache.spark.sql.Column => org.apache.spark.sql.Column = identity)(
      f: (LowLatencyMemoryStream[Int], StreamingQuery, File) => Unit): Unit = {
    assume(shouldTestPandasUDFs)
    spark.dataSource.registerPython(
      dataSourceName, createUserDefinedPythonDataSource(dataSourceName, lookupSourceScript))
    withTempDir { markerDir =>
      withSQLConf(
          SQLConf.PYTHON_EXTERNAL_LOOKUP_ENABLED.key -> "true",
          SQLConf.PYTHON_EXTERNAL_LOOKUP_REAL_TIME_MODE_ENABLED.key -> "true") {
        val input = LowLatencyMemoryStream.singlePartition[Int]
        val query = runStreamingQuery(queryName, joined(input, markerDir, options, streamKey))
        try {
          f(input, query, markerDir)
        } finally {
          query.stop()
        }
        assert(query.exception.isEmpty)
      }
    }
  }

  /** The inner join of the stream `input` of ids with the lookup source, as `id:department`. */
  private def joined(
      input: LowLatencyMemoryStream[Int],
      markerDir: File,
      options: Seq[(String, String)] = Nil,
      streamKey: org.apache.spark.sql.Column => org.apache.spark.sql.Column = identity)
    : DataFrame = {
    val stream = input.toDF().toDF("id")
    val lookup: DataFrame = spark.read.format(dataSourceName)
      .options(options.toMap + ("markerDir" -> markerDir.getAbsolutePath)).load()
    stream.join(lookup, streamKey(stream("id")) === lookup("id"))
      .select(concat(stream("id").cast("string"), lit(":"),
        coalesce(lookup("department"), lit("null"))))
  }

  private def outputs: Seq[String] =
    Option(ResultsCollector.get(sinkName)).map(_.asScala.toSeq).getOrElse(Nil)

  private def streamExecution(query: StreamingQuery) =
    query.asInstanceOf[StreamingQueryWrapper].streamingQuery

  /** Waits until `condition` holds, and fails at once if the query fails first. */
  private def awaitQuery(query: StreamingQuery)(condition: => Boolean): Unit = {
    eventually(timeout(60.seconds)) {
      assert(query.exception.isDefined || condition, s"outputs: $outputs")
    }
    query.exception.foreach(error => throw error)
  }

  /** Ends the current real-time batch, and waits until the next one starts. */
  private def endBatch(query: StreamingQuery, clock: GlobalSingletonManualClock): Unit = {
    val batchId = streamExecution(query).getLatestExecutionContext().batchId
    clock.advance(defaultTrigger.batchDurationMs)
    awaitQuery(query)(streamExecution(query).getLatestExecutionContext().batchId > batchId)
  }

  test("a lookup join with a Python data source runs in real-time mode only when turned on") {
    assume(shouldTestPandasUDFs)
    spark.dataSource.registerPython(
      dataSourceName, createUserDefinedPythonDataSource(dataSourceName, lookupSourceScript))
    withTempDir { markerDir =>
      // Both lookup flags are pinned, so that the test checks the opt-in, not a default.
      withSQLConf(
          SQLConf.PYTHON_EXTERNAL_LOOKUP_ENABLED.key -> "true",
          SQLConf.PYTHON_EXTERNAL_LOOKUP_REAL_TIME_MODE_ENABLED.key -> "false") {
        val input = LowLatencyMemoryStream.singlePartition[Int]
        val query = runStreamingQuery("python_lookup_rtm_off", joined(input, markerDir))
        try {
          input.addData(1)
          eventually(timeout(60.seconds)) {
            assert(query.exception.isDefined)
          }
          checkError(
            exception = query.exception.get.getCause.asInstanceOf[SparkThrowable],
            condition = "EXTERNAL_LOOKUP_JOIN_UNSUPPORTED.PYTHON_REAL_TIME_MODE",
            parameters = Map(
              "table" -> s"`$dataSourceName`",
              "config" -> "\"spark.sql.python.externalLookup.realTimeMode.enabled\""))
          assert(requests(markerDir).isEmpty)
        } finally {
          query.stop()
        }
      }
    }
  }

  test("real-time mode sends each stream row's request to Python as it reads the row") {
    val clock = useManualClock()
    // The source allows two parameter rows per request, but a real-time request takes one
    // stream row, the default, so the join sends it, and emits its output, while the batch still
    // runs.
    withSQLConf(SQLConf.JOIN_EXTERNAL_LOOKUP_REAL_TIME_MODE_MAX_BATCH_ROWS.key -> "1") {
      withJoin("python_lookup_rtm_one_row", "maxRows" -> "2")() { (input, query, markerDir) =>
        val ids = Seq(1, 3, 2, 4)
        ids.indices.foreach { n =>
          input.addData(ids(n))
          awaitQuery(query) {
            requests(markerDir).length == n + 1 &&
              outputs.sorted == joinOutputs(ids.take(n + 1): _*).sorted
          }
        }
        assert(requests(markerDir).map(_._1) === Seq(1, 1, 1, 1))
        endBatch(query, clock)
        assert(outputs.sorted === joinOutputs(ids: _*).sorted)
      }
    }
  }

  test("real-time mode fills a request of more rows before it sends it to Python") {
    withSQLConf(SQLConf.JOIN_EXTERNAL_LOOKUP_REAL_TIME_MODE_MAX_BATCH_ROWS.key -> "1000") {
      val clock = useManualClock()
      // The join computes the key of each stream row that it reads once, so the calls of the key
      // count the rows that it has read.
      val countedKey = udf { (id: Int) =>
        CountingKeyUDF.calls.incrementAndGet()
        id
      }
      CountingKeyUDF.calls.set(0)
      withJoin("python_lookup_rtm_multi_row", "maxRows" -> "2")(countedKey(_)) {
        (input, query, markerDir) =>
          // A request takes up to the two stream rows that the source allows, and its output
          // comes once it is full, before the next stream row arrives.
          input.addData(1, 3, 2)
          awaitQuery(query)(outputs.sorted == joinOutputs(1, 3).sorted)
          // The last request is not full, so the join sends it when the batch ends. The batch
          // ends only once the join has read row 2, so that the row is in this batch.
          awaitQuery(query)(CountingKeyUDF.calls.get() == 3)
          endBatch(query, clock)
          awaitQuery(query)(requests(markerDir).map(_._1) == Seq(2, 1))
          assert(outputs.sorted === joinOutputs(1, 3, 2).sorted)
      }
    }
  }

  test("one Python worker serves every request of a real-time batch") {
    useManualClock()
    // One stream row per request, and the lookup cache on, which the count of requests relies on.
    withSQLConf(
        SQLConf.JOIN_EXTERNAL_LOOKUP_REAL_TIME_MODE_MAX_BATCH_ROWS.key -> "1",
        SQLConf.JOIN_EXTERNAL_LOOKUP_CACHE_ENABLED.key -> "true") {
      withJoin("python_lookup_rtm_one_worker")() { (input, query, markerDir) =>
        val ids = Seq(1, 2, 5, 1, 2, 6)
        ids.indices.foreach { n =>
          input.addData(ids(n))
          awaitQuery(query)(outputs.sorted == joinOutputs(ids.take(n + 1): _*).sorted)
        }
        // Keys 1 and 2 come again within the task, which serves them from its cache. The four
        // other requests all reach the same worker, also while the long-running task waits for
        // input between them.
        awaitQuery(query)(requests(markerDir).length == 4)
        assert(requests(markerDir).map(_._2).distinct.length === 1)
      }
    }
  }
}
