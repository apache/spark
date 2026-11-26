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
import java.nio.file.{Files, Paths}
import java.util.concurrent.atomic.AtomicInteger

import scala.jdk.CollectionConverters._

import org.apache.spark.{SparkException, SparkThrowable}
import org.apache.spark.sql.{DataFrame, Encoders, QueryTest, Row}
import org.apache.spark.sql.IntegratedUDFTestUtils.{createUserDefinedPythonDataSource, shouldTestPandasUDFs}
import org.apache.spark.sql.execution.joins.ExternalLookupJoinExec
import org.apache.spark.sql.execution.streaming.runtime.{IncrementalExecution, MemoryStream, StreamingQueryWrapper}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.StreamingQueryException
import org.apache.spark.sql.types.StructType

/**
 * End-to-end tests of streaming lookup joins with Python data sources.
 *
 * The lookup source stores, for each `id` in [1, 20], `id % 3` rows `(id, k, "name{id}-{k}")`
 * with `k` in [0, id % 3), so an `id` that is a multiple of 3 has no row, and, with the option
 * `nullKey`, one row `(NULL, 0, "null id")`. Its schema is `id BIGINT, tier INT, name STRING`.
 * The reader selects the candidates at the positions that the Python expression of the option
 * `select` picks in `candidates`, by default every equality on `id`, and evaluates them on the
 * stored rows.
 * It appends the number of parameter rows of each request, and its other events, to the file
 * `events` in `markerDir`: `serve` at the first request of each reader, and, with the option
 * `markParameters`, each parameter row. With the option `behavior` set to `sparkFile`, the `name`
 * of each returned row is the text of the file `file`, which it finds through `SparkFiles`.
 */
class PythonDataSourceExternalLookupExecutionSuite extends PythonDataSourceSuiteBase {
  private val runCount = new AtomicInteger(0)

  private def lookupSourceScript(schema: String): String =
    s"""
       |import operator
       |import os
       |import time
       |from pyspark.sql.datasource import (
       |    DataSource, DataSourceLookupReader,
       |    EqualNullSafe, EqualTo, GreaterThan, GreaterThanOrEqual, LessThan, LessThanOrEqual)
       |
       |OPS = {
       |    EqualTo: operator.eq,
       |    GreaterThan: operator.gt,
       |    GreaterThanOrEqual: operator.ge,
       |    LessThan: operator.lt,
       |    LessThanOrEqual: operator.le,
       |}
       |COLUMNS = ["id", "tier", "name"]
       |
       |def stored_rows(options):
       |    rows = [(i, k, f"name{i}-{k}") for i in range(1, 21) for k in range(i % 3)]
       |    if options.get("nullKey") == "true":
       |        rows.append((None, 0, "null id"))
       |    return rows
       |
       |def spark_file_text(name):
       |    from pyspark import SparkFiles
       |
       |    with open(SparkFiles.get(name)) as f:
       |        return f.read()
       |
       |def holds(predicate, row, value):
       |    stored = row[COLUMNS.index(predicate.attribute[0])]
       |    if isinstance(predicate, EqualNullSafe):
       |        return stored == value
       |    return stored is not None and value is not None and OPS[type(predicate)](stored, value)
       |
       |class TestLookupReader(DataSourceLookupReader):
       |    def __init__(self, options):
       |        self.options = options
       |        self.selected = []
       |        self.served = False
       |
       |    def mark(self, event):
       |        with open(os.path.join(self.options["markerDir"], "events"), "a") as f:
       |            f.write(f"{event}\\n")
       |
       |    def selectLookupPredicates(self, candidates):
       |        select = self.options.get(
       |            "select",
       |            "[i for i, c in enumerate(candidates) if c.attribute == ('id',) "
       |            "and isinstance(c, (EqualTo, EqualNullSafe))]")
       |        positions = eval(select, {
       |            "candidates": candidates, "EqualTo": EqualTo, "EqualNullSafe": EqualNullSafe})
       |        self.selected = [candidates[i] for i in positions]
       |        return positions
       |
       |    def matches(self, parameters):
       |        rows = stored_rows(self.options)
       |        behavior = self.options.get("behavior", "")
       |        repeat = int(self.options.get("repeat", "1"))
       |        if not self.served:
       |            self.served = True
       |            self.mark("serve")
       |        self.mark(f"request {len(parameters)}")
       |        if self.options.get("markParameters") == "true":
       |            for values in parameters:
       |                self.mark(f"parameter {values}")
       |        yielded = 0
       |        try:
       |            for ordinal, values in enumerate(parameters):
       |                for row in rows:
       |                    if all(holds(p, row, v) for p, v in zip(self.selected, values)):
       |                        for _ in range(repeat):
       |                            if behavior == "fail":
       |                                raise RuntimeError("lookup failed on purpose")
       |                            if behavior == "badOrdinal":
       |                                ordinal = len(parameters)
       |                            if behavior == "nullTier":
       |                                row = (row[0], None, row[2])
       |                            if behavior == "sparkFile":
       |                                row = (*row[:2], spark_file_text(self.options["file"]))
       |                            yield ordinal, row
       |                            yielded += 1
       |        finally:
       |            self.mark(f"finally {yielded}")
       |
       |    def lookup(self, parameters):
       |        return self.matches(parameters)
       |
       |    def close(self):
       |        self.mark("close")
       |        if self.options.get("closeBehavior") == "fail":
       |            raise RuntimeError("close failed on purpose")
       |        if self.options.get("closeBehavior") == "block":
       |            time.sleep(30)
       |
       |class $dataSourceName(DataSource):
       |    def schema(self):
       |        return "$schema"
       |
       |    def lookupReader(self, schema):
       |        return TestLookupReader(self.options)
       |""".stripMargin

  /** The stored rows of the lookup source, as `stored_rows` in Python builds them. */
  private def storedRows(nullKey: Boolean = false): Seq[(java.lang.Long, Int, String)] =
    (1 to 20).flatMap(i => (0 until i % 3).map(k => (Long.box(i.toLong), k, s"name$i-$k"))) ++
      (if (nullKey) Seq((null, 0, "null id")) else Seq.empty)

  private case class StreamRun(rows: Seq[Row], execution: IncrementalExecution) {
    def lookupJoin: ExternalLookupJoinExec = execution.executedPlan.collectFirst {
      case exec: ExternalLookupJoinExec => exec
    }.get
  }

  /**
   * Runs `f` with the lookup source registered, a lookup table with the options, and a new
   * directory for the events of its readers.
   */
  private def withLookupSource(
      schema: String = "id BIGINT, tier INT, name STRING",
      confs: Seq[(String, String)] = Seq.empty)(
      options: (String, String)*)(f: (DataFrame, File) => Unit): Unit = {
    assume(shouldTestPandasUDFs)
    spark.dataSource.registerPython(
      dataSourceName, createUserDefinedPythonDataSource(dataSourceName, lookupSourceScript(schema)))
    withTempDir { markerDir =>
      withSQLConf(Seq(SQLConf.PYTHON_EXTERNAL_LOOKUP_ENABLED.key -> "true") ++ confs: _*) {
        val lookup = spark.read.format(dataSourceName)
          .options(options.toMap + ("markerDir" -> markerDir.getAbsolutePath)).load()
        f(lookup, markerDir)
      }
    }
  }

  private def events(markerDir: File): Seq[String] =
    Files.readAllLines(new File(markerDir, "events").toPath, StandardCharsets.UTF_8).asScala.toSeq

  /** A stream of the rows, in one micro-batch of `numPartitions` partitions. */
  private def streamOf(schema: StructType, rows: Seq[Row], numPartitions: Int = 2): DataFrame = {
    val input = MemoryStream[Row](numPartitions)(Encoders.row(schema), spark.sqlContext)
    input.addData(rows)
    input.toDF()
  }

  private def idStream(ids: Option[Long]*): DataFrame =
    streamOf(new StructType().add("sid", "bigint"), ids.map(id => Row(id.map(Long.box).orNull)))

  private def runStream(df: DataFrame): StreamRun = {
    val queryName = s"python_lookup_stream_${runCount.incrementAndGet()}"
    val query = df.writeStream.format("memory").queryName(queryName).start()
    try {
      query.processAllAvailable()
      StreamRun(
        spark.table(queryName).collect().toSeq,
        query.asInstanceOf[StreamingQueryWrapper].streamingQuery.lastExecution)
    } finally {
      query.stop()
      spark.catalog.dropTempView(queryName)
    }
  }

  private def checkStream(df: DataFrame, expected: Seq[Row]): StreamRun = {
    val run = runStream(df)
    QueryTest.sameRows(expected, run.rows).foreach(fail(_))
    run
  }

  /** The error that the streaming `df` fails with. */
  private def streamFailure(df: DataFrame): Throwable = {
    try {
      runStream(df)
      fail("The streaming query did not fail")
    } catch {
      case e: StreamingQueryException => e.getCause
      case e: Throwable with SparkThrowable => e
    }
  }

  private val ids = Seq(1L, 2L, 3L, 5L, 2L, 30L).map(Some(_)) :+ None

  test("lookups join a stream, for each join type") {
    withLookupSource()() { (lookup, markerDir) =>
      def joined(joinType: String): DataFrame = {
        val stream = idStream(ids: _*)
        stream.join(lookup, stream("sid") === lookup("id"), joinType)
          .select(stream("sid"), lookup("tier"), lookup("name"))
      }
      val matches = ids.map { sid =>
        sid -> storedRows().filter(r => sid.contains(r._1.longValue()))
      }
      checkStream(joined("inner"), matches.flatMap { case (sid, rows) =>
        rows.map(r => Row(sid.get, r._2, r._3))
      })
      checkStream(joined("left_outer"), matches.flatMap {
        case (sid, Seq()) => Seq(Row(sid.orNull, null, null))
        case (sid, rows) => rows.map(r => Row(sid.get, r._2, r._3))
      })
      def stream(joinType: String): DataFrame = {
        val stream = idStream(ids: _*)
        stream.join(lookup, stream("sid") === lookup("id"), joinType)
      }
      checkStream(stream("left_semi"),
        matches.collect { case (sid, rows) if rows.nonEmpty => Row(sid.get) })
      checkStream(stream("left_anti"),
        matches.collect { case (sid, Seq()) => Row(sid.orNull) })
      // Every Python reader that served a request was closed gracefully, once.
      val readerEvents = events(markerDir)
      assert(readerEvents.contains("serve"))
      assert(readerEvents.count(_ == "close") === readerEvents.count(_ == "serve"))
    }
  }

  test("null-safe keys, constant keys and ranges") {
    withLookupSource()("nullKey" -> "true") { (lookup, _) =>
      val stream = idStream(Some(1L), None, Some(2L))
      checkStream(
        stream.join(lookup, stream("sid") <=> lookup("id")).select(stream("sid"), col("name")),
        Seq(Row(1L, "name1-0"), Row(null, "null id"), Row(2L, "name2-0"), Row(2L, "name2-1")))
    }
    // The constant tier of a filter is offered as a candidate, and selected with the key: each
    // parameter row holds the key and the tier, in the order of the selected positions.
    val byColumn = "sorted(range(len(candidates)), key=lambda i: candidates[i].attribute)"
    withLookupSource()("select" -> byColumn, "markParameters" -> "true") { (lookup, markerDir) =>
      val stream = idStream(Some(2L), Some(5L), Some(4L))
      checkStream(
        stream.join(lookup.filter("tier = 1"), stream("sid") === lookup("id"))
          .select(stream("sid"), col("name")),
        Seq(Row(2L, "name2-1"), Row(5L, "name5-1")))
      assert(events(markerDir).filter(_.startsWith("parameter ")).toSet ===
        Set("parameter (2, 1)", "parameter (5, 1)", "parameter (4, 1)"))
    }
    // A selected range on a stream column gives one parameter row per request.
    withLookupSource()("select" -> "range(len(candidates))") { (lookup, markerDir) =>
      val schema = new StructType().add("sid", "bigint").add("minTier", "int")
      val stream = streamOf(schema, Seq(Row(2L, 1), Row(5L, 0), Row(4L, 2)), numPartitions = 1)
      checkStream(
        stream.join(lookup,
          stream("sid") === lookup("id") && lookup("tier") >= stream("minTier"))
          .select(stream("sid"), col("name")),
        Seq(Row(2L, "name2-1"), Row(5L, "name5-0"), Row(5L, "name5-1")))
      assert(events(markerDir).filter(_.startsWith("request ")).toSet === Set("request 1"))
    }
  }

  test("each type of lookup key reaches Python as its Python type, through the JVM") {
    assume(shouldTestPandasUDFs)
    val sourceName = "TypedLookupSource"
    val columns = Seq("b", "i", "d", "ts", "ntz", "s", "bin", "dec")
    val script =
      s"""
         |import datetime
         |from pyspark.sql.datasource import DataSource, DataSourceLookupReader
         |
         |COLUMNS = ["b", "i", "d", "ts", "ntz", "s", "bin", "dec", "types"]
         |
         |class TypedLookupReader(DataSourceLookupReader):
         |    def selectLookupPredicates(self, candidates):
         |        self.columns = [c.attribute[0] for c in candidates]
         |        return list(range(len(candidates)))
         |
         |    def lookup(self, parameters):
         |        # Each parameter row is returned as the stored row, so the join keeps it only if
         |        # its values reach Python and come back unchanged, with their Python types.
         |        for ordinal, values in enumerate(parameters):
         |            row = dict(zip(self.columns, values))
         |            row["types"] = ",".join(sorted(
         |                f"{c}:{type(v).__name__}"
         |                + ("-naive" if isinstance(v, datetime.datetime) and v.tzinfo is None
         |                   else "")
         |                for c, v in zip(self.columns, values)))
         |            yield ordinal, tuple(row.get(c) for c in COLUMNS)
         |
         |class $sourceName(DataSource):
         |    def schema(self):
         |        return ("b BOOLEAN, i INT, d DATE, ts TIMESTAMP, ntz TIMESTAMP_NTZ, s STRING, "
         |                "bin BINARY, dec DECIMAL(10, 2), types STRING")
         |
         |    def lookupReader(self, schema):
         |        return TypedLookupReader()
         |""".stripMargin
    spark.dataSource.registerPython(
      sourceName, createUserDefinedPythonDataSource(sourceName, script))
    val schema = new StructType().add("k_b", "boolean").add("k_i", "int").add("k_d", "date")
      .add("k_ts", "timestamp").add("k_ntz", "timestamp_ntz").add("k_s", "string")
      .add("k_bin", "binary").add("k_dec", "decimal(10, 2)")
    val key = Row(true, 7, java.sql.Date.valueOf("2026-03-15"),
      java.sql.Timestamp.from(java.time.Instant.parse("2026-03-15T12:34:56.123456Z")),
      java.time.LocalDateTime.parse("2026-03-15T01:30:00.654321"), "key",
      Array[Byte](0, 1, 2), new java.math.BigDecimal("12.34"))
    Seq(true, false).foreach { binaryAsBytes =>
      withSQLConf(
          SQLConf.PYTHON_EXTERNAL_LOOKUP_ENABLED.key -> "true",
          SQLConf.PYSPARK_BINARY_AS_BYTES.key -> binaryAsBytes.toString) {
        val lookup = spark.read.format(sourceName).load()
        val stream = streamOf(schema, Seq(key), numPartitions = 1)
        val joined = stream.join(lookup,
          columns.map(c => stream(s"k_$c") === lookup(c)).reduce(_ && _))
        val binaryType = if (binaryAsBytes) "bytes" else "bytearray"
        checkStream(joined.select(lookup("types")), Seq(Row(
          s"b:bool,bin:$binaryType,d:date,dec:Decimal,i:int,ntz:datetime-naive,s:str," +
            "ts:datetime-naive")))
      }
    }
  }

  test("a semi join reads no further than its answer") {
    // Each key matches 5000 rows, in batches of 10, and the first match decides a stream row.
    withLookupSource(confs = Seq(SQLConf.ARROW_EXECUTION_MAX_RECORDS_PER_BATCH.key -> "10"))(
        "repeat" -> "5000") { (lookup, markerDir) =>
      val stream = idStream(Some(1L))
      checkStream(stream.join(lookup, stream("sid") === lookup("id"), "left_semi"),
        Seq(Row(1L)))
      val yielded = events(markerDir).collect {
        case event if event.startsWith("finally ") => event.stripPrefix("finally ").toInt
      }
      assert(yielded.length === 1 && yielded.head < 5000, yielded)
      assert(events(markerDir).last === "close")
    }
  }

  test("results that break the contract fail the query") {
    withLookupSource()("behavior" -> "badOrdinal") { (lookup, _) =>
      val stream = idStream(Some(1L))
      checkError(
        exception = streamFailure(stream.join(lookup, stream("sid") === lookup("id")))
          .asInstanceOf[SparkThrowable],
        condition = "DATA_SOURCE_INVALID_EXTERNAL_LOOKUP.INVALID_INPUT_ORDINAL",
        parameters = Map(
          "table" -> s"`$dataSourceName`", "ordinal" -> "1", "numParameterRows" -> "1"))
    }
    withLookupSource(schema = "id BIGINT, tier INT NOT NULL, name STRING")(
        "behavior" -> "nullTier") { (lookup, _) =>
      val stream = idStream(Some(1L))
      checkError(
        exception = streamFailure(stream.join(lookup, stream("sid") === lookup("id")))
          .asInstanceOf[SparkThrowable],
        condition = "DATA_SOURCE_INVALID_EXTERNAL_LOOKUP.NULL_IN_NOT_NULL_FIELD",
        parameters = Map("table" -> s"`$dataSourceName`", "field" -> "`tier`"))
    }
    withLookupSource()("behavior" -> "fail") { (lookup, _) =>
      val stream = idStream(Some(1L))
      val failure = streamFailure(stream.join(lookup, stream("sid") === lookup("id")))
      val message = failure match {
        case e: SparkException => Option(e.getCause).map(_.getMessage).getOrElse(e.getMessage)
        case e => e.getMessage
      }
      assert(message.contains("RuntimeError: lookup failed on purpose"), message)
    }
  }

  test("a close() that fails does not hide an earlier failure of the lookup") {
    Seq("fail", "block").foreach { closeBehavior =>
      withLookupSource()("behavior" -> "badOrdinal", "closeBehavior" -> closeBehavior) {
          (lookup, markerDir) =>
        val stream = idStream(Some(1L))
        val failure = streamFailure(stream.join(lookup, stream("sid") === lookup("id")))
        checkError(
          exception = failure.asInstanceOf[SparkThrowable],
          condition = "DATA_SOURCE_INVALID_EXTERNAL_LOOKUP.INVALID_INPUT_ORDINAL",
          parameters = Map(
            "table" -> s"`$dataSourceName`", "ordinal" -> "1", "numParameterRows" -> "1"))
        // The reader was closed after the failure, and its close() failed too.
        assert(events(markerDir).contains("close"), closeBehavior)
      }
    }
  }

  test("a limit above a lookup join ends its readers gracefully") {
    withLookupSource(confs = Seq(SQLConf.ARROW_EXECUTION_MAX_RECORDS_PER_BATCH.key -> "10"))(
        "repeat" -> "5000") { (lookup, markerDir) =>
      val stream = idStream(Some(1L))
      val run = runStream(stream.join(lookup, stream("sid") === lookup("id")).limit(3))
      assert(run.rows.length === 3)
      // The open response was cancelled at the end of the task, and the reader then closed.
      val readerEvents = events(markerDir)
      assert(readerEvents.last === "close")
      val yielded = readerEvents.collect {
        case event if event.startsWith("finally ") => event.stripPrefix("finally ").toInt
      }
      assert(yielded.length === 1 && yielded.head < 5000, yielded)
    }
  }

  test("lookups read the files of the session through SparkFiles") {
    // A file of the session, as a Spark Connect client adds it. The lookup is planned with each
    // micro-batch, before Spark sets the artifacts of the session for the job of the batch.
    withTempDir { dir =>
      val staged = new File(dir, "lookup_session_file.txt")
      Files.write(staged.toPath, "from the session".getBytes(StandardCharsets.UTF_8))
      spark.artifactManager.addArtifact(
        Paths.get("files", staged.getName), staged.toPath, None, deleteStagedFile = false)
      try {
        withLookupSource()("behavior" -> "sparkFile", "file" -> staged.getName) { (lookup, _) =>
          val stream = idStream(Some(1L))
          checkStream(
            stream.join(lookup, stream("sid") === lookup("id")).select(stream("sid"), col("name")),
            Seq(Row(1L, "from the session")))
        }
      } finally {
        // The suite shares its session, whose streaming queries copy its files, so the file must
        // not outlive this test.
        spark.artifactManager.cleanUpResourcesForTesting()
      }
    }
  }

  test("the lookup join shows the bytes sent to and received from Python") {
    withLookupSource()() { (lookup, _) =>
      val stream = idStream(ids: _*)
      val run = runStream(stream.join(lookup, stream("sid") === lookup("id")))
      val metrics = run.lookupJoin.metrics
      assert(metrics(ExternalLookupJoinExec.sourceMetricKey("pythonDataSent")).value > 0)
      assert(metrics(ExternalLookupJoinExec.sourceMetricKey("pythonDataReceived")).value > 0)
    }
  }
}
