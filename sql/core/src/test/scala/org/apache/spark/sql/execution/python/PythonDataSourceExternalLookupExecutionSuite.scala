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

import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

import org.apache.spark.{SparkException, SparkThrowable}
import org.apache.spark.sql.{DataFrame, Encoders, QueryTest, Row}
import org.apache.spark.sql.IntegratedUDFTestUtils.{createUserDefinedPythonDataSource, shouldTestPandasUDFs}
import org.apache.spark.sql.execution.joins.ExternalLookupJoinExec
import org.apache.spark.sql.execution.streaming.runtime.{IncrementalExecution, MemoryStream, StreamingQueryWrapper}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.{StreamingQuery, StreamingQueryException}
import org.apache.spark.sql.types.StructType

/**
 * End-to-end tests of streaming lookup joins with Python data sources.
 *
 * The lookup source stores, for each `id` in [1, 20], `id % 3` rows `(id, k, "name{id}-{k}")`
 * with `k` in [0, id % 3), so an `id` that is a multiple of 3 has no row, and, with the option
 * `nullKey`, one row `(NULL, 0, "null id")`. Its schema is `id BIGINT, tier INT, name STRING`.
 * The reader selects the candidates at the positions that the Python expression of the option
 * `select` picks in `candidates`, by default every equality on `id`, and evaluates them on the
 * stored rows. It allows `maxRows` parameter rows per request, by default 1000. It appends
 * `plan` for each planning call, the number of parameter rows of each request, and its other
 * events, to the file `events` in `markerDir`: `serve` at the first request of each reader, and,
 * with the option `markParameters`, each parameter row. With the option `behavior` set to
 * `sparkFile`, the `name` of each returned row is the text of the file `file`, which it finds
 * through `SparkFiles`. With the option `recordBatches`, it returns the same rows as
 * `pyarrow.RecordBatch`es of `batchRows` rows, by default 10, with the ordinal first.
 */
class PythonDataSourceExternalLookupExecutionSuite extends PythonDataSourceSuiteBase {
  private val runCount = new AtomicInteger(0)

  private def lookupSourceScript(schema: String): String =
    s"""
       |import operator
       |import os
       |import time
       |from pyspark.sql.datasource import (
       |    DataSource, DataSourceLookupReader, EqualNullSafe, EqualTo, GreaterThan,
       |    GreaterThanOrEqual, LessThan, LessThanOrEqual)
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
       |def mark(options, event):
       |    with open(os.path.join(options["markerDir"], "events"), "a") as f:
       |        f.write(f"{event}\\n")
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
       |        self.candidates = []
       |        self.positions = []
       |        self.served = False
       |
       |    def mark(self, event):
       |        mark(self.options, event)
       |
       |    def selectLookupPredicates(self, candidates):
       |        self.mark("plan")
       |        select = self.options.get(
       |            "select",
       |            "[i for i, c in enumerate(candidates) if c.attribute == ('id',) "
       |            "and isinstance(c, (EqualTo, EqualNullSafe))]")
       |        positions = eval(select, {
       |            "candidates": candidates, "EqualTo": EqualTo, "EqualNullSafe": EqualNullSafe})
       |        # Kept as returned: Spark rejects an invalid selection before any lookup.
       |        self.candidates = candidates
       |        self.positions = positions
       |        return positions
       |
       |    def maxParameterRowsPerRequest(self):
       |        return eval(self.options.get("maxRows", "1000"))
       |
       |    def matches(self, parameters):
       |        rows = stored_rows(self.options)
       |        selected = [self.candidates[i] for i in self.positions]
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
       |                    if all(holds(p, row, v) for p, v in zip(selected, values)):
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
       |        pairs = self.matches(parameters)
       |        if self.options.get("recordBatches") != "true":
       |            return pairs
       |        return self.record_batches(pairs)
       |
       |    def record_batches(self, pairs):
       |        import pyarrow as pa
       |        from itertools import islice
       |        types = [pa.int64(), pa.int32(), pa.string()]
       |        try:
       |            while True:
       |                chunk = list(islice(pairs, int(self.options.get("batchRows", "10"))))
       |                if not chunk:
       |                    return
       |                yield pa.RecordBatch.from_arrays(
       |                    [pa.array([o for o, _ in chunk], pa.int64())] +
       |                    [pa.array([r[i] for _, r in chunk], t) for i, t in enumerate(types)],
       |                    names=["ordinal"] + COLUMNS)
       |        finally:
       |            pairs.close()
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

  // The configurations that the assertions of the tests depend on, which a test may override:
  // lookups on, and Python pushdown off, so that a plan is reused across micro-batches.
  private val pinnedConfs = Seq(
    SQLConf.PYTHON_EXTERNAL_LOOKUP_ENABLED.key -> "true",
    SQLConf.PYTHON_FILTER_PUSHDOWN_ENABLED.key -> "false",
    SQLConf.PYTHON_LIMIT_PUSHDOWN_ENABLED.key -> "false")

  /**
   * Runs `f` with the lookup source registered, a lookup table with the options, and a new
   * directory for the events of its readers, with `pinnedConfs` unless `pinned` is false, and
   * then `confs`.
   */
  private def withLookupSource(
      schema: String = "id BIGINT, tier INT, name STRING",
      confs: Seq[(String, String)] = Seq.empty,
      pinned: Boolean = true)(
      options: (String, String)*)(f: (DataFrame, File) => Unit): Unit = {
    assume(shouldTestPandasUDFs)
    spark.dataSource.registerPython(
      dataSourceName, createUserDefinedPythonDataSource(dataSourceName, lookupSourceScript(schema)))
    withTempDir { markerDir =>
      withSQLConf((if (pinned) pinnedConfs else Nil) ++ confs: _*) {
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

  /**
   * Runs the lookup join of `lookup` with a one-partition stream of `sid BIGINT`, in one
   * micro-batch per element of `batches`, and returns the rows of each micro-batch. `check` runs
   * after the query has started, before it has any input.
   */
  private def runBatches(
      lookup: DataFrame,
      batches: Seq[Seq[Long]],
      joinType: String = "inner")(check: StreamingQuery => Unit = _ => ()): Seq[Seq[Row]] = {
    val input = MemoryStream[Long](1)(Encoders.scalaLong, spark.sqlContext)
    val stream = input.toDF().toDF("sid")
    val joined = stream.join(lookup, stream("sid") === lookup("id"), joinType)
      .select(stream("sid"), lookup("name"))
    val queryName = s"python_lookup_batches_${runCount.incrementAndGet()}"
    val query = joined.writeStream.format("memory").queryName(queryName).start()
    try {
      check(query)
      var seen = 0
      batches.map { batch =>
        input.addData(batch)
        query.processAllAvailable()
        val rows = spark.table(queryName).collect().toSeq
        try rows.drop(seen) finally seen = rows.length
      }
    } finally {
      query.stop()
      spark.catalog.dropTempView(queryName)
    }
  }

  /** The rows of the inner join for the stream rows `sids`. */
  private def innerJoin(sids: Long*): Seq[Row] = sids.flatMap { sid =>
    storedRows().filter(_._1.longValue() == sid).map(r => Row(sid, r._3))
  }

  /** The number of events of each kind among the reader's events: `plan`, `request 2`, ... */
  private def eventCounts(markerDir: File): Map[String, Int] =
    events(markerDir).groupBy(identity).view.mapValues(_.length).toMap

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

  test("Python data sources serve lookups by default") {
    assume(shouldTestPandasUDFs)
    // No configuration is set: lookups are enabled by default.
    assert(spark.conf.get(SQLConf.PYTHON_EXTERNAL_LOOKUP_ENABLED.key) === "true")
    withLookupSource(pinned = false)() { (lookup, _) =>
      checkStream(idStream(Some(1L)).join(lookup, col("sid") === lookup("id"))
        .select(col("sid"), col("name")), Seq(Row(1L, "name1-0")))
    }
  }

  test("a streaming query plans its lookup in Python once, when it starts") {
    withLookupSource()() { (lookup, markerDir) =>
      val batches = Seq(Seq(1L, 2L), Seq(4L, 3L), Seq(5L, 1L))
      val rows = runBatches(lookup, batches) { query =>
        // The startup check plans the lookup before the first micro-batch, which has no input
        // yet, and fills the planning cache that the micro-batches use.
        eventually(timeout(60.seconds)) {
          assert(new File(markerDir, "events").exists() && events(markerDir) === Seq("plan"))
        }
        assert(query.recentProgress.forall(_.numInputRows == 0))
      }
      assert(rows.map(_.toSet) === batches.map(batch => innerJoin(batch: _*).toSet))
      assert(eventCounts(markerDir)("plan") === 1)
      assert(eventCounts(markerDir)("request 2") === 3)
    }
  }

  test("lookups reuse a plan unless the planning pushes filters, as batch reads do") {
    withLookupSource(confs = Seq(
        SQLConf.UNSUPPORTED_OPERATION_CHECK_ENABLED.key -> "false"))() { (lookup, markerDir) =>
      val stream = MemoryStream[Long](1)(Encoders.scalaLong, spark.sqlContext).toDF().toDF("sid")
      def plans(nullSafe: Boolean = false, filtered: Boolean = false): Int = {
        val side = if (filtered) lookup.filter("tier = 1") else lookup
        val condition = if (nullSafe) {
          stream("sid") <=> side("id")
        } else {
          stream("sid") === side("id")
        }
        stream.join(side, condition).queryExecution.optimizedPlan
        eventCounts(markerDir)("plan")
      }
      // With both pushdowns off, as by default, a plan is reused for the same candidates, and
      // another candidate plans again.
      assert(plans() === 1)
      assert(plans() === 1)
      assert(plans(nullSafe = true) === 2)
      assert(plans(nullSafe = true) === 2)
      // With filter pushdown on, a planning that pushes a filter into the lookup reader plans
      // again each time, and one without a lookup-side filter reuses its plan. A change of the
      // configuration plans again, once.
      withSQLConf(SQLConf.PYTHON_FILTER_PUSHDOWN_ENABLED.key -> "true") {
        assert(plans(filtered = true) === 3)
        assert(plans(filtered = true) === 4)
        assert(plans() === 5)
        assert(plans() === 5)
      }
      // Spark pushes no limit into a lookup, so with limit pushdown on, a plan is reused.
      withSQLConf(SQLConf.PYTHON_LIMIT_PUSHDOWN_ENABLED.key -> "true") {
        assert(plans() === 6)
        assert(plans() === 6)
      }
      // Turned off again, the plan of the first call is reused.
      assert(plans() === 6)
    }
  }

  test("micro-batches send no request for a key that the lookup cache holds") {
    // Two stream rows per request. Within a micro-batch, the repeated keys 1 and 2 of the second
    // request come from the lookup cache, so the request is not sent; each micro-batch starts
    // with an empty cache.
    val batches = Seq(Seq(1L, 2L, 1L, 2L, 4L), Seq(1L, 2L, 1L, 2L, 4L))
    Seq(true -> Seq("request 2", "request 1"), false -> Seq("request 2", "request 2", "request 1"))
      .foreach { case (cacheEnabled, requestsPerBatch) =>
        withLookupSource(confs = Seq(
            SQLConf.JOIN_EXTERNAL_LOOKUP_CACHE_ENABLED.key -> cacheEnabled.toString))(
            "maxRows" -> "2") { (lookup, markerDir) =>
          val rows = runBatches(lookup, batches)()
          assert(rows.map(_.toSet) === batches.map(batch => innerJoin(batch: _*).toSet))
          assert(events(markerDir).filter(_.startsWith("request ")) ===
            requestsPerBatch ++ requestsPerBatch, s"cache enabled: $cacheEnabled")
        }
      }
  }

  test("a selected range on a stream column gives one parameter row per request") {
    withLookupSource()("select" -> "range(len(candidates))") { (lookup, markerDir) =>
      val input = MemoryStream[(Long, Int)](1)(
        Encoders.tuple(Encoders.scalaLong, Encoders.scalaInt), spark.sqlContext)
      val stream = input.toDF().toDF("sid", "minTier")
      val joined = stream.join(lookup,
        stream("sid") === lookup("id") && lookup("tier") >= stream("minTier"))
        .select(stream("sid"), lookup("name"))
      val queryName = s"python_lookup_ranges_${runCount.incrementAndGet()}"
      val query = joined.writeStream.format("memory").queryName(queryName).start()
      try {
        input.addData((2L, 1), (5L, 0), (4L, 2))
        query.processAllAvailable()
        input.addData((2L, 0), (8L, 1))
        query.processAllAvailable()
        checkAnswer(spark.table(queryName), Seq(Row(2L, "name2-1"), Row(5L, "name5-0"),
          Row(5L, "name5-1"), Row(2L, "name2-0"), Row(2L, "name2-1"), Row(8L, "name8-1")))
      } finally {
        query.stop()
        spark.catalog.dropTempView(queryName)
      }
      assert(eventCounts(markerDir).keySet.filter(_.startsWith("request ")) === Set("request 1"))
      assert(eventCounts(markerDir)("request 1") === 5)
    }
  }

  test("the startup check stops a query whose lookup cannot be planned, before a micro-batch") {
    // An empty selection breaks the lookup join's own contract, which the startup check reports.
    withLookupSource()("select" -> "[]") { (lookup, markerDir) =>
      val input = MemoryStream[Long](1)(Encoders.scalaLong, spark.sqlContext)
      val stream = input.toDF().toDF("sid")
      val query = stream.join(lookup, stream("sid") === lookup("id"))
        .writeStream.format("memory").queryName(s"python_lookup_start_${runCount.get()}").start()
      try {
        eventually(timeout(60.seconds)) {
          assert(query.exception.isDefined)
        }
        checkError(
          exception = query.exception.get.getCause.asInstanceOf[SparkThrowable],
          condition = "EXTERNAL_LOOKUP_JOIN_UNSUPPORTED.NO_LOOKUP_PREDICATE",
          parameters = Map("table" -> s"`$dataSourceName`"))
        assert(query.recentProgress.forall(_.numInputRows == 0))
        assert(events(markerDir) === Seq("plan"))
      } finally {
        query.stop()
      }
    }
    // A broken contract of the lookup API that Python finds stops the query when it starts too,
    // with the error of a Scala connector.
    Seq(
      Seq("select" -> "[5]") ->
        "DATA_SOURCE_INVALID_EXTERNAL_LOOKUP.INVALID_SELECTED_PREDICATE_POSITION",
      Seq("select" -> "candidates[0]") -> "DATA_SOURCE_INVALID_EXTERNAL_LOOKUP.INVALID_RESULT_TYPE",
      Seq("maxRows" -> "True") -> "DATA_SOURCE_INVALID_EXTERNAL_LOOKUP.INVALID_RESULT_TYPE"
    ).foreach { case (options, condition) =>
      withLookupSource()(options: _*) { (lookup, markerDir) =>
        val input = MemoryStream[Long](1)(Encoders.scalaLong, spark.sqlContext)
        val stream = input.toDF().toDF("sid")
        val query = stream.join(lookup, stream("sid") === lookup("id"))
          .writeStream.format("memory").queryName(s"python_lookup_start_${runCount.get()}")
          .start()
        try {
          eventually(timeout(60.seconds)) {
            assert(query.exception.isDefined)
          }
          assert(query.exception.get.getCause.asInstanceOf[SparkThrowable].getCondition ===
            condition)
          assert(query.recentProgress.forall(_.numInputRows == 0))
          assert(events(markerDir) === Seq("plan"))
        } finally {
          query.stop()
        }
      }
    }
    // An exception that the Python reader raises while Spark plans the lookup is left to the
    // first micro-batch, as for a Scala connector, so the query runs until it has input.
    withLookupSource()("select" -> "1 / 0") { (lookup, markerDir) =>
      val failure = intercept[StreamingQueryException] {
        runBatches(lookup, Seq(Seq(1L))) { query =>
          eventually(timeout(60.seconds)) {
            assert(new File(markerDir, "events").exists())
          }
          assert(query.isActive && query.exception.isEmpty)
        }
      }
      checkError(
        exception = failure.getCause.asInstanceOf[SparkThrowable],
        condition = "PYTHON_DATA_SOURCE_ERROR",
        parameters = Map(
          "action" -> "plan", "type" -> "lookup", "msg" -> "(?s).*ZeroDivisionError.*"),
        matchPVals = true)
    }
  }

  test("the RecordBatch form gives the join results of the tuple form") {
    // For each form, the rows of each join: the four join types, a null-safe key, and a selected
    // range on a stream column.
    def joins(recordBatches: Boolean): Seq[Seq[Row]] = {
      var results = Seq.empty[Seq[Row]]
      withLookupSource()("recordBatches" -> recordBatches.toString, "nullKey" -> "true") {
        (lookup, _) =>
          results = Seq("inner", "left_outer", "left_semi", "left_anti").map { joinType =>
            val stream = idStream(ids: _*)
            runStream(stream.join(lookup, stream("sid") === lookup("id"), joinType)).rows
          }
          val stream = idStream(Some(1L), None, Some(2L))
          results :+= runStream(stream.join(lookup, stream("sid") <=> lookup("id"))).rows
      }
      withLookupSource()(
          "recordBatches" -> recordBatches.toString, "select" -> "range(len(candidates))") {
        (lookup, _) =>
          val schema = new StructType().add("sid", "bigint").add("minTier", "int")
          val stream = streamOf(schema, Seq(Row(2L, 1), Row(5L, 0), Row(4L, 2)), numPartitions = 1)
          results :+= runStream(stream.join(lookup,
            stream("sid") === lookup("id") && lookup("tier") >= stream("minTier"))).rows
      }
      results.map(_.sortBy(_.toString))
    }
    val tupleRows = joins(recordBatches = false)
    assert(joins(recordBatches = true) === tupleRows)
    assert(tupleRows.head.map(_.getAs[String]("name")).sorted === ids.flatten.flatMap { sid =>
      storedRows().filter(_._1.longValue() == sid).map(_._3)
    }.sorted)
  }

  test("a semi join reads no further than its answer, in the RecordBatch form") {
    // Each key matches 5000 rows, in batches of 10, and the first match decides a stream row.
    withLookupSource()("recordBatches" -> "true", "repeat" -> "5000") { (lookup, markerDir) =>
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
}
