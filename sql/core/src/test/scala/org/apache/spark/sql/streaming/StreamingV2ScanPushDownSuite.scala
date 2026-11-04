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

package org.apache.spark.sql.streaming

import java.util
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicInteger

import scala.jdk.CollectionConverters._

import org.apache.logging.log4j.Level
import org.scalatest.BeforeAndAfter

import org.apache.spark.SparkException
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.catalyst.{InternalRow, ProjectingInternalRow}
import org.apache.spark.sql.catalyst.expressions.{Alias, And}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, Project}
import org.apache.spark.sql.catalyst.streaming.StreamingRelationV2
import org.apache.spark.sql.catalyst.types.DataTypeUtils
import org.apache.spark.sql.classic.Dataset
import org.apache.spark.sql.connector.catalog.{Identifier, InMemoryBaseTable, InMemoryTableCatalog, SupportsRead, Table, TableCapability}
import org.apache.spark.sql.connector.read.{InputPartition, Scan, ScanBuilder, SupportsPushDownFilters, SupportsPushDownRequiredColumns}
import org.apache.spark.sql.connector.read.streaming.{ContinuousPartitionReader, ContinuousPartitionReaderFactory, ContinuousStream, MicroBatchStream, Offset, PartitionOffset}
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2Relation, MicroBatchScanExec, StreamingDataSourceV2ScanRelation}
import org.apache.spark.sql.execution.streaming.runtime.{MemoryStream, StreamExecution, StreamingQueryWrapper, StreamingV2ScanPushDown}
import org.apache.spark.sql.execution.streaming.sources.ContinuousMemoryStream
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.{Filter => SourceFilter}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class StreamingV2ScanPushDownSuite extends StreamTest with BeforeAndAfter {
  import testImplicits._

  private val tbl = "testcat.t"

  before {
    spark.conf.set("spark.sql.catalog.testcat", classOf[InMemoryTableCatalog].getName)
  }

  after {
    spark.sessionState.catalogManager.reset()
    spark.conf.unset("spark.sql.catalog.testcat")
  }

  private def withSourceTable(properties: String = "")(f: => Unit): Unit = {
    withTable(tbl) {
      sql(s"CREATE TABLE $tbl (id bigint, data string, extra int) $properties")
      sql(s"INSERT INTO $tbl VALUES (1, 'a', 10), (2, 'b', 20)")
      f
    }
  }

  private def insert(values: String, table: String = tbl): StreamAction = Execute { _ =>
    sql(s"INSERT INTO $table VALUES $values")
  }

  private def pruningEnabled(enabled: Boolean): Map[String, String] =
    Map(SQLConf.STREAMING_V2_COLUMN_PRUNING_ENABLED.key -> enabled.toString)

  /**
   * Runs `f`, which restarts streams with other values of the confs of these pushdowns, and then
   * restores their values: `testStream` restores the value that a conf had before the last
   * `StartStream` that set it, rather than before the first.
   */
  private def withPushDownConfsRestored(f: => Unit): Unit = withSQLConf(
    SQLConf.STREAMING_V2_COLUMN_PRUNING_ENABLED.key -> "true",
    SQLConf.STREAMING_V2_FILTER_PUSHDOWN_ENABLED.key -> "true")(f)

  private def filterPushdownEnabled(enabled: Boolean): Map[String, String] =
    Map(SQLConf.STREAMING_V2_FILTER_PUSHDOWN_ENABLED.key -> enabled.toString)

  /** Returns the filters pushed down to the scan of each streaming source of the query. */
  private def pushedFilters(query: StreamExecution): Seq[Seq[String]] = {
    query.logicalPlan.collect {
      case relation: StreamingDataSourceV2ScanRelation =>
        relation.scan match {
          case scan: InMemoryBaseTable#InMemoryBatchScan =>
            scan.pushedFilters.map(_.toString).toSeq.sorted
          case scan =>
            fail(s"The scan $scan of ${relation.name} is not a scan of an in-memory table.")
        }
    }
  }

  /** Checks that the filters pushed down to the scan of each streaming source are `filters`. */
  private def assertPushedFilters(filters: String*): StreamAction = AssertOnQuery { query =>
    val pushed = pushedFilters(query)
    assert(pushed.nonEmpty)
    pushed.foreach(p => assert(p === filters.sorted))
    true
  }

  /** Checks the read schemas of the scans of the streaming sources of the query. */
  private def assertReadSchemas(columns: Seq[String]*): StreamAction = AssertOnQuery { query =>
    val readSchemas = query.logicalPlan.collect {
      case relation: StreamingDataSourceV2ScanRelation =>
        relation.scan.readSchema().fieldNames.toSeq
    }
    assert(readSchemas.sortBy(_.mkString(",")) === columns.sortBy(_.mkString(",")))
    true
  }

  /** Checks that the scan of each streaming source of the query reads `columns`. */
  private def assertReadSchema(columns: String*): StreamAction = AssertOnQuery { query =>
    val readSchemas = query.logicalPlan.collect {
      case relation: StreamingDataSourceV2ScanRelation =>
        relation.scan.readSchema().fieldNames.toSeq
    }
    assert(readSchemas.nonEmpty)
    readSchemas.foreach(readSchema => assert(readSchema === columns))
    true
  }

  /** Checks the output of the scan nodes of the last micro-batch. */
  private def assertScanOutputs(columns: Seq[String]*): StreamAction = AssertOnQuery { query =>
    val outputs = query.lastExecution.executedPlan.collect {
      case scan: MicroBatchScanExec => scan.output.map(_.name)
    }
    assert(outputs.sortBy(_.mkString(",")) === columns.sortBy(_.mkString(",")))
    true
  }

  test("read only the columns that a streaming query needs, also after a restart") {
    withSourceTable() {
      val df = spark.readStream.table(tbl).where("extra > 10").select("data")
      testStream(df)(
        ProcessAllAvailable(),
        assertReadSchema("data", "extra"),
        assertScanOutputs(Seq("data", "extra")),
        CheckAnswer(Row("b")),
        StopStream,
        insert("(3, 'c', 30), (4, 'd', 5)"),
        StartStream(),
        ProcessAllAvailable(),
        assertReadSchema("data", "extra"),
        CheckAnswer(Row("b"), Row("c")))
    }
  }

  // Each case is a query over the source, and the columns that the optimizer reads for it, which
  // fewer than those that the operators of its analyzed plan reference.
  Seq[(String, DataFrame => DataFrame, Seq[String])](
    ("a column that a later projection drops",
      _.withColumn("x", lit(1)).select("data"), Seq("data")),
    ("chained projections", _.select("id", "data").select("data"), Seq("data")),
    ("expressions and functions that a later projection drops",
      _.select(upper($"data").as("u"), ($"id" * 2).as("i")).select("u"), Seq("data")),
    ("several aliases of a column",
      _.select($"id".as("x"), $"data".as("y")).select($"x".as("z")).where("z > 1"), Seq("id")),
    ("a field of a struct of columns",
      _.select(struct($"id", $"data").as("s")).select("s.data"), Seq("data")),
    ("the fields of a struct of columns",
      _.select(struct($"id", $"extra").as("s")).select("s.*"), Seq("id", "extra")),
    ("a filter between projections",
      _.select("id", "data").where("id > 1").select("data"), Seq("id", "data")),
    ("a projection of all the columns", _.select("*").select("data"), Seq("data"))
  ).foreach { case (name, query, columns) =>
    test(s"read only the columns that the optimized query reads: $name") {
      withSourceTable() {
        val expected = query(spark.read.table(tbl)).collect().toSeq
        testStream(query(spark.readStream.table(tbl)))(
          ProcessAllAvailable(),
          assertReadSchema(columns: _*),
          assertScanOutputs(columns),
          CheckAnswer(expected: _*))
      }
    }
  }

  Seq[(String, DataFrame => DataFrame)](
    ("from_json", _.select(from_json($"json", new StructType().add("a", "int")).as("p"))
      .select("p.a")),
    ("get_json_object", _.select(get_json_object($"json", "$.a").as("a"))),
    ("parse_json", _.select(parse_json($"json").as("v"))
      .select(variant_get($"v", "$.a", "int").as("a")))
  ).foreach { case (name, query) =>
    test(s"read only the parsed column of $name") {
      withTable(tbl) {
        sql(s"CREATE TABLE $tbl (id bigint, json string, extra int)")
        sql(s"""INSERT INTO $tbl VALUES (1, '{"a": 1}', 10), (2, '{"a": 2}', 20)""")
        val expected = query(spark.read.table(tbl)).collect().toSeq
        testStream(query(spark.readStream.table(tbl)))(
          ProcessAllAvailable(),
          assertReadSchema("json"),
          CheckAnswer(expected: _*))
      }
    }
  }

  test("read only the columns of a streaming aggregation, also after a restart") {
    withSourceTable() {
      val df = spark.readStream.table(tbl).groupBy("data").count()
      testStream(df, OutputMode.Complete())(
        ProcessAllAvailable(),
        assertReadSchema("data"),
        CheckAnswer(Row("a", 1L), Row("b", 1L)),
        StopStream,
        insert("(3, 'a', 30)"),
        StartStream(),
        ProcessAllAvailable(),
        assertReadSchema("data"),
        CheckAnswer(Row("a", 2L), Row("b", 1L)))
    }
  }

  test("read only the event-time column of a time window") {
    withTable(tbl) {
      sql(s"CREATE TABLE $tbl (id bigint, data string, ts timestamp)")
      sql(s"INSERT INTO $tbl VALUES " +
        "(1, 'a', timestamp_seconds(0)), (2, 'b', timestamp_seconds(15))")
      val df = spark.readStream.table(tbl).groupBy(window($"ts", "10 seconds")).count()
        .select($"window.start".cast("long"), $"count")
      testStream(df, OutputMode.Complete())(
        ProcessAllAvailable(),
        assertReadSchema("ts"),
        CheckAnswer(Row(0L, 1L), Row(10L, 1L)))
    }
  }

  test("read no column of the source when the query needs none, also after a restart") {
    withSourceTable() {
      testStream(spark.readStream.table(tbl).groupBy().count(), OutputMode.Complete())(
        ProcessAllAvailable(),
        assertReadSchema(),
        assertScanOutputs(Nil),
        CheckAnswer(Row(2L)),
        StopStream,
        insert("(3, 'c', 30)"),
        StartStream(),
        ProcessAllAvailable(),
        assertReadSchema(),
        CheckAnswer(Row(3L)))
      testStream(spark.readStream.table(tbl).select(lit(1).as("one")))(
        ProcessAllAvailable(),
        assertReadSchema(),
        CheckAnswer(Row(1), Row(1), Row(1)))
    }
  }

  test("read only the columns of a streaming deduplication, also after a restart") {
    withSourceTable() {
      val df = spark.readStream.table(tbl).dropDuplicates("data").select("id")
      testStream(df)(
        ProcessAllAvailable(),
        assertReadSchema("id", "data"),
        CheckAnswer(Row(1L), Row(2L)),
        StopStream,
        insert("(3, 'a', 30), (4, 'c', 40)"),
        StartStream(),
        ProcessAllAvailable(),
        assertReadSchema("id", "data"),
        CheckAnswer(Row(1L), Row(2L), Row(4L)))
    }
  }

  test("read the metadata columns that a streaming query needs") {
    withSourceTable() {
      val df = spark.readStream.table(tbl).select("data", "index")
      testStream(df)(
        ProcessAllAvailable(),
        assertReadSchema("data", "index"),
        CheckAnswerRowsByFunc(rows => {
          assert(rows.map(_.getString(0)).sorted === Seq("a", "b"))
          assert(rows.forall(!_.isNullAt(1)))
        }, lastOnly = false))
    }
  }

  test("read only the columns of each child of a union") {
    withSourceTable() {
      // Each read of the table has its own relation and scan, and the optimizer prunes the
      // columns of each child of the union by position.
      val twoReads = spark.readStream.table(tbl).union(spark.readStream.table(tbl)).select("data")
      testStream(twoReads)(
        ProcessAllAvailable(),
        assertReadSchemas(Seq("data"), Seq("data")),
        CheckAnswer(Row("a"), Row("b"), Row("a"), Row("b")))

      val differentColumns = spark.readStream.table(tbl).select("id")
        .union(spark.readStream.table(tbl).select($"extra".cast("long")))
      testStream(differentColumns)(
        ProcessAllAvailable(),
        assertReadSchemas(Seq("id"), Seq("extra")),
        CheckAnswer(Row(1L), Row(2L), Row(10L), Row(20L)))

      // The children of a self-union read the same relation, whose scan reads the columns that
      // both of them read.
      val source = spark.readStream.table(tbl)
      testStream(source.select("id", "data").union(source.select("id", "data")).select("data"))(
        ProcessAllAvailable(),
        AssertOnQuery { query =>
          val relations = query.logicalPlan.collect {
            case relation: StreamingDataSourceV2ScanRelation => relation
          }
          assert(relations.length == 2 && relations.map(_.stream).distinct.length == 1)
          assert(relations.head.scan.readSchema().fieldNames.toSeq === Seq("data"))
          true
        },
        CheckAnswer(Row("a"), Row("b"), Row("a"), Row("b")))
    }
  }

  test("read the columns that a scan returns when it reads more than the required ones") {
    withSourceTable(
        s"TBLPROPERTIES ('${InMemoryBaseTable.SIMULATE_PARTIAL_COLUMN_PRUNING}' = 'true')") {
      val df = spark.readStream.table(tbl).select("data", "index")
      testStream(df)(
        ProcessAllAvailable(),
        assertReadSchema("id", "data", "extra", "index"),
        CheckAnswerRowsByFunc(rows => {
          assert(rows.map(_.getString(0)).sorted === Seq("a", "b"))
          assert(rows.forall(!_.isNullAt(1)))
        }, lastOnly = false))
    }
  }

  test("read all the columns when a scan does not read the required ones") {
    withSourceTable(
        s"TBLPROPERTIES ('${InMemoryBaseTable.SIMULATE_INCOMPLETE_COLUMN_PRUNING}' = 'true')") {
      val table = spark.sessionState.catalogManager.catalog("testcat")
        .asInstanceOf[InMemoryTableCatalog].liveTable(Identifier.of(Array.empty, "t"))
        .asInstanceOf[InMemoryBaseTable]
      val logAppender = new LogAppender("the fallback of streaming column pruning")
      withLogAppender(logAppender,
          loggerNames = Seq(StreamingV2ScanPushDown.getClass.getName.stripSuffix("$")),
          level = Some(Level.WARN)) {
        testStream(spark.readStream.table(tbl).select("data"))(
          ProcessAllAvailable(),
          // The scan that does not read "data" is replaced by one of all the columns before its
          // stream is created, so the source only has the stream of the scan of all the columns.
          assertReadSchema("id", "data", "extra"),
          AssertOnQuery(_ => table.createdMicroBatchStreams.get() == 1),
          CheckAnswer(Row("a"), Row("b")))
      }
      val warnings = logAppender.loggingEvents.map(_.getMessage.getFormattedMessage)
      assert(warnings.exists { message =>
        message.contains("could not be built with the pushdowns (columns: data, filters: none)") &&
          message.contains("does not have the columns data") &&
          message.contains("It reads all the columns with no filter instead")
      }, warnings)
    }
  }

  test("read the event-time column of a watermark that the query does not output") {
    withTable(tbl) {
      sql(s"CREATE TABLE $tbl (id bigint, data string, ts timestamp)")
      sql(s"INSERT INTO $tbl VALUES " +
        "(1, 'a', timestamp_seconds(0)), (2, 'b', timestamp_seconds(10))")
      val df = spark.readStream.table(tbl).withWatermark("ts", "5 seconds").select("data")
      testStream(df)(
        ProcessAllAvailable(),
        assertReadSchema("data", "ts"),
        CheckAnswer(Row("a"), Row("b")),
        // The watermark gets the event times of the rows.
        AssertOnQuery { query =>
          query.recentProgress.filter(_.numInputRows > 0).map(_.eventTime.get("max")).toSeq ==
            Seq("1970-01-01T00:00:10.000Z")
        })
    }
  }

  test("read only the columns of a stream-static join") {
    withSourceTable() {
      val static = spark.range(2, 4).toDF("sid")
      val source = spark.readStream.table(tbl)
      val df = source.join(static, source("id") === static("sid")).select("data")
      testStream(df)(
        ProcessAllAvailable(),
        assertReadSchema("id", "data"),
        CheckAnswer(Row("b")),
        insert("(3, 'c', 30), (4, 'd', 40)"),
        ProcessAllAvailable(),
        CheckAnswer(Row("b"), Row("c")))
    }
  }

  test("read only the columns of each side of a stream-stream join, also after a restart") {
    withSourceTable() {
      val left = spark.readStream.table(tbl).select("id", "data")
      val right = spark.readStream.table(tbl).selectExpr("id AS rid", "extra")
      val df = left.join(right, left("id") === right("rid")).select("data", "extra")
      testStream(df)(
        ProcessAllAvailable(),
        assertReadSchemas(Seq("id", "data"), Seq("id", "extra")),
        CheckAnswer(Row("a", 10), Row("b", 20)),
        StopStream,
        insert("(3, 'c', 30)"),
        StartStream(),
        ProcessAllAvailable(),
        assertReadSchemas(Seq("id", "data"), Seq("id", "extra")),
        CheckAnswer(Row("a", 10), Row("b", 20), Row("c", 30)))
    }
  }

  test("keep the layout of the state of a stream-stream join across the values of the conf " +
      "when a scan returns the columns in another order") {
    withPushDownConfsRestored {
      val (leftTable, rightTable) = ("testcat.l", "testcat.r")
      withTable(leftTable, rightTable) {
        sql(s"CREATE TABLE $leftTable (k bigint, v bigint, unused int) TBLPROPERTIES " +
          s"('${InMemoryBaseTable.SIMULATE_REORDERED_COLUMN_PRUNING}' = 'true')")
        sql(s"CREATE TABLE $rightTable (rk bigint, w int)")
        // The left row is kept in the state of the join, which is written without pruning first.
        sql(s"INSERT INTO $leftTable VALUES (1, 100, 0)")
        val left = spark.readStream.table(leftTable)
        val right = spark.readStream.table(rightTable)
        val df = left.join(right, $"k" === $"rk").select("v", "w")
        testStream(df)(
          StartStream(additionalConfs = pruningEnabled(false)),
          ProcessAllAvailable(),
          assertReadSchemas(Seq("k", "v", "unused"), Seq("rk", "w")),
          CheckAnswer(),
          StopStream,
          // The pruned scan returns (v, k), and the join still gets (k, v).
          insert("(1, 7)", rightTable),
          StartStream(additionalConfs = pruningEnabled(true)),
          ProcessAllAvailable(),
          assertReadSchemas(Seq("v", "k"), Seq("rk", "w")),
          CheckAnswer(Row(100L, 7)),
          // And the other way around.
          insert("(2, 200, 0)", leftTable),
          ProcessAllAvailable(),
          StopStream,
          insert("(2, 8)", rightTable),
          StartStream(additionalConfs = pruningEnabled(false)),
          ProcessAllAvailable(),
          CheckAnswer(Row(100L, 7), Row(200L, 8)))
      }
    }
  }

  test("keep the state of a streaming aggregation across the values of the conf when a scan " +
      "returns the columns in another order") {
    withPushDownConfsRestored {
      withTable(tbl) {
        sql(s"CREATE TABLE $tbl (k bigint, v bigint, unused int) TBLPROPERTIES " +
          s"('${InMemoryBaseTable.SIMULATE_REORDERED_COLUMN_PRUNING}' = 'true')")
        sql(s"INSERT INTO $tbl VALUES (1, 10, 0), (2, 20, 0)")
        val df = spark.readStream.table(tbl).groupBy("k").agg(sum("v"))
        testStream(df, OutputMode.Complete())(
          StartStream(additionalConfs = pruningEnabled(false)),
          ProcessAllAvailable(),
          assertReadSchema("k", "v", "unused"),
          CheckAnswer(Row(1L, 10L), Row(2L, 20L)),
          StopStream,
          insert("(1, 1, 0)"),
          StartStream(additionalConfs = pruningEnabled(true)),
          ProcessAllAvailable(),
          assertReadSchema("v", "k"),
          CheckAnswer(Row(1L, 11L), Row(2L, 20L)),
          StopStream,
          insert("(2, 2, 0)"),
          StartStream(additionalConfs = pruningEnabled(false)),
          ProcessAllAvailable(),
          CheckAnswer(Row(1L, 11L), Row(2L, 22L)))
      }
    }
  }

  test("fail to plan a micro-batch that reads a column that its scan does not read") {
    withSourceTable() {
      testStream(spark.readStream.table(tbl).select("data"))(
        ProcessAllAvailable(),
        AssertOnQuery { query =>
          val relation = query.logicalPlan.collectFirst {
            case relation: StreamingDataSourceV2ScanRelation => relation
          }.get
          assert(relation.output.map(_.name) === Seq("id", "data", "extra"))
          assert(relation.prunedOutput.map(_.map(_.name)) === Some(Seq("data")))
          assert(relation.simpleString(10).contains("PrunedOutput: [data#"))
          val offset = relation.stream.initialOffset()
          val batch = relation.copy(startOffset = Some(offset), endOffset = Some(offset))
          // Unlike the query, this plan reads all the columns of the relation.
          val e = intercept[SparkException] {
            spark.sessionState.planner.plan(Project(relation.output, batch)).next()
          }
          assert(e.getCondition === "INTERNAL_ERROR")
          assert(e.getMessage.contains("does not read the columns"))
          assert(e.getMessage.contains(SQLConf.STREAMING_V2_COLUMN_PRUNING_ENABLED.key))
          true
        })
    }
  }

  test("read only the columns that a continuous query needs") {
    val input = ContinuousMemoryStream.singlePartition[(Long, String, Int)]
    val table = new PruningContinuousTable(input)
    val df = Dataset.ofRows(spark, StreamingRelationV2(None, "pruning_continuous", table,
      CaseInsensitiveStringMap.empty(), DataTypeUtils.toAttributes(table.schema()), None, None,
      None)).toDF("id", "data", "extra").where("extra > 10").select("data")
    withTempView("continuous_pruning") {
      val query = df.writeStream.format("memory").queryName("continuous_pruning")
        .trigger(Trigger.Continuous(100)).start()
      try {
        input.addData((1L, "a", 10), (2L, "b", 20), (3L, "c", 30))
        eventually(timeout(streamingTimeout)) {
          checkAnswer(spark.table("continuous_pruning"), Seq(Row("b"), Row("c")))
        }
        val readSchemas = query.asInstanceOf[StreamingQueryWrapper].streamingQuery.logicalPlan
          .collect { case r: StreamingDataSourceV2ScanRelation => r.scan.readSchema().fieldNames }
        assert(readSchemas.map(_.toSeq) === Seq(Seq("_2", "_3")))
      } finally {
        query.stop()
      }
    }
  }

  test("push the filters of a streaming query down to the scan of its source") {
    withPushDownConfsRestored {
      withSourceTable() {
        val df = spark.readStream.table(tbl).where("extra > 10").select("data")
        testStream(df)(
          StartStream(additionalConfs = filterPushdownEnabled(true)),
          ProcessAllAvailable(),
          assertPushedFilters("GreaterThan(extra,10)"),
          // The scan of the in-memory table does not apply the filters, which Spark evaluates.
          CheckAnswer(Row("b")),
          StopStream,
          insert("(3, 'c', 30), (4, 'd', 5)"),
          StartStream(additionalConfs = filterPushdownEnabled(false)),
          ProcessAllAvailable(),
          assertPushedFilters(),
          CheckAnswer(Row("b"), Row("c")),
          StopStream,
          insert("(5, 'e', 50), (6, 'f', 6)"),
          StartStream(additionalConfs = filterPushdownEnabled(true)),
          ProcessAllAvailable(),
          assertPushedFilters("GreaterThan(extra,10)"),
          CheckAnswer(Row("b"), Row("c"), Row("e")))
      }
    }
  }

  test("push down only the filters that every read of a relation applies") {
    withSourceTable() {
      val source = spark.readStream.table(tbl)
      val df = source.where("extra > 10").union(source.where("extra > 10 AND id > 1"))
      testStream(df)(
        ProcessAllAvailable(),
        assertPushedFilters("GreaterThan(extra,10)"),
        CheckAnswer(Row(2L, "b", 20), Row(2L, "b", 20)))
    }
  }

  // Each case is a condition on a value that the optimizer of each micro-batch folds into its own
  // value, such as its current time, and the column that it reads.
  Seq(
    "ts < current_timestamp()" -> "ts",
    "ts < now()" -> "ts",
    "CAST(ts AS DATE) < current_date()" -> "ts",
    "CAST(ts AS TIMESTAMP_NTZ) < localtimestamp()" -> "ts",
    "tod < current_time()" -> "tod",
    "CAST(tod AS TIMESTAMP_NTZ) < TIMESTAMP_NTZ'2100-01-01 00:00:00'" -> "tod",
    "CAST(tod AS TIMESTAMP) < TIMESTAMP'2100-01-01 00:00:00'" -> "tod",
    "data = current_database()" -> "data",
    "data = current_timezone()" -> "data"
  ).foreach { case (condition, column) =>
    test(s"do not push down a filter on a value that depends on when it is evaluated: $condition") {
      withTable(tbl) {
        sql(s"CREATE TABLE $tbl (id bigint, data string, ts timestamp, tod time)")
        val df = spark.readStream.table(tbl).where(s"id > 1 AND $condition").select("id")
        val reads = StreamingV2ScanPushDown.reads(spark, df.queryExecution.analyzed, None)
        assert(reads.size === 1)
        val read = reads.values.head
        // The column of the condition is still read, but only the filter on the id is pushed down.
        assert(read.requiredColumns.map(_.name).toSet === Set("id", column))
        assert(read.filters.nonEmpty)
        assert(read.filters.forall(_.references.map(_.name).toSet == Set("id")), read.filters)
      }
    }
  }

  test("do not push down a filter on the current time of a micro-batch") {
    withTable(tbl) {
      sql(s"CREATE TABLE $tbl (id bigint, data string, ts timestamp)")
      sql(s"INSERT INTO $tbl VALUES " +
        "(1, 'a', timestamp_seconds(0)), (2, 'b', timestamp_seconds(10))")
      // Each micro-batch has its own current time, so the time when the query started cannot be
      // pushed down in its place.
      Seq("current_timestamp()", "now()").foreach { time =>
        val df = spark.readStream.table(tbl).where(s"ts < $time AND id > 0").select("data")
        testStream(df)(
          ProcessAllAvailable(),
          assertPushedFilters("GreaterThan(id,0)"),
          CheckAnswer(Row("a"), Row("b")))
      }
    }
  }

  test("read the columns and apply the filters of every reference to a common table expression") {
    withSourceTable() {
      withTempView("s") {
        spark.readStream.table(tbl).createOrReplaceTempView("s")
        // Both references to the common table expression have the relation of the source, so its
        // scan reads the columns of both, and applies no filter, as only one of them filters.
        val df = sql("WITH c AS (SELECT * FROM s) " +
          "SELECT a.id, b.extra FROM c a JOIN c b ON a.id = b.id WHERE a.data = 'a'")
        testStream(df)(
          ProcessAllAvailable(),
          assertReadSchema("id", "data", "extra"),
          assertPushedFilters(),
          CheckAnswer(Row(1L, 10)),
          insert("(3, 'a', 30)"),
          ProcessAllAvailable(),
          CheckAnswer(Row(1L, 10), Row(3L, 30)))

        // Each reference applies its own filter.
        val pruned = sql("WITH c AS (SELECT * FROM s) " +
          "SELECT data FROM c WHERE extra > 15 UNION ALL SELECT data FROM c WHERE extra < 15")
        testStream(pruned)(
          ProcessAllAvailable(),
          assertReadSchema("data", "extra"),
          assertPushedFilters(),
          CheckAnswer(Row("a"), Row("a"), Row("b")))
      }
    }
  }

  test("push down no filter that the optimizer infers from the filters of another source") {
    val otherTable = "testcat.other"
    withSourceTable() {
      withTable(otherTable) {
        sql(s"CREATE TABLE $otherTable (oid bigint, value string)")
        val source = spark.readStream.table(tbl)
        val other = spark.readStream.table(otherTable).where("oid < 3")
        val df = source.join(other, source("id") === other("oid")).select("data", "value")
        testStream(df)(
          ProcessAllAvailable(),
          // The optimizer infers a filter on the id of the source from the one on the other
          // source, which is not pushed down: a scan only gets the filters that the query applies
          // to its own source.
          AssertOnQuery { query =>
            assert(pushedFilters(query).sortBy(_.mkString) === Seq(Nil, Seq("LessThan(oid,3)")))
            true
          },
          CheckAnswer(),
          insert("(1, 'x'), (2, 'y'), (3, 'z')", otherTable),
          ProcessAllAvailable(),
          CheckAnswer(Row("a", "x"), Row("b", "y")),
          insert("(3, 'c', 30)"),
          ProcessAllAvailable(),
          CheckAnswer(Row("a", "x"), Row("b", "y")))
      }
    }
  }

  test("plan the micro-batches whose filters imply the pushed filters in another form") {
    val otherTable = "testcat.other"
    withSourceTable() {
      withTable(otherTable) {
        sql(s"CREATE TABLE $otherTable (oid bigint, value string)")
        sql(s"INSERT INTO $otherTable VALUES (1, 'x'), (2, 'y')")
        // A micro-batch infers id = 2 from the equality of the static side, and puts its value in
        // place of the column in the pushed filter id > 0, which it then drops: id = 2 implies it.
        val source = spark.readStream.table(tbl)
        val static = spark.range(1, 3).toDF("sid").where("sid = 2")
        val joined = source.where("id > 0").join(static, source("id") === static("sid"))
        testStream(joined.select("data"))(
          ProcessAllAvailable(),
          assertPushedFilters("GreaterThan(id,0)"),
          CheckAnswer(Row("b")))
        // So does a stream-stream join, in the micro-batches where both sources have data.
        val other = spark.readStream.table(otherTable).where("oid = 2")
        val streams = source.where("id > 0").join(other, source("id") === other("oid"))
        testStream(streams.select("data", "value"))(
          ProcessAllAvailable(),
          CheckAnswer(Row("b", "y")),
          insert("(2, 'bb', 22)"),
          insert("(2, 'yy')", otherTable),
          ProcessAllAvailable(),
          CheckAnswer(Row("b", "y"), Row("b", "yy"), Row("bb", "y"), Row("bb", "yy")))
      }
    }
  }

  test("plan the micro-batches that fold a value of their own into a pushed filter") {
    withTable(tbl) {
      sql(s"CREATE TABLE $tbl (id bigint, owner string, d date)")
      sql(s"INSERT INTO $tbl VALUES (1, current_user(), current_date()), (2, 'other', NULL)")
      // A micro-batch folds current_user() and now() into literals, which it then puts in place
      // of the columns in the pushed filters owner <> '' and d >= DATE'2020-01-01'.
      testStream(spark.readStream.table(tbl).where("owner = current_user() AND owner <> ''")
          .select("id"))(
        ProcessAllAvailable(),
        assertPushedFilters("Not(EqualTo(owner,))"),
        CheckAnswer(Row(1L)))
      testStream(spark.readStream.table(tbl)
          .where("d = to_date(now()) AND d >= DATE'2020-01-01'").select("id"))(
        ProcessAllAvailable(),
        assertPushedFilters("GreaterThanOrEqual(d,2020-01-01)"))
    }
  }

  test("plan the micro-batches whose filters fold the pushed filters in part") {
    withTable(tbl) {
      sql(s"CREATE TABLE $tbl (id bigint, owner string, d date)")
      sql(s"INSERT INTO $tbl VALUES (1, current_user(), current_date()), " +
        "(6, current_user(), current_date()), (7, 'other', current_date())")
      // A micro-batch folds current_user() into a literal, which it puts in place of the column
      // in the pushed filter owner = '' OR id > 5, which it then folds into id > 5.
      testStream(spark.readStream.table(tbl)
          .where("owner = current_user() AND (owner = '' OR id > 5)").select("id"))(
        ProcessAllAvailable(),
        assertPushedFilters("Or(EqualTo(owner,),GreaterThan(id,5))"),
        CheckAnswer(Row(6L)))
      // So does a date that is not the date of the micro-batch, as on the day after the start.
      // The micro-batch is planned whether the rows have its date or not.
      testStream(spark.readStream.table(tbl)
          .where("d = to_date(now()) AND (d < DATE'2000-01-01' OR id > 5)").select("id"))(
        ProcessAllAvailable(),
        assertPushedFilters("Or(LessThan(d,2000-01-01),GreaterThan(id,5))"))
    }
    // And an equality that a micro-batch infers through a stream-static join.
    withTable(tbl) {
      sql(s"CREATE TABLE $tbl (a bigint, b bigint)")
      sql(s"INSERT INTO $tbl VALUES (2, 0), (0, 0), (5, 3)")
      val source = spark.readStream.table(tbl).where("a > 1 OR b > 2")
      val static = spark.range(0, 2).toDF("x").where("x = 0")
      testStream(source.join(static, source("b") === static("x")).select("a"))(
        ProcessAllAvailable(),
        assertPushedFilters("Or(GreaterThan(a,1),GreaterThan(b,2))"),
        CheckAnswer(Row(2L)))
    }
  }

  test("read all the columns with no filter from a table without the capabilities") {
    withSourceTable(s"TBLPROPERTIES ('${InMemoryBaseTable.STREAMING_PUSHDOWNS}' = '')") {
      val df = spark.readStream.table(tbl).where("extra > 10").select("data")
      testStream(df)(
        ProcessAllAvailable(),
        assertReadSchema("id", "data", "extra"),
        assertScanOutputs(Seq("id", "data", "extra")),
        assertPushedFilters(),
        AssertOnQuery { query =>
          // The plan of the query only applies the filter of the query.
          val relations = query.logicalPlan.collect {
            case relation: StreamingDataSourceV2ScanRelation => relation
          }
          relations.nonEmpty && relations.forall(_.pushedFilters.isEmpty)
        },
        CheckAnswer(Row("b")))
    }
  }

  test("push down only what the capabilities of the table allow") {
    val df = () => spark.readStream.table(tbl).where("extra > 10").select("data")
    withSourceTable(s"TBLPROPERTIES ('${InMemoryBaseTable.STREAMING_PUSHDOWNS}' = 'columns')") {
      testStream(df())(
        ProcessAllAvailable(),
        assertReadSchema("data", "extra"),
        assertPushedFilters(),
        CheckAnswer(Row("b")))
    }
    withSourceTable(s"TBLPROPERTIES ('${InMemoryBaseTable.STREAMING_PUSHDOWNS}' = 'filters')") {
      testStream(df())(
        ProcessAllAvailable(),
        assertReadSchema("id", "data", "extra"),
        assertPushedFilters("GreaterThan(extra,10)"),
        CheckAnswer(Row("b")))
    }
  }

  Seq("pushFilters", "pruneColumns", "build", "toMicroBatchStream").foreach { failingStep =>
    test(s"read all the columns with no filter when the scan fails with the pushdowns: " +
        failingStep) {
      val input = MemoryStream[(Long, String, Int)]
      val table = new FailingPushDownTable(input, failingStep)
      val df = Dataset.ofRows(spark, StreamingRelationV2(None, "failing_push_down", table,
        CaseInsensitiveStringMap.empty(), DataTypeUtils.toAttributes(table.schema()), None, None,
        None)).toDF("id", "data", "extra").where("extra > 10").select("data")
      val logs = new LogAppender("the fallback of a streaming scan")
      withLogAppender(logs,
          loggerNames = Seq(StreamingV2ScanPushDown.getClass.getName.stripSuffix("$")),
          level = Some(Level.WARN)) {
        testStream(df)(
          StartStream(),
          AddData(input, (1L, "a", 10), (2L, "b", 20)),
          ProcessAllAvailable(),
          // A new scan builder builds a scan of all the columns, with no filter, as without the
          // pushdowns.
          assertReadSchema("_1", "_2", "_3"),
          AssertOnQuery(_ => table.streams.asScala.toSeq == Seq(Seq("_1", "_2", "_3") -> Nil)),
          CheckAnswer(Row("b")))
      }
      val warnings = logs.loggingEvents.map(_.getMessage.getFormattedMessage)
      assert(warnings.exists { message =>
        message.contains("could not be built with the pushdowns (columns: _2, _3, filters: ") &&
          message.contains(s"$failingStep failed with the pushdowns")
      }, warnings)
    }
  }

  test("keep the placeholders of the values that depend on when they are evaluated apart") {
    withSourceTable() {
      val df = spark.readStream.table(tbl)
        .select(current_user(), current_database(), current_catalog(), col("data"))
      val placeholders = StreamingV2ScanPushDown.forStartup(df.queryExecution.analyzed)
        .collectFirst { case project: Project => project.projectList.take(3) }.get
        .map(_.asInstanceOf[Alias].child)
      // The placeholders of different expressions differ, although they have the same type.
      assert(placeholders.map(_.dataType).distinct.length === 1)
      assert(placeholders.map(_.canonicalized).distinct.length === 3)
    }
  }

  test("push down a filter on a cast that does not depend on when it is evaluated") {
    withTable(tbl) {
      sql(s"CREATE TABLE $tbl (id bigint, data string)")
      val df = spark.readStream.table(tbl)
        .where("CAST(data AS TIMESTAMP) < TIMESTAMP'2100-01-01 00:00:00'").select("id")
      val reads = StreamingV2ScanPushDown.reads(spark, df.queryExecution.analyzed, None)
      val filters = reads.values.flatMap(_.filters).toSeq
      assert(filters.nonEmpty)
      assert(filters.forall(_.references.map(_.name).toSet == Set("data")), filters)
    }
  }

  test("apply all the filters that the scan builder got in the plan of each micro-batch") {
    withSourceTable() {
      // id + extra > 10 cannot be translated to a filter of the source, so the scan builder only
      // takes extra > 15. Each micro-batch still applies both to the rows of the source, which
      // the query applies anyway, so the scan cannot drop a row that the plan keeps, whatever it
      // does with the filters.
      testStream(spark.readStream.table(tbl).where("id + extra > 10 AND extra > 15")
          .select("data"))(
        ProcessAllAvailable(),
        assertPushedFilters("GreaterThan(extra,15)"),
        AssertOnQuery { query =>
          val filtered = query.logicalPlan.collect {
            case Filter(condition, relation: StreamingDataSourceV2ScanRelation) =>
              condition -> relation
          }
          assert(filtered.length === 1)
          val (condition, relation) = filtered.head
          assert(relation.pushedFilters.map(_.references.map(_.name).toSet) ===
            Seq(Set("id", "extra"), Set("extra")))
          assert(condition === relation.pushedFilters.reduce(And))
          true
        },
        CheckAnswer(Row("b")),
        insert("(3, 'c', 5), (4, 'd', 30)"),
        ProcessAllAvailable(),
        CheckAnswer(Row("b"), Row("d")))
    }
  }

  test("push down the filters of a query that joins a source with an inline table") {
    withSourceTable() {
      val source = spark.readStream.table(tbl)
      val inline = sql("SELECT * FROM VALUES (2, current_date()) AS s(k, d)")
      val df = source.where("extra > 10").join(inline, source("id") === inline("k"))
        .select("data", "d")
      val reads = StreamingV2ScanPushDown.reads(spark, df.queryExecution.analyzed, None)
      assert(reads.values.map(_.requiredColumns.map(_.name).toSet).toSeq ===
        Seq(Set("id", "data", "extra")))
      assert(reads.values.flatMap(_.filters).map(_.references.map(_.name).toSet).toSeq ===
        Seq(Set("extra")))
    }
  }

  test("filter the rows of each read of a source with the filters that its scan builder got") {
    withSourceTable() {
      val source = spark.readStream.table(tbl)
      val df = source.where("extra > 10").select("data")
        .union(source.where("extra > 10 AND id > 1").select("data"))
      testStream(df)(
        ProcessAllAvailable(),
        AssertOnQuery { query =>
          // Both reads of the relation are filtered with the filter that they both apply.
          val filtered = query.logicalPlan.collect {
            case Filter(condition, relation: StreamingDataSourceV2ScanRelation) =>
              condition -> relation
          }
          assert(filtered.length === 2)
          filtered.foreach { case (condition, relation) =>
            assert(relation.pushedFilters.map(_.toString) ===
              Seq(s"(${relation.output(2)} > 10)"))
            assert(condition === relation.pushedFilters.head)
          }
          // So is each read of the relation in the optimized plan of a micro-batch.
          val microBatchFilters = query.lastExecution.optimizedPlan.collect {
            case Filter(condition, relation: StreamingDataSourceV2ScanRelation) =>
              condition.exists(c => relation.pushedFilters.exists(c.semanticEquals))
          }
          assert(microBatchFilters === Seq(true, true))
          true
        },
        CheckAnswer(Row("b"), Row("b")))
    }
  }

  test("do not build the scans of the batch relations of a streaming query when it starts") {
    withSourceTable() {
      val table = new CountingBatchTable
      val static = Dataset.ofRows(spark, DataSourceV2Relation.create(
        table, None, None, CaseInsensitiveStringMap.empty()))
      val source = spark.readStream.table(tbl)
      val df = source.join(static, source("id") === static("sid")).select("data")
      val reads = StreamingV2ScanPushDown.reads(spark, df.queryExecution.analyzed, None)
      assert(reads.values.map(_.requiredColumns.map(_.name).toSet).toSeq === Seq(Set("id", "data")))
      assert(table.scanBuilders.get() === 0)
    }
  }

  test("read all the columns of a source whose pruned scan has no read schema") {
    val input = MemoryStream[(Long, String, Int)]
    val table = new NoPrunedReadSchemaTable(input)
    val df = Dataset.ofRows(spark, StreamingRelationV2(None, "no_pruned_read_schema", table,
      CaseInsensitiveStringMap.empty(), DataTypeUtils.toAttributes(table.schema()), None, None,
      None)).toDF("id", "data", "extra").select("data")
    testStream(df)(
      StartStream(),
      AddData(input, (1L, "a", 10), (2L, "b", 20)),
      ProcessAllAvailable(),
      // The pruned scan is replaced by a scan of all the columns before its stream is created.
      assertReadSchema("_1", "_2", "_3"),
      AssertOnQuery(_ => table.streams.get() == 1),
      CheckAnswer(Row("a"), Row("b")))
  }

  test("read all the columns when the pruning of streaming scans is disabled") {
    withSQLConf(SQLConf.STREAMING_V2_COLUMN_PRUNING_ENABLED.key -> "false") {
      withSourceTable() {
        val df = spark.readStream.table(tbl).select("data")
        testStream(df)(
          ProcessAllAvailable(),
          assertReadSchema("id", "data", "extra"),
          assertScanOutputs(Seq("id", "data", "extra")),
          CheckAnswer(Row("a"), Row("b")))
      }
    }
  }
}

/**
 * A table over a continuous memory stream, whose scans only read the columns that they are asked
 * for: their readers return the rows of the stream with only these columns.
 */
private class PruningContinuousTable(input: ContinuousMemoryStream[_])
  extends Table with SupportsRead {

  override def name(): String = "pruning_continuous"

  override def schema(): StructType = input.fullSchema()

  override def capabilities(): util.Set[TableCapability] =
    util.EnumSet.of(TableCapability.CONTINUOUS_READ, TableCapability.STREAMING_COLUMN_PRUNING)

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    new ScanBuilder with SupportsPushDownRequiredColumns {
      private var columns = schema()

      override def pruneColumns(requiredSchema: StructType): Unit = {
        columns = StructType(requiredSchema.fieldNames.map(schema()(_)))
      }

      override def build(): Scan = new Scan {
        override def readSchema(): StructType = columns

        override def toContinuousStream(checkpointLocation: String): ContinuousStream = {
          new PruningContinuousStream(input, columns.fieldNames.map(schema().fieldIndex), columns)
        }
      }
    }
  }
}

private class PruningContinuousStream(
    input: ContinuousStream,
    ordinals: Array[Int],
    columns: StructType) extends ContinuousStream {

  override def initialOffset(): Offset = input.initialOffset()

  override def deserializeOffset(json: String): Offset = input.deserializeOffset(json)

  override def mergeOffsets(offsets: Array[PartitionOffset]): Offset = input.mergeOffsets(offsets)

  override def planInputPartitions(start: Offset): Array[InputPartition] =
    input.planInputPartitions(start)

  override def createContinuousReaderFactory(): ContinuousPartitionReaderFactory =
    new PruningContinuousReaderFactory(input.createContinuousReaderFactory(), ordinals, columns)

  override def commit(end: Offset): Unit = input.commit(end)

  override def stop(): Unit = input.stop()
}

private class PruningContinuousReaderFactory(
    input: ContinuousPartitionReaderFactory,
    ordinals: Array[Int],
    columns: StructType) extends ContinuousPartitionReaderFactory {

  override def createReader(partition: InputPartition): ContinuousPartitionReader[InternalRow] = {
    val reader = input.createReader(partition)
    new ContinuousPartitionReader[InternalRow] {
      private val projection = ProjectingInternalRow(columns, ordinals.toSeq)

      override def next(): Boolean = reader.next()

      override def get(): InternalRow = {
        projection.project(reader.get())
        projection
      }

      override def getOffset: PartitionOffset = reader.getOffset

      override def close(): Unit = reader.close()
    }
  }
}

/** A batch table that counts the scan builders that it creates, which cannot build a scan. */
private class CountingBatchTable extends Table with SupportsRead {
  val scanBuilders = new AtomicInteger()

  override def name(): String = "counting_batch"

  override def schema(): StructType = StructType.fromDDL("sid BIGINT")

  override def capabilities(): util.Set[TableCapability] =
    util.EnumSet.of(TableCapability.BATCH_READ)

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    scanBuilders.incrementAndGet()
    () => throw new UnsupportedOperationException("The scan of the batch table is not built.")
  }
}

/**
 * A table over a memory stream, whose scans can only tell their read schema when they read all
 * the columns. It counts the streams that its scans create.
 */
private class NoPrunedReadSchemaTable(input: MemoryStream[_]) extends Table with SupportsRead {
  val streams = new AtomicInteger()

  override def name(): String = "no_pruned_read_schema"

  override def schema(): StructType = input.fullSchema()

  override def capabilities(): util.Set[TableCapability] =
    util.EnumSet.of(TableCapability.MICRO_BATCH_READ, TableCapability.STREAMING_COLUMN_PRUNING)

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    new ScanBuilder with SupportsPushDownRequiredColumns {
      private var pruned = false

      override def pruneColumns(requiredSchema: StructType): Unit = {
        pruned = requiredSchema.length < schema().length
      }

      override def build(): Scan = new Scan {
        override def readSchema(): StructType = if (pruned) {
          throw new IllegalStateException("The read schema of a pruned scan is unknown.")
        } else {
          schema()
        }

        override def toMicroBatchStream(checkpointLocation: String): MicroBatchStream = {
          streams.incrementAndGet()
          input
        }
      }
    }
  }
}

/**
 * A table over a memory stream, whose scan builder fails in `failingStep` when it gets a pushdown
 * of a streaming query: "pushFilters", "pruneColumns", "build" or "toMicroBatchStream". It records
 * the read schema and the filters of each scan whose stream it creates.
 */
private class FailingPushDownTable(input: MemoryStream[_], failingStep: String)
  extends Table with SupportsRead {
  val streams = new ConcurrentLinkedQueue[(Seq[String], Seq[String])]()

  override def name(): String = "failing_push_down"

  override def schema(): StructType = input.fullSchema()

  override def capabilities(): util.Set[TableCapability] = util.EnumSet.of(
    TableCapability.MICRO_BATCH_READ,
    TableCapability.STREAMING_COLUMN_PRUNING,
    TableCapability.STREAMING_FILTER_PUSHDOWN)

  private def failIf(step: String, pushedDown: Boolean): Unit = {
    if (step == failingStep && pushedDown) {
      throw new IllegalStateException(s"$step failed with the pushdowns")
    }
  }

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    new ScanBuilder with SupportsPushDownRequiredColumns with SupportsPushDownFilters {
      private var columns = schema()
      private var filters = Array.empty[SourceFilter]

      private def pushedDown: Boolean = columns.length < schema().length || filters.nonEmpty

      override def pushFilters(filters: Array[SourceFilter]): Array[SourceFilter] = {
        failIf("pushFilters", filters.nonEmpty)
        this.filters = filters
        filters
      }

      override def pushedFilters(): Array[SourceFilter] = filters

      override def pruneColumns(requiredSchema: StructType): Unit = {
        failIf("pruneColumns", requiredSchema.length < schema().length)
        columns = requiredSchema
      }

      override def build(): Scan = {
        failIf("build", pushedDown)
        val (readColumns, readFilters, readPushedDown) = (columns, filters, pushedDown)
        new Scan {
          override def readSchema(): StructType = readColumns

          override def toMicroBatchStream(checkpointLocation: String): MicroBatchStream = {
            failIf("toMicroBatchStream", readPushedDown)
            streams.add(readColumns.fieldNames.toSeq -> readFilters.map(_.toString).toSeq)
            input
          }
        }
      }
    }
  }
}
