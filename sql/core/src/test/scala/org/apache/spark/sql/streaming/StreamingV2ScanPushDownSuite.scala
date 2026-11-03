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

import org.apache.logging.log4j.Level
import org.scalatest.BeforeAndAfter

import org.apache.spark.SparkException
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.plans.logical.Project
import org.apache.spark.sql.catalyst.streaming.StreamingRelationV2
import org.apache.spark.sql.catalyst.types.DataTypeUtils
import org.apache.spark.sql.classic.Dataset
import org.apache.spark.sql.connector.catalog.{Identifier, InMemoryBaseTable, InMemoryTableCatalog, SupportsRead, Table, TableCapability}
import org.apache.spark.sql.connector.read.{InputPartition, Scan, ScanBuilder, SupportsPushDownRequiredColumns}
import org.apache.spark.sql.connector.read.streaming.{ContinuousPartitionReader, ContinuousPartitionReaderFactory, ContinuousStream, Offset, PartitionOffset}
import org.apache.spark.sql.execution.datasources.v2.{MicroBatchScanExec, StreamingDataSourceV2ScanRelation}
import org.apache.spark.sql.execution.streaming.runtime.{StreamExecution, StreamingQueryWrapper, StreamingV2ScanPushDown}
import org.apache.spark.sql.execution.streaming.sources.ContinuousMemoryStream
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DataType, StructType}
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
          // The scan that does not read "data" is replaced by one of all the columns, and the
          // stream of the replaced scan is stopped.
          assertReadSchema("id", "data", "extra"),
          AssertOnQuery(_ => table.stoppedMicroBatchStreams.get() == 1),
          CheckAnswer(Row("a"), Row("b")))
      }
      val warnings = logAppender.loggingEvents.map(_.getMessage.getFormattedMessage)
      assert(warnings.exists { message =>
        message.contains("was asked to read the columns data") &&
          message.contains("does not have the columns data") &&
          message.contains("It reads all the columns instead")
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

  test("push the filters of a streaming query down to the scan of its source") {
    withPushDownConfsRestored {
      withSourceTable() {
        val df = spark.readStream.table(tbl).where("extra > 10").select("data")
        testStream(df)(
          StartStream(additionalConfs = filterPushdownEnabled(true)),
          ProcessAllAvailable(),
          assertPushedFilters("GreaterThan(extra,10)", "IsNotNull(extra)"),
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
          assertPushedFilters("GreaterThan(extra,10)", "IsNotNull(extra)"),
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
        assertPushedFilters("GreaterThan(extra,10)", "IsNotNull(extra)"),
        CheckAnswer(Row(2L, "b", 20), Row(2L, "b", 20)))
    }
  }

  test("do not push down a filter on the current time of a micro-batch") {
    withTable(tbl) {
      sql(s"CREATE TABLE $tbl (id bigint, data string, ts timestamp)")
      sql(s"INSERT INTO $tbl VALUES " +
        "(1, 'a', timestamp_seconds(0)), (2, 'b', timestamp_seconds(10))")
      // Each micro-batch has its own current time, so the time when the query started cannot be
      // pushed down in its place.
      val df = spark.readStream.table(tbl).where("ts < current_timestamp()").select("data")
      testStream(df)(
        ProcessAllAvailable(),
        AssertOnQuery { query =>
          val pushed = pushedFilters(query)
          assert(pushed.nonEmpty && pushed.flatten.forall(!_.contains("LessThan")), pushed)
          true
        },
        CheckAnswer(Row("a"), Row("b")))
    }
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
    util.EnumSet.of(TableCapability.CONTINUOUS_READ)

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    new ScanBuilder with SupportsPushDownRequiredColumns {
      private var columns = schema()

      override def pruneColumns(requiredSchema: StructType): Unit = {
        columns = StructType(requiredSchema.fieldNames.map(schema()(_)))
      }

      override def build(): Scan = new Scan {
        override def readSchema(): StructType = columns

        override def toContinuousStream(checkpointLocation: String): ContinuousStream = {
          new PruningContinuousStream(
            input, columns.fieldNames.map(schema().fieldIndex), columns.map(_.dataType).toArray)
        }
      }
    }
  }
}

private class PruningContinuousStream(
    input: ContinuousStream,
    ordinals: Array[Int],
    dataTypes: Array[DataType]) extends ContinuousStream {

  override def initialOffset(): Offset = input.initialOffset()

  override def deserializeOffset(json: String): Offset = input.deserializeOffset(json)

  override def mergeOffsets(offsets: Array[PartitionOffset]): Offset = input.mergeOffsets(offsets)

  override def planInputPartitions(start: Offset): Array[InputPartition] =
    input.planInputPartitions(start)

  override def createContinuousReaderFactory(): ContinuousPartitionReaderFactory =
    new PruningContinuousReaderFactory(input.createContinuousReaderFactory(), ordinals, dataTypes)

  override def commit(end: Offset): Unit = input.commit(end)

  override def stop(): Unit = input.stop()
}

private class PruningContinuousReaderFactory(
    input: ContinuousPartitionReaderFactory,
    ordinals: Array[Int],
    dataTypes: Array[DataType]) extends ContinuousPartitionReaderFactory {

  override def createReader(partition: InputPartition): ContinuousPartitionReader[InternalRow] = {
    val reader = input.createReader(partition)
    new ContinuousPartitionReader[InternalRow] {
      override def next(): Boolean = reader.next()

      override def get(): InternalRow = {
        val row = reader.get()
        new GenericInternalRow(Array.tabulate[Any](ordinals.length) { i =>
          row.get(ordinals(i), dataTypes(i))
        })
      }

      override def getOffset: PartitionOffset = reader.getOffset

      override def close(): Unit = reader.close()
    }
  }
}
