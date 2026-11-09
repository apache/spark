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

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.catalyst.{InternalRow, ProjectingInternalRow}
import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeSet, ScalarSubquery}
import org.apache.spark.sql.catalyst.optimizer.ColumnPruning
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project, WithCTE}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.streaming.StreamingRelationV2
import org.apache.spark.sql.catalyst.types.DataTypeUtils
import org.apache.spark.sql.classic.Dataset
import org.apache.spark.sql.connector.catalog.{Identifier, InMemoryBaseTable, InMemoryTableCatalog, SupportsRead, Table, TableCapability}
import org.apache.spark.sql.connector.read.{InputPartition, Scan, ScanBuilder, SupportsPushDownRequiredColumns}
import org.apache.spark.sql.connector.read.streaming.{ContinuousPartitionReader, ContinuousPartitionReaderFactory, ContinuousStream, MicroBatchStream, Offset, PartitionOffset}
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2Relation, MicroBatchScanExec, StreamingDataSourceV2ScanRelation}
import org.apache.spark.sql.execution.streaming.runtime.{MemoryStream, StreamingQueryWrapper, StreamingV2ScanPushDown}
import org.apache.spark.sql.execution.streaming.sources.ContinuousMemoryStream
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{CharType, StringType, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class StreamingV2ScanPushDownSuite extends StreamTest with BeforeAndAfter {
  import testImplicits._

  private val tbl = "testcat.t"

  // The pruning of the scans of streaming queries is disabled by default.
  override protected def sparkConf: SparkConf =
    super.sparkConf.set(SQLConf.STREAMING_V2_COLUMN_PRUNING_ENABLED, true)

  before {
    spark.conf.set("spark.sql.catalog.testcat", classOf[InMemoryTableCatalog].getName)
  }

  after {
    spark.sessionState.catalogManager.reset()
    spark.conf.unset("spark.sql.catalog.testcat")
  }

  /**
   * Creates the in-memory `table` with `columns`, `properties`, and the capability
   * STREAMING_COLUMN_PRUNING, which in-memory tables do not have by default.
   */
  private def createTable(table: String, columns: String, properties: (String, String)*): Unit = {
    val allProperties = ((InMemoryBaseTable.STREAMING_COLUMN_PRUNING -> "true") +: properties).toMap
    sql(s"CREATE TABLE $table ($columns) TBLPROPERTIES (" +
      allProperties.map { case (key, value) => s"'$key' = '$value'" }.mkString(", ") + ")")
  }

  private def withSourceTable(properties: (String, String)*)(f: => Unit): Unit = {
    withTable(tbl) {
      createTable(tbl, "id bigint, data string, extra int", properties: _*)
      sql(s"INSERT INTO $tbl VALUES (1, 'a', 10), (2, 'b', 20)")
      f
    }
  }

  /** The columns that a streaming query needs from each of its sources, when it starts. */
  private def requiredColumns(df: DataFrame): Map[StreamingRelationV2, AttributeSet] =
    StreamingV2ScanPushDown.requiredColumnsOfSources(spark, df.queryExecution.analyzed, None)

  private def insert(values: String, table: String = tbl): StreamAction = Execute { _ =>
    sql(s"INSERT INTO $table VALUES $values")
  }

  private def pruningEnabled(enabled: Boolean): Map[String, String] =
    Map(SQLConf.STREAMING_V2_COLUMN_PRUNING_ENABLED.key -> enabled.toString)

  /**
   * Runs `f`, which restarts streams with other values of the conf of the pruning, and then
   * restores its value: `testStream` restores the value that a conf had before the last
   * `StartStream` that set it, rather than before the first.
   */
  private def withPruningConfRestored(f: => Unit): Unit = withSQLConf(
    SQLConf.STREAMING_V2_COLUMN_PRUNING_ENABLED.key -> "true")(f)

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
        createTable(tbl, "id bigint, json string, extra int")
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
      createTable(tbl, "id bigint, data string, ts timestamp")
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
    withSourceTable(InMemoryBaseTable.SIMULATE_PARTIAL_COLUMN_PRUNING -> "true") {
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

  Seq(
    InMemoryBaseTable.SIMULATE_INCOMPLETE_COLUMN_PRUNING -> "does not have the columns data",
    InMemoryBaseTable.SIMULATE_RETYPED_COLUMN_PRUNING ->
      "has the columns data with other types than the relation outputs them with"
  ).foreach { case (property, reason) =>
    test(s"read all the columns when a scan does not read the required ones: $property") {
      withSourceTable(property -> "true") {
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
            assertScanOutputs(Seq("id", "data", "extra")),
            AssertOnQuery(_ => table.createdMicroBatchStreams.get() == 1),
            CheckAnswer(Row("a"), Row("b")))
        }
        val warnings = logAppender.loggingEvents.map(_.getMessage.getFormattedMessage)
        assert(warnings.exists { message =>
          message.contains("could not be built to read only the columns data") &&
            message.contains(reason) &&
            message.contains("It reads all the columns instead")
        }, warnings)
      }
    }
  }

  test("keep the nullability of the columns of the relation when a scan reports another") {
    withSourceTable(InMemoryBaseTable.SIMULATE_NON_NULLABLE_COLUMN_PRUNING -> "true") {
      sql(s"INSERT INTO $tbl VALUES (3, NULL, 30)")
      testStream(spark.readStream.table(tbl).select("data"))(
        ProcessAllAvailable(),
        AssertOnQuery { query =>
          val readSchemas = query.logicalPlan.collect {
            case relation: StreamingDataSourceV2ScanRelation => relation.scan.readSchema()
          }
          assert(readSchemas.map(_.fields.map(f => f.name -> f.nullable).toSeq) ===
            Seq(Seq("data" -> false)))
          // The plan of each micro-batch is optimized with the columns of the relation, whose
          // nullability the scan node keeps.
          val outputs = query.lastExecution.executedPlan.collect {
            case scan: MicroBatchScanExec => scan.output.map(a => a.name -> a.nullable)
          }
          assert(outputs === Seq(Seq("data" -> true)))
          true
        },
        CheckAnswer(Row("a"), Row("b"), Row(null)))
    }
  }

  test("read the columns of a scan of all the columns in the order of its read schema") {
    withPruningConfRestored {
      withSourceTable(InMemoryBaseTable.SIMULATE_REORDERED_COLUMN_PRUNING -> "true") {
        // The streams of the table return the columns of the read schemas, which are in the
        // reverse order, also when the pruning is disabled.
        Seq(true, false).foreach { enabled =>
          testStream(spark.readStream.table(tbl))(
            StartStream(additionalConfs = pruningEnabled(enabled)),
            ProcessAllAvailable(),
            assertReadSchema("extra", "data", "id"),
            assertScanOutputs(Seq("extra", "data", "id")),
            CheckAnswer(Row(1L, "a", 10), Row(2L, "b", 20)))
        }
      }
    }
  }

  test("read the event-time column of a watermark that the query does not output") {
    withTable(tbl) {
      createTable(tbl, "id bigint, data string, ts timestamp")
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
    withPruningConfRestored {
      val (leftTable, rightTable) = ("testcat.l", "testcat.r")
      withTable(leftTable, rightTable) {
        createTable(leftTable, "k bigint, v bigint, unused int",
          InMemoryBaseTable.SIMULATE_REORDERED_COLUMN_PRUNING -> "true")
        createTable(rightTable, "rk bigint, w int")
        // The left row is kept in the state of the join, which is written without pruning first.
        sql(s"INSERT INTO $leftTable VALUES (1, 100, 0)")
        val left = spark.readStream.table(leftTable)
        val right = spark.readStream.table(rightTable)
        val df = left.join(right, $"k" === $"rk").select("v", "w")
        testStream(df)(
          StartStream(additionalConfs = pruningEnabled(false)),
          ProcessAllAvailable(),
          // The scan of all the columns returns them in the reverse order too.
          assertReadSchemas(Seq("unused", "v", "k"), Seq("rk", "w")),
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
    withPruningConfRestored {
      withTable(tbl) {
        createTable(tbl, "k bigint, v bigint, unused int",
          InMemoryBaseTable.SIMULATE_REORDERED_COLUMN_PRUNING -> "true")
        sql(s"INSERT INTO $tbl VALUES (1, 10, 0), (2, 20, 0)")
        val df = spark.readStream.table(tbl).groupBy("k").agg(sum("v"))
        testStream(df, OutputMode.Complete())(
          StartStream(additionalConfs = pruningEnabled(false)),
          ProcessAllAvailable(),
          assertReadSchema("unused", "v", "k"),
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
          assert(relation.scanOutput.map(_.map(_.name)) === Some(Seq("data")))
          assert(relation.simpleString(10).contains("ScanOutput: [data#"))
          val offset = relation.stream.initialOffset()
          val batch = relation.copy(startOffset = Some(offset), endOffset = Some(offset))
          // Unlike the query, this plan reads all the columns of the relation.
          checkError(
            exception = intercept[SparkException] {
              spark.sessionState.planner.plan(Project(relation.output, batch)).next()
            },
            condition = "STREAMING_SCAN_MISSING_COLUMNS",
            parameters = Map(
              "columns" -> "`id`, `extra`",
              "source" -> "`testcat`.`t`",
              "config" -> s"\"${SQLConf.STREAMING_V2_COLUMN_PRUNING_ENABLED.key}\""))
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

  test("read the columns of every reference to a common table expression") {
    withSourceTable() {
      withTempView("s") {
        spark.readStream.table(tbl).createOrReplaceTempView("s")
        // Both references to the common table expression have the relation of the source, so its
        // scan reads the columns of both.
        val df = sql("WITH c AS (SELECT * FROM s) " +
          "SELECT a.id, b.extra FROM c a JOIN c b ON a.id = b.id WHERE a.data = 'a'")
        testStream(df)(
          ProcessAllAvailable(),
          assertReadSchema("id", "data", "extra"),
          CheckAnswer(Row(1L, 10)),
          insert("(3, 'a', 30)"),
          ProcessAllAvailable(),
          CheckAnswer(Row(1L, 10), Row(3L, 30)))

        // Both references read the column of their filters.
        val pruned = sql("WITH c AS (SELECT * FROM s) " +
          "SELECT data FROM c WHERE extra > 15 UNION ALL SELECT data FROM c WHERE extra < 15")
        testStream(pruned)(
          ProcessAllAvailable(),
          assertReadSchema("data", "extra"),
          CheckAnswer(Row("a"), Row("a"), Row("b")))
      }
    }
  }

  test("read only the columns of a join with a static common table expression") {
    withSourceTable() {
      withTempView("s") {
        spark.readStream.table(tbl).createOrReplaceTempView("s")
        val df = sql("WITH dims AS (SELECT id AS sid FROM range(2, 4)) " +
          "SELECT s.data FROM s JOIN dims ON s.id = dims.sid")
        // The definition of the common table expression stays in the plan optimized when the
        // query starts, with a placeholder of its plan.
        assert(df.queryExecution.analyzed.exists(_.isInstanceOf[WithCTE]))
        assert(requiredColumns(df).values.map(_.map(_.name).toSet).toSeq ===
          Seq(Set("id", "data")))
        testStream(df)(
          ProcessAllAvailable(),
          assertReadSchema("id", "data"),
          CheckAnswer(Row("b")))
      }
    }
  }

  test("read the columns of a condition on a subquery, whatever its plan is") {
    withSourceTable() {
      withTempView("s") {
        spark.readStream.table(tbl).createOrReplaceTempView("s")
        // The plan of the subquery does not fold into the plan optimized when the query starts,
        // as a micro-batch may replace it with the cache, so the scan reads both columns.
        val df = sql("SELECT CASE WHEN (SELECT true) THEN data ELSE CAST(extra AS STRING) END " +
          "AS v FROM s")
        assert(requiredColumns(df).values.map(_.map(_.name).toSet).toSeq ===
          Seq(Set("data", "extra")))
        testStream(df)(
          ProcessAllAvailable(),
          assertReadSchema("data", "extra"),
          CheckAnswer(Row("a"), Row("b")))
        // The nodes of a subquery that reference the columns of the query stay, so the scan reads
        // these columns.
        val correlated = sql("SELECT data FROM s " +
          "WHERE EXISTS (SELECT 1 FROM range(0, 2) r WHERE r.id + 1 = s.id)")
        assert(requiredColumns(correlated).values.map(_.map(_.name).toSet).toSeq ===
          Seq(Set("id", "data")))
        testStream(correlated)(
          ProcessAllAvailable(),
          assertReadSchema("id", "data"),
          CheckAnswer(Row("a"), Row("b")))
      }
    }
  }

  // The values that the optimizer computes from the clock or from the session, which have the
  // pattern CURRENT_LIKE, by which ComputeCurrentTime and ReplaceCurrentLike find them.
  Seq(
    "current_timezone() = 'UTC'",
    "current_date() > DATE'2000-01-01'",
    "curdate() > DATE'2000-01-01'",
    "current_timestamp() > TIMESTAMP'2000-01-01 00:00:00'",
    "now() > TIMESTAMP'2000-01-01 00:00:00'",
    "localtimestamp() > TIMESTAMP_NTZ'2000-01-01 00:00:00'",
    "current_time() >= TIME'00:00:00'",
    "localtime() >= TIME'00:00:00'",
    "current_database() = 'default'",
    "current_schema() = 'default'",
    "current_catalog() = 'spark_catalog'",
    "length(current_path()) >= 0",
    "length(current_user()) >= 0",
    "length(user()) >= 0",
    "length(session_user()) >= 0"
  ).foreach { condition =>
    test(s"read the columns of a condition on a value of the clock or the session: $condition") {
      withSourceTable() {
        // Each micro-batch gets its own value, which may differ from the value when the query
        // starts, so the condition is not folded then, and the scan reads both columns.
        val df = spark.readStream.table(tbl).select(expr(s"CASE WHEN $condition THEN data " +
          "ELSE CAST(extra AS STRING) END AS v"))
        assert(requiredColumns(df).values.map(_.map(_.name).toSet).toSeq ===
          Seq(Set("data", "extra")))
        val holds = sql(s"SELECT $condition").head().getBoolean(0)
        testStream(df)(
          ProcessAllAvailable(),
          assertReadSchema("data", "extra"),
          CheckAnswer((if (holds) Seq("a", "b") else Seq("10", "20")).map(Row(_)): _*))
      }
    }
  }

  Seq(
    "CAST('today' AS DATE) < DATE'2100-01-01'",
    "CAST('12:00:00' AS TIMESTAMP) < TIMESTAMP'2100-01-01 00:00:00'",
    // The casts of strings to arrays, structs and maps of timestamps cast each string.
    "element_at(CAST(array('12:00:00') AS ARRAY<TIMESTAMP>), 1) < TIMESTAMP'2100-01-01 00:00:00'",
    "CAST(named_struct('t', '12:00:00') AS STRUCT<t: TIMESTAMP>).t < " +
      "TIMESTAMP'2100-01-01 00:00:00'",
    "element_at(CAST(map('k', '12:00:00') AS MAP<STRING, TIMESTAMP>), 'k') < " +
      "TIMESTAMP'2100-01-01 00:00:00'",
    // The casts of times to timestamps take the current date, at any depth too.
    "CAST(TIME'12:00:00' AS TIMESTAMP_NTZ) < TIMESTAMP_NTZ'2100-01-01 00:00:00'",
    "element_at(CAST(array(TIME'12:00:00') AS ARRAY<TIMESTAMP>), 1) < " +
      "TIMESTAMP'2100-01-01 00:00:00'",
    // So do the other conversions of strings, and of variants that have them, to timestamps.
    """from_json('{"t": "12:00:00"}', 't TIMESTAMP').t < TIMESTAMP'2100-01-01 00:00:00'""",
    """variant_get(parse_json('"12:00:00"'), '$', 'timestamp') < """ +
      "TIMESTAMP'2100-01-01 00:00:00'",
    """CAST(parse_json('"12:00:00"') AS TIMESTAMP) < TIMESTAMP'2100-01-01 00:00:00'"""
  ).foreach { condition =>
    test(s"read the columns of a condition on a value that depends on when it is converted: " +
        condition) {
      withSourceTable() {
        // The condition takes the date of each micro-batch, so the scan reads both columns.
        val df = spark.readStream.table(tbl).select(expr(s"CASE WHEN $condition THEN data " +
          "ELSE CAST(extra AS STRING) END AS v"))
        val reads = requiredColumns(df)
        assert(reads.values.map(_.map(_.name).toSet).toSeq ===
          Seq(Set("data", "extra")))
        testStream(df)(
          ProcessAllAvailable(),
          assertReadSchema("data", "extra"),
          CheckAnswer(Row("a"), Row("b")))
      }
    }
  }

  test("read the columns of a condition on a cast whose string is a literal only once optimized") {
    withSourceTable() {
      // The cast of `cutoff` is only foldable once the optimizer replaces the alias with the
      // string, and then takes the date of the start of the query, so the condition would be
      // folded with it if the cast stayed a cast.
      val df = spark.readStream.table(tbl).withColumn("cutoff", lit("12:00:00"))
        .select(expr("CASE WHEN CAST(cutoff AS TIMESTAMP) < TIMESTAMP'2100-01-01 00:00:00' " +
          "THEN data ELSE CAST(extra AS STRING) END AS v"))
      val reads = requiredColumns(df)
      assert(reads.values.map(_.map(_.name).toSet).toSeq ===
        Seq(Set("data", "extra")))
      testStream(df)(
        ProcessAllAvailable(),
        assertReadSchema("data", "extra"),
        CheckAnswer(Row("a"), Row("b")))
    }
  }

  test("read all the columns from a table without the capability") {
    withSourceTable(InMemoryBaseTable.STREAMING_COLUMN_PRUNING -> "false") {
      val df = spark.readStream.table(tbl).where("extra > 10").select("data")
      testStream(df)(
        ProcessAllAvailable(),
        assertReadSchema("id", "data", "extra"),
        assertScanOutputs(Seq("id", "data", "extra")),
        CheckAnswer(Row("b")))
    }
  }

  Seq("pruneColumns", "build").foreach { failingStep =>
    test(s"read all the columns when the pruned scan fails: $failingStep") {
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
          // A new scan builder builds a scan of all the columns, as without the pruning.
          assertReadSchema("_1", "_2", "_3"),
          AssertOnQuery(_ => table.streams.asScala.toSeq == Seq(Seq("_1", "_2", "_3"))),
          CheckAnswer(Row("b")))
      }
      val warnings = logs.loggingEvents.map(_.getMessage.getFormattedMessage)
      assert(warnings.exists { message =>
        message.contains("could not be built to read only the columns _2, _3") &&
          message.contains(s"$failingStep failed with the pruning")
      }, warnings)
    }
  }

  test("fail the query when the stream of its pruned scan cannot be created") {
    val input = MemoryStream[(Long, String, Int)]
    val table = new FailingPushDownTable(input, "toMicroBatchStream")
    val df = Dataset.ofRows(spark, StreamingRelationV2(None, "failing_push_down", table,
      CaseInsensitiveStringMap.empty(), DataTypeUtils.toAttributes(table.schema()), None, None,
      None)).toDF("id", "data", "extra").where("extra > 10").select("data")
    testStream(df)(
      StartStream(),
      ExpectFailure[IllegalStateException](e =>
        assert(e.getMessage.contains("toMicroBatchStream failed with the pruning"))))
    // The stream may have started to use the checkpoint of the source when it failed, so no
    // other stream is created for a scan of all the columns.
    assert(table.streamAttempts.get() === 1)
    assert(table.streams.isEmpty)
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

  test("keep the placeholders of different static plans apart") {
    withSourceTable() {
      val df = spark.readStream.table(tbl).select(
        expr("(SELECT max(id) FROM range(3))").as("a"),
        expr("(SELECT max(id) FROM range(4))").as("b"),
        expr("(SELECT max(id) FROM range(3))").as("c"),
        col("data"))
      val placeholders = StreamingV2ScanPushDown.forStartup(df.queryExecution.analyzed)
        .collectFirst { case project: Project => project.projectList.take(3) }.get
        .map(_.collectFirst { case subquery: ScalarSubquery => subquery.plan.canonicalized }.get)
      // The placeholders of different plans differ, although they have the same output, while
      // those of the same plan do not, as the plans in the micro-batches.
      assert(placeholders(0) !== placeholders(1))
      assert(placeholders(0) === placeholders(2))
    }
  }

  test("plan the placeholders in the expressions of other placeholders too") {
    withSourceTable() {
      // The cast of the string is a placeholder whose expression has the placeholder of the
      // current date.
      val df = spark.readStream.table(tbl).select(
        expr("CAST(concat(CAST(current_date() AS STRING), ' 12:00') AS TIMESTAMP)").as("t"),
        col("data"))
      val startup = StreamingV2ScanPushDown.forStartup(df.queryExecution.analyzed)
      assert(!startup.flatMap(_.expressions).forall(_.deterministic))
      // The planner of a micro-batch gets the values of both, which are deterministic.
      val planned = StreamingV2ScanPushDown.forPlanning(startup)
      assert(planned.flatMap(_.expressions).forall(_.deterministic))
    }
  }

  test("fail the start of a query whose optimization for the pruning fails, in tests") {
    // A rule that fails in the optimization when the query starts, whose plan has the relations of
    // the sources, unlike the plans of the micro-batches.
    val failing = new Rule[LogicalPlan] {
      override def apply(plan: LogicalPlan): LogicalPlan = {
        if (plan.exists(_.isInstanceOf[StreamingRelationV2])) {
          throw new IllegalStateException("The optimization at the start failed.")
        }
        plan
      }
    }
    withSourceTable() {
      spark.experimental.extraOptimizations = Seq(failing)
      try {
        // Outside of tests, the scans read all their columns instead, with a warning.
        testStream(spark.readStream.table(tbl).select("data"))(
          StartStream(),
          ExpectFailure[IllegalStateException](e =>
            assert(e.getMessage.contains("The optimization at the start failed."))))
      } finally {
        spark.experimental.extraOptimizations = Nil
      }
    }
  }

  test("read a source whose scans report strings for its char columns") {
    withSQLConf(SQLConf.CHAR_VARCHAR_STANDARD_SEMANTICS.key -> "true") {
      val input = MemoryStream[(Long, String)]
      val table = new StringReadSchemaTable(input)
      val df = Dataset.ofRows(spark, StreamingRelationV2(None, "string_read_schema", table,
        CaseInsensitiveStringMap.empty(), DataTypeUtils.toAttributes(table.schema()), None, None,
        None))
      // The relation outputs a char column, which the read schema of the scan has as a string:
      // the readers return strings for both.
      testStream(df)(
        StartStream(),
        AddData(input, (1L, "a"), (2L, "b")),
        ProcessAllAvailable(),
        assertReadSchema("_1", "_2"),
        CheckAnswerRowsByFunc(rows => assert(rows.map(row => row.getLong(0) ->
          row.getString(1).trim).toSet === Set(1L -> "a", 2L -> "b")), lastOnly = false))
    }
  }

  // The confs with which the plans have char and varchar types, which the analysis rejects
  // otherwise.
  Seq(SQLConf.CHAR_VARCHAR_STANDARD_SEMANTICS, SQLConf.PRESERVE_CHAR_VARCHAR_TYPE_INFO)
    .foreach { conf =>
      test(s"read only the columns of a query from a table with char and varchar columns: " +
          conf.key) {
        withSQLConf(conf.key -> "true") {
          withTable(tbl) {
            createTable(tbl, "id bigint, c char(3), v varchar(5), extra int")
            sql(s"INSERT INTO $tbl VALUES (1, 'a', 'x', 10), (2, 'b', 'y', 20)")
            // The relation of the source outputs the columns with the types char and varchar,
            // while the read schemas of its scans may have strings.
            testStream(spark.readStream.table(tbl).select("c", "v"))(
              ProcessAllAvailable(),
              assertReadSchema("c", "v"),
              CheckAnswer(spark.read.table(tbl).select("c", "v").collect().toSeq: _*))
            val logs = new LogAppender("the read schemas of the scans of char and varchar columns")
            withLogAppender(logs,
                loggerNames = Seq(StreamingV2ScanPushDown.getClass.getName.stripSuffix("$")),
                level = Some(Level.WARN)) {
              Seq(true, false).foreach { enabled =>
                withSQLConf(SQLConf.STREAMING_V2_COLUMN_PRUNING_ENABLED.key -> enabled.toString) {
                  testStream(spark.readStream.table(tbl))(
                    ProcessAllAvailable(),
                    assertReadSchema("id", "c", "v", "extra"),
                    CheckAnswer(spark.read.table(tbl).collect().toSeq: _*))
                }
              }
            }
            // The scan of all the columns follows the relation, with the pruning or without it.
            assert(logs.loggingEvents.isEmpty,
              logs.loggingEvents.map(_.getMessage.getFormattedMessage))
          }
        }
      }
    }

  test("replace the expressions that make dates or timestamps of strings, times or variants") {
    withSourceTable() {
      // Each expression, and whether it is replaced: one whose type has a date or a timestamp, of
      // any precision and time zone, at any depth, made of a string, a time or a variant, which
      // may take the current date, whatever its child is, as a cast or another conversion.
      val expressions = Seq(
        "CAST('12:00:00' AS TIMESTAMP)" -> true,
        "CAST('12:00:00' AS TIMESTAMP_LTZ(9))" -> true,
        "CAST('2100-01-01 12:00:00' AS TIMESTAMP)" -> true,
        "CAST(data AS TIMESTAMP)" -> true,
        "CAST('now' AS TIMESTAMP_NTZ)" -> true,
        "CAST('today' AS DATE)" -> true,
        "CAST(array('12:00:00') AS ARRAY<TIMESTAMP>)" -> true,
        "CAST(named_struct('t', '12:00:00') AS STRUCT<t: TIMESTAMP>)" -> true,
        "CAST(map('k', '12:00:00') AS MAP<STRING, TIMESTAMP>)" -> true,
        "CAST(TIME'12:00:00' AS TIMESTAMP_NTZ)" -> true,
        "CAST(array(TIME'12:00:00') AS ARRAY<TIMESTAMP>)" -> true,
        """CAST(parse_json('"12:00:00"') AS TIMESTAMP)""" -> true,
        """variant_get(parse_json('"12:00:00"'), '$', 'timestamp')""" -> true,
        """from_json('{"t": "12:00:00"}', 't TIMESTAMP')""" -> true,
        "to_timestamp(data, 'HH:mm:ss')" -> true,
        // Neither the conversions of other values, nor the expressions that take the dates and
        // the timestamps of their children.
        "CAST(DATE'2100-01-01' AS TIMESTAMP)" -> false,
        "CAST(1 AS TIMESTAMP)" -> false,
        "CAST(array(1) AS ARRAY<STRING>)" -> false,
        "CAST(TIMESTAMP'2100-01-01 00:00:00' AS STRING)" -> false,
        "named_struct('s', data, 't', TIMESTAMP'2100-01-01 00:00:00')" -> false,
        "date_trunc('DAY', TIMESTAMP'2100-01-01 00:00:00')" -> false)
      val df = spark.readStream.table(tbl)
        .select(expressions.map(e => expr(e._1)) :+ col("data"): _*)
      val replaced = StreamingV2ScanPushDown.forStartup(df.queryExecution.analyzed)
        .collectFirst { case project: Project => project.projectList.take(expressions.length) }.get
        .map(_.asInstanceOf[Alias].child)
      assert(replaced.map(_.nodeName == "StartupValue") === expressions.map(_._2))
    }
  }

  test("read the columns of a query that joins a source with an inline table") {
    withSourceTable() {
      val source = spark.readStream.table(tbl)
      val inline = sql("SELECT * FROM VALUES (2, current_date()) AS s(k, d)")
      val df = source.where("extra > 10").join(inline, source("id") === inline("k"))
        .select("data", "d")
      val reads = requiredColumns(df)
      assert(reads.values.map(_.map(_.name).toSet).toSeq ===
        Seq(Set("id", "data", "extra")))
    }
  }

  test("do not build the scans of the batch relations of a streaming query when it starts") {
    withSourceTable() {
      val table = new CountingBatchTable
      val static = Dataset.ofRows(spark, DataSourceV2Relation.create(
        table, None, None, CaseInsensitiveStringMap.empty()))
      val source = spark.readStream.table(tbl)
      val df = source.join(static, source("id") === static("sid")).select("data")
      val reads = requiredColumns(df)
      assert(reads.values.map(_.map(_.name).toSet).toSeq === Seq(Set("id", "data")))
      assert(table.scanBuilders.get() === 0)
    }
  }

  test("read the columns of a condition on a static side, whatever the cache holds") {
    withSourceTable() {
      val static = spark.range(1, 4).toDF("sid").withColumn("on", lit(false))
      val source = spark.readStream.table(tbl)
      val df = source.join(static, source("id") === static("sid"))
        .select(when(col("on"), col("data")).otherwise(col("extra").cast("string")).as("v"))
      // The static side is opaque when the query starts, so the scan reads both columns of the
      // condition, also when a micro-batch cannot fold it, as with the static side cached after
      // the query started.
      assert(requiredColumns(df).values.map(_.map(_.name).toSet).toSeq ===
        Seq(Set("id", "data", "extra")))
      static.cache()
      try {
        testStream(df)(
          ProcessAllAvailable(),
          assertReadSchemas(Seq("id", "data", "extra")),
          CheckAnswer(Row("10"), Row("20")),
          insert("(3, 'c', 30)"),
          ProcessAllAvailable(),
          CheckAnswer(Row("10"), Row("20"), Row("30")))
      } finally {
        static.unpersist()
      }
    }
  }

  test("fail a query whose scan of all the columns has a read schema that does not follow the " +
      "relation") {
    val input = MemoryStream[(Long, String, Int)]
    val table = new UnknownColumnReadSchemaTable(input)
    val df = Dataset.ofRows(spark, StreamingRelationV2(None, "unknown_column_read_schema", table,
      CaseInsensitiveStringMap.empty(), DataTypeUtils.toAttributes(table.schema()), None, None,
      None)).toDF("id", "data", "extra")
    // The readers of the stream would return the column that the relation does not output, so
    // the scan node could not output the columns of the relation in their order.
    testStream(df)(
      StartStream(),
      ExpectFailure[SparkException] { e =>
        checkError(e.asInstanceOf[SparkException],
          condition = "STREAMING_SCAN_INVALID_READ_SCHEMA",
          parameters = Map(
            "source" -> "`unknown_column_read_schema`",
            "reason" -> ("struct<_1:bigint,_2:string,_3:int,unknown:int> has the columns " +
              "unknown, which the relation does not output"),
            "config" -> s"\"${SQLConf.STREAMING_V2_COLUMN_PRUNING_ENABLED.key}\""))
      })
    // Without the column pruning, the scan node outputs the columns of the relation instead, as
    // it did before the capability.
    withPruningConfRestored {
      testStream(df)(
        StartStream(additionalConfs = pruningEnabled(false)),
        AddData(input, (1L, "a", 10)),
        ProcessAllAvailable(),
        assertScanOutputs(Seq("_1", "_2", "_3")),
        CheckAnswer(Row(1L, "a", 10)))
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

  test("fail a micro-batch that reads other columns once the configuration of the optimizer " +
      "changes in the session of the query") {
    withSourceTable() {
      val static = spark.range(1, 4).toDF("sid")
      val source = spark.readStream.table(tbl)
      val df = source.join(static, source("id") === static("sid")).select("data")
      val batches = new ConcurrentLinkedQueue[Seq[Row]]()
      val query = df.writeStream.foreachBatch { (batch: DataFrame, _: Long) =>
        batches.add(batch.collect().toSeq)
        // The next micro-batches are optimized without ColumnPruning, so their join reads all
        // the columns of the source, while its scan only reads those of the plan optimized
        // when the query started.
        batch.sparkSession.conf.set(
          SQLConf.OPTIMIZER_EXCLUDED_RULES.key, ColumnPruning.ruleName)
      }.start()
      try {
        query.processAllAvailable()
        assert(batches.asScala.toSeq.map(_.map(_.getString(0)).sorted) === Seq(Seq("a", "b")))
        sql(s"INSERT INTO $tbl VALUES (3, 'c', 30)")
        val e = intercept[StreamingQueryException](query.processAllAvailable())
        checkError(
          exception = e.getCause.asInstanceOf[SparkException],
          condition = "STREAMING_SCAN_MISSING_COLUMNS",
          parameters = Map(
            "columns" -> "`extra`",
            "source" -> "`testcat`.`t`",
            "config" -> s"\"${SQLConf.STREAMING_V2_COLUMN_PRUNING_ENABLED.key}\""))
      } finally {
        query.stop()
      }
    }
  }

  test("read all the columns when the pruning of streaming scans is disabled, as by default") {
    assert(!SQLConf.STREAMING_V2_COLUMN_PRUNING_ENABLED.defaultValue.get)
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
 * A table over a memory stream that declares STREAMING_COLUMN_PRUNING, whose scans have a column
 * in their read schema that the table does not have.
 */
private class UnknownColumnReadSchemaTable(input: MemoryStream[_]) extends Table with SupportsRead {
  override def name(): String = "unknown_column_read_schema"

  override def schema(): StructType = input.fullSchema()

  override def capabilities(): util.Set[TableCapability] =
    util.EnumSet.of(TableCapability.MICRO_BATCH_READ, TableCapability.STREAMING_COLUMN_PRUNING)

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = () => new Scan {
    override def readSchema(): StructType = schema().add("unknown", "int")

    override def toMicroBatchStream(checkpointLocation: String): MicroBatchStream = input
  }
}

/**
 * A table over a memory stream of (id, data) that declares STREAMING_COLUMN_PRUNING, whose schema
 * has the column of the data as a char column, while the read schemas of its scans have it as a
 * string.
 */
private class StringReadSchemaTable(input: MemoryStream[_]) extends Table with SupportsRead {
  override def name(): String = "string_read_schema"

  override def schema(): StructType = StructType(input.fullSchema().map { field =>
    if (field.dataType == StringType) field.copy(dataType = CharType(3)) else field
  })

  override def capabilities(): util.Set[TableCapability] =
    util.EnumSet.of(TableCapability.MICRO_BATCH_READ, TableCapability.STREAMING_COLUMN_PRUNING)

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = () => new Scan {
    override def readSchema(): StructType = input.fullSchema()

    override def toMicroBatchStream(checkpointLocation: String): MicroBatchStream = input
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
 * A table over a memory stream, whose scan builder fails in `failingStep` when it gets the pruning
 * of a streaming query: "pruneColumns", "build" or "toMicroBatchStream". It counts the streams
 * that its scans try to create, and records the read schema of each scan whose stream it creates.
 */
private class FailingPushDownTable(input: MemoryStream[_], failingStep: String)
  extends Table with SupportsRead {
  val streamAttempts = new AtomicInteger()
  val streams = new ConcurrentLinkedQueue[Seq[String]]()

  override def name(): String = "failing_push_down"

  override def schema(): StructType = input.fullSchema()

  override def capabilities(): util.Set[TableCapability] = util.EnumSet.of(
    TableCapability.MICRO_BATCH_READ,
    TableCapability.STREAMING_COLUMN_PRUNING)

  private def failIf(step: String, pruned: Boolean): Unit = {
    if (step == failingStep && pruned) {
      throw new IllegalStateException(s"$step failed with the pruning")
    }
  }

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    new ScanBuilder with SupportsPushDownRequiredColumns {
      private var columns = schema()

      private def pruned: Boolean = columns.length < schema().length

      override def pruneColumns(requiredSchema: StructType): Unit = {
        failIf("pruneColumns", requiredSchema.length < schema().length)
        columns = requiredSchema
      }

      override def build(): Scan = {
        failIf("build", pruned)
        val (readColumns, readPruned) = (columns, pruned)
        new Scan {
          override def readSchema(): StructType = readColumns

          override def toMicroBatchStream(checkpointLocation: String): MicroBatchStream = {
            streamAttempts.incrementAndGet()
            failIf("toMicroBatchStream", readPruned)
            streams.add(readColumns.fieldNames.toSeq)
            input
          }
        }
      }
    }
  }
}
