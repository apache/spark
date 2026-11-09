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

import java.time.LocalTime
import java.util
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}

import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}

import org.apache.logging.log4j.Level
import org.scalatest.BeforeAndAfter

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.sql.{DataFrame, Encoder, Encoders, Row}
import org.apache.spark.sql.catalyst.{InternalRow, ProjectingInternalRow, StructFilters}
import org.apache.spark.sql.catalyst.expressions.{Add, Alias, And, ArrayContains, AttributeReference, AttributeSet, BoundReference, Cast, Divide, EqualTo, EvalMode, Expression, GetArrayItem, GetStructField, GreaterThan, IsNotNull, LeafExpression, LessThan, Literal, Or, Pow, Predicate => CatalystPredicate, Rand, RLike, ScalarSubquery, ScalaUDF, TypeOf}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.optimizer.{ColumnPruning, InferFiltersFromConstraints, InferFiltersFromGenerate, PruneFilters}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan, Project, WithCTE}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.streaming.StreamingRelationV2
import org.apache.spark.sql.catalyst.types.DataTypeUtils
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.classic.{Dataset, ExpressionUtils, SparkSession}
import org.apache.spark.sql.connector.catalog.{Identifier, InMemoryBaseTable, InMemoryTableCatalog, SupportsRead, Table, TableCapability}
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory, Scan, ScanBuilder, SupportsPushDownFilters, SupportsPushDownRequiredColumns, SupportsPushDownV2Filters}
import org.apache.spark.sql.connector.read.streaming.{ContinuousPartitionReader, ContinuousPartitionReaderFactory, ContinuousStream, MicroBatchStream, Offset, PartitionOffset}
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2Relation, DataSourceV2Strategy, MicroBatchScanExec, StreamingDataSourceV2ScanRelation}
import org.apache.spark.sql.execution.streaming.runtime.{MemoryStream, MemoryStreamBaseClass, StreamExecution, StreamingQueryWrapper, StreamingV2ScanPushDown}
import org.apache.spark.sql.execution.streaming.sources.ContinuousMemoryStream
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.connector.{PredicateUtils, SupportsPushDownCatalystFilters}
import org.apache.spark.sql.sources.{Filter => SourceFilter}
import org.apache.spark.sql.types.{ArrayType, BooleanType, CharType, DataType, DoubleType, IntegerType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.unsafe.types.UTF8String

class StreamingV2ScanPushDownSuite extends StreamTest with BeforeAndAfter {
  import testImplicits._

  private val tbl = "testcat.t"

  // The pushdowns to the scans of streaming queries are disabled by default.
  override protected def sparkConf: SparkConf = super.sparkConf
    .set(SQLConf.STREAMING_V2_COLUMN_PRUNING_ENABLED, true)
    .set(SQLConf.STREAMING_V2_FILTER_PUSHDOWN_ENABLED, true)

  before {
    spark.conf.set("spark.sql.catalog.testcat", classOf[InMemoryTableCatalog].getName)
  }

  after {
    spark.sessionState.catalogManager.reset()
    spark.conf.unset("spark.sql.catalog.testcat")
  }

  // Streams that drop the rows that fail the pushed filters.
  private val filtering = InMemoryBaseTable.SIMULATE_STREAMING_FILTERS -> "true"

  /**
   * Creates the in-memory `table` with `columns`, `properties`, and the capabilities
   * STREAMING_COLUMN_PRUNING and STREAMING_FILTER_PUSHDOWN, which in-memory tables
   * do not have by default.
   */
  private def createTable(table: String, columns: String, properties: (String, String)*): Unit = {
    val allProperties = (Seq(InMemoryBaseTable.STREAMING_COLUMN_PRUNING -> "true",
      InMemoryBaseTable.STREAMING_FILTER_PUSHDOWN -> "true") ++ properties).toMap
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

  /** What a streaming query reads from each of its sources, when it starts. */
  private def reads(df: DataFrame): Map[StreamingRelationV2, StreamingV2ScanPushDown.Reads] =
    StreamingV2ScanPushDown.columnsAndFiltersOfSources(spark, df.queryExecution.analyzed, None)

  /** The columns that a streaming query needs from each of its sources, when it starts. */
  private def requiredColumns(df: DataFrame): Map[StreamingRelationV2, AttributeSet] =
    reads(df).map { case (relation, read) => relation -> read.requiredColumns }

  // The columns of each filter that all the reads of the sources of `df` apply first.
  private def pushed(df: DataFrame): Seq[Set[String]] =
    reads(df).values.toSeq.flatMap(_.filters).map(_.references.map(_.name).toSet)

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

  /**
   * Checks that the last micro-batch evaluates the filters on `columns` once, apart from the
   * IsNotNull that the optimizer infers.
   */
  private def assertEvaluatedOnce(columns: String*): StreamAction = AssertOnQuery { query =>
    val conditions = query.lastExecution.optimizedPlan.collect {
      case filter: Filter => filter.condition
    }.flatMap(splitConjunctivePredicates).filter { condition =>
      val names = condition.references.map(_.name).toSet
      names.nonEmpty && names.subsetOf(columns.toSet)
    }.filterNot(_.isInstanceOf[IsNotNull])
    assert(conditions.length === 1, conditions)
    true
  }

  // A static side with one date, DATE'2026-01-01', which a micro-batch infers for the column that
  // a join compares with it.
  private def staticDate: DataFrame =
    spark.range(0, 1).selectExpr("DATE'2026-01-01' + CAST(id AS INT) AS sd")
      .where("sd = DATE'2026-01-01'")

  /**
   * Checks that no filter that the last micro-batch applies to the rows of its streaming sources
   * has `sql` in its SQL.
   */
  private def assertNotOverSources(sql: String): StreamAction = AssertOnQuery { query =>
    val conditions = query.lastExecution.optimizedPlan.collect {
      case Filter(condition, _: StreamingDataSourceV2ScanRelation) =>
        splitConjunctivePredicates(condition)
    }.flatten
    assert(conditions.nonEmpty && !conditions.exists(_.sql.contains(sql)), conditions)
    true
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
          message.contains("could not be built with (columns: data, filters: none): ") &&
            message.contains(reason) &&
            message.contains("Trying (columns: all, filters: none) next.")
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
    withPushDownConfsRestored {
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
    withPushDownConfsRestored {
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
    withPushDownConfsRestored {
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
    val table = new PruningContinuousTable(input, takesFilters = false)
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
    // The suite enables the filter pushdown, which is disabled by default.
    assert(!SQLConf.STREAMING_V2_FILTER_PUSHDOWN_ENABLED.defaultValue.get)
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
    "data = current_timezone()" -> "data",
    // The casts of the special strings, which SpecialDatetimeValues folds.
    "d = 'today'" -> "d",
    "ts < 'now'" -> "ts",
    "d = to_date('today')" -> "d",
    "d < CAST('tomorrow' AS DATE)" -> "d",
    "CAST(ts AS TIMESTAMP_NTZ) > CAST('yesterday' AS TIMESTAMP_NTZ)" -> "ts",
    // The casts of strings with only a time to timestamps, which take the current date.
    "ts < '12:00:00'" -> "ts",
    "ts > CAST('T09:30:00' AS TIMESTAMP)" -> "ts",
    // Every cast of a string to a timestamp with local time zone is a placeholder when the query
    // starts, also of a string with a date, or of a string column. A follow-up could push these
    // down again when their values do not depend on the date.
    "CAST(data AS TIMESTAMP) < TIMESTAMP'2100-01-01 00:00:00'" -> "data",
    "ts < '2100-01-01 12:00:00'" -> "ts"
  ).foreach { case (condition, column) =>
    test(s"do not push down a filter on a value that depends on when it is evaluated: $condition") {
      withTable(tbl) {
        createTable(tbl, "id bigint, data string, ts timestamp, tod time, d date")
        val df = spark.readStream.table(tbl).where(s"id > 1 AND $condition").select("id")
        val read = reads(df).values.toSeq match { case Seq(read) => read }
        // The column of the condition is still read, but only the filter on the id is pushed down.
        assert(read.requiredColumns.map(_.name).toSet === Set("id", column))
        assert(read.filters.nonEmpty)
        assert(read.filters.forall(_.references.map(_.name).toSet == Set("id")), read.filters)
      }
    }
  }

  test("do not push down a filter on the current time of a micro-batch") {
    withTable(tbl) {
      createTable(tbl, "id bigint, data string, ts timestamp")
      sql(s"INSERT INTO $tbl VALUES " +
        "(1, 'a', timestamp_seconds(0)), (2, 'b', timestamp_seconds(10))")
      // Each micro-batch has its own current time, so the time when the query started cannot be
      // pushed down in its place.
      Seq("current_timestamp()", "now()").foreach { time =>
        val df = spark.readStream.table(tbl).where(s"id > 0 AND ts < $time").select("data")
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
        createTable(otherTable, "oid bigint, value string")
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
        createTable(otherTable, "oid bigint, value string")
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
        // Or into false, with id > 5, which the micro-batch keeps over the relation: it keeps no
        // row, so it implies any filter.
        val none = source.where("id > 5").join(static, source("id") === static("sid"))
        testStream(none.select("data"))(
          ProcessAllAvailable(),
          assertPushedFilters("GreaterThan(id,5)"),
          CheckAnswer())
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

  test("plan the micro-batches that fold a value into a pushed filter") {
    withTable(tbl) {
      createTable(tbl, "id bigint, owner string, d date")
      sql(s"INSERT INTO $tbl VALUES (1, current_user(), DATE'2026-01-01'), (2, 'other', NULL)")
      // A micro-batch folds current_user() into a literal, which it then puts in place of the
      // column in the pushed filter owner <> ''.
      testStream(spark.readStream.table(tbl).where("owner <> '' AND owner = current_user()")
          .select("id"))(
        ProcessAllAvailable(),
        assertPushedFilters("Not(EqualTo(owner,))"),
        assertNotOverSources("<> ''"),
        CheckAnswer(Row(1L)))
      // And a date that it infers through a join with a static side, in d >= DATE'2020-01-01'.
      val source = spark.readStream.table(tbl).where("d >= DATE'2020-01-01'")
      val dates = staticDate
      testStream(source.join(dates, source("d") === dates("sd")).select("id"))(
        ProcessAllAvailable(),
        assertPushedFilters("GreaterThanOrEqual(d,2020-01-01)"),
        assertNotOverSources("2020-01-01"),
        CheckAnswer(Row(1L)))
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

  test("push down no filter that may fail after a filter that is not pushed down") {
    withTable(tbl) {
      createTable(tbl, "id bigint, owner string, qty int, payload string")
      sql(s"INSERT INTO $tbl VALUES (1, current_user(), 1, '1'), (2, 'other', 0, 'abc')")
      val raise = "CASE WHEN qty <= 0 THEN raise_error('bad qty') ELSE true END"
      val source = spark.readStream.table(tbl)
      // The filter on the owner has a value of each micro-batch, so it is not pushed down, and
      // neither is a filter written after it that may fail: the streams may evaluate the pushed
      // filters first, on the rows of the source, so a filter that fails on the row of the other
      // owner, as the cast of its payload does with ANSI, or the error raised for its quantity 0,
      // would fail the query.
      Seq(raise -> Nil, "CAST(payload AS INT) > 0" -> Nil, "id > 0" -> Seq("GreaterThan(id,0)"))
        .foreach { case (filter, taken) =>
          val df = source.where(s"owner = current_user() AND $filter")
          assert(pushed(df) === Nil, filter)
          // The filters of the V1 builder of the in-memory tables compare columns with values,
          // which cannot fail, so it gets id > 0 wherever it is. It cannot take the others.
          testStream(df.select("id"))(
            ProcessAllAvailable(),
            assertPushedFilters(taken: _*),
            CheckAnswer(Row(1L)))
        }
      // The filters written before it are pushed down, as the query evaluates them first anyway.
      assert(pushed(source.where(s"$raise AND id > 0 AND owner = current_user() AND qty > 0")) ===
        Seq(Set("qty"), Set("id")))
      // With several reads of the relation, the pushed filters are those that all of them
      // evaluate first, in the same order.
      val read = source.where(s"$raise AND id > 0")
      assert(pushed(read.union(read)) === Seq(Set("qty"), Set("id")))
      assert(pushed(read.union(source.where(s"$raise AND qty > 0"))) === Seq(Set("qty")))
      assert(pushed(read.union(source.where(s"id > 0 AND $raise"))) === Nil)
    }
  }

  test("drop the rows that fail the pushed filters in the stream of a source") {
    withSourceTable(filtering) {
      // The stream only returns the rows that pass the pushed filter, so the progress of the query
      // only counts them.
      def inputRows(query: StreamExecution): Long = query.recentProgress.map(_.numInputRows).sum
      testStream(spark.readStream.table(tbl).where("extra > 10").select("data"))(
        ProcessAllAvailable(),
        assertPushedFilters("GreaterThan(extra,10)"),
        CheckAnswer(Row("b")),
        AssertOnQuery(inputRows(_) == 1),
        insert("(3, 'c', 30), (4, 'd', 5), (5, 'e', 50)"),
        ProcessAllAvailable(),
        CheckAnswer(Row("b"), Row("c"), Row("e")),
        AssertOnQuery(inputRows(_) == 3))
    }
  }

  test("filter the rows of a continuous stream with the filters that its scan builder took") {
    val input = ContinuousMemoryStream.singlePartition[(Long, String, Int)]
    val table = new PruningContinuousTable(input, takesFilters = true)
    val df = Dataset.ofRows(spark, StreamingRelationV2(None, "pruning_continuous", table,
      CaseInsensitiveStringMap.empty(), DataTypeUtils.toAttributes(table.schema()), None, None,
      None)).toDF("id", "data", "extra").where("extra > 10").select("data")
    PruningContinuousTable.returnedRows.set(0)
    withTempView("continuous_filtering") {
      val query = df.writeStream.format("memory").queryName("continuous_filtering")
        .trigger(Trigger.Continuous(100)).start()
      try {
        input.addData((1L, "a", 10), (2L, "b", 20), (3L, "c", 30))
        eventually(timeout(streamingTimeout)) {
          checkAnswer(spark.table("continuous_filtering"), Seq(Row("b"), Row("c")))
        }
        // The readers dropped the row that fails the filter, which the plan of the query applies
        // too.
        assert(PruningContinuousTable.returnedRows.get() === 2)
        val filtered = query.asInstanceOf[StreamingQueryWrapper].streamingQuery.logicalPlan
          .collect {
            case relation: StreamingDataSourceV2ScanRelation =>
              relation.pushedFilters.map(_.references.map(_.name).toSet)
          }
        assert(filtered === Seq(Seq(Set("_3"))))
      } finally {
        query.stop()
      }
    }
  }

  test("plan the micro-batches whose filters fold the pushed filters in part") {
    // The streams drop the rows that fail the pushed filters, so the answers depend on them.
    withTable(tbl) {
      createTable(tbl, "id bigint, owner string, d date", filtering)
      sql(s"INSERT INTO $tbl VALUES (1, current_user(), DATE'2026-01-01'), " +
        "(6, current_user(), DATE'2026-01-01'), (7, 'other', DATE'2026-01-01')")
      // A micro-batch folds current_user() into a literal, which it puts in place of the column
      // in the pushed filter owner = '' OR id > 5, which it then folds into id > 5.
      testStream(spark.readStream.table(tbl)
          .where("(owner = '' OR id > 5) AND owner = current_user()").select("id"))(
        ProcessAllAvailable(),
        assertPushedFilters("Or(EqualTo(owner,),GreaterThan(id,5))"),
        CheckAnswer(Row(6L)))
      // So does a date that it infers through a join with a static side, which it puts in place of
      // the column in d < DATE'2000-01-01' OR id > 5, which it then folds into id > 5.
      val source = spark.readStream.table(tbl).where("d < DATE'2000-01-01' OR id > 5")
      val dates = staticDate
      testStream(source.join(dates, source("d") === dates("sd")).select("id"))(
        ProcessAllAvailable(),
        assertPushedFilters("Or(LessThan(d,2000-01-01),GreaterThan(id,5))"),
        assertNotOverSources("2000-01-01"),
        CheckAnswer(Row(6L), Row(7L)))
    }
    // And an equality that a micro-batch infers through a stream-static join.
    withTable(tbl) {
      createTable(tbl, "a bigint, b bigint", filtering)
      sql(s"INSERT INTO $tbl VALUES (2, 0), (0, 0), (5, 3)")
      val source = spark.readStream.table(tbl).where("a > 1 OR b > 2")
      val static = spark.range(0, 2).toDF("x").where("x = 0")
      testStream(source.join(static, source("b") === static("x")).select("a"))(
        ProcessAllAvailable(),
        assertPushedFilters("Or(GreaterThan(a,1),GreaterThan(b,2))"),
        CheckAnswer(Row(2L)))
      // Which may fold a pushed filter into true, so that the micro-batch no longer has it: b < 3
      // with b = 0.
      val folded = spark.readStream.table(tbl).where("b < 3")
      testStream(folded.join(static, folded("b") === static("x")).select("a"))(
        ProcessAllAvailable(),
        assertPushedFilters("LessThan(b,3)"),
        assertNotOverSources("< 3"),
        CheckAnswer(Row(2L), Row(0L)))
    }
    // Also on a struct key, whose value the micro-batch puts in place of the column in k.a > 0.
    withTable(tbl) {
      createTable(tbl, "k struct<a: int>, v string", filtering)
      sql(s"INSERT INTO $tbl VALUES (named_struct('a', 1), 'x'), (named_struct('a', 0), 'y')")
      val source = spark.readStream.table(tbl).where("k.a > 0")
      val static = spark.range(0, 1).selectExpr("named_struct('a', CAST(id AS INT) + 1) AS sk")
        .where("sk = named_struct('a', 1)")
      testStream(source.join(static, source("k") === static("sk")).select("v"))(
        ProcessAllAvailable(),
        assertPushedFilters("GreaterThan(k.a,0)"),
        assertNotOverSources("> 0"),
        CheckAnswer(Row("x")))
      // Also when the fields of the struct of the static side have other names, as the join
      // compares structs by their fields in order.
      val other = spark.range(0, 1).selectExpr("named_struct('x', CAST(id AS INT) + 1) AS sk")
        .where("sk = named_struct('x', 1)")
      testStream(source.join(other, source("k") === other("sk")).select("v"))(
        ProcessAllAvailable(),
        assertPushedFilters("GreaterThan(k.a,0)"),
        assertNotOverSources("> 0"),
        CheckAnswer(Row("x")))
    }
    // A struct key with a double, and a double key, get their values in the micro-batches too, but
    // not in their check, as a double equal to 0.0 may be -0.0, so their filters are not pushed
    // down: the micro-batches fold them in Spark.
    withTable(tbl) {
      createTable(tbl, "k struct<a: double>, v string", filtering)
      sql(s"INSERT INTO $tbl VALUES (named_struct('a', 1.0D), 'x'), " +
        "(named_struct('a', -1.0D), 'y')")
      val source = spark.readStream.table(tbl).where("k.a > 0")
      val static = spark.range(0, 1)
        .selectExpr("named_struct('a', CAST(id AS DOUBLE) + 1.0D) AS sk")
        .where("sk = named_struct('a', 1.0D)")
      testStream(source.join(static, source("k") === static("sk")).select("v"))(
        ProcessAllAvailable(),
        assertPushedFilters(),
        assertNotOverSources("> 0"),
        CheckAnswer(Row("x")))
    }
    withTable(tbl) {
      createTable(tbl, "d double, v string", filtering)
      sql(s"INSERT INTO $tbl VALUES (0.0, 'a'), (CAST('-0.0' AS DOUBLE), 'b'), (1.0, 'c')")
      val source = spark.readStream.table(tbl).where("d > -1.0")
      val static = spark.range(0, 1).selectExpr("CAST(id AS DOUBLE) AS x").where("x = 0.0")
      testStream(source.join(static, source("d") === static("x")).select("v"))(
        ProcessAllAvailable(),
        assertPushedFilters(),
        assertNotOverSources("> -1.0"),
        CheckAnswer(Row("a"), Row("b")))
    }
  }

  test("plan the micro-batches whose optimizer rewrites a pushed filter with an inferred value") {
    val input = new FilteringMemoryStream(spark)
    val table = new FilterStreamTable(input, catalyst = true)
    val source = Dataset.ofRows(spark, StreamingRelationV2(None, table.name(), table,
      CaseInsensitiveStringMap.empty(), DataTypeUtils.toAttributes(table.schema()), None, None,
      None)).toDF("id", "payload").where("CAST(length(payload) AS BIGINT) < id")
    val static = spark.range(5, 6).toDF("k").where("k = 5")
    // The plans that the check optimizes: a filter over a relation without its pushed filters,
    // unlike those of the micro-batches.
    val checks = new AtomicInteger()
    spark.experimental.extraOptimizations = Seq(new Rule[LogicalPlan] {
      override def apply(plan: LogicalPlan): LogicalPlan = {
        plan match {
          case Filter(_, relation: StreamingDataSourceV2ScanRelation)
              if relation.pushedFilters.isEmpty => checks.incrementAndGet()
          case _ =>
        }
        plan
      }
    })
    try {
      // A micro-batch infers id = 5 through the join, and the optimizer then unwraps the cast of
      // the filter that the builder got, so the micro-batch has length(payload) < 5. The check
      // optimizes the pushed filter with the value once, for all the micro-batches.
      var checksOfFirstBatch = 0
      testStream(source.join(static, source("id") === static("k")).select("payload"))(
        StartStream(),
        AddData(input, (5L, "abc"), (5L, "abcdef"), (3L, "a")),
        ProcessAllAvailable(),
        AssertOnQuery(_ => table.pushed.length == 1 && table.pushed.head.contains("CAST")),
        assertNotOverSources("CAST"),
        CheckAnswer(Row("abc")),
        AssertOnQuery { _ =>
          checksOfFirstBatch = checks.get()
          checksOfFirstBatch > 0
        },
        AddData(input, (5L, "ab")),
        ProcessAllAvailable(),
        AddData(input, (5L, "a")),
        ProcessAllAvailable(),
        CheckAnswer(Row("abc"), Row("ab"), Row("a")),
        AssertOnQuery(_ => checks.get() == checksOfFirstBatch),
        // A query that restarts rewrites it again, as the rewrites belong to the query.
        StopStream,
        StartStream(),
        AddData(input, (5L, "abcd")),
        ProcessAllAvailable(),
        AssertOnQuery(_ => checks.get() > checksOfFirstBatch))
    } finally {
      spark.experimental.extraOptimizations = Nil
    }
  }

  test("fail the query when the read schema of a scan of all the columns with filters does not " +
      "follow the relation") {
    val input = MemoryStream[(Long, String, Int)]
    val table = new UnknownColumnReadSchemaTable(input, takesFilters = true)
    val df = Dataset.ofRows(spark, StreamingRelationV2(None, "unknown_column_read_schema", table,
      CaseInsensitiveStringMap.empty(), DataTypeUtils.toAttributes(table.schema()), None, None,
      None)).toDF("id", "data", "extra").where("extra > 10")
    val logs = new LogAppender("the read schema of a scan of all the columns with filters")
    withLogAppender(logs,
        loggerNames = Seq(StreamingV2ScanPushDown.getClass.getName.stripSuffix("$")),
        level = Some(Level.WARN)) {
      testStream(df)(
        StartStream(),
        ExpectFailure[SparkException] { e =>
          assert(e.asInstanceOf[SparkException].getCondition ===
            "STREAMING_SCAN_INVALID_READ_SCHEMA")
        })
    }
    // The read schema fails the query as that of the scan with no filter would, rather than the
    // scan with no filter being built again in its place.
    val warnings = logs.loggingEvents.map(_.getMessage.getFormattedMessage)
    assert(!warnings.exists(_.contains("could not be built")), warnings)
    assert(table.pushed == Seq("GreaterThan(_3,10)"))
  }

  test("read all the columns with no filter from a table without the capabilities") {
    withSourceTable(InMemoryBaseTable.STREAMING_COLUMN_PRUNING -> "false",
        InMemoryBaseTable.STREAMING_FILTER_PUSHDOWN -> "false") {
      val df = spark.readStream.table(tbl).where("extra > 10").select("data")
      testStream(df)(
        ProcessAllAvailable(),
        assertReadSchema("id", "data", "extra"),
        assertScanOutputs(Seq("id", "data", "extra")),
        assertPushedFilters(),
        CheckAnswer(Row("b")))
    }
  }

  test("push down only what the capabilities of the table allow") {
    val df = () => spark.readStream.table(tbl).where("extra > 10").select("data")
    withSourceTable(InMemoryBaseTable.STREAMING_FILTER_PUSHDOWN -> "false") {
      testStream(df())(
        ProcessAllAvailable(),
        assertReadSchema("data", "extra"),
        assertPushedFilters(),
        CheckAnswer(Row("b")))
    }
    withSourceTable(InMemoryBaseTable.STREAMING_COLUMN_PRUNING -> "false") {
      testStream(df())(
        ProcessAllAvailable(),
        assertReadSchema("id", "data", "extra"),
        assertPushedFilters("GreaterThan(extra,10)"),
        CheckAnswer(Row("b")))
    }
  }

  Seq(
    // The column pruning alone works when the filters fail.
    "pushFilters" -> (Seq("_2", "_3") -> Nil),
    // The filters alone work when the column pruning fails.
    "pruneColumns" -> (Seq("_1", "_2", "_3") -> Seq("GreaterThan(_3,10)")),
    // Neither works when any pushdown fails the build, so the scan reads all the columns with no
    // filter, as without the pushdowns.
    "build" -> (Seq("_1", "_2", "_3") -> Nil),
    // Nor when the read schema cannot be read with any pushdown, also that of the scan of all the
    // columns with the filters, which the filters may make fail.
    "readSchema" -> (Seq("_1", "_2", "_3") -> Nil)
  ).foreach { case (failingStep, (columns, filters)) =>
    test(s"read with fewer pushdowns when the scan fails with the pushdowns: $failingStep") {
      val input = new ProjectingMemoryStream(spark)
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
          // Each new scan builder builds the scan with fewer pushdowns: the column pruning
          // alone, then the filters alone, and then all the columns with no filter.
          assertReadSchema(columns: _*),
          AssertOnQuery(_ => table.streams.asScala.toSeq == Seq(columns -> filters)),
          CheckAnswer(Row("b")))
      }
      val warnings = logs.loggingEvents.map(_.getMessage.getFormattedMessage)
      // Whether a warning says that the scan with `pushdowns` failed, and which one is next.
      def failed(pushdowns: String, next: String): Boolean = warnings.exists { message =>
        message.contains(s"could not be built with $pushdowns: ") &&
          message.contains(s"$failingStep failed with the pushdowns") &&
          message.contains(s"Trying $next next.")
      }
      val filter = "filters: (_3 > 10)"
      assert(failed(s"(columns: _2, _3, $filter)", "(columns: _2, _3, filters: none)"), warnings)
      assert(failed("(columns: _2, _3, filters: none)", s"(columns: all, $filter)") ===
        (failingStep != "pushFilters"), warnings)
      assert(failed(s"(columns: all, $filter)", "(columns: all, filters: none)") ===
        Set("build", "readSchema").contains(failingStep), warnings)
    }
  }

  test("fail the query when the stream of its scan with the pushdowns cannot be created") {
    val input = new ProjectingMemoryStream(spark)
    val table = new FailingPushDownTable(input, "toMicroBatchStream")
    val df = Dataset.ofRows(spark, StreamingRelationV2(None, "failing_push_down", table,
      CaseInsensitiveStringMap.empty(), DataTypeUtils.toAttributes(table.schema()), None, None,
      None)).toDF("id", "data", "extra").where("extra > 10").select("data")
    testStream(df)(
      StartStream(),
      ExpectFailure[IllegalStateException](e =>
        assert(e.getMessage.contains("toMicroBatchStream failed with the pushdowns"))))
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

  Seq(true, false).foreach { failOnError =>
    test(s"handle a failure of the optimization when the query starts: $failOnError") {
      // A rule that fails in the optimization when the query starts, whose plan has the relations
      // of the sources, unlike the plans of the micro-batches.
      val failing = new Rule[LogicalPlan] {
        override def apply(plan: LogicalPlan): LogicalPlan = {
          if (plan.exists(_.isInstanceOf[StreamingRelationV2])) {
            throw new IllegalStateException("The optimization at the start failed.")
          }
          plan
        }
      }
      // The tests fail on a failure of the optimization at the start by default.
      assert(SQLConf.STREAMING_V2_SCAN_PUSHDOWN_FAIL_ON_ERROR.defaultValue.contains(true))
      withSourceTable() {
        spark.experimental.extraOptimizations = Seq(failing)
        val logs = new LogAppender("the failure of the optimization at the start")
        try {
          val failOnErrorConf = SQLConf.STREAMING_V2_SCAN_PUSHDOWN_FAIL_ON_ERROR.key
          withSQLConf(failOnErrorConf -> failOnError.toString) {
            val df = spark.readStream.table(tbl).where("extra > 10").select("data")
            if (failOnError) {
              testStream(df)(
                StartStream(),
                ExpectFailure[IllegalStateException](e =>
                  assert(e.getMessage.contains("The optimization at the start failed."))))
            } else {
              // As outside of the tests, the scan reads all the columns with no filter instead.
              withLogAppender(logs,
                  loggerNames = Seq(StreamingV2ScanPushDown.getClass.getName.stripSuffix("$")),
                  level = Some(Level.WARN)) {
                testStream(df)(
                  ProcessAllAvailable(),
                  assertReadSchema("id", "data", "extra"),
                  assertPushedFilters(),
                  CheckAnswer(Row("b")))
              }
              assert(logs.loggingEvents.exists(_.getMessage.getFormattedMessage.contains(
                "Could not optimize the streaming query to push down what it reads")))
            }
          }
        } finally {
          spark.experimental.extraOptimizations = Nil
        }
      }
    }
  }

  test("fail a micro-batch whose query no longer applies the filters that its scan got") {
    withSourceTable() {
      // The filter has a value that changes each time that it is evaluated, which the optimization
      // at the start folds, as it would fold a function of the time that it does not replace.
      ChangingValue.values.set(0)
      val df = spark.readStream.table(tbl)
        .where(col("extra") < ExpressionUtils.column(ChangingValue())).select("data")
      // The scan drops the rows that fail extra < 1, while the plan of the micro-batch has a
      // larger value, so it would keep some of them: the micro-batch fails before it reads them.
      testStream(df)(
        StartStream(),
        ExpectFailure[SparkException] { e =>
          checkError(e.asInstanceOf[SparkException],
            condition = "STREAMING_SCAN_FILTERS_CHANGED",
            parameters = Map(
              "filters" -> "\"(extra < 1)\"",
              "source" -> "`testcat`.`t`",
              "config" -> s"\"${SQLConf.STREAMING_V2_FILTER_PUSHDOWN_ENABLED.key}\""))
        })
    }
  }

  Seq(false, true).foreach { catalyst =>
    val kind = if (catalyst) "Catalyst" else "V2"
    test(s"fail a micro-batch whose query no longer applies the filters of a $kind builder") {
      ChangingValue.values.set(0)
      val input = new FilteringMemoryStream(spark)
      val table = new FilterStreamTable(input, catalyst)
      val df = Dataset.ofRows(spark, StreamingRelationV2(None, table.name(), table,
        CaseInsensitiveStringMap.empty(), DataTypeUtils.toAttributes(table.schema()), None, None,
        None)).toDF("id", "payload")
        .where(col("id") < ExpressionUtils.column(ChangingValue())).select("payload")
      // The stream drops the rows that fail id < 1, while the plan of the micro-batch would keep
      // those of id < 2.
      testStream(df)(
        StartStream(),
        AddData(input, (1L, "a")),
        ExpectFailure[SparkException] { e =>
          assert(e.asInstanceOf[SparkException].getCondition === "STREAMING_SCAN_FILTERS_CHANGED")
        })
      assert(table.pushed.length === 1, table.pushed)
    }
  }

  test("check that the plan of each micro-batch implies the filters that its scans got") {
    withTable(tbl) {
      createTable(tbl,
        "id bigint, data string, d double, e int, t time, k struct<a: int>, xs array<int>, " +
          "f struct<a: double>, ks array<struct<a: int>>")
      // A relation of the source, whose pushed filters each case sets.
      var relation: StreamingDataSourceV2ScanRelation = null
      testStream(spark.readStream.table(tbl))(
        ProcessAllAvailable(),
        AssertOnQuery { query =>
          relation = query.logicalPlan.collectFirst {
            case relation: StreamingDataSourceV2ScanRelation => relation
          }.get
          true
        })
      val Seq(id, data, d, e, t, k, xs, f, ks) = relation.output
      // Checks the plan of a micro-batch that `plan` makes of the relation, once the scan of the
      // relation got `pushed`.
      def check(pushed: Expression)(plan: LogicalPlan => LogicalPlan): Unit = {
        StreamingV2ScanPushDown.checkPushedFilters(
          spark, plan(relation.copy(pushedFilters = Seq(pushed))))
      }
      // Whether the check accepts that plan.
      def accepts(pushed: Expression)(plan: LogicalPlan => LogicalPlan): Boolean = {
        try {
          check(pushed)(plan)
          true
        } catch {
          case e: SparkException if e.getCondition == "STREAMING_SCAN_FILTERS_CHANGED" => false
        }
      }
      val idAbove1 = GreaterThan(id, Literal(1L))
      val random = GreaterThan(Rand(Literal(0L)), Literal(0.5))
      // The plan applies the filter, with others or not, or a weaker one, or none.
      assert(accepts(idAbove1)(Filter(And(EqualTo(data, Literal("a")), idAbove1), _)))
      assert(!accepts(idAbove1)(Filter(GreaterThan(id, Literal(0L)), _)))
      assert(!accepts(idAbove1)(identity))
      // Or it keeps no row.
      assert(accepts(idAbove1)(Filter(Literal(false), _)))
      assert(accepts(idAbove1)(
        Filter(And(EqualTo(data, Literal("a")), Literal.create(null, BooleanType)), _)))
      // Through a projection, under an alias, but not through a projection or above a filter that
      // is not deterministic, as these get the rows that the filter would drop.
      assert(accepts(idAbove1) { relation =>
        val alias = Alias(id, "i")()
        Filter(GreaterThan(alias.toAttribute, Literal(1L)), Project(Seq(alias, data), relation))
      })
      assert(!accepts(idAbove1) { relation =>
        Filter(idAbove1, Project(Seq(id, Alias(Rand(Literal(0L)), "r")()), relation))
      })
      assert(!accepts(idAbove1)(relation => Filter(idAbove1, Filter(random, relation))))
      // A filter that is not deterministic gets its rows before its conjuncts after the first
      // nondeterministic one, but not before those that it evaluates first.
      assert(!accepts(idAbove1)(Filter(And(random, idAbove1), _)))
      assert(accepts(idAbove1)(Filter(And(idAbove1, random), _)))
      // With the values of the columns that the plan makes equal to literals.
      assert(accepts(idAbove1)(Filter(EqualTo(id, Literal(5L)), _)))
      assert(!accepts(idAbove1)(Filter(EqualTo(id, Literal(0L)), _)))
      val either = Or(EqualTo(data, Literal("")), GreaterThan(id, Literal(5L)))
      assert(accepts(either)(
        Filter(And(EqualTo(data, Literal("a")), GreaterThan(id, Literal(5L))), _)))
      assert(!accepts(either)(Filter(EqualTo(data, Literal("a")), _)))
      assert(accepts(EqualTo(Cast(id, StringType), Literal("5")))(
        Filter(EqualTo(id, Literal(5L)), _)))
      assert(accepts(GreaterThan(t, Literal(LocalTime.of(0, 0, 1))))(
        Filter(EqualTo(t, Literal(LocalTime.of(12, 0))), _)))
      // Also for a struct or an array of such values, whose literal may have another nullability.
      val struct = Literal.create(InternalRow(1),
        StructType(Seq(StructField("a", IntegerType, nullable = false))))
      assert(accepts(GreaterThan(GetStructField(k, 0, Some("a")), Literal(0)))(
        Filter(EqualTo(k, struct), _)))
      val array = Literal.create(ArrayData.toArrayData(Array(1, 2)),
        ArrayType(IntegerType, containsNull = false))
      assert(accepts(ArrayContains(xs, Literal(2)))(Filter(EqualTo(xs, array), _)))
      // Or whose fields have other names, as an equality compares structs by their fields in
      // order, also in arrays. The column gets the value with its own type, which `typeof` reads.
      def structOf(name: String): Literal = Literal.create(InternalRow(1),
        StructType(Seq(StructField(name, IntegerType, nullable = false))))
      val aAbove0 = GreaterThan(GetStructField(k, 0, Some("a")), Literal(0))
      assert(accepts(aAbove0)(Filter(EqualTo(k, structOf("x")), _)))
      assert(accepts(aAbove0)(Filter(EqualTo(k, structOf("A")), _)))
      def typeIs(name: String): Expression =
        EqualTo(TypeOf(k), Literal(s"struct<$name:int>"))
      assert(accepts(typeIs("a"))(Filter(EqualTo(k, structOf("x")), _)))
      assert(!accepts(typeIs("x"))(Filter(EqualTo(k, structOf("x")), _)))
      val structs = Literal.create(ArrayData.toArrayData(Array(InternalRow(1))),
        ArrayType(StructType(Seq(StructField("x", IntegerType)))))
      assert(accepts(GreaterThan(GetStructField(GetArrayItem(ks, Literal(0)), 0, Some("a")),
        Literal(0)))(Filter(EqualTo(ks, structs), _)))
      // The check does not evaluate a filter with the values when the result is not foldable, as
      // with a UDF, which the optimizer does not evaluate either: it folds my_udf(id) > 0 OR
      // id > 3 into true with id = 5.
      val calls = new AtomicInteger()
      val myUdf = ScalaUDF((i: Long) => { calls.incrementAndGet(); i }, LongType, Seq(id),
        udfName = Some("my_udf"))
      assert(accepts(Or(GreaterThan(myUdf, Literal(0L)), GreaterThan(id, Literal(3L))))(
        Filter(EqualTo(id, Literal(5L)), _)))
      assert(calls.get() === 0)
      // But not a double, as a double equal to 0.0 may be -0.0, nor a struct of a double: the
      // filters on such columns are not pushed down.
      assert(!accepts(GreaterThan(d, Literal(1.0)))(Filter(EqualTo(d, Literal(1.5)), _)))
      val structOf1 = Literal.create(InternalRow(1.0),
        StructType(Seq(StructField("a", DoubleType, nullable = false))))
      assert(!accepts(GreaterThan(GetStructField(f, 0, Some("a")), Literal(0.0)))(
        Filter(EqualTo(f, structOf1), _)))
      // A filter that only differs in the sign of a zero is another filter, as pow(0.0, x) and
      // pow(-0.0, x) differ for x = -1.
      def power(zero: Double): Expression =
        GreaterThan(Pow(Literal(zero), Cast(id, DoubleType)), Literal(0.0))
      assert(accepts(power(-0.0))(Filter(power(-0.0), _)))
      assert(!accepts(power(-0.0))(Filter(power(0.0), _)))
      // And once the optimizer of the micro-batch rewrote it, as it rewrites the plan: the plans
      // below are those that the optimizer makes of the pushed filter with the conditions.
      def optimized(conditions: Expression*)(relation: LogicalPlan): LogicalPlan = {
        withSQLConf(SQLConf.OPTIMIZER_EXCLUDED_RULES.key ->
            Seq(InferFiltersFromConstraints.ruleName, InferFiltersFromGenerate.ruleName)
              .mkString(",")) {
          spark.sessionState.optimizer.execute(Filter(conditions.reduce(And), relation))
        }
      }
      val castBelow = LessThan(Cast(e, LongType), id)
      assert(accepts(castBelow)(optimized(EqualTo(id, Literal(5L)), castBelow)))
      // In part, as the optimizer keeps the columns of an expression that may fail: with e = 1,
      // (id + e > 3 AND e > 0) OR id > 9 becomes id + e > 3 OR id > 9.
      val part = Or(And(GreaterThan(Add(id, Cast(e, LongType)), Literal(3L)),
        GreaterThan(e, Literal(0))), GreaterThan(id, Literal(9L)))
      assert(accepts(part)(optimized(EqualTo(e, Literal(1)), part)))
      // Or into true, without the division by 0 that it keeps.
      withSQLConf(SQLConf.ANSI_ENABLED.key -> "true") {
        val division = Or(GreaterThan(Divide(Literal(10.0), Cast(id, DoubleType)), Literal(1.0)),
          LessThan(id, Literal(1L)))
        assert(accepts(division)(optimized(EqualTo(id, Literal(0L)), division)))
      }
      // A failure of the optimizer is the cause of the error. The filter stays over the relation
      // once optimized, unlike 7 > 2.
      spark.experimental.extraOptimizations = Seq(new Rule[LogicalPlan] {
        override def apply(plan: LogicalPlan): LogicalPlan = plan match {
          case Filter(_, relation: StreamingDataSourceV2ScanRelation)
              if relation.pushedFilters.isEmpty =>
            throw new IllegalStateException("The rewrite failed.")
          case _ => plan
        }
      })
      try {
        val error = intercept[SparkException] {
          check(LessThan(Cast(e, LongType), id))(Filter(EqualTo(id, Literal(7L)), _))
        }
        assert(error.getCondition === "STREAMING_SCAN_FILTERS_CHANGED")
        assert(error.getCause.getMessage === "The rewrite failed.")
      } finally {
        spark.experimental.extraOptimizations = Nil
      }
    }
  }

  Seq(false, true).foreach { catalyst =>
    val kind = if (catalyst) "Catalyst" else "V2"
    test(s"evaluate the filters of the micro-batches in their order with a $kind filter builder") {
      withSQLConf(SQLConf.ANSI_ENABLED.key -> "true") {
        val input = new FilteringMemoryStream(spark)
        val table = new FilterStreamTable(input, catalyst)
        // The cast fails on a payload that is not a number with ANSI, which the RLIKE written
        // before it drops.
        val df = Dataset.ofRows(spark, StreamingRelationV2(None, table.name(), table,
          CaseInsensitiveStringMap.empty(), DataTypeUtils.toAttributes(table.schema()), None, None,
          None)).toDF("id", "payload")
          .where("id > 0 AND payload RLIKE '^[0-9]+$' AND CAST(payload AS INT) > 1").select("id")
        testStream(df)(
          StartStream(),
          AddData(input, (1L, "1"), (2L, "abc"), (3L, "3")),
          ProcessAllAvailable(),
          CheckAnswer(Row(3L)),
          AssertOnQuery { query =>
            // A V2 builder gets the filter before the RLIKE, which it cannot take, and a
            // Catalyst builder all of them, in their order.
            if (catalyst) {
              assert(table.pushed.length === 3, table.pushed)
              assert(table.pushed(1).contains("RLIKE") && table.pushed(2).contains("CAST"),
                table.pushed)
            } else {
              assert(table.pushed.length === 1 && table.pushed.head.contains("_1"), table.pushed)
            }
            // Each micro-batch evaluates the RLIKE before the cast, over the relation.
            val conditions = query.lastExecution.optimizedPlan.collect {
              case Filter(condition, _: StreamingDataSourceV2ScanRelation) =>
                splitConjunctivePredicates(condition)
            }.flatten
            val rlike = conditions.indexWhere(_.isInstanceOf[RLike])
            val cast = conditions.indexWhere(_.exists(_.isInstanceOf[Cast]))
            assert(rlike >= 0 && rlike < cast, conditions)
            true
          })
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

  test("apply the filters that the scan builder got after the scan of each micro-batch") {
    withSourceTable() {
      // id + extra > 10 cannot be translated to a filter of the source, so the scan builder only
      // gets extra > 15. Each micro-batch applies both after the scan, as without the pushdown,
      // so the scan cannot drop a row that the plan keeps, whatever it does with its filter.
      testStream(spark.readStream.table(tbl).where("id + extra > 10 AND extra > 15")
          .select("data"))(
        ProcessAllAvailable(),
        assertPushedFilters("GreaterThan(extra,15)"),
        AssertOnQuery { query =>
          val filtered = query.lastExecution.optimizedPlan.collect {
            case Filter(condition, relation: StreamingDataSourceV2ScanRelation) =>
              condition -> relation
          }
          assert(filtered.length === 1)
          val (condition, relation) = filtered.head
          assert(relation.pushedFilters.map(_.references.map(_.name).toSet) === Seq(Set("extra")))
          val conditions = splitConjunctivePredicates(condition)
          assert(conditions.exists(_.semanticEquals(relation.pushedFilters.head)), conditions)
          assert(conditions.exists(_.references.map(_.name).toSet == Set("id", "extra")),
            conditions)
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
      assert(requiredColumns(df).values.map(_.map(_.name).toSet).toSeq ===
        Seq(Set("id", "data", "extra")))
      assert(pushed(df) === Seq(Set("extra")))
    }
  }

  test("push down no filter that the reads of a source apply with another zero") {
    withTable(tbl) {
      createTable(tbl, "x bigint")
      val source = spark.readStream.table(tbl)
      // pow(0.0, x) and pow(-0.0, x) differ for x = -1, though `Literal.equals` finds their zeros
      // equal, so the scan gets neither.
      def above(zero: Double): DataFrame =
        source.where(pow(lit(zero), col("x").cast("double")) > 0)
      assert(pushed(above(0.0).union(above(-0.0))) === Nil)
      assert(pushed(above(-0.0).union(above(-0.0))) === Seq(Set("x")))
    }
  }

  test("push down no filter on columns that the check of the micro-batches cannot give values") {
    withTable(tbl) {
      createTable(tbl, "id bigint, d double, s struct<a: double>, c string collate UTF8_LCASE",
        filtering)
      sql(s"INSERT INTO $tbl VALUES (1, 1.5, named_struct('a', 0.0D), 'a'), " +
        "(2, 2.5, named_struct('a', 1.0D), 'B')")
      val source = spark.readStream.table(tbl)
      // A micro-batch may rewrite a pushed filter with the values that its plan gives to the
      // columns of the filter, which the check cannot give to a double, as a double equal to 0.0
      // may be -0.0, to a struct of a double, or to a string of UTF8_LCASE, which 'A' equals too,
      // so the filters on these columns stay in Spark.
      testStream(source.where("d > 1.0 AND s.a > -1.0 AND c = 'a' AND id > 0").select("id"))(
        ProcessAllAvailable(),
        assertPushedFilters("GreaterThan(id,0)"),
        CheckAnswer(Row(1L)))
      // As where a join gives the struct its value in the micro-batches, which fold the filter.
      val static = spark.range(1).selectExpr("named_struct('a', 0.0D) AS k")
        .where("k = named_struct('a', 0.0D)")
      testStream(source.where("s.a > -1.0").join(static, col("s") === col("k")).select("id"))(
        ProcessAllAvailable(),
        CheckAnswer(Row(1L)),
        insert("(3, 3.5, named_struct('a', 0.0D), 'c')"),
        ProcessAllAvailable(),
        CheckAnswer(Row(1L), Row(3L)))
    }
  }

  test("push down the filters on columns of types that no equality compares") {
    withTable(tbl) {
      createTable(tbl,
        "id bigint, p struct<id: int, attrs: map<string, string>>, m map<string, int>, v variant",
        filtering)
      sql(s"INSERT INTO $tbl VALUES " +
        "(1, named_struct('id', 5, 'attrs', map('k', 'a')), map('k', 1), parse_json('1')), " +
        "(2, named_struct('id', 6, 'attrs', map('k', 'b')), map('k', 2), parse_json('2'))")
      val source = spark.readStream.table(tbl)
      // No equality compares maps, variants, or structs with maps, so neither the micro-batches
      // nor their check give a value to such a column, and its filters are pushed down.
      assert(pushed(source.where("p.id = 5 AND m['k'] = 1 AND variant_get(v, '$', 'int') = 1"))
        .toSet === Set(Set("p"), Set("m"), Set("v")))
      testStream(source.where("p.id = 5").select("id"))(
        ProcessAllAvailable(),
        assertPushedFilters("EqualTo(p.id,5)"),
        CheckAnswer(Row(1L)))
    }
  }

  test("exclude the rules that infer filters when the query starts only for the filter pushdown") {
    withTable(tbl) {
      createTable(tbl, "id bigint, data string")
      val plan = spark.readStream.table(tbl).where("id > 1").queryExecution.analyzed
      def inferFilters(conf: SQLConf): Boolean = !conf.optimizerExcludedRules.toSeq
        .flatMap(_.split(",")).contains(InferFiltersFromConstraints.ruleName)
      assert(!inferFilters(StreamingV2ScanPushDown.startupConf(spark, plan, None).get))
      // The column pruning alone optimizes the plan as the micro-batches do.
      withSQLConf(filterPushdownEnabled(false).toSeq: _*) {
        assert(inferFilters(StreamingV2ScanPushDown.startupConf(spark, plan, None).get))
      }
      withSQLConf((filterPushdownEnabled(false) ++ pruningEnabled(false)).toSeq: _*) {
        assert(StreamingV2ScanPushDown.startupConf(spark, plan, None).isEmpty)
      }
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
          // The scan of the relation of both reads gets the filter that they both apply.
          val relations = query.logicalPlan.collect {
            case relation: StreamingDataSourceV2ScanRelation => relation
          }
          assert(relations.length === 2 && relations.distinct.length === 1)
          assert(relations.head.pushedFilters.map(_.toString) ===
            Seq(s"(${relations.head.output(2)} > 10)"))
          // Each read of the relation applies it in the optimized plan of a micro-batch.
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
    withPushDownConfsRestored {
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

  test("canonicalize and show the pushed filters of a streaming scan relation") {
    withSourceTable() {
      testStream(spark.readStream.table(tbl).where("extra > 10").select("data"))(
        ProcessAllAvailable(),
        AssertOnQuery { query =>
          val relation = query.logicalPlan.collectFirst {
            case relation: StreamingDataSourceV2ScanRelation => relation
          }.get
          assert(relation.pushedFilters.nonEmpty)
          // A copy of the relation has new attributes, also in its pushed filters, and the same
          // result.
          val copy = relation.newInstance().asInstanceOf[StreamingDataSourceV2ScanRelation]
          assert(copy.pushedFilters.flatMap(_.references).map(_.exprId).toSet
            .intersect(relation.output.map(_.exprId).toSet).isEmpty)
          assert(copy.sameResult(relation))
          // The relation shows the filters that its scan builder was given.
          val string = relation.simpleString(100)
          assert(string.contains("FiltersOfScanBuilder: [") && string.contains(" > 10)]"), string)
          true
        },
        CheckAnswer(Row("b")))
    }
  }

  // The confs with which the optimizer of a micro-batch prunes the filters that the constraints of
  // their children imply, or does not.
  private val withoutPruneFilters = Seq(
    Map.empty[String, String],
    Map(SQLConf.CONSTRAINT_PROPAGATION_ENABLED.key -> "false"),
    Map(SQLConf.OPTIMIZER_EXCLUDED_RULES.key -> PruneFilters.ruleName))

  test("evaluate a pushed filter once above a projection of a value of each micro-batch") {
    withSourceTable() {
      // The query outputs the value, so that the optimizer keeps its projection.
      val df = spark.readStream.table(tbl).withColumn("pt", current_timestamp())
        .where("extra > 10").select("data", "pt")
      // Each micro-batch has the value of the projection once it is optimized, so it applies the
      // filter above the projection, once, and its check finds the filter there, whether the
      // optimizer prunes filters or not.
      withoutPruneFilters.foreach { confs =>
        withSQLConf(confs.toSeq: _*) {
          assert(pushed(df) === Seq(Set("extra")), confs)
          testStream(df)(
            ProcessAllAvailable(),
            assertPushedFilters("GreaterThan(extra,10)"),
            assertEvaluatedOnce("extra"))
        }
      }
    }
  }

  test("evaluate a pushed filter once after a separate filter that is not pushed down") {
    withTable(tbl) {
      createTable(tbl, "id bigint, data string, ts timestamp, extra int")
      sql(s"INSERT INTO $tbl VALUES " +
        "(1, 'a', timestamp_seconds(0), 10), (2, 'b', timestamp_seconds(0), 20)")
      // The filters cannot be combined when the query starts, as the first one has a value of
      // each micro-batch. The V1 builder of the in-memory tables gets the second one, which each
      // micro-batch applies above the first one.
      val df = spark.readStream.table(tbl).where("ts < current_timestamp()").where("extra > 10")
        .select("data")
      withoutPruneFilters.foreach { confs =>
        withSQLConf(confs.toSeq: _*) {
          testStream(df)(
            ProcessAllAvailable(),
            assertPushedFilters("GreaterThan(extra,10)"),
            assertEvaluatedOnce("extra"),
            CheckAnswer(Row("b")))
        }
      }
    }
  }

  Seq(
    // Two filters above the projection, which the micro-batch combines.
    "two filters" -> ((df: DataFrame) =>
      df.withColumn("pt", current_timestamp()).where("extra > 10").where("data = 'b'")
        .select("data", "pt"), Seq(Seq("extra"), Seq("data")),
      Seq("GreaterThan(extra,10)", "EqualTo(data,b)")),
    // A filter that the optimizer rewrites, as id > CAST(1 AS BIGINT), whose pushed filter is
    // optimized too.
    "a rewritten filter" -> ((df: DataFrame) =>
      df.withColumn("pt", current_timestamp()).where("id > 1").select("data", "pt"),
      Seq(Seq("id")), Seq("GreaterThan(id,1)")),
    // A filter on an alias of the column of the pushed filter.
    "a filter on an alias" -> ((df: DataFrame) =>
      df.select(col("data"), col("extra").as("e"), current_timestamp().as("pt"))
        .where("e > 10").select("data", "pt"), Seq(Seq("e", "extra")),
      Seq("GreaterThan(extra,10)")),
    // A filter above a watermark, which keeps the rows and the columns of its child.
    "a filter above a watermark" -> ((df: DataFrame) =>
      df.withColumn("pt", current_timestamp()).withWatermark("pt", "10 seconds")
        .where("extra > 10").select("data"), Seq(Seq("extra")), Seq("GreaterThan(extra,10)"))
  ).foreach { case (name, (query, columns, pushedFilters)) =>
    test(s"evaluate the pushed filters once with $name above a projection of a value of each " +
        "micro-batch") {
      withSourceTable() {
        val df = query(spark.readStream.table(tbl))
        withoutPruneFilters.foreach { confs =>
          withSQLConf(confs.toSeq: _*) {
            testStream(df)(
              Seq(ProcessAllAvailable(), assertPushedFilters(pushedFilters: _*)) ++
                columns.map(names => assertEvaluatedOnce(names: _*)): _*)
          }
        }
      }
    }
  }

  test("read all the columns with the filters when only the column pruning fails") {
    // The pruned scans of the table do not have the column data, so the scan reads all the
    // columns, still with the filter, without trying the same columns with no filter first.
    withSourceTable(InMemoryBaseTable.SIMULATE_INCOMPLETE_COLUMN_PRUNING -> "true") {
      val logs = new LogAppender("the fallback after a pruned read schema")
      withLogAppender(logs,
          loggerNames = Seq(StreamingV2ScanPushDown.getClass.getName.stripSuffix("$")),
          level = Some(Level.WARN)) {
        testStream(spark.readStream.table(tbl).where("extra > 10").select("data"))(
          ProcessAllAvailable(),
          assertReadSchema("id", "data", "extra"),
          assertPushedFilters("GreaterThan(extra,10)"),
          CheckAnswer(Row("b")))
      }
      val warnings = logs.loggingEvents.map(_.getMessage.getFormattedMessage)
        .filter(_.contains("could not be built"))
      assert(warnings.length === 1 &&
        warnings.head.contains("Trying (columns: all, filters: (testcat.t.extra > 10)) next."),
        warnings)
    }
  }

  test("push down the filters to a scan of all the columns whose read schema does not follow " +
      "the relation, without the column pruning") {
    val input = MemoryStream[(Long, String, Int)]
    val table = new UnknownColumnReadSchemaTable(input, takesFilters = true)
    val df = Dataset.ofRows(spark, StreamingRelationV2(None, "unknown_column_read_schema", table,
      CaseInsensitiveStringMap.empty(), DataTypeUtils.toAttributes(table.schema()), None, None,
      None)).toDF("id", "data", "extra").where("extra > 10").select("data")
    val logs = new LogAppender("the read schema of a scan of all the columns with filters")
    withLogAppender(logs,
        loggerNames = Seq(StreamingV2ScanPushDown.getClass.getName.stripSuffix("$")),
        level = Some(Level.WARN)) {
      withSQLConf(SQLConf.STREAMING_V2_COLUMN_PRUNING_ENABLED.key -> "false") {
        testStream(df)(
          StartStream(),
          AddData(input, (1L, "a", 10), (2L, "b", 20)),
          ProcessAllAvailable(),
          AssertOnQuery(_ => table.pushed == Seq("GreaterThan(_3,10)")),
          CheckAnswer(Row("b")))
      }
    }
    // The scan with the filter goes through the check of a scan of all the columns, which only
    // logs the read schema without the column pruning, rather than building it again.
    val warnings = logs.loggingEvents.map(_.getMessage.getFormattedMessage)
    assert(warnings.exists(_.contains("as the column pruning is disabled")), warnings)
    assert(!warnings.exists(_.contains("could not be built")), warnings)
  }

  test("do not optimize a query without conditions when no source prunes its columns") {
    withSourceTable(InMemoryBaseTable.STREAMING_COLUMN_PRUNING -> "false") {
      val source = spark.readStream.table(tbl)
      // Nothing can be pushed down without a condition, so the query is not optimized, and its
      // scan gets no filter.
      assert(reads(source.select("data")).isEmpty)
      assert(reads(source.union(source)).isEmpty)
      assert(reads(source.where("extra > 10")).nonEmpty)
      assert(reads(source.join(spark.range(3).toDF("id"), "id")).nonEmpty)
      // The optimizer turns the conditions of an as-of or a lateral join into filters too.
      val static = spark.range(3).toDF("sid")
      assert(reads(source.joinAsOf(static, source("id"), static("sid"), source("extra") > 10,
        "inner", null, allowExactMatches = true, "backward")).nonEmpty)
      withTempView("s") {
        source.createOrReplaceTempView("s")
        assert(reads(sql("SELECT * FROM s JOIN LATERAL (SELECT s.id AS x) ON s.extra > 10"))
          .nonEmpty)
      }
    }
    // A source that prunes its columns needs the optimized query either way.
    withSourceTable() {
      assert(reads(spark.readStream.table(tbl).select("data")).nonEmpty)
    }
  }

  test("push down no filter that the optimizer infers from a generator") {
    // The optimizer infers isnotnull(arr) and size(arr) > 0 from the explode of arr, which the
    // query does not write: the scan only gets the filters that the query applies, whether the
    // query has another filter or not, and whether the table prunes its columns or not.
    Seq(Nil, Seq(InMemoryBaseTable.STREAMING_COLUMN_PRUNING -> "false"))
      .foreach { properties =>
        withTable(tbl) {
          createTable(tbl, "id bigint, arr array<int>", properties: _*)
          val source = spark.readStream.table(tbl)
          assert(pushed(source.select(explode(col("arr")))) === Nil, properties)
          assert(pushed(source.where("id > 0").select(explode(col("arr")))) === Seq(Set("id")),
            properties)
        }
      }
  }

  test("push down only the leading predicates that a V2 filter builder gets, or that cannot fail") {
    val table = new FilterStreamTable(new FilteringMemoryStream(spark), catalyst = false)
    val output = DataTypeUtils.toAttributes(table.schema())
    val payload = output(1)
    // RLIKE cannot be translated to a predicate of the source, while the cast can, and fails on
    // a payload that is not a number with ANSI, which the RLIKE written before it drops.
    val matches = RLike(payload, Literal("^[0-9]+$"))
    val positive = GreaterThan(Cast(payload, IntegerType, evalMode = EvalMode.ANSI), Literal(0))
    assert(DataSourceV2Strategy.translateFilterV2(matches).isEmpty)
    assert(DataSourceV2Strategy.translateFilterV2(positive).nonEmpty)
    def pushed(filters: Expression*): Seq[Expression] = {
      StreamingV2ScanPushDown.buildScan(table, CaseInsensitiveStringMap.empty(), output,
          spark.sessionState.conf,
          Some(StreamingV2ScanPushDown.Reads(AttributeSet(output), Seq(filters))),
          Seq(table.name()))(_ => null)
        .pushedFilters
    }
    assert(pushed(matches, positive) === Nil)
    assert(table.pushed.isEmpty)
    assert(pushed(positive, matches) === Seq(positive))
    assert(table.pushed.length === 1)
    // Also a predicate that cannot fail, wherever it is.
    val idAbove1 = GreaterThan(output.head, Literal(1L))
    assert(pushed(matches, positive, idAbove1) === Seq(idAbove1))
  }

  test("push down the filters that a V1 filter builder takes wherever they are") {
    withSourceTable() {
      val source = spark.readStream.table(tbl)
      // A filter on a value of each micro-batch comes first, as in a window over the last hour:
      // the filter after it is still pushed down to a V1 builder.
      testStream(source.where("data <> current_user() AND extra > 10").select("data"))(
        ProcessAllAvailable(),
        assertPushedFilters("GreaterThan(extra,10)"),
        CheckAnswer(Row("b")))
      // Both reads apply both filters, in other orders.
      testStream(source.where("id > 1 AND extra > 10").select("data")
          .union(source.where("extra > 10 AND id > 1").select("data")))(
        ProcessAllAvailable(),
        assertPushedFilters("GreaterThan(extra,10)", "GreaterThan(id,1)"),
        CheckAnswer(Row("b"), Row("b")))
    }
  }

  test("keep the rows of the test table that a pushed filter keeps, or cannot tell") {
    import org.apache.spark.sql.sources.{EqualTo, GreaterThanOrEqual, LessThan}
    val schema = StructType.fromDDL("d double, s string")
    // A character outside the BMP, which UTF-16 code units order before U+FFFD.
    val emoji = new String(Character.toChars(0x1F600))
    val replacement = new String(Character.toChars(0xFFFD))
    val row = InternalRow(-0.0, UTF8String.fromString(emoji))
    // -0.0 is equal to 0.0 in Spark.
    assert(InMemoryBaseTable.passes(EqualTo("d", 0.0), row, schema))
    assert(InMemoryBaseTable.passes(GreaterThanOrEqual("d", 0.0), row, schema))
    // Strings are ordered by code point: a character outside the BMP is after U+FFFD.
    assert(InMemoryBaseTable.passes(GreaterThanOrEqual("s", replacement), row, schema))
    assert(!InMemoryBaseTable.passes(LessThan("s", replacement), row, schema))
    // A filter on a nested or quoted column, or on a column added after the row was written,
    // keeps the row.
    assert(InMemoryBaseTable.passes(EqualTo("s.x", 1), row, schema))
    assert(InMemoryBaseTable.passes(EqualTo("added", 1), row,
      StructType.fromDDL("d double, s string, added int")))
  }
}

/**
 * A table over a continuous memory stream, whose scans only read the columns that they are asked
 * for: their readers return the rows of the stream with only these columns. If `takesFilters`,
 * its scan builders also take the filters, and the readers drop the rows that fail them.
 */
private class PruningContinuousTable(input: ContinuousMemoryStream[_], takesFilters: Boolean)
  extends Table with SupportsRead {

  override def name(): String = "pruning_continuous"

  override def schema(): StructType = input.fullSchema()

  override def capabilities(): util.Set[TableCapability] = {
    val capabilities =
      util.EnumSet.of(TableCapability.CONTINUOUS_READ, TableCapability.STREAMING_COLUMN_PRUNING)
    if (takesFilters) capabilities.add(TableCapability.STREAMING_FILTER_PUSHDOWN)
    capabilities
  }

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    new ScanBuilder with SupportsPushDownRequiredColumns with SupportsPushDownFilters {
      private var columns = schema()
      private var filters = Array.empty[SourceFilter]

      override def pruneColumns(requiredSchema: StructType): Unit = {
        columns = StructType(requiredSchema.fieldNames.map(schema()(_)))
      }

      override def pushFilters(filters: Array[SourceFilter]): Array[SourceFilter] = {
        if (takesFilters) this.filters = filters
        filters
      }

      override def pushedFilters(): Array[SourceFilter] = filters

      override def build(): Scan = new Scan {
        private val readFilters = filters

        override def readSchema(): StructType = columns

        override def toContinuousStream(checkpointLocation: String): ContinuousStream = {
          new PruningContinuousStream(input, columns.fieldNames.map(schema().fieldIndex), columns,
            readFilters, schema())
        }
      }
    }
  }
}

private object PruningContinuousTable {
  // The rows that the readers of the streams that take filters returned.
  val returnedRows = new AtomicLong()
}

private class PruningContinuousStream(
    input: ContinuousStream,
    ordinals: Array[Int],
    columns: StructType,
    filters: Array[SourceFilter],
    fullSchema: StructType) extends ContinuousStream {

  override def initialOffset(): Offset = input.initialOffset()

  override def deserializeOffset(json: String): Offset = input.deserializeOffset(json)

  override def mergeOffsets(offsets: Array[PartitionOffset]): Offset = input.mergeOffsets(offsets)

  override def planInputPartitions(start: Offset): Array[InputPartition] =
    input.planInputPartitions(start)

  override def createContinuousReaderFactory(): ContinuousPartitionReaderFactory =
    new PruningContinuousReaderFactory(input.createContinuousReaderFactory(), ordinals, columns,
      filters, fullSchema)

  override def commit(end: Offset): Unit = input.commit(end)

  override def stop(): Unit = input.stop()
}

private class PruningContinuousReaderFactory(
    input: ContinuousPartitionReaderFactory,
    ordinals: Array[Int],
    columns: StructType,
    filters: Array[SourceFilter],
    fullSchema: StructType) extends ContinuousPartitionReaderFactory {

  override def createReader(partition: InputPartition): ContinuousPartitionReader[InternalRow] = {
    val reader = input.createReader(partition)
    new ContinuousPartitionReader[InternalRow] {
      private val projection = ProjectingInternalRow(columns, ordinals.toSeq)

      // Skips the rows that fail the filters.
      override def next(): Boolean = {
        var hasNext = reader.next()
        while (hasNext && !filters.forall(InMemoryBaseTable.passes(_, reader.get(), fullSchema))) {
          hasNext = reader.next()
        }
        if (hasNext && filters.nonEmpty) PruningContinuousTable.returnedRows.incrementAndGet()
        hasNext
      }

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
 * in their read schema that the table does not have. With `takesFilters`, it declares
 * STREAMING_FILTER_PUSHDOWN too, and its scan builder takes all the filters, which it records.
 */
private class UnknownColumnReadSchemaTable(input: MemoryStream[_], takesFilters: Boolean = false)
  extends Table with SupportsRead {
  @volatile var pushed: Seq[String] = Nil

  override def name(): String = "unknown_column_read_schema"

  override def schema(): StructType = input.fullSchema()

  override def capabilities(): util.Set[TableCapability] = {
    val capabilities = util.EnumSet.of(
      TableCapability.MICRO_BATCH_READ, TableCapability.STREAMING_COLUMN_PRUNING)
    if (takesFilters) capabilities.add(TableCapability.STREAMING_FILTER_PUSHDOWN)
    capabilities
  }

  private def scan(): Scan = new Scan {
    override def readSchema(): StructType = schema().add("unknown", "int")

    override def toMicroBatchStream(checkpointLocation: String): MicroBatchStream = input
  }

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = if (takesFilters) {
    new ScanBuilder with SupportsPushDownFilters {
      private var filters = Array.empty[SourceFilter]

      override def pushFilters(filters: Array[SourceFilter]): Array[SourceFilter] = {
        this.filters = filters
        pushed = filters.map(_.toString).toSeq
        filters
      }

      override def pushedFilters(): Array[SourceFilter] = filters

      override def build(): Scan = scan()
    }
  } else {
    () => scan()
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
 * A table over a memory stream, whose scan builder fails in `failingStep` when it gets a pushdown
 * of a streaming query: "pushFilters", "pruneColumns", "build" or "toMicroBatchStream". It counts
 * the streams that its scans try to create, and records the read schema and the filters of each
 * scan whose stream it creates.
 */
private class FailingPushDownTable(input: ProjectingMemoryStream, failingStep: String)
  extends Table with SupportsRead {
  val streamAttempts = new AtomicInteger()
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
          override def readSchema(): StructType = {
            failIf("readSchema", readPushedDown)
            readColumns
          }

          override def toMicroBatchStream(checkpointLocation: String): MicroBatchStream = {
            streamAttempts.incrementAndGet()
            failIf("toMicroBatchStream", readPushedDown)
            streams.add(readColumns.fieldNames.toSeq -> readFilters.map(_.toString).toSeq)
            input.columns = Some(readColumns.fieldNames.map(schema().fieldIndex) -> readColumns)
            input
          }
        }
      }
    }
  }
}

/**
 * A memory stream of (id, data, extra), whose readers return the columns that its table was asked
 * for, as the streams of a table with STREAMING_COLUMN_PRUNING do, see [[FailingPushDownTable]].
 */
private class ProjectingMemoryStream(sparkSession: SparkSession)
  extends MemoryStreamBaseClass[(Long, String, Int)](
    ProjectingMemoryStream.ids.getAndIncrement(), sparkSession)(ProjectingMemoryStream.encoder) {

  // The ordinals of the columns that the readers return, and their schema.
  @volatile var columns: Option[(Array[Int], StructType)] = None

  override def createReaderFactory(): PartitionReaderFactory = columns match {
    case Some((ordinals, schema)) =>
      val input = super.createReaderFactory()
      (partition: InputPartition) => new PartitionReader[InternalRow] {
        private val reader = input.createReader(partition)
        private val projection = ProjectingInternalRow(schema, ordinals.toSeq)

        override def next(): Boolean = reader.next()

        override def get(): InternalRow = {
          projection.project(reader.get())
          projection
        }

        override def close(): Unit = reader.close()
      }
    case None => super.createReaderFactory()
  }
}

private object ProjectingMemoryStream {
  // Far from the ids of the other memory streams of the JVM.
  val ids = new AtomicInteger(1 << 21)
  val encoder: Encoder[(Long, String, Int)] =
    Encoders.tuple(Encoders.scalaLong, Encoders.STRING, Encoders.scalaInt)
}

/**
 * A table over a memory stream of (id, payload), whose scan builder takes V2 predicates, or
 * Catalyst filters, which it records, and whose stream drops the rows that fail them, see
 * [[FilteringMemoryStream]].
 */
private class FilterStreamTable(input: FilteringMemoryStream, catalyst: Boolean)
  extends Table with SupportsRead {
  @volatile var pushed: Seq[String] = Nil

  override def name(): String = "filter_stream"

  override def schema(): StructType = input.fullSchema()

  override def capabilities(): util.Set[TableCapability] = util.EnumSet.of(
    TableCapability.MICRO_BATCH_READ, TableCapability.STREAMING_FILTER_PUSHDOWN)

  // The scan, whose stream drops the rows that fail `filters`, bound to the columns of the stream.
  private def scan(filters: Seq[Expression]): Scan = new Scan {
    override def readSchema(): StructType = schema()

    override def toMicroBatchStream(checkpointLocation: String): MicroBatchStream = {
      input.filters = filters
      input
    }
  }

  private def reference(name: String): Option[BoundReference] = schema().getFieldIndex(name)
    .map(i => BoundReference(i, schema()(i).dataType, nullable = true))

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = if (catalyst) {
    new ScanBuilder with SupportsPushDownCatalystFilters {
      private var filters = Seq.empty[Expression]

      override def pushFilters(filters: Seq[Expression]): Seq[Expression] = {
        this.filters = filters
        pushed = filters.map(_.sql)
        filters
      }

      override def pushedFilters: Array[Predicate] = Array.empty

      override def build(): Scan = scan(filters.map(_.transform {
        case column: AttributeReference => reference(column.name).get
      }))
    }
  } else {
    new ScanBuilder with SupportsPushDownV2Filters {
      private var predicates = Array.empty[Predicate]

      override def pushPredicates(predicates: Array[Predicate]): Array[Predicate] = {
        this.predicates = predicates
        pushed = predicates.map(_.toString).toSeq
        predicates
      }

      override def pushedPredicates(): Array[Predicate] = predicates

      // The predicates that the stream can evaluate as data source filters, the others keeping
      // all the rows.
      override def build(): Scan = scan(predicates.toSeq.flatMap(PredicateUtils.toV1)
        .flatMap(StructFilters.filterToExpression(_, reference)))
    }
  }
}

/**
 * A memory stream of (id, payload), whose readers drop the rows that fail the filters that its
 * table set, as the streams of a table with STREAMING_FILTER_PUSHDOWN may: they evaluate the
 * filters in their order, and keep a row from the first filter that cannot be evaluated on it,
 * as one that may fail would not be evaluated on the rows that a filter before it drops.
 */
private class FilteringMemoryStream(sparkSession: SparkSession)
  extends MemoryStreamBaseClass[(Long, String)](
    FilteringMemoryStream.ids.getAndIncrement(), sparkSession)(FilteringMemoryStream.encoder) {

  // The filters, bound to the columns of the stream.
  @volatile var filters: Seq[Expression] = Nil

  override def createReaderFactory(): PartitionReaderFactory = {
    val input = super.createReaderFactory()
    val filters = this.filters
    (partition: InputPartition) => new PartitionReader[InternalRow] {
      private val reader = input.createReader(partition)
      private val predicates = filters.map(CatalystPredicate.createInterpreted)

      private def keeps(row: InternalRow): Boolean = predicates.iterator
        .map(predicate => Try(predicate.eval(row)))
        .collectFirst {
          case Success(false) => false
          case Failure(_) => true
        }.getOrElse(true)

      override def next(): Boolean = {
        while (reader.next()) {
          if (keeps(reader.get())) return true
        }
        false
      }

      override def get(): InternalRow = reader.get()

      override def close(): Unit = reader.close()
    }
  }
}

private object FilteringMemoryStream {
  // Far from the ids of the other memory streams of the JVM.
  val ids = new AtomicInteger(1 << 22)
  val encoder: Encoder[(Long, String)] = Encoders.tuple(Encoders.scalaLong, Encoders.STRING)
}

/** A foldable value that changes each time that it is evaluated, as a function of time would. */
private case class ChangingValue() extends LeafExpression with CodegenFallback {
  override def dataType: DataType = IntegerType

  override def nullable: Boolean = false

  override def foldable: Boolean = true

  override def eval(input: InternalRow): Any = ChangingValue.values.incrementAndGet()
}

private object ChangingValue {
  val values = new AtomicInteger()
}
