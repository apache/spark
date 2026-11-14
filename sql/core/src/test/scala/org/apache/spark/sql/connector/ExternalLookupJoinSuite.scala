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

package org.apache.spark.sql.connector

import java.nio.charset.StandardCharsets
import java.util.concurrent.{ConcurrentLinkedQueue, CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicReference}

import scala.collection.mutable
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._
import scala.reflect.{classTag, ClassTag}

import org.apache.spark.{SparkException, SparkIllegalArgumentException, SparkRuntimeException,
  SparkThrowable, SparkUnsupportedOperationException, TaskContext}
import org.apache.spark.rdd.DeterministicLevel
import org.apache.spark.sql.{AnalysisException, Column, DataFrame, Encoders, Row, SaveMode,
  SQLContext}
import org.apache.spark.sql.IntegratedUDFTestUtils.{shouldTestPythonUDFs, TestPythonUDF}
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.{CheckExternalLookupJoin,
  ExternalLookupJoinEligibility, FormExternalLookupJoin, NoSuchTableException}
import org.apache.spark.sql.catalyst.expressions.{Add, Alias, Attribute, AttributeMap,
  AttributeReference, Cast, EqualTo, Expression, GenericInternalRow, LessThan, Literal,
  NamedExpression, Rand, UnsafeProjection, Upper}
import org.apache.spark.sql.catalyst.plans.{Inner, QueryPlan}
import org.apache.spark.sql.catalyst.plans.logical.{Deduplicate, ExternalLookupFilter,
  ExternalLookupJoin, ExternalLookupProject, ExternalLookupTransform, Join, JoinHint, LeafNode,
  LogicalPlan, PreparedExternalLookupJoin, RepartitionByExpression}
import org.apache.spark.sql.catalyst.trees.TreePattern.LOOKUP_ONLY_RELATION
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, GenericArrayData}
import org.apache.spark.sql.classic.Dataset
import org.apache.spark.sql.connector.catalog.{Identifier, InMemoryBaseTable,
  InMemoryTableCatalog, MetadataColumn, SupportsMetadataColumns, SupportsRead, Table,
  TableCapability, TableCatalog, TableChange,
  TableProvider}
import org.apache.spark.sql.connector.catalog.TableCapability.{BATCH_READ, EXTERNAL_LOOKUP}
import org.apache.spark.sql.connector.expressions.{NamedReference, Transform}
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.connector.metric.{CustomMetric, CustomSumMetric, CustomTaskMetric}
import org.apache.spark.sql.connector.read.{Batch, ExternalLookup, ExternalLookupReader,
  ExternalLookupReaderFactory, ExternalLookupResultCursor, InputPartition, LookupParameter,
  PartitionReader, PartitionReaderFactory, Scan, ScanBuilder, SupportsExternalLookup,
  SupportsPushDownRequiredColumns, SupportsPushDownV2Filters}
import org.apache.spark.sql.execution.{CoalesceExec, LocalTableScanExec}
import org.apache.spark.sql.execution.datasources.{FileFormat, SaveIntoDataSourceCommand}
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2Relation, ExternalLookupRelation,
  FileDataSourceV2, PrepareExternalLookupJoin, StreamingDataSourceV2ScanRelation,
  TableCapabilityCheck}
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.execution.joins.ExternalLookupJoinExec
import org.apache.spark.sql.execution.streaming.operators.stateful.StreamingDeduplicateExec
import org.apache.spark.sql.execution.streaming.runtime.{IncrementalExecution, MemoryStream,
  StreamingQueryWrapper}
import org.apache.spark.sql.execution.streaming.sources.LowLatencyMemoryStream
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector
import org.apache.spark.sql.functions.{broadcast, coalesce, col, concat, current_date,
  current_timestamp, date_from_unix_date, expr, length, lit, max, raise_error, rand, struct,
  to_json, udf, upper, when}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.{BaseRelation, CreatableRelationProvider, InsertableRelation,
  RelationProvider, SchemaRelationProvider}
import org.apache.spark.sql.streaming.{StreamingQueryException, StreamRealTimeModeE2ESuiteBase,
  StreamTest, Trigger}
import org.apache.spark.sql.streaming.util.StreamManualClock
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{ArrayType, DataType, Decimal, DecimalType, DoubleType,
  IntegerType, MapType, StringType, StructType, UserDefinedType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.tags.DataSourceV2Test
import org.apache.spark.unsafe.types.UTF8String

@DataSourceV2Test
class ExternalLookupJoinSuite extends QueryTest with SharedSparkSession {

  import testImplicits._

  private val EXTERNAL_LOOKUP_CATALOG_NAME = "external_lookup_cat"
  // The supportedJoinTypes parameter of a JOIN_TYPE error.
  private val SUPPORTED_JOIN_TYPES = "INNER, LEFT OUTER, LEFT SEMI, LEFT ANTI"

  private val streamRunCount = new AtomicInteger()

  override def beforeEach(): Unit = {
    super.beforeEach()
    SimpleExternalLookupDataSource.resetCounters()
    SemanticKeyExternalLookupDataSource.requestSizes.clear()
  }

  /** The rows of `df` as a streaming DataFrame: one micro-batch, in `numPartitions` partitions. */
  private def streamOf(df: DataFrame, numPartitions: Int = 1): DataFrame = {
    val input = MemoryStream[Row](numPartitions)(Encoders.row(df.schema), spark.sqlContext)
    input.addData(df.collect().toSeq)
    input.toDF()
  }

  private case class StreamRun(rows: Seq[Row], execution: IncrementalExecution) {
    def plan: LogicalPlan = execution.optimizedPlan
    def lookupJoin: ExternalLookupJoinExec = execution.executedPlan.collectFirst {
      case exec: ExternalLookupJoinExec => exec
    }.get
  }

  /** The last two metric reads or closes that the connector's readers saw, in order. */
  private def lastReaderEvents: Seq[String] =
    SimpleExternalLookupDataSource.readerEvents.asScala.toSeq.takeRight(2)

  /** Runs the streaming `df` on a memory sink until it has processed all of its input. */
  private def runStream(df: DataFrame, outputMode: String = "append"): StreamRun = {
    val queryName = s"external_lookup_stream_${streamRunCount.incrementAndGet()}"
    val query =
      df.writeStream.format("memory").outputMode(outputMode).queryName(queryName).start()
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

  private def checkStream(df: DataFrame, expected: Seq[Row]): Unit =
    QueryTest.sameRows(expected, runStream(df).rows).foreach(fail(_))

  /** The error that the streaming `df` fails with, when it starts or in a micro-batch. */
  private def streamFailure[T <: Throwable : ClassTag](df: DataFrame): T = {
    val cause = try {
      runStream(df)
      fail("The streaming query did not fail")
    } catch {
      case e: StreamingQueryException => e.getCause
      case e: Throwable with SparkThrowable => e
    }
    assert(classTag[T].runtimeClass.isInstance(cause), cause)
    cause.asInstanceOf[T]
  }

  test("a stream whose scan is pruned at the start reads the columns of its lookup joins") {
    withSQLConf(
        "spark.sql.catalog.stream_cat" -> classOf[InMemoryTableCatalog].getName,
        SQLConf.STREAMING_V2_COLUMN_PRUNING_ENABLED.key -> "true") {
      withTable("stream_cat.ns.badge_swipes") {
        sql("CREATE TABLE stream_cat.ns.badge_swipes (employee INT, door STRING, minute INT) " +
          s"TBLPROPERTIES ('${InMemoryBaseTable.STREAMING_COLUMN_PRUNING}' = 'true')")
        sql("INSERT INTO stream_cat.ns.badge_swipes VALUES (1, 'lobby', 10), (3, 'garage', 20)")
        val swipes = spark.readStream.table("stream_cat.ns.badge_swipes")
        val lookup = spark.read.format(classOf[SimpleExternalLookupDataSource].getName).load()
        checkStream(
          swipes.join(lookup, swipes("employee") === lookup("id"))
            .select(swipes("door"), lookup("department")),
          Seq(Row("lobby", "engineering"), Row("lobby", "research")))
        // An aggregate that reads only stream columns and ignores duplicates does not need a left
        // outer join, so the optimizer removes an ordinary one. A lookup join stays.
        val latestByDoor = swipes.join(lookup, swipes("employee") === lookup("id"), "left_outer")
          .groupBy(swipes("door")).agg(max(swipes("minute")))
        QueryTest.sameRows(
          Seq(Row("lobby", 10), Row("garage", 20)),
          runStream(latestByDoor, "complete").rows).foreach(fail(_))
      }
    }
  }

  test("a stream whose scan gets the query's filters joins its lookup joins as without them") {
    // The stream drops the rows that fail the filters that its scan builder took when the query
    // started, so a micro-batch that does not apply one of them fails.
    val lookup = spark.read.format(classOf[SimpleExternalLookupDataSource].getName).load()
    def results(pushdown: Boolean): (Seq[Seq[Row]], Seq[Seq[Expression]]) = {
      var result: (Seq[Seq[Row]], Seq[Seq[Expression]]) = null
      withSQLConf(
          "spark.sql.catalog.stream_cat" -> classOf[InMemoryTableCatalog].getName,
          SQLConf.STREAMING_V2_FILTER_PUSHDOWN_ENABLED.key -> pushdown.toString) {
        withTable("stream_cat.ns.badge_swipes") {
          sql("CREATE TABLE stream_cat.ns.badge_swipes (employee INT, door STRING, minute INT) " +
            s"TBLPROPERTIES ('${InMemoryBaseTable.STREAMING_FILTER_PUSHDOWN}' = 'true', " +
            s"'${InMemoryBaseTable.SIMULATE_STREAMING_FILTERS}' = 'true')")
          sql("INSERT INTO stream_cat.ns.badge_swipes VALUES " +
            "(1, 'lobby', 10), (2, 'garage', 20), (3, 'dock', 30), (1, 'garage', 40)")
          val swipes = spark.readStream.table("stream_cat.ns.badge_swipes")
          val onEmployee = swipes("employee") === lookup("id")
          val runs = Seq(
            // A filter on stream columns below the join, and above an inner and a left outer join.
            runStream(swipes.where(swipes("minute") > 15).join(lookup, onEmployee)),
            runStream(swipes.join(lookup, onEmployee).where(swipes("minute") > 15)),
            runStream(swipes.join(lookup, onEmployee, "left_outer")
              .where(swipes("door") === "garage")),
            // A filter on both sides, which predicate pushdown can move into the join condition.
            runStream(swipes.join(lookup, onEmployee)
              .where(swipes("door") =!= lookup("department") && swipes("minute") < 35)),
            // An aggregate that ignores duplicates needs no ordinary left outer join, which the
            // optimizer removes, with its filters pushed to the stream. A lookup join stays.
            runStream(swipes.join(lookup, onEmployee, "left_outer").where(swipes("minute") > 15)
              .groupBy(swipes("door")).agg(max(swipes("minute"))), "complete"))
          val pushed = runs.map(_.execution.optimizedPlan.collect {
            case relation: StreamingDataSourceV2ScanRelation => relation.pushedFilters
          }.flatten)
          result = (runs.map(_.rows), pushed)
        }
      }
      result
    }
    val (rows, pushed) = results(pushdown = true)
    val (expected, notPushed) = results(pushdown = false)
    rows.zip(expected).foreach { case (actual, expected) =>
      QueryTest.sameRows(expected, actual).foreach(fail(_))
    }
    assert(notPushed.forall(_.isEmpty))
    // At least the filter below the join reaches the stream's scan.
    assert(pushed.head.nonEmpty)
  }

  test("external lookup capability produces a unary external lookup join") {
    val lookup = spark.read.format(classOf[SimpleExternalLookupDataSource].getName).load()
    val left = streamOf(Seq(1).toDF("id"))
    val joined = left.join(lookup, left("id") === lookup("id"), "inner")

    val plan = runStream(joined).plan
    assert(plan.exists(_.isInstanceOf[PreparedExternalLookupJoin]))
    assert(!plan.toString.contains("SimpleExternalLookupScan"))
  }

  test("a dual-capability source keeps an ordinary join unless the hint opts in") {
    val lookup = spark.read.format(classOf[DualCapabilityExternalLookupDataSource].getName).load()
    val left = streamOf(Seq(1).toDF("id"))
    val expected = Seq(Row(1, 1, "engineering"), Row(1, 1, "research"))

    assert(lookup.queryExecution.analyzed.isInstanceOf[DataSourceV2Relation])
    assert(lookup.queryExecution.analyzed.isInstanceOf[ExternalLookupRelation] === false)
    SimpleExternalLookupDataSource.resetCounters()
    val ordinary = left.join(lookup, left("id") === lookup("id"), "inner")
    assert(runStream(ordinary).plan.exists(_.isInstanceOf[PreparedExternalLookupJoin]) === false)
    checkStream(ordinary, expected)
    assert(SimpleExternalLookupDataSource.readerCount.get() === 0)

    SimpleExternalLookupDataSource.resetCounters()
    val hinted = lookup.hint("external_lookup")
    val lookupJoin = left.join(hinted, left("id") === hinted("id"), "inner")
    assert(runStream(lookupJoin).plan.exists(_.isInstanceOf[PreparedExternalLookupJoin]))
    checkStream(lookupJoin, expected)
    assert(SimpleExternalLookupDataSource.requestCount.get() > 0)
  }

  test("a hinted or aliased lookup side follows the stream-static contract") {
    val dual = spark.read.format(classOf[DualCapabilityExternalLookupDataSource].getName).load()
    val lookupOnly = spark.read.format(classOf[SimpleExternalLookupDataSource].getName).load()
    val batch = Seq(1).toDF("id")
    // Without the hint, a dual-capability table keeps an ordinary batch join.
    checkAnswer(
      batch.join(dual, batch("id") === dual("id")),
      Seq(Row(1, 1, "engineering"), Row(1, 1, "research")))
    // With it, the batch join fails: a hint the user wrote is never silently dropped.
    Seq(
      dual -> "`DualCapabilityExternalLookupTable`",
      lookupOnly -> "`SimpleExternalLookupTable`").foreach { case (table, relation) =>
      val hinted = table.hint("external_lookup")
      checkError(
        exception = intercept[AnalysisException] {
          batch.join(hinted, batch("id") === hinted("id"))
        },
        condition = "EXTERNAL_LOOKUP_JOIN_UNSUPPORTED.STATIC_LEFT_SIDE",
        parameters = Map("relation" -> relation))
    }
    // A lookup-only table on the left side is found beneath an alias too.
    val stream = streamOf(batch)
    checkError(
      exception = intercept[AnalysisException] {
        lookupOnly.as("l").join(stream, $"l.id" === stream("id"))
      },
      condition = "EXTERNAL_LOOKUP_JOIN_UNSUPPORTED.LOOKUP_TABLE_ON_LEFT",
      parameters = Map("relation" -> "`SimpleExternalLookupTable`"))
  }

  test("a physical join strategy hint conflicts with the external lookup hint") {
    val dual = spark.read.format(classOf[DualCapabilityExternalLookupDataSource].getName).load()
    val left = streamOf(Seq(1).toDF("id"))
    val lookupSide = dual.hint("external_lookup").hint("broadcast")
    val streamSide = left.hint("broadcast")
    val hinted = dual.hint("external_lookup")
    Seq(
      () => left.join(lookupSide, left("id") === lookupSide("id")),
      () => streamSide.join(hinted, streamSide("id") === hinted("id"))).foreach { query =>
      checkError(
        exception = intercept[AnalysisException] { query().queryExecution.analyzed },
        condition = "EXTERNAL_LOOKUP_JOIN_UNSUPPORTED.CONFLICTING_JOIN_STRATEGY",
        parameters = Map(
          "strategy" -> "BROADCAST",
          "relation" -> "`DualCapabilityExternalLookupTable`"))
    }
    // Both hints may name the same relation, in either order. A strategy hint is never merged
    // into the lookup hint, so the conflict is still reported.
    withTempView("external_lookup_left", "external_lookup_right") {
      left.createOrReplaceTempView("external_lookup_left")
      dual.createOrReplaceTempView("external_lookup_right")
      Seq("EXTERNAL_LOOKUP(r), MERGE(r)", "MERGE(r), EXTERNAL_LOOKUP(r)").foreach { hints =>
        checkError(
          exception = intercept[AnalysisException] {
            sql(s"SELECT /*+ $hints */ * FROM external_lookup_left l " +
              "JOIN external_lookup_right r ON l.id = r.id")
          },
          condition = "EXTERNAL_LOOKUP_JOIN_UNSUPPORTED.CONFLICTING_JOIN_STRATEGY",
          parameters = Map(
            "strategy" -> "MERGE",
            "relation" -> "`DualCapabilityExternalLookupTable`"))
      }
    }

    // Without the lookup hint, the strategy hint plans the ordinary join it asks for.
    SimpleExternalLookupDataSource.resetCounters()
    val ordinary = left.join(dual.hint("merge"), left("id") === dual("id"))
    assert(runStream(ordinary).plan.exists(_.isInstanceOf[PreparedExternalLookupJoin]) === false)
    checkStream(ordinary, Seq(Row(1, 1, "engineering"), Row(1, 1, "research")))
    assert(SimpleExternalLookupDataSource.requestCount.get() === 0)
  }

  test("a hinted dual-capability source fails when no lookup access path exists") {
    val lookup = spark.read.format(classOf[DualCapabilityExternalLookupDataSource].getName).load()
    val invoices = streamOf(Seq((1, "sales")).toDF("invoice", "costCenter"))

    checkStream(
      invoices.join(lookup, invoices("costCenter") === lookup("department")),
      Seq(Row(1, "sales", 2, "sales")))
    val hinted = lookup.hint("external_lookup")
    checkError(
      exception = streamFailure[AnalysisException](
        invoices.join(hinted, invoices("costCenter") === hinted("department"))),
      condition = "EXTERNAL_LOOKUP_JOIN_UNSUPPORTED.NO_LOOKUP_PREDICATE",
      parameters = Map("table" -> "`DualCapabilityExternalLookupTable`"))
  }

  test("a physical strategy hint on a lookup-only join is rejected") {
    val lookup = spark.read.format(classOf[SimpleExternalLookupDataSource].getName).load()
    val left = streamOf(Seq(1, 3).toDF("id"))
    val withLookupHint = lookup.hint("external_lookup").hint("shuffle_hash")
    Seq(
      "BROADCAST" ->
        (() => left.join(lookup.hint("broadcast"), left("id") === lookup("id"), "left_outer")),
      "BROADCAST" ->
        (() => left.join(broadcast(lookup), left("id") === lookup("id"), "left_outer")),
      "MERGE" -> (() => left.hint("merge").join(lookup, left("id") === lookup("id"))),
      "SHUFFLE_HASH" ->
        (() => left.join(withLookupHint, left("id") === withLookupHint("id")))).foreach {
      case (strategy, query) =>
        checkError(
          exception = intercept[AnalysisException] { query().queryExecution.analyzed },
          condition = "EXTERNAL_LOOKUP_JOIN_UNSUPPORTED.CONFLICTING_JOIN_STRATEGY",
          parameters = Map("strategy" -> strategy, "relation" -> "`SimpleExternalLookupTable`"))
    }
  }

  test("an ordinary join over an enriched lookup join result stays ordinary") {
    val lookup = spark.read.format(classOf[SimpleExternalLookupDataSource].getName).load()
    val events = streamOf(Seq(1, 2).toDF("id"))
    val enriched = events.join(lookup, events("id") === lookup("id"))
      .select(events("id").as("event_id"), lookup("department"))
    val base = Seq((1, "a"), (2, "b")).toDF("id", "tag")
    val expected = Seq(
      Row(1, "a", 1, "engineering"), Row(1, "a", 1, "research"), Row(2, "b", 2, "sales"))
    def lookupJoinCount(df: DataFrame): Int =
      runStream(df).plan.collect { case j: PreparedExternalLookupJoin => j }.size

    val joined = base.join(enriched, base("id") === enriched("event_id"))
    assert(lookupJoinCount(joined) === 1)
    checkStream(joined, expected)

    withTempView("external_lookup_base", "external_lookup_events", "external_lookup_right") {
      base.createOrReplaceTempView("external_lookup_base")
      events.createOrReplaceTempView("external_lookup_events")
      lookup.createOrReplaceTempView("external_lookup_right")
      val sqlJoined = sql(
        """
          |SELECT b.id, b.tag, e.event_id, e.department
          |FROM external_lookup_base b JOIN (
          |  SELECT v.id AS event_id, r.department
          |  FROM external_lookup_events v JOIN external_lookup_right r ON v.id = r.id) e
          |ON b.id = e.event_id
          |""".stripMargin)
      assert(lookupJoinCount(sqlJoined) === 1)
      checkStream(sqlJoined, expected)
    }
  }

  test("an external lookup hint outside any join is ignored") {
    val dual = spark.read.format(classOf[DualCapabilityExternalLookupDataSource].getName).load()
    val expected = Seq(Row(1, "engineering"), Row(1, "research"), Row(2, "sales"))

    checkAnswer(dual.hint("external_lookup"), expected)
    assert(dual.hint("external_lookup").count() === 3)
    withTempView("external_lookup_right") {
      dual.createOrReplaceTempView("external_lookup_right")
      checkAnswer(sql("SELECT /*+ EXTERNAL_LOOKUP */ * FROM external_lookup_right"), expected)
      checkAnswer(sql("SELECT /*+ EXTERNAL_LOOKUP(r) */ * FROM external_lookup_right r"), expected)
      checkAnswer(
        sql("SELECT /*+ EXTERNAL_LOOKUP(r) */ id, count(*) FROM external_lookup_right r " +
          "GROUP BY id"),
        Seq(Row(1, 2), Row(2, 1)))
    }
  }

  test("a query without a lookup-only table or the hint is not searched for lookup joins") {
    // Classifying the joins of a plan inlines its CTE references, so a plan that can have no lookup
    // join is left alone, such as this self-join of a CTE over a table that supports batch reads.
    // The pattern bits tell it, so the plan is not walked and its tables are not asked for their
    // capabilities, also for a subquery, a micro-batch, cache substitution and each join.
    val dual = spark.read.format(classOf[DualCapabilityExternalLookupDataSource].getName).load()
    val lookupOnly = spark.read.format(classOf[SimpleExternalLookupDataSource].getName).load()
    withTempView("employee_directory", "lookup_directory", "badge_swipes") {
      dual.createOrReplaceTempView("employee_directory")
      lookupOnly.createOrReplaceTempView("lookup_directory")
      streamOf(Seq((1, "lobby")).toDF("employee", "door")).createOrReplaceTempView("badge_swipes")
      val ordinary = sql(
        """
          |WITH d AS (TABLE employee_directory)
          |SELECT a.id, a.department, b.department FROM d a JOIN d b ON a.id = b.id
          |""".stripMargin)
      val withSubquery = sql(
        """
          |SELECT id, department FROM employee_directory WHERE id IN (
          |  SELECT a.id FROM employee_directory a JOIN employee_directory b ON a.id = b.id)
          |""".stripMargin)
      val microBatch = runStream(sql(
        """
          |SELECT s.door, d.department
          |FROM badge_swipes s JOIN employee_directory d ON s.employee = d.id
          |""".stripMargin)).execution.analyzed
      // An entry in the cache makes cache substitution look for lookup sides.
      val cached = spark.range(1).toDF("cached").cache()
      try {
        assert(cached.count() === 1)
        Seq(ordinary.queryExecution.analyzed, withSubquery.queryExecution.analyzed, microBatch)
          .foreach { plan =>
            // Computes the pattern bits, with which each relation asks its table once.
            assert(ExternalLookupJoinEligibility.mayHaveLookupJoin(plan) === false)
            DualCapabilityExternalLookupTable.capabilityCalls.set(0)
            assert(ExternalLookupJoinEligibility.lookupSidePlans(plan).isEmpty)
            CheckExternalLookupJoin(plan)
            assert(FormExternalLookupJoin(plan) eq plan)
            spark.sharedState.cacheManager.useCachedData(plan)
            plan.collectWithSubqueries { case join: Join => join }.foreach { join =>
              assert(
                ExternalLookupJoinEligibility(join) === ExternalLookupJoinEligibility.NotLookupJoin)
            }
            assert(DualCapabilityExternalLookupTable.capabilityCalls.get() === 0)
          }
      } finally {
        cached.unpersist(blocking = true)
      }
      checkAnswer(ordinary, Seq(
        Row(1, "engineering", "engineering"), Row(1, "engineering", "research"),
        Row(1, "research", "engineering"), Row(1, "research", "research"),
        Row(2, "sales", "sales")))
      // The hint or a lookup-only table makes a lookup join possible, so the joins are classified.
      Seq("/*+ EXTERNAL_LOOKUP(d) */" -> "employee_directory", "" -> "lookup_directory").foreach {
        case (hint, table) =>
          val query = sql(
            s"""
              |WITH d AS (TABLE $table)
              |SELECT $hint s.door, d.department FROM badge_swipes s JOIN d ON s.employee = d.id
              |""".stripMargin)
          assert(ExternalLookupJoinEligibility.mayHaveLookupJoin(query.queryExecution.analyzed))
          checkStream(query, Seq(Row("lobby", "engineering"), Row("lobby", "research")))
      }
    }
  }

  test("a lookup side may be hinted more than once") {
    val dual = spark.read.format(classOf[DualCapabilityExternalLookupDataSource].getName).load()
    val swipes = streamOf(Seq((1, "door-a")).toDF("employee", "door"))
    val expected = Seq(Row(1, "door-a", "engineering"), Row(1, "door-a", "research"))
    def lookupJoinCount(df: DataFrame): Int =
      runStream(df).plan.collect { case j: PreparedExternalLookupJoin => j }.size

    val twice = dual.hint("external_lookup").hint("external_lookup")
    val joined = swipes.join(twice, swipes("employee") === twice("id"))
      .select(swipes("employee"), swipes("door"), twice("department"))
    assert(lookupJoinCount(joined) === 1)
    checkStream(joined, expected)
    withTempView("badge_swipes", "hinted_directory", "employee_directory") {
      swipes.createOrReplaceTempView("badge_swipes")
      dual.hint("external_lookup").createOrReplaceTempView("hinted_directory")
      dual.createOrReplaceTempView("employee_directory")
      // A view over a hinted table that the hint names too, and a name given twice in any case.
      Seq(
        "EXTERNAL_LOOKUP(d)" -> "hinted_directory",
        "EXTERNAL_LOOKUP(d, d)" -> "employee_directory",
        "EXTERNAL_LOOKUP(d, D)" -> "employee_directory").foreach { case (hint, directory) =>
        val df = sql(
          s"""
            |SELECT /*+ $hint */ s.employee, s.door, d.department
            |FROM badge_swipes s JOIN $directory d ON s.employee = d.id
            |""".stripMargin)
        assert(lookupJoinCount(df) === 1, hint)
        checkStream(df, expected)
      }
    }
  }

  test("an external lookup hint in a subquery without a join is ignored like a strategy hint") {
    val dual = spark.read.format(classOf[DualCapabilityExternalLookupDataSource].getName).load()
    withTempView("badge_swipes", "employee_directory") {
      Seq((1, "door-a"), (3, "door-b")).toDF("employee", "door")
        .createOrReplaceTempView("badge_swipes")
      dual.createOrReplaceTempView("employee_directory")
      Seq("BROADCAST", "EXTERNAL_LOOKUP").foreach { hint =>
        checkAnswer(
          sql(
            s"""
              |SELECT door FROM badge_swipes s WHERE EXISTS (
              |  SELECT /*+ $hint */ * FROM employee_directory d WHERE d.id = s.employee)
              |""".stripMargin),
          Seq(Row("door-a")))
        checkAnswer(
          sql(
            s"""
              |SELECT door, (
              |  SELECT /*+ $hint */ max(d.department) FROM employee_directory d
              |  WHERE d.id = s.employee)
              |FROM badge_swipes s
              |""".stripMargin),
          Seq(Row("door-a", "research"), Row("door-b", null)))
      }
    }
  }

  test("an empty lookup side does not call the connector") {
    val lookup = spark.read.format(classOf[SimpleExternalLookupDataSource].getName).load()
    val left = streamOf(Seq(1, 3).toDF("id"))
    withTempView("empty_side_lookup", "empty_side_left") {
      lookup.createOrReplaceTempView("empty_side_lookup")
      left.createOrReplaceTempView("empty_side_left")
      Seq("inner" -> "INNER", "left_outer" -> "LEFT OUTER").foreach { case (joinType, sqlJoin) =>
        val expected =
          if (joinType == "inner") Seq.empty[Row] else Seq(Row(1, null, null), Row(3, null, null))
        val empty = lookup.where(lit(false))
        Seq(
          () => left.join(empty, left("id") === empty("id"), joinType),
          () => sql(s"SELECT * FROM empty_side_left l $sqlJoin JOIN " +
            "(SELECT * FROM empty_side_lookup WHERE 1 = 0) r ON l.id = r.id")).foreach { query =>
          SimpleExternalLookupDataSource.resetCounters()
          val joined = query()
          assert(
            runStream(joined).plan.exists(_.isInstanceOf[PreparedExternalLookupJoin]) === false)
          checkStream(joined, expected)
          assert(SimpleExternalLookupDataSource.lookupCandidates.get() === Seq.empty)
          assert(SimpleExternalLookupDataSource.readerCount.get() === 0)
          assert(SimpleExternalLookupDataSource.requestCount.get() === 0)
        }
      }
    }
  }

  test("a lookup side with another operator above the table fails when the DataFrame is built") {
    val lookup = spark.read.format(classOf[SimpleExternalLookupDataSource].getName).load()
    val dual = spark.read.format(classOf[DualCapabilityExternalLookupDataSource].getName).load()
    val left = streamOf(Seq(1, 3).toDF("id"))
    val values = Seq(1).toDF("v")
    // A lookup-only table, which no other join can read, and a hinted table that supports batch
    // reads too. The join fails even where the optimizer would remove the operator. A subquery is
    // planned apart from the lookup results, so a projection or filter with one cannot run either.
    Seq(
      lookup.limit(1),
      lookup.distinct(),
      dual.limit(1).hint("external_lookup"),
      lookup.filter($"id".isin(values)),
      lookup.filter(values.where(col("v") === 1).exists()),
      lookup.filter($"id" < spark.range(2).select(max($"id")).scalar())).foreach { right =>
      checkError(
        exception = intercept[AnalysisException] {
          left.join(right, left("id") === right("id"), "left_outer")
        },
        condition = "EXTERNAL_LOOKUP_JOIN_UNSUPPORTED.LOOKUP_PLAN",
        parameters = Map.empty)
    }
    assert(SimpleExternalLookupDataSource.readerCount.get() === 0)
  }

  test("a USING join with the EXTERNAL_LOOKUP hint runs as an external lookup join") {
    val left = streamOf(Seq(1, 3).toDF("id"))
    Seq[Class[_]](
      classOf[DualCapabilityExternalLookupDataSource],
      classOf[SimpleExternalLookupDataSource]).foreach { source =>
      val lookup = spark.read.format(source.getName).load().hint("external_lookup")
      val joined = left.join(lookup, Seq("id"), "left_outer")
      assert(runStream(joined).plan.exists(_.isInstanceOf[PreparedExternalLookupJoin]))
      checkStream(joined, Seq(Row(1, "engineering"), Row(1, "research"), Row(3, null)))
    }

    // Without a condition the hinted join still fails, on the final plan.
    val dual = spark.read.format(classOf[DualCapabilityExternalLookupDataSource].getName).load()
      .hint("external_lookup")
    checkError(
      exception = streamFailure[AnalysisException](left.join(dual)),
      condition = "EXTERNAL_LOOKUP_JOIN_UNSUPPORTED.MISSING_JOIN_CONDITION",
      parameters = Map.empty)
  }

  test("an external lookup hint is found beneath an alias on either join side") {
    val dual = spark.read.format(classOf[DualCapabilityExternalLookupDataSource].getName).load()
    val left = streamOf(Seq(1).toDF("id"))

    val aliased = dual.hint("external_lookup").as("r")
    val joined = left.join(aliased, left("id") === $"r.id")
    assert(runStream(joined).plan.exists(_.isInstanceOf[PreparedExternalLookupJoin]))
    checkStream(joined, Seq(Row(1, 1, "engineering"), Row(1, 1, "research")))

    checkError(
      exception = intercept[AnalysisException] {
        left.hint("external_lookup").as("l").join(dual, $"l.id" === dual("id"))
          .queryExecution.analyzed
      },
      condition = "INVALID_EXTERNAL_LOOKUP_HINT.NOT_ON_LOOKUP_SIDE",
      parameters = Map.empty)
  }

  test("a strategy hint on a nested join does not disable an outer lookup join") {
    val left = streamOf(Seq((1, 10), (2, 20)).toDF("id", "k"))
    val other = Seq((10, "a"), (20, "b")).toDF("k", "v")
    val nested = left.join(other.hint("merge"), "k")
    def usesLookup(df: DataFrame): Boolean =
      runStream(df).plan.exists(_.isInstanceOf[PreparedExternalLookupJoin])

    val dual = spark.read.format(classOf[DualCapabilityExternalLookupDataSource].getName).load()
      .hint("external_lookup")
    val dualJoin = nested.join(dual, nested("id") === dual("id"))
    assert(usesLookup(dualJoin))
    checkStream(dualJoin, Seq(
      Row(10, 1, "a", 1, "engineering"), Row(10, 1, "a", 1, "research"),
      Row(20, 2, "b", 2, "sales")))

    // A lookup-only table on the outer join becomes a lookup join too, despite the nested join's
    // MERGE hint.
    val lookupOnly = spark.read.format(classOf[SimpleExternalLookupDataSource].getName).load()
    val lookupOnlyJoin = nested.join(lookupOnly, nested("id") === lookupOnly("id"))
    assert(usesLookup(lookupOnlyJoin))
    checkStream(lookupOnlyJoin, Seq(
      Row(10, 1, "a", 1, "engineering"), Row(10, 1, "a", 1, "research"),
      Row(20, 2, "b", 2, "sales")))
  }

  test("an explicit external lookup option marker rejects unsupported join shapes") {
    val lookup = spark.read.format(classOf[DualCapabilityExternalLookupDataSource].getName).load()
    val left = streamOf(Seq(1).toDF("id"))

    checkError(
      exception = intercept[AnalysisException] {
        left.join(
          lookup.hint("external_lookup"),
          left("id") === lookup("id"),
          "right_outer").queryExecution.analyzed
      },
      condition = "EXTERNAL_LOOKUP_JOIN_UNSUPPORTED.JOIN_TYPE",
      parameters = Map(
        "joinType" -> "RIGHT OUTER",
        "relation" -> "`DualCapabilityExternalLookupTable`",
        "supportedJoinTypes" -> SUPPORTED_JOIN_TYPES))

    // A cross join fails with its join type while it is analyzed, before its missing condition.
    checkError(
      exception = intercept[AnalysisException](left.crossJoin(lookup.hint("external_lookup"))),
      condition = "EXTERNAL_LOOKUP_JOIN_UNSUPPORTED.JOIN_TYPE",
      parameters = Map(
        "joinType" -> "CROSS",
        "relation" -> "`DualCapabilityExternalLookupTable`",
        "supportedJoinTypes" -> SUPPORTED_JOIN_TYPES))

    // A strategy hint on the same join conflicts before the join type is checked.
    checkError(
      exception = intercept[AnalysisException] {
        left.join(
          lookup.hint("external_lookup").hint("merge"),
          left("id") === lookup("id"),
          "right_outer").queryExecution.analyzed
      },
      condition = "EXTERNAL_LOOKUP_JOIN_UNSUPPORTED.CONFLICTING_JOIN_STRATEGY",
      parameters = Map("strategy" -> "MERGE", "relation" -> "`DualCapabilityExternalLookupTable`"))
  }

  test("external lookup predicate negotiation includes type and null semantics") {
    SimpleExternalLookupDataSource.resetCounters()
    val lookup = spark.read.format(classOf[SimpleExternalLookupDataSource].getName).load()
    val left = streamOf(Seq[java.lang.Integer](null, 1).toDF("id"))

    runStream(left.join(lookup, left("id") <=> lookup("id"), "inner"))
    assert(SimpleExternalLookupDataSource.lookupCandidates.get() ===
      Seq(("<=>", "id", IntegerType)))
  }

  test("an external lookup hint on an ineligible join is rejected") {
    val dual = spark.read.format(classOf[DualCapabilityExternalLookupDataSource].getName).load()
      .hint("external_lookup")
    val left = streamOf(Seq(1).toDF("id"))

    // A cross join fails with its join type too, although it has no condition.
    Seq[(() => DataFrame, String)](
      (() => left.join(dual, left("id") === dual("id"), "right_outer"), "RIGHT OUTER"),
      (() => left.crossJoin(dual), "CROSS")).foreach { case (joined, joinType) =>
      val error = intercept[AnalysisException](joined())
      assert(error.getCondition === "EXTERNAL_LOOKUP_JOIN_UNSUPPORTED.JOIN_TYPE")
      assert(error.getMessageParameters.get("joinType") === joinType)
      assert(error.getMessageParameters.get("relation") === "`DualCapabilityExternalLookupTable`")
    }
  }

  test("an external lookup join is a stream-static join with the lookup-only table on the right") {
    val lookup = spark.read.format(classOf[SimpleExternalLookupDataSource].getName).load()
    val batch = Seq(1).toDF("id")
    // The left side must be a streaming query.
    checkError(
      exception = intercept[AnalysisException] {
        batch.join(lookup, batch("id") === lookup("id"))
      },
      condition = "EXTERNAL_LOOKUP_JOIN_UNSUPPORTED.STATIC_LEFT_SIDE",
      parameters = Map("relation" -> "`SimpleExternalLookupTable`"))
    // The sides are never swapped.
    val stream = streamOf(batch)
    checkError(
      exception = intercept[AnalysisException] {
        lookup.join(stream, lookup("id") === stream("id"))
      },
      condition = "EXTERNAL_LOOKUP_JOIN_UNSUPPORTED.LOOKUP_TABLE_ON_LEFT",
      parameters = Map("relation" -> "`SimpleExternalLookupTable`"))
  }

  test("external lookup validates connector planning and request contracts") {
    val left = streamOf(Seq(1).toDF("id"))
    Seq(
      ("nullKeySelection", "true", "NULL_RESULT",
        Map("method" -> "SupportsExternalLookup.selectLookupPredicates()")),
      ("selectionMode", "nullElement", "UNOFFERED_SELECTED_PREDICATE", Map("predicate" -> "null")),
      ("selectionMode", "duplicate", "DUPLICATE_SELECTED_PREDICATE", Map("predicate" -> "id = ?")),
      ("selectionMode", "copy", "UNOFFERED_SELECTED_PREDICATE", Map("predicate" -> "id = ?")),
      ("maxBatchSize", "0", "NON_POSITIVE_MAX_PARAMETER_ROWS", Map("value" -> "0"))).foreach {
      case (option, value, subClass, parameters) =>
        val lookup = spark.read
          .format(classOf[SimpleExternalLookupDataSource].getName)
          .option(option, value)
          .load()
        checkError(
          exception = streamFailure[AnalysisException](
            left.join(lookup, left("id") === lookup("id"))),
          condition = s"DATA_SOURCE_INVALID_EXTERNAL_LOOKUP.$subClass",
          parameters = Map("table" -> "`SimpleExternalLookupTable`") ++ parameters)
    }

    def misconfigured(kind: String): DataFrame = spark.read
      .format(classOf[MisconfiguredExternalLookupDataSource].getName)
      .option("kind", kind)
      .load()
    val table = Map("table" -> "`MisconfiguredExternalLookupTable`")
    Seq(
      ("unknownField", "UNKNOWN_SCAN_FIELD", Map("field" -> "`unknown`")),
      ("duplicateField", "DUPLICATE_SCAN_FIELD", Map("field" -> "`id`"))).foreach {
      case (kind, subClass, parameters) =>
        val lookup = misconfigured(kind)
        checkError(
          exception = streamFailure[AnalysisException](
            left.join(lookup, left("id") === lookup("id"))),
          condition = s"DATA_SOURCE_INVALID_EXTERNAL_LOOKUP.$subClass",
          parameters = table ++ parameters)
    }

    val noExternalLookup = misconfigured("noExternalLookup")
    checkError(
      exception = streamFailure[SparkUnsupportedOperationException](
        left.join(noExternalLookup, left("id") === noExternalLookup("id"))),
      condition = "DATA_SOURCE_EXTERNAL_LOOKUP_NOT_SUPPORTED",
      parameters = Map("description" -> "NoExternalLookupScan"))

    // Violations found while the join runs are reported the same way, by the tasks.
    val nullCursor = spark.read
      .format(classOf[SimpleExternalLookupDataSource].getName)
      .option("nullCursor", "true")
      .load()
    Seq(
      (nullCursor, "NULL_RESULT", Map("table" -> "`SimpleExternalLookupTable`",
        "method" -> "ExternalLookupReader.lookupBatch()")),
      (misconfigured("nullReader"), "NULL_RESULT",
        table ++ Map("method" -> "ExternalLookupReaderFactory.createReader()")),
      (misconfigured("invalidOrdinal"), "INVALID_INPUT_ORDINAL",
        table ++ Map("ordinal" -> "5", "numParameterRows" -> "1")),
      // The ordinals just outside the request's parameter rows.
      (misconfigured("negativeOrdinal"), "INVALID_INPUT_ORDINAL",
        table ++ Map("ordinal" -> "-1", "numParameterRows" -> "1")),
      (misconfigured("ordinalAtCount"), "INVALID_INPUT_ORDINAL",
        table ++ Map("ordinal" -> "1", "numParameterRows" -> "1")),
      (misconfigured("nullRow"), "NULL_RESULT",
        table ++ Map("method" -> "ExternalLookupResultCursor.get()")),
      // A value that breaks its column's type, which Spark relied on while it optimized.
      (misconfigured("nullValue"), "NULL_IN_NOT_NULL_FIELD",
        table ++ Map("field" -> "`department`")),
      (misconfigured("nullUnderNotNullReadSchema"), "NULL_IN_NOT_NULL_FIELD",
        table ++ Map("field" -> "`department`")),
      (misconfigured("decimalOutOfRange"), "DECIMAL_OUT_OF_RANGE",
        table ++ Map("field" -> "`amount`", "type" -> "\"DECIMAL(5,2)\"")),
      // The same checks reach nested values: a decimal array element with too many digits, and
      // a decimal map value that would need rounding.
      (misconfigured("nestedDecimalOutOfRange"), "DECIMAL_OUT_OF_RANGE",
        table ++ Map("field" -> "`amounts`.`element`", "type" -> "\"DECIMAL(5,2)\"")),
      (misconfigured("nestedDecimalRounding"), "DECIMAL_OUT_OF_RANGE",
        table ++ Map("field" -> "`rates`.`value`", "type" -> "\"DECIMAL(5,2)\"")),
      // A compact decimal with too many digits, written into an UnsafeRow, a struct in one, or a
      // columnar row or array, each of which records its type's precision without checking it.
      (misconfigured("compactDecimal:unsafe:outOfRange"), "DECIMAL_OUT_OF_RANGE",
        table ++ Map("field" -> "`amount`", "type" -> "\"DECIMAL(5,2)\"")),
      (misconfigured("compactDecimal:unsafeNested:outOfRange"), "DECIMAL_OUT_OF_RANGE",
        table ++ Map("field" -> "`price`.`amount`", "type" -> "\"DECIMAL(5,2)\"")),
      (misconfigured("compactDecimal:columnar:outOfRange"), "DECIMAL_OUT_OF_RANGE",
        table ++ Map("field" -> "`amount`", "type" -> "\"DECIMAL(5,2)\"")),
      (misconfigured("compactDecimal:columnarArray:outOfRange"), "DECIMAL_OUT_OF_RANGE",
        table ++ Map("field" -> "`amounts`.`element`", "type" -> "\"DECIMAL(5,2)\"")),
      // A map key is never null, whether it is a primitive or a struct.
      (misconfigured("nullMapKey"), "NULL_IN_NOT_NULL_FIELD",
        table ++ Map("field" -> "`labels`.`key`")),
      (misconfigured("nullComplexMapKey"), "NULL_IN_NOT_NULL_FIELD",
        table ++ Map("field" -> "`parts`.`key`"))).foreach {
      case (lookup, subClass, parameters) =>
        // A left outer join binds the lookup columns as nullable for its output, but checks the
        // returned values against the table and the read schema, as an inner join does.
        Seq("inner", "left_outer").foreach { joinType =>
          checkError(
            exception = streamFailure[SparkRuntimeException](
              left.join(lookup, left("id") === lookup("id"), joinType)),
            condition = s"DATA_SOURCE_INVALID_EXTERNAL_LOOKUP.$subClass",
            parameters = parameters)
        }
    }

    // Planning failures that need no hint. A catalog can serve a table without SupportsRead.
    val noLookupBuilder = misconfigured("noLookupBuilder")
    val nonReadable = Dataset.ofRows(spark, DataSourceV2Relation.create(
      NonReadableExternalLookupTable, None, None, CaseInsensitiveStringMap.empty))
    val simple = spark.read.format(classOf[SimpleExternalLookupDataSource].getName).load()
    Seq(
      (noLookupBuilder, left("id") === noLookupBuilder("id"), "SCAN_BUILDER",
        "MisconfiguredExternalLookupTable"),
      (nonReadable, left("id") === nonReadable("id"), "TABLE", "NonReadableExternalLookupTable"),
      (simple, simple("department") === "sales", "NO_LOOKUP_PREDICATE",
        "SimpleExternalLookupTable")).foreach { case (lookup, condition, subClass, name) =>
      checkError(
        exception = streamFailure[AnalysisException](left.join(lookup, condition)),
        condition = s"EXTERNAL_LOOKUP_JOIN_UNSUPPORTED.$subClass",
        parameters = Map("table" -> s"`$name`"))
    }
    // A BIGINT stream key casts the INT lookup column, so Spark has no candidate to offer.
    val bigintLeft = streamOf(Seq(1L).toDF("id"))
    checkError(
      exception = streamFailure[AnalysisException](
        bigintLeft.join(simple, bigintLeft("id") === simple("id"))),
      condition = "EXTERNAL_LOOKUP_JOIN_UNSUPPORTED.NO_LOOKUP_CANDIDATE",
      parameters = Map("table" -> "`SimpleExternalLookupTable`"))
  }

  test("an external lookup hinted DataFrame can be reused by multiple joins") {
    val lookup = spark.read.format(classOf[SimpleExternalLookupDataSource].getName).load()
      .hint("external_lookup")

    val first = streamOf(Seq(1).toDF("id"))
    val second = streamOf(Seq(2).toDF("id"))
    checkStream(first.join(lookup, first("id") === lookup("id")),
      Seq(Row(1, 1, "engineering"), Row(1, 1, "research")))
    checkStream(second.join(lookup, second("id") === lookup("id")),
      Seq(Row(2, 2, "sales")))
  }

  test("external lookup finalizes expressions on the stream side after analysis") {
    val lookup = spark.read.format(classOf[SimpleExternalLookupDataSource].getName).load()
    val left = streamOf(Seq[java.lang.Integer](null, 1).toDF("id"))

    checkStream(
      left.join(
        lookup,
        coalesce(left("id"), lit(0)) === lookup("id"),
        "inner"),
      Seq(Row(1, 1, "engineering"), Row(1, 1, "research")))
  }

  test("external lookup prunes unused throwable lookup expressions") {
    val rawLookup = spark.read.format(classOf[SimpleExternalLookupDataSource].getName).load()
    val lookup = rawLookup.select($"id", expr("raise_error('unused')").as("unused"))
    val left = streamOf(Seq(1).toDF("id"))

    checkStream(
      left.join(lookup, left("id") === lookup("id"), "left_semi"),
      Seq(Row(1)))

    val projected = left.as("left").join(
      lookup.as("lookup"),
      $"left.id" === $"lookup.id",
      "inner").select($"left.id")
    checkStream(projected, Seq(Row(1), Row(1)))
  }

  test("external lookup rejects a non-deterministic lookup-side projection or filter") {
    val rawLookup = spark.read.format(classOf[SimpleExternalLookupDataSource].getName).load()
    val left = streamOf(Seq(1, 1, 2).toDF("id"))
    // The join evaluates the lookup-side stages on each returned row, so a random value could
    // differ between two stream rows that join the same stored row.
    Seq(
      rawLookup.select($"id", $"department", rand(3).as("sample")),
      rawLookup.filter(rand(3) >= 0.5)).foreach { lookup =>
      checkError(
        exception = intercept[AnalysisException] {
          left.join(lookup, left("id") === lookup("id"), "inner")
        },
        condition = "EXTERNAL_LOOKUP_JOIN_UNSUPPORTED.LOOKUP_PLAN",
        parameters = Map.empty)
    }
    assert(SimpleExternalLookupDataSource.readerCount.get() === 0)
  }

  test("external lookup hint rejects invalid placement and relation targets") {
    val lookup = spark.read.format(classOf[SimpleExternalLookupDataSource].getName).load()
    val left = streamOf(Seq(1).toDF("id"))

    checkError(
      exception = intercept[AnalysisException] {
        left.hint("external_lookup").join(lookup, left("id") === lookup("id"))
          .queryExecution.analyzed
      },
      condition = "INVALID_EXTERNAL_LOOKUP_HINT.NOT_ON_LOOKUP_SIDE",
      parameters = Map.empty)

    withTempView("external_lookup_left", "external_lookup_right") {
      left.createOrReplaceTempView("external_lookup_left")
      lookup.createOrReplaceTempView("external_lookup_right")
      def query(hint: String): DataFrame = sql(
        s"""
          |SELECT /*+ $hint */ l.id, r.department
          |FROM external_lookup_left l JOIN external_lookup_right r ON l.id = r.id
          |""".stripMargin)
      checkError(
        exception = intercept[AnalysisException] {
          query("EXTERNAL_LOOKUP(missing)").queryExecution.analyzed
        },
        condition = "INVALID_EXTERNAL_LOOKUP_HINT.RELATION_NOT_FOUND",
        parameters = Map("relations" -> "`missing`"))
      // A name that is repeated is reported once.
      checkError(
        exception = intercept[AnalysisException] {
          query("EXTERNAL_LOOKUP(missing, missing)").queryExecution.analyzed
        },
        condition = "INVALID_EXTERNAL_LOOKUP_HINT.RELATION_NOT_FOUND",
        parameters = Map("relations" -> "`missing`"))
      // A missing name fails next to one that matches, and every missing name is reported, but
      // not the one that matches.
      Seq(
        "EXTERNAL_LOOKUP(r, missing)" -> "`missing`",
        "EXTERNAL_LOOKUP(missing, r, other)" -> "`missing`, `other`").foreach {
        case (hint, missing) =>
          checkError(
            exception = intercept[AnalysisException](query(hint).queryExecution.analyzed),
            condition = "INVALID_EXTERNAL_LOOKUP_HINT.RELATION_NOT_FOUND",
            parameters = Map("relations" -> missing))
      }
    }
    checkError(
      exception = intercept[AnalysisException] {
        left.join(lookup.as("r"), left("id") === col("r.id"))
          .hint("external_lookup", "r", "missing").queryExecution.analyzed
      },
      condition = "INVALID_EXTERNAL_LOOKUP_HINT.RELATION_NOT_FOUND",
      parameters = Map("relations" -> "`missing`"))

    val stream = MemoryStream[Int].toDF().toDF("id")
    checkError(
      exception = intercept[AnalysisException] {
        left.join(stream.hint("external_lookup"), left("id") === stream("id"))
          .queryExecution.analyzed
      },
      condition = "EXTERNAL_LOOKUP_JOIN_UNSUPPORTED.STREAMING_LOOKUP_SIDE",
      parameters = Map.empty)
  }

  test("external lookup reports lookup sides and conditions it cannot plan") {
    val claims = streamOf(Seq((1, "sales")).toDF("id", "costCenter"))
    def planningError(lookup: DataFrame, condition: Column): AnalysisException =
      streamFailure[AnalysisException](claims.join(lookup, condition))
    def analysisError(lookup: DataFrame, condition: Column): AnalysisException =
      intercept[AnalysisException](claims.join(lookup, condition))

    // A hinted lookup side fails when the DataFrame is built: with LOOKUP_PLAN when it reads no
    // DSv2 table, as a local relation does, and with TABLE for a DSv2 table that does not support
    // external lookup.
    val local = Seq(1).toDF("id").hint("external_lookup")
    checkError(
      exception = analysisError(local, claims("id") === local("id")),
      condition = "EXTERNAL_LOOKUP_JOIN_UNSUPPORTED.LOOKUP_PLAN",
      parameters = Map.empty)

    val batchOnly = spark.read
      .format(classOf[MisconfiguredExternalLookupDataSource].getName)
      .option("kind", "batchOnly")
      .load()
      .hint("external_lookup")
    checkError(
      exception = analysisError(batchOnly, claims("id") === batchOnly("id")),
      condition = "EXTERNAL_LOOKUP_JOIN_UNSUPPORTED.TABLE",
      parameters = Map("table" -> "`MisconfiguredExternalLookupTable`"))

    val noLookupBuilder = spark.read
      .format(classOf[MisconfiguredExternalLookupDataSource].getName)
      .option("kind", "noLookupBuilder")
      .load()
    checkError(
      exception = planningError(noLookupBuilder, claims("id") === noLookupBuilder("id")),
      condition = "EXTERNAL_LOOKUP_JOIN_UNSUPPORTED.SCAN_BUILDER",
      parameters = Map("table" -> "`MisconfiguredExternalLookupTable`"))

    val lookup = spark.read.format(classOf[SimpleExternalLookupDataSource].getName).load()
    checkError(
      exception = planningError(lookup, claims("costCenter") === lookup("department")),
      condition = "EXTERNAL_LOOKUP_JOIN_UNSUPPORTED.NO_LOOKUP_PREDICATE",
      parameters = Map("table" -> "`SimpleExternalLookupTable`"))
  }

  test("external lookup maps a selected predicate to its own occurrence") {
    val lookup = spark.read
      .format(classOf[SimpleExternalLookupDataSource].getName)
      .option("selectionMode", "last")
      .load()
    val left = streamOf(Seq((1, 2), (2, 2), (2, 1)).toDF("a", "b"))

    // Both candidates have the shape `id = ?`; only the second one, bound to `b`, is selected.
    val joined =
      left.join(lookup, lookup("id") === left("a") && lookup("id") === left("b"), "left_outer")
    assert(runStream(joined).rows.toSet ===
      Set(Row(1, 2, null, null), Row(2, 2, 2, "sales"), Row(2, 1, null, null)))
    assert(SimpleExternalLookupDataSource.lookupCandidates.get() ===
      Seq(("=", "id", IntegerType), ("=", "id", IntegerType)))
    assert(SimpleExternalLookupDataSource.lookupCandidateDescriptions.get() ===
      Seq("id = ?", "id = ?"))
    assert(SimpleExternalLookupDataSource.requestedKeys.asScala.toSeq === Seq(2, 1))

    // Selected in reverse order, each parameter row is `(b, a)`.
    SimpleExternalLookupDataSource.resetCounters()
    val reversed = spark.read
      .format(classOf[SimpleExternalLookupDataSource].getName)
      .option("selectionMode", "reversed")
      .load()
    val condition = reversed("id") === left("a") && reversed("id") === left("b")
    assert(runStream(left.join(reversed, condition, "left_outer")).rows.toSet ===
      Set(Row(1, 2, null, null), Row(2, 2, 2, "sales"), Row(2, 1, null, null)))
    assert(SimpleExternalLookupDataSource.requestedKeys.asScala.toSeq === Seq(2, 2, 1))
  }

  test("a parent projection of some lookup columns still plans an external lookup join") {
    val lookup = spark.read.format(classOf[SimpleExternalLookupDataSource].getName).load()
    val left = streamOf(Seq(1, 3).toDF("id"))
    // Column pruning narrows the join's requiredLookupOutput to the lookup columns that the
    // projection reads, and projects the lookup side once the join no longer needs some of its
    // columns, which lookup planning accepts. Here the join still reads both columns of the table,
    // the key and `department`, so the lookup side stays the table.
    val joined = left.join(lookup, left("id") === lookup("id"), "left_outer")
      .select(left("id"), lookup("department"))

    assert(runStream(joined).plan.exists(_.isInstanceOf[PreparedExternalLookupJoin]))
    // The source is asked for the lookup column that the projection reads, and for the key, which
    // Spark checks on every returned row.
    assert(SimpleExternalLookupDataSource.requiredColumns.get() === Seq("id", "department"))
    checkStream(joined, Seq(Row(1, "engineering"), Row(1, "research"), Row(3, null)))
  }

  test("external lookup join supports inner and left outer joins") {
    val lookup = spark.read.format(classOf[SimpleExternalLookupDataSource].getName).load()
    val left = streamOf(Seq((1, "one"), (2, "two"), (3, "three")).toDF("id", "label"))

    checkStream(
      left.join(lookup, left("id") === lookup("id"), "inner"),
      Seq(Row(1, "one", 1, "engineering"), Row(1, "one", 1, "research"),
        Row(2, "two", 2, "sales")))
    checkStream(
      left.join(lookup, left("id") === lookup("id"), "left_outer"),
      Seq(Row(1, "one", 1, "engineering"), Row(1, "one", 1, "research"),
        Row(2, "two", 2, "sales"), Row(3, "three", null, null)))

    // Only comparisons of a lookup field with a stream-side value are offered. The connector
    // selects `id = ?`, and Spark evaluates the other conjuncts: the stream-side one on each stream
    // row before the lookup, and the others on the joined rows.
    SimpleExternalLookupDataSource.resetCounters()
    val flagged = streamOf(Seq((1, true), (2, true), (1, false)).toDF("id", "flag"))
    checkStream(
      flagged.join(
        lookup,
        flagged("id") === lookup("id") && flagged("flag") &&
          lookup("department") === "engineering" && length(lookup("department")) > lookup("id"),
        "left_outer"),
      Seq(Row(1, true, 1, "engineering"), Row(2, true, null, null), Row(1, false, null, null)))
    assert(SimpleExternalLookupDataSource.lookupCandidates.get() ===
      Seq(("=", "id", IntegerType), ("=", "department", StringType)))
  }

  test("a stream row that fails a stream-side conjunct of the condition is not looked up") {
    val lookup = spark.read.format(classOf[SimpleExternalLookupDataSource].getName).load()
    val left = streamOf(
      Seq((1, Some(true)), (2, Some(false)), (3, Some(true)), (4, None)).toDF("id", "flag"))
    val key = left("id") === lookup("id")
    // The join checks `flag` on each stream row before the lookup. A row where it is false or null
    // cannot match, so its key is not requested: an inner or left semi join drops the row, a left
    // outer join pads it with nulls, and a left anti join returns it.
    val matched = Seq(Row(1, true, 1, "engineering"), Row(1, true, 1, "research"))
    val unmatched =
      Seq(Row(2, false, null, null), Row(3, true, null, null), Row(4, null, null, null))
    Seq(
      "inner" -> matched,
      "left_outer" -> (matched ++ unmatched),
      "left_semi" -> Seq(Row(1, true)),
      "left_anti" -> Seq(Row(2, false), Row(3, true), Row(4, null))).foreach {
      case (joinType, expected) =>
        SimpleExternalLookupDataSource.resetCounters()
        checkStream(left.join(lookup, key && left("flag"), joinType), expected)
        assert(SimpleExternalLookupDataSource.requestedKeys.asScala.toSeq === Seq(1, 3))
    }

    // A conjunct that can throw is not checked early: it is evaluated on the joined rows only, so
    // never for id 3, which has no match, and every key is requested.
    SimpleExternalLookupDataSource.resetCounters()
    val throwing = when(left("id") === 3, raise_error(lit("evaluated for id 3"))).otherwise(true)
    checkStream(
      left.join(lookup, key && throwing, "left_outer"),
      matched ++ Seq(Row(2, false, 2, "sales"), Row(3, true, null, null), Row(4, null, null, null)))
    assert(SimpleExternalLookupDataSource.requestedKeys.asScala.toSeq === Seq(1, 2, 3, 4))

    // A conjunct that is not deterministic is not checked early either. Analysis rejects one in a
    // join condition, so the physical join gets one directly: `id + rand(0) < 0`. It is false for
    // every row, but it is evaluated on the joined rows only, so every key is requested.
    val lookupJoin = runStream(left.join(lookup, key, "left_outer")).lookupJoin
    val streamId = lookupJoin.child.output.head
    val nonDeterministic =
      LessThan(Add(Cast(streamId, DoubleType), Rand(Literal(0L))), Literal(0.0))
    SimpleExternalLookupDataSource.resetCounters()
    val rows = lookupJoin.copy(
      residualCondition = Some(nonDeterministic),
      child = LocalTableScanExec(
        lookupJoin.child.output, Seq(InternalRow(1, true), InternalRow(2, false)), None))
      .executeCollect()
    assert(rows.map(row => row.getInt(0) -> row.isNullAt(2)).toSeq === Seq(1 -> true, 2 -> true))
    // Each row is in its own partition, so the order of the requests can vary.
    assert(SimpleExternalLookupDataSource.requestedKeys.asScala.toSeq.sorted === Seq(1, 2))
  }

  test("external lookup-only source joins without an explicit hint") {
    val lookup = spark.read.format(classOf[SimpleExternalLookupDataSource].getName).load()
    val left = streamOf(Seq(1).toDF("id"))

    assert(lookup.queryExecution.analyzed.isInstanceOf[ExternalLookupRelation])
    val joined = left.join(lookup, "id")
    assert(runStream(joined).plan.exists(_.isInstanceOf[PreparedExternalLookupJoin]))
    checkStream(joined, Seq(Row(1, "engineering"), Row(1, "research")))
  }

  test("an external lookup-only source cannot be read outside an eligible lookup join") {
    val lookup = spark.read.format(classOf[SimpleExternalLookupDataSource].getName).load()
    val left = streamOf(Seq(1).toDF("id"))
    checkError(
      exception = intercept[AnalysisException] { lookup.queryExecution.optimizedPlan },
      condition = "UNSUPPORTED_FEATURE.TABLE_OPERATION",
      parameters = Map(
        "tableName" -> "`SimpleExternalLookupTable`",
        "operation" -> "batch scan"))
    // A join that reads it but cannot be a lookup join fails with the reason. The condition must
    // be written in the join, not in a later filter.
    checkError(
      exception = streamFailure[AnalysisException](
        left.join(lookup).where(left("id") === lookup("id"))),
      condition = "EXTERNAL_LOOKUP_JOIN_UNSUPPORTED.MISSING_JOIN_CONDITION",
      parameters = Map.empty)
    // An unsupported join type fails when the DataFrame is built, with or without a condition.
    Seq[(() => DataFrame, String)](
      (() => left.join(lookup, left("id") === lookup("id"), "full_outer"), "FULL OUTER"),
      (() => left.crossJoin(lookup), "CROSS"),
      (() => left.crossJoin(lookup).where(left("id") === lookup("id")), "CROSS")).foreach {
      case (joined, joinType) =>
        val error = intercept[AnalysisException](joined())
        assert(error.getCondition === "EXTERNAL_LOOKUP_JOIN_UNSUPPORTED.JOIN_TYPE")
        assert(error.getMessageParameters.asScala.toMap === Map(
          "joinType" -> joinType,
          "relation" -> "`SimpleExternalLookupTable`",
          "supportedJoinTypes" -> SUPPORTED_JOIN_TYPES))
    }
  }

  test("catalog-resolved tables follow the same lookup selection") {
    withSQLConf(
        s"spark.sql.catalog.$EXTERNAL_LOOKUP_CATALOG_NAME" ->
          classOf[ExternalLookupTestCatalog].getName) {
      withTempView("external_lookup_left") {
        streamOf(Seq(1, 3).toDF("id")).createOrReplaceTempView("external_lookup_left")
        def query(table: String, hint: String = ""): DataFrame = sql(
          s"""
            |SELECT $hint l.id, r.department
            |FROM external_lookup_left l
            |LEFT JOIN $EXTERNAL_LOOKUP_CATALOG_NAME.ns.$table r ON l.id = r.id
            |""".stripMargin)
        val expected = Seq(Row(1, "engineering"), Row(1, "research"), Row(3, null))
        def usesLookup(df: DataFrame): Boolean =
          runStream(df).plan.exists(_.isInstanceOf[PreparedExternalLookupJoin])

        SimpleExternalLookupDataSource.resetCounters()
        val lookupOnly = query("lookup")
        assert(usesLookup(lookupOnly))
        checkStream(lookupOnly, expected)
        assert(SimpleExternalLookupDataSource.requestCount.get() > 0)

        SimpleExternalLookupDataSource.resetCounters()
        val dual = query("dual")
        assert(!usesLookup(dual))
        checkStream(dual, expected)
        assert(SimpleExternalLookupDataSource.readerCount.get() === 0)

        val hintedDual = query("dual", "/*+ EXTERNAL_LOOKUP(r) */")
        assert(usesLookup(hintedDual))
        checkStream(hintedDual, expected)
      }
    }
  }

  test("a named external lookup hint marks every relation that it names") {
    // As with join strategy hints, the name matches each unaliased reference to the table, so
    // both joins read it through external lookup.
    withSQLConf(
        s"spark.sql.catalog.$EXTERNAL_LOOKUP_CATALOG_NAME" ->
          classOf[ExternalLookupTestCatalog].getName) {
      withTempView("external_lookup_left") {
        streamOf(Seq(1, 3).toDF("id")).createOrReplaceTempView("external_lookup_left")
        val catalogTable = s"$EXTERNAL_LOOKUP_CATALOG_NAME.ns.dual"
        val twice = sql(
          s"""
            |SELECT /*+ EXTERNAL_LOOKUP(dual) */ *
            |FROM external_lookup_left JOIN $catalogTable USING (id) JOIN $catalogTable USING (id)
            |""".stripMargin)
        val run = runStream(twice)
        assert(run.plan.collect { case join: PreparedExternalLookupJoin => join }.size === 2)
        QueryTest.sameRows(
          Seq(Row(1, "engineering", "engineering"), Row(1, "engineering", "research"),
            Row(1, "research", "engineering"), Row(1, "research", "research")),
          run.rows).foreach(fail(_))
      }
    }
  }

  test("a named external lookup hint targets unaliased, multipart, and aliased relations") {
    withSQLConf(
        s"spark.sql.catalog.$EXTERNAL_LOOKUP_CATALOG_NAME" ->
          classOf[ExternalLookupTestCatalog].getName) {
      withTempView("external_lookup_left", "external_lookup_right") {
        streamOf(Seq(1, 3).toDF("id")).createOrReplaceTempView("external_lookup_left")
        spark.read.format(classOf[DualCapabilityExternalLookupDataSource].getName).load()
          .createOrReplaceTempView("external_lookup_right")
        val expected = Seq(Row(1, "engineering"), Row(1, "research"), Row(3, null))
        def query(hintTarget: String, right: String, rightName: String): DataFrame = sql(
          s"""
            |SELECT /*+ EXTERNAL_LOOKUP($hintTarget) */ l.id, $rightName.department
            |FROM external_lookup_left l LEFT JOIN $right ON l.id = $rightName.id
            |""".stripMargin)

        val catalogTable = s"$EXTERNAL_LOOKUP_CATALOG_NAME.ns.dual"
        Seq(
          ("external_lookup_right", "external_lookup_right", "external_lookup_right"),
          ("dual", catalogTable, "dual"),
          ("ns.dual", catalogTable, "dual"),
          (catalogTable, catalogTable, "dual"),
          ("r", s"$catalogTable r", "r"),
          ("s", "(SELECT * FROM external_lookup_right) s", "s")).foreach {
          case (hintTarget, right, rightName) =>
            val df = query(hintTarget, right, rightName)
            assert(runStream(df).plan.exists(_.isInstanceOf[PreparedExternalLookupJoin]),
              hintTarget)
            checkStream(df, expected)
        }

        // Like join strategy hints, a target name does not reach into an aliased subquery.
        checkError(
          exception = intercept[AnalysisException] {
            query("external_lookup_right", "(SELECT * FROM external_lookup_right) s", "s")
              .queryExecution.analyzed
          },
          condition = "INVALID_EXTERNAL_LOOKUP_HINT.RELATION_NOT_FOUND",
          parameters = Map("relations" -> "`external_lookup_right`"))
      }
    }
  }

  test("a catalog-resolved lookup-only table in an ineligible join fails with the reason") {
    withSQLConf(
        s"spark.sql.catalog.$EXTERNAL_LOOKUP_CATALOG_NAME" ->
          classOf[ExternalLookupTestCatalog].getName) {
      val table = s"$EXTERNAL_LOOKUP_CATALOG_NAME.ns.lookup"
      val left = streamOf(Seq(1).toDF("id"))
      val crossJoin = intercept[AnalysisException](left.crossJoin(spark.read.table(table)))
      assert(crossJoin.getCondition === "EXTERNAL_LOOKUP_JOIN_UNSUPPORTED.JOIN_TYPE")
      assert(crossJoin.getMessageParameters.asScala.toMap === Map("joinType" -> "CROSS",
        "relation" -> "`SimpleExternalLookupTable`", "supportedJoinTypes" -> SUPPORTED_JOIN_TYPES))
      withTempView("external_lookup_left") {
        left.createOrReplaceTempView("external_lookup_left")
        val fullOuter = intercept[AnalysisException] {
          sql(s"SELECT * FROM external_lookup_left l FULL OUTER JOIN $table r ON l.id = r.id")
        }
        assert(fullOuter.getCondition === "EXTERNAL_LOOKUP_JOIN_UNSUPPORTED.JOIN_TYPE")
        assert(fullOuter.getMessageParameters.asScala.toMap === Map(
          "joinType" -> "FULL OUTER",
          "relation" -> "`SimpleExternalLookupTable`",
          "supportedJoinTypes" -> SUPPORTED_JOIN_TYPES))
      }
    }
  }

  test("a catalog-resolved table can be composed into DataFrame joins") {
    withSQLConf(
        s"spark.sql.catalog.$EXTERNAL_LOOKUP_CATALOG_NAME" ->
          classOf[ExternalLookupTestCatalog].getName) {
      val left = streamOf(Seq(1, 3).toDF("id"))
      val expected = Seq(Row(1, 1, "engineering"), Row(1, 1, "research"), Row(3, null, null))
      def usesLookup(df: DataFrame): Boolean =
        runStream(df).plan.exists(_.isInstanceOf[PreparedExternalLookupJoin])

      val lookupOnly = spark.table(s"$EXTERNAL_LOOKUP_CATALOG_NAME.ns.lookup")
      val lookupJoin = left.join(lookupOnly, left("id") === lookupOnly("id"), "left_outer")
      assert(usesLookup(lookupJoin))
      checkStream(lookupJoin, expected)

      val dual = spark.table(s"$EXTERNAL_LOOKUP_CATALOG_NAME.ns.dual")
      val ordinary = left.join(dual, left("id") === dual("id"), "left_outer")
      assert(!usesLookup(ordinary))
      checkStream(ordinary, expected)
      val hinted = dual.hint("external_lookup")
      val hintedJoin = left.join(hinted, left("id") === hinted("id"), "left_outer")
      assert(usesLookup(hintedJoin))
      checkStream(hintedJoin, expected)

      checkError(
        exception = intercept[AnalysisException] { lookupOnly.collect() },
        condition = "UNSUPPORTED_FEATURE.TABLE_OPERATION",
        parameters = Map(
          "tableName" -> "`SimpleExternalLookupTable`",
          "operation" -> "batch scan"))
    }
  }

  test("a lookup join reads a table whose name does not parse as an identifier") {
    // Table.name() is free-form, such as the path of the table's files, so classifying a join
    // renders it only for an error. Cache substitution classifies the joins of a plan only while
    // something is cached.
    withSQLConf(
        s"spark.sql.catalog.$EXTERNAL_LOOKUP_CATALOG_NAME" ->
          classOf[ExternalLookupTestCatalog].getName) {
      val swipes = streamOf(Seq((1, "lobby"), (3, "garage")).toDF("employee", "door"))
      val lookupOnly = spark.table(s"$EXTERNAL_LOOKUP_CATALOG_NAME.ns.path_lookup")
      val dual = spark.table(s"$EXTERNAL_LOOKUP_CATALOG_NAME.ns.path_dual")
      def checkLookupJoins(): Unit = Seq(lookupOnly, dual.hint("external_lookup")).foreach {
        directory =>
          val run = runStream(swipes.join(directory, swipes("employee") === directory("id"))
            .select(swipes("employee"), directory("department")))
          assert(run.plan.exists(_.isInstanceOf[PreparedExternalLookupJoin]))
          QueryTest.sameRows(Seq(Row(1, "engineering"), Row(1, "research")), run.rows)
            .foreach(fail(_))
      }
      checkLookupJoins()
      val unrelated = Seq(1).toDF("id").cache()
      try {
        assert(unrelated.count() === 1)
        checkLookupJoins()
      } finally {
        unrelated.unpersist(blocking = true)
      }
    }
  }

  test("an ineligible join names a table whose name does not parse as an identifier") {
    // The error renders the whole name as one quoted part, and a name that parses as a multipart
    // identifier as its quoted parts.
    withSQLConf(
        s"spark.sql.catalog.$EXTERNAL_LOOKUP_CATALOG_NAME" ->
          classOf[ExternalLookupTestCatalog].getName,
        SQLConf.SQL_ASOF_JOIN_ENABLED.key -> "true") {
      val swipes = streamOf(Seq((1, "lobby"), (3, "garage")).toDF("employee", "door"))
      val lookupOnly = spark.table(s"$EXTERNAL_LOOKUP_CATALOG_NAME.ns.path_lookup")
      val hinted =
        spark.table(s"$EXTERNAL_LOOKUP_CATALOG_NAME.ns.path_dual").hint("external_lookup")
      val multipart = spark.table(s"$EXTERNAL_LOOKUP_CATALOG_NAME.ns.multipart_lookup")
      def analysisError(query: => DataFrame): AnalysisException =
        intercept[AnalysisException](query.queryExecution.analyzed)
      def joinTypeError(joinType: String, relation: String): Map[String, String] = Map(
        "joinType" -> joinType,
        "relation" -> relation,
        "supportedJoinTypes" -> SUPPORTED_JOIN_TYPES)

      val badges = Seq(1, 3).toDF("employee")
      checkError(
        exception = analysisError(badges.join(hinted, badges("employee") === hinted("id"))),
        condition = "EXTERNAL_LOOKUP_JOIN_UNSUPPORTED.STATIC_LEFT_SIDE",
        parameters = Map("relation" -> "`./users`"))
      checkError(
        exception = analysisError(
          swipes.join(hinted, swipes("employee") === hinted("id"), "right_outer")),
        condition = "EXTERNAL_LOOKUP_JOIN_UNSUPPORTED.JOIN_TYPE",
        parameters = joinTypeError("RIGHT OUTER", "`./users`"))
      Seq(lookupOnly -> "`./users`", multipart -> "`ns`.`users`").foreach {
        case (directory, relation) =>
          checkError(
            exception = analysisError(
              swipes.join(directory, swipes("employee") === directory("id"), "full_outer")),
            condition = "EXTERNAL_LOOKUP_JOIN_UNSUPPORTED.JOIN_TYPE",
            parameters = joinTypeError("FULL OUTER", relation))
      }
      withTempView("renamed_swipes") {
        swipes.createOrReplaceTempView("renamed_swipes")
        checkError(
          exception = analysisError(sql(
            s"""SELECT /*+ EXTERNAL_LOOKUP(d) */ s.employee FROM renamed_swipes s
               |ASOF JOIN $EXTERNAL_LOOKUP_CATALOG_NAME.ns.path_lookup d
               |MATCH_CONDITION(s.employee >= d.id) ON s.employee = d.id""".stripMargin)),
          condition = "EXTERNAL_LOOKUP_JOIN_UNSUPPORTED.JOIN_TYPE",
          parameters = joinTypeError("ASOF", "`./users`"))
      }
    }
  }

  test("a named external lookup hint accepts relation names given as strings") {
    withSQLConf(
        s"spark.sql.catalog.$EXTERNAL_LOOKUP_CATALOG_NAME" ->
          classOf[ExternalLookupTestCatalog].getName) {
      val catalogTable = s"$EXTERNAL_LOOKUP_CATALOG_NAME.ns.dual"
      val swipes = streamOf(Seq((1, "lobby"), (3, "garage")).toDF("employee", "door"))
      val expected = Seq(Row(1, "lobby", "engineering"), Row(1, "lobby", "research"))
      def checkLookupJoin(df: DataFrame, name: String): Unit = {
        val run = runStream(df)
        assert(run.plan.exists(_.isInstanceOf[PreparedExternalLookupJoin]), name)
        QueryTest.sameRows(expected, run.rows).foreach(fail(_))
      }

      // The Dataset API, which PySpark and Spark Connect use, passes a relation name as a string.
      val aliased = spark.table(catalogTable).as("r")
      val unaliased = spark.table(catalogTable)
      Seq(
        "r" -> swipes.join(aliased, swipes("employee") === $"r.id"),
        catalogTable -> swipes.join(unaliased, swipes("employee") === unaliased("id")),
        "ns.dual" -> swipes.join(unaliased, swipes("employee") === unaliased("id"))).foreach {
        case (name, joined) =>
          checkLookupJoin(
            joined.hint("external_lookup", name).select("employee", "door", "department"), name)
      }
      checkError(
        exception = intercept[AnalysisException] {
          swipes.join(aliased, swipes("employee") === $"r.id").hint("external_lookup", 1)
        },
        condition = "INVALID_EXTERNAL_LOOKUP_HINT.UNSUPPORTED_PARAMETER",
        parameters = Map("parameter" -> "\"1\""))

      withTempView("badge_swipes") {
        swipes.createOrReplaceTempView("badge_swipes")
        Seq(
          "'r'" -> s"$catalogTable r ON s.employee = r.id",
          "'ns.dual'" -> s"$catalogTable ON s.employee = dual.id").foreach { case (name, join) =>
          checkLookupJoin(
            sql(s"SELECT /*+ EXTERNAL_LOOKUP($name) */ employee, door, department " +
              s"FROM badge_swipes s JOIN $join"),
            name)
        }
      }
    }
  }

  test("with hints disabled, only a lookup-only table runs as an external lookup join") {
    val swipes = streamOf(Seq((1, "lobby"), (3, "garage")).toDF("employee", "door"))
    val expected = Seq(Row(1, "lobby", 1, "engineering"), Row(1, "lobby", 1, "research"))
    withSQLConf(SQLConf.DISABLE_HINTS.key -> "true") {
      // The hint is removed with the others, so the table that also supports batch reads keeps an
      // ordinary join.
      val dual = spark.read.format(classOf[DualCapabilityExternalLookupDataSource].getName).load()
        .hint("external_lookup")
      val ordinary = runStream(swipes.join(dual, swipes("employee") === dual("id")))
      assert(ordinary.plan.exists(_.isInstanceOf[PreparedExternalLookupJoin]) === false)
      QueryTest.sameRows(expected, ordinary.rows).foreach(fail(_))
      assert(SimpleExternalLookupDataSource.readerCount.get() === 0)

      val lookupOnly = spark.read.format(classOf[SimpleExternalLookupDataSource].getName).load()
      val lookupJoin = runStream(swipes.join(lookupOnly, swipes("employee") === lookupOnly("id")))
      assert(lookupJoin.plan.exists(_.isInstanceOf[PreparedExternalLookupJoin]))
      QueryTest.sameRows(expected, lookupJoin.rows).foreach(fail(_))
    }
  }

  test("a lookup side that reads a metadata column of the table is rejected") {
    withSQLConf(
        s"spark.sql.catalog.$EXTERNAL_LOOKUP_CATALOG_NAME" ->
          classOf[ExternalLookupTestCatalog].getName) {
      val table = s"$EXTERNAL_LOOKUP_CATALOG_NAME.ns.versioned"
      withTempView("badge_swipes") {
        streamOf(Seq((1, "lobby"), (3, "garage")).toDF("employee", "door"))
          .createOrReplaceTempView("badge_swipes")
        val dataColumns =
          sql(s"SELECT s.door, r.department FROM badge_swipes s JOIN $table r ON s.employee = r.id")
        checkStream(dataColumns, Seq(Row("lobby", "engineering"), Row("lobby", "research")))
        Seq(
          s"SELECT s.door, r._version FROM badge_swipes s JOIN $table r ON s.employee = r.id",
          s"""SELECT s.door FROM badge_swipes s JOIN $table r
             |  ON s.employee = r.id AND r._version > 0""".stripMargin).foreach { query =>
          checkError(
            exception = intercept[AnalysisException](sql(query)),
            condition = "EXTERNAL_LOOKUP_JOIN_UNSUPPORTED.METADATA_COLUMN",
            parameters = Map("relation" -> "`VersionedExternalLookupTable`"))
        }
      }
    }
  }

  test("a lookup side read through a WITH clause runs as an external lookup join") {
    withSQLConf(
        s"spark.sql.catalog.$EXTERNAL_LOOKUP_CATALOG_NAME" ->
          classOf[ExternalLookupTestCatalog].getName) {
      withTempView("badge_swipes") {
        streamOf(Seq((1, "lobby"), (3, "garage")).toDF("employee", "door"))
          .createOrReplaceTempView("badge_swipes")
        val expected = Seq(Row(1, "lobby", "engineering"), Row(1, "lobby", "research"))
        def query(table: String, hint: String = "", joinType: String = "JOIN"): DataFrame = sql(
          s"""
            |WITH lk AS (TABLE $EXTERNAL_LOOKUP_CATALOG_NAME.ns.$table)
            |SELECT $hint s.employee, s.door, lk.department
            |FROM badge_swipes s $joinType lk ON s.employee = lk.id
            |""".stripMargin)
        def usesLookup(df: DataFrame): Boolean =
          runStream(df).plan.exists(_.isInstanceOf[PreparedExternalLookupJoin])

        Seq(query("lookup"), query("dual", hint = "/*+ EXTERNAL_LOOKUP(lk) */")).foreach { df =>
          assert(usesLookup(df))
          checkStream(df, expected)
        }
        val ordinary = query("dual")
        assert(usesLookup(ordinary) === false)
        checkStream(ordinary, expected)

        val fullOuter =
          intercept[AnalysisException](query("lookup", joinType = "FULL OUTER JOIN"))
        assert(fullOuter.getCondition === "EXTERNAL_LOOKUP_JOIN_UNSUPPORTED.JOIN_TYPE")
        assert(fullOuter.getMessageParameters.asScala.toMap === Map(
          "joinType" -> "FULL OUTER",
          "relation" -> "`SimpleExternalLookupTable`",
          "supportedJoinTypes" -> SUPPORTED_JOIN_TYPES))
      }
    }
  }

  test("a lookup side read through chained WITH definitions runs as an external lookup join") {
    // The lookup side reads `a` through `b`, so classifying the join inlines both definitions, and
    // cache substitution leaves both alone.
    withSQLConf(
        s"spark.sql.catalog.$EXTERNAL_LOOKUP_CATALOG_NAME" ->
          classOf[ExternalLookupTestCatalog].getName) {
      withTempView("badge_swipes") {
        streamOf(Seq((1, "lobby"), (3, "garage")).toDF("employee", "door"))
          .createOrReplaceTempView("badge_swipes")
        def query(table: String, hint: String = ""): DataFrame = sql(
          s"""
            |WITH a AS (TABLE $EXTERNAL_LOOKUP_CATALOG_NAME.ns.$table), b AS (TABLE a)
            |SELECT $hint s.employee, b.department FROM badge_swipes s JOIN b ON s.employee = b.id
            |""".stripMargin)
        def checkLookupJoin(df: DataFrame, expected: Seq[Row]): Unit = {
          val run = runStream(df)
          assert(run.plan.exists(_.isInstanceOf[PreparedExternalLookupJoin]))
          QueryTest.sameRows(expected, run.rows).foreach(fail(_))
        }
        val directory = Seq(Row(1, "engineering"), Row(1, "research"))
        checkLookupJoin(query("lookup"), directory)
        checkLookupJoin(query("dual", "/*+ EXTERNAL_LOOKUP(b) */"), directory)
        // Data that the cache read before the directory changed replaces neither definition.
        val cached = spark.table(s"$EXTERNAL_LOOKUP_CATALOG_NAME.ns.dual").cache()
        try {
          assert(cached.count() === 3)
          SimpleExternalLookupDataSource.departmentsById.set(
            Map(1 -> Seq("legal"), 2 -> Seq("sales")))
          checkLookupJoin(query("dual", "/*+ EXTERNAL_LOOKUP(b) */"), Seq(Row(1, "legal")))
        } finally {
          cached.unpersist(blocking = true)
        }
      }
    }
  }

  test("a lookup side that is a DataFrame of a WITH query runs as an external lookup join") {
    // The WithCTE of the DataFrame is on the lookup side's path, below the join.
    withSQLConf(
        s"spark.sql.catalog.$EXTERNAL_LOOKUP_CATALOG_NAME" ->
          classOf[ExternalLookupTestCatalog].getName) {
      val swipes = streamOf(Seq((1, "lobby"), (3, "garage")).toDF("employee", "door"))
      def withQuery(table: String): DataFrame =
        sql(s"WITH d AS (TABLE $EXTERNAL_LOOKUP_CATALOG_NAME.ns.$table) TABLE d")
      Seq(withQuery("lookup"), withQuery("dual").hint("external_lookup")).foreach { directory =>
        val run = runStream(swipes.join(directory, swipes("employee") === directory("id"))
          .select(swipes("employee"), directory("department")))
        assert(run.plan.exists(_.isInstanceOf[PreparedExternalLookupJoin]))
        QueryTest.sameRows(Seq(Row(1, "engineering"), Row(1, "research")), run.rows)
          .foreach(fail(_))
      }
    }
  }

  test("a lookup side read through a MATERIALIZED CTE fails when the DataFrame is built") {
    withSQLConf(
        s"spark.sql.catalog.$EXTERNAL_LOOKUP_CATALOG_NAME" ->
          classOf[ExternalLookupTestCatalog].getName) {
      withTempView("badge_swipes") {
        streamOf(Seq((1, "lobby"), (3, "garage")).toDF("employee", "door"))
          .createOrReplaceTempView("badge_swipes")
        def query(table: String, option: String, hint: String = ""): DataFrame = sql(
          s"""
            |WITH lk AS $option (TABLE $EXTERNAL_LOOKUP_CATALOG_NAME.ns.$table)
            |SELECT $hint s.employee, lk.department
            |FROM badge_swipes s JOIN lk ON s.employee = lk.id
            |""".stripMargin)
        val expected = Seq(Row(1, "engineering"), Row(1, "research"))
        Seq("lookup" -> "", "dual" -> "/*+ EXTERNAL_LOOKUP(lk) */").foreach { case (table, hint) =>
          // A MATERIALIZED CTE is evaluated once, and a lookup join cannot read that result.
          checkError(
            exception = intercept[AnalysisException](query(table, "MATERIALIZED", hint)),
            condition = "EXTERNAL_LOOKUP_JOIN_UNSUPPORTED.MATERIALIZED_CTE",
            parameters = Map("cteName" -> "`lk`"))
          Seq("NOT MATERIALIZED", "").foreach { option =>
            val run = runStream(query(table, option, hint))
            assert(run.plan.exists(_.isInstanceOf[PreparedExternalLookupJoin]), option)
            QueryTest.sameRows(expected, run.rows).foreach(fail(_))
          }
        }
        // A MATERIALIZED CTE that no lookup side reads is still evaluated once: it is not
        // inlined, so the repartition that replaces its references is in the plan.
        val shared = runStream(sql(
          s"""
            |WITH d AS MATERIALIZED (TABLE $EXTERNAL_LOOKUP_CATALOG_NAME.ns.dual)
            |SELECT s.employee, lk.department, d.department
            |FROM badge_swipes s JOIN $EXTERNAL_LOOKUP_CATALOG_NAME.ns.lookup lk
            |  ON s.employee = lk.id
            |JOIN d ON s.employee = d.id
            |""".stripMargin))
        assert(shared.plan.exists(_.isInstanceOf[PreparedExternalLookupJoin]))
        assert(shared.plan.exists {
          case repartition: RepartitionByExpression => repartition.partitionExpressions.isEmpty
          case _ => false
        })
        QueryTest.sameRows(
          Seq(Row(1, "engineering", "engineering"), Row(1, "engineering", "research"),
            Row(1, "research", "engineering"), Row(1, "research", "research")),
          shared.rows).foreach(fail(_))
      }
    }
  }

  test("a WITH-clause lookup side that two lookup joins read") {
    // The second reference to the CTE gets new attributes, so it is inlined under a projection to
    // them, which a lookup side may have.
    withSQLConf(
        s"spark.sql.catalog.$EXTERNAL_LOOKUP_CATALOG_NAME" ->
          classOf[ExternalLookupTestCatalog].getName) {
      withTempView("badge_swipes") {
        streamOf(Seq((1, 2), (3, 1)).toDF("employee", "escort"))
          .createOrReplaceTempView("badge_swipes")
        val run = runStream(sql(
          s"""
            |WITH lk AS (TABLE $EXTERNAL_LOOKUP_CATALOG_NAME.ns.lookup)
            |SELECT s.employee, e.department, x.department
            |FROM badge_swipes s JOIN lk e ON s.employee = e.id JOIN lk x ON s.escort = x.id
            |""".stripMargin))
        assert(run.plan.collect { case join: PreparedExternalLookupJoin => join }.size === 2)
        QueryTest.sameRows(
          Seq(Row(1, "engineering", "sales"), Row(1, "research", "sales")),
          run.rows).foreach(fail(_))
      }
    }
  }

  test("cached data does not replace the lookup side of an external lookup join") {
    withSQLConf(
        s"spark.sql.catalog.$EXTERNAL_LOOKUP_CATALOG_NAME" ->
          classOf[ExternalLookupTestCatalog].getName) {
      val dualTable = s"$EXTERNAL_LOOKUP_CATALOG_NAME.ns.dual"
      val swipes = streamOf(Seq((1, "lobby"), (3, "garage")).toDF("employee", "door"))
      val cachedRows = Seq(Row(1, "engineering"), Row(1, "research"))
      val currentRows = Seq(Row(1, "legal"))
      def join(directory: DataFrame): DataFrame =
        swipes.join(directory, swipes("employee") === directory("id"))
          .select(swipes("employee"), directory("department"))
      def checkRun(df: DataFrame, lookupJoin: Boolean, expected: Seq[Row]): Unit = {
        val run = runStream(df)
        assert(run.plan.exists(_.isInstanceOf[PreparedExternalLookupJoin]) === lookupJoin)
        QueryTest.sameRows(expected, run.rows).foreach(fail(_))
      }
      // Changes the directory after the cache has read it.
      def moveEmployeeToLegal(): Unit = SimpleExternalLookupDataSource.departmentsById.set(
        Map(1 -> Seq("legal"), 2 -> Seq("sales")))

      // A table that is cached, then hinted.
      val dual = spark.table(dualTable).cache()
      try {
        assert(dual.count() === 3)
        moveEmployeeToLegal()
        checkRun(join(spark.table(dualTable)), lookupJoin = false, cachedRows)
        checkRun(join(spark.table(dualTable).hint("external_lookup")), lookupJoin = true,
          currentRows)
        withTempView("badge_swipes") {
          swipes.createOrReplaceTempView("badge_swipes")
          checkRun(
            sql(
              s"""
                |WITH lk AS (TABLE $dualTable)
                |SELECT /*+ EXTERNAL_LOOKUP(lk) */ s.employee, lk.department
                |FROM badge_swipes s JOIN lk ON s.employee = lk.id
                |""".stripMargin),
            lookupJoin = true,
            currentRows)
        }
      } finally {
        dual.unpersist(blocking = true)
      }

      // A hinted DataFrame that is cached.
      SimpleExternalLookupDataSource.resetCounters()
      val hinted = spark.table(dualTable).hint("external_lookup").cache()
      try {
        assert(hinted.count() === 3)
        moveEmployeeToLegal()
        checkRun(join(hinted), lookupJoin = true, currentRows)
      } finally {
        hinted.unpersist(blocking = true)
      }

      // A lookup-only table cannot be cached, as caching reads it with a batch scan.
      val lookupOnly = spark.table(s"$EXTERNAL_LOOKUP_CATALOG_NAME.ns.lookup")
      checkError(
        exception = intercept[AnalysisException] { lookupOnly.cache() },
        condition = "UNSUPPORTED_FEATURE.TABLE_OPERATION",
        parameters = Map(
          "tableName" -> "`SimpleExternalLookupTable`",
          "operation" -> "batch scan"))
      assert(spark.sharedState.cacheManager.lookupCachedData(lookupOnly).isEmpty)
      checkRun(join(lookupOnly), lookupJoin = true, currentRows)
    }
  }

  test("an EXTERNAL_LOOKUP hint that attaches to no join shares cached data with its plan") {
    withSQLConf(
        s"spark.sql.catalog.$EXTERNAL_LOOKUP_CATALOG_NAME" ->
          classOf[ExternalLookupTestCatalog].getName) {
      val dualTable = s"$EXTERNAL_LOOKUP_CATALOG_NAME.ns.dual"
      val cacheManager = spark.sharedState.cacheManager
      spark.catalog.clearCache()
      val hinted = spark.table(dualTable).hint("external_lookup").cache()
      try {
        assert(hinted.count() === 3)
        // Changes the directory after the cache has read it.
        SimpleExternalLookupDataSource.departmentsById.set(
          Map(1 -> Seq("legal"), 2 -> Seq("sales")))
        // As for a strategy hint, the plan without the hint reads the cached data.
        val plain = spark.table(dualTable)
        checkAnswer(plain, Seq(Row(1, "engineering"), Row(1, "research"), Row(2, "sales")))
        // A hinted lookup side still reads the connector's current data.
        val swipes = streamOf(Seq((1, "lobby"), (3, "garage")).toDF("employee", "door"))
        val directory = spark.table(dualTable).hint("external_lookup")
        val run = runStream(swipes.join(directory, swipes("employee") === directory("id"))
          .select(swipes("employee"), directory("department")))
        assert(run.plan.exists(_.isInstanceOf[PreparedExternalLookupJoin]))
        QueryTest.sameRows(Seq(Row(1, "legal")), run.rows).foreach(fail(_))
        // Caching the plan without the hint adds no entry, and unpersisting it releases the one
        // that the hinted DataFrame made.
        plain.cache()
        plain.unpersist(blocking = true)
        assert(cacheManager.lookupCachedData(hinted).isEmpty)
        assert(cacheManager.isEmpty)
        checkAnswer(hinted, Seq(Row(1, "legal"), Row(2, "sales")))
      } finally {
        hinted.unpersist(blocking = true)
      }
    }
  }

  test("a common expression in the join condition is evaluated in the lookup join") {
    val lookup = spark.read.format(classOf[SimpleExternalLookupDataSource].getName).load()
    val left = streamOf(Seq((1, "b"), (2, "c")).toDF("id", "code"))
    val sameId = left("id") === lookup("id")
    // BETWEEN evaluates its input once, and upper() is not cheap enough to repeat, so it would
    // otherwise be computed in a projection below the join: over the lookup side, or the stream.
    Seq(
      expr("upper(department) BETWEEN 'F' AND 'Z'") ->
        Seq(Row(1, "b", 1, "research"), Row(2, "c", 2, "sales")),
      expr("upper(code) BETWEEN 'A' AND 'B'") ->
        Seq(Row(1, "b", 1, "engineering"), Row(1, "b", 1, "research"))).foreach {
      case (between, expected) =>
        val run = runStream(left.join(lookup, sameId && between))
        assert(run.plan.exists(_.isInstanceOf[PreparedExternalLookupJoin]))
        QueryTest.sameRows(expected, run.rows).foreach(fail(_))
    }
  }

  test("a subquery in the join condition of a lookup join is rejected") {
    val lookup = spark.read.format(classOf[SimpleExternalLookupDataSource].getName).load()
    val left = streamOf(Seq((1, 1)).toDF("id", "x"))
    val values = Seq(1).toDF("v")
    val sameId = left("id") === lookup("id")
    // The optimizer rewrites such subqueries only in a Join, so they fail when the DataFrame is
    // built, whether they read the stream side, the lookup side or neither. The scalar subquery
    // references neither side, so it could be offered as a parameter.
    Seq(
      sameId && left("x").isin(values),
      sameId && values.where(col("v") === 1).exists(),
      lookup("id") === values.scalar(),
      sameId && lookup("id").isin(values)).foreach { condition =>
      checkError(
        exception = intercept[AnalysisException](left.join(lookup, condition)),
        condition = "EXTERNAL_LOOKUP_JOIN_UNSUPPORTED.SUBQUERY_IN_JOIN_CONDITION",
        parameters = Map("relation" -> "`SimpleExternalLookupTable`"))
    }
  }

  test("a SQL subquery over lookup columns in the join condition of a lookup join is rejected") {
    withSQLConf(
        s"spark.sql.catalog.$EXTERNAL_LOOKUP_CATALOG_NAME" ->
          classOf[ExternalLookupTestCatalog].getName) {
      withTempView("events", "codes") {
        streamOf(Seq(1).toDF("id")).createOrReplaceTempView("events")
        Seq(1).toDF("code").createOrReplaceTempView("codes")
        // A correlated subquery cannot be written in a Dataset join condition, but can in SQL.
        Seq(
          "EXISTS (SELECT 1 FROM codes WHERE codes.code = l.id)",
          "l.id IN (SELECT code FROM codes)").foreach { subquery =>
          checkError(
            exception = intercept[AnalysisException] {
              sql(s"SELECT * FROM events e JOIN $EXTERNAL_LOOKUP_CATALOG_NAME.ns.lookup l " +
                s"ON e.id = l.id AND $subquery")
            },
            condition = "EXTERNAL_LOOKUP_JOIN_UNSUPPORTED.SUBQUERY_IN_JOIN_CONDITION",
            parameters = Map("relation" -> "`SimpleExternalLookupTable`"))
        }
      }
    }
  }

  test("Dataset join APIs that analyze a join plan lookup joins or fail with a named error") {
    val lookup = spark.read.format(classOf[SimpleExternalLookupDataSource].getName).load()
    val left = streamOf(Seq((1, 2)).toDF("sender", "receiver"))

    val typed = left.joinWith(lookup, left("sender") === lookup("id"))
    val typedRun = runStream(typed.toDF())
    assert(typedRun.plan.exists(_.isInstanceOf[PreparedExternalLookupJoin]))
    QueryTest.sameRows(
      Seq(Row(Row(1, 2), Row(1, "engineering")), Row(Row(1, 2), Row(1, "research"))),
      typedRun.rows).foreach(fail(_))

    // A lookup DataFrame reused in a second join is ambiguous, as any other DataFrame is.
    def reusedJoinError(right: DataFrame): AnalysisException = intercept[AnalysisException] {
      val enriched = left.join(right, left("sender") === right("id"))
      enriched.join(right, enriched("receiver") === right("id"))
    }
    val ordinary = Seq((1, "engineering")).toDF("id", "department")
    Seq("true", "false").foreach { autoResolve =>
      withSQLConf(SQLConf.DATAFRAME_SELF_JOIN_AUTO_RESOLVE_AMBIGUITY.key -> autoResolve) {
        val error = reusedJoinError(lookup)
        assert(error.getCondition === reusedJoinError(ordinary).getCondition)
        assert(error.getMessage.contains("ambiguous"), error)
      }
    }
    // Loaded twice, the table enriches the stream with two lookup joins.
    val receivers = spark.read.format(classOf[SimpleExternalLookupDataSource].getName).load()
    val enriched = left.join(lookup, left("sender") === lookup("id"))
    val twice = enriched.join(receivers, enriched("receiver") === receivers("id"))
    val twiceRun = runStream(twice)
    assert(twiceRun.plan.collect { case join: PreparedExternalLookupJoin => join }.size === 2)
    QueryTest.sameRows(
      Seq(Row(1, 2, 1, "engineering", 2, "sales"), Row(1, 2, 1, "research", 2, "sales")),
      twiceRun.rows).foreach(fail(_))

    // An AS OF join reads the lookup-only table outside a lookup join.
    checkError(
      exception = streamFailure[AnalysisException](left.asInstanceOf[Dataset[Row]].joinAsOf(
        lookup, left("sender"), lookup("id"), left("sender") === lookup("id"), "inner",
        tolerance = null, allowExactMatches = true, direction = "backward")),
      condition = "UNSUPPORTED_FEATURE.TABLE_OPERATION",
      parameters = Map(
        "tableName" -> "`SimpleExternalLookupTable`",
        "operation" -> "batch scan"))

    // A condition that is not boolean fails as for any other join.
    val notBoolean = intercept[AnalysisException] {
      left.join(lookup, left("sender") + lookup("id"))
    }
    assert(notBoolean.getCondition === "JOIN_CONDITION_IS_NOT_BOOLEAN_TYPE")
  }

  test("a catalog-resolved external lookup-only table cannot be read outside a lookup join") {
    withSQLConf(
        s"spark.sql.catalog.$EXTERNAL_LOOKUP_CATALOG_NAME" ->
          classOf[ExternalLookupTestCatalog].getName) {
      val table = s"$EXTERNAL_LOOKUP_CATALOG_NAME.ns.lookup"
      val left = Seq(1).toDF("id")
      Seq[() => DataFrame](
        () => spark.table(table),
        () => sql(s"SELECT * FROM $table WHERE id = 1")).foreach {
        df =>
          checkError(
            exception = intercept[AnalysisException] { df().queryExecution.optimizedPlan },
            condition = "UNSUPPORTED_FEATURE.TABLE_OPERATION",
            parameters = Map(
              "tableName" -> "`SimpleExternalLookupTable`",
              "operation" -> "batch scan"))
      }
    }
  }

  test("a command that reads a lookup-only table outside a lookup join fails before it writes") {
    withSQLConf(
        "spark.sql.catalog.testcat" -> classOf[InMemoryTableCatalog].getName,
        s"spark.sql.catalog.$EXTERNAL_LOOKUP_CATALOG_NAME" ->
          classOf[ExternalLookupTestCatalog].getName) {
      val lookup = spark.read.format(classOf[SimpleExternalLookupDataSource].getName).load()
      val catalogLookup = s"$EXTERNAL_LOOKUP_CATALOG_NAME.ns.lookup"
      def checkBatchScanError(write: () => Any): Unit = checkError(
        exception = intercept[AnalysisException](write()),
        condition = "UNSUPPORTED_FEATURE.TABLE_OPERATION",
        parameters = Map(
          "tableName" -> "`SimpleExternalLookupTable`",
          "operation" -> "batch scan"))

      // Each write would replace the archived assignment that its target holds.
      withTable("testcat.ns.target", "v1_target") {
        sql("CREATE TABLE testcat.ns.target (id INT, department STRING)")
        sql("INSERT INTO testcat.ns.target VALUES (7, 'archive')")
        sql("CREATE TABLE v1_target (id INT, department STRING) USING parquet")
        sql("INSERT INTO v1_target VALUES (7, 'archive')")
        Seq[(String, () => Any)](
          "testcat.ns.target" ->
            (() => lookup.write.mode("overwrite").saveAsTable("testcat.ns.target")),
          "testcat.ns.target" -> (() => lookup.writeTo("testcat.ns.target").replace()),
          "testcat.ns.target" -> (() =>
            sql(s"REPLACE TABLE testcat.ns.target USING foo AS SELECT * FROM $catalogLookup")),
          "testcat.ns.target" ->
            (() => sql(s"INSERT OVERWRITE testcat.ns.target SELECT * FROM $catalogLookup")),
          "v1_target" -> (() => lookup.write.mode("overwrite").saveAsTable("v1_target")),
          "v1_target" -> (() => lookup.write.mode("overwrite").insertInto("v1_target")),
          // A subquery of the query reads it.
          "v1_target" -> (() => sql(
            "INSERT OVERWRITE v1_target SELECT id, 'none' FROM VALUES (8) AS t(id) " +
              s"WHERE id NOT IN (SELECT id FROM $catalogLookup)"))).foreach {
          case (target, write) =>
            checkBatchScanError(write)
            checkAnswer(spark.table(target), Row(7, "archive"))
        }
      }

      // V1 commands that hold their query outside their children.
      RecordingV1WriteSource.writes.clear()
      withTempView("recording_sink") {
        sql("CREATE TEMPORARY VIEW recording_sink (id INT, department STRING) " +
          s"USING ${classOf[RecordingV1WriteSource].getName}")
        checkBatchScanError(() =>
          lookup.write.mode("overwrite").format(classOf[RecordingV1WriteSource].getName).save())
        checkBatchScanError(() =>
          sql(s"INSERT OVERWRITE TABLE recording_sink SELECT * FROM $catalogLookup"))
        // The query reads it below a union under an ordinary join, which is no lookup join at any
        // layer, so the check reaches the read only by entering the sides of the join.
        val departments = Seq((1, "x")).toDF("id", "label")
        checkBatchScanError(() => departments.join(lookup.union(lookup), "id")
          .write.mode("overwrite").format(classOf[RecordingV1WriteSource].getName).save())
      }
      assert(RecordingV1WriteSource.writes.isEmpty)
    }
  }

  test("a command reads a lookup-only table only through a lookup join, and a view not at all") {
    val lookup = spark.read.format(classOf[SimpleExternalLookupDataSource].getName).load()
    val left = streamOf(Seq(1).toDF("id"))
    def save(query: DataFrame): LogicalPlan = SaveIntoDataSourceCommand(
      query.queryExecution.analyzed, new RecordingV1WriteSource, Map.empty, SaveMode.Overwrite)
    TableCapabilityCheck(save(left.join(lookup, left("id") === lookup("id"))))
    // A join of a command, which is analyzed in full, cannot gain a condition later.
    checkError(
      exception = intercept[AnalysisException](TableCapabilityCheck(save(left.join(lookup)))),
      condition = "EXTERNAL_LOOKUP_JOIN_UNSUPPORTED.MISSING_JOIN_CONDITION",
      parameters = Map.empty)

    withTempView("lookup_view", "enriched_view") {
      lookup.createOrReplaceTempView("lookup_view")
      left.join(lookup, left("id") === lookup("id")).createOrReplaceTempView("enriched_view")
      checkStream(
        spark.table("enriched_view"), Seq(Row(1, 1, "engineering"), Row(1, 1, "research")))
      checkError(
        exception = intercept[AnalysisException] {
          spark.table("lookup_view").queryExecution.optimizedPlan
        },
        condition = "UNSUPPORTED_FEATURE.TABLE_OPERATION",
        parameters = Map(
          "tableName" -> "`SimpleExternalLookupTable`",
          "operation" -> "batch scan"))
    }
  }

  test("foreachBatch writes the micro-batches of a lookup join with batch commands") {
    withSQLConf("spark.sql.catalog.testcat" -> classOf[InMemoryTableCatalog].getName) {
      withTempPath { path =>
        withTable("enriched_events") {
          val lookup = spark.read.format(classOf[SimpleExternalLookupDataSource].getName).load()
          val events = streamOf(Seq(1, 2, 3).toDF("id"))
          val enriched = events.join(lookup, events("id") === lookup("id"))
            .select(events("id"), lookup("department"))
          val v2Rows = new AtomicReference(Seq.empty[Row])
          // Each micro-batch reaches the function as a plan that is already planned, so the batch
          // commands over it read no lookup-only table.
          val query = enriched.writeStream.foreachBatch { (batch: DataFrame, _: Long) =>
            batch.write.format("parquet").mode("append").save(path.getCanonicalPath)
            batch.write.mode("append").saveAsTable("enriched_events")
            // The function runs in the query's session, which has its own instance of the test
            // catalog, so the V2 table is created and read there.
            val session = batch.sparkSession
            session.sql(
              "CREATE TABLE IF NOT EXISTS testcat.ns.enriched_events (id INT, department STRING)")
            batch.writeTo("testcat.ns.enriched_events").append()
            v2Rows.set(session.table("testcat.ns.enriched_events").collect().toSeq)
          }.start()
          try query.processAllAvailable() finally query.stop()
          val expected = Seq(Row(1, "engineering"), Row(1, "research"), Row(2, "sales"))
          checkAnswer(spark.read.parquet(path.getCanonicalPath), expected)
          checkAnswer(spark.table("enriched_events"), expected)
          QueryTest.sameRows(expected, v2Rows.get()).foreach(fail(_))
          // Each write computes the micro-batch again, as batchDF is not persisted.
          assert(SimpleExternalLookupDataSource.requestCount.get() === 3)
        }
      }
    }
  }

  test("view definitions over a lookup-only table, in either view catalog, are not reads") {
    withSQLConf(
        s"spark.sql.catalog.$EXTERNAL_LOOKUP_CATALOG_NAME" ->
          classOf[ExternalLookupTestCatalog].getName,
        "spark.sql.catalog.view_only" -> classOf[TestingViewOnlyCatalog].getName) {
      val lookupTable = s"$EXTERNAL_LOOKUP_CATALOG_NAME.ns.lookup"
      // ALTER VIEW of a session view, and CREATE VIEW and ALTER VIEW in a V2 view catalog.
      withTempView("departments") {
        sql("CREATE TEMPORARY VIEW departments AS SELECT 1 AS id, 'none' AS department")
        sql(s"ALTER VIEW departments AS SELECT * FROM $lookupTable")
        assert(spark.table("departments").queryExecution.analyzed.exists {
          case relation: DataSourceV2Relation => relation.table eq SimpleExternalLookupTable
          case _ => false
        })
      }
      val catalog = spark.sessionState.catalogManager.catalog("view_only")
        .asInstanceOf[TestingViewOnlyCatalog]
      sql(s"CREATE VIEW view_only.default.departments AS SELECT * FROM $lookupTable")
      sql(s"ALTER VIEW view_only.default.pure_v AS SELECT * FROM $lookupTable")
      Seq("departments", "pure_v").foreach { name =>
        assert(catalog.loadView(Identifier.of(Array("default"), name)).queryText() ===
          s"SELECT * FROM $lookupTable")
      }
    }
  }

  test("a stream-only Python UDF in a lookup join's residual or lookup parameter") {
    assume(shouldTestPythonUDFs)
    // The UDF returns its input, so each join has the same rows as the one with the column itself.
    val pythonUDF = TestPythonUDF("stream_key")
    val lookup = spark.read.format(classOf[SimpleExternalLookupDataSource].getName).load()
    val left = streamOf(Seq((1, 1), (2, -1), (3, 1)).toDF("id", "shift"))
    Seq[(Column => Column, String)](
      (key => left("id") === lookup("id") && key > 0, "residual"),
      (key => lookup("id") === key + left("id") - left("shift"), "parameter")).foreach {
      case (condition, place) =>
        Seq("inner", "left_outer").foreach { joinType =>
          val withUDF = runStream(left.join(lookup, condition(pythonUDF(left("shift"))), joinType))
          assert(withUDF.plan.exists(_.isInstanceOf[PreparedExternalLookupJoin]), place)
          val builtIn = runStream(left.join(lookup, condition(left("shift")), joinType))
          QueryTest.sameRows(builtIn.rows, withUDF.rows).foreach(error => fail(s"$place: $error"))
        }
    }
  }

  test("a table that also supports batch read is read as a batch scan outside a lookup join") {
    withSQLConf(
        s"spark.sql.catalog.$EXTERNAL_LOOKUP_CATALOG_NAME" ->
          classOf[ExternalLookupTestCatalog].getName) {
      Seq[() => DataFrame](
        () => spark.read.format(classOf[DualCapabilityExternalLookupDataSource].getName).load(),
        () => spark.table(s"$EXTERNAL_LOOKUP_CATALOG_NAME.ns.dual")).foreach { dual =>
        val df = dual()
        assert(df.queryExecution.analyzed.exists(_.isInstanceOf[ExternalLookupRelation]) === false)
        checkAnswer(df, Seq(Row(1, "engineering"), Row(1, "research"), Row(2, "sales")))
      }
      assert(SimpleExternalLookupDataSource.readerCount.get() === 0)
    }
  }

  test("a provider with a V1 read path cannot serve a lookup-only table") {
    Seq(
      classOf[RelationProviderExternalLookupDataSource],
      classOf[SchemaRelationProviderExternalLookupDataSource],
      classOf[FileFormatExternalLookupDataSource],
      classOf[FileDataSourceV2ExternalLookupDataSource]).foreach { provider =>
      val parameters = Map("table" -> "`SimpleExternalLookupTable`", "provider" -> provider.getName)
      checkError(
        exception = intercept[AnalysisException](spark.read.format(provider.getName).load()),
        condition = "DATA_SOURCE_INVALID_EXTERNAL_LOOKUP.LOOKUP_ONLY_TABLE_WITH_V1_READ",
        parameters = parameters)
      withTempView("hybrid_view") {
        checkError(
          exception = intercept[AnalysisException] {
            sql(s"CREATE TEMPORARY VIEW hybrid_view USING ${provider.getName}")
          },
          condition = "DATA_SOURCE_INVALID_EXTERNAL_LOOKUP.LOOKUP_ONLY_TABLE_WITH_V1_READ",
          parameters = parameters)
      }
    }

    // A provider without a V1 read path loads the table through both.
    val provider = classOf[SimpleExternalLookupDataSource].getName
    def readsLookupOnlyTable(df: DataFrame): Boolean =
      df.queryExecution.analyzed.exists(_.isInstanceOf[ExternalLookupRelation])
    assert(readsLookupOnlyTable(spark.read.format(provider).load()))
    withTempView("lookup_view") {
      sql(s"CREATE TEMPORARY VIEW lookup_view USING $provider")
      assert(readsLookupOnlyTable(spark.table("lookup_view")))
    }
  }

  test("external lookup join applies lookup-side projection and filter before joining") {
    val rawLookup = spark.read.format(classOf[SimpleExternalLookupDataSource].getName).load()
    val lookup = rawLookup
      .filter($"department" =!= "research")
      .select($"id", upper($"department").as("department"))
    val left = streamOf(Seq(1, 2).toDF("id"))

    checkStream(
      left.join(lookup, left("id") === lookup("id"), "inner"),
      Seq(Row(1, 1, "ENGINEERING"), Row(2, 2, "SALES")))
  }

  test("external lookup rebinds projections after required-column pruning") {
    val lookup = spark.read.format(classOf[SimpleExternalLookupDataSource].getName).load()
    val left = streamOf(Seq(1, 2).toDF("id"))

    val result = left.join(
      lookup, left("id") === lookup("id"), "inner")
      .select(upper(lookup("department")))

    checkStream(result, Seq(Row("ENGINEERING"), Row("RESEARCH"), Row("SALES")))
  }

  test("external lookup join resolves a projected key alias") {
    val rawLookup = spark.read.format(classOf[SimpleExternalLookupDataSource].getName).load()
    val lookup = rawLookup.select($"id".as("lookup_id"), $"department")
    val left = streamOf(Seq(1, 3).toDF("id"))

    checkStream(
      left.join(
        lookup, left("id") === lookup("lookup_id"), "left_outer"),
      Seq(Row(1, 1, "engineering"), Row(1, 1, "research"), Row(3, null, null)))
  }

  test("external lookup join composes nested aliases and filters") {
    val rawLookup = spark.read.format(classOf[SimpleExternalLookupDataSource].getName).load()
    val once = rawLookup.select($"id".as("lookup_id"), $"department")
    val lookup = once.filter($"lookup_id" > 0).select($"lookup_id", $"department")
    val left = streamOf(Seq(1, 3).toDF("id"))

    checkStream(
      left.join(
        lookup, left("id") === lookup("lookup_id"), "left_outer"),
      Seq(Row(1, 1, "engineering"), Row(1, 1, "research"), Row(3, null, null)))
  }

  test("lookup-side stages that cannot collapse resolve the key and prune columns across them") {
    val directory = spark.read.format(classOf[SimpleExternalLookupDataSource].getName).load()
    // The second projection reads `d` twice, so the first one, which computes it, does not
    // collapse into it.
    val badges = directory
      .select($"id".as("k"), upper($"department").as("d"))
      .select($"k", concat($"d", lit("-"), $"d").as("badge"))
    val swipes = streamOf(Seq((1, "door-a"), (2, "door-b")).toDF("employee", "door"))
    val joined = swipes.join(badges, swipes("employee") === badges("k"))
    val stages = runStream(joined).lookupJoin.lookupTransforms
    assert(stages.count(_.isInstanceOf[ExternalLookupProject]) === 2, stages)
    checkStream(
      joined.select(swipes("door"), badges("badge")),
      Seq(
        Row("door-a", "ENGINEERING-ENGINEERING"),
        Row("door-a", "RESEARCH-RESEARCH"),
        Row("door-b", "SALES-SALES")))
    assert(SimpleExternalLookupDataSource.requiredColumns.get() === Seq("id", "department"))
    // Nothing above the join reads `badge`, so neither stage computes it, nor reads the department.
    checkStream(
      joined.select(swipes("door"), badges("k")),
      Seq(Row("door-a", 1), Row("door-a", 1), Row("door-b", 2)))
    assert(SimpleExternalLookupDataSource.requiredColumns.get() === Seq("id"))
  }

  test("an external lookup hint without relation names over a join fails") {
    val lookup = spark.read.format(classOf[SimpleExternalLookupDataSource].getName).load()
    val dual = spark.read.format(classOf[DualCapabilityExternalLookupDataSource].getName).load()
    val left = streamOf(Seq(1).toDF("id"))

    // As with a join strategy hint, the hint applies to the plan it annotates, here the join
    // itself rather than its lookup side, so no join could read it.
    withTempView("external_lookup_left", "external_lookup_right", "external_lookup_dual") {
      left.createOrReplaceTempView("external_lookup_left")
      lookup.createOrReplaceTempView("external_lookup_right")
      dual.createOrReplaceTempView("external_lookup_dual")
      Seq("external_lookup_right", "external_lookup_dual").foreach { right =>
        checkError(
          exception = intercept[AnalysisException] {
            sql(s"""
              |SELECT /*+ EXTERNAL_LOOKUP */ l.id, r.department
              |FROM external_lookup_left l JOIN $right r ON l.id = r.id
              |""".stripMargin)
          },
          condition = "INVALID_EXTERNAL_LOOKUP_HINT.NOT_ON_LOOKUP_SIDE",
          parameters = Map.empty)
        checkError(
          exception = intercept[AnalysisException] {
            sql(s"""
              |SELECT /*+ EXTERNAL_LOOKUP */ id FROM (
              |  SELECT l.id FROM external_lookup_left l JOIN $right r ON l.id = r.id
              |  UNION ALL
              |  SELECT l.id FROM external_lookup_left l JOIN $right r ON l.id = r.id)
              |""".stripMargin)
          },
          condition = "INVALID_EXTERNAL_LOOKUP_HINT.NOT_ON_LOOKUP_SIDE",
          parameters = Map.empty)
      }
    }
    checkError(
      exception = intercept[AnalysisException] {
        left.join(dual, left("id") === dual("id")).hint("external_lookup").queryExecution.analyzed
      },
      condition = "INVALID_EXTERNAL_LOOKUP_HINT.NOT_ON_LOOKUP_SIDE",
      parameters = Map.empty)
    // A set operation passes the hint to no join below it either.
    val returns = streamOf(Seq(2).toDF("id"))
    checkError(
      exception = intercept[AnalysisException] {
        left.join(dual, left("id") === dual("id"))
          .union(returns.join(dual, returns("id") === dual("id")))
          .hint("external_lookup").queryExecution.analyzed
      },
      condition = "INVALID_EXTERNAL_LOOKUP_HINT.NOT_ON_LOOKUP_SIDE",
      parameters = Map.empty)
  }

  test("the hint on or above an ASOF, NEAREST BY or lateral join fails while it is analyzed") {
    val directory =
      spark.read.format(classOf[DualCapabilityExternalLookupDataSource].getName).load()
    val scans = streamOf(Seq(1, 3).toDF("id"))
    val batchScans = Seq(1, 3).toDF("id")
    def analysisError(query: => DataFrame): AnalysisException =
      intercept[AnalysisException](query.queryExecution.analyzed)
    def joinTypeError(joinType: String): Map[String, String] = Map(
      "joinType" -> joinType,
      "relation" -> "`DualCapabilityExternalLookupTable`",
      "supportedJoinTypes" -> SUPPORTED_JOIN_TYPES)
    withTempView("hinted_scans", "hinted_batch_scans", "hinted_directory", "hinted_values") {
      withSQLConf(SQLConf.SQL_ASOF_JOIN_ENABLED.key -> "true") {
        scans.createOrReplaceTempView("hinted_scans")
        batchScans.createOrReplaceTempView("hinted_batch_scans")
        directory.createOrReplaceTempView("hinted_directory")
        Seq((1, "lobby")).toDF("id", "department").createOrReplaceTempView("hinted_values")
        def asOf(left: String)(right: String): String =
          s"$left s ASOF JOIN $right d MATCH_CONDITION(s.id >= d.id) ON s.id = d.id"
        def nearestBy(left: String)(right: String): String =
          s"$left s JOIN $right d APPROX NEAREST 1 BY DISTANCE abs(s.id - d.id)"
        // Streaming and batch ASOF joins, and a NEAREST BY join, which only a batch query allows.
        val joins = Seq[(String, String => String)](
          "ASOF" -> asOf("hinted_scans"), "ASOF" -> asOf("hinted_batch_scans"),
          "NEAREST BY" -> nearestBy("hinted_batch_scans"))
        joins.foreach { case (joinType, from) =>
          // On the right input, the join type is not supported, or the input is no lookup side when
          // it reads no DSv2 table.
          checkError(
            exception = analysisError(
              sql(s"SELECT /*+ EXTERNAL_LOOKUP(d) */ s.id FROM ${from("hinted_directory")}")),
            condition = "EXTERNAL_LOOKUP_JOIN_UNSUPPORTED.JOIN_TYPE",
            parameters = joinTypeError(joinType))
          checkError(
            exception = analysisError(
              sql(s"SELECT /*+ EXTERNAL_LOOKUP(d) */ s.id FROM ${from("hinted_values")}")),
            condition = "EXTERNAL_LOOKUP_JOIN_UNSUPPORTED.LOOKUP_PLAN",
            parameters = Map.empty)
        }
        // On the left input, and above the join, the hint is on the lookup side of no join.
        val lateral = "hinted_batch_scans s JOIN LATERAL " +
          "(SELECT department FROM hinted_directory p WHERE p.id = s.id) d"
        (joins.map(_._2("hinted_directory")) :+ lateral).foreach { from =>
          Seq("EXTERNAL_LOOKUP(s)", "EXTERNAL_LOOKUP").foreach { hint =>
            checkError(
              exception = analysisError(sql(s"SELECT /*+ $hint */ s.id FROM $from")),
              condition = "INVALID_EXTERNAL_LOOKUP_HINT.NOT_ON_LOOKUP_SIDE",
              parameters = Map.empty)
          }
        }
      }
    }

    // A streaming joinAsOf with the hint on its right input fails with the join type, in
    // parameterless and named form. A batch joinAsOf first analyzes the join of its two inputs,
    // whose left side is static.
    def joinAsOf(left: DataFrame, right: DataFrame): DataFrame =
      left.asInstanceOf[Dataset[Row]].joinAsOf(
        right, left("id"), right("id"), left("id") === right("id"), "inner",
        tolerance = null, allowExactMatches = true, direction = "backward")
    Seq(directory.hint("external_lookup"), directory.as("d").hint("external_lookup", "d"))
      .foreach { hinted =>
        checkError(
          exception = analysisError(joinAsOf(scans, hinted)),
          condition = "EXTERNAL_LOOKUP_JOIN_UNSUPPORTED.JOIN_TYPE",
          parameters = joinTypeError("ASOF"))
        checkError(
          exception = analysisError(joinAsOf(batchScans, hinted)),
          condition = "EXTERNAL_LOOKUP_JOIN_UNSUPPORTED.STATIC_LEFT_SIDE",
          parameters = Map("relation" -> "`DualCapabilityExternalLookupTable`"))
      }
    // On the left input of joinAsOf and lateralJoin, and above them.
    val departments =
      directory.where(directory("id") === col("id").outer()).select("department")
    Seq[() => DataFrame](
      () => joinAsOf(scans.hint("external_lookup"), directory),
      () => joinAsOf(scans, directory).hint("external_lookup"),
      () => batchScans.hint("external_lookup").lateralJoin(departments),
      () => batchScans.lateralJoin(departments).hint("external_lookup")).foreach { query =>
      checkError(
        exception = analysisError(query()),
        condition = "INVALID_EXTERNAL_LOOKUP_HINT.NOT_ON_LOOKUP_SIDE",
        parameters = Map.empty)
    }
  }

  test("ASOF, NEAREST BY and lateral joins without the hint read a dual-capability table") {
    // As without the hint, they read the table, which also serves batch scans, with a scan, and
    // return the rows that they return over a copy of its rows. A hint with no join below it, over
    // the table or inside a lateral subquery, is ignored.
    val directory =
      spark.read.format(classOf[DualCapabilityExternalLookupDataSource].getName).load()
    withTempView("plain_scans", "dual_directory", "copied_directory") {
      withSQLConf(SQLConf.SQL_ASOF_JOIN_ENABLED.key -> "true") {
        Seq(1, 2, 3).toDF("id").createOrReplaceTempView("plain_scans")
        directory.createOrReplaceTempView("dual_directory")
        spark.createDataFrame(directory.collect().toSeq.asJava, directory.schema)
          .createOrReplaceTempView("copied_directory")
        Seq(
          "SELECT s.id, d.department FROM plain_scans s " +
            "ASOF JOIN %s d MATCH_CONDITION(s.id >= d.id) ON s.id = d.id",
          "SELECT s.id, d.department FROM plain_scans s " +
            "JOIN %s d APPROX NEAREST 1 BY DISTANCE abs(s.id - d.id)",
          "SELECT s.id, d.department FROM plain_scans s " +
            "JOIN LATERAL (SELECT department FROM %s p WHERE p.id = s.id) d",
          "SELECT s.id, d.department FROM plain_scans s " +
            "JOIN LATERAL (SELECT /*+ EXTERNAL_LOOKUP */ department FROM %s p WHERE p.id = s.id) d",
          "SELECT /*+ EXTERNAL_LOOKUP */ id, department FROM %s").foreach { query =>
          val expected = sql(query.format("copied_directory")).collect().toSeq
          assert(expected.nonEmpty, query)
          checkAnswer(sql(query.format("dual_directory")), expected)
        }
      }
    }
  }

  test("external lookup pushes accepted filters and required columns to the source") {
    SimpleExternalLookupDataSource.resetCounters()
    val rawLookup = spark.read.format(classOf[SimpleExternalLookupDataSource].getName).load()
    val lookup = rawLookup.filter($"department" =!= "research").select($"id")
    val left = streamOf(Seq(1, 2).toDF("id"))

    checkStream(
      left.join(lookup, left("id") === lookup("id"), "inner"),
      Seq(Row(1, 1), Row(2, 2)))
    // Spark evaluates the accepted filter again on the returned rows, so it asks for the column
    // that the filter reads.
    assert(SimpleExternalLookupDataSource.requiredColumns.get() === Seq("id", "department"))
    assert(SimpleExternalLookupDataSource.pushedPredicateCount.get() > 0)
  }

  test("external lookup applies a pushed filter that the source accepts but does not apply") {
    SimpleExternalLookupDataSource.resetCounters()
    val lookup = spark.read
      .format(classOf[SimpleExternalLookupDataSource].getName)
      .option("ignoreAcceptedFilters", "true")
      .load()
      .filter($"department" =!= "research")
    val left = streamOf(Seq(1, 2).toDF("id"))

    checkStream(
      left.join(lookup, left("id") === lookup("id"), "inner"),
      Seq(Row(1, 1, "engineering"), Row(2, 2, "sales")))
    assert(SimpleExternalLookupDataSource.pushedPredicateCount.get() > 0)
  }

  test("external lookup canonicalizes filters through a relation alias") {
    SimpleExternalLookupDataSource.resetCounters()
    val rawLookup = spark.read.format(classOf[SimpleExternalLookupDataSource].getName).load()
      .alias("lookup_source")
    val lookup = rawLookup
      .filter($"lookup_source.DEPARTMENT" =!= "research")
      .select($"lookup_source.id")
    val left = streamOf(Seq(1, 2).toDF("id"))

    checkStream(
      left.join(lookup, left("id") === lookup("id"), "inner"),
      Seq(Row(1, 1), Row(2, 2)))
    assert(SimpleExternalLookupDataSource.pushedPredicateFields.get() === Seq("department"))
  }

  test("an empty lookup side gives semi and anti join results without a connector call") {
    val lookup = spark.read.format(classOf[SimpleExternalLookupDataSource].getName).load()
    val left = streamOf(Seq(1, 3).toDF("id"))
    val empty = lookup.where(lit(false))
    Seq(
      ("left_semi", Seq.empty[Row]),
      ("left_anti", Seq(Row(1), Row(3)))).foreach { case (joinType, expected) =>
      Seq(
        () => left.join(empty, left("id") === empty("id"), joinType),
        () => left.join(lookup, left("id") === lookup("id") && lit(false), joinType)).foreach {
        query =>
          SimpleExternalLookupDataSource.resetCounters()
          val joined = query()
          checkStream(joined, expected)
          assert(SimpleExternalLookupDataSource.lookupCandidates.get() === Seq.empty)
          assert(SimpleExternalLookupDataSource.readerCount.get() === 0)
          assert(SimpleExternalLookupDataSource.requestCount.get() === 0)
      }
    }
  }

  test("lookup-side stages evaluate withField and dropFields values of a struct column") {
    val lookup = readSchemaLookup("struct")
    val left = streamOf(Seq(1, 2).toDF("id"))
    // Whole-struct values, which the optimizer lowers only after the lookup-side stages are built.
    val upgraded = lookup
      .where($"info".dropFields("seats") === struct(lit("pro").as("plan")))
      .select($"id", $"info".withField("seats", $"info.seats" + 1).as("info"))
    val run = runStream(left.join(upgraded, left("id") === upgraded("id")))
    QueryTest.sameRows(Seq(Row(1, 1, Row("pro", 11))), run.rows).foreach(fail(_))
    assert(run.lookupJoin.lookupTransforms.nonEmpty)
  }

  test("external lookup pads CHAR columns as a batch read does") {
    val lookup = spark.read
      .format(classOf[ReadSchemaExternalLookupDataSource].getName)
      .option("kind", "char")
      .load()
    val left = streamOf(Seq(1).toDF("id"))
    // The connector returns "ab" for a CHAR(5) column; read-side padding makes it "ab   ".
    checkStream(left.join(lookup, left("id") === lookup("id")), Seq(Row(1, 1, "ab   ")))
  }

  test("external lookup join supports left semi and left anti joins") {
    SimpleExternalLookupDataSource.resetCounters()
    val lookup = spark.read.format(classOf[SimpleExternalLookupDataSource].getName).load()
    val left = streamOf(Seq(1, 2, 3).toDF("id"))

    checkStream(
      left.join(lookup, left("id") === lookup("id"), "left_semi"),
      Seq(Row(1), Row(2)))
    assert(SimpleExternalLookupDataSource.requiredColumns.get() === Seq("id"))
    checkStream(
      left.join(lookup, left("id") === lookup("id"), "left_anti"),
      Seq(Row(3)))
  }

  test("semi and anti joins stop evaluating a stream row's residual at its first match") {
    val lookup = spark.read.format(classOf[SimpleExternalLookupDataSource].getName).load()
    val left = streamOf(Seq(1, 3).toDF("id"))
    // Id 1 has two departments, returned in this order: engineering, then research. The residual
    // would fail on research, so it must not be evaluated once engineering has matched.
    val residual = when(lookup("department") === "research", raise_error(lit("evaluated")))
      .otherwise(left("id") > 0)

    Seq("left_semi" -> Seq(Row(1)), "left_anti" -> Seq(Row(3))).foreach {
      case (joinType, expected) =>
        SimpleExternalLookupDataSource.resetCounters()
        checkStream(left.join(lookup, left("id") === lookup("id") && residual, joinType), expected)
        // The cursor is still read to the end, because stream row 3 never matches.
        assert(SimpleExternalLookupDataSource.resultRowsRead.get() === 2)
    }
  }

  test("semi and anti joins close the cursor once every stream row of the batch has matched") {
    val lookup = spark.read.format(classOf[SimpleExternalLookupDataSource].getName).load()
    // Employee 1 has two departments, so the first returned row decides both badge swipes.
    val swipes = streamOf(Seq((1, "lobby"), (1, "garage")).toDF("employee", "door"))
    // A shift that covers employee ids from 1 matches every stored row, and the first decides it.
    val shifts = streamOf(Seq(("early", 1)).toDF("shift", "firstId"))
    Seq(
      ("left_semi", swipes, swipes("employee") === lookup("id"),
        Seq(Row(1, "lobby"), Row(1, "garage"))),
      ("left_anti", swipes, swipes("employee") === lookup("id"), Nil),
      ("left_semi", shifts, lookup("id") >= shifts("firstId"), Seq(Row("early", 1))),
      ("left_anti", shifts, lookup("id") >= shifts("firstId"), Nil)).foreach {
      case (joinType, left, condition, expected) =>
        SimpleExternalLookupDataSource.resetCounters()
        val cursorEnds = new AtomicInteger(0)
        SimpleExternalLookupDataSource.beforeCursorEnds.set { () =>
          cursorEnds.incrementAndGet()
          ()
        }
        checkStream(left.join(lookup, condition, joinType), expected)
        assert(SimpleExternalLookupDataSource.resultRowsRead.get() === 1, joinType)
        // The cursor is closed before its end.
        assert(cursorEnds.get() === 0, joinType)
        assert(SimpleExternalLookupDataSource.closeEvents.asScala.toSeq === Seq("cursor", "reader"),
          joinType)
    }
  }

  test("external lookup reads only the key when no lookup column is read above the join") {
    val lookup = spark.read.format(classOf[SimpleExternalLookupDataSource].getName).load()
    val left = streamOf(Seq(1, 3).toDF("id"))

    Seq("inner", "left_outer").foreach { joinType =>
      SimpleExternalLookupDataSource.resetCounters()
      val result = left.join(
        lookup, left("id") === lookup("id"), joinType)
        .select(left("id"))
      val expected = if (joinType == "inner") Seq(Row(1), Row(1)) else {
        Seq(Row(1), Row(1), Row(3))
      }
      checkStream(result, expected)
      // Spark reads the key to check it on every returned row.
      assert(SimpleExternalLookupDataSource.requiredColumns.get() === Seq("id"))
    }
  }

  test("external lookup evaluates semi and anti residuals on the pruned response layout") {
    val lookup = spark.read.format(classOf[SimpleExternalLookupDataSource].getName).load()
    val claims = streamOf(Seq((1, "engineering"), (1, "missing"), (3, "missing"))
      .toDF("id", "costCenter"))
    val condition = claims("id") === lookup("id") &&
      claims("costCenter") === lookup("department")

    SimpleExternalLookupDataSource.resetCounters()
    checkStream(
      claims.join(lookup, condition, "left_semi"),
      Seq(Row(1, "engineering")))
    assert(SimpleExternalLookupDataSource.requiredColumns.get() === Seq("id", "department"))

    SimpleExternalLookupDataSource.resetCounters()
    checkStream(
      claims.join(lookup, condition, "left_anti"),
      Seq(Row(1, "missing"), Row(3, "missing")))
    assert(SimpleExternalLookupDataSource.requiredColumns.get() === Seq("id", "department"))
  }

  test("external lookup join handles null, missing, duplicate, and partial batches") {
    val lookup = spark.read
      .format(classOf[SimpleExternalLookupDataSource].getName)
      .option("maxBatchSize", "3")
      .load()
    val left = streamOf(Seq[java.lang.Integer](1, 1, null, 3, 2).toDF("id"))

    val run = runStream(left.join(lookup, left("id") === lookup("id"), "left_outer"))
    val rows = run.rows
    assert(rows.toSet === Set(
      Row(1, 1, "engineering"),
      Row(1, 1, "research"),
      Row(null, null, null),
      Row(3, null, null),
      Row(2, 2, "sales")))
    assert(rows.count(_.get(0) == 1) === 4)
    assert(SimpleExternalLookupDataSource.requestSizes.asScala.toSeq === Seq(1, 2))
    assert(SimpleExternalLookupDataSource.requestedKeys.asScala.toSeq === Seq(1, 3, 2))
    assert(SimpleExternalLookupDataSource.nullKeyRequests.get() === 0)
    // Two batches, [1, 1, null] and [3, 2], send their 1 + 2 distinct requestable keys.
    val metrics = run.lookupJoin.metrics
    assert(Seq("numInputRows", "numInputBatches", "numLookupRequests", "numLookupParameterRows",
      "numOutputRows").map(metrics(_).value) === Seq(5, 2, 2, 3, 7))
    assert(metrics("lookupTime").value > 0)
  }

  test("a lookup join computes its key once per stream row that passes the stream condition") {
    // A UDF over the badge computes the key, and counts its calls. The stream condition
    // `verified` reads only the stream, so the join checks it before it computes the key, and
    // never computes the key of a row that fails it.
    val badgeId = udf { (badge: String) =>
      CountingKeyUDF.calls.incrementAndGet()
      badge.toInt
    }
    val employees = spark.read.format(classOf[SimpleExternalLookupDataSource].getName).load()
    val scans = streamOf(
      Seq(("1", true), ("2", false), ("1", true), ("3", true)).toDF("badge", "verified"))
    val condition = scans("verified") && employees("id") === badgeId(scans("badge"))
    val matched = Seq(Row("1", true, 1, "engineering"), Row("1", true, 1, "research"))
    Seq(
      "inner" -> (matched ++ matched),
      "left_outer" -> (matched ++ matched :+ Row("2", false, null, null) :+
        Row("3", true, null, null))).foreach { case (joinType, expected) =>
      CountingKeyUDF.calls.set(0)
      checkStream(scans.join(employees, condition, joinType), expected)
      assert(CountingKeyUDF.calls.get() === 3, joinType)
    }
  }

  test("external lookup batches rows with null keys, failing rows and repeated keys") {
    // Up to three stream rows per request. An inner join leaves out the row with a null key and
    // the unverified row, so its batches are [1, 1, 3] and [2]. A left outer join keeps them, so
    // its batches are [1, null, 1] and [2, 3, 2], and requests no parameter for them.
    val lookup = spark.read
      .format(classOf[SimpleExternalLookupDataSource].getName)
      .option("maxBatchSize", "3")
      .load()
    val scans = streamOf(Seq[(java.lang.Integer, Boolean)](
      (1, true), (null, true), (1, true), (2, false), (3, true), (2, true)).toDF("id", "verified"))
    val condition = scans("verified") && scans("id") === lookup("id")
    val matched = Seq(Row(1, true, 1, "engineering"), Row(1, true, 1, "research"))
    val inner = matched ++ matched :+ Row(2, true, 2, "sales")
    val unmatched = Seq(Row(null, true, null, null), Row(2, false, null, null),
      Row(3, true, null, null))
    // The rows, request sizes and the metrics: input rows, batches, requests, parameter rows and
    // output rows.
    Seq(
      ("inner", inner, Seq(2, 1), Seq(6, 2, 2, 3, 5)),
      ("left_outer", inner ++ unmatched, Seq(1, 2), Seq(6, 2, 2, 3, 8))).foreach {
      case (joinType, expected, requestSizes, metricValues) =>
        SimpleExternalLookupDataSource.resetCounters()
        val run = runStream(scans.join(lookup, condition, joinType))
        QueryTest.sameRows(expected, run.rows).foreach(error => fail(s"$joinType: $error"))
        assert(SimpleExternalLookupDataSource.requestSizes.asScala.toSeq === requestSizes,
          joinType)
        assert(SimpleExternalLookupDataSource.requestedKeys.asScala.toSeq === Seq(1, 3, 2),
          joinType)
        assert(SimpleExternalLookupDataSource.nullKeyRequests.get() === 0, joinType)
        assert(Seq("numInputRows", "numInputBatches", "numLookupRequests",
          "numLookupParameterRows", "numOutputRows").map(run.lookupJoin.metrics(_).value) ===
          metricValues, joinType)
    }
  }

  test("external lookup join supports null-safe lookup keys") {
    SimpleExternalLookupDataSource.resetCounters()
    val lookup = spark.read
      .format(classOf[SimpleExternalLookupDataSource].getName)
      .load()
    val left = streamOf(Seq[java.lang.Integer](1, null, 3).toDF("id"))

    checkStream(
      left.join(lookup, left("id") <=> lookup("id"), "left_outer"),
      Seq(
        Row(1, 1, "engineering"),
        Row(1, 1, "research"),
        Row(null, null, "unknown"),
        Row(3, null, null)))
    assert(SimpleExternalLookupDataSource.nullKeyRequests.get() > 0)
  }

  test("external lookup join supports complete composite equi-keys") {
    val lookup = spark.read
      .format(classOf[CompositeExternalLookupDataSource].getName)
      .load()
    val left = streamOf(Seq((1, 1), (1, 2), (2, 1)).toDF("tenant", "id"))

    checkStream(
      left.join(
        lookup,
        left("tenant") === lookup("tenant") && left("id") === lookup("id"),
        "left_outer"),
      Seq(Row(1, 1, 1, 1, "one-one"), Row(1, 2, null, null, null),
        Row(2, 1, 2, 1, "two-one")))
  }

  test("external lookup combines equality and range predicates in one access path") {
    CompositeExternalLookupDataSource.selectedLookupPredicates.set(Seq.empty)
    val lookup = spark.read
      .format(classOf[CompositeExternalLookupDataSource].getName)
      .load()
    // In tenant 1, the range from 0 covers the stored ids 1 and 3, so each of them joins two
    // stream rows of the same tenant.
    val left = streamOf(Seq(
      (1, 1, 3),
      (1, 0, 4),
      (1, 2, 4),
      (2, 0, 2),
      (3, 0, 10)).toDF("tenant", "start", "end"))
    val condition = lookup("tenant") === left("tenant") &&
      lookup("id") >= left("start") &&
      lookup("id") < left("end")

    checkStream(
      left.join(lookup, condition, "inner"),
      Seq(
        Row(1, 1, 3, 1, 1, "one-one"),
        Row(1, 0, 4, 1, 1, "one-one"),
        Row(1, 0, 4, 1, 3, "one-three"),
        Row(1, 2, 4, 1, 3, "one-three"),
        Row(2, 0, 2, 2, 1, "two-one")))
    assert(CompositeExternalLookupDataSource.selectedLookupPredicates.get() === Seq(
      ("=", "tenant"),
      (">=", "id"),
      ("<", "id")))
  }

  test("external lookup join supports multiple range predicates on one field") {
    SimpleExternalLookupDataSource.resetCounters()
    val lookup = spark.read
      .format(classOf[SimpleExternalLookupDataSource].getName)
      .load()
    val left = streamOf(Seq((1, 2), (1, 3)).toDF("start", "end"))

    checkStream(
      left.join(
        lookup,
        lookup("id") >= left("start") && lookup("id") < left("end"),
        "inner"),
      Seq(
        Row(1, 2, 1, "engineering"),
        Row(1, 2, 1, "research"),
        Row(1, 3, 1, "engineering"),
        Row(1, 3, 1, "research"),
        Row(1, 3, 2, "sales")))
    assert(SimpleExternalLookupDataSource.lookupCandidates.get() ===
      Seq((">=", "id", IntegerType), ("<", "id", IntegerType)))

    checkStream(
      left.join(
        lookup,
        left("start") < lookup("id") && left("end") >= lookup("id"),
        "inner"),
      Seq(
        Row(1, 2, 2, "sales"),
        Row(1, 3, 2, "sales")))
    assert(SimpleExternalLookupDataSource.lookupCandidates.get() ===
      Seq((">", "id", IntegerType), ("<=", "id", IntegerType)))
  }

  test("external lookup sends one parameter row per request when it selects a range") {
    val lookup = spark.read.format(classOf[SimpleExternalLookupDataSource].getName).load()
    // Three shifts that cover employee ids from 1, from 2 and from 3. Ids 1 and 2 are stored, so
    // id 2 is in the first two ranges. Each range is a request of its own, so the store returns
    // id 2 once in each, and each shift joins it once.
    val shifts = streamOf(Seq(("early", 1), ("late", 2), ("night", 3)).toDF("shift", "firstId"))
    val early = Seq(
      Row("early", 1, 1, "engineering"),
      Row("early", 1, 1, "research"),
      Row("early", 1, 2, "sales"))
    val late = Seq(Row("late", 2, 2, "sales"))

    Seq(
      "inner" -> (early ++ late),
      "left_outer" -> (early ++ late :+ Row("night", 3, null, null)),
      "left_semi" -> Seq(Row("early", 1), Row("late", 2)),
      "left_anti" -> Seq(Row("night", 3))).foreach { case (joinType, expected) =>
      SimpleExternalLookupDataSource.resetCounters()
      checkStream(shifts.join(lookup, lookup("id") >= shifts("firstId"), joinType), expected)
      assert(SimpleExternalLookupDataSource.requestSizes.asScala.toSeq === Seq(1, 1, 1), joinType)
    }
    // An equality-only lookup still sends the three ids in one request.
    SimpleExternalLookupDataSource.resetCounters()
    checkStream(
      shifts.join(lookup, lookup("id") === shifts("firstId")),
      Seq(
        Row("early", 1, 1, "engineering"),
        Row("early", 1, 1, "research"),
        Row("late", 2, 2, "sales")))
    assert(SimpleExternalLookupDataSource.requestSizes.asScala.toSeq === Seq(3))
    // So does a range with a constant bound: it has the same value in every parameter row, so a
    // stored id still matches only the parameter row of its own id.
    SimpleExternalLookupDataSource.resetCounters()
    checkStream(
      shifts.join(lookup, lookup("id") === shifts("firstId") && lookup("id") > 0),
      Seq(
        Row("early", 1, 1, "engineering"),
        Row("early", 1, 1, "research"),
        Row("late", 2, 2, "sales")))
    assert(SimpleExternalLookupDataSource.requestSizes.asScala.toSeq === Seq(3))
  }

  test("a range request holds one parameter row, which equal stream rows share") {
    val lookup = spark.read.format(classOf[SimpleExternalLookupDataSource].getName).load()
    val stored = Seq(1 -> "engineering", 1 -> "research", 2 -> "sales")
    // Five shifts in a row, where the first three cover employee ids from 1. A batch ends at the
    // first shift with another first id, so the five make three requests, not five.
    val shiftRows =
      Seq(("early", 1), ("early", 1), ("early", 1), ("late", 2), ("night", 1))
    val shifts = streamOf(shiftRows.toDF("shift", "firstId"))
    SimpleExternalLookupDataSource.resetCounters()
    checkStream(
      shifts.join(lookup, lookup("id") >= shifts("firstId")),
      for ((shift, firstId) <- shiftRows; (id, department) <- stored if id >= firstId)
        yield Row(shift, firstId, id, department))
    assert(SimpleExternalLookupDataSource.requestSizes.asScala.toSeq === Seq(1, 1, 1))
    assert(SimpleExternalLookupDataSource.requestedKeys.asScala.toSeq === Seq(1, 2, 1))
  }

  test("a range request takes up to 1000 equal stream rows, whatever the source's limit") {
    // Every shift covers employee ids from 1, so each request holds the one parameter row 1.
    def requests(shifts: Int, maxBatchSize: String, conf: (String, String)*): Seq[Int] = {
      val lookup = spark.read.format(classOf[SimpleExternalLookupDataSource].getName)
        .option("maxBatchSize", maxBatchSize).load()
      // Each shift carries a note of 1000 characters, so its row takes 1032 bytes.
      val note = "n" * 1000
      val left = streamOf((1 to shifts).map(shift => (shift, 1, note))
        .toDF("shift", "firstId", "note"))
      SimpleExternalLookupDataSource.resetCounters()
      withSQLConf(conf: _*) {
        assert(runStream(left.join(lookup, lookup("id") >= left("firstId"))).rows.size ===
          3 * shifts)
      }
      SimpleExternalLookupDataSource.requestSizes.asScala.toSeq
    }
    // Spark's cap of 1000 rows and maxBatchBytes each end a batch. The source's limit does not,
    // as the rows of a batch share the one parameter row of its request, also when the source
    // takes one parameter row per request.
    assert(requests(1500, "5000") === Seq(1, 1))
    assert(requests(1500, "1") === Seq(1, 1))
    assert(requests(5, "1") === Seq(1))
    assert(requests(5, "2") === Seq(1))
    assert(requests(7, "100") === Seq(1))
    assert(requests(7, "100", SQLConf.JOIN_EXTERNAL_LOOKUP_MAX_BATCH_BYTES.key -> "2500") ===
      Seq(1, 1, 1))
  }

  test("Spark drops the returned rows that fail a selected range") {
    // The store selects each range but returns every stored row, including the one without an
    // id, so only Spark's own check of the selected predicates decides which rows join.
    val lookup = spark.read.format(classOf[SimpleExternalLookupDataSource].getName)
      .option("ignoreSelectedRanges", "true").load()
    val stored = Seq(1 -> "engineering", 1 -> "research", 2 -> "sales")
    val shiftRows = Seq("early" -> 1, "late" -> 2)
    val shifts = streamOf(shiftRows.toDF("shift", "boundary"))
    val id = lookup("id")
    val boundary = shifts("boundary")
    // Each range, as written with the lookup column on the left and on the right.
    Seq[(String, (Int, Int) => Boolean, Column, Column)](
      ("<", _ < _, id < boundary, boundary > id),
      ("<=", _ <= _, id <= boundary, boundary >= id),
      (">", _ > _, id > boundary, boundary < id),
      (">=", _ >= _, id >= boundary, boundary <= id)).foreach {
      case (operator, holds, written, swapped) =>
        val expected = for ((shift, bound) <- shiftRows; (storedId, department) <- stored
          if holds(storedId, bound)) yield Row(shift, bound, storedId, department)
        Seq(written, swapped).foreach { condition =>
          SimpleExternalLookupDataSource.resetCounters()
          checkStream(shifts.join(lookup, condition), expected)
          assert(SimpleExternalLookupDataSource.lookupCandidates.get() ===
            Seq((operator, "id", IntegerType)), condition)
          // Each of the two requests returned all four stored rows.
          assert(SimpleExternalLookupDataSource.resultRowsRead.get() === 8, condition)
        }
    }
  }

  test("a range against a cast that cannot fail runs as an external lookup join") {
    // ExprUtils.canEvaluateUnconditionally takes a TIMESTAMP cast to DATE and a try_cast as unable
    // to fail, so a range on either is offered, here as the only lookup predicate. Each join
    // returns the rows of the ordinary join that it replaces, over the same rows, and a stream row
    // whose try_cast is NULL matches nothing, so its parameter is not requested.
    def check(joined: DataFrame, replaced: DataFrame): Unit = {
      val run = runStream(joined)
      assert(run.plan.exists(_.isInstanceOf[PreparedExternalLookupJoin]))
      val expected = runStream(replaced).rows
      assert(expected.nonEmpty)
      QueryTest.sameRows(expected, run.rows).foreach(fail(_))
    }
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "true", SQLConf.SESSION_LOCAL_TIMEZONE.key -> "UTC") {
      def day(date: String): Int = java.time.LocalDate.parse(date).toEpochDay.toInt
      TypedKeyExternalLookupTable.storedRows.set(
        Seq(day("2026-01-01") -> "new year", day("2026-01-03") -> "holiday"))
      val calendar = spark.read.format(classOf[TypedKeyExternalLookupDataSource].getName)
        .option("keyType", "DATE").option("ranges", "true").load()
      val calendarRows = sql("SELECT DATE'2026-01-01' AS key, 'new year' AS payload " +
        "UNION ALL SELECT DATE'2026-01-03', 'holiday'")
      val visits = streamOf(sql("SELECT TIMESTAMP'2026-01-02 10:00:00' AS at"))
      check(
        visits.join(calendar, calendar("key") >= visits("at").cast("date")),
        visits.join(calendarRows, calendarRows("key") >= visits("at").cast("date")))

      val employees = spark.read.format(classOf[SimpleExternalLookupDataSource].getName).load()
      val employeeRows =
        Seq((1, "engineering"), (1, "research"), (2, "sales")).toDF("id", "department")
      val badges = streamOf(Seq("2", "abc").toDF("code"))
      SimpleExternalLookupDataSource.resetCounters()
      check(
        badges.join(employees, employees("id") >= expr("try_cast(code AS INT)")),
        badges.join(employeeRows, employeeRows("id") >= expr("try_cast(code AS INT)")))
      assert(SimpleExternalLookupDataSource.lookupCandidates.get() ===
        Seq((">=", "id", IntegerType)))
      assert(SimpleExternalLookupDataSource.requestedKeys.asScala.toSeq === Seq(2))
      assert(SimpleExternalLookupDataSource.nullKeyRequests.get() === 0)
    }
  }

  test("lookup join nodes with different lookup-side stages canonicalize apart") {
    // A projection stage that computes a label from the product, and a filter stage on it, over a
    // table of two STRING columns.
    val products =
      spark.read.format(classOf[CaseInsensitiveExternalLookupDataSource].getName).load()
    val staged = products.select($"sku", $"product", upper($"product").as("label"))
      .where($"label" =!= "NONE")
    val orders = streamOf(Seq((1, "AB-1")).toDF("order", "sku"))
    val exec = runStream(orders.join(staged, orders("sku") === staged("sku"))).lookupJoin
    assert(exec.lookupTransforms.length === 2)
    val Seq(sku, product) = exec.lookupInput
    val label = exec.lookupTransforms.collectFirst {
      case project: ExternalLookupProject => project.output.last
    }.get
    // The same join after another analysis, which gives every lookup-side attribute another id.
    val newIds = AttributeMap(ExternalLookupTransform.attributes(exec.lookupInput,
      exec.lookupTransforms).map(attr => attr -> attr.newInstance()))
    def renew[T <: Expression](expression: T): T = expression.transformUp {
      case alias: Alias if newIds.contains(alias.toAttribute) =>
        Alias(alias.child, alias.name)(newIds(alias.toAttribute).exprId)
      case attr: Attribute if newIds.contains(attr) => newIds(attr)
    }.asInstanceOf[T]
    val reanalyzed = exec.transformExpressions {
      case attr: Attribute if newIds.contains(attr) => newIds(attr)
    }.asInstanceOf[ExternalLookupJoinExec].copy(lookupTransforms = exec.lookupTransforms.map {
      case ExternalLookupProject(projectList, input) =>
        ExternalLookupProject(projectList.map(renew), input.map(renew))
      case ExternalLookupFilter(condition, input) =>
        ExternalLookupFilter(renew(condition), input.map(renew))
    })
    assert(reanalyzed.lookupInput != exec.lookupInput)
    assert(reanalyzed.sameResult(exec))
    // A stage expression over the other column of the same type.
    val bySku = exec.copy(lookupTransforms = exec.lookupTransforms.map {
      case project: ExternalLookupProject => project.copy(projectList =
        project.projectList.map(_.transform { case Upper(_) => Upper(sku) }
          .asInstanceOf[NamedExpression]))
      case other => other
    })
    assert(bySku.sameResult(exec) === false)
    // Joins that keep different columns of one type, of the read schema or of a stage.
    Seq(Seq(sku), Seq(product), Seq(label)).combinations(2).foreach { case Seq(one, other) =>
      assert(exec.copy(lookupOutput = one).sameResult(exec.copy(lookupOutput = other)) === false,
        s"$one, $other")
    }
  }

  test("a range on a widened stream column runs as an external lookup join") {
    val lookup = spark.read.format(classOf[SimpleExternalLookupDataSource].getName).load()
    // Shifts that start at an employee id held as SMALLINT. Analysis casts it to the INT `id`,
    // and the cast cannot fail, so Spark offers the range to the store instead of failing with
    // NO_LOOKUP_CANDIDATE.
    val shifts =
      streamOf(Seq(("early", 1.toShort), ("late", 2.toShort)).toDF("shift", "firstId"))

    checkStream(
      shifts.join(lookup, lookup("id") >= shifts("firstId")),
      Seq(
        Row("early", 1.toShort, 1, "engineering"),
        Row("early", 1.toShort, 1, "research"),
        Row("early", 1.toShort, 2, "sales"),
        Row("late", 2.toShort, 2, "sales")))
    assert(SimpleExternalLookupDataSource.lookupCandidates.get() ===
      Seq((">=", "id", IntegerType)))
  }

  test("a range with a cast that can fail is not offered, so an unmatched row does not fail") {
    ValidityExternalLookupTable.offered.set(Seq.empty)
    val rates = spark.read.format(classOf[ValidityExternalLookupDataSource].getName).load()
    // An event on the last day that a DATE can hold. Its cast to TIMESTAMP_NTZ overflows, and no
    // rate is stored for its id. A join evaluates the range only for the rows whose id matches,
    // so this event must not fail the query.
    val events = streamOf(Seq((2, Int.MaxValue)).toDF("id", "dayNumber"))
      .select($"id", date_from_unix_date($"dayNumber").as("day"))
    val condition = rates("id") === events("id") &&
      rates("validFrom") >= events("day").cast("timestamp_ntz")

    checkStream(
      events.join(rates, condition, "left_outer").select(events("id"), rates("id")),
      Seq(Row(2, null)))
    assert(ValidityExternalLookupTable.offered.get() === Seq("id = ?"))
  }

  test("external lookup leaves unselected criteria as Spark residuals") {
    SimpleExternalLookupDataSource.resetCounters()
    val lookup = spark.read
      .format(classOf[SimpleExternalLookupDataSource].getName)
      .load()
    val runs = streamOf(Seq(
      (1, 3, "sales"),
      (1, 3, "research"),
      (1, 2, "missing")).toDF("start", "end", "costCenter"))
    val condition = lookup("id") >= runs("start") &&
      lookup("id") < runs("end") &&
      lookup("department") === runs("costCenter")

    checkStream(
      runs.join(lookup, condition, "left_semi"),
      Seq(Row(1, 3, "sales"), Row(1, 3, "research")))
    assert(SimpleExternalLookupDataSource.lookupCandidates.get() === Seq(
      (">=", "id", IntegerType),
      ("<", "id", IntegerType),
      ("=", "department", StringType)))
    assert(SimpleExternalLookupDataSource.requiredColumns.get() === Seq("id", "department"))
  }

  test("an upstream limit stops one-to-many cursor consumption") {
    SimpleExternalLookupDataSource.departmentsById.set(Map(1 -> (1 to 2000).map(_.toString)))
    val lookup = spark.read.format(classOf[SimpleExternalLookupDataSource].getName).load()
    val left = streamOf(Seq(1).toDF("id"))

    assert(runStream(left.join(
      lookup, left("id") === lookup("id"), "inner")
      .limit(1)).rows.size === 1)
    assert(SimpleExternalLookupDataSource.resultRowsRead.get() === 1)
    assert(SimpleExternalLookupDataSource.closeEvents.asScala.toSeq === Seq("cursor", "reader"))
  }

  test("external lookup samples connector metrics while consuming a large response") {
    SimpleExternalLookupDataSource.departmentsById.set(Map(1 -> (1 to 250).map(_.toString)))
    val lookup = spark.read.format(classOf[SimpleExternalLookupDataSource].getName).load()
    val left = streamOf(Seq(1).toDF("id"))

    assert(runStream(left.join(
      lookup, left("id") === lookup("id"), "inner")).rows.size === 250)
    // Two periodic samples, followed by request-completion and reader-close samples.
    assert(SimpleExternalLookupDataSource.metricSamples.get() >= 4)
  }

  test("external lookup offers no lookup key on a double or a string with a non-binary collation") {
    // A store may compare such values differently from Spark, for NaN and -0.0 or for case, so the
    // equality stays in Spark's residual condition. Without another lookup key the join fails.
    Seq(
      "double" -> streamOf(Seq(-0.0d, 0.0d).toDF("key")),
      "string collate utf8_lcase" -> streamOf(Seq("lookup", "LOOKUP").toDF("key"))
        .selectExpr("CAST(key AS STRING COLLATE UTF8_LCASE) AS key")).foreach {
      case (keyType, left) =>
        val lookup = semanticKeyLookup(keyType)
        checkError(
          exception = streamFailure[AnalysisException](
            left.join(lookup, left("key") === lookup("key"))),
          condition = "EXTERNAL_LOOKUP_JOIN_UNSUPPORTED.NO_LOOKUP_CANDIDATE",
          parameters = Map("table" -> "`SemanticKeyExternalLookupTable`"))
        assert(SemanticKeyExternalLookupDataSource.requestSizes.isEmpty)
    }
  }

  test("external lookup drops returned rows that do not satisfy the selected lookup predicates") {
    // The store matches SKUs case-insensitively, so it returns both `AB-1` and `ab-1` for every
    // order. Spark checks `sku = ?` itself, and keeps only the product with the exact SKU.
    val products =
      spark.read.format(classOf[CaseInsensitiveExternalLookupDataSource].getName).load()
    val orders = streamOf(Seq((1, "AB-1"), (2, "ab-1"), (3, "Ab-1")).toDF("order", "sku"))
    val matched = Seq(Row(1, "AB-1", "AB-1", "adapter"), Row(2, "ab-1", "ab-1", "cable"))
    Seq(
      "inner" -> matched,
      "left_outer" -> (matched :+ Row(3, "Ab-1", null, null)),
      "left_semi" -> Seq(Row(1, "AB-1"), Row(2, "ab-1")),
      "left_anti" -> Seq(Row(3, "Ab-1"))).foreach {
      case (joinType, expected) =>
        CaseInsensitiveExternalLookupTable.returnedRows.set(0)
        checkStream(orders.join(products, orders("sku") === products("sku"), joinType), expected)
        assert(CaseInsensitiveExternalLookupTable.returnedRows.get() === 6)
    }
  }

  test("external lookup checks every selected lookup predicate on the returned rows") {
    // The source selects `tenant = ?` and `id = ?`, but ignores one of them, each in turn, when it
    // matches, so it also returns stored rows that fail that predicate. Spark checks both on every
    // returned row, and joins only the stored rows whose tenant and id both match the stream row.
    val lookup = spark.read.format(classOf[CompositeExternalLookupDataSource].getName).load()
    val left = streamOf(Seq((1, 1), (1, 2), (2, 1)).toDF("tenant", "id"))
    val condition = left("tenant") === lookup("tenant") && left("id") === lookup("id")
    val matched = Seq(Row(1, 1, 1, 1, "one-one"), Row(2, 1, 2, 1, "two-one"))
    // The stored rows that the source returns for each stream row, as (tenant, id) pairs.
    val returnedIgnoring = Map(
      "tenant" -> Set((1, 1) -> (1, 1), (1, 1) -> (2, 1), (2, 1) -> (1, 1), (2, 1) -> (2, 1)),
      "id" -> Set((1, 1) -> (1, 1), (1, 1) -> (1, 3), (1, 2) -> (1, 1), (1, 2) -> (1, 3),
        (2, 1) -> (2, 1)))
    try {
      for {
        (ignored, returned) <- returnedIgnoring
        (joinType, expected) <-
          Seq("inner" -> matched, "left_outer" -> (matched :+ Row(1, 2, null, null, null)))
      } {
        CompositeExternalLookupDataSource.ignoredField.set(Some(ignored))
        CompositeExternalLookupDataSource.returnedRows.clear()
        checkStream(left.join(lookup, condition, joinType), expected)
        assert(CompositeExternalLookupDataSource.selectedLookupPredicates.get() ===
          Seq("=" -> "tenant", "=" -> "id"), s"$ignored, $joinType")
        assert(CompositeExternalLookupDataSource.returnedRows.asScala.toSet === returned,
          s"$ignored, $joinType")
      }
    } finally {
      CompositeExternalLookupDataSource.ignoredField.set(None)
    }
  }

  test("a stream-side guard keeps a lookup key or a residual from failing on rows it rejects") {
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "true") {
      val employees = spark.read.format(classOf[SimpleExternalLookupDataSource].getName).load()
      // The guard is checked before the key is computed, so the badge `bad` is never cast: an
      // inner join checks it when it batches the stream rows, and a left outer join when it reads
      // the batch's keys.
      val scans = streamOf(Seq(("1", true), ("bad", false)).toDF("badge", "verified"))
      val verified = Seq(Row("1", true, 1, "engineering"), Row("1", true, 1, "research"))
      Seq("inner" -> verified, "left_outer" -> (verified :+ Row("bad", false, null, null)))
        .foreach { case (joinType, expected) =>
          checkStream(
            scans.join(employees,
              scans("verified") && employees("id") === scans("badge").cast("int"), joinType),
            expected)
        }
      // A conjunct that can fail is evaluated on joined rows only, as a join does, so never for
      // employee 99, who has no match.
      val timesheets = streamOf(Seq((1, "8"), (99, "bad")).toDF("id", "hours"))
      checkStream(
        timesheets.join(employees,
          timesheets("id") === employees("id") && timesheets("hours").cast("int") > 0,
          "left_outer"),
        Seq(Row(1, "8", 1, "engineering"), Row(1, "8", 1, "research"),
          Row(99, "bad", null, null)))
    }
  }

  test("external lookup finishes no stream row after a failed or closed read") {
    // After a failure or a close the result may be partial, so a stream row without a match could
    // be one whose match was never read: Spark emits no unmatched row then. Id 1 has two matches
    // and id 3 has none, and both are in one request.
    val lookup = spark.read.format(classOf[SimpleExternalLookupDataSource].getName).load()
    val left = streamOf(Seq(1).toDF("id"))
    val outerJoin =
      runStream(left.join(lookup, left("id") === lookup("id"), "left_outer")).lookupJoin
    val input = CoalesceExec(1,
      LocalTableScanExec(outerJoin.child.output, Seq(InternalRow(1), InternalRow(3)), None))
    Seq(
      "cursorFailure" -> 0,
      "closeDuringNext" -> 0,
      "closeDuringGet" -> 0,
      "closeBetweenResultRows" -> 1,
      "closeAtCursorEnd" -> 2).foreach {
      case (failure, outputRows) =>
        SimpleExternalLookupDataSource.resetCounters()
        val readerFactory = outerJoin.readerFactory match {
          case factory: SimpleExternalLookupReaderFactory if failure == "cursorFailure" =>
            factory.copy(failAt = "cursor")
          case factory => factory
        }
        val failingJoin = outerJoin.copy(readerFactory = readerFactory, child = input)
        val rdd = failingJoin.execute()
        val context = TaskContext.empty()
        TaskContext.setTaskContext(context)
        try {
          val rows = rdd.iterator(rdd.partitions.head, context)
          val completeTask = () => context.markTaskCompleted(None)
          failure match {
            case "closeBetweenResultRows" =>
              rows.next()
              completeTask()
            case "closeAtCursorEnd" =>
              SimpleExternalLookupDataSource.beforeCursorEnds.set(completeTask)
            // The cursor had a row when the close came, but Spark reads no row from it.
            case "closeDuringNext" =>
              SimpleExternalLookupDataSource.beforeNextReturns.set(completeTask)
            // The close comes while the row is read, so Spark neither copies nor joins it.
            case "closeDuringGet" =>
              SimpleExternalLookupDataSource.beforeGetReturns.set(completeTask)
            case _ =>
          }
          intercept[Exception] {
            while (rows.hasNext) rows.next()
          }
          assert(failingJoin.metrics("numOutputRows").value === outputRows, failure)
          assert(SimpleExternalLookupDataSource.rowReadsAfterClose.get() === 0, failure)
        } finally {
          TaskContext.unset()
        }
    }
  }

  private def semanticKeyLookup(keyType: String): DataFrame = {
    spark.read
      .format(classOf[SemanticKeyExternalLookupDataSource].getName)
      .option("keyType", keyType)
      .load()
  }

  test("a join strategy hint on the stream side of a lookup-only join is rejected") {
    val lookup = spark.read.format(classOf[SimpleExternalLookupDataSource].getName).load()
    val left = streamOf(Seq(1).toDF("id"))
    checkError(
      exception = intercept[AnalysisException] {
        left.hint("merge").join(lookup, left("id") === lookup("id")).queryExecution.analyzed
      },
      condition = "EXTERNAL_LOOKUP_JOIN_UNSUPPORTED.CONFLICTING_JOIN_STRATEGY",
      parameters = Map("strategy" -> "MERGE", "relation" -> "`SimpleExternalLookupTable`"))

    // A strategy hint on a different join of the same query does not conflict.
    val other = Seq((1, "a")).toDF("id", "tag")
    val nested = left.join(other.hint("merge"), "id")
    val joined = nested.join(lookup, nested("id") === lookup("id"))
    assert(runStream(joined).plan.exists(_.isInstanceOf[PreparedExternalLookupJoin]))
    checkStream(joined, Seq(Row(1, "a", 1, "engineering"), Row(1, "a", 1, "research")))
  }

  test("a lookup join whose condition folds to false does not call the connector") {
    val lookup = spark.read.format(classOf[SimpleExternalLookupDataSource].getName).load()
    val left = streamOf(Seq(1, 3, 3).toDF("id")).dropDuplicates("id")
    Seq(
      "inner" -> Seq.empty[Row],
      "left_outer" -> Seq(Row(1, null, null), Row(3, null, null))).foreach {
      case (joinType, expected) =>
        SimpleExternalLookupDataSource.resetCounters()
        val joined = left.join(lookup, left("id") === lookup("id") && lit(false), joinType)
        val run = runStream(joined)
        assert(run.plan.exists(_.isInstanceOf[PreparedExternalLookupJoin]) === false)
        QueryTest.sameRows(expected, run.rows).foreach(fail(_))
        // The stateful stream side still runs.
        assert(run.plan.exists(_.isInstanceOf[Deduplicate]))
        val deduplicate = run.execution.executedPlan.collectFirst {
          case exec: StreamingDeduplicateExec => exec
        }.get
        assert(deduplicate.metrics("numTotalStateRows").value === 2)
        assert(SimpleExternalLookupDataSource.lookupCandidates.get() === Seq.empty)
        assert(SimpleExternalLookupDataSource.readerCount.get() === 0)
        assert(SimpleExternalLookupDataSource.requestCount.get() === 0)
    }
  }

  private def readSchemaLookup(kind: String): DataFrame = spark.read
    .format(classOf[ReadSchemaExternalLookupDataSource].getName)
    .option("kind", kind)
    .load()

  test("external lookup reads value rows that follow the read schema") {
    val left = streamOf(Seq(1, 2).toDF("id"))

    val varchar = readSchemaLookup("varchar")
    checkStream(
      left.join(varchar, left("id") === varchar("id")),
      Seq(Row(1, 1, "ab"), Row(2, 2, "ab")))
    // The read schema lists the table columns in another order.
    val reordered = readSchemaLookup("reordered")
    checkStream(
      left.join(reordered, left("id") === reordered("id")),
      Seq(Row(1, 1, "ab"), Row(2, 2, "ab")))
    // The table declares `s.a` NOT NULL and the read schema does not, which the binding accepts.
    // Returned values must satisfy both, so the null that the connector returns for id 2 fails
    // the task.
    val nested = readSchemaLookup("nested")
    val firstId = streamOf(Seq(1).toDF("id"))
    checkStream(
      firstId.join(nested, firstId("id") === nested("id"))
        .select(firstId("id"), to_json(nested("s"))),
      Seq(Row(1, "{\"a\":10}")))
    Seq("inner", "left_outer").foreach { joinType =>
      checkError(
        exception = streamFailure[SparkRuntimeException](
          left.join(nested, left("id") === nested("id"), joinType)),
        condition = "DATA_SOURCE_INVALID_EXTERNAL_LOOKUP.NULL_IN_NOT_NULL_FIELD",
        parameters = Map("table" -> "`ReadSchemaExternalLookupTable`", "field" -> "`s`.`a`"))
    }
    // A field whose type really differs is still rejected.
    val wrongType = readSchemaLookup("wrongType")
    checkError(
      exception = streamFailure[AnalysisException](
        left.join(wrongType, left("id") === wrongType("id"))),
      condition = "DATA_SOURCE_INVALID_EXTERNAL_LOOKUP.UNKNOWN_SCAN_FIELD",
      parameters = Map("table" -> "`ReadSchemaExternalLookupTable`", "field" -> "`name`"))
  }

  test("external lookup checks a user-defined type's value as its underlying type") {
    def lookup(container: String, value: String): DataFrame = spark.read
      .format(classOf[UserTypeExternalLookupDataSource].getName)
      .option("container", container)
      .option("value", value)
      .load()
    val left = streamOf(Seq(1, 2).toDF("id"))
    def badge(id: Int): Badge = Badge(id, s"badge-$id", new java.math.BigDecimal("12.50"))
    // Valid badges are joined unchanged, alone and inside an array, a map and a struct.
    Seq[(String, Int => Any)](
      ("none", (id: Int) => badge(id)),
      ("array", (id: Int) => Seq(badge(id))),
      ("map", (id: Int) => Map("lobby" -> badge(id))),
      ("struct", (id: Int) => Row(badge(id)))).foreach { case (container, column) =>
      val valid = lookup(container, "valid")
      Seq("inner", "left_outer").foreach { joinType =>
        checkStream(
          left.join(valid, left("id") === valid("id"), joinType),
          Seq(1, 2).map(id => Row(id, id, column(id))))
      }
    }
    // A badge that breaks the struct that stores it fails the task, at each nested level: a struct
    // of nulls, which would give a kind of 0, a null label, and a credit with too many digits.
    val table = Map("table" -> "`UserTypeExternalLookupTable`")
    for {
      (container, path) <- Seq(
        "none" -> "`badge`",
        "array" -> "`badge`.`element`",
        "map" -> "`badge`.`value`",
        "struct" -> "`badge`.`current`")
      (value, subClass, parameters) <- Seq(
        ("nulls", "NULL_IN_NOT_NULL_FIELD", Map("field" -> s"$path.`kind`")),
        ("nullLabel", "NULL_IN_NOT_NULL_FIELD", Map("field" -> s"$path.`label`")),
        ("outOfRange", "DECIMAL_OUT_OF_RANGE",
          Map("field" -> s"$path.`credit`", "type" -> "\"DECIMAL(5,2)\"")))
      joinType <- Seq("inner", "left_outer")
    } {
      val invalid = lookup(container, value)
      checkError(
        exception = streamFailure[SparkRuntimeException](
          left.join(invalid, left("id") === invalid("id"), joinType)),
        condition = s"DATA_SOURCE_INVALID_EXTERNAL_LOOKUP.$subClass",
        parameters = table ++ parameters)
    }
  }

  test("external lookup checks nested nulls against the stricter of the two schemas") {
    // At each nested level, the table rules out a null that the read schema allows, and the
    // connector returns that null.
    val left = streamOf(Seq(1).toDF("id"))
    Seq(
      "nullArrayElement" -> "`tags`.`element`",
      "nullMapValue" -> "`scores`.`value`",
      "nullStructInArray" -> "`items`.`element`.`a`").foreach { case (kind, field) =>
      val lookup = readSchemaLookup(kind)
      Seq("inner", "left_outer").foreach { joinType =>
        checkError(
          exception = streamFailure[SparkRuntimeException](
            left.join(lookup, left("id") === lookup("id"), joinType)),
          condition = "DATA_SOURCE_INVALID_EXTERNAL_LOOKUP.NULL_IN_NOT_NULL_FIELD",
          parameters = Map("table" -> "`ReadSchemaExternalLookupTable`", "field" -> field))
      }
    }
  }

  test("external lookup applies the read-side VARCHAR check under standard semantics") {
    withSQLConf(SQLConf.CHAR_VARCHAR_STANDARD_SEMANTICS.key -> "true") {
      val left = streamOf(Seq(1).toDF("id"))
      val varchar = readSchemaLookup("varchar")
      val run = runStream(left.join(varchar, left("id") === varchar("id")))
      QueryTest.sameRows(Seq(Row(1, 1, "ab")), run.rows).foreach(fail(_))
      // The check is a lookup-side projection over the connector's value rows.
      assert(run.lookupJoin.lookupTransforms.nonEmpty)
    }
  }

  test("external lookup keeps a decimal that fits its type, at any nested level") {
    val lookup = spark.read
      .format(classOf[MisconfiguredExternalLookupDataSource].getName)
      .option("kind", "decimalInRange")
      .load()
    val left = streamOf(Seq(1).toDF("id"))
    def decimal(value: String): java.math.BigDecimal = new java.math.BigDecimal(value)
    checkStream(
      left.join(lookup, left("id") === lookup("id"))
        .select(lookup("amount"), lookup("amounts"), lookup("rates")),
      Seq(Row(
        decimal("999.99"),
        Seq(decimal("-999.99"), decimal("1.50")),
        Map("EUR" -> decimal("0.01")))))
    // So does a compact decimal in an UnsafeRow, a struct in one, or a columnar row or array.
    Seq(
      "unsafe" -> Row(decimal("999.99")),
      "unsafeNested" -> Row(Row(decimal("999.99"))),
      "columnar" -> Row(decimal("999.99")),
      "columnarArray" -> Row(Seq(decimal("999.99")))).foreach { case (rowKind, expected) =>
      val compact = spark.read
        .format(classOf[MisconfiguredExternalLookupDataSource].getName)
        .option("kind", s"compactDecimal:$rowKind:inRange")
        .load()
      checkStream(
        left.join(compact, left("id") === compact("id")).select(compact(compact.columns(1))),
        Seq(expected))
    }
  }

  test("a lookup-only CHAR column is read as stored when read-side padding is off") {
    val left = streamOf(Seq(1).toDF("id"))
    withSQLConf(SQLConf.READ_SIDE_CHAR_PADDING.key -> "false") {
      // The connector returns "ab" for a CHAR(5) column, and Spark returns it unpadded.
      val char = readSchemaLookup("char")
      checkStream(left.join(char, left("id") === char("id")), Seq(Row(1, 1, "ab")))
      // Standard semantics apply the read-side padding anyway, as a lookup-side stage.
      withSQLConf(SQLConf.CHAR_VARCHAR_STANDARD_SEMANTICS.key -> "true") {
        val paddedChar = readSchemaLookup("char")
        val run = runStream(left.join(paddedChar, left("id") === paddedChar("id")))
        QueryTest.sameRows(Seq(Row(1, 1, "ab   ")), run.rows).foreach(fail(_))
        assert(run.lookupJoin.lookupTransforms.nonEmpty)
      }
    }
  }

  test("a Python UDF over lookup columns in the join condition") {
    assume(shouldTestPythonUDFs)
    val pythonUDF = TestPythonUDF("lookup_department")
    val lookup = spark.read.format(classOf[SimpleExternalLookupDataSource].getName).load()
    val left = streamOf(Seq(1, 2).toDF("id"))
    val condition =
      left("id") === lookup("id") && pythonUDF(lookup("department")) === "engineering"

    // An inner join evaluates it above the lookup join; other join types cannot.
    checkStream(left.join(lookup, condition), Seq(Row(1, 1, "engineering")))
    Seq("left_outer" -> "LEFT OUTER", "left_semi" -> "LEFT SEMI").foreach {
      case (joinType, sqlJoinType) =>
        checkError(
          exception = streamFailure[AnalysisException](left.join(lookup, condition, joinType)),
          condition = "UNSUPPORTED_FEATURE.PYTHON_UDF_IN_ON_CLAUSE",
          parameters = Map("joinType" -> sqlJoinType))
    }
    // A lookup-side filter is evaluated inside the lookup join, which cannot run a Python UDF, so
    // the join fails when the DataFrame is built.
    val filtered = lookup.filter(pythonUDF(lookup("department")) === "engineering")
    checkError(
      exception = intercept[AnalysisException](left.join(filtered, left("id") === filtered("id"))),
      condition = "EXTERNAL_LOOKUP_JOIN_UNSUPPORTED.LOOKUP_PLAN",
      parameters = Map.empty)
  }

  test("external lookup merges the connector metrics of every reader in a task") {
    withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
        SQLConf.SQL_STRING_REDACTION_PATTERN.key -> "secret-token") {
      val lookup = spark.read.format(classOf[MetricsExternalLookupDataSource].getName).load()
      // Two one-row input partitions, computed by one task under the coalesce, open two readers
      // that send one request each.
      val left = streamOf(spark.range(0, 2).selectExpr("CAST(id + 1 AS INT) AS id"), 2)
      val joined = left.join(lookup, left("id") === lookup("id")).coalesce(1)

      val run = runStream(joined)
      assert(run.rows.length === 2)
      val lookupJoin = run.lookupJoin
      assert(lookupJoin.metrics("source.connectorRequests").value === 2)
      // A connector metric named like an operator metric is kept apart from it.
      assert(lookupJoin.metrics("source.numOutputRows").value === 7)
      assert(lookupJoin.metrics("numOutputRows").value === 2)
      // The connector description is redacted.
      assert(lookupJoin.simpleString(10).contains("MetricsExternalLookupScan(token="))
      assert(run.execution.executedPlan.toString.contains("secret-token") === false)
    }
  }

  test("the physical lookup join shows its join type, parameters and residual condition") {
    withSQLConf(SQLConf.SQL_STRING_REDACTION_PATTERN.key -> "secret-token") {
      val lookup = spark.read.format(classOf[MetricsExternalLookupDataSource].getName).load()
      val left = streamOf(Seq(1, 2).toDF("id"))
      val residual = left("id") =!= lookup("id") + 5
      val lookupJoin =
        runStream(left.join(lookup, left("id") === lookup("id") && residual, "left_outer"))
          .lookupJoin
      val rendered = lookupJoin.simpleString(10)
      Seq("LeftOuter", lookupJoin.lookupParameters.head.toString,
          lookupJoin.residualCondition.get.toString, "MetricsExternalLookupScan(token=")
        .foreach(part => assert(rendered.contains(part), rendered))
      // The connector description is still redacted.
      assert(rendered.contains("secret-token") === false, rendered)
    }
  }

  test("the physical lookup join shows its selected predicates and lookup-side stages") {
    val lookup = spark.read.format(classOf[SimpleExternalLookupDataSource].getName).load()
      .where($"department" =!= "research")
      .select($"id", upper($"department").as("department"))
    val shifts = streamOf(Seq(("early", 1)).toDF("shift", "firstId"))
    val lookupJoin = runStream(shifts.join(lookup, lookup("id") >= shifts("firstId"))).lookupJoin
    val selected = lookupJoin.lookupKeyComparisons.head(
      lookupJoin.lookupKeys.head, lookupJoin.lookupParameters.head)
    val rendered = lookupJoin.simpleString(10)
    Seq(selected.toString, "Filter NOT (department", "Project [", "upper(department")
      .foreach(part => assert(rendered.contains(part), rendered))
  }

  test("the physical lookup join keeps the determinism level of its input") {
    // Spark does not mark the join's output INDETERMINATE, although a connector may return other
    // rows for a rerun task.
    val lookup = spark.read.format(classOf[SimpleExternalLookupDataSource].getName).load()
    val left = streamOf(Seq(1, 2).toDF("id"))
    val lookupJoin = runStream(left.join(lookup, left("id") === lookup("id"))).lookupJoin
    val childLevel = lookupJoin.child.execute().outputDeterministicLevel
    assert(childLevel === DeterministicLevel.DETERMINATE)
    assert(lookupJoin.execute().outputDeterministicLevel === childLevel)
  }

  test("external lookup closes task-local readers after success and failure") {
    def lookup(options: (String, String)*): DataFrame = {
      var reader = spark.read.format(classOf[SimpleExternalLookupDataSource].getName)
      options.foreach { case (key, value) => reader = reader.option(key, value) }
      reader.load()
    }
    val left = streamOf(Seq(1).toDF("id"))

    val successfulLookup = lookup()
    runStream(left.join(successfulLookup, left("id") === successfulLookup("id")))
    assert(SimpleExternalLookupDataSource.readerCount.get() === 1)
    // The cursor is closed before its reader, and the final metrics are read before that.
    assert(SimpleExternalLookupDataSource.closeEvents.asScala.toSeq === Seq("cursor", "reader"))
    assert(lastReaderEvents === Seq("metrics", "close"))

    // A request that fails returns no cursor; a failing cursor is still closed.
    Seq("request" -> Seq("reader"), "cursor" -> Seq("cursor", "reader")).foreach {
      case (failAt, closeEvents) =>
        SimpleExternalLookupDataSource.resetCounters()
        val failingLookup = lookup("failAt" -> failAt)
        val failure = streamFailure[SparkException](
          left.join(failingLookup, left("id") === failingLookup("id")))
        assert(failure.getMessage.contains(s"injected external lookup $failAt failure"))
        assert(SimpleExternalLookupDataSource.readerCount.get() === 1)
        assert(SimpleExternalLookupDataSource.closeEvents.asScala.toSeq === closeEvents)
    }
  }

  test("external lookup makes no connector call once its task completed") {
    val lookup = spark.read.format(classOf[SimpleExternalLookupDataSource].getName).load()
    val left = streamOf(Seq(1).toDF("id"))
    val lookupJoin = runStream(left.join(lookup, left("id") === lookup("id"))).lookupJoin
    // Computes the join over a local input, in a task that completes before the join is
    // consumed, before its first request, while its reader is created, while the request is in
    // progress, or between the two result rows of the request. Another thread consumes the join,
    // as when a thread other than the task's drives it.
    val rdd = lookupJoin
      .copy(child = LocalTableScanExec(lookupJoin.child.output, Seq(InternalRow(1)), None))
      .execute()
    val completions = Seq(
      "beforeEval", "beforeRequest", "duringReaderCreation", "duringRequest", "betweenResultRows")
    completions.foreach { completion =>
      SimpleExternalLookupDataSource.resetCounters()
      val context = TaskContext.empty()
      TaskContext.setTaskContext(context)
      try {
        if (completion == "beforeEval") context.markTaskCompleted(None)
        val rows = rdd.iterator(rdd.partitions.head, context)
        if (completion == "betweenResultRows") rows.next()
        val completeTask = () => context.markTaskCompleted(None)
        completion match {
          case "duringReaderCreation" =>
            SimpleExternalLookupDataSource.beforeReaderReturns.set(completeTask)
          case "duringRequest" =>
            SimpleExternalLookupDataSource.beforeLookupReturns.set(completeTask)
          case _ => completeTask()
        }
        val resultRowsRead = SimpleExternalLookupDataSource.resultRowsRead.get()

        val failure = new AtomicReference[Throwable]()
        val consumer = new Thread(() => {
          try rows.hasNext catch { case error: Throwable => failure.set(error) }
        })
        consumer.start()
        consumer.join()
        // No new reader, request or cursor advance after the close.
        val (readers, requests) = completion match {
          case "beforeEval" | "beforeRequest" => (0, 0)
          case "duringReaderCreation" => (1, 0)
          case _ => (1, 1)
        }
        assert(SimpleExternalLookupDataSource.readerCount.get() === readers)
        assert(SimpleExternalLookupDataSource.requestCount.get() === requests)
        assert(SimpleExternalLookupDataSource.resultRowsRead.get() === resultRowsRead)
        // By the task-end rule of ExternalLookupReader: between the result rows, the close finds
        // the open cursor and closes it before the reader. A request in progress publishes its
        // cursor after the close took the empty slot, so Spark leaves it to the reader's close.
        val closeEvents = completion match {
          case "beforeEval" | "beforeRequest" => Seq.empty
          case "duringReaderCreation" | "duringRequest" => Seq("reader")
          case _ => Seq("cursor", "reader")
        }
        assert(SimpleExternalLookupDataSource.closeEvents.asScala.toSeq === closeEvents)
        // The final metrics are read before the reader is closed, also when the request closes a
        // reader that it created while the task-end close ran.
        assert(lastReaderEvents === (if (readers == 0) Nil else Seq("metrics", "close")))
        checkError(
          exception = failure.get().asInstanceOf[SparkException],
          condition = "INTERNAL_ERROR",
          parameters = Map("message" -> "External lookup reader used after it was closed"))
      } finally {
        TaskContext.unset()
      }
    }
  }

  /**
   * Runs the join of `lookupJoin` over stream row 1 in a task, on a consuming thread, and ends the
   * task on another thread once the consumer has reached `hook`, which then blocks it until
   * `whileBlocked` has run. Returns the consumer's failure.
   */
  private def endTaskWhileConsumerIsIn(
      lookupJoin: ExternalLookupJoinExec,
      hook: AtomicReference[() => Unit],
      departmentsById: Option[Map[Int, Seq[String]]] = None)(
      whileBlocked: Thread => Unit): Throwable = {
    val rdd = lookupJoin
      .copy(child = LocalTableScanExec(lookupJoin.child.output, Seq(InternalRow(1)), None))
      .execute()
    SimpleExternalLookupDataSource.resetCounters()
    departmentsById.foreach(SimpleExternalLookupDataSource.departmentsById.set)
    val reached = new CountDownLatch(1)
    val release = new CountDownLatch(1)
    val first = new AtomicBoolean(true)
    hook.set { () =>
      if (first.getAndSet(false)) {
        reached.countDown()
        assert(release.await(30, TimeUnit.SECONDS))
      }
    }
    val context = TaskContext.empty()
    TaskContext.setTaskContext(context)
    try {
      val rows = rdd.iterator(rdd.partitions.head, context)
      val failure = new AtomicReference[Throwable]()
      val consumer = new Thread(() => {
        try {
          while (rows.hasNext) rows.next()
        } catch {
          case error: Throwable => failure.set(error)
        }
      })
      consumer.start()
      val completion = new Thread(() => context.markTaskCompleted(None))
      try {
        assert(reached.await(10, TimeUnit.SECONDS))
        completion.start()
        whileBlocked(completion)
      } finally {
        release.countDown()
        consumer.join(TimeUnit.SECONDS.toMillis(10))
        completion.join(TimeUnit.SECONDS.toMillis(10))
      }
      // Both threads have ended, so every close the task makes has happened.
      assert(consumer.isAlive === false)
      assert(completion.isAlive === false)
      failure.get()
    } finally {
      TaskContext.unset()
    }
  }

  test("once the join has seen the task end, it makes no connector call") {
    val lookup = spark.read.format(classOf[SimpleExternalLookupDataSource].getName).load()
    val left = streamOf(Seq(1).toDF("id"))
    val lookupJoin = runStream(left.join(lookup, left("id") === lookup("id"))).lookupJoin
    // The consumer is inside the cursor's next() or get(), or in the metrics sample that the
    // 100th result row takes, when the task ends on another thread.
    Seq(
      ("next", SimpleExternalLookupDataSource.beforeNextReturns, None),
      ("get", SimpleExternalLookupDataSource.beforeGetReturns, None),
      ("metrics", SimpleExternalLookupDataSource.beforeMetricsReturn,
        Some(Map(1 -> (1 to 150).map(_.toString))))).foreach {
      case (call, hook, departmentsById) =>
        var metricSamples = 0
        var resultRowsRead = 0
        val failure = endTaskWhileConsumerIsIn(lookupJoin, hook, departmentsById) { completion =>
          // The close does not wait for the call: it closes the cursor, then the reader.
          completion.join(TimeUnit.SECONDS.toMillis(10))
          assert(completion.isAlive === false, call)
          assert(
            SimpleExternalLookupDataSource.closeEvents.asScala.toSeq === Seq("cursor", "reader"))
          metricSamples = SimpleExternalLookupDataSource.metricSamples.get()
          resultRowsRead = SimpleExternalLookupDataSource.resultRowsRead.get()
        }
        checkError(
          exception = failure.asInstanceOf[SparkException],
          condition = "INTERNAL_ERROR",
          parameters = Map("message" -> "External lookup reader used after it was closed"))
        // When the call returns, the consumer sees the close and leaves the cursor and the
        // reader's metrics to it: each is closed once, and no call follows.
        assert(
          SimpleExternalLookupDataSource.closeEvents.asScala.toSeq === Seq("cursor", "reader"))
        assert(SimpleExternalLookupDataSource.metricSamples.get() === metricSamples, call)
        assert(SimpleExternalLookupDataSource.resultRowsRead.get() === resultRowsRead, call)
        assert(SimpleExternalLookupDataSource.rowReadsAfterClose.get() === 0, call)
    }
  }

  test("the task end closes the reader while the join closes a finished cursor") {
    val lookup = spark.read.format(classOf[SimpleExternalLookupDataSource].getName).load()
    val left = streamOf(Seq(1).toDF("id"))
    val lookupJoin = runStream(left.join(lookup, left("id") === lookup("id"))).lookupJoin
    // The consumer has read the cursor to its end, and has taken it to close it, when the task
    // ends on another thread. The close finds no open cursor and closes the reader, and the
    // consumer then closes its cursor, after the reader, as the task-end rule of
    // ExternalLookupReader allows. Each is closed once.
    val failure = endTaskWhileConsumerIsIn(
        lookupJoin, SimpleExternalLookupDataSource.beforeCursorCloses) { completion =>
      completion.join(TimeUnit.SECONDS.toMillis(10))
      assert(completion.isAlive === false)
      assert(SimpleExternalLookupDataSource.closeEvents.asScala.toSeq === Seq("reader"))
    }
    // The consumer had finished its reads, so it makes no connector call after the close.
    assert(failure === null)
    assert(SimpleExternalLookupDataSource.closeEvents.asScala.toSeq === Seq("reader", "cursor"))
  }

  test("a lookup join node canonicalizes each lookup column by its place in the read schema") {
    val lookup = spark.read.format(classOf[SimpleExternalLookupDataSource].getName).load()
    val left = streamOf(Seq(1).toDF("id"))
    val exec = runStream(left.join(lookup,
      left("id") === lookup("id") && left("id") < length(lookup("department")),
      "left_outer")).lookupJoin
    assert(exec.residualCondition.isDefined)
    // The canonical node finds its lookup columns in its read schema.
    assert(exec.canonicalized.output.map(_.dataType) === exec.output.map(_.dataType))
    // Another analysis of the query gives the lookup columns other ids.
    val newIds = AttributeMap(exec.lookupInput.map(attr => attr -> attr.newInstance()))
    val reanalyzed = exec.transformExpressions {
      case attr: Attribute if newIds.contains(attr) => newIds(attr)
    }
    assert(reanalyzed.lookupInput != exec.lookupInput)
    assert(reanalyzed.sameResult(exec))
  }

  test("an aggregate over a filtered lookup join runs") {
    // Exchange reuse canonicalizes the shuffle of the aggregate, with the filter and the lookup
    // join below it, and the filter reads the output of the canonical join.
    val lookup = spark.read.format(classOf[SimpleExternalLookupDataSource].getName).load()
    val swipes = streamOf(Seq((1, "lobby", 10), (2, "garage", 20), (3, "dock", 30),
      (1, "garage", 40)).toDF("employee", "door", "minute"))
    Seq("inner" -> Seq(Row("garage", 40)),
        "left_outer" -> Seq(Row("garage", 40), Row("dock", 30))).foreach {
      case (joinType, expected) =>
        val latestByDoor = swipes.join(lookup, swipes("employee") === lookup("id"), joinType)
          .where(swipes("minute") > 15)
          .groupBy(swipes("door")).agg(max(swipes("minute")))
        QueryTest.sameRows(expected, runStream(latestByDoor, "complete").rows).foreach(fail(_))
    }
    // Also with a lookup-side stage that computes a column, which a filter above the join reads.
    val teams = lookup.select(lookup("id"), upper(lookup("department")).as("team"))
    val latestByTeam = swipes.join(teams, swipes("employee") === teams("id"))
      .where(teams("team") =!= "SALES")
      .groupBy(teams("team")).agg(max(swipes("minute")))
    val run = runStream(latestByTeam, "complete")
    QueryTest.sameRows(Seq(Row("ENGINEERING", 40), Row("RESEARCH", 40)), run.rows)
      .foreach(fail(_))
    assert(run.lookupJoin.lookupTransforms.nonEmpty)
    // The canonical node finds its lookup columns among the stages' outputs.
    assert(run.lookupJoin.canonicalized.output.map(_.dataType) ===
      run.lookupJoin.output.map(_.dataType))
  }

  test("the command check walks each subquery of a command once") {
    // Ten levels of IN subqueries over ordinary tables. A walk of both a node's subqueries and its
    // inner children, which default to them, would visit the innermost plan 1024 times.
    val innermost = InnerChildrenCountingRelation(Seq(AttributeReference("v", IntegerType)()))
    var query: DataFrame = Dataset.ofRows(spark, innermost)
    (1 to 10).foreach { _ => query = Seq(1).toDF("v").where(col("v").isin(query)) }
    val command = SaveIntoDataSourceCommand(
      query.queryExecution.analyzed, new RecordingV1WriteSource, Map.empty, SaveMode.Overwrite)
    InnerChildrenCountingRelation.calls.set(0)
    TableCapabilityCheck(command)
    assert(InnerChildrenCountingRelation.calls.get() === 1)
  }

  test("an external lookup join matches each offered key type exactly") {
    // For each type: the stored keys, and a key that is not stored. Each request holds the two
    // distinct keys of three stream rows, and the source also returns a row with another key for
    // each, which Spark drops when it checks the selected key.
    Seq(
      ("BIGINT", Seq("10000000000", "20000000000"), "30000000000"),
      ("SMALLINT", Seq("1", "2"), "3"),
      ("TINYINT", Seq("1", "2"), "3"),
      ("BOOLEAN", Seq("true"), "false"),
      ("DECIMAL(10, 4)", Seq("1.2345", "2.5"), "1.2346"),
      ("DATE", Seq("DATE'2026-01-01'", "DATE'2026-01-02'"), "DATE'2026-01-03'"),
      ("TIMESTAMP", Seq("TIMESTAMP'2026-01-01 10:00:00.000001'",
        "TIMESTAMP'2026-01-01 10:00:00.000002'"), "TIMESTAMP'2026-01-01 10:00:00.000003'"),
      ("TIMESTAMP_NTZ", Seq("TIMESTAMP_NTZ'2026-01-01 10:00:00.000001'",
        "TIMESTAMP_NTZ'2026-01-01 10:00:00.000002'"), "TIMESTAMP_NTZ'2026-01-01 10:00:00.000003'"),
      ("BINARY", Seq("X'0102'", "X'010203'"), "X'01'"),
      // VARCHAR compares as STRING unless standard CHAR/VARCHAR semantics is on.
      ("VARCHAR(8)", Seq("'ab'", "'abc'"), "'abd'")).foreach {
      case (keyType, stored, missing) =>
        val dataType = DataType.fromDDL(keyType)
        def internalValue(literal: String): Any =
          sql(s"SELECT CAST($literal AS $keyType)").queryExecution.executedPlan.executeCollect()
            .head.get(0, dataType)
        TypedKeyExternalLookupTable.storedRows.set(
          stored.map(internalValue).zip(Seq("first", "second")))
        TypedKeyExternalLookupTable.otherKeys.set(Seq(internalValue(missing)))
        TypedKeyExternalLookupTable.requestSizes.clear()
        val events = streamOf(sql(s"SELECT CAST(key AS $keyType) AS key, tag FROM VALUES " +
          s"(${stored.head}, 'a'), (${stored.head}, 'b'), ($missing, 'c') AS t(key, tag)"))
        val lookup = spark.read
          .format(classOf[TypedKeyExternalLookupDataSource].getName)
          .option("keyType", keyType)
          .load()
        checkStream(
          events.join(lookup, events("key") === lookup("key"), "left_outer")
            .select(events("tag"), lookup("payload")),
          Seq(Row("a", "first"), Row("b", "first"), Row("c", null)))
        assert(TypedKeyExternalLookupTable.requestSizes.asScala.toSeq === Seq(2), keyType)
    }
  }

  test("external lookup copies a value row that the connector reuses with its buffers") {
    val lookup = spark.read.format(classOf[ReusedRowExternalLookupDataSource].getName).load()
    val left = streamOf(Seq(1, 2, 1).toDF("id"))
    def payload(byte: Int): Array[Byte] = Array.fill[Byte](4)(byte.toByte)
    val matchesOfOne = Seq(Row(1, 1, "alpha", payload(1)), Row(1, 1, "beta", payload(2)))
    checkStream(
      left.join(lookup, left("id") === lookup("id")),
      matchesOfOne ++ matchesOfOne :+ Row(2, 2, "gamma", payload(3)))
  }

  test("task completion closes the reader without waiting for the request in progress") {
    val lookup = spark.read.format(classOf[SimpleExternalLookupDataSource].getName).load()
    val left = streamOf(Seq(1).toDF("id"))
    val lookupJoin = runStream(left.join(lookup, left("id") === lookup("id"))).lookupJoin
    val rdd = lookupJoin
      .copy(child = LocalTableScanExec(lookupJoin.child.output, Seq(InternalRow(1)), None))
      .execute()
    SimpleExternalLookupDataSource.resetCounters()
    val responded = new CountDownLatch(1)
    SimpleExternalLookupDataSource.beforeLookupReturns.set { () =>
      assert(responded.await(30, TimeUnit.SECONDS))
    }
    val context = TaskContext.empty()
    TaskContext.setTaskContext(context)
    try {
      val rows = rdd.iterator(rdd.partitions.head, context)
      val failure = new AtomicReference[Throwable]()
      // Another thread drives the join, and waits in the connector for the response.
      val consumer = new Thread(() => {
        try rows.hasNext catch { case error: Throwable => failure.set(error) }
      })
      consumer.start()
      try {
        assert(SimpleExternalLookupDataSource.requestStarted.get().await(10, TimeUnit.SECONDS))
        val metricSamples = SimpleExternalLookupDataSource.metricSamples.get()
        val completion = new Thread(() => context.markTaskCompleted(None))
        completion.start()
        completion.join(TimeUnit.SECONDS.toMillis(10))
        assert(completion.isAlive === false)
        // The task end read the reader's metrics and closed it while the request was running.
        assert(SimpleExternalLookupDataSource.metricSamples.get() > metricSamples)
        assert(SimpleExternalLookupDataSource.closeEvents.asScala.toSeq === Seq("reader"))
        assert(lastReaderEvents === Seq("metrics", "close"))
      } finally {
        responded.countDown()
        consumer.join()
      }
      checkError(
        exception = failure.get().asInstanceOf[SparkException],
        condition = "INTERNAL_ERROR",
        parameters = Map("message" -> "External lookup reader used after it was closed"))
      // The request publishes its cursor after the close took the empty slot, so Spark leaves it
      // to the reader's close, as the task-end rule of ExternalLookupReader allows.
      assert(SimpleExternalLookupDataSource.closeEvents.asScala.toSeq === Seq("reader"))
      assert(SimpleExternalLookupDataSource.resultRowsRead.get() === 0)
    } finally {
      TaskContext.unset()
    }
  }

  test("external lookup failures propagate from outer joins instead of becoming misses") {
    val lookup = spark.read
      .format(classOf[SimpleExternalLookupDataSource].getName)
      .option("failAt", "request")
      .load()
    val left = streamOf(Seq(1, 3).toDF("id"))

    Seq("left_outer", "left_anti").foreach { joinType =>
      SimpleExternalLookupDataSource.resetCounters()
      val failure = streamFailure[SparkException](
        left.join(lookup, left("id") === lookup("id"), joinType))
      assert(failure.getMessage.contains("injected external lookup request failure"))
    }
  }

  test("external lookup bounds each request by the engine batch size") {
    SimpleExternalLookupDataSource.resetCounters()
    val lookup = spark.read
      .format(classOf[SimpleExternalLookupDataSource].getName)
      .option("maxBatchSize", "5000")
      .load()
    val left = streamOf((1 to 1500).toDF("id"))

    assert(runStream(left.join(lookup, left("id") === lookup("id"), "left_semi")).rows.size === 2)
    assert(SimpleExternalLookupDataSource.requestSizes.asScala.toSeq === Seq(1000, 500))
  }

  test("external lookup caps each inner-join request at the engine batch size") {
    val lookup = spark.read
      .format(classOf[SimpleExternalLookupDataSource].getName)
      .option("maxBatchSize", "5000")
      .load()
    val left = streamOf((1 to 1500).toDF("id"))

    assert(runStream(left.join(lookup, left("id") === lookup("id"))).rows.size === 3)
    assert(SimpleExternalLookupDataSource.requestSizes.asScala.toSeq === Seq(1000, 500))
  }

  test("an inner or left semi join batches only the stream rows that can match") {
    val lookup = spark.read
      .format(classOf[SimpleExternalLookupDataSource].getName)
      .option("maxBatchSize", "5000")
      .load()
    // One badge swipe in 100 carries an employee id. An inner or left semi join emits nothing for
    // the others, so its batches leave them out, and one request serves the 30 swipes with an id.
    // A left outer or left anti join emits them, so its batches of 1000 rows keep them, and each
    // sends a request.
    val swipes = streamOf((1 to 3000).map { swipe =>
      (swipe, if (swipe % 100 == 0) Some(swipe / 100 % 2 + 1) else None)
    }.toDF("swipe", "employee"))
    Seq("inner" -> (45, 1), "left_semi" -> (30, 1), "left_outer" -> (3015, 3),
        "left_anti" -> (2970, 3)).foreach {
      case (joinType, (outputRows, requests)) =>
        SimpleExternalLookupDataSource.resetCounters()
        val run = runStream(swipes.join(lookup, swipes("employee") === lookup("id"), joinType))
        assert(run.rows.size === outputRows, joinType)
        assert(SimpleExternalLookupDataSource.requestCount.get() === requests, joinType)
        assert(run.lookupJoin.metrics("numInputRows").value === 3000, joinType)
    }
  }

  test("the lookup join rules cannot be excluded") {
    // Every lookup join would fail without them: as a batch scan of a lookup-only table without
    // the formation, and with no strategy for the unprepared join without the preparation.
    val lookup = spark.read.format(classOf[SimpleExternalLookupDataSource].getName).load()
    val left = streamOf(Seq(1, 3).toDF("id"))
    val rules = Seq(FormExternalLookupJoin.ruleName, PrepareExternalLookupJoin.ruleName)
    withSQLConf(SQLConf.OPTIMIZER_EXCLUDED_RULES.key -> rules.mkString(",")) {
      val run = runStream(left.join(lookup, left("id") === lookup("id")))
      assert(run.plan.exists(_.isInstanceOf[PreparedExternalLookupJoin]))
      QueryTest.sameRows(Seq(Row(1, 1, "engineering"), Row(1, 1, "research")), run.rows)
        .foreach(fail(_))
    }
  }

  test("a join over a dual-capability table without the hint is not searched for lookup joins") {
    // The relation of a lookup-only table and the EXTERNAL_LOOKUP hint each carry a tree pattern,
    // so the analysis check, the formation and the preparation leave a plan with neither alone,
    // without asking its tables for their capabilities.
    val dual = spark.read.format(classOf[DualCapabilityExternalLookupDataSource].getName).load()
    val swipes = streamOf(Seq((1, "lobby")).toDF("employee", "door"))
    val batch = dual.as("a").join(dual.as("b"), col("a.id") === col("b.id"))
      .select(col("a.id"), col("b.department"))
    val withSubquery = batch.where(col("a.id") <= dual.select(max("id")).scalar())
    val stream = swipes.join(dual, swipes("employee") === dual("id"))
    Seq(batch.queryExecution.analyzed, withSubquery.queryExecution.analyzed,
        runStream(stream).execution.analyzed).foreach { plan =>
      // Computes the pattern bits, with which each relation asks its table once.
      assert(plan.containsPattern(LOOKUP_ONLY_RELATION) === false)
      DualCapabilityExternalLookupTable.capabilityCalls.set(0)
      CheckExternalLookupJoin(plan)
      assert(FormExternalLookupJoin(plan) eq plan)
      assert(PrepareExternalLookupJoin(plan) eq plan)
      assert(DualCapabilityExternalLookupTable.capabilityCalls.get() === 0)
    }
    checkAnswer(batch, Seq(Row(1, "engineering"), Row(1, "engineering"), Row(1, "research"),
      Row(1, "research"), Row(2, "sales")))
    // A lookup-only relation that no lookup join reads still fails as a batch scan when the
    // lookup joins are prepared, alone and next to a lookup join.
    def lookupOnly: DataFrame =
      spark.read.format(classOf[SimpleExternalLookupDataSource].getName).load()
    val lookup = lookupOnly
    val lookupJoin = FormExternalLookupJoin(
      swipes.join(lookup, swipes("employee") === lookup("id")).queryExecution.analyzed)
    assert(lookupJoin.exists(_.isInstanceOf[ExternalLookupJoin]))
    val unread = lookupOnly.queryExecution.analyzed
    Seq(unread, Join(lookupJoin, unread, Inner, None, JoinHint.NONE)).foreach { plan =>
      checkError(
        exception = intercept[AnalysisException] { PrepareExternalLookupJoin(plan) },
        condition = "UNSUPPORTED_FEATURE.TABLE_OPERATION",
        parameters = Map(
          "tableName" -> "`SimpleExternalLookupTable`",
          "operation" -> "batch scan"))
    }
  }

  test("external lookup sends the requests of a task through one reader") {
    val lookup = spark.read
      .format(classOf[SimpleExternalLookupDataSource].getName)
      .option("maxBatchSize", "1")
      .load()
    val left = streamOf(Seq(1, 2).toDF("id"))
    SimpleExternalLookupDataSource.afterParameterRowsRead.set { _ =>
      SimpleExternalLookupDataSource.closeEvents.add("request")
      ()
    }
    checkStream(
      left.join(lookup, left("id") === lookup("id")),
      Seq(Row(1, 1, "engineering"), Row(1, 1, "research"), Row(2, 2, "sales")))
    assert(SimpleExternalLookupDataSource.readerCount.get() === 1)
    // Each cursor is closed before the next request, and the reader after the last cursor.
    assert(SimpleExternalLookupDataSource.closeEvents.asScala.toSeq ===
      Seq("request", "cursor", "request", "cursor", "reader"))
  }

  test("external lookup caps the size of the input rows that one request holds") {
    val lookup = spark.read.format(classOf[SimpleExternalLookupDataSource].getName).load()
    // Each event carries a note of 1000 characters, so its row takes 1024 bytes.
    val note = "n" * 1000
    val left = streamOf((1 to 7).map(id => (id, note)).toDF("id", "note"))
    val joined = left.join(lookup, left("id") === lookup("id"))
    val expected =
      Seq(Row(1, note, 1, "engineering"), Row(1, note, 1, "research"), Row(2, note, 2, "sales"))
    // A request takes rows until they reach the limit, and always takes one.
    Seq(
      None -> Seq(7),
      Some("2500") -> Seq(3, 3, 1),
      Some("1") -> Seq.fill(7)(1)).foreach { case (maxBatchBytes, requestSizes) =>
      SimpleExternalLookupDataSource.resetCounters()
      val conf = maxBatchBytes.map(SQLConf.JOIN_EXTERNAL_LOOKUP_MAX_BATCH_BYTES.key -> _).toSeq
      withSQLConf(conf: _*) {
        checkStream(joined, expected)
      }
      assert(SimpleExternalLookupDataSource.requestSizes.asScala.toSeq === requestSizes)
    }
  }

  test("external lookup keeps the partitioning of its stream side") {
    val lookup = spark.read.format(classOf[SimpleExternalLookupDataSource].getName).load()
    val events = streamOf(Seq(1, 1, 2).toDF("id"), 2).dropDuplicates("id")
    val counts = events.join(lookup, events("id") === lookup("id")).groupBy(events("id")).count()
    val run = runStream(counts, outputMode = "complete")
    QueryTest.sameRows(Seq(Row(1, 2), Row(2, 1)), run.rows).foreach(fail(_))
    // The aggregation groups by the key that the deduplication shuffled on, so it needs no
    // shuffle of its own.
    val plan = run.execution.executedPlan
    assert(plan.exists(_.isInstanceOf[ExternalLookupJoinExec]))
    assert(plan.collect { case shuffle: ShuffleExchangeExec => shuffle }.size === 1)
  }

  test("external lookup does not create readers without requestable keys") {
    val lookup = spark.read.format(classOf[SimpleExternalLookupDataSource].getName).load()
    val empty = streamOf(Seq.empty[java.lang.Integer].toDF("id"))
    val nullOnly = streamOf(Seq[java.lang.Integer](null).toDF("id"))

    Seq(empty, nullOnly).foreach { left =>
      SimpleExternalLookupDataSource.resetCounters()
      runStream(left.join(
        lookup, left("id") === lookup("id"), "left_outer"))
      assert(SimpleExternalLookupDataSource.readerCount.get() === 0)
      assert(SimpleExternalLookupDataSource.closeCount.get() === 0)
    }
  }

  test("external lookup cleanup attempts every resource and preserves the primary failure") {
    val attempted = mutable.ArrayBuffer.empty[String]
    val cursorFailure = new IllegalStateException("cursor close")
    val metricsFailure = new IllegalStateException("metrics")
    val readerFailure = new IllegalStateException("reader close")

    val failure = intercept[IllegalStateException] {
      ExternalLookupJoinExec.closeResources(
        { attempted += "cursor"; throw cursorFailure },
        { attempted += "metrics"; throw metricsFailure },
        { attempted += "reader"; throw readerFailure })
    }
    assert(failure eq cursorFailure)
    assert(attempted === Seq("cursor", "metrics", "reader"))
    assert(failure.getSuppressed.toSeq === Seq(metricsFailure))
    assert(metricsFailure.getSuppressed.toSeq === Seq(readerFailure))

    val lookupFailure = new IllegalStateException("lookup")
    val cleanupFailure = new IllegalStateException("cleanup")
    val preserved = intercept[IllegalStateException] {
      org.apache.spark.util.Utils.tryWithSafeFinally(throw lookupFailure) {
        ExternalLookupJoinExec.closeResources(throw cleanupFailure, (), ())
      }
    }
    assert(preserved eq lookupFailure)
    assert(preserved.getSuppressed.toSeq === Seq(cleanupFailure))
  }

  test("external lookup responds to task cancellation and closes the reader") {
    val lookup = spark.read
      .format(classOf[SimpleExternalLookupDataSource].getName)
      .option("responseDelayMillis", "60000")
      .load()
    val left = streamOf(Seq(1).toDF("id"))
    // Stopping the query cancels the task that waits for the response.
    val query = left.join(lookup, left("id") === lookup("id"))
      .writeStream.format("memory").queryName("external_lookup_cancellation").start()
    try {
      assert(SimpleExternalLookupDataSource.requestStarted.get().await(10, TimeUnit.SECONDS))
    } finally {
      query.stop()
      spark.catalog.dropTempView("external_lookup_cancellation")
    }
    assert(SimpleExternalLookupDataSource.readerCount.get() === 1)
    eventually(timeout(10.seconds)) {
      assert(SimpleExternalLookupDataSource.closeCount.get() === 1)
    }
  }

  test("external lookup join reads the current lookup values in every micro-batch") {
    val input = MemoryStream[Int]
    val left = input.toDF().toDF("id")
    // The connector changes the value of key 2 after each request.
    val lookup = spark.read
      .format(classOf[SimpleExternalLookupDataSource].getName)
      .option("mutateAfterRequest", "true")
      .load()
    val joined = left.join(lookup, left("id") === lookup("id"), "left_outer")
    val queryName = "external_lookup_mbm"
    val query = joined.writeStream.format("memory").queryName(queryName).start()
    try {
      input.addData(1, 2, 3)
      query.processAllAvailable()
      checkAnswer(
        spark.table(queryName),
        Seq(Row(1, 1, "engineering"), Row(1, 1, "research"),
          Row(2, 2, "sales"), Row(3, null, null)))
      input.addData(2)
      query.processAllAvailable()
      checkAnswer(
        spark.table(queryName),
        Seq(Row(1, 1, "engineering"), Row(1, 1, "research"),
          Row(2, 2, "sales"), Row(3, null, null), Row(2, 2, "sales-v2")))
    } finally {
      query.stop()
      spark.catalog.dropTempView(queryName)
    }
  }

  test("a stream over a versioned lookup table keeps running after the table gains a column") {
    // A micro-batch loads a versioned table again, and when the table has gained a column or a
    // nested field, a projection to the analyzed columns sits above the relation. The catalog has
    // a name of its own, as the session keeps the catalog that it loaded under a name.
    withSQLConf(
        "spark.sql.catalog.evolving_lookup_cat" ->
          classOf[EvolvingExternalLookupTestCatalog].getName) {
      for {
        (table, hint) <- Seq("lookup" -> "", "dual" -> "/*+ EXTERNAL_LOOKUP(r) */")
        change <- Seq("column", "nestedField")
        joinType <- Seq("JOIN", "LEFT JOIN")
      } {
        EvolvingExternalLookupTable.schemaChange.set("none")
        val input = MemoryStream[Int]
        withTempView("badge_swipes") {
          input.toDF().toDF("employee").createOrReplaceTempView("badge_swipes")
          val queryName = s"external_lookup_stream_${streamRunCount.incrementAndGet()}"
          val query = sql(
            s"""SELECT $hint s.employee, r.info
               |FROM badge_swipes s $joinType evolving_lookup_cat.ns.$table r
               |  ON s.employee = r.id""".stripMargin)
            .writeStream.format("memory").queryName(queryName).start()
          try {
            input.addData(1, 3)
            query.processAllAvailable()
            EvolvingExternalLookupTable.schemaChange.set(change)
            input.addData(2, 4)
            query.processAllAvailable()
            val clue = s"$table, $change, $joinType"
            val execution = query.asInstanceOf[StreamingQueryWrapper].streamingQuery.lastExecution
            assert(execution.executedPlan.exists(_.isInstanceOf[ExternalLookupJoinExec]), clue)
            val matched = Seq(Row(1, Row("pro")), Row(2, Row("basic")))
            val unmatched = if (joinType == "JOIN") Nil else Seq(Row(3, null), Row(4, null))
            QueryTest.sameRows(matched ++ unmatched, spark.table(queryName).collect().toSeq)
              .foreach(error => fail(s"$clue: $error"))
          } finally {
            query.stop()
            spark.catalog.dropTempView(queryName)
          }
        }
      }
    }
  }
}

/**
 * A catalog serving `ns.lookup` (lookup-only), `ns.dual` (dual-capability), `ns.versioned`
 * (lookup-only, with a `_version` metadata column), `ns.path_lookup` and `ns.path_dual`, the
 * first two under a name that is a file path, and `ns.multipart_lookup`, the first under a name
 * that parses as a multipart identifier.
 */
class ExternalLookupTestCatalog extends TableCatalog {
  private var catalogName: String = _

  override def initialize(name: String, options: CaseInsensitiveStringMap): Unit =
    catalogName = name
  override def name(): String = catalogName
  override def listTables(namespace: Array[String]): Array[Identifier] =
    Array("lookup", "dual", "versioned", "path_lookup", "path_dual", "multipart_lookup")
      .map(Identifier.of(Array("ns"), _))
  override def loadTable(ident: Identifier): Table = (ident.namespace().toSeq, ident.name()) match {
    case (Seq("ns"), "lookup") => SimpleExternalLookupTable
    case (Seq("ns"), "dual") => DualCapabilityExternalLookupTable
    case (Seq("ns"), "versioned") => VersionedExternalLookupTable
    case (Seq("ns"), "path_lookup") =>
      RenamedExternalLookupTable(SimpleExternalLookupTable, "./users")
    case (Seq("ns"), "path_dual") =>
      RenamedExternalLookupTable(DualCapabilityExternalLookupTable, "./users")
    case (Seq("ns"), "multipart_lookup") =>
      RenamedExternalLookupTable(SimpleExternalLookupTable, "ns.users")
    case _ => throw new NoSuchTableException(ident.namespace().mkString("."), ident.name())
  }
  override def alterTable(ident: Identifier, changes: TableChange*): Table =
    throw new UnsupportedOperationException()
  override def dropTable(ident: Identifier): Boolean = throw new UnsupportedOperationException()
  override def renameTable(oldIdent: Identifier, newIdent: Identifier): Unit =
    throw new UnsupportedOperationException()
}

/**
 * Lookup joins in micro-batches that run at the times of a manual clock: on 1970-01-02 and
 * 1970-01-03, after the query starts on 1970-01-01. The only stored rate is valid from 1970-01-03,
 * so it is valid in the second micro-batch, by time and by date, and in neither at the start.
 */
@DataSourceV2Test
class ExternalLookupJoinBatchTimeSuite extends StreamTest {
  import testImplicits._

  private val day = TimeUnit.DAYS.toMillis(1)

  override def beforeEach(): Unit = {
    super.beforeEach()
    ValidityExternalLookupTable.validFrom.set(TimeUnit.DAYS.toMicros(2))
  }

  override def afterEach(): Unit = {
    try ValidityExternalLookupTable.validFrom.set(0L) finally super.afterEach()
  }

  // A filter that keeps a rate valid at the current time, and one that keeps it at the current
  // date, with the rows of the first and the second micro-batch, also for its negation.
  private def validNowCases: Seq[(Column, Row, Row)] = Seq(
    $"validFrom" <= current_timestamp(),
    $"validFrom".cast("date") <= current_date()).flatMap { validNow =>
    Seq((validNow, Row(1, null), Row(1, 1)), (!validNow, Row(1, 1), Row(1, null)))
  }

  /**
   * Runs `joined` in two micro-batches, a day apart, each after `addRow`, the first of them the
   * micro-batch `firstBatchId`.
   */
  private def checkBatches(
      joined: DataFrame,
      addRow: StreamAction,
      first: Row,
      second: Row,
      firstBatchId: Long = 0): Unit = {
    val clock = new StreamManualClock
    testStream(joined)(
      StartStream(Trigger.ProcessingTime(day), triggerClock = clock),
      addRow,
      AdvanceManualClock(day),
      WaitUntilBatchProcessed(firstBatchId),
      CheckNewAnswer(first),
      addRow,
      AdvanceManualClock(day),
      WaitUntilBatchProcessed(firstBatchId + 1),
      CheckNewAnswer(second))
  }

  test("a lookup-side filter reads the current time of each micro-batch") {
    // MicroBatchExecution gives current_timestamp() and current_date() the time of the batch, as
    // on a static side.
    withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> "UTC") {
      val rates = spark.read.format(classOf[ValidityExternalLookupDataSource].getName).load()
      validNowCases.foreach { case (filter, first, second) =>
        val input = MemoryStream[Int]
        val events = input.toDF().toDF("id")
        val current = rates.where(filter)
        checkBatches(
          events.join(current, events("id") === current("id"), "left_outer")
            .select(events("id"), current("id")),
          AddData(input, 1), first, second)
      }
    }
  }

  test("a lookup-side filter reads the current time of each micro-batch when the scan is pruned") {
    // The pruning of the stream's scan optimizes the query at its start with placeholders for the
    // current time, but not on the lookup sides, which it keeps whole, so the start forms the
    // lookup join, as each micro-batch does.
    withSQLConf(
        SQLConf.SESSION_LOCAL_TIMEZONE.key -> "UTC",
        "spark.sql.catalog.stream_cat" -> classOf[InMemoryTableCatalog].getName,
        SQLConf.STREAMING_V2_COLUMN_PRUNING_ENABLED.key -> "true") {
      val rates = spark.read.format(classOf[ValidityExternalLookupDataSource].getName).load()
      // Inserts an event while no trigger runs, as AddData adds data, so that the event goes into
      // the micro-batch of the next AdvanceManualClock.
      val insertEvent = Execute("insert an event") { query =>
        val clock = query.triggerClock.asInstanceOf[StreamManualClock]
        eventually(timeout(streamingTimeout)) {
          assert(clock.isStreamWaitingAt(clock.getTimeMillis()))
        }
        sql("INSERT INTO stream_cat.ns.events VALUES (1, 'lobby')")
      }
      validNowCases.foreach { case (filter, first, second) =>
        withTable("stream_cat.ns.events") {
          sql("CREATE TABLE stream_cat.ns.events (id INT, note STRING) " +
            s"TBLPROPERTIES ('${InMemoryBaseTable.STREAMING_COLUMN_PRUNING}' = 'true')")
          val events = spark.readStream.table("stream_cat.ns.events")
          val current = rates.where(filter)
          // The stream starts with a micro-batch over the empty table, as the table's offsets
          // begin at 0, so the events come in the micro-batches after it.
          checkBatches(
            events.join(current, events("id") === current("id"), "left_outer")
              .select(events("id"), current("id")),
            insertEvent, first, second, firstBatchId = 1)
        }
      }
    }
  }
}

@DataSourceV2Test
class ExternalLookupJoinRealTimeModeSuite extends StreamRealTimeModeE2ESuiteBase {
  import testImplicits._

  test("real-time mode stops a lookup join that cannot be planned when the query starts") {
    val input = LowLatencyMemoryStream[Long](2)
    val left = input.toDF().toDF("id")
    val lookup = spark.read.format(classOf[SimpleExternalLookupDataSource].getName).load()
    val query = runStreamingQuery("external_lookup_rtm_startup",
      left.join(lookup, left("id") === lookup("id")).select(col("department")))
    try {
      eventually(timeout(60.seconds)) {
        checkError(
          exception = query.exception.get.getCause.asInstanceOf[AnalysisException],
          condition = "EXTERNAL_LOOKUP_JOIN_UNSUPPORTED.NO_LOOKUP_CANDIDATE",
          parameters = Map("table" -> "`SimpleExternalLookupTable`"))
      }
    } finally {
      query.stop()
    }
  }

  test("real-time mode rejects external lookup join") {
    SimpleExternalLookupDataSource.resetCounters()
    val input = LowLatencyMemoryStream[Int](2)
    val left = input.toDF().toDF("id")
    val lookup = spark.read.format(classOf[SimpleExternalLookupDataSource].getName).load()
    val joined = left.join(lookup, left("id") === lookup("id"))
      .select(concat(col("department"), lit("-"), left("id").cast("string")).as("output"))

    val query = runStreamingQuery("external_lookup_rtm", joined)
    try {
      eventually(timeout(60.seconds)) {
        checkError(
          exception = query.exception.get.getCause.asInstanceOf[SparkIllegalArgumentException],
          condition = "STREAMING_REAL_TIME_MODE.OPERATOR_OR_SINK_NOT_IN_ALLOWLIST",
          parameters = Map(
            "errorType" -> "operator",
            "message" -> "org.apache.spark.sql.execution.joins.ExternalLookupJoinExec is"))
      }
    } finally {
      query.stop()
    }
    assert(SimpleExternalLookupDataSource.readerCount.get() === 0)
  }
}

class SimpleExternalLookupDataSource extends TableProvider {
  override def inferSchema(options: CaseInsensitiveStringMap): StructType =
    SimpleExternalLookupDataSource.outputSchema

  override def getTable(
      schema: StructType,
      partitioning: Array[Transform],
      properties: java.util.Map[String, String]): Table = SimpleExternalLookupTable
}

class DualCapabilityExternalLookupDataSource extends TableProvider {
  override def inferSchema(options: CaseInsensitiveStringMap): StructType =
    SimpleExternalLookupDataSource.outputSchema

  override def getTable(
      schema: StructType,
      partitioning: Array[Transform],
      properties: java.util.Map[String, String]): Table = DualCapabilityExternalLookupTable
}

/** Serves a lookup-only table, and has a V1 read path, which Spark rejects. */
class RelationProviderExternalLookupDataSource
  extends SimpleExternalLookupDataSource with RelationProvider {
  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String]): BaseRelation =
    throw new IllegalStateException("The V1 read path is not used")
}

/** Serves a lookup-only table, and has a V1 read path, which Spark rejects. */
class SchemaRelationProviderExternalLookupDataSource
  extends SimpleExternalLookupDataSource with SchemaRelationProvider {
  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String],
      schema: StructType): BaseRelation =
    throw new IllegalStateException("The V1 read path is not used")
}

/** Serves a lookup-only table, and has a V1 file read path, which Spark rejects. */
class FileFormatExternalLookupDataSource extends ParquetFileFormat with TableProvider {
  // Not a name in the list of sources that are always read through V1.
  override def shortName(): String = "file_format_external_lookup"

  override def inferSchema(options: CaseInsensitiveStringMap): StructType =
    SimpleExternalLookupDataSource.outputSchema

  override def getTable(
      schema: StructType,
      partitioning: Array[Transform],
      properties: java.util.Map[String, String]): Table = SimpleExternalLookupTable
}

/** Serves a lookup-only table, and has a V1 file read path through its fallback format. */
class FileDataSourceV2ExternalLookupDataSource extends FileDataSourceV2 {
  override def fallbackFileFormat: Class[_ <: FileFormat] = classOf[ParquetFileFormat]
  // Not a name in the list of sources that are always read through V1.
  override def shortName(): String = "file_v2_external_lookup"
  override protected def getTable(options: CaseInsensitiveStringMap): Table =
    SimpleExternalLookupTable
  override protected def getTable(options: CaseInsensitiveStringMap, schema: StructType): Table =
    SimpleExternalLookupTable
}

/** A V1 source that records the writes that it receives. */
class RecordingV1WriteSource extends SchemaRelationProvider with CreatableRelationProvider {
  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String],
      schema: StructType): BaseRelation = RecordingV1Relation(schema)(sqlContext)

  override def createRelation(
      sqlContext: SQLContext,
      mode: SaveMode,
      parameters: Map[String, String],
      data: DataFrame): BaseRelation = {
    RecordingV1WriteSource.writes.add(s"save $mode")
    RecordingV1Relation(data.schema)(sqlContext)
  }
}

object RecordingV1WriteSource {
  val writes = new ConcurrentLinkedQueue[String]()
}

case class RecordingV1Relation(schema: StructType)(@transient val sqlContext: SQLContext)
  extends BaseRelation with InsertableRelation {
  override def insert(data: DataFrame, overwrite: Boolean): Unit = {
    RecordingV1WriteSource.writes.add(s"insert overwrite=$overwrite")
  }
}

object DualCapabilityExternalLookupTable extends Table with SupportsRead {
  // The calls of capabilities(), which a search for lookup-only tables makes.
  val capabilityCalls = new AtomicInteger()

  override def name(): String = "DualCapabilityExternalLookupTable"
  override def schema(): StructType = SimpleExternalLookupDataSource.outputSchema
  override def capabilities(): java.util.Set[TableCapability] = {
    capabilityCalls.incrementAndGet()
    java.util.EnumSet.of(BATCH_READ, EXTERNAL_LOOKUP)
  }
  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder =
    SimpleExternalLookupTable.newScanBuilder(options)
}

/** `table` under another name, such as the path of its files, as `Table.name()` allows. */
case class RenamedExternalLookupTable(table: Table with SupportsRead, tableName: String)
  extends Table with SupportsRead {
  override def name(): String = tableName
  override def schema(): StructType = table.schema()
  override def capabilities(): java.util.Set[TableCapability] = table.capabilities()
  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder =
    table.newScanBuilder(options)
}

/** A lookup-only table that also exposes the version of each row as a metadata column. */
object VersionedExternalLookupTable extends Table with SupportsRead with SupportsMetadataColumns {
  override def name(): String = "VersionedExternalLookupTable"
  override def schema(): StructType = SimpleExternalLookupDataSource.outputSchema
  override def capabilities(): java.util.Set[TableCapability] =
    java.util.EnumSet.of(EXTERNAL_LOOKUP)
  override def metadataColumns(): Array[MetadataColumn] = Array(new MetadataColumn {
    override def name(): String = "_version"
    override def dataType(): DataType = IntegerType
  })
  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder =
    SimpleExternalLookupTable.newScanBuilder(options)
}

object SimpleExternalLookupDataSource {
  val requestCount = new AtomicInteger()
  val readerCount = new AtomicInteger()
  val closeCount = new AtomicInteger()
  val requestStarted = new AtomicReference(new CountDownLatch(1))
  val requiredColumns = new AtomicReference(Seq.empty[String])
  val pushedPredicateCount = new AtomicInteger()
  val pushedPredicateFields = new AtomicReference(Seq.empty[String])
  val requestSizes = new ConcurrentLinkedQueue[Int]()
  val requestedKeys = new ConcurrentLinkedQueue[Int]()
  val nullKeyRequests = new AtomicInteger(0)
  val resultRowsRead = new AtomicInteger(0)
  val metricSamples = new AtomicInteger(0)
  val closeEvents = new ConcurrentLinkedQueue[String]()
  // The metric reads and the closes of the readers, in order.
  val readerEvents = new ConcurrentLinkedQueue[String]()
  val beforeReaderReturns = new AtomicReference[() => Unit](() => ())
  val beforeLookupReturns = new AtomicReference[() => Unit](() => ())
  // Called with the parameter rows of each request, after the results are read.
  val afterParameterRowsRead = new AtomicReference[Array[InternalRow] => Unit](_ => ())
  // Called when a cursor reaches its end, before it reports it.
  val beforeCursorEnds = new AtomicReference[() => Unit](() => ())
  // Called when a cursor is closed, before the close is recorded.
  val beforeCursorCloses = new AtomicReference[() => Unit](() => ())
  // Called when a cursor has advanced to a row, before next() returns.
  val beforeNextReturns = new AtomicReference[() => Unit](() => ())
  // Called when the join reads a row with get(), before it returns.
  val beforeGetReturns = new AtomicReference[() => Unit](() => ())
  // The get() and inputOrdinal() calls on a closed cursor.
  val rowReadsAfterClose = new AtomicInteger(0)
  // Called when Spark reads the reader's metrics, before they are returned.
  val beforeMetricsReturn = new AtomicReference[() => Unit](() => ())
  val lookupCandidates =
    new AtomicReference(Seq.empty[(String, String, DataType)])
  val lookupCandidateDescriptions = new AtomicReference(Seq.empty[String])
  val outputSchema: StructType = new StructType()
    .add("id", "int", nullable = true)
    .add("department", "string", nullable = false)
  private val INITIAL_DEPARTMENTS_BY_ID: Map[Int, Seq[String]] = Map(
    1 -> Seq("engineering", "research"),
    2 -> Seq("sales"))
  val departmentsById = new AtomicReference(INITIAL_DEPARTMENTS_BY_ID)

  def resetCounters(): Unit = {
    requestCount.set(0)
    readerCount.set(0)
    closeCount.set(0)
    requestStarted.set(new CountDownLatch(1))
    requiredColumns.set(Seq.empty)
    pushedPredicateCount.set(0)
    pushedPredicateFields.set(Seq.empty)
    requestSizes.clear()
    requestedKeys.clear()
    nullKeyRequests.set(0)
    resultRowsRead.set(0)
    metricSamples.set(0)
    closeEvents.clear()
    readerEvents.clear()
    beforeReaderReturns.set(() => ())
    beforeLookupReturns.set(() => ())
    afterParameterRowsRead.set(_ => ())
    beforeCursorEnds.set(() => ())
    beforeCursorCloses.set(() => ())
    beforeNextReturns.set(() => ())
    beforeGetReturns.set(() => ())
    rowReadsAfterClose.set(0)
    beforeMetricsReturn.set(() => ())
    lookupCandidates.set(Seq.empty)
    lookupCandidateDescriptions.set(Seq.empty)
    departmentsById.set(INITIAL_DEPARTMENTS_BY_ID)
  }
}

object SimpleExternalLookupTable extends Table with SupportsRead {
  override def name(): String = "SimpleExternalLookupTable"
  override def schema(): StructType = SimpleExternalLookupDataSource.outputSchema
  override def capabilities(): java.util.Set[TableCapability] =
    java.util.EnumSet.of(EXTERNAL_LOOKUP)

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    val configuredMaxBatchSize = options.getInt("maxBatchSize", 100)
    val responseDelayMillis = options.getLong("responseDelayMillis", 0)
    val failAt = options.getOrDefault("failAt", "none")
    val mutateAfterRequest = options.getBoolean("mutateAfterRequest", false)
    val nullKeySelection = options.getBoolean("nullKeySelection", false)
    val selectionMode = options.getOrDefault("selectionMode", "all")
    val nullCursor = options.getBoolean("nullCursor", false)
    // Accepts the filters on department but returns the rows that fail them.
    val ignoreAcceptedFilters = options.getBoolean("ignoreAcceptedFilters", false)
    // Selects the ranges on id but returns the rows that fail them.
    val ignoreSelectedRanges = options.getBoolean("ignoreSelectedRanges", false)
    new SupportsExternalLookup
      with SupportsPushDownRequiredColumns
      with SupportsPushDownV2Filters {
      private var requiredSchema = SimpleExternalLookupDataSource.outputSchema
      private var excludeResearch = false
      private var acceptedPredicates = Array.empty[Predicate]
      private var selectedLookupPredicates = Seq.empty[String]

      override def selectLookupPredicates(candidates: Array[Predicate]): Array[Predicate] = {
        if (nullKeySelection) return null
        SimpleExternalLookupDataSource.lookupCandidates.set(candidates.map { candidate =>
          val field = candidate.children()(0).asInstanceOf[NamedReference]
          val parameter = candidate.children()(1).asInstanceOf[LookupParameter]
          (candidate.name(), field.fieldNames().mkString("."), parameter.dataType())
        }.toSeq)
        SimpleExternalLookupDataSource.lookupCandidateDescriptions.set(
          candidates.map(_.toString).toSeq)
        val supportedPredicates = Set("=", "<=>", "<", "<=", ">", ">=")
        val supported = candidates.filter { candidate =>
          supportedPredicates.contains(candidate.name()) &&
            candidate.children()(0).asInstanceOf[NamedReference]
              .fieldNames().sameElements(Array("id"))
        }
        val selected = selectionMode match {
          case "all" => supported
          case "last" => supported.takeRight(1)
          case "reversed" => supported.reverse
          case "duplicate" => supported.take(1) ++ supported.take(1)
          // An equal-shaped predicate that is not one of the offered instances.
          case "copy" => supported.take(1).map(p => new Predicate(p.name(), p.children()))
          case "nullElement" => Array[Predicate](null)
        }
        selectedLookupPredicates = selected.filter(_ != null).map(_.name()).map {
          case "<" | "<=" | ">" | ">=" if ignoreSelectedRanges => "any"
          case name => name
        }.toSeq
        selected
      }

      override def pruneColumns(schema: StructType): Unit = {
        requiredSchema = schema
        SimpleExternalLookupDataSource.requiredColumns.set(schema.fieldNames.toSeq)
      }
      override def pushPredicates(predicates: Array[Predicate]): Array[Predicate] = {
        def fields(predicate: Predicate): Seq[NamedReference] =
          predicate.children().toSeq.flatMap {
            case reference: NamedReference => Seq(reference)
            case child: Predicate => fields(child)
            case _ => Seq.empty
          }
        val (accepted, rejected) = predicates.partition { predicate =>
          fields(predicate).exists(_.fieldNames().sameElements(Array("department")))
        }
        if (accepted.nonEmpty) {
          acceptedPredicates = accepted
          excludeResearch = !ignoreAcceptedFilters
          SimpleExternalLookupDataSource.pushedPredicateCount.addAndGet(accepted.length)
          SimpleExternalLookupDataSource.pushedPredicateFields.set(
            accepted.flatMap(fields).map(_.fieldNames().mkString(".")).toSeq)
        }
        rejected
      }
      override def pushedPredicates(): Array[Predicate] = acceptedPredicates
      override def build(): Scan = SimpleExternalLookupScan(
        configuredMaxBatchSize, responseDelayMillis, failAt,
        mutateAfterRequest, nullCursor, requiredSchema, excludeResearch,
        selectedLookupPredicates)
    }
  }
}

case class SimpleExternalLookupScan(
    configuredMaxBatchSize: Int,
    responseDelayMillis: Long,
    failAt: String,
    mutateAfterRequest: Boolean,
    nullCursor: Boolean,
    requiredSchema: StructType,
    excludeResearch: Boolean,
    selectedLookupPredicates: Seq[String]) extends Scan {

  override def readSchema(): StructType = requiredSchema
  override def toBatch(): Batch =
    SimpleExternalLookupBatch(requiredSchema.fieldNames.toSeq, excludeResearch)
  override def toExternalLookup(): ExternalLookup =
    TestExternalLookup(configuredMaxBatchSize, SimpleExternalLookupReaderFactory(
      responseDelayMillis,
      failAt,
      mutateAfterRequest,
      nullCursor,
      requiredSchema.fieldNames.toSeq,
      excludeResearch,
      selectedLookupPredicates))
}

case class SimpleExternalLookupBatch(
    requiredColumns: Seq[String],
    excludeResearch: Boolean) extends Batch {
  override def planInputPartitions(): Array[InputPartition] =
    Array(SimpleExternalLookupInputPartition)

  override def createReaderFactory(): PartitionReaderFactory =
    SimpleExternalLookupBatchReaderFactory(requiredColumns, excludeResearch)
}

case object SimpleExternalLookupInputPartition extends InputPartition

case class SimpleExternalLookupBatchReaderFactory(
    requiredColumns: Seq[String],
    excludeResearch: Boolean) extends PartitionReaderFactory {
  override def createReader(partition: InputPartition): PartitionReader[InternalRow] =
    new PartitionReader[InternalRow] {
      private val rows = SimpleExternalLookupDataSource.departmentsById.get().toSeq
        .sortBy(_._1)
        .iterator
        .flatMap { case (id, departments) =>
          departments.iterator
            .filterNot(department => excludeResearch && department == "research")
            .map { department =>
              InternalRow.fromSeq(requiredColumns.map {
                case "id" => Int.box(id)
                case "department" => UTF8String.fromString(department)
              })
            }
        }
      private var current: InternalRow = _

      override def next(): Boolean = {
        if (rows.hasNext) {
          current = rows.next()
          true
        } else {
          false
        }
      }

      override def get(): InternalRow = current

      override def close(): Unit = {}
    }
}

case class SimpleExternalLookupReaderFactory(
    responseDelayMillis: Long,
    failAt: String,
    mutateAfterRequest: Boolean,
    nullCursor: Boolean,
    requiredColumns: Seq[String],
    excludeResearch: Boolean,
    selectedLookupPredicates: Seq[String]) extends ExternalLookupReaderFactory {
  override def createReader(): ExternalLookupReader = new ExternalLookupReader {
    SimpleExternalLookupDataSource.readerCount.incrementAndGet()
    SimpleExternalLookupDataSource.beforeReaderReturns.get()()

    override def lookupBatch(keys: Array[InternalRow]): ExternalLookupResultCursor = {
      SimpleExternalLookupDataSource.requestStarted.get().countDown()
      if (responseDelayMillis > 0) Thread.sleep(responseDelayMillis)
      if (failAt == "request") {
        throw new IllegalStateException("injected external lookup request failure")
      }
      if (nullCursor) return null
      SimpleExternalLookupDataSource.requestCount.incrementAndGet()
      SimpleExternalLookupDataSource.requestSizes.add(keys.length)
      val results = keys.iterator.zipWithIndex.flatMap { case (key, ordinal) =>
        if (key.isNullAt(0)) {
          SimpleExternalLookupDataSource.nullKeyRequests.incrementAndGet()
        } else {
          val value = key.getInt(0)
          SimpleExternalLookupDataSource.requestedKeys.add(value)
        }
        val values = SimpleExternalLookupDataSource.departmentsById.get().toSeq.map {
          case (id, values) => Some(id) -> values
        } :+ (None -> Seq("unknown"))
        values.iterator.filter { case (id, _) =>
          selectedLookupPredicates.zipWithIndex.forall { case (predicate, index) =>
            val parameter = if (key.isNullAt(index)) None else Some(key.getInt(index))
            predicate match {
              case "=" => id.nonEmpty && parameter.nonEmpty && id == parameter
              case "<=>" => id == parameter
              case "<" => id.nonEmpty && parameter.nonEmpty && id.get < parameter.get
              case "<=" => id.nonEmpty && parameter.nonEmpty && id.get <= parameter.get
              case ">" => id.nonEmpty && parameter.nonEmpty && id.get > parameter.get
              case ">=" => id.nonEmpty && parameter.nonEmpty && id.get >= parameter.get
              case "any" => true
            }
          }
        }.flatMap { case (id, departments) => departments.iterator.map(id -> _) }
          .filterNot { case (_, department) => excludeResearch && department == "research" }
          .map { case (id, department) =>
          ordinal -> new GenericInternalRow(requiredColumns.map {
              case "id" => id.map(Int.box).orNull
              case "department" => UTF8String.fromString(department)
            }.toArray[Any])
        }
      }.toArray.iterator
      if (mutateAfterRequest) {
        SimpleExternalLookupDataSource.departmentsById.updateAndGet { current =>
          current.updated(2, Seq("sales-v2"))
        }
      }
      SimpleExternalLookupDataSource.afterParameterRowsRead.get()(keys)
      SimpleExternalLookupDataSource.beforeLookupReturns.get()()
      new TestExternalLookupResultCursor(
        if (failAt == "cursor") {
          Iterator.continually(
            throw new IllegalStateException("injected external lookup cursor failure"))
        } else {
          results
        },
        onNext = () => {
          SimpleExternalLookupDataSource.resultRowsRead.incrementAndGet()
          SimpleExternalLookupDataSource.beforeNextReturns.get()()
        },
        onEnd = () => SimpleExternalLookupDataSource.beforeCursorEnds.get()(),
        onClose = () => {
          SimpleExternalLookupDataSource.beforeCursorCloses.get()()
          SimpleExternalLookupDataSource.closeEvents.add("cursor")
        },
        onReadAfterClose =
          () => SimpleExternalLookupDataSource.rowReadsAfterClose.incrementAndGet(),
        onGet = () => SimpleExternalLookupDataSource.beforeGetReturns.get()())
    }

    override def currentMetricsValues():
        Array[org.apache.spark.sql.connector.metric.CustomTaskMetric] = {
      SimpleExternalLookupDataSource.metricSamples.incrementAndGet()
      SimpleExternalLookupDataSource.readerEvents.add("metrics")
      SimpleExternalLookupDataSource.beforeMetricsReturn.get()()
      Array.empty
    }

    override def close(): Unit = {
      SimpleExternalLookupDataSource.closeCount.incrementAndGet()
      SimpleExternalLookupDataSource.closeEvents.add("reader")
      SimpleExternalLookupDataSource.readerEvents.add("close")
    }
  }
}

class CompositeExternalLookupDataSource extends TableProvider {
  override def inferSchema(options: CaseInsensitiveStringMap): StructType =
    CompositeExternalLookupDataSource.outputSchema

  override def getTable(
      schema: StructType,
      partitioning: Array[Transform],
      properties: java.util.Map[String, String]): Table = CompositeExternalLookupTable
}

object CompositeExternalLookupDataSource {
  val outputSchema: StructType = new StructType()
    .add("tenant", "int", nullable = false)
    .add("id", "int", nullable = false)
    .add("value", "string", nullable = false)
  val values: Map[(Int, Int), String] = Map(
    (1, 1) -> "one-one",
    (1, 3) -> "one-three",
    (2, 1) -> "two-one")
  val selectedLookupPredicates =
    new AtomicReference(Seq.empty[(String, String)])
  // A selected field that the source does not compare when it matches, if any.
  val ignoredField = new AtomicReference(Option.empty[String])
  // The (tenant, id) parameters of each request row, with the (tenant, id) of each stored row
  // that the source returned for it, while it ignores a field.
  val returnedRows = new ConcurrentLinkedQueue[((Int, Int), (Int, Int))]()
}

/** Counts the calls of a test UDF that computes a lookup key, in every task of the JVM. */
object CountingKeyUDF {
  val calls = new AtomicInteger()
}

object CompositeExternalLookupTable extends Table with SupportsRead {
  override def name(): String = "CompositeExternalLookupTable"
  override def schema(): StructType = CompositeExternalLookupDataSource.outputSchema
  override def capabilities(): java.util.Set[TableCapability] =
    java.util.EnumSet.of(EXTERNAL_LOOKUP)

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder =
    new SupportsExternalLookup with SupportsPushDownRequiredColumns {
    private var requiredSchema = CompositeExternalLookupDataSource.outputSchema
    private var selectedLookupPredicates = Seq.empty[(String, String)]

    override def selectLookupPredicates(candidates: Array[Predicate]): Array[Predicate] = {
      val supported = candidates.filter { predicate =>
        val field = predicate.children()(0).asInstanceOf[NamedReference]
          .fieldNames().mkString(".")
        (field == "tenant" && predicate.name() == "=") ||
          (field == "id" && Set("=", "<", "<=", ">", ">=").contains(predicate.name()))
      }
      val selected = supported.map { predicate =>
        predicate.name() -> predicate.children()(0).asInstanceOf[NamedReference]
          .fieldNames().mkString(".")
      }.toSeq
      if (selected.contains("=" -> "tenant") && selected.exists(_._2 == "id")) {
        selectedLookupPredicates = selected
        CompositeExternalLookupDataSource.selectedLookupPredicates.set(selected)
        supported
      } else {
        Array.empty
      }
    }

    override def pruneColumns(schema: StructType): Unit = requiredSchema = schema
    override def build(): Scan =
      CompositeExternalLookupScan(requiredSchema, selectedLookupPredicates)
  }
}

case class CompositeExternalLookupScan(
    requiredSchema: StructType,
    selectedLookupPredicates: Seq[(String, String)]) extends Scan {
  override def readSchema(): StructType = requiredSchema
  override def toExternalLookup(): ExternalLookup =
    TestExternalLookup(
      100,
      CompositeExternalLookupReaderFactory(requiredSchema, selectedLookupPredicates))
}

class ValidityExternalLookupDataSource extends TableProvider {
  override def inferSchema(options: CaseInsensitiveStringMap): StructType =
    ValidityExternalLookupTable.schema()

  override def getTable(
      schema: StructType,
      partitioning: Array[Transform],
      properties: java.util.Map[String, String]): Table = ValidityExternalLookupTable
}

/**
 * A lookup-only table of rates, each valid from a time, which holds one rate, for id 1. Its
 * source selects every offered predicate.
 */
object ValidityExternalLookupTable extends Table with SupportsRead {
  // The descriptions of the predicates that Spark offered.
  val offered = new AtomicReference(Seq.empty[String])
  // The time from which the rate is valid, in microseconds, 1970-01-01 unless a test sets it.
  val validFrom = new AtomicReference(0L)

  override def name(): String = "ValidityExternalLookupTable"
  override def schema(): StructType = StructType.fromDDL("id INT, validFrom TIMESTAMP_NTZ")
  override def capabilities(): java.util.Set[TableCapability] =
    java.util.EnumSet.of(EXTERNAL_LOOKUP)

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder =
    new SupportsExternalLookup {
      override def selectLookupPredicates(candidates: Array[Predicate]): Array[Predicate] = {
        offered.set(candidates.map(_.toString).toSeq)
        candidates
      }
      override def build(): Scan = new Scan {
        override def readSchema(): StructType = schema()
        override def toExternalLookup(): ExternalLookup =
          TestExternalLookup(100, ValidityExternalLookupReaderFactory)
      }
    }
}

/** Returns the rate of id 1 for each parameter row that asks for id 1. */
object ValidityExternalLookupReaderFactory extends ExternalLookupReaderFactory {
  override def createReader(): ExternalLookupReader = new ExternalLookupReader {
    override def lookupBatch(keys: Array[InternalRow]): ExternalLookupResultCursor =
      new TestExternalLookupResultCursor(keys.iterator.zipWithIndex.collect {
        case (key, ordinal) if key.getInt(0) == 1 =>
          ordinal -> InternalRow(1, ValidityExternalLookupTable.validFrom.get())
      })

    override def close(): Unit = {}
  }
}

case class CompositeExternalLookupReaderFactory(
    requiredSchema: StructType,
    selectedLookupPredicates: Seq[(String, String)]) extends ExternalLookupReaderFactory {
  override def createReader(): ExternalLookupReader = new ExternalLookupReader {
    override def lookupBatch(keys: Array[InternalRow]): ExternalLookupResultCursor = {
      val results = keys.iterator.zipWithIndex.flatMap { case (key, ordinal) =>
        val ignored = CompositeExternalLookupDataSource.ignoredField.get()
        CompositeExternalLookupDataSource.values.iterator.filter { case ((tenant, id), _) =>
          selectedLookupPredicates.zipWithIndex.forall {
            case ((_, field), _) if ignored.contains(field) => true
            case ((predicate, field), index) =>
              val lookupValue = if (field == "tenant") tenant else id
              val parameter = key.getInt(index)
              predicate match {
                case "=" => lookupValue == parameter
                case "<" => lookupValue < parameter
                case "<=" => lookupValue <= parameter
                case ">" => lookupValue > parameter
                case ">=" => lookupValue >= parameter
              }
          }
        }.map { case ((tenant, id), lookupValue) =>
          if (ignored.nonEmpty) {
            CompositeExternalLookupDataSource.returnedRows.add(
              (key.getInt(0), key.getInt(1)) -> (tenant, id))
          }
          ordinal -> InternalRow.fromSeq(requiredSchema.fieldNames.toSeq.map {
            case "tenant" => Int.box(tenant)
            case "id" => Int.box(id)
            case "value" => UTF8String.fromString(lookupValue)
          })
        }
      }
      new TestExternalLookupResultCursor(results)
    }

    override def close(): Unit = {}
  }
}

class TestExternalLookupResultCursor(
    results: Iterator[(Int, InternalRow)],
    onNext: () => Unit = () => (),
    onEnd: () => Unit = () => (),
    onClose: () => Unit = () => (),
    onReadAfterClose: () => Unit = () => (),
    onGet: () => Unit = () => ())
    extends ExternalLookupResultCursor {
  private var current: (Int, InternalRow) = _
  @volatile private var closed = false

  override def next(): Boolean = if (results.hasNext) {
    current = results.next()
    onNext()
    true
  } else {
    current = null
    onEnd()
    false
  }

  override def get(): InternalRow = {
    if (closed) onReadAfterClose()
    onGet()
    current._2
  }

  override def inputOrdinal(): Int = {
    if (closed) onReadAfterClose()
    current._1
  }

  override def close(): Unit = {
    closed = true
    onClose()
  }
}

class SemanticKeyExternalLookupDataSource extends TableProvider {
  override def inferSchema(options: CaseInsensitiveStringMap): StructType =
    SemanticKeyExternalLookupDataSource.schema(options.get("keyType"))

  override def getTable(
      schema: StructType,
      partitioning: Array[Transform],
      properties: java.util.Map[String, String]): Table = {
    val options = new CaseInsensitiveStringMap(properties)
    SemanticKeyExternalLookupTable(options.get("keyType"))
  }
}

object SemanticKeyExternalLookupDataSource {
  val requestSizes = new ConcurrentLinkedQueue[Int]()

  def schema(keyType: String): StructType = new StructType()
    .add("key", DataType.fromDDL(keyType), nullable = false)
    .add("payload", "string", nullable = false)
}

case class SemanticKeyExternalLookupTable(keyType: String) extends Table with SupportsRead {
  override def name(): String = "SemanticKeyExternalLookupTable"
  override def schema(): StructType = SemanticKeyExternalLookupDataSource.schema(keyType)
  override def capabilities(): java.util.Set[TableCapability] =
    java.util.EnumSet.of(EXTERNAL_LOOKUP)

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder =
    new SupportsExternalLookup with SupportsPushDownRequiredColumns {
      private var requiredSchema = schema()

      override def selectLookupPredicates(candidates: Array[Predicate]): Array[Predicate] =
        candidates.filter { candidate =>
          candidate.name() == "=" &&
            candidate.children()(0).asInstanceOf[NamedReference]
              .fieldNames().sameElements(Array("key"))
        }.take(1)

      override def pruneColumns(schema: StructType): Unit = requiredSchema = schema
      override def build(): Scan = SemanticKeyExternalLookupScan(
        DataType.fromDDL(keyType), requiredSchema)
    }
}

case class SemanticKeyExternalLookupScan(
    keyType: DataType,
    requiredSchema: StructType) extends Scan {
  override def readSchema(): StructType = requiredSchema
  override def toExternalLookup(): ExternalLookup =
    TestExternalLookup(2, SemanticKeyExternalLookupReaderFactory(keyType, requiredSchema))
}

case class SemanticKeyExternalLookupReaderFactory(
    keyType: DataType,
    requiredSchema: StructType) extends ExternalLookupReaderFactory {
  override def createReader(): ExternalLookupReader = new ExternalLookupReader {
    override def lookupBatch(keys: Array[InternalRow]): ExternalLookupResultCursor = {
      SemanticKeyExternalLookupDataSource.requestSizes.add(keys.length)
      val results = keys.iterator.zipWithIndex.map { case (key, ordinal) =>
        val value = key.get(0, keyType)
        ordinal -> InternalRow.fromSeq(requiredSchema.fieldNames.toSeq.map {
          case "key" => value
          case "payload" => UTF8String.fromString("value")
        })
      }
      new TestExternalLookupResultCursor(results)
    }

    override def close(): Unit = {}
  }
}

class CaseInsensitiveExternalLookupDataSource extends TableProvider {
  override def inferSchema(options: CaseInsensitiveStringMap): StructType =
    CaseInsensitiveExternalLookupTable.schema()

  override def getTable(
      schema: StructType,
      partitioning: Array[Transform],
      properties: java.util.Map[String, String]): Table = CaseInsensitiveExternalLookupTable
}

/**
 * A lookup-only product table in a store that matches SKUs case-insensitively, so it returns more
 * rows than `sku = ?` selects in Spark.
 */
object CaseInsensitiveExternalLookupTable extends Table with SupportsRead {
  // Two products whose SKUs differ only in case.
  val products: Seq[(String, String)] = Seq("AB-1" -> "adapter", "ab-1" -> "cable")
  // The rows that the store returned, before Spark checked them.
  val returnedRows = new AtomicInteger()

  override def name(): String = "CaseInsensitiveExternalLookupTable"
  override def schema(): StructType = StructType.fromDDL("sku STRING, product STRING")
  override def capabilities(): java.util.Set[TableCapability] =
    java.util.EnumSet.of(EXTERNAL_LOOKUP)

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    val maxBatchSize = options.getInt("maxBatchSize", 100)
    new SupportsExternalLookup {
      override def selectLookupPredicates(candidates: Array[Predicate]): Array[Predicate] =
        candidates.filter(_.name() == "=").take(1)
      override def build(): Scan = new Scan {
        override def readSchema(): StructType = schema()
        override def toExternalLookup(): ExternalLookup =
          TestExternalLookup(maxBatchSize, CaseInsensitiveExternalLookupReaderFactory)
      }
    }
  }
}

object CaseInsensitiveExternalLookupReaderFactory extends ExternalLookupReaderFactory {
  override def createReader(): ExternalLookupReader = new ExternalLookupReader {
    override def lookupBatch(keys: Array[InternalRow]): ExternalLookupResultCursor = {
      val results = keys.iterator.zipWithIndex.flatMap { case (key, ordinal) =>
        val sku = key.getUTF8String(0).toString
        CaseInsensitiveExternalLookupTable.products.iterator
          .filter { case (storedSku, _) => storedSku.equalsIgnoreCase(sku) }
          .map { case (storedSku, product) =>
            CaseInsensitiveExternalLookupTable.returnedRows.incrementAndGet()
            ordinal -> InternalRow(UTF8String.fromString(storedSku), UTF8String.fromString(product))
          }
      }
      new TestExternalLookupResultCursor(results)
    }

    override def close(): Unit = {}
  }
}

class ReadSchemaExternalLookupDataSource extends TableProvider {
  override def inferSchema(options: CaseInsensitiveStringMap): StructType =
    ReadSchemaExternalLookupTable(options.get("kind")).schema()

  override def getTable(
      schema: StructType,
      partitioning: Array[Transform],
      properties: java.util.Map[String, String]): Table =
    ReadSchemaExternalLookupTable(new CaseInsensitiveStringMap(properties).get("kind"))
}

class MisconfiguredExternalLookupDataSource extends TableProvider {
  override def inferSchema(options: CaseInsensitiveStringMap): StructType =
    MisconfiguredExternalLookupTable(options.get("kind")).schema()

  override def getTable(
      schema: StructType,
      partitioning: Array[Transform],
      properties: java.util.Map[String, String]): Table =
    MisconfiguredExternalLookupTable(new CaseInsensitiveStringMap(properties).get("kind"))
}

/** A lookup-only table whose scan reports a read schema that differs from the table schema. */
case class ReadSchemaExternalLookupTable(kind: String) extends Table with SupportsRead {
  override def name(): String = "ReadSchemaExternalLookupTable"
  override def schema(): StructType = kind match {
    case "char" => StructType.fromDDL("id INT, name CHAR(5)")
    case "varchar" => StructType.fromDDL("id INT, name VARCHAR(5)")
    case "nested" => new StructType()
      .add("id", IntegerType)
      .add("s", new StructType().add("a", IntegerType, nullable = false))
    case "wrongType" | "reordered" => StructType.fromDDL("id INT, name STRING")
    case "struct" => StructType.fromDDL("id INT, info STRUCT<plan: STRING, seats: INT>")
    case "nullArrayElement" => new StructType()
      .add("id", IntegerType)
      .add("tags", ArrayType(IntegerType, containsNull = false))
    case "nullMapValue" => new StructType()
      .add("id", IntegerType)
      .add("scores", MapType(StringType, IntegerType, valueContainsNull = false))
    case "nullStructInArray" => new StructType()
      .add("id", IntegerType)
      .add("items", ArrayType(new StructType().add("a", IntegerType, nullable = false)))
  }
  override def capabilities(): java.util.Set[TableCapability] =
    java.util.EnumSet.of(EXTERNAL_LOOKUP)

  private def readSchema: StructType = kind match {
    case "nested" => new StructType()
      .add("id", IntegerType)
      .add("s", new StructType().add("a", IntegerType))
    case "wrongType" => StructType.fromDDL("id INT, name INT")
    case "reordered" => StructType.fromDDL("name STRING, id INT")
    // Each of these allows the null that the table rules out.
    case "nullArrayElement" => new StructType()
      .add("id", IntegerType)
      .add("tags", ArrayType(IntegerType, containsNull = true))
    case "nullMapValue" => new StructType()
      .add("id", IntegerType)
      .add("scores", MapType(StringType, IntegerType, valueContainsNull = true))
    case "nullStructInArray" => new StructType()
      .add("id", IntegerType)
      .add("items", ArrayType(new StructType().add("a", IntegerType)))
    case _ => schema()
  }

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder =
    new SupportsExternalLookup {
      override def selectLookupPredicates(candidates: Array[Predicate]): Array[Predicate] =
        candidates.filter(_.name() == "=").take(1)
      override def build(): Scan = ReadSchemaExternalLookupScan(readSchema, kind)
    }
}

case class ReadSchemaExternalLookupScan(schema: StructType, kind: String) extends Scan {
  override def readSchema(): StructType = schema
  override def toExternalLookup(): ExternalLookup =
    TestExternalLookup(100, ReadSchemaExternalLookupReaderFactory(kind))
}

/** Returns value rows in the read schema of the given kind of table. */
case class ReadSchemaExternalLookupReaderFactory(kind: String)
  extends ExternalLookupReaderFactory {
  override def createReader(): ExternalLookupReader = new ExternalLookupReader {
    override def lookupBatch(keys: Array[InternalRow]): ExternalLookupResultCursor = {
      val results = keys.iterator.zipWithIndex.map { case (key, ordinal) =>
        val id = key.getInt(0)
        val name = UTF8String.fromString("ab")
        ordinal -> (kind match {
          case "nested" => InternalRow(id, InternalRow(if (id == 2) null else id * 10))
          case "reordered" => InternalRow(name, id)
          case "struct" => InternalRow(
            id, InternalRow(UTF8String.fromString(if (id == 1) "pro" else "basic"), id * 10))
          case "nullArrayElement" => InternalRow(id, new GenericArrayData(Array[Any](id, null)))
          case "nullMapValue" => InternalRow(id, new ArrayBasedMapData(
            new GenericArrayData(Array[Any](UTF8String.fromString("math"))),
            new GenericArrayData(Array[Any](null))))
          case "nullStructInArray" =>
            InternalRow(id, new GenericArrayData(Array[Any](InternalRow(null))))
          case _ => InternalRow(id, name)
        })
      }
      new TestExternalLookupResultCursor(results)
    }

    override def close(): Unit = {}
  }
}

class MetricsExternalLookupDataSource extends TableProvider {
  override def inferSchema(options: CaseInsensitiveStringMap): StructType =
    MetricsExternalLookupTable.schema()

  override def getTable(
      schema: StructType,
      partitioning: Array[Transform],
      properties: java.util.Map[String, String]): Table = MetricsExternalLookupTable
}

/** A lookup-only table whose readers report how many requests each of them sent. */
object MetricsExternalLookupTable extends Table with SupportsRead {
  override def name(): String = "MetricsExternalLookupTable"
  override def schema(): StructType = StructType.fromDDL("id INT, value INT")
  override def capabilities(): java.util.Set[TableCapability] =
    java.util.EnumSet.of(EXTERNAL_LOOKUP)

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder =
    new SupportsExternalLookup {
      override def selectLookupPredicates(candidates: Array[Predicate]): Array[Predicate] =
        candidates.filter(_.name() == "=").take(1)
      override def build(): Scan = MetricsExternalLookupScan
    }
}

class ConnectorRequestsMetric extends CustomSumMetric {
  override def name(): String = "connectorRequests"
  override def description(): String = "connector requests"
}

/** A connector metric named like an operator metric, reported on the driver. */
class ConnectorOutputRowsMetric extends CustomSumMetric {
  override def name(): String = "numOutputRows"
  override def description(): String = "connector output rows"
}

object MetricsExternalLookupScan extends Scan {
  override def readSchema(): StructType = MetricsExternalLookupTable.schema()
  override def description(): String = "MetricsExternalLookupScan(token=secret-token)"
  override def supportedCustomMetrics(): Array[CustomMetric] =
    Array(new ConnectorRequestsMetric, new ConnectorOutputRowsMetric)
  override def reportDriverMetrics(): Array[CustomTaskMetric] = Array(new CustomTaskMetric {
    override def name(): String = "numOutputRows"
    override def value(): Long = 7
  })
  override def toExternalLookup(): ExternalLookup =
    TestExternalLookup(100, MetricsExternalLookupReaderFactory)
}

object MetricsExternalLookupReaderFactory extends ExternalLookupReaderFactory {
  override def createReader(): ExternalLookupReader = new ExternalLookupReader {
    private var requests = 0L

    override def lookupBatch(keys: Array[InternalRow]): ExternalLookupResultCursor = {
      requests += 1
      new TestExternalLookupResultCursor(keys.iterator.zipWithIndex.map { case (key, ordinal) =>
        ordinal -> InternalRow(key.getInt(0), key.getInt(0) * 10)
      })
    }

    override def currentMetricsValues(): Array[CustomTaskMetric] = Array(new CustomTaskMetric {
      override def name(): String = "connectorRequests"
      override def value(): Long = requests
    })

    override def close(): Unit = {}
  }
}

case class MisconfiguredExternalLookupTable(kind: String) extends Table with SupportsRead {
  override def name(): String = "MisconfiguredExternalLookupTable"
  override def schema(): StructType = kind match {
    case "nullUnderNotNullReadSchema" => StructType.fromDDL("id INT, department STRING")
    case "decimalOutOfRange" => StructType.fromDDL("id INT, amount DECIMAL(5, 2)")
    case "nestedDecimalOutOfRange" => StructType.fromDDL("id INT, amounts ARRAY<DECIMAL(5, 2)>")
    case "nestedDecimalRounding" => StructType.fromDDL("id INT, rates MAP<STRING, DECIMAL(5, 2)>")
    case "decimalInRange" => StructType.fromDDL("id INT, amount DECIMAL(5, 2), " +
      "amounts ARRAY<DECIMAL(5, 2)>, rates MAP<STRING, DECIMAL(5, 2)>")
    case "nullMapKey" => StructType.fromDDL("id INT, labels MAP<INT, STRING>")
    case "nullComplexMapKey" => StructType.fromDDL("id INT, parts MAP<STRUCT<a: INT>, STRING>")
    case compact if compact.startsWith("compactDecimal:") =>
      CompactDecimalRows.schema(compact.split(":")(1))
    case _ => SimpleExternalLookupDataSource.outputSchema
  }
  override def capabilities(): java.util.Set[TableCapability] = kind match {
    case "batchOnly" => java.util.EnumSet.of(BATCH_READ)
    case _ => java.util.EnumSet.of(EXTERNAL_LOOKUP)
  }

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = kind match {
    case "batchOnly" | "noLookupBuilder" => () =>
      SimpleExternalLookupScan(100, 0, failAt = "none", mutateAfterRequest = false,
        nullCursor = false, schema(), excludeResearch = false, Seq("="))
    case _ => new SupportsExternalLookup {
      override def selectLookupPredicates(candidates: Array[Predicate]): Array[Predicate] =
        candidates.take(1)
      override def build(): Scan = kind match {
        case "unknownField" => NoExternalLookupScan(new StructType().add("unknown", "int"))
        case "duplicateField" =>
          NoExternalLookupScan(new StructType().add("id", "int").add("id", "int"))
        case "noExternalLookup" => NoExternalLookupScan(schema())
        case compact if compact.startsWith("compactDecimal:") => new Scan {
          override def readSchema(): StructType = schema()
          override def toExternalLookup(): ExternalLookup =
            TestExternalLookup(10, MisconfiguredExternalLookupReaderFactory(kind))
        }
        case "invalidOrdinal" | "negativeOrdinal" | "ordinalAtCount" | "nullRow" | "nullReader" |
            "nullValue" | "nullUnderNotNullReadSchema" | "decimalOutOfRange" |
            "nestedDecimalOutOfRange" | "nestedDecimalRounding" | "decimalInRange" | "nullMapKey" |
            "nullComplexMapKey" => new Scan {
          override def readSchema(): StructType = kind match {
            case "nullUnderNotNullReadSchema" =>
              StructType.fromDDL("id INT, department STRING NOT NULL")
            case _ => schema()
          }
          override def toExternalLookup(): ExternalLookup =
            TestExternalLookup(10, MisconfiguredExternalLookupReaderFactory(kind))
        }
      }
    }
  }
}

/** A lookup-only table that does not implement `SupportsRead`. */
object NonReadableExternalLookupTable extends Table {
  override def name(): String = "NonReadableExternalLookupTable"
  override def schema(): StructType = SimpleExternalLookupDataSource.outputSchema
  override def capabilities(): java.util.Set[TableCapability] =
    java.util.EnumSet.of(EXTERNAL_LOOKUP)
}

case class NoExternalLookupScan(readSchema: StructType) extends Scan {
  override def description(): String = "NoExternalLookupScan"
}

case class MisconfiguredExternalLookupReaderFactory(kind: String)
  extends ExternalLookupReaderFactory {
  override def createReader(): ExternalLookupReader = kind match {
    case "nullReader" => null
    // A DECIMAL(5, 2) value of 999.99, or of 12345678.90, which has too many digits.
    case compact if compact.startsWith("compactDecimal:") => new ExternalLookupReader {
      private val Array(_, rowKind, range) = compact.split(":")
      override def lookupBatch(keys: Array[InternalRow]): ExternalLookupResultCursor =
        new TestExternalLookupResultCursor(Iterator(0 -> CompactDecimalRows.row(
          rowKind, keys(0).getInt(0), if (range == "inRange") 99999L else 1234567890L)))
      override def close(): Unit = {}
    }
    case "invalidOrdinal" | "negativeOrdinal" | "ordinalAtCount" => new ExternalLookupReader {
      override def lookupBatch(keys: Array[InternalRow]): ExternalLookupResultCursor = {
        val ordinal = kind match {
          case "invalidOrdinal" => 5
          case "negativeOrdinal" => -1
          case "ordinalAtCount" => keys.length
        }
        new TestExternalLookupResultCursor(
          Iterator(ordinal -> InternalRow(1, UTF8String.fromString("value"))))
      }
      override def close(): Unit = {}
    }
    case "nullRow" => new ExternalLookupReader {
      override def lookupBatch(keys: Array[InternalRow]): ExternalLookupResultCursor =
        new TestExternalLookupResultCursor(Iterator[(Int, InternalRow)](0 -> null))
      override def close(): Unit = {}
    }
    // DECIMAL(5, 2) values that fit their type, at the top level and nested.
    case "decimalInRange" => new ExternalLookupReader {
      override def lookupBatch(keys: Array[InternalRow]): ExternalLookupResultCursor =
        new TestExternalLookupResultCursor(keys.iterator.zipWithIndex.map { case (key, ordinal) =>
          ordinal -> InternalRow(
            key.getInt(0),
            Decimal(BigDecimal("999.99")),
            new GenericArrayData(
              Array[Any](Decimal(BigDecimal("-999.99")), Decimal(BigDecimal("1.5")))),
            new ArrayBasedMapData(
              new GenericArrayData(Array[Any](UTF8String.fromString("EUR"))),
              new GenericArrayData(Array[Any](Decimal(BigDecimal("0.01"))))))
        })
      override def close(): Unit = {}
    }
    // A null in a column that the table or the read schema declares NOT NULL.
    case "nullValue" | "nullUnderNotNullReadSchema" => new ExternalLookupReader {
      override def lookupBatch(keys: Array[InternalRow]): ExternalLookupResultCursor =
        new TestExternalLookupResultCursor(Iterator(0 -> InternalRow(keys(0).getInt(0), null)))
      override def close(): Unit = {}
    }
    // A DECIMAL(5, 2) value with more digits than its type allows.
    case "decimalOutOfRange" => new ExternalLookupReader {
      override def lookupBatch(keys: Array[InternalRow]): ExternalLookupResultCursor =
        new TestExternalLookupResultCursor(
          Iterator(0 -> InternalRow(keys(0).getInt(0), Decimal(BigDecimal("12345.678")))))
      override def close(): Unit = {}
    }
    // A DECIMAL(5, 2) array element with more digits than its type allows.
    case "nestedDecimalOutOfRange" => new ExternalLookupReader {
      override def lookupBatch(keys: Array[InternalRow]): ExternalLookupResultCursor =
        new TestExternalLookupResultCursor(Iterator(0 -> InternalRow(
          keys(0).getInt(0), new GenericArrayData(Array[Any](Decimal(BigDecimal("12345.67")))))))
      override def close(): Unit = {}
    }
    // A map with a null key, which no map may hold.
    case "nullMapKey" | "nullComplexMapKey" => new ExternalLookupReader {
      override def lookupBatch(keys: Array[InternalRow]): ExternalLookupResultCursor =
        new TestExternalLookupResultCursor(Iterator(0 -> InternalRow(
          keys(0).getInt(0),
          new ArrayBasedMapData(
            new GenericArrayData(Array[Any](null)),
            new GenericArrayData(Array[Any](UTF8String.fromString("x")))))))
      override def close(): Unit = {}
    }
    // A DECIMAL(5, 2) map value with more decimal places than its type allows.
    case "nestedDecimalRounding" => new ExternalLookupReader {
      override def lookupBatch(keys: Array[InternalRow]): ExternalLookupResultCursor =
        new TestExternalLookupResultCursor(Iterator(0 -> InternalRow(
          keys(0).getInt(0),
          new ArrayBasedMapData(
            new GenericArrayData(Array[Any](UTF8String.fromString("EUR"))),
            new GenericArrayData(Array[Any](Decimal(BigDecimal("1.234"))))))))
      override def close(): Unit = {}
    }
  }
}

/**
 * Rows of an id and a DECIMAL(5, 2) value, which a connector writes as a compact decimal, without
 * checking its digits, into an UnsafeRow, a struct in one, or a columnar row or array.
 */
object CompactDecimalRows {
  private val amountType = DecimalType(5, 2)

  def schema(rowKind: String): StructType = rowKind match {
    case "unsafe" | "columnar" => StructType.fromDDL("id INT, amount DECIMAL(5, 2)")
    case "unsafeNested" => StructType.fromDDL("id INT, price STRUCT<amount: DECIMAL(5, 2)>")
    case "columnarArray" => StructType.fromDDL("id INT, amounts ARRAY<DECIMAL(5, 2)>")
  }

  def row(rowKind: String, id: Int, unscaled: Long): InternalRow = {
    val amount = Decimal.createUnsafe(unscaled, 18, 2)
    def columnar(values: OnHeapColumnVector): InternalRow = {
      val ids = new OnHeapColumnVector(1, IntegerType)
      ids.putInt(0, id)
      val batch = new ColumnarBatch(Array(ids, values))
      batch.setNumRows(1)
      batch.getRow(0)
    }
    rowKind match {
      case "unsafe" =>
        val row = UnsafeProjection.create(schema(rowKind))(InternalRow(id, Decimal(0, 5, 2)))
        row.setDecimal(1, amount, amountType.precision)
        row
      case "unsafeNested" =>
        val row = UnsafeProjection.create(schema(rowKind))(
          InternalRow(id, InternalRow(Decimal(0, 5, 2))))
        // The struct shares the buffer of the row, so the value is written into the row.
        row.getStruct(1, 1).setDecimal(0, amount, amountType.precision)
        row
      case "columnar" =>
        val amounts = new OnHeapColumnVector(1, amountType)
        amounts.putDecimal(0, amount, amountType.precision)
        columnar(amounts)
      case "columnarArray" =>
        val amounts = new OnHeapColumnVector(1, ArrayType(amountType))
        amounts.arrayData().reserve(1)
        amounts.arrayData().putDecimal(0, amount, amountType.precision)
        amounts.putArray(0, 0, 1)
        columnar(amounts)
    }
  }
}

case class TestExternalLookup(
    maxRequestSize: Int,
    factory: ExternalLookupReaderFactory) extends ExternalLookup {
  override def maxParameterRowsPerRequest(): Int = maxRequestSize
  override def createReaderFactory(): ExternalLookupReaderFactory = factory
}

/** A badge, which [[BadgeUDT]] stores as a struct whose fields are NOT NULL. */
case class Badge(kind: Int, label: String, credit: java.math.BigDecimal)

class BadgeUDT extends UserDefinedType[Badge] {
  override def sqlType: DataType = new StructType()
    .add("kind", IntegerType, nullable = false)
    .add("label", StringType, nullable = false)
    .add("credit", DecimalType(5, 2), nullable = false)
  override def serialize(badge: Badge): InternalRow =
    InternalRow(badge.kind, UTF8String.fromString(badge.label), Decimal(badge.credit, 5, 2))
  override def deserialize(datum: Any): Badge = datum match {
    case row: InternalRow => Badge(
      row.getInt(0), row.getUTF8String(1).toString, row.getDecimal(2, 5, 2).toJavaBigDecimal)
  }
  override def userClass: Class[Badge] = classOf[Badge]
}

class UserTypeExternalLookupDataSource extends TableProvider {
  override def inferSchema(options: CaseInsensitiveStringMap): StructType =
    UserTypeExternalLookupTable(options.get("container"), options.get("value")).schema()

  override def getTable(
      schema: StructType,
      partitioning: Array[Transform],
      properties: java.util.Map[String, String]): Table = {
    val options = new CaseInsensitiveStringMap(properties)
    UserTypeExternalLookupTable(options.get("container"), options.get("value"))
  }
}

/**
 * A lookup-only table whose `badge` column holds a [[BadgeUDT]] value, alone or inside an array, a
 * map or a struct. The lookup returns the badge of the kind `value` for every key.
 */
case class UserTypeExternalLookupTable(container: String, value: String)
  extends Table with SupportsRead {
  override def name(): String = "UserTypeExternalLookupTable"
  override def schema(): StructType = new StructType()
    .add("id", IntegerType)
    .add("badge", container match {
      case "none" => new BadgeUDT
      case "array" => ArrayType(new BadgeUDT)
      case "map" => MapType(StringType, new BadgeUDT)
      case "struct" => new StructType().add("current", new BadgeUDT)
    })
  override def capabilities(): java.util.Set[TableCapability] =
    java.util.EnumSet.of(EXTERNAL_LOOKUP)

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder =
    new SupportsExternalLookup {
      override def selectLookupPredicates(candidates: Array[Predicate]): Array[Predicate] =
        candidates.filter(_.name() == "=").take(1)
      override def build(): Scan = UserTypeExternalLookupScan(schema(), container, value)
    }
}

case class UserTypeExternalLookupScan(readSchema: StructType, container: String, value: String)
  extends Scan {
  override def toExternalLookup(): ExternalLookup =
    TestExternalLookup(100, UserTypeExternalLookupReaderFactory(container, value))
}

case class UserTypeExternalLookupReaderFactory(container: String, value: String)
  extends ExternalLookupReaderFactory {
  override def createReader(): ExternalLookupReader = new ExternalLookupReader {
    override def lookupBatch(keys: Array[InternalRow]): ExternalLookupResultCursor = {
      val results = keys.iterator.zipWithIndex.map { case (key, ordinal) =>
        val id = key.getInt(0)
        val label = UTF8String.fromString(s"badge-$id")
        val badge = value match {
          case "valid" => InternalRow(id, label, Decimal("12.50"))
          case "nulls" => new GenericInternalRow(Array[Any](null, null, null))
          case "nullLabel" => InternalRow(id, null, Decimal("12.50"))
          case "outOfRange" => InternalRow(id, label, Decimal("12345678.90"))
        }
        ordinal -> InternalRow(id, container match {
          case "none" => badge
          case "array" => new GenericArrayData(Array[Any](badge))
          case "map" => new ArrayBasedMapData(
            new GenericArrayData(Array[Any](UTF8String.fromString("lobby"))),
            new GenericArrayData(Array[Any](badge)))
          case "struct" => InternalRow(badge)
        })
      }
      new TestExternalLookupResultCursor(results)
    }

    override def close(): Unit = {}
  }
}

/** A relation that counts how often a plan walk asks it for its inner children. */
case class InnerChildrenCountingRelation(output: Seq[Attribute]) extends LeafNode {
  override def innerChildren: Seq[QueryPlan[_]] = {
    InnerChildrenCountingRelation.calls.incrementAndGet()
    Nil
  }
}

object InnerChildrenCountingRelation {
  val calls = new AtomicInteger()
}

class TypedKeyExternalLookupDataSource extends TableProvider {
  override def inferSchema(options: CaseInsensitiveStringMap): StructType =
    TypedKeyExternalLookupTable.schema(options.get("keyType"))

  override def getTable(
      schema: StructType,
      partitioning: Array[Transform],
      properties: java.util.Map[String, String]): Table =
    TypedKeyExternalLookupTable(new CaseInsensitiveStringMap(properties).get("keyType"),
      new CaseInsensitiveStringMap(properties).getBoolean("ranges", false))
}

/**
 * A lookup-only table keyed on a column of the given type, whose rows the test sets. For each
 * parameter row, its source returns the stored rows with that key, and one row with another key.
 */
/**
 * A lookup-only table keyed by a column of `keyType`. Its source selects an equality, or with
 * `ranges` a range, which it serves with every stored row, as Spark checks the range on each.
 */
case class TypedKeyExternalLookupTable(keyType: String, ranges: Boolean = false)
  extends Table with SupportsRead {
  override def name(): String = "TypedKeyExternalLookupTable"
  override def schema(): StructType = TypedKeyExternalLookupTable.schema(keyType)
  override def capabilities(): java.util.Set[TableCapability] =
    java.util.EnumSet.of(EXTERNAL_LOOKUP)

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder =
    new SupportsExternalLookup {
      override def selectLookupPredicates(candidates: Array[Predicate]): Array[Predicate] =
        candidates.filter(candidate => (candidate.name() == "=") != ranges).take(1)
      override def build(): Scan = new Scan {
        override def readSchema(): StructType = schema()
        override def toExternalLookup(): ExternalLookup = TestExternalLookup(
          100, TypedKeyExternalLookupReaderFactory(DataType.fromDDL(keyType), ranges))
      }
    }
}

object TypedKeyExternalLookupTable {
  def schema(keyType: String): StructType =
    new StructType().add("key", DataType.fromDDL(keyType)).add("payload", "string")
  // The stored rows, as the internal value of the key and a payload.
  val storedRows = new AtomicReference(Seq.empty[(Any, String)])
  // Keys of no stored row, which the source may return as rows that do not match.
  val otherKeys = new AtomicReference(Seq.empty[Any])
  val requestSizes = new ConcurrentLinkedQueue[Int]()
}

case class TypedKeyExternalLookupReaderFactory(keyType: DataType, ranges: Boolean = false)
  extends ExternalLookupReaderFactory {
  override def createReader(): ExternalLookupReader = new ExternalLookupReader {
    private def sameKey(first: Any, second: Any): Boolean =
      EqualTo(Literal(first, keyType), Literal(second, keyType)).eval() == true

    override def lookupBatch(keys: Array[InternalRow]): ExternalLookupResultCursor = {
      TypedKeyExternalLookupTable.requestSizes.add(keys.length)
      val stored = TypedKeyExternalLookupTable.storedRows.get()
      val results = keys.iterator.zipWithIndex.flatMap { case (key, ordinal) =>
        val parameter = key.get(0, keyType)
        val matches =
          stored.filter { case (storedKey, _) => ranges || sameKey(storedKey, parameter) }
        val other = if (ranges) None else {
          (stored.map(_._1) ++ TypedKeyExternalLookupTable.otherKeys.get())
            .find(!sameKey(_, parameter)).map(_ -> "other")
        }
        (matches ++ other).map { case (rowKey, payload) =>
          ordinal -> InternalRow(rowKey, UTF8String.fromString(payload))
        }
      }
      new TestExternalLookupResultCursor(results)
    }

    override def close(): Unit = {}
  }
}

class ReusedRowExternalLookupDataSource extends TableProvider {
  override def inferSchema(options: CaseInsensitiveStringMap): StructType =
    ReusedRowExternalLookupTable.schema()

  override def getTable(
      schema: StructType,
      partitioning: Array[Transform],
      properties: java.util.Map[String, String]): Table = ReusedRowExternalLookupTable
}

/** A lookup-only table whose cursor reuses one value row and its buffers for every result. */
object ReusedRowExternalLookupTable extends Table with SupportsRead {
  val values: Map[Int, Seq[(String, Array[Byte])]] = Map(
    1 -> Seq("alpha" -> Array.fill[Byte](4)(1), "beta" -> Array.fill[Byte](4)(2)),
    2 -> Seq("gamma" -> Array.fill[Byte](4)(3)))

  override def name(): String = "ReusedRowExternalLookupTable"
  override def schema(): StructType = StructType.fromDDL("id INT, name STRING, payload BINARY")
  override def capabilities(): java.util.Set[TableCapability] =
    java.util.EnumSet.of(EXTERNAL_LOOKUP)

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder =
    new SupportsExternalLookup {
      override def selectLookupPredicates(candidates: Array[Predicate]): Array[Predicate] =
        candidates.filter(_.name() == "=").take(1)
      override def build(): Scan = new Scan {
        override def readSchema(): StructType = schema()
        override def toExternalLookup(): ExternalLookup =
          TestExternalLookup(100, ReusedRowExternalLookupReaderFactory)
      }
    }
}

object ReusedRowExternalLookupReaderFactory extends ExternalLookupReaderFactory {
  override def createReader(): ExternalLookupReader = new ExternalLookupReader {
    override def lookupBatch(keys: Array[InternalRow]): ExternalLookupResultCursor = {
      val results = keys.iterator.zipWithIndex.flatMap { case (key, ordinal) =>
        ReusedRowExternalLookupTable.values.getOrElse(key.getInt(0), Nil).iterator.map {
          case (name, payload) => (ordinal, key.getInt(0), name, payload)
        }
      }
      new ExternalLookupResultCursor {
        private val row = new GenericInternalRow(3)
        private val nameBuffer = new Array[Byte](16)
        private val payloadBuffer = new Array[Byte](4)
        private var ordinal = -1

        override def next(): Boolean = results.hasNext && {
          val (resultOrdinal, id, name, payload) = results.next()
          val nameBytes = name.getBytes(StandardCharsets.UTF_8)
          System.arraycopy(nameBytes, 0, nameBuffer, 0, nameBytes.length)
          System.arraycopy(payload, 0, payloadBuffer, 0, payload.length)
          ordinal = resultOrdinal
          row.update(0, id)
          row.update(1, UTF8String.fromBytes(nameBuffer, 0, nameBytes.length))
          row.update(2, payloadBuffer)
          true
        }
        override def inputOrdinal(): Int = ordinal
        override def get(): InternalRow = row
        override def close(): Unit = {}
      }
    }

    override def close(): Unit = {}
  }
}

/** A catalog serving `ns.lookup` and `ns.dual` as an [[EvolvingExternalLookupTable]]. */
class EvolvingExternalLookupTestCatalog extends TableCatalog {
  private var catalogName: String = _

  override def initialize(name: String, options: CaseInsensitiveStringMap): Unit =
    catalogName = name
  override def name(): String = catalogName
  override def listTables(namespace: Array[String]): Array[Identifier] =
    Array("lookup", "dual").map(Identifier.of(Array("ns"), _))
  override def loadTable(ident: Identifier): Table = (ident.namespace().toSeq, ident.name()) match {
    case (Seq("ns"), "lookup") => EvolvingExternalLookupTable(lookupOnly = true)
    case (Seq("ns"), "dual") => EvolvingExternalLookupTable(lookupOnly = false)
    case _ => throw new NoSuchTableException(ident.namespace().mkString("."), ident.name())
  }
  override def alterTable(ident: Identifier, changes: TableChange*): Table =
    throw new UnsupportedOperationException()
  override def dropTable(ident: Identifier): Boolean = throw new UnsupportedOperationException()
  override def renameTable(oldIdent: Identifier, newIdent: Identifier): Unit =
    throw new UnsupportedOperationException()
}

object EvolvingExternalLookupTable {
  // "none", "column" once a top-level column is added, or "nestedField" once the struct gains one.
  val schemaChange = new AtomicReference("none")
}

/**
 * A table whose schema grows while a query runs, as ALTER TABLE ADD COLUMNS grows it. It reports
 * a version, so each micro-batch loads it again.
 */
case class EvolvingExternalLookupTable(lookupOnly: Boolean) extends Table with SupportsRead {
  override def name(): String = "EvolvingExternalLookupTable"
  override def schema(): StructType = EvolvingExternalLookupTable.schemaChange.get() match {
    case "none" => StructType.fromDDL("id INT, info STRUCT<plan: STRING>")
    case "column" => StructType.fromDDL("id INT, info STRUCT<plan: STRING>, seats INT")
    case "nestedField" => StructType.fromDDL("id INT, info STRUCT<plan: STRING, seats: INT>")
  }
  override def version(): String = EvolvingExternalLookupTable.schemaChange.get()
  override def capabilities(): java.util.Set[TableCapability] =
    if (lookupOnly) java.util.EnumSet.of(EXTERNAL_LOOKUP)
    else java.util.EnumSet.of(BATCH_READ, EXTERNAL_LOOKUP)

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder =
    new SupportsExternalLookup with SupportsPushDownRequiredColumns {
      private var requiredSchema = schema()
      override def pruneColumns(required: StructType): Unit = requiredSchema = required
      override def selectLookupPredicates(candidates: Array[Predicate]): Array[Predicate] =
        candidates.filter(_.name() == "=").take(1)
      override def build(): Scan = EvolvingExternalLookupScan(requiredSchema)
    }
}

case class EvolvingExternalLookupScan(readSchema: StructType) extends Scan {
  override def toExternalLookup(): ExternalLookup =
    TestExternalLookup(100, EvolvingExternalLookupReaderFactory(readSchema))
}

/** Returns a row of the read schema for ids 1 and 2, whatever columns the schema has. */
case class EvolvingExternalLookupReaderFactory(readSchema: StructType)
  extends ExternalLookupReaderFactory {
  override def createReader(): ExternalLookupReader = new ExternalLookupReader {
    override def lookupBatch(keys: Array[InternalRow]): ExternalLookupResultCursor = {
      def value(name: String, dataType: DataType, id: Int): Any = (name, dataType) match {
        case ("id", _) => id
        case ("plan", _) => UTF8String.fromString(if (id == 1) "pro" else "basic")
        case ("seats", _) => id * 10
        case ("info", info: StructType) =>
          new GenericInternalRow(info.fields.map(field => value(field.name, field.dataType, id)))
      }
      val results = keys.iterator.zipWithIndex.collect {
        case (key, ordinal) if key.getInt(0) == 1 || key.getInt(0) == 2 =>
          ordinal -> new GenericInternalRow(
            readSchema.fields.map(field => value(field.name, field.dataType, key.getInt(0))))
      }
      new TestExternalLookupResultCursor(results)
    }

    override def close(): Unit = {}
  }
}
