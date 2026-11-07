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

import java.util.concurrent.{ConcurrentLinkedQueue, CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}

import scala.collection.mutable
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._
import scala.reflect.{classTag, ClassTag}

import org.apache.spark.{SparkException, SparkIllegalArgumentException, SparkRuntimeException,
  SparkUnsupportedOperationException, TaskContext}
import org.apache.spark.sql.{AnalysisException, Column, DataFrame, Encoders, Row}
import org.apache.spark.sql.IntegratedUDFTestUtils.{shouldTestPythonUDFs, TestPythonUDF}
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.catalyst.expressions.{Add, Cast, LessThan, Literal, Rand}
import org.apache.spark.sql.catalyst.plans.logical.{Deduplicate, ExternalLookupCacheOptions,
  LogicalPlan, PreparedExternalLookupJoin}
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, GenericArrayData}
import org.apache.spark.sql.classic.Dataset
import org.apache.spark.sql.connector.catalog.{Identifier, SupportsRead, Table, TableCapability,
  TableCatalog, TableChange, TableProvider}
import org.apache.spark.sql.connector.catalog.TableCapability.{BATCH_READ, EXTERNAL_LOOKUP}
import org.apache.spark.sql.connector.expressions.{NamedReference, Transform}
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.connector.metric.{CustomMetric, CustomSumMetric, CustomTaskMetric}
import org.apache.spark.sql.connector.read.{Batch, ExternalLookup, ExternalLookupReader,
  ExternalLookupReaderFactory, ExternalLookupResultCursor, InputPartition, LookupParameter,
  PartitionReader, PartitionReaderFactory, Scan, ScanBuilder, SupportsExternalLookup,
  SupportsPushDownRequiredColumns, SupportsPushDownV2Filters}
import org.apache.spark.sql.execution.{CoalesceExec, LocalTableScanExec}
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2Relation, ExternalLookupRelation}
import org.apache.spark.sql.execution.joins.ExternalLookupJoinExec
import org.apache.spark.sql.execution.streaming.operators.stateful.StreamingDeduplicateExec
import org.apache.spark.sql.execution.streaming.runtime.{IncrementalExecution, MemoryStream,
  StreamingQueryWrapper}
import org.apache.spark.sql.execution.streaming.sources.LowLatencyMemoryStream
import org.apache.spark.sql.functions.{broadcast, coalesce, col, concat, date_from_unix_date,
  expr, length, lit, max, raise_error, rand, to_json, upper, when}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.{StreamingQueryException, StreamRealTimeModeE2ESuiteBase}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{ArrayType, DataType, Decimal, DoubleType, IntegerType,
  MapType, StringType, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.{Clock, ManualClock}

class ExternalLookupJoinSuite extends QueryTest with SharedSparkSession {

  import testImplicits._

  private val EXTERNAL_LOOKUP_CATALOG_NAME = "external_lookup_cat"

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

  /** Runs the streaming `df` on a memory sink until it has processed all of its input. */
  private def runStream(df: DataFrame): StreamRun = {
    val queryName = s"external_lookup_stream_${streamRunCount.incrementAndGet()}"
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

  /** Runs `body` with `clock` as the clock of the lookup cache TTL. */
  private def withCacheClock[T](clock: Clock)(body: => T): T = {
    val previous = ExternalLookupJoinExec.cacheClock
    ExternalLookupJoinExec.cacheClock = clock
    try body finally ExternalLookupJoinExec.cacheClock = previous
  }

  private def checkStream(df: DataFrame, expected: Seq[Row]): Unit =
    QueryTest.sameRows(expected, runStream(df).rows).foreach(fail(_))

  /** The error that the streaming `df` fails with. */
  private def streamFailure[T <: Throwable : ClassTag](df: DataFrame): T = {
    val cause = intercept[StreamingQueryException](runStream(df)).getCause
    assert(classTag[T].runtimeClass.isInstance(cause), cause)
    cause.asInstanceOf[T]
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
    Seq(
      "broadcast" -> "BROADCAST",
      "merge" -> "MERGE",
      "shuffle_hash" -> "SHUFFLE_HASH",
      "shuffle_replicate_nl" -> "SHUFFLE_REPLICATE_NL").foreach { case (hint, strategy) =>
      val lookupSide = dual.hint("external_lookup").hint(hint)
      val streamSide = left.hint(hint)
      val hinted = dual.hint("external_lookup")
      Seq(
        () => left.join(lookupSide, left("id") === lookupSide("id")),
        () => streamSide.join(hinted, streamSide("id") === hinted("id"))).foreach { query =>
        checkError(
          exception = intercept[AnalysisException] { query().queryExecution.analyzed },
          condition = "INVALID_EXTERNAL_LOOKUP_HINT.CONFLICTING_JOIN_STRATEGY",
          parameters = Map(
            "strategy" -> strategy,
            "relation" -> "`DualCapabilityExternalLookupTable`"))
      }
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
          condition = "INVALID_EXTERNAL_LOOKUP_HINT.CONFLICTING_JOIN_STRATEGY",
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
    assert(dual.hint("external_lookup", "cache_enabled=false").count() === 3)
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
          lookup.hint("external_lookup", "cache_enabled=false"),
          left("id") === lookup("id"),
          "right_outer").queryExecution.analyzed
      },
      condition = "EXTERNAL_LOOKUP_JOIN_UNSUPPORTED.JOIN_TYPE",
      parameters = Map(
        "joinType" -> "RIGHT OUTER",
        "relation" -> "`DualCapabilityExternalLookupTable`",
        "supportedJoinTypes" -> "INNER, LEFT OUTER, LEFT SEMI, LEFT ANTI"))

    // A cross join fails with its join type while it is analyzed, before its missing condition.
    checkError(
      exception = intercept[AnalysisException](left.crossJoin(lookup.hint("external_lookup"))),
      condition = "EXTERNAL_LOOKUP_JOIN_UNSUPPORTED.JOIN_TYPE",
      parameters = Map(
        "joinType" -> "CROSS",
        "relation" -> "`DualCapabilityExternalLookupTable`",
        "supportedJoinTypes" -> "INNER, LEFT OUTER, LEFT SEMI, LEFT ANTI"))

    // A strategy hint on the same join conflicts before the join type is checked.
    checkError(
      exception = intercept[AnalysisException] {
        left.join(
          lookup.hint("external_lookup", "cache_enabled=false").hint("merge"),
          left("id") === lookup("id"),
          "right_outer").queryExecution.analyzed
      },
      condition = "INVALID_EXTERNAL_LOOKUP_HINT.CONFLICTING_JOIN_STRATEGY",
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
      ("nullKeySelection", "true", "NULL_PREDICATE_SELECTION", Map.empty[String, String]),
      ("selectionMode", "nullElement", "NULL_PREDICATE_SELECTION", Map.empty[String, String]),
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
      (nullCursor, "NULL_RESULT_CURSOR", Map("table" -> "`SimpleExternalLookupTable`")),
      (misconfigured("nullReader"), "NULL_LOOKUP_READER", table),
      (misconfigured("invalidOrdinal"), "INVALID_INPUT_ORDINAL",
        table ++ Map("ordinal" -> "5", "numParameterRows" -> "1")),
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
      // A map key is never null, whether it is a primitive or a struct.
      (misconfigured("nullMapKey"), "NULL_IN_NOT_NULL_FIELD",
        table ++ Map("field" -> "`labels`.`key`")),
      (misconfigured("nullComplexMapKey"), "NULL_IN_NOT_NULL_FIELD",
        table ++ Map("field" -> "`parts`.`key`"))).foreach {
      case (lookup, subClass, parameters) =>
        checkError(
          exception = streamFailure[SparkRuntimeException](
            left.join(lookup, left("id") === lookup("id"))),
          condition = s"DATA_SOURCE_INVALID_EXTERNAL_LOOKUP.$subClass",
          parameters = parameters)
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

  test("external lookup hint rejects duplicate and malformed options") {
    val lookup = spark.read.format(classOf[SimpleExternalLookupDataSource].getName).load()
    val positiveInteger = Map("expected" -> "a positive integer")
    val duration = Map("expected" -> "a non-negative duration, such as 500ms, 5s, or 10m")
    val supportedOptions = Map("supportedOptions" ->
      "\"cache_enabled\", \"cache_max_entries\", \"cache_max_rows_per_key\", \"cache_ttl\"")
    Seq(
      (Seq("cache_enabled=true", "cache_enabled=false"), "cache_enabled", "DUPLICATE", Map()),
      (Seq("cache_enabled=unknown"), "cache_enabled", "INVALID_VALUE",
        Map("expected" -> "true or false")),
      (Seq("cache_enabled"), "cache_enabled", "INVALID_FORMAT", Map()),
      (Seq("cache_enabled="), "cache_enabled=", "INVALID_FORMAT", Map()),
      (Seq("cache_mode=enabled"), "cache_mode", "UNSUPPORTED", supportedOptions),
      (Seq("cache_max_entries=0"), "cache_max_entries", "INVALID_VALUE", positiveInteger),
      (Seq("cache_max_entries=abc"), "cache_max_entries", "INVALID_VALUE", positiveInteger),
      (Seq("cache_ttl=-1ms"), "cache_ttl", "INVALID_VALUE", duration),
      (Seq("cache_ttl=abc"), "cache_ttl", "INVALID_VALUE", duration),
      (Seq("cache_max_rows_per_key=0"), "cache_max_rows_per_key", "INVALID_VALUE",
        positiveInteger),
      (Seq("unknown_option=1"), "unknown_option", "UNSUPPORTED", supportedOptions)).foreach {
      case (options, option, subClass, parameters) =>
        checkError(
          exception = intercept[AnalysisException] {
            lookup.hint("external_lookup", options: _*).queryExecution.analyzed
          },
          condition = s"INVALID_EXTERNAL_LOOKUP_OPTION.$subClass",
          parameters = Map("option" -> ("\"" + option + "\"")) ++ parameters)
    }
    checkError(
      exception = intercept[AnalysisException] {
        lookup.hint("external_lookup", 1).queryExecution.analyzed
      },
      condition = "INVALID_EXTERNAL_LOOKUP_HINT.UNSUPPORTED_PARAMETER",
      parameters = Map("parameter" -> "\"1\""))
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
    checkError(
      exception = intercept[AnalysisException] {
        val hinted = lookup.hint("external_lookup").hint("external_lookup")
        left.join(hinted, left("id") === hinted("id")).queryExecution.analyzed
      },
      condition = "INVALID_EXTERNAL_LOOKUP_HINT.MULTIPLE_HINTS",
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
      checkError(
        exception = intercept[AnalysisException] {
          query("EXTERNAL_LOOKUP(r, r)").queryExecution.analyzed
        },
        condition = "INVALID_EXTERNAL_LOOKUP_HINT.DUPLICATE_RELATION",
        parameters = Map("relation" -> "`r`"))
    }

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
      .hint("external_lookup", "cache_enabled=false")
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
    // Generic column pruning must not project the lookup relation, which would hide it from
    // lookup planning; the join's requiredLookupOutput keeps it from doing so.
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
        assert(error.getMessageParameters.asScala.toMap - "supportedJoinTypes" ===
          Map("joinType" -> joinType, "relation" -> "`SimpleExternalLookupTable`"))
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
      assert(crossJoin.getMessageParameters.asScala.toMap - "supportedJoinTypes" ===
        Map("joinType" -> "CROSS", "relation" -> "`SimpleExternalLookupTable`"))
      withTempView("external_lookup_left") {
        left.createOrReplaceTempView("external_lookup_left")
        val fullOuter = intercept[AnalysisException] {
          sql(s"SELECT * FROM external_lookup_left l FULL OUTER JOIN $table r ON l.id = r.id")
        }
        assert(fullOuter.getCondition === "EXTERNAL_LOOKUP_JOIN_UNSUPPORTED.JOIN_TYPE")
        assert(fullOuter.getMessageParameters.asScala.toMap - "supportedJoinTypes" ===
          Map("joinType" -> "FULL OUTER", "relation" -> "`SimpleExternalLookupTable`"))
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
    // built. The scalar subquery references neither side, so it could be offered as a parameter.
    Seq(
      sameId && left("x").isin(values),
      sameId && values.where(col("v") === 1).exists(),
      lookup("id") === values.scalar()).foreach { condition =>
      checkError(
        exception = intercept[AnalysisException](left.join(lookup, condition)),
        condition = "EXTERNAL_LOOKUP_JOIN_UNSUPPORTED.SUBQUERY_IN_JOIN_CONDITION",
        parameters = Map("relation" -> "`SimpleExternalLookupTable`"))
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

  test("an external lookup hint without relation names marks the whole query") {
    val lookup = spark.read.format(classOf[SimpleExternalLookupDataSource].getName).load()
    val dual = spark.read.format(classOf[DualCapabilityExternalLookupDataSource].getName).load()
    val left = streamOf(Seq(1).toDF("id"))
    val expected = Seq(Row(1, "engineering"), Row(1, "research"))

    // As with a join strategy hint, the hint applies to the plan it annotates, here the join
    // itself rather than its lookup side, so it is ignored. A lookup-only table still needs no
    // hint.
    withTempView("external_lookup_left", "external_lookup_right", "external_lookup_dual") {
      left.createOrReplaceTempView("external_lookup_left")
      lookup.createOrReplaceTempView("external_lookup_right")
      dual.createOrReplaceTempView("external_lookup_dual")
      Seq("external_lookup_right" -> true, "external_lookup_dual" -> false).foreach {
        case (right, usesLookup) =>
          val df = sql(s"""
            |SELECT /*+ EXTERNAL_LOOKUP */ l.id, r.department
            |FROM external_lookup_left l JOIN $right r ON l.id = r.id
            |""".stripMargin)
          assert(runStream(df).plan.exists(_.isInstanceOf[PreparedExternalLookupJoin]) ===
            usesLookup, right)
          checkStream(df, expected)
      }
    }
    val dataFrameJoin = left.join(dual, left("id") === dual("id")).hint("external_lookup")
    assert(
      runStream(dataFrameJoin).plan.exists(_.isInstanceOf[PreparedExternalLookupJoin]) === false)
  }

  test("SQL external lookup hint accepts cache options") {
    SimpleExternalLookupDataSource.resetCounters()
    val lookup = spark.read
      .format(classOf[SimpleExternalLookupDataSource].getName)
      .option("maxBatchSize", "1")
      .load()
    val left = streamOf(Seq(1, 2).toDF("id"))

    withTempView("external_lookup_left", "external_lookup_right") {
      left.createOrReplaceTempView("external_lookup_left")
      lookup.createOrReplaceTempView("external_lookup_right")
      val query = sql("""
          |SELECT /*+ EXTERNAL_LOOKUP(
          |  r,
          |  'cache_enabled=false',
          |  'cache_max_entries=123',
          |  'cache_ttl=2s',
          |  'cache_max_rows_per_key=5'
          |) */ l.id, r.department
          |FROM external_lookup_left l JOIN external_lookup_right r ON l.id = r.id
          |""".stripMargin)
      val run = runStream(query)
      assert(QueryTest.sameRows(
        Seq(Row(1, "engineering"), Row(1, "research"), Row(2, "sales")), run.rows).isEmpty)
      val prepared = run.plan.collectFirst {
        case externalLookup: PreparedExternalLookupJoin => externalLookup
      }.get
      assert(prepared.cacheOptions === ExternalLookupCacheOptions(
        enabled = false,
        maxEntries = 123,
        ttlMillis = 2000,
        maxRowsPerKey = 5))
    }
    assert(SimpleExternalLookupDataSource.requestCount.get() > 0)
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
        // The cursor is still read to the end.
        assert(SimpleExternalLookupDataSource.resultRowsRead.get() === 2)
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
      .hint("external_lookup", "cache_enabled=false")
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

  test("external lookup cache can be disabled without disabling request deduplication") {
    def run(cacheHint: Seq[Any]): Int = {
      SimpleExternalLookupDataSource.requestCount.set(0)
      val lookup = spark.read
        .format(classOf[SimpleExternalLookupDataSource].getName)
        .option("maxBatchSize", "1")
        .load()
      val hinted = lookup.hint("external_lookup", cacheHint: _*)
      val left = streamOf(Seq(1, 2, 1).toDF("id"))
      checkStream(
        left.join(hinted, left("id") === hinted("id"), "inner"),
        Seq(Row(1, 1, "engineering"), Row(1, 1, "research"), Row(2, 2, "sales"),
          Row(1, 1, "engineering"), Row(1, 1, "research")))
      SimpleExternalLookupDataSource.requestCount.get()
    }

    val cachedRequests = run(Nil)
    val uncachedRequests = run(Seq("cache_enabled=false"))
    assert(uncachedRequests > cachedRequests)
    assert(uncachedRequests * 2 === cachedRequests * 3)
  }

  test("cache options of a hint apply through the operators between the hint and the join") {
    val lookup = spark.read
      .format(classOf[SimpleExternalLookupDataSource].getName)
      .option("maxBatchSize", "1")
      .load()
      .hint("external_lookup", "cache_enabled=false")
      .select($"id", upper($"department").as("department"))
    val left = streamOf(Seq(1, 1).toDF("id"))

    val run = runStream(left.join(lookup, left("id") === lookup("id")))
    assert(run.rows.size === 4)
    val prepared = run.plan.collectFirst { case join: PreparedExternalLookupJoin => join }.get
    assert(prepared.cacheOptions.enabled === false)
    assert(SimpleExternalLookupDataSource.requestCount.get() === 2)
  }

  test("external lookup cache uses SQLConf defaults and hint overrides") {
    def run(hintParameters: Seq[Any]): Int = {
      SimpleExternalLookupDataSource.resetCounters()
      val lookup = spark.read
        .format(classOf[SimpleExternalLookupDataSource].getName)
        .option("maxBatchSize", "1")
        .load()
      val hinted = if (hintParameters.isEmpty) lookup else {
        lookup.hint("external_lookup", hintParameters: _*)
      }
      val left = streamOf(Seq(1, 1).toDF("id"))
      runStream(left.join(hinted, left("id") === hinted("id"), "inner"))
      SimpleExternalLookupDataSource.requestCount.get()
    }

    withSQLConf(SQLConf.JOIN_EXTERNAL_LOOKUP_CACHE_ENABLED.key -> "false") {
      assert(run(Nil) === 2)
      assert(run(Seq("cache_enabled=true")) === 1)
    }
  }

  test("external lookup cache controls freshness and caches misses") {
    def run(keys: Seq[Int], cacheDisabled: Boolean, mutate: Boolean): (Seq[Row], Int) = {
      SimpleExternalLookupDataSource.resetCounters()
      val unhintedLookup = spark.read
        .format(classOf[SimpleExternalLookupDataSource].getName)
        .option("maxBatchSize", "1")
        .option("mutateAfterRequest", mutate.toString)
        .load()
      val hintParameters: Seq[Any] = if (cacheDisabled) Seq("cache_enabled=false") else Nil
      val lookup = unhintedLookup.hint("external_lookup", hintParameters: _*)
      val left = streamOf(keys.toDF("id"))
      val rows = runStream(left.join(lookup, left("id") === lookup("id"), "left_outer")).rows
      rows -> SimpleExternalLookupDataSource.requestCount.get()
    }

    val (cachedRows, cachedRequests) = run(Seq(2, 2), cacheDisabled = false, mutate = true)
    assert(cachedRows === Seq(Row(2, 2, "sales"), Row(2, 2, "sales")))
    assert(cachedRequests === 1)

    val (freshRows, freshRequests) = run(Seq(2, 2), cacheDisabled = true, mutate = true)
    assert(freshRows === Seq(Row(2, 2, "sales"), Row(2, 2, "sales-v2")))
    assert(freshRequests === 2)

    val (missingRows, missingRequests) = run(Seq(3, 3), cacheDisabled = false, mutate = false)
    assert(missingRows === Seq(Row(3, null, null), Row(3, null, null)))
    assert(missingRequests === 1)
  }

  test("external lookup cache TTL expires entries") {
    SimpleExternalLookupDataSource.resetCounters()
    // Each reading of the clock moves it past the TTL, so an entry has expired when it is probed.
    // The cache reads time only from this clock, never the wall clock, so the 1ms TTL is exact.
    val clock = new ManualClock() {
      override def nanoTime(): Long = {
        val now = super.nanoTime()
        advance(2)
        now
      }
    }
    val lookup = spark.read
      .format(classOf[SimpleExternalLookupDataSource].getName)
      .option("maxBatchSize", "1")
      .load()
      .hint("external_lookup", "cache_ttl=1ms")
    val left = streamOf(Seq(2, 2).toDF("id"))

    withCacheClock(clock) {
      val joined = left.join(lookup, left("id") === lookup("id"), "left_outer")
      assert(runStream(joined).rows.size === 2)
    }
    assert(SimpleExternalLookupDataSource.requestedKeys.asScala.toSeq === Seq(2, 2))
  }

  test("a cache entry expiring between duplicate keys in one batch is looked up once") {
    SimpleExternalLookupDataSource.resetCounters()
    // From its fourth reading on, the clock is past the TTL of the entries written before.
    val clock = new ManualClock() {
      private val readings = new AtomicInteger(0)
      override def nanoTime(): Long = {
        if (readings.incrementAndGet() == 4) advance(2)
        super.nanoTime()
      }
    }
    val lookup = spark.read
      .format(classOf[SimpleExternalLookupDataSource].getName)
      .option("maxBatchSize", "2")
      .load()
      .hint("external_lookup", "cache_ttl=1ms")
    // Batches [2, 1] and [2, 2]. The clock is read when the first batch caches each of its two
    // keys and on each probe of a non-empty cache; it expires the entry for key 2 between the two
    // rows of the second batch.
    val left = streamOf(Seq(2, 1, 2, 2).toDF("id"))

    val rows = withCacheClock(clock) {
      runStream(left.join(lookup, left("id") === lookup("id"), "inner")).rows
    }
    // The rows are what the join returns without a cache: key 1 has two lookup rows and key 2
    // has one.
    val expected = Seq(Row(2, 2, "sales"), Row(1, 1, "engineering"), Row(1, 1, "research"),
      Row(2, 2, "sales"), Row(2, 2, "sales"))
    QueryTest.sameRows(expected, rows).foreach(fail(_))
    // The second batch's key 2 is served from the cache once, at its first row, and is not
    // requested again although its entry expires before the duplicate row.
    assert(SimpleExternalLookupDataSource.requestedKeys.asScala.toSeq === Seq(2, 1))
  }

  test("external lookup reports cache metrics") {
    SimpleExternalLookupDataSource.resetCounters()
    val lookup = spark.read
      .format(classOf[SimpleExternalLookupDataSource].getName)
      .option("maxBatchSize", "1")
      .load()
      .hint("external_lookup")
    val left = streamOf(Seq(1, 1).toDF("id"))
    val query = left.join(lookup, left("id") === lookup("id"), "inner")

    val operator = runStream(query).execution.executedPlan.collectFirst {
      case externalLookup: ExternalLookupJoinExec => externalLookup
    }.get
    assert(operator.metrics("numInputRows").value === 2)
    assert(operator.metrics("numInputBatches").value === 2)
    assert(operator.metrics("numLookupRequests").value === 1)
    assert(operator.metrics("numCacheHits").value === 1)
    assert(operator.metrics("lookupTime").value >= 0)
  }

  test("external lookup cache evicts least recently used entries at the configured bound") {
    SimpleExternalLookupDataSource.resetCounters()
    val lookup = spark.read
      .format(classOf[SimpleExternalLookupDataSource].getName)
      .option("maxBatchSize", "1")
      .load()
      .hint("external_lookup", "cache_max_entries=2")
    val left = streamOf(Seq(1, 2, 1, 3, 1, 2).toDF("id"))

    val rows = runStream(left.join(lookup, left("id") === lookup("id"), "left_outer")).rows
    val engineering = Seq(Row(1, 1, "engineering"), Row(1, 1, "research"))
    val expected = engineering ++ engineering ++ engineering ++
      Seq(Row(2, 2, "sales"), Row(2, 2, "sales"), Row(3, null, null))
    QueryTest.sameRows(expected, rows).foreach(fail(_))
    // With room for two keys, the third key evicts the least recently used key 2, not key 1,
    // which was used more recently.
    assert(SimpleExternalLookupDataSource.requestedKeys.asScala.toSeq === Seq(1, 2, 3, 2))
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

  test("oversized one-to-many results are streamed but not cached") {
    SimpleExternalLookupDataSource.resetCounters()
    SimpleExternalLookupDataSource.departmentsById.set(Map(1 -> Seq("engineering", "research")))
    val lookup = spark.read
      .format(classOf[SimpleExternalLookupDataSource].getName)
      .option("maxBatchSize", 1)
      .load()
      .hint("external_lookup", "cache_max_rows_per_key=1")
    val left = streamOf(Seq(1, 1).toDF("id"))

    assert(runStream(left.join(
      lookup, left("id") === lookup("id"), "inner")).rows.size === 4)
    assert(SimpleExternalLookupDataSource.requestCount.get() === 2)
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

  test("external lookup caches only the returned rows that satisfy the selected predicates") {
    // Requests hold one row, so the second order for `AB-1` reads its product from the cache. The
    // cache holds only the row that passed Spark's check of `sku = ?`.
    val products = spark.read
      .format(classOf[CaseInsensitiveExternalLookupDataSource].getName)
      .option("maxBatchSize", "1")
      .load()
    val orders = streamOf(Seq((1, "AB-1"), (2, "AB-1")).toDF("order", "sku"))
    CaseInsensitiveExternalLookupTable.returnedRows.set(0)
    val run = runStream(orders.join(products, orders("sku") === products("sku")))
    QueryTest.sameRows(
      Seq(Row(1, "AB-1", "AB-1", "adapter"), Row(2, "AB-1", "AB-1", "adapter")),
      run.rows).foreach(fail(_))
    assert(run.lookupJoin.metrics("numCacheHits").value === 1)
    // The one request returned both case variants.
    assert(CaseInsensitiveExternalLookupTable.returnedRows.get() === 2)
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

  test("external lookup checks returned rows against its own copy of each request row") {
    // A connector that overwrites the parameter rows it was given cannot make Spark drop rows.
    val lookup = spark.read.format(classOf[SimpleExternalLookupDataSource].getName).load()
    val left = streamOf(Seq(1, 2).toDF("id"))
    SimpleExternalLookupDataSource.afterParameterRowsRead.set(_.foreach(_.setInt(0, -1)))
    checkStream(
      left.join(lookup, left("id") === lookup("id")),
      Seq(Row(1, 1, "engineering"), Row(1, 1, "research"), Row(2, 2, "sales")))
  }

  test("a stream-side guard keeps a lookup key or a residual from failing on rows it rejects") {
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "true") {
      val employees = spark.read.format(classOf[SimpleExternalLookupDataSource].getName).load()
      // The guard is checked before the key is computed, so the badge `bad` is never cast.
      val scans = streamOf(Seq(("1", true), ("bad", false)).toDF("badge", "verified"))
      checkStream(
        scans.join(employees,
          scans("verified") && employees("id") === scans("badge").cast("int"), "left_outer"),
        Seq(Row("1", true, 1, "engineering"), Row("1", true, 1, "research"),
          Row("bad", false, null, null)))
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
    // Requests hold one row, so the repeated keys are served from the cache, in the same shape.
    val twice = streamOf(Seq(1, 2, 1, 2).toDF("id"))
    // The read schema lists the table columns in another order.
    val reordered = readSchemaLookup("reordered")
    val reorderedRun = runStream(twice.join(reordered, twice("id") === reordered("id")))
    QueryTest.sameRows(
      Seq(Row(1, 1, "ab"), Row(2, 2, "ab"), Row(1, 1, "ab"), Row(2, 2, "ab")),
      reorderedRun.rows).foreach(fail(_))
    assert(reorderedRun.lookupJoin.metrics("numCacheHits").value === 2)
    // The table declares `s.a` NOT NULL and the read schema does not, which the binding accepts.
    // Returned values must satisfy both, so the null that the connector returns for id 2 fails
    // the task.
    val nested = readSchemaLookup("nested")
    val firstIdTwice = streamOf(Seq(1, 1).toDF("id"))
    checkStream(
      firstIdTwice.join(nested, firstIdTwice("id") === nested("id"))
        .select(firstIdTwice("id"), to_json(nested("s"))),
      Seq(Row(1, "{\"a\":10}"), Row(1, "{\"a\":10}")))
    checkError(
      exception = streamFailure[SparkRuntimeException](
        left.join(nested, left("id") === nested("id"))),
      condition = "DATA_SOURCE_INVALID_EXTERNAL_LOOKUP.NULL_IN_NOT_NULL_FIELD",
      parameters = Map("table" -> "`ReadSchemaExternalLookupTable`", "field" -> "`s`.`a`"))
    // A field whose type really differs is still rejected.
    val wrongType = readSchemaLookup("wrongType")
    checkError(
      exception = streamFailure[AnalysisException](
        left.join(wrongType, left("id") === wrongType("id"))),
      condition = "DATA_SOURCE_INVALID_EXTERNAL_LOOKUP.UNKNOWN_SCAN_FIELD",
      parameters = Map("table" -> "`ReadSchemaExternalLookupTable`", "field" -> "`name`"))
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
      checkError(
        exception = streamFailure[SparkRuntimeException](
          left.join(lookup, left("id") === lookup("id"))),
        condition = "DATA_SOURCE_INVALID_EXTERNAL_LOOKUP.NULL_IN_NOT_NULL_FIELD",
        parameters = Map("table" -> "`ReadSchemaExternalLookupTable`", "field" -> field))
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
    // The cursor is closed before its reader.
    assert(SimpleExternalLookupDataSource.closeEvents.asScala.toSeq === Seq("cursor", "reader"))

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

  test("cached lookup results have passed the lookup-side stages and serve every join type") {
    val lookup = spark.read
      .format(classOf[SimpleExternalLookupDataSource].getName)
      .option("maxBatchSize", "1")
      .load()
      .select($"id", upper($"department").as("department"))
      .filter($"department" =!= "RESEARCH")
    val left = streamOf(Seq(1, 1).toDF("id"))
    Seq(
      "inner" -> Seq(Row(1, 1, "ENGINEERING"), Row(1, 1, "ENGINEERING")),
      "left_semi" -> Seq(Row(1), Row(1)),
      "left_anti" -> Seq.empty[Row]).foreach { case (joinType, expected) =>
      SimpleExternalLookupDataSource.resetCounters()
      val run = runStream(left.join(lookup, left("id") === lookup("id"), joinType))
      QueryTest.sameRows(expected, run.rows).foreach(fail(_))
      assert(SimpleExternalLookupDataSource.requestCount.get() === 1)
      assert(run.lookupJoin.metrics("numCacheHits").value === 1)
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
        // No new reader, request or cursor advance after the close, and every reader and cursor
        // is closed.
        val (readers, requests) = completion match {
          case "beforeEval" | "beforeRequest" => (0, 0)
          case "duringReaderCreation" => (1, 0)
          case _ => (1, 1)
        }
        assert(SimpleExternalLookupDataSource.readerCount.get() === readers)
        assert(SimpleExternalLookupDataSource.requestCount.get() === requests)
        assert(SimpleExternalLookupDataSource.resultRowsRead.get() === resultRowsRead)
        // The close closes the cursor before the reader. A cursor that a request in progress
        // returns after the close is closed after the reader.
        val closeEvents = completion match {
          case "beforeEval" | "beforeRequest" => Seq.empty
          case "duringReaderCreation" => Seq("reader")
          case "duringRequest" => Seq("reader", "cursor")
          case _ => Seq("cursor", "reader")
        }
        assert(SimpleExternalLookupDataSource.closeEvents.asScala.toSeq === closeEvents)
        checkError(
          exception = failure.get().asInstanceOf[SparkException],
          condition = "INTERNAL_ERROR",
          parameters = Map("message" -> "External lookup reader used after it was closed"))
      } finally {
        TaskContext.unset()
      }
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
      .hint("external_lookup", "cache_enabled=false")
    val left = streamOf((1 to 1500).toDF("id"))

    assert(runStream(left.join(lookup, left("id") === lookup("id"), "left_semi")).rows.size === 2)
    assert(SimpleExternalLookupDataSource.requestSizes.asScala.toSeq === Seq(1000, 500))
  }

  test("external lookup cache is scoped to a task") {
    SimpleExternalLookupDataSource.resetCounters()
    // One row per batch, so a task can avoid its second request only through its cache.
    val lookup = spark.read
      .format(classOf[SimpleExternalLookupDataSource].getName)
      .option("maxBatchSize", "1")
      .load()
    val left = streamOf(Seq(2, 2, 2, 2).toDF("id"), 2)

    assert(runStream(left.join(lookup, left("id") === lookup("id"))).rows.size === 4)
    assert(SimpleExternalLookupDataSource.readerCount.get() === 2)
    assert(SimpleExternalLookupDataSource.requestedKeys.asScala.toSeq === Seq(2, 2))
  }

  test("external lookup does not cache the results of a range lookup") {
    // One row per batch, so the second shift can avoid its request only through the cache.
    val lookup = spark.read
      .format(classOf[SimpleExternalLookupDataSource].getName)
      .option("maxBatchSize", "1")
      .load()
      .hint("external_lookup", "cache_enabled=true")
    // Two shifts that both start at employee id 2.
    val shifts = streamOf(Seq(("early", 2), ("late", 2)).toDF("shift", "firstId"))

    Seq(
      ("range", lookup("id") >= shifts("firstId"), 2, 0),
      ("equality", lookup("id") === shifts("firstId"), 1, 1)).foreach {
      case (kind, condition, requests, cacheHits) =>
        SimpleExternalLookupDataSource.resetCounters()
        val run = runStream(shifts.join(lookup, condition, "inner"))
        QueryTest.sameRows(
          Seq(Row("early", 2, 2, "sales"), Row("late", 2, 2, "sales")),
          run.rows).foreach(fail(_))
        assert(run.lookupJoin.cacheOptions.enabled === (kind == "equality"), kind)
        assert(SimpleExternalLookupDataSource.requestCount.get() === requests, kind)
        assert(run.lookupJoin.metrics("numCacheHits").value === cacheHits, kind)
    }
  }

  test("a connector that reuses its byte buffer cannot change cached rows") {
    // The connector writes each BINARY value into one array. The cache keeps the row of id 1
    // while the row of id 2 overwrites the array, then serves it to the second visit of id 1.
    val badges = readSchemaLookup("reusedBuffer")
    val visits = streamOf(Seq(1, 2, 1).toDF("id"))

    val run = runStream(visits.join(badges, visits("id") === badges("id")))
    QueryTest.sameRows(
      Seq(Row(1, 1, Array[Byte](1)), Row(2, 2, Array[Byte](2)), Row(1, 1, Array[Byte](1))),
      run.rows).foreach(fail(_))
    assert(run.lookupJoin.metrics("numCacheHits").value === 1)
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

  test("micro-batch stream-static joins follow the same lookup selection") {
    val dual = spark.read.format(classOf[DualCapabilityExternalLookupDataSource].getName).load()
    Seq(false, true).foreach { hinted =>
      SimpleExternalLookupDataSource.resetCounters()
      val input = MemoryStream[Int]
      val left = input.toDF().toDF("id")
      val lookup = if (hinted) dual.hint("external_lookup") else dual
      val queryName = s"external_lookup_dual_mbm_$hinted"
      val query = left.join(lookup, left("id") === lookup("id"))
        .writeStream.format("memory").queryName(queryName).start()
      try {
        input.addData(2)
        query.processAllAvailable()
        checkAnswer(spark.table(queryName), Seq(Row(2, 2, "sales")))
        val usesLookup = query.asInstanceOf[StreamingQueryWrapper].streamingQuery
          .lastExecution.executedPlan.exists(_.isInstanceOf[ExternalLookupJoinExec])
        assert(usesLookup === hinted)
        assert((SimpleExternalLookupDataSource.requestCount.get() > 0) === hinted)
      } finally {
        query.stop()
        spark.catalog.dropTempView(queryName)
      }
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
}

/** A catalog serving `ns.lookup` (lookup-only) and `ns.dual` (dual-capability). */
class ExternalLookupTestCatalog extends TableCatalog {
  private var catalogName: String = _

  override def initialize(name: String, options: CaseInsensitiveStringMap): Unit =
    catalogName = name
  override def name(): String = catalogName
  override def listTables(namespace: Array[String]): Array[Identifier] =
    Array("lookup", "dual").map(Identifier.of(Array("ns"), _))
  override def loadTable(ident: Identifier): Table = (ident.namespace().toSeq, ident.name()) match {
    case (Seq("ns"), "lookup") => SimpleExternalLookupTable
    case (Seq("ns"), "dual") => DualCapabilityExternalLookupTable
    case _ => throw new NoSuchTableException(ident.namespace().mkString("."), ident.name())
  }
  override def alterTable(ident: Identifier, changes: TableChange*): Table =
    throw new UnsupportedOperationException()
  override def dropTable(ident: Identifier): Boolean = throw new UnsupportedOperationException()
  override def renameTable(oldIdent: Identifier, newIdent: Identifier): Unit =
    throw new UnsupportedOperationException()
}

class ExternalLookupJoinRealTimeModeSuite extends StreamRealTimeModeE2ESuiteBase {
  import testImplicits._

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

object DualCapabilityExternalLookupTable extends Table with SupportsRead {
  override def name(): String = "DualCapabilityExternalLookupTable"
  override def schema(): StructType = SimpleExternalLookupDataSource.outputSchema
  override def capabilities(): java.util.Set[TableCapability] =
    java.util.EnumSet.of(BATCH_READ, EXTERNAL_LOOKUP)
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
  val beforeReaderReturns = new AtomicReference[() => Unit](() => ())
  val beforeLookupReturns = new AtomicReference[() => Unit](() => ())
  // Called with the parameter rows of each request, after the results are read.
  val afterParameterRowsRead = new AtomicReference[Array[InternalRow] => Unit](_ => ())
  // Called when a cursor reaches its end, before it reports it.
  val beforeCursorEnds = new AtomicReference[() => Unit](() => ())
  // Called when a cursor has advanced to a row, before next() returns.
  val beforeNextReturns = new AtomicReference[() => Unit](() => ())
  // Called when the join reads a row with get(), before it returns.
  val beforeGetReturns = new AtomicReference[() => Unit](() => ())
  // The get() and inputOrdinal() calls on a closed cursor.
  val rowReadsAfterClose = new AtomicInteger(0)
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
    beforeReaderReturns.set(() => ())
    beforeLookupReturns.set(() => ())
    afterParameterRowsRead.set(_ => ())
    beforeCursorEnds.set(() => ())
    beforeNextReturns.set(() => ())
    beforeGetReturns.set(() => ())
    rowReadsAfterClose.set(0)
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
        selectedLookupPredicates = selected.filter(_ != null).map(_.name()).toSeq
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
            }
          }
        }.flatMap { case (id, departments) => departments.iterator.map(id -> _) }
          .filterNot { case (_, department) => excludeResearch && department == "research" }
          .map { case (id, department) =>
          ordinal -> InternalRow.fromSeq(requiredColumns.map {
              case "id" => id.map(Int.box).orNull
              case "department" => UTF8String.fromString(department)
            })
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
        onClose = () => SimpleExternalLookupDataSource.closeEvents.add("cursor"),
        onReadAfterClose =
          () => SimpleExternalLookupDataSource.rowReadsAfterClose.incrementAndGet(),
        onGet = () => SimpleExternalLookupDataSource.beforeGetReturns.get()())
    }

    override def currentMetricsValues():
        Array[org.apache.spark.sql.connector.metric.CustomTaskMetric] = {
      SimpleExternalLookupDataSource.metricSamples.incrementAndGet()
      Array.empty
    }

    override def close(): Unit = {
      SimpleExternalLookupDataSource.closeCount.incrementAndGet()
      SimpleExternalLookupDataSource.closeEvents.add("reader")
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

/** Returns the rate of id 1, valid from 1970-01-01, for each parameter row that asks for id 1. */
object ValidityExternalLookupReaderFactory extends ExternalLookupReaderFactory {
  override def createReader(): ExternalLookupReader = new ExternalLookupReader {
    override def lookupBatch(keys: Array[InternalRow]): ExternalLookupResultCursor =
      new TestExternalLookupResultCursor(keys.iterator.zipWithIndex.collect {
        case (key, ordinal) if key.getInt(0) == 1 => ordinal -> InternalRow(1, 0L)
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
        CompositeExternalLookupDataSource.values.iterator.filter { case ((tenant, id), _) =>
          selectedLookupPredicates.zipWithIndex.forall {
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
    case "nullArrayElement" => new StructType()
      .add("id", IntegerType)
      .add("tags", ArrayType(IntegerType, containsNull = false))
    case "nullMapValue" => new StructType()
      .add("id", IntegerType)
      .add("scores", MapType(StringType, IntegerType, valueContainsNull = false))
    case "nullStructInArray" => new StructType()
      .add("id", IntegerType)
      .add("items", ArrayType(new StructType().add("a", IntegerType, nullable = false)))
    case "reusedBuffer" => StructType.fromDDL("id INT, badge BINARY")
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
    TestExternalLookup(1, ReadSchemaExternalLookupReaderFactory(kind))
}

/** Returns value rows in the read schema of the given kind of table. */
case class ReadSchemaExternalLookupReaderFactory(kind: String)
  extends ExternalLookupReaderFactory {
  override def createReader(): ExternalLookupReader = new ExternalLookupReader {
    // The one array that a "reusedBuffer" reader writes every BINARY value into.
    private val badgeBuffer = new Array[Byte](1)

    override def lookupBatch(keys: Array[InternalRow]): ExternalLookupResultCursor = {
      val results = keys.iterator.zipWithIndex.map { case (key, ordinal) =>
        val id = key.getInt(0)
        val name = UTF8String.fromString("ab")
        ordinal -> (kind match {
          case "nested" => InternalRow(id, InternalRow(if (id == 2) null else id * 10))
          case "reordered" => InternalRow(name, id)
          case "nullArrayElement" => InternalRow(id, new GenericArrayData(Array[Any](id, null)))
          case "nullMapValue" => InternalRow(id, new ArrayBasedMapData(
            new GenericArrayData(Array[Any](UTF8String.fromString("math"))),
            new GenericArrayData(Array[Any](null))))
          case "nullStructInArray" =>
            InternalRow(id, new GenericArrayData(Array[Any](InternalRow(null))))
          case "reusedBuffer" =>
            badgeBuffer(0) = id.toByte
            InternalRow(id, badgeBuffer)
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
    case "nullMapKey" => StructType.fromDDL("id INT, labels MAP<INT, STRING>")
    case "nullComplexMapKey" => StructType.fromDDL("id INT, parts MAP<STRUCT<a: INT>, STRING>")
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
        case "invalidOrdinal" | "nullReader" | "nullValue" | "nullUnderNotNullReadSchema" |
            "decimalOutOfRange" | "nestedDecimalOutOfRange" | "nestedDecimalRounding" |
            "nullMapKey" | "nullComplexMapKey" => new Scan {
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
    case "invalidOrdinal" => new ExternalLookupReader {
      override def lookupBatch(keys: Array[InternalRow]): ExternalLookupResultCursor =
        new TestExternalLookupResultCursor(
          Iterator(5 -> InternalRow(1, UTF8String.fromString("value"))))
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

case class TestExternalLookup(
    maxRequestSize: Int,
    factory: ExternalLookupReaderFactory) extends ExternalLookup {
  override def maxParameterRowsPerRequest(): Int = maxRequestSize
  override def createReaderFactory(): ExternalLookupReaderFactory = factory
}
