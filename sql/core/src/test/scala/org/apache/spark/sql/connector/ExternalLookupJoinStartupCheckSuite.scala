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

import java.util.concurrent.atomic.AtomicInteger

import org.apache.logging.log4j.Level

import org.apache.spark.SparkUnsupportedOperationException
import org.apache.spark.sql.{AnalysisException, DataFrame, Row, SQLContext}
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.expressions.{OuterReference, SubqueryExpression}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.streaming.WriteToStream
import org.apache.spark.sql.connector.catalog.{SupportsRead, Table, TableCapability, TableProvider}
import org.apache.spark.sql.connector.catalog.TableCapability.{BATCH_READ, EXTERNAL_LOOKUP}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2Relation,
  ExternalLookupRelation}
import org.apache.spark.sql.execution.streaming.runtime.{ExternalLookupJoinStartupCheck,
  MemoryStream, StreamingV2ScanPushDown}
import org.apache.spark.sql.functions.{col, lit, max}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery, StreamingQueryException}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/** Which errors of an external lookup join a streaming query reports when it starts. */
class ExternalLookupJoinStartupCheckSuite extends QueryTest with SharedSparkSession {
  import testImplicits._

  private val queryCount = new AtomicInteger()

  private implicit def sqlContextOfStreams: SQLContext = spark.sqlContext

  override def beforeEach(): Unit = {
    super.beforeEach()
    SimpleExternalLookupDataSource.resetCounters()
    ThrowingScanBuilderTable.scanBuilderCount.set(0)
  }

  private def lookupOnly: DataFrame =
    spark.read.format(classOf[SimpleExternalLookupDataSource].getName).load()

  private def throwingStatic: DataFrame =
    spark.read.format(classOf[ThrowingScanBuilderDataSource].getName).load()

  private def start(df: DataFrame): StreamingQuery =
    df.writeStream.format("memory").queryName(s"startup_check_${queryCount.incrementAndGet()}")
      .start()

  /** The error that `start()` throws for `df`. */
  private def startFailure(df: DataFrame): AnalysisException = {
    val e = intercept[AnalysisException](start(df).stop())
    // The connector served no lookup at the start.
    assert(SimpleExternalLookupDataSource.readerCount.get() === 0)
    e
  }

  /** Runs `body`, and returns the warnings that the startup check logged meanwhile. */
  private def startupWarnings(body: => Unit): Seq[String] = {
    val appender = new LogAppender("the startup check")
    withLogAppender(
        appender,
        loggerNames = Seq(ExternalLookupJoinStartupCheck.getClass.getName.stripSuffix("$")),
        level = Some(Level.WARN)) {
      body
    }
    appender.loggingEvents.map(_.getMessage.getFormattedMessage).toSeq
  }

  /** Adds `rows` to `input`, and fails unless the first micro-batch of `query` fails. */
  private def firstBatchFailure(
      query: StreamingQuery,
      input: MemoryStream[Int],
      rows: Int*): StreamingQueryException = {
    try {
      query.processAllAvailable()
      assert(query.isActive && query.exception.isEmpty)
      input.addData(rows)
      intercept[StreamingQueryException](query.processAllAvailable())
    } finally {
      query.stop()
    }
  }

  test("an ordinary stream-static join fails at its first micro-batch, not at start") {
    val input = MemoryStream[Int]
    val stream = input.toDF().toDF("id")
    val static = throwingStatic
    val query = start(stream.join(static, stream("id") === static("id")))
    // Without data, no micro-batch runs, so the query keeps running.
    assert(ThrowingScanBuilderTable.scanBuilderCount.get() === 0)
    val e = firstBatchFailure(query, input, 1)
    assert(e.getCause.getMessage.contains("The scan builder fails"))
  }

  test("MISSING_JOIN_CONDITION fails start()") {
    val stream = MemoryStream[Int].toDF().toDF("id")
    val lookup = lookupOnly
    Seq(
      stream.join(lookup).where(stream("id") === lookup("id")),
      stream.join(lookup, Seq.empty[String], "left_outer")).foreach { joined =>
      checkError(
        exception = startFailure(joined),
        condition = "EXTERNAL_LOOKUP_JOIN_UNSUPPORTED.MISSING_JOIN_CONDITION",
        parameters = Map.empty)
    }
    // A USING join gets its condition after its first analysis, so it starts.
    start(stream.join(lookup, Seq("id"))).stop()
  }

  test("NO_LOOKUP_CANDIDATE fails start(), also before any data arrives") {
    val stream = MemoryStream[Long].toDF().toDF("id")
    val lookup = lookupOnly
    // A BIGINT stream key casts the INT lookup column, so Spark has no candidate to offer.
    checkError(
      exception = startFailure(stream.join(lookup, stream("id") === lookup("id"))),
      condition = "EXTERNAL_LOOKUP_JOIN_UNSUPPORTED.NO_LOOKUP_CANDIDATE",
      parameters = Map("table" -> "`SimpleExternalLookupTable`"))
  }

  test("NO_LOOKUP_PREDICATE fails start()") {
    val stream = MemoryStream[Int].toDF().toDF("id")
    val lookup = lookupOnly
    checkError(
      exception = startFailure(stream.join(lookup, lookup("department") === "sales")),
      condition = "EXTERNAL_LOOKUP_JOIN_UNSUPPORTED.NO_LOOKUP_PREDICATE",
      parameters = Map("table" -> "`SimpleExternalLookupTable`"))
  }

  test("connector contract errors fail start() when Spark prepares the lookup, not when it runs") {
    val stream = MemoryStream[Int].toDF().toDF("id")
    def source(format: Class[_], option: String, value: String): DataFrame =
      spark.read.format(format.getName).option(option, value).load()
    val duplicate =
      source(classOf[SimpleExternalLookupDataSource], "selectionMode", "duplicate")
    checkError(
      exception = startFailure(stream.join(duplicate, stream("id") === duplicate("id"))),
      condition = "DATA_SOURCE_INVALID_EXTERNAL_LOOKUP.DUPLICATE_SELECTED_PREDICATE",
      parameters = Map("table" -> "`SimpleExternalLookupTable`", "predicate" -> "id = ?"))
    val noLookup =
      source(classOf[MisconfiguredExternalLookupDataSource], "kind", "noExternalLookup")
    val e = intercept[SparkUnsupportedOperationException] {
      start(stream.join(noLookup, stream("id") === noLookup("id"))).stop()
    }
    assert(e.getCondition === "DATA_SOURCE_EXTERNAL_LOOKUP_NOT_SUPPORTED")

    // A violation found while the join runs is reported by the tasks of a micro-batch.
    val input = MemoryStream[Int]
    val running = input.toDF().toDF("id")
    val nullCursor = source(classOf[SimpleExternalLookupDataSource], "nullCursor", "true")
    val query = start(running.join(nullCursor, running("id") === nullCursor("id")))
    val failure = firstBatchFailure(query, input, 1).getMessage
    assert(failure.contains("DATA_SOURCE_INVALID_EXTERNAL_LOOKUP.NULL_RESULT"), failure)
    assert(failure.contains("ExternalLookupReader.lookupBatch()"), failure)
  }

  test("a connector exception while Spark plans the lookup is left to the micro-batch") {
    val input = MemoryStream[Int]
    val stream = input.toDF().toDF("id")
    val lookup = spark.read.format(classOf[ThrowingLookupScanBuilderDataSource].getName).load()
    var query: StreamingQuery = null
    val warnings = startupWarnings {
      query = start(stream.join(lookup, stream("id") === lookup("id")))
    }
    assert(warnings.exists(_.contains("Could not plan the external lookup joins")))
    val e = firstBatchFailure(query, input, 1)
    assert(e.getCause.getMessage.contains("The scan builder fails"))
  }

  test("a lookup predicate that the optimizer finds does not fail start()") {
    val input = MemoryStream[Int]
    val stream = input.toDF().toDF("id")
    val lookup = lookupOnly
    // The written condition offers no candidate until the optimizer simplifies it.
    val joined = stream.join(lookup, stream("id") === lookup("id") || lit(false))
    var query: StreamingQuery = null
    assert(startupWarnings { query = start(joined.select("department")) }.isEmpty)
    try {
      input.addData(1, 3)
      query.processAllAvailable()
      checkAnswer(spark.table(query.name), Seq(Row("engineering"), Row("research")))
    } finally {
      query.stop()
    }
  }

  test("the start plans only the lookup sides") {
    val input = MemoryStream[Int]
    val stream = input.toDF().toDF("id")
    val static = throwingStatic
    val lookup = lookupOnly
    val joined = stream.join(static, stream("id") === static("id"))
      .join(lookup, stream("id") === lookup("id"))
    val query = start(joined)
    assert(ThrowingScanBuilderTable.scanBuilderCount.get() === 0)
    val e = firstBatchFailure(query, input, 1)
    assert(e.getCause.getMessage.contains("The scan builder fails"))
  }

  test("the start keeps correlated subqueries correlated and plans none of their tables") {
    // The stream key has a name that the subqueries cannot resolve, so `outer()` references it.
    def withSubqueries(stream: DataFrame, static: DataFrame): DataFrame = {
      val keys = static.select(col("id").as("key"))
      stream
        .where(keys.where(col("key") === col("sid").outer()).exists())
        .where(col("sid") <= keys.select(max("key")).scalar())
    }
    val lookup = lookupOnly
    def lookupJoin(stream: DataFrame): DataFrame =
      stream.join(lookup, stream("sid") === lookup("id"))
    val joined = lookupJoin(withSubqueries(MemoryStream[Int].toDF().toDF("sid"), throwingStatic))
    val masked = StreamingV2ScanPushDown.forStartup(
      joined.queryExecution.analyzed, withStartupValues = false)
    // The placeholder of the column pruning of streaming scans is private to it.
    def isPlaceholder(plan: LogicalPlan): Boolean = plan.nodeName == "BatchRelation"
    val subqueryPlans = masked.flatMap(_.expressions.flatMap(_.collect {
      case subquery: SubqueryExpression => subquery.plan
    }))
    def isCorrelated(plan: LogicalPlan): Boolean =
      plan.exists(_.expressions.exists(_.exists(_.isInstanceOf[OuterReference])))
    // The correlated EXISTS keeps the filter with its outer reference, over a placeholder.
    val Seq(correlated) = subqueryPlans.filter(isCorrelated)
    assert(correlated.collectLeaves().forall(isPlaceholder))
    // The plan of the uncorrelated scalar subquery is one placeholder.
    assert(subqueryPlans.filterNot(isCorrelated).map(isPlaceholder) === Seq(true))
    // Only the lookup side keeps its table.
    assert(masked.collectWithSubqueries {
      case relation @ (_: DataSourceV2Relation | _: ExternalLookupRelation) => relation
    }.length === 1)

    // The start plans the lookup join through the subqueries, without their scans.
    val bigintStream = MemoryStream[Long].toDF().toDF("sid")
    checkError(
      exception = startFailure(lookupJoin(withSubqueries(bigintStream, throwingStatic))),
      condition = "EXTERNAL_LOOKUP_JOIN_UNSUPPORTED.NO_LOOKUP_CANDIDATE",
      parameters = Map("table" -> "`SimpleExternalLookupTable`"))
    assert(ThrowingScanBuilderTable.scanBuilderCount.get() === 0)

    val input = MemoryStream[Int]
    val valid = lookupJoin(withSubqueries(input.toDF().toDF("sid"), Seq(1, 2).toDF("id")))
      .select("department")
    var query: StreamingQuery = null
    assert(startupWarnings { query = start(valid) }.isEmpty)
    try {
      input.addData(1, 3)
      query.processAllAvailable()
      checkAnswer(spark.table(query.name), Seq(Row("engineering"), Row("research")))
    } finally {
      query.stop()
    }
  }

  test("foreachBatch and toTable fail at start too") {
    val stream = MemoryStream[Long].toDF().toDF("id")
    val lookup = lookupOnly
    val joined = stream.join(lookup, stream("id") === lookup("id"))
      .select(stream("id"), lookup("department"))
    val e = intercept[AnalysisException] {
      joined.writeStream.foreachBatch((_: DataFrame, _: Long) => ()).start().stop()
    }
    assert(e.getCondition === "EXTERNAL_LOOKUP_JOIN_UNSUPPORTED.NO_LOOKUP_CANDIDATE")
    withTempDir { dir =>
      withTable("startup_check_sink") {
        val e = intercept[AnalysisException] {
          joined.writeStream.option("checkpointLocation", dir.getCanonicalPath)
            .toTable("startup_check_sink").stop()
        }
        assert(e.getCondition === "EXTERNAL_LOOKUP_JOIN_UNSUPPORTED.NO_LOOKUP_CANDIDATE")
      }
    }
  }

  test("the start plans with the confs that the offset log pins for a restarted query") {
    val policy = SQLConf.STREAMING_MULTIPLE_WATERMARK_POLICY
    withTempDir { dir =>
      val checkpoint = dir.getCanonicalPath
      val input = MemoryStream[Int]
      val stream = input.toDF().toDF("id")
      val lookup = lookupOnly
      val joined = stream.join(lookup, stream("id") === lookup("id"))
      // Only the checkpoint location of the write is read.
      def startupConf(): SQLConf = ExternalLookupJoinStartupCheck.startupConf(spark,
        WriteToStream("q", None, checkpoint, ThrowingScanBuilderTable, OutputMode.Append(),
          deleteCheckpointOnStop = false, joined.queryExecution.analyzed, None, None))
      withSQLConf(policy.key -> "max", SQLConf.CBO_ENABLED.key -> "true") {
        // A new query plans with the session's values, without CBO.
        assert(startupConf().getConf(policy) === "max")
        assert(startupConf().getConf(SQLConf.CBO_ENABLED) === false)
        val query = joined.writeStream.format("memory").queryName("startup_check_restarted")
          .option("checkpointLocation", checkpoint).start()
        try {
          input.addData(1)
          query.processAllAvailable()
        } finally {
          query.stop()
        }
      }
      withSQLConf(policy.key -> "min") {
        assert(startupConf().getConf(policy) === "max")
        assert(spark.conf.get(policy) === "min")
      }
    }
  }

  test("with the startup check disabled, the first micro-batch reports the error") {
    withSQLConf(SQLConf.JOIN_EXTERNAL_LOOKUP_STARTUP_CHECK_ENABLED.key -> "false") {
      val input = MemoryStream[Int]
      val stream = input.toDF().toDF("id")
      val lookup = lookupOnly
      val query = start(stream.join(lookup, lookup("department") === "sales"))
      val e = firstBatchFailure(query, input, 1)
      assert(e.getCause.asInstanceOf[AnalysisException].getCondition ===
        "EXTERNAL_LOOKUP_JOIN_UNSUPPORTED.NO_LOOKUP_PREDICATE")
    }
  }

  test("a lookup-only table read outside a lookup join is left to the micro-batch") {
    val lookup = lookupOnly
    // The optimizer removes this read, so the query runs. A check of the analyzed plan, as for
    // commands, would reject it.
    val pruned = lookup.union(lookup).where(lit(false))
    val input = MemoryStream[Int]
    val stream = input.toDF().toDF("id")
    val query = start(stream.join(pruned, stream("id") === pruned("id")))
    try {
      input.addData(1)
      query.processAllAvailable()
      assert(spark.table(query.name).isEmpty)
    } finally {
      query.stop()
    }
    // A read that stays fails the first micro-batch, as before.
    val kept = lookup.union(lookup)
    val nextInput = MemoryStream[Int]
    val nextStream = nextInput.toDF().toDF("id")
    val e = firstBatchFailure(
      start(nextStream.join(kept, nextStream("id") === kept("id"))), nextInput, 1)
    assert(e.getCause.asInstanceOf[AnalysisException].getCondition ===
      "UNSUPPORTED_FEATURE.TABLE_OPERATION")
  }
}

/** Serves a batch-readable table whose scan builder throws. */
class ThrowingScanBuilderDataSource extends TableProvider {
  override def inferSchema(options: CaseInsensitiveStringMap): StructType =
    ThrowingScanBuilderTable.schema()

  override def getTable(
      schema: StructType,
      partitioning: Array[Transform],
      properties: java.util.Map[String, String]): Table = ThrowingScanBuilderTable
}

object ThrowingScanBuilderTable extends Table with SupportsRead {
  val scanBuilderCount = new AtomicInteger()
  override def name(): String = "ThrowingScanBuilderTable"
  override def schema(): StructType = new StructType().add("id", "int")
  override def capabilities(): java.util.Set[TableCapability] = java.util.EnumSet.of(BATCH_READ)
  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    scanBuilderCount.incrementAndGet()
    throw new IllegalStateException("The scan builder fails")
  }
}

/** Serves a lookup-only table whose scan builder throws. */
class ThrowingLookupScanBuilderDataSource extends TableProvider {
  override def inferSchema(options: CaseInsensitiveStringMap): StructType =
    ThrowingLookupScanBuilderTable.schema()

  override def getTable(
      schema: StructType,
      partitioning: Array[Transform],
      properties: java.util.Map[String, String]): Table = ThrowingLookupScanBuilderTable
}

object ThrowingLookupScanBuilderTable extends Table with SupportsRead {
  override def name(): String = "ThrowingLookupScanBuilderTable"
  override def schema(): StructType = new StructType().add("id", "int")
  override def capabilities(): java.util.Set[TableCapability] =
    java.util.EnumSet.of(EXTERNAL_LOOKUP)
  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder =
    throw new IllegalStateException("The scan builder fails")
}
