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
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicReference}

import scala.jdk.CollectionConverters._
import scala.reflect.{classTag, ClassTag}

import org.apache.logging.log4j.Level

import org.apache.spark.{SparkRuntimeException, SparkUnsupportedOperationException}
import org.apache.spark.sql.{AnalysisException, DataFrame, Row, SQLContext}
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.expressions.{CurrentBatchTimestamp, CurrentDate,
  CurrentTimestamp, LocalTimestamp, OuterReference, SubqueryExpression}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.connector.catalog.{SupportsRead, Table, TableCapability, TableProvider}
import org.apache.spark.sql.connector.catalog.TableCapability.{BATCH_READ, EXTERNAL_LOOKUP}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2Relation,
  ExternalLookupRelation}
import org.apache.spark.sql.execution.streaming.runtime.{ExternalLookupJoinStartupCheck,
  MemoryStream, MicroBatchExecution, StreamingQueryCheckpointMetadata, StreamingQueryWrapper,
  StreamingV2ScanPushDown}
import org.apache.spark.sql.functions.{col, current_timestamp, lit, max}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.{StreamingQuery, StreamingQueryException,
  StreamingQueryListener}
import org.apache.spark.sql.streaming.StreamingQueryListener.{QueryProgressEvent,
  QueryStartedEvent, QueryTerminatedEvent}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{DateType, StructType, TimestampNTZType, TimestampType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/** Which errors of an external lookup join stop a streaming query when it starts. */
class ExternalLookupJoinStartupCheckSuite extends QueryTest with SharedSparkSession {
  import testImplicits._

  private val queryCount = new AtomicInteger()

  private val timeoutMs = 60000L

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

  /** The error that `start()` throws for `df`, which analysis reports. */
  private def startFailure(df: DataFrame): AnalysisException = {
    val e = intercept[AnalysisException](start(df).stop())
    // The connector served no lookup at the start.
    assert(SimpleExternalLookupDataSource.readerCount.get() === 0)
    e
  }

  /**
   * The error that stops the query of `df` when its execution starts: `start()` returns, and the
   * query stops before it has any data.
   */
  private def queryFailure[T <: Throwable : ClassTag](df: DataFrame): T = {
    val query = start(df)
    val e = try {
      intercept[StreamingQueryException](query.awaitTermination(timeoutMs))
    } finally {
      query.stop()
    }
    assert(query.exception.contains(e))
    // The connector served no lookup at the start.
    assert(SimpleExternalLookupDataSource.readerCount.get() === 0)
    assert(classTag[T].runtimeClass.isInstance(e.getCause), e.getCause)
    e.getCause.asInstanceOf[T]
  }

  /** Waits until `query` has planned its lookup joins and created its sources. */
  private def awaitInitialization(query: StreamingQuery): Unit =
    query.asInstanceOf[StreamingQueryWrapper].streamingQuery.awaitInitialization(timeoutMs)

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
    awaitInitialization(query)
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

  test("MISSING_JOIN_CONDITION fails start() for a hinted join") {
    val stream = MemoryStream[Int].toDF().toDF("id")
    checkError(
      exception = startFailure(stream.join(lookupOnly.hint("external_lookup"))),
      condition = "EXTERNAL_LOOKUP_JOIN_UNSUPPORTED.MISSING_JOIN_CONDITION",
      parameters = Map.empty)
  }

  test("the start plans a lookup side read through a WITH clause") {
    withTempView("startup_stream", "startup_bigint_stream", "startup_lookup", "startup_dual") {
      MemoryStream[Long].toDF().toDF("id").createOrReplaceTempView("startup_bigint_stream")
      lookupOnly.createOrReplaceTempView("startup_lookup")
      spark.read.format(classOf[DualCapabilityExternalLookupDataSource].getName).load()
        .createOrReplaceTempView("startup_dual")
      def withClause(stream: String, table: String, hint: String = ""): DataFrame = spark.sql(
        s"WITH lk AS (TABLE $table) SELECT $hint lk.department " +
          s"FROM $stream s JOIN lk ON s.id = lk.id")
      checkError(
        exception = queryFailure[AnalysisException](
          withClause("startup_bigint_stream", "startup_lookup")),
        condition = "EXTERNAL_LOOKUP_JOIN_UNSUPPORTED.NO_LOOKUP_CANDIDATE",
        parameters = Map("table" -> "`SimpleExternalLookupTable`"))
      Seq(("startup_lookup", ""), ("startup_dual", "/*+ EXTERNAL_LOOKUP(lk) */")).foreach {
        case (table, hint) =>
          val input = MemoryStream[Int]
          input.toDF().toDF("id").createOrReplaceTempView("startup_stream")
          var query: StreamingQuery = null
          assert(startupWarnings {
            query = start(withClause("startup_stream", table, hint))
            awaitInitialization(query)
          }.isEmpty)
          try {
            input.addData(1, 3)
            query.processAllAvailable()
            checkAnswer(spark.table(query.name), Seq(Row("engineering"), Row("research")))
          } finally {
            query.stop()
          }
      }
    }
  }

  test("NO_LOOKUP_CANDIDATE stops a hinted join over a dual-capability table when it starts") {
    withTempView("startup_bigint_stream", "startup_dual") {
      MemoryStream[Long].toDF().toDF("id").createOrReplaceTempView("startup_bigint_stream")
      spark.read.format(classOf[DualCapabilityExternalLookupDataSource].getName).load()
        .createOrReplaceTempView("startup_dual")
      // A BIGINT stream key casts the INT lookup column, so Spark has no candidate to offer. The
      // table also serves batch scans, so only the hint makes it a lookup side, directly or
      // through a WITH clause.
      Seq(
        "SELECT /*+ EXTERNAL_LOOKUP(d) */ d.department FROM startup_bigint_stream s " +
          "JOIN startup_dual d ON s.id = d.id",
        "WITH lk AS (TABLE startup_dual) SELECT /*+ EXTERNAL_LOOKUP(lk) */ lk.department " +
          "FROM startup_bigint_stream s JOIN lk ON s.id = lk.id").foreach { query =>
        checkError(
          exception = queryFailure[AnalysisException](spark.sql(query)),
          condition = "EXTERNAL_LOOKUP_JOIN_UNSUPPORTED.NO_LOOKUP_CANDIDATE",
          parameters = Map("table" -> "`DualCapabilityExternalLookupTable`"))
      }
    }
  }

  test("NO_LOOKUP_CANDIDATE names a table whose name does not parse as an identifier") {
    withSQLConf(
        "spark.sql.catalog.startup_lookup_cat" -> classOf[ExternalLookupTestCatalog].getName) {
      val stream = MemoryStream[Long].toDF().toDF("id")
      val lookup = spark.table("startup_lookup_cat.ns.path_dual").hint("external_lookup")
      // A BIGINT stream key casts the INT lookup column, so Spark has no candidate to offer. The
      // error renders the table's name, `./users`, as one quoted part.
      checkError(
        exception = queryFailure[AnalysisException](
          stream.join(lookup, stream("id") === lookup("id"))),
        condition = "EXTERNAL_LOOKUP_JOIN_UNSUPPORTED.NO_LOOKUP_CANDIDATE",
        parameters = Map("table" -> "`./users`"))
    }
  }

  test("NO_LOOKUP_CANDIDATE stops the query when it starts, before any data arrives") {
    val stream = MemoryStream[Long].toDF().toDF("id")
    val lookup = lookupOnly
    // A BIGINT stream key casts the INT lookup column, so Spark has no candidate to offer.
    checkError(
      exception = queryFailure[AnalysisException](
        stream.join(lookup, stream("id") === lookup("id"))),
      condition = "EXTERNAL_LOOKUP_JOIN_UNSUPPORTED.NO_LOOKUP_CANDIDATE",
      parameters = Map("table" -> "`SimpleExternalLookupTable`"))
  }

  test("NO_LOOKUP_PREDICATE stops the query when it starts") {
    val stream = MemoryStream[Int].toDF().toDF("id")
    val lookup = lookupOnly
    checkError(
      exception = queryFailure[AnalysisException](
        stream.join(lookup, lookup("department") === "sales")),
      condition = "EXTERNAL_LOOKUP_JOIN_UNSUPPORTED.NO_LOOKUP_PREDICATE",
      parameters = Map("table" -> "`SimpleExternalLookupTable`"))
  }

  test("connector contract errors stop the query when Spark prepares the lookup, not later") {
    val stream = MemoryStream[Int].toDF().toDF("id")
    def source(format: Class[_], option: String, value: String): DataFrame =
      spark.read.format(format.getName).option(option, value).load()
    val duplicate =
      source(classOf[SimpleExternalLookupDataSource], "selectionMode", "duplicate")
    checkError(
      exception = queryFailure[AnalysisException](
        stream.join(duplicate, stream("id") === duplicate("id"))),
      condition = "DATA_SOURCE_INVALID_EXTERNAL_LOOKUP.DUPLICATE_SELECTED_PREDICATE",
      parameters = Map("table" -> "`SimpleExternalLookupTable`", "predicate" -> "id = ?"))
    val noLookup =
      source(classOf[MisconfiguredExternalLookupDataSource], "kind", "noExternalLookup")
    val e = queryFailure[SparkUnsupportedOperationException](
      stream.join(noLookup, stream("id") === noLookup("id")))
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
    // Also a SparkThrowable that is not an error of the lookup join or of its connector contract.
    Seq(classOf[ThrowingLookupScanBuilderDataSource],
        classOf[SparkThrowingLookupScanBuilderDataSource]).foreach { format =>
      val input = MemoryStream[Int]
      val stream = input.toDF().toDF("id")
      val lookup = spark.read.format(format.getName).load()
      var query: StreamingQuery = null
      val warnings = startupWarnings {
        query = start(stream.join(lookup, stream("id") === lookup("id")))
        awaitInitialization(query)
      }
      assert(warnings.exists(_.contains("Could not plan the external lookup joins")), format)
      val e = firstBatchFailure(query, input, 1)
      assert(e.getCause.getMessage.contains("The scan builder fails"), format)
    }
  }

  test("a lookup predicate that the optimizer finds does not stop the query") {
    val lookup = lookupOnly
    // The written condition offers no candidate until the optimizer simplifies it: also through
    // date conversions, which the start folds as each micro-batch does.
    Seq(lit(false), lit("1999-01-01").cast("date") > lit("2000-01-01").cast("date")).foreach {
      never =>
        val input = MemoryStream[Int]
        val stream = input.toDF().toDF("id")
        val joined = stream.join(lookup, stream("id") === lookup("id") || never)
        var query: StreamingQuery = null
        assert(startupWarnings {
          query = start(joined.select("department"))
          awaitInitialization(query)
        }.isEmpty, never)
        try {
          input.addData(1, 3)
          query.processAllAvailable()
          checkAnswer(spark.table(query.name), Seq(Row("engineering"), Row("research")))
        } finally {
          query.stop()
        }
    }
  }

  test("the start reads the current time and date as the batch timestamp, as micro-batches do") {
    val plan = sql("SELECT current_timestamp(), current_date(), localtimestamp()")
      .queryExecution.analyzed
    val rewritten = MicroBatchExecution.withBatchTimestamp(plan, 1234L)
    val batchTimestamps = rewritten.expressions.flatMap(_.collect {
      case timestamp: CurrentBatchTimestamp => timestamp
    })
    assert(batchTimestamps.map(t => (t.timestampMs, t.dataType)) ===
      Seq((1234L, TimestampType), (1234L, DateType), (1234L, TimestampNTZType)))
    assert(rewritten.expressions.forall(_.collect {
      case _: CurrentTimestamp | _: CurrentDate | _: LocalTimestamp => true
    }.isEmpty))
  }

  test("the start plans only the lookup sides") {
    val input = MemoryStream[Int]
    val stream = input.toDF().toDF("id")
    val static = throwingStatic
    val lookup = lookupOnly
    val joined = stream.join(static, stream("id") === static("id"))
      .join(lookup, stream("id") === lookup("id"))
    val query = start(joined)
    awaitInitialization(query)
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
      exception = queryFailure[AnalysisException](
        lookupJoin(withSubqueries(bigintStream, throwingStatic))),
      condition = "EXTERNAL_LOOKUP_JOIN_UNSUPPORTED.NO_LOOKUP_CANDIDATE",
      parameters = Map("table" -> "`SimpleExternalLookupTable`"))
    assert(ThrowingScanBuilderTable.scanBuilderCount.get() === 0)

    val input = MemoryStream[Int]
    val valid = lookupJoin(withSubqueries(input.toDF().toDF("sid"), Seq(1, 2).toDF("id")))
      .select("department")
    var query: StreamingQuery = null
    assert(startupWarnings {
      query = start(valid)
      awaitInitialization(query)
    }.isEmpty)
    try {
      input.addData(1, 3)
      query.processAllAvailable()
      checkAnswer(spark.table(query.name), Seq(Row("engineering"), Row("research")))
    } finally {
      query.stop()
    }
  }

  test("foreachBatch and toTable stop the query when it starts too") {
    val stream = MemoryStream[Long].toDF().toDF("id")
    val lookup = lookupOnly
    val joined = stream.join(lookup, stream("id") === lookup("id"))
      .select(stream("id"), lookup("department"))
    def failure(query: StreamingQuery): String = try {
      intercept[StreamingQueryException](query.awaitTermination(timeoutMs))
        .getCause.asInstanceOf[AnalysisException].getCondition
    } finally {
      query.stop()
    }
    assert(failure(joined.writeStream.foreachBatch((_: DataFrame, _: Long) => ()).start()) ===
      "EXTERNAL_LOOKUP_JOIN_UNSUPPORTED.NO_LOOKUP_CANDIDATE")
    withTempDir { dir =>
      withTable("startup_check_sink") {
        val query = joined.writeStream.option("checkpointLocation", dir.getCanonicalPath)
          .toTable("startup_check_sink")
        assert(failure(query) === "EXTERNAL_LOOKUP_JOIN_UNSUPPORTED.NO_LOOKUP_CANDIDATE")
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
      // The query thread passes the last entry of the offset log, which it reads anyway.
      def startupConf(): SQLConf = ExternalLookupJoinStartupCheck.startupConf(spark,
        new StreamingQueryCheckpointMetadata(spark, checkpoint, readOnly = true)
          .offsetLog.getLatest().flatMap(_._2.metadataOpt))
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

  test("start() returns while a planning call to the lookup connector blocks") {
    BlockingLookupTable.reset()
    val input = MemoryStream[Int]
    val stream = input.toDF().toDF("id")
    val lookup = spark.read.format(classOf[BlockingLookupDataSource].getName).load()
    val query = start(stream.join(lookup, stream("id") === lookup("id")).select("department"))
    try {
      // start() has returned, and the query thread is in the call, which waits to be released.
      // Had start() planned the lookup itself, the call would have returned, once its wait ran
      // out, before start() did.
      assert(BlockingLookupTable.entered.get().await(timeoutMs, TimeUnit.MILLISECONDS))
      assert(BlockingLookupTable.returned.get() === false)
      assert(query.isActive && query.exception.isEmpty)
      BlockingLookupTable.release.get().countDown()
      input.addData(1, 3)
      query.processAllAvailable()
      checkAnswer(spark.table(query.name), Seq(Row("engineering"), Row("research")))
    } finally {
      BlockingLookupTable.release.get().countDown()
      query.stop()
    }
  }

  test("the error stops the query for awaitTermination(), exception and the listeners") {
    val events = new ConcurrentLinkedQueue[StreamingQueryListener.Event]()
    val listener = new StreamingQueryListener {
      override def onQueryStarted(event: QueryStartedEvent): Unit = events.add(event)
      override def onQueryProgress(event: QueryProgressEvent): Unit = events.add(event)
      override def onQueryTerminated(event: QueryTerminatedEvent): Unit = events.add(event)
    }
    spark.streams.addListener(listener)
    try {
      val stream = MemoryStream[Long].toDF().toDF("id")
      val lookup = lookupOnly
      val query = start(stream.join(lookup, stream("id") === lookup("id")))
      val e = intercept[StreamingQueryException](query.awaitTermination(timeoutMs))
      assert(e.getCause.asInstanceOf[AnalysisException].getCondition ===
        "EXTERNAL_LOOKUP_JOIN_UNSUPPORTED.NO_LOOKUP_CANDIDATE")
      assert(!query.isActive && query.exception.contains(e) && query.lastProgress == null)
      spark.sparkContext.listenerBus.waitUntilEmpty()
      // No micro-batch ran, so there is no progress event.
      val queryEvents = events.asScala.toSeq.filter {
        case started: QueryStartedEvent => started.id == query.id
        case terminated: QueryTerminatedEvent => terminated.id == query.id
        case progress: QueryProgressEvent => progress.progress.id == query.id
      }
      assert(queryEvents.map(_.getClass) ===
        Seq(classOf[QueryStartedEvent], classOf[QueryTerminatedEvent]))
      val terminated = queryEvents(1).asInstanceOf[QueryTerminatedEvent]
      assert(terminated.errorClassOnException ===
        Some("EXTERNAL_LOOKUP_JOIN_UNSUPPORTED.NO_LOOKUP_CANDIDATE"))
      assert(terminated.exception.exists(_.contains("NO_LOOKUP_CANDIDATE")))
    } finally {
      spark.streams.removeListener(listener)
    }
  }

  test("a restarted query plans its lookup joins again and resumes") {
    withTempDir { dir =>
      val input = MemoryStream[Int]
      val stream = input.toDF().toDF("id")
      val lookup = lookupOnly
      val joined = stream.join(lookup, stream("id") === lookup("id")).select("department")
      val rows = new ConcurrentLinkedQueue[Row]()
      def run(data: Int*): Unit = {
        val query = joined.writeStream
          .foreachBatch { (batch: DataFrame, _: Long) =>
            rows.addAll(batch.collect().toSeq.asJava)
            ()
          }
          .option("checkpointLocation", dir.getCanonicalPath)
          .start()
        try {
          input.addData(data)
          query.processAllAvailable()
          assert(query.exception.isEmpty)
        } finally {
          query.stop()
        }
      }
      run(1)
      run(2)
      assert(rows.asScala.toSeq.map(_.getString(0)).sorted ===
        Seq("engineering", "research", "sales"))
    }
  }

  test("a query that does not analyze with the batch timestamp is left to the micro-batch") {
    // A micro-batch analyzes its plan again with its batch timestamp, which fails for the current
    // time in a join condition, as for an ordinary stream-static join. The start leaves that
    // error to the micro-batch, and does not report the lookup error that it would find.
    val input = MemoryStream[Int]
    val stream = input.toDF().toDF("id")
    val lookup = lookupOnly
    val joined = stream.join(lookup, lookup("department") === "sales" &&
      current_timestamp() > lit("2000-01-01 00:00:00").cast("timestamp"))
    var query: StreamingQuery = null
    val warnings = startupWarnings {
      query = start(joined)
      awaitInitialization(query)
    }
    assert(warnings.exists(_.contains("does not pass analysis with the batch timestamp")))
    val e = firstBatchFailure(query, input, 1)
    assert(e.getCause.asInstanceOf[AnalysisException].getCondition ===
      "INVALID_NON_DETERMINISTIC_EXPRESSIONS")
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

class SparkThrowingLookupScanBuilderDataSource extends TableProvider {
  override def inferSchema(options: CaseInsensitiveStringMap): StructType =
    SparkThrowingLookupScanBuilderTable.schema()

  override def getTable(
      schema: StructType,
      partitioning: Array[Transform],
      properties: java.util.Map[String, String]): Table = SparkThrowingLookupScanBuilderTable
}

/** A lookup-only table whose scan builder fails with a SparkThrowable of its own. */
object SparkThrowingLookupScanBuilderTable extends Table with SupportsRead {
  override def name(): String = "SparkThrowingLookupScanBuilderTable"
  override def schema(): StructType = new StructType().add("id", "int")
  override def capabilities(): java.util.Set[TableCapability] =
    java.util.EnumSet.of(EXTERNAL_LOOKUP)
  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder =
    throw new SparkRuntimeException("INTERNAL_ERROR", Map("message" -> "The scan builder fails"))
}

object ThrowingLookupScanBuilderTable extends Table with SupportsRead {
  override def name(): String = "ThrowingLookupScanBuilderTable"
  override def schema(): StructType = new StructType().add("id", "int")
  override def capabilities(): java.util.Set[TableCapability] =
    java.util.EnumSet.of(EXTERNAL_LOOKUP)
  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder =
    throw new IllegalStateException("The scan builder fails")
}

/** Serves the lookup-only table of SimpleExternalLookupDataSource after a call that blocks. */
class BlockingLookupDataSource extends TableProvider {
  override def inferSchema(options: CaseInsensitiveStringMap): StructType =
    BlockingLookupTable.schema()

  override def getTable(
      schema: StructType,
      partitioning: Array[Transform],
      properties: java.util.Map[String, String]): Table = BlockingLookupTable
}

object BlockingLookupTable extends Table with SupportsRead {
  // Counted down when a scan builder is requested; the request waits for `release`.
  val entered = new AtomicReference(new CountDownLatch(1))
  val release = new AtomicReference(new CountDownLatch(1))
  // Whether a request has returned, once released or once its wait ran out.
  val returned = new AtomicBoolean(false)

  def reset(): Unit = {
    entered.set(new CountDownLatch(1))
    release.set(new CountDownLatch(1))
    returned.set(false)
  }

  override def name(): String = "BlockingLookupTable"
  override def schema(): StructType = SimpleExternalLookupTable.schema()
  override def capabilities(): java.util.Set[TableCapability] =
    java.util.EnumSet.of(EXTERNAL_LOOKUP)
  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    entered.get().countDown()
    release.get().await(60, TimeUnit.SECONDS)
    returned.set(true)
    SimpleExternalLookupTable.newScanBuilder(options)
  }
}
