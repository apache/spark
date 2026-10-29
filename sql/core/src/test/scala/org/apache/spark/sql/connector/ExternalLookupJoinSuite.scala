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

import org.apache.spark.{SparkException, SparkIllegalArgumentException,
  SparkUnsupportedOperationException}
import org.apache.spark.sql.{AnalysisException, DataFrame, Encoders, Row}
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, PreparedExternalLookupJoin}
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
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2Relation, ExternalLookupRelation}
import org.apache.spark.sql.execution.joins.ExternalLookupJoinExec
import org.apache.spark.sql.execution.streaming.runtime.{IncrementalExecution, MemoryStream,
  StreamingQueryWrapper}
import org.apache.spark.sql.execution.streaming.sources.LowLatencyMemoryStream
import org.apache.spark.sql.functions.{coalesce, col, concat, length, lit}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.{StreamingQueryException, StreamRealTimeModeE2ESuiteBase}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{DataType, IntegerType, StringType, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.unsafe.types.UTF8String

class ExternalLookupJoinSuite extends QueryTest with SharedSparkSession {

  import testImplicits._

  private val ExternalLookupCatalogName = "external_lookup_cat"

  private val streamRuns = new AtomicInteger()

  /** The rows of `df` as a streaming DataFrame: one micro-batch, in `numPartitions` partitions. */
  private def streamOf(df: DataFrame, numPartitions: Int = 1): DataFrame = {
    val input = MemoryStream[Row](numPartitions)(Encoders.row(df.schema), spark.sqlContext)
    input.addData(df.collect().toSeq)
    input.toDF()
  }

  private case class StreamRun(rows: Seq[Row], execution: IncrementalExecution) {
    def plan: LogicalPlan = execution.optimizedPlan
  }

  /** Runs the streaming `df` on a memory sink until it has processed all of its input. */
  private def runStream(df: DataFrame): StreamRun = {
    val queryName = s"external_lookup_stream_${streamRuns.incrementAndGet()}"
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

    val lookup = spark.read
      .format(classOf[SimpleExternalLookupDataSource].getName)
      .option("nullCursor", "true")
      .load()
    val error = streamFailure[SparkException](left.join(lookup, left("id") === lookup("id")))
    assert(error.getMessage.contains("External lookup returned a null result cursor"))

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

    val invalidOrdinal = misconfigured("invalidOrdinal")
    val ordinalError = streamFailure[SparkException](
      left.join(invalidOrdinal, left("id") === invalidOrdinal("id")))
    assert(ordinalError.getMessage.contains(
      "External lookup returned invalid input ordinal 5 for 1 requested parameter rows"))

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

  test("external lookup maps a selected predicate to its own occurrence") {
    SimpleExternalLookupDataSource.resetCounters()
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
    // Generic column pruning must not project the lookup relation, which would hide it from
    // lookup planning; the join's requiredLookupOutput keeps it from doing so.
    val joined = left.join(lookup, left("id") === lookup("id"), "left_outer")
      .select(left("id"), lookup("department"))

    assert(runStream(joined).plan.exists(_.isInstanceOf[PreparedExternalLookupJoin]))
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
    // selects `id = ?`, and Spark evaluates the other conjuncts on the joined rows.
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
    // A join that reads it but cannot be a lookup join fails with the reason.
    checkError(
      exception = streamFailure[AnalysisException](left.crossJoin(lookup)),
      condition = "EXTERNAL_LOOKUP_JOIN_UNSUPPORTED.MISSING_JOIN_CONDITION",
      parameters = Map.empty)
    val fullOuter = intercept[AnalysisException] {
      left.join(lookup, left("id") === lookup("id"), "full_outer").queryExecution.analyzed
    }
    assert(fullOuter.getCondition === "EXTERNAL_LOOKUP_JOIN_UNSUPPORTED.JOIN_TYPE")
    assert(fullOuter.getMessageParameters.asScala.toMap - "supportedJoinTypes" ===
      Map("joinType" -> "FULL OUTER", "relation" -> "`SimpleExternalLookupTable`"))
  }

  test("a catalog-resolved external lookup-only table cannot be read outside a lookup join") {
    withSQLConf(
        s"spark.sql.catalog.$ExternalLookupCatalogName" ->
          classOf[ExternalLookupTestCatalog].getName) {
      val table = s"$ExternalLookupCatalogName.ns.lookup"
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

  test("external lookup join handles null, missing, duplicate, and partial batches") {
    SimpleExternalLookupDataSource.resetCounters()
    val lookup = spark.read
      .format(classOf[SimpleExternalLookupDataSource].getName)
      .option("maxBatchSize", "3")
      .load()
    val left = streamOf(Seq[java.lang.Integer](1, 1, null, 3, 2).toDF("id"))

    val rows = runStream(left.join(lookup, left("id") === lookup("id"), "left_outer")).rows
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

  test("an upstream limit stops one-to-many cursor consumption") {
    SimpleExternalLookupDataSource.resetCounters()
    SimpleExternalLookupDataSource.values.set(Map(1 -> (1 to 2000).map(_.toString)))
    val lookup = spark.read.format(classOf[SimpleExternalLookupDataSource].getName).load()
    val left = streamOf(Seq(1).toDF("id"))

    assert(runStream(left.join(
      lookup, left("id") === lookup("id"), "inner")
      .limit(1)).rows.size === 1)
    assert(SimpleExternalLookupDataSource.resultRowsRead.get() === 1)
  }

  test("external lookup samples connector metrics while consuming a large response") {
    SimpleExternalLookupDataSource.resetCounters()
    SimpleExternalLookupDataSource.values.set(Map(1 -> (1 to 250).map(_.toString)))
    val lookup = spark.read.format(classOf[SimpleExternalLookupDataSource].getName).load()
    val left = streamOf(Seq(1).toDF("id"))

    assert(runStream(left.join(
      lookup, left("id") === lookup("id"), "inner")).rows.size === 250)
    // Two periodic samples, followed by request-completion and reader-close samples.
    assert(SimpleExternalLookupDataSource.metricSamples.get() >= 4)
  }

  test("external lookup deduplicates floating-point keys within a request") {
    SemanticKeyExternalLookupDataSource.requestSizes.clear()
    val nan1 = java.lang.Double.longBitsToDouble(0x7ff8000000000001L)
    val nan2 = java.lang.Double.longBitsToDouble(0x7ff8000000000002L)
    // Requests hold two rows, so each pair of equivalent keys shares one request row.
    val left = streamOf(Seq(-0.0d, 0.0d, nan1, nan2).toDF("key"))
    val lookup = semanticLookup("double")

    assert(runStream(left.join(lookup, left("key") === lookup("key"))).rows.size === 4)
    assert(SemanticKeyExternalLookupDataSource.requestSizes.asScala.toSeq === Seq(1, 1))
  }

  test("external lookup deduplicates keys using Spark collation equality") {
    SemanticKeyExternalLookupDataSource.requestSizes.clear()
    val left = streamOf(Seq("lookup", "LOOKUP").toDF("key"))
      .selectExpr("CAST(key AS STRING COLLATE UTF8_LCASE) AS key")
    val lookup = semanticLookup("string collate utf8_lcase")

    assert(runStream(left.join(lookup, left("key") === lookup("key"))).rows.size === 2)
    assert(SemanticKeyExternalLookupDataSource.requestSizes.asScala.toSeq === Seq(1))
  }

  private def semanticLookup(keyType: String): DataFrame = {
    spark.read
      .format(classOf[SemanticKeyExternalLookupDataSource].getName)
      .option("keyType", keyType)
      .load()
  }

  test("a join strategy hint on the stream side of a lookup-only join is rejected") {
    SimpleExternalLookupDataSource.resetCounters()
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
    val left = streamOf(Seq(1, 3).toDF("id"))
    Seq(
      "inner" -> Seq.empty[Row],
      "left_outer" -> Seq(Row(1, null, null), Row(3, null, null))).foreach {
      case (joinType, expected) =>
        SimpleExternalLookupDataSource.resetCounters()
        val joined = left.join(lookup, left("id") === lookup("id") && lit(false), joinType)
        assert(!runStream(joined).plan.exists(_.isInstanceOf[PreparedExternalLookupJoin]))
        checkStream(joined, expected)
        assert(SimpleExternalLookupDataSource.lookupCandidates.get() === Seq.empty)
        assert(SimpleExternalLookupDataSource.readerCount.get() === 0)
        assert(SimpleExternalLookupDataSource.requestCount.get() === 0)
    }
  }

  test("external lookup reads VARCHAR columns and nested fields whose nullability differs") {
    def lookup(kind: String): DataFrame = spark.read
      .format(classOf[ReadSchemaExternalLookupDataSource].getName)
      .option("kind", kind)
      .load()
    val left = streamOf(Seq(1, 2).toDF("id"))

    val varchar = lookup("varchar")
    checkStream(
      left.join(varchar, left("id") === varchar("id")),
      Seq(Row(1, 1, "ab"), Row(2, 2, "ab")))
    val nested = lookup("nested")
    checkStream(
      left.join(nested, left("id") === nested("id")),
      Seq(Row(1, 1, Row(10)), Row(2, 2, Row(20))))
    // A field whose type really differs is still rejected.
    val wrongType = lookup("wrongType")
    checkError(
      exception = streamFailure[AnalysisException](
        left.join(wrongType, left("id") === wrongType("id"))),
      condition = "DATA_SOURCE_INVALID_EXTERNAL_LOOKUP.UNKNOWN_SCAN_FIELD",
      parameters = Map("table" -> "`ReadSchemaExternalLookupTable`", "field" -> "`name`"))
  }

  test("external lookup merges the connector metrics of every reader in a task") {
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      val lookup = spark.read.format(classOf[MetricsExternalLookupDataSource].getName).load()
      // Two one-row input partitions, computed by one task under the coalesce, open two readers
      // that send one request each.
      val left = streamOf(spark.range(0, 2).selectExpr("CAST(id + 1 AS INT) AS id"), 2)
      val joined = left.join(lookup, left("id") === lookup("id")).coalesce(1)

      val run = runStream(joined)
      assert(run.rows.length === 2)
      val lookupJoin = run.execution.executedPlan.collectFirst {
        case exec: ExternalLookupJoinExec => exec
      }.get
      assert(lookupJoin.metrics("source.connectorRequests").value === 2)
      // A connector metric named like an operator metric is kept apart from it.
      assert(lookupJoin.metrics("source.numOutputRows").value === 7)
      assert(lookupJoin.metrics("numOutputRows").value === 2)
    }
  }

  test("external lookup closes task-local readers after success and failure") {
    def lookup(options: (String, String)*): DataFrame = {
      var reader = spark.read.format(classOf[SimpleExternalLookupDataSource].getName)
      options.foreach { case (key, value) => reader = reader.option(key, value) }
      reader.load()
    }
    val left = streamOf(Seq(1).toDF("id"))

    SimpleExternalLookupDataSource.resetCounters()
    val successfulLookup = lookup()
    runStream(left.join(successfulLookup, left("id") === successfulLookup("id")))
    assert(SimpleExternalLookupDataSource.readerCount.get() === 1)
    assert(SimpleExternalLookupDataSource.closeCount.get() === 1)

    SimpleExternalLookupDataSource.resetCounters()
    val failingLookup = lookup("failRequests" -> "true")
    val failure = streamFailure[SparkException](
      left.join(failingLookup, left("id") === failingLookup("id")))
    assert(failure.getMessage.contains("injected external lookup failure"))
    assert(SimpleExternalLookupDataSource.readerCount.get() === 1)
    assert(SimpleExternalLookupDataSource.closeCount.get() === 1)
  }

  test("external lookup caps each inner-join request at the engine batch size") {
    SimpleExternalLookupDataSource.resetCounters()
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
    SimpleExternalLookupDataSource.resetCounters()
    val lookup = spark.read
      .format(classOf[SimpleExternalLookupDataSource].getName)
      .option("responseDelayMillis", "60000")
      .load()
    val left = streamOf(Seq(1).toDF("id"))
    // Stopping the query cancels the task that waits for the response.
    val query = left.join(lookup, left("id") === lookup("id"))
      .writeStream.format("memory").queryName("external_lookup_cancellation").start()

    assert(SimpleExternalLookupDataSource.requestStarted.get().await(10, TimeUnit.SECONDS))
    query.stop()
    spark.catalog.dropTempView("external_lookup_cancellation")
    assert(SimpleExternalLookupDataSource.readerCount.get() === 1)
    eventually(timeout(10.seconds)) {
      assert(SimpleExternalLookupDataSource.closeCount.get() === 1)
    }
  }

  test("external lookup join runs unchanged in micro-batch streaming") {
    val input = MemoryStream[Int]
    val left = input.toDF().toDF("id")
    val lookup = spark.read
      .format(classOf[SimpleExternalLookupDataSource].getName)
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
  val lookupCandidates =
    new AtomicReference(Seq.empty[(String, String, DataType)])
  val lookupCandidateDescriptions = new AtomicReference(Seq.empty[String])
  val outputSchema: StructType = new StructType()
    .add("id", "int", nullable = true)
    .add("department", "string", nullable = false)
  private val InitialValues: Map[Int, Seq[String]] = Map(
    1 -> Seq("engineering", "research"),
    2 -> Seq("sales"))
  val values = new AtomicReference(InitialValues)

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
    lookupCandidates.set(Seq.empty)
    lookupCandidateDescriptions.set(Seq.empty)
    values.set(InitialValues)
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
    val failRequests = options.getBoolean("failRequests", false)
    val mutateAfterRequest = options.getBoolean("mutateAfterRequest", false)
    val nullKeySelection = options.getBoolean("nullKeySelection", false)
    val selectionMode = options.getOrDefault("selectionMode", "all")
    val nullCursor = options.getBoolean("nullCursor", false)
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
          excludeResearch = true
          SimpleExternalLookupDataSource.pushedPredicateCount.addAndGet(accepted.length)
          SimpleExternalLookupDataSource.pushedPredicateFields.set(
            accepted.flatMap(fields).map(_.fieldNames().mkString(".")).toSeq)
        }
        rejected
      }
      override def pushedPredicates(): Array[Predicate] = acceptedPredicates
      override def build(): Scan = SimpleExternalLookupScan(
        configuredMaxBatchSize, responseDelayMillis, failRequests,
        mutateAfterRequest, nullCursor, requiredSchema, excludeResearch,
        selectedLookupPredicates)
    }
  }
}

case class SimpleExternalLookupScan(
    configuredMaxBatchSize: Int,
    responseDelayMillis: Long,
    failRequests: Boolean,
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
      failRequests,
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
      private val rows = SimpleExternalLookupDataSource.values.get().toSeq
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
    failRequests: Boolean,
    mutateAfterRequest: Boolean,
    nullCursor: Boolean,
    requiredColumns: Seq[String],
    excludeResearch: Boolean,
    selectedLookupPredicates: Seq[String]) extends ExternalLookupReaderFactory {
  override def createReader(): ExternalLookupReader = new ExternalLookupReader {
    SimpleExternalLookupDataSource.readerCount.incrementAndGet()

    override def lookupBatch(keys: Array[InternalRow]): ExternalLookupResultCursor = {
      SimpleExternalLookupDataSource.requestStarted.get().countDown()
      if (responseDelayMillis > 0) Thread.sleep(responseDelayMillis)
      if (failRequests) throw new IllegalStateException("injected external lookup failure")
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
        val values = SimpleExternalLookupDataSource.values.get().toSeq.map { case (id, values) =>
          Some(id) -> values
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
        SimpleExternalLookupDataSource.values.updateAndGet { current =>
          current.updated(2, Seq("sales-v2"))
        }
      }
      new TestExternalLookupResultCursor(
        results, () => SimpleExternalLookupDataSource.resultRowsRead.incrementAndGet())
    }

    override def currentMetricsValues():
        Array[org.apache.spark.sql.connector.metric.CustomTaskMetric] = {
      SimpleExternalLookupDataSource.metricSamples.incrementAndGet()
      Array.empty
    }

    override def close(): Unit = SimpleExternalLookupDataSource.closeCount.incrementAndGet()
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
    onNext: () => Unit = () => ())
    extends ExternalLookupResultCursor {
  private var current: (Int, InternalRow) = _

  override def next(): Boolean = if (results.hasNext) {
    current = results.next()
    onNext()
    true
  } else {
    current = null
    false
  }

  override def get(): InternalRow = current._2
  override def inputOrdinal(): Int = current._1
  override def close(): Unit = {}
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
    SimpleExternalLookupDataSource.outputSchema

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
    case "wrongType" => StructType.fromDDL("id INT, name STRING")
  }
  override def capabilities(): java.util.Set[TableCapability] =
    java.util.EnumSet.of(EXTERNAL_LOOKUP)

  private def readSchema: StructType = kind match {
    case "nested" => new StructType()
      .add("id", IntegerType)
      .add("s", new StructType().add("a", IntegerType))
    case "wrongType" => StructType.fromDDL("id INT, name INT")
    case _ => schema()
  }

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder =
    new SupportsExternalLookup {
      override def selectLookupPredicates(candidates: Array[Predicate]): Array[Predicate] =
        candidates.filter(_.name() == "=").take(1)
      override def build(): Scan = ReadSchemaExternalLookupScan(readSchema, kind == "nested")
    }
}

case class ReadSchemaExternalLookupScan(schema: StructType, nested: Boolean) extends Scan {
  override def readSchema(): StructType = schema
  override def toExternalLookup(): ExternalLookup =
    TestExternalLookup(100, ReadSchemaExternalLookupReaderFactory(nested))
}

case class ReadSchemaExternalLookupReaderFactory(nested: Boolean)
  extends ExternalLookupReaderFactory {
  override def createReader(): ExternalLookupReader = new ExternalLookupReader {
    override def lookupBatch(keys: Array[InternalRow]): ExternalLookupResultCursor = {
      val results = keys.iterator.zipWithIndex.map { case (key, ordinal) =>
        val id = key.getInt(0)
        ordinal -> (if (nested) InternalRow(id, InternalRow(id * 10))
          else InternalRow(id, UTF8String.fromString("ab")))
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
  override def schema(): StructType = SimpleExternalLookupDataSource.outputSchema
  override def capabilities(): java.util.Set[TableCapability] = kind match {
    case "batchOnly" => java.util.EnumSet.of(BATCH_READ)
    case _ => java.util.EnumSet.of(EXTERNAL_LOOKUP)
  }

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = kind match {
    case "batchOnly" | "noLookupBuilder" => () =>
      SimpleExternalLookupScan(100, 0, failRequests = false, mutateAfterRequest = false,
        nullCursor = false, schema(), excludeResearch = false, Seq("="))
    case _ => new SupportsExternalLookup {
      override def selectLookupPredicates(candidates: Array[Predicate]): Array[Predicate] =
        candidates.take(1)
      override def build(): Scan = kind match {
        case "unknownField" => NoExternalLookupScan(new StructType().add("unknown", "int"))
        case "duplicateField" =>
          NoExternalLookupScan(new StructType().add("id", "int").add("id", "int"))
        case "noExternalLookup" => NoExternalLookupScan(schema())
        case "invalidOrdinal" => new Scan {
          override def readSchema(): StructType = schema()
          override def toExternalLookup(): ExternalLookup =
            TestExternalLookup(10, InvalidOrdinalExternalLookupReaderFactory)
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

object InvalidOrdinalExternalLookupReaderFactory extends ExternalLookupReaderFactory {
  override def createReader(): ExternalLookupReader = new ExternalLookupReader {
    override def lookupBatch(keys: Array[InternalRow]): ExternalLookupResultCursor =
      new TestExternalLookupResultCursor(
        Iterator(5 -> InternalRow(1, UTF8String.fromString("value"))))
    override def close(): Unit = {}
  }
}

case class TestExternalLookup(
    maxRequestSize: Int,
    factory: ExternalLookupReaderFactory) extends ExternalLookup {
  override def maxParameterRowsPerRequest(): Int = maxRequestSize
  override def createReaderFactory(): ExternalLookupReaderFactory = factory
}
