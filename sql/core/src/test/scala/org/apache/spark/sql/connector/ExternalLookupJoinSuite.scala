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
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.catalyst.expressions.{Add, Attribute, AttributeMap, AttributeReference,
  Cast, EqualTo, Expression, GenericInternalRow, LessThan, Literal, Rand, UnsafeProjection}
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.logical.{Deduplicate, LeafNode, LogicalPlan,
  PreparedExternalLookupJoin}
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, GenericArrayData}
import org.apache.spark.sql.classic.Dataset
import org.apache.spark.sql.connector.catalog.{Identifier, InMemoryBaseTable,
  InMemoryTableCatalog, SupportsRead, Table, TableCapability, TableCatalog, TableChange,
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
  FileDataSourceV2, StreamingDataSourceV2ScanRelation, TableCapabilityCheck}
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.execution.joins.ExternalLookupJoinExec
import org.apache.spark.sql.execution.streaming.operators.stateful.StreamingDeduplicateExec
import org.apache.spark.sql.execution.streaming.runtime.{IncrementalExecution, MemoryStream,
  StreamingQueryWrapper}
import org.apache.spark.sql.execution.streaming.sources.LowLatencyMemoryStream
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector
import org.apache.spark.sql.functions.{coalesce, col, concat, expr, length, lit, max, raise_error,
  to_json, when}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.{BaseRelation, CreatableRelationProvider, InsertableRelation,
  RelationProvider, SchemaRelationProvider}
import org.apache.spark.sql.streaming.{StreamingQueryException, StreamRealTimeModeE2ESuiteBase}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{ArrayType, DataType, Decimal, DecimalType, DoubleType,
  IntegerType, MapType, StringType, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.unsafe.types.UTF8String

class ExternalLookupJoinSuite extends QueryTest with SharedSparkSession {

  import testImplicits._

  private val EXTERNAL_LOOKUP_CATALOG_NAME = "external_lookup_cat"
  // The supportedJoinTypes parameter of a JOIN_TYPE error.
  private val SUPPORTED_JOIN_TYPES = "INNER, LEFT OUTER"

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
    // cannot match, so its key is not requested: an inner join drops the row, and a left outer
    // join pads it with nulls.
    val matched = Seq(Row(1, true, 1, "engineering"), Row(1, true, 1, "research"))
    val unmatched =
      Seq(Row(2, false, null, null), Row(3, true, null, null), Row(4, null, null, null))
    Seq("inner" -> matched, "left_outer" -> (matched ++ unmatched)).foreach {
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
    Seq("inner" -> matched, "left_outer" -> (matched :+ Row(3, "Ab-1", null, null))).foreach {
      case (joinType, expected) =>
        CaseInsensitiveExternalLookupTable.returnedRows.set(0)
        checkStream(orders.join(products, orders("sku") === products("sku"), joinType), expected)
        assert(CaseInsensitiveExternalLookupTable.returnedRows.get() === 6)
    }
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

  test("a lookup-only table that needs read-side CHAR or VARCHAR handling is rejected") {
    val parameters = Map("relation" -> "`ReadSchemaExternalLookupTable`", "column" -> "`name`")
    checkError(
      exception = intercept[AnalysisException](readSchemaLookup("char")),
      condition = "EXTERNAL_LOOKUP_JOIN_UNSUPPORTED.CHAR_VARCHAR_COLUMN",
      parameters = parameters)
    withSQLConf(SQLConf.CHAR_VARCHAR_STANDARD_SEMANTICS.key -> "true") {
      checkError(
        exception = intercept[AnalysisException](readSchemaLookup("varchar")),
        condition = "EXTERNAL_LOOKUP_JOIN_UNSUPPORTED.CHAR_VARCHAR_COLUMN",
        parameters = parameters)
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
      // Standard semantics apply the read-side padding anyway, so the table is still rejected.
      withSQLConf(SQLConf.CHAR_VARCHAR_STANDARD_SEMANTICS.key -> "true") {
        checkError(
          exception = intercept[AnalysisException](readSchemaLookup("char")),
          condition = "EXTERNAL_LOOKUP_JOIN_UNSUPPORTED.CHAR_VARCHAR_COLUMN",
          parameters =
            Map("relation" -> "`ReadSchemaExternalLookupTable`", "column" -> "`name`"))
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

    // An inner join evaluates it above the lookup join; a left outer join cannot.
    checkStream(left.join(lookup, condition), Seq(Row(1, 1, "engineering")))
    checkError(
      exception = streamFailure[AnalysisException](left.join(lookup, condition, "left_outer")),
      condition = "UNSUPPORTED_FEATURE.PYTHON_UDF_IN_ON_CLAUSE",
      parameters = Map("joinType" -> "LEFT OUTER"))
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

  test("external lookup caps each inner-join request at the engine batch size") {
    val lookup = spark.read
      .format(classOf[SimpleExternalLookupDataSource].getName)
      .option("maxBatchSize", "5000")
      .load()
    val left = streamOf((1 to 1500).toDF("id"))

    assert(runStream(left.join(lookup, left("id") === lookup("id"))).rows.size === 3)
    assert(SimpleExternalLookupDataSource.requestSizes.asScala.toSeq === Seq(1000, 500))
  }

  test("an inner join batches only the stream rows that can match") {
    val lookup = spark.read
      .format(classOf[SimpleExternalLookupDataSource].getName)
      .option("maxBatchSize", "5000")
      .load()
    // One badge swipe in 100 carries an employee id. An inner join emits nothing for the others,
    // so its batches leave them out, and one request serves the 30 swipes with an id. A left outer
    // join emits them, so its batches of 1000 rows keep them, and each sends a request.
    val swipes = streamOf((1 to 3000).map { swipe =>
      (swipe, if (swipe % 100 == 0) Some(swipe / 100 % 2 + 1) else None)
    }.toDF("swipe", "employee"))
    Seq("inner" -> (45, 1), "left_outer" -> (3015, 3)).foreach {
      case (joinType, (outputRows, requests)) =>
        SimpleExternalLookupDataSource.resetCounters()
        val run = runStream(swipes.join(lookup, swipes("employee") === lookup("id"), joinType))
        assert(run.rows.size === outputRows, joinType)
        assert(SimpleExternalLookupDataSource.requestCount.get() === requests, joinType)
        assert(run.lookupJoin.metrics("numInputRows").value === 3000, joinType)
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
      SimpleExternalLookupDataSource.beforeMetricsReturn.get()()
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
    TypedKeyExternalLookupTable(new CaseInsensitiveStringMap(properties).get("keyType"))
}

/**
 * A lookup-only table keyed on a column of the given type, whose rows the test sets. For each
 * parameter row, its source returns the stored rows with that key, and one row with another key.
 */
case class TypedKeyExternalLookupTable(keyType: String) extends Table with SupportsRead {
  override def name(): String = "TypedKeyExternalLookupTable"
  override def schema(): StructType = TypedKeyExternalLookupTable.schema(keyType)
  override def capabilities(): java.util.Set[TableCapability] =
    java.util.EnumSet.of(EXTERNAL_LOOKUP)

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder =
    new SupportsExternalLookup {
      override def selectLookupPredicates(candidates: Array[Predicate]): Array[Predicate] =
        candidates.filter(_.name() == "=").take(1)
      override def build(): Scan = new Scan {
        override def readSchema(): StructType = schema()
        override def toExternalLookup(): ExternalLookup = TestExternalLookup(
          100, TypedKeyExternalLookupReaderFactory(DataType.fromDDL(keyType)))
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

case class TypedKeyExternalLookupReaderFactory(keyType: DataType)
  extends ExternalLookupReaderFactory {
  override def createReader(): ExternalLookupReader = new ExternalLookupReader {
    private def sameKey(first: Any, second: Any): Boolean =
      EqualTo(Literal(first, keyType), Literal(second, keyType)).eval() == true

    override def lookupBatch(keys: Array[InternalRow]): ExternalLookupResultCursor = {
      TypedKeyExternalLookupTable.requestSizes.add(keys.length)
      val stored = TypedKeyExternalLookupTable.storedRows.get()
      val results = keys.iterator.zipWithIndex.flatMap { case (key, ordinal) =>
        val parameter = key.get(0, keyType)
        val matches = stored.filter { case (storedKey, _) => sameKey(storedKey, parameter) }
        val other = (stored.map(_._1) ++ TypedKeyExternalLookupTable.otherKeys.get())
          .find(!sameKey(_, parameter)).map(_ -> "other")
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
