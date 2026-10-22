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

import scala.collection.mutable

import org.apache.spark.sql.{DataFrame, QueryTest}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeMap, AttributeReference, AttributeSet, BloomFilterMightContain, Cast, Divide, EvalMode, GreaterThan, If, Literal, NumericEvalContext, Rand}
import org.apache.spark.sql.execution.{FilterExec, SparkPlan}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.datasources.{FileFormat, StorageFilterPlanning}
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.execution.joins.SortMergeJoinExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.connector.StorageFilterImpl
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{DoubleType, LongType}

/**
 * Covers the storage-filter contract for a data source v2 scan. With
 * `V2_STORAGE_FILTER_PUSHDOWN_ENABLED` on, a scan that declares columns through
 * `SupportsStorageFilters` gets the runtime bloom filter over those columns in every reader. The
 * bloom stays in the Filter above the scan, skipped on the rows the reader marks as checked.
 */
class StorageFilterPushDownSuite
  extends QueryTest with SharedSparkSession with AdaptiveSparkPlanHelper {

  private def sourceName: String = classOf[StorageFilterTestSource].getName

  // Runs `body` with runtime blooms and V2 storage filters on, `conf` on top, and the build table.
  private def withBloomJoin(conf: (String, String)*)(body: => Unit): Unit = {
    val bloomJoinConf = Seq(
      SQLConf.V2_STORAGE_FILTER_PUSHDOWN_ENABLED.key -> "true",
      SQLConf.RUNTIME_BLOOM_FILTER_ENABLED.key -> "true",
      SQLConf.RUNTIME_BLOOM_FILTER_APPLICATION_SIDE_SCAN_SIZE_THRESHOLD.key -> "1000",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false")
    withSQLConf(bloomJoinConf ++ conf: _*) {
      withTable("bf_build") {
        spark.range(0, 30).selectExpr("id AS bk", "id AS bv")
          .write.format("parquet").saveAsTable("bf_build")
        body
      }
    }
  }

  // A join whose build side is small and carries a selective predicate, which is the shape
  // InjectRuntimeFilter builds a bloom filter for. The bloom lands on `probe`'s `k`.
  private def bloomJoin(probe: DataFrame): DataFrame = {
    val build = spark.table("bf_build").where("bv = 5")
    probe.join(build, probe("k") === build("bk")).select(probe.columns.map(probe(_)).toSeq: _*)
  }

  private def v2Probe(options: (String, String)*): DataFrame =
    spark.read.format(sourceName).options(options.toMap).load()

  private val checkedName = FileFormat.STORAGE_FILTER_CHECKED_COLUMN_NAME

  private def scanOf(plan: SparkPlan): BatchScanExec =
    collect(plan) { case b: BatchScanExec => b }.headOption
      .getOrElse(fail(s"no BatchScanExec in plan:\n$plan"))

  // The blooms the Filters above the scan evaluate on every row, and those they skip on the rows
  // the reader marks as checked.
  private def bloomsAbove(plan: SparkPlan): (Int, Int) = {
    val conditions = collect(plan) { case f: FilterExec => f.condition }
    val all = conditions.map(_.collect { case _: BloomFilterMightContain => 1 }.sum).sum
    val skipped = conditions.map(_.collect {
      case If(a: Attribute, Literal.TrueLiteral, _: BloomFilterMightContain)
        if a.name == checkedName => 1
    }.sum).sum
    (all - skipped, skipped)
  }

  // The rows the Filter on the scan returned, counted before the join.
  private def rowsOutOfFilter(plan: SparkPlan): Long = {
    val filters = collect(plan) {
      case f: FilterExec if f.exists(_.isInstanceOf[BatchScanExec]) => f
    }
    assert(filters.size == 1, s"expected one Filter on the scan:\n$plan")
    filters.head.metrics("numOutputRows").value
  }

  // Runs the bloom join over the v2 source read with `options`, and returns its rows and plan.
  private def runBloomJoin(options: (String, String)*): (Set[(Long, Long)], SparkPlan) = {
    StorageFilterTestSource.reset()
    val df = bloomJoin(v2Probe(options: _*))
    (df.collect().map(r => (r.getLong(0), r.getLong(1))).toSet, df.queryExecution.executedPlan)
  }

  // The build side keeps `bv = 5`, so the join returns that one row.
  private val joinResult = Set((5L, 50L))

  for (columnar <- Seq(false, true); aqe <- Seq(false, true)) {
    val kind = if (columnar) "columnar" else "row-based"
    test(s"a $kind scan gets the checked column, and the Filter skips the bloom on checked rows " +
        s"(AQE $aqe)") {
      withBloomJoin(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> aqe.toString) {
        val (result, plan) = runBloomJoin("columnar" -> columnar.toString)
        val scan = scanOf(plan)
        assert(scan.supportsColumnar == columnar, s"the scan must read $kind:\n$plan")
        assert(scan.storageFilters.size == 1,
          s"the bloom filter must be on the scan; got ${scan.storageFilters}\n$plan")
        assert(scan.checkedColumn.isDefined && scan.checkedColumn == scan.output.lastOption,
          s"the scan must get the checked column, last:\n$plan")
        assert(bloomsAbove(plan) == (0, 1),
          s"the Filter must skip the bloom on checked rows:\n$plan")
        assert(rowsOutOfFilter(plan) == StorageFilterTestSource.rowsReturned.get(),
          s"the Filter must keep every checked row; got ${rowsOutOfFilter(plan)} of " +
            s"${StorageFilterTestSource.rowsReturned.get()}")
        val probeSide = collectFirst(plan) { case j: SortMergeJoinExec => j.left.output }
        assert(probeSide.exists(!_.exists(_.name == checkedName)),
          s"the Project above the scan must drop the checked column; got $probeSide")
        assert(StorageFilterTestSource.filteredReaders.get() == 1,
          s"the one partition's reader must get the filter; " +
            s"got ${StorageFilterTestSource.filteredReaders.get()}")
        assert(StorageFilterTestSource.checkedColumnRequests.get() == 1,
          "the reader must be asked for the checked column")
        assert(result == joinResult, s"got $result")
        assert(StorageFilterTestSource.rowsEvaluated.get() == StorageFilterTestSource.numRows,
          s"the reader must evaluate every row; got ${StorageFilterTestSource.rowsEvaluated.get()}")
        assert(StorageFilterTestSource.rowsReturned.get() < StorageFilterTestSource.numRows,
          "the reader must have dropped rows, otherwise the filter did nothing")
      }
    }
  }

  Seq(false, true).foreach { columnar =>
    val kind = if (columnar) "columnar" else "row-based"
    test(s"a $kind reader that gives the filter up marks its rows unchecked, and the Filter " +
        "filters them") {
      withBloomJoin() {
        val (result, plan) = runBloomJoin("columnar" -> columnar.toString, "giveUp" -> "true")
        assert(bloomsAbove(plan) == (0, 1), s"the plan is that of a filtering reader:\n$plan")
        assert(StorageFilterTestSource.rowsEvaluated.get() == 0,
          s"the reader must evaluate nothing; got ${StorageFilterTestSource.rowsEvaluated.get()}")
        assert(StorageFilterTestSource.rowsReturned.get() == StorageFilterTestSource.numRows,
          s"the reader must return every row; got ${StorageFilterTestSource.rowsReturned.get()}")
        assert(rowsOutOfFilter(plan) < StorageFilterTestSource.numRows,
          s"the Filter must apply the bloom to unchecked rows; got ${rowsOutOfFilter(plan)}")
        assert(result == joinResult, s"got $result")
      }
    }

    test(s"the Filter takes a $kind reader's word on the rows it marks checked") {
      // This reader breaks the contract on purpose: it applies no filter but marks every row
      // checked. Only skipping the bloom on those rows keeps all of them.
      withBloomJoin() {
        val (result, plan) =
          runBloomJoin("columnar" -> columnar.toString, "markAllChecked" -> "true")
        assert(rowsOutOfFilter(plan) == StorageFilterTestSource.numRows,
          s"the Filter must keep every row marked checked; got ${rowsOutOfFilter(plan)}")
        assert(result == joinResult, s"the join must still filter; got $result")
      }
    }
  }

  test("a filter that cannot be applied is given up, and its rows come back unchecked") {
    // `1 / k` under ANSI throws at k = 0, the partition's first row, so the reader gives the
    // filter up before it keeps or drops anything.
    val k = AttributeReference("k", LongType)()
    val inverse = Divide(Literal(1.0), Cast(k, DoubleType), NumericEvalContext(EvalMode.ANSI))
    val filter = StorageFilterImpl(GreaterThan(inverse, Literal(0.0)), Seq(k))
    val reader = new StorageFilterTestColumnarReader(Some(filter), appendsChecked = true)
    val rows = mutable.ArrayBuffer.empty[(Long, Boolean)]
    try {
      while (reader.next()) {
        val batch = reader.get()
        (0 until batch.numRows()).foreach { i =>
          rows += ((batch.column(0).getLong(i), batch.column(2).getBoolean(i)))
        }
      }
    } finally {
      reader.close()
    }
    assert(rows == (0L until StorageFilterTestSource.numRows.toLong).map(_ -> false),
      s"expected every row, unchecked; got ${rows.take(5)} and ${rows.size} rows in all")
  }

  // Under a limit of two fields, the checked column would make three.
  Seq(
    ("too wide for whole-stage codegen", SQLConf.WHOLESTAGE_MAX_NUM_FIELDS.key -> "2"),
    ("without whole-stage codegen", SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "false")
  ).foreach { case (when, conf) =>
    test(s"a scan $when gets the bloom without the checked column") {
      withBloomJoin(conf) {
        val (result, plan) = runBloomJoin("columnar" -> "true")
        val scan = scanOf(plan)
        assert(scan.storageFilters.size == 1,
          s"the bloom filter must be on the scan; got ${scan.storageFilters}\n$plan")
        assert(scan.checkedColumn.isEmpty, s"the scan must not get the checked column:\n$plan")
        assert(StorageFilterTestSource.checkedColumnRequests.get() == 0,
          "the reader must not be asked for the checked column")
        assert(bloomsAbove(plan) == (1, 0), s"the Filter must keep the bloom as it is:\n$plan")
        assert(result == joinResult, s"got $result")
        assert(StorageFilterTestSource.rowsReturned.get() < StorageFilterTestSource.numRows,
          "the reader must have dropped rows, otherwise the filter did nothing")
      }
    }
  }

  Seq(
    ("with the conf off", Seq(SQLConf.V2_STORAGE_FILTER_PUSHDOWN_ENABLED.key -> "false"), Nil),
    ("to a scan that declares no column", Nil, Seq("declared" -> "")),
    ("to a scan that declares only another column", Nil, Seq("declared" -> "v")),
    ("to a scan that declares only a nested field", Nil, Seq("declared" -> "k.x")),
    ("to a scan with a column named like the checked column", Nil,
      Seq("valueName" -> checkedName.toUpperCase(java.util.Locale.ROOT)))
  ).foreach { case (when, conf, options) =>
    test(s"nothing is offered $when") {
      withBloomJoin(conf: _*) {
        val (result, plan) = runBloomJoin(options :+ ("columnar" -> "true"): _*)
        val scan = scanOf(plan)
        assert(scan.storageFilters.isEmpty && scan.checkedColumn.isEmpty,
          s"the scan must get no storage filter:\n$plan")
        assert(bloomsAbove(plan) == (1, 0), s"the Filter must keep the bloom as it is:\n$plan")
        assert(StorageFilterTestSource.filteredReaders.get() == 0,
          "no reader may get a storage filter")
        assert(result == joinResult, s"got $result")
      }
    }
  }

  test("the storage filter shows in the scan description and separates canonicalized plans") {
    withBloomJoin() {
      val plan = bloomJoin(v2Probe("columnar" -> "true")).queryExecution.executedPlan
      val scan = scanOf(plan)
      assert(scan.simpleString(100).contains("StorageFilters:"),
        s"the scan description must name the filters: ${scan.simpleString(100)}")
      assert(scan.verboseStringWithOperatorId().contains("StorageFilters:"),
        s"EXPLAIN FORMATTED must name the filters: ${scan.verboseStringWithOperatorId()}")

      val plain = scan.copy(storageFilters = Nil)
      assert(scan != plain, "equals must see storageFilters")
      assert(!scan.sameResult(plain),
        s"a filtering scan must not be reusable as a plain one:\n${scan.canonicalized}\n" +
          s"${plain.canonicalized}")
      assert(!plain.simpleString(100).contains("StorageFilters:"),
        s"a plain scan's description must not name storage filters: ${plain.simpleString(100)}")
      assert(!plain.verboseStringWithOperatorId().contains("StorageFilters:"),
        s"nor its EXPLAIN FORMATTED: ${plain.verboseStringWithOperatorId()}")

      // As another relation instance of the same table gets, with fresh exprIds.
      val fresh = AttributeMap(scan.output.map(a => a -> a.newInstance()))
      val twin = scan.copy(
        output = scan.output.map(fresh(_)),
        checkedColumn = scan.checkedColumn.map(fresh(_)),
        storageFilters = scan.storageFilters.map(_.transform {
          case a: Attribute => fresh.getOrElse(a, a)
        }))
      assert(scan.sameResult(twin),
        s"the same filtering scan must be reusable:\n${scan.canonicalized}\n${twin.canonicalized}")
    }
  }

  test("a declared column the scan does not read is ignored") {
    withBloomJoin() {
      val (result, plan) = runBloomJoin("columnar" -> "true", "declared" -> "k,w")
      assert(scanOf(plan).storageFilters.size == 1,
        s"the bloom filter must be on the scan:\n$plan")
      assert(result == joinResult, s"got $result")
    }
  }

  test("a non-deterministic conjunct is not offered") {
    // The post-scan Filter takes the reader's word on the rows it checked, so the reader's
    // evaluation must be the one the Filter would make.
    val k = AttributeReference("k", LongType)()
    val v = AttributeReference("v", LongType)()
    val deterministic = GreaterThan(k, Literal(0L))
    val nonDeterministic = GreaterThan(k, Cast(Rand(Literal(0L)), LongType))
    val offered = StorageFilterPlanning.offeredConjuncts(
      Seq(deterministic, nonDeterministic), AttributeSet(k), AttributeSet(Seq(k, v)), _ => true)
    assert(offered == Seq(deterministic), s"got $offered")
  }
}
