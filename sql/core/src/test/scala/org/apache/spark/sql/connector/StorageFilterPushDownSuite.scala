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

import org.apache.spark.SparkException
import org.apache.spark.sql.{DataFrame, QueryTest}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeMap, AttributeReference, AttributeSet, BloomFilterMightContain, Cast, Divide, EvalMode, GetStructField, GreaterThan, If, LessThanOrEqual, Literal, NumericEvalContext, PredicateHelper, Rand, XxHash64}
import org.apache.spark.sql.catalyst.plans.logical.ReturnAnswer
import org.apache.spark.sql.execution.{BaseLimitExec, CoalesceExec, ExecSubqueryExpression, FilterExec, SampleExec, SparkPlan, TakeOrderedAndProjectExec}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.datasources.{FileFormat, StorageFilterPlanning}
import org.apache.spark.sql.execution.datasources.v2.{BatchScanExec, DataSourceV2ScanRelation}
import org.apache.spark.sql.execution.joins.SortMergeJoinExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.connector.StorageFilterImpl
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{BinaryType, DoubleType, LongType, StructType}

/**
 * Covers the storage-filter contract for a data source v2 scan. With
 * `V2_STORAGE_FILTER_PUSHDOWN_ENABLED` on, a scan that declares columns through
 * `SupportsStorageFilters` gets the runtime bloom filter over those columns in every reader. The
 * bloom stays in the Filter above the scan, skipped on the rows the reader marks as checked.
 */
class StorageFilterPushDownSuite
  extends QueryTest with SharedSparkSession with AdaptiveSparkPlanHelper with PredicateHelper {

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

  // The scan offered nothing, and the Filter evaluates the bloom on every row.
  private def assertNothingOffered(plan: SparkPlan): Unit = {
    val scan = scanOf(plan)
    assert(scan.storageFilters.isEmpty, s"the scan must get no storage filter:\n$plan")
    assert(bloomsAbove(plan) == (1, 0), s"the Filter must keep the bloom as it is:\n$plan")
  }

  // The rows the Filter on the scan returned, counted before the join.
  private def rowsOutOfFilter(plan: SparkPlan): Long = {
    val filters = collect(plan) {
      case f: FilterExec if f.exists(_.isInstanceOf[BatchScanExec]) => f
    }
    assert(filters.size == 1, s"expected one Filter on the scan:\n$plan")
    filters.head.metrics("numOutputRows").value
  }

  // Runs `df`, checks that it returns `expected`, and returns its plan.
  private def checkJoin(df: DataFrame, expected: Set[Seq[Long]]): SparkPlan = {
    val result = df.collect().map(_.toSeq.map(_.asInstanceOf[Long])).toSet
    assert(result == expected, s"got $result")
    df.queryExecution.executedPlan
  }

  // The build side keeps `bv = 5`, so the join returns that one row.
  private val joinResult = Set(Seq(5L, 50L))

  // Runs the bloom join over the v2 source read with `options`, checks its rows, and returns its
  // plan.
  private def runBloomJoin(options: (String, String)*): SparkPlan = {
    StorageFilterTestSource.reset()
    checkJoin(bloomJoin(v2Probe(options: _*)), joinResult)
  }

  for (columnar <- Seq(false, true); aqe <- Seq(false, true)) {
    val kind = if (columnar) "columnar" else "row-based"
    test(s"a $kind scan gets the checked column, and the Filter skips the bloom on checked rows " +
        s"(AQE $aqe)") {
      withBloomJoin(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> aqe.toString) {
        val plan = runBloomJoin("columnar" -> columnar.toString)
        val scan = scanOf(plan)
        assert(scan.supportsColumnar == columnar, s"the scan must read $kind:\n$plan")
        assert(scan.storageFilters.size == 1,
          s"the bloom filter must be on the scan; got ${scan.storageFilters}\n$plan")
        assert(scan.appendsCheckedColumn, s"the scan must get the checked column:\n$plan")
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
        val plan = runBloomJoin("columnar" -> columnar.toString, "giveUp" -> "true")
        assert(bloomsAbove(plan) == (0, 1), s"the plan is that of a filtering reader:\n$plan")
        assert(StorageFilterTestSource.rowsEvaluated.get() == 0,
          s"the reader must evaluate nothing; got ${StorageFilterTestSource.rowsEvaluated.get()}")
        assert(StorageFilterTestSource.rowsReturned.get() == StorageFilterTestSource.numRows,
          s"the reader must return every row; got ${StorageFilterTestSource.rowsReturned.get()}")
        assert(rowsOutOfFilter(plan) < StorageFilterTestSource.numRows,
          s"the Filter must apply the bloom to unchecked rows; got ${rowsOutOfFilter(plan)}")
      }
    }

    test(s"the Filter takes a $kind reader's word on the rows it marks checked") {
      // This reader breaks the contract on purpose: it applies no filter but marks every row
      // checked. Only skipping the bloom on those rows keeps all of them.
      withBloomJoin() {
        // The join must still filter, which `runBloomJoin` checks.
        val plan = runBloomJoin("columnar" -> columnar.toString, "markAllChecked" -> "true")
        assert(rowsOutOfFilter(plan) == StorageFilterTestSource.numRows,
          s"the Filter must keep every row marked checked; got ${rowsOutOfFilter(plan)}")
      }
    }

    Seq(false, true).foreach { aqe =>
      test(s"a $kind reader that leaves out the checked column it was asked for fails the task, " +
          s"from any row or batch (AQE $aqe)") {
        // A reader that gives the filter up returns every row, so it returns several batches.
        Seq(Seq("dropChecked" -> "true"), Seq("dropChecked" -> "later", "giveUp" -> "true"))
          .foreach { options =>
            withBloomJoin(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> aqe.toString) {
              StorageFilterTestSource.reset()
              val df = bloomJoin(v2Probe(options :+ ("columnar" -> columnar.toString): _*))
              // `k`, `v` and the checked column.
              checkError(
                exception = intercept[SparkException](df.collect()),
                condition = "DATA_SOURCE_STORAGE_FILTER_CHECKED_COLUMN_MISSING",
                sqlState = Some("KD000"),
                parameters = Map(
                  "class" -> classOf[StorageFilterTestReaderFactory].getName,
                  "expected" -> "3",
                  "actual" -> "2"))
            }
          }
      }
    }

    test(s"a $kind reader may return more columns after the checked column") {
      withBloomJoin() {
        val plan = runBloomJoin("columnar" -> columnar.toString, "checkedLayout" -> "extra")
        assert(bloomsAbove(plan) == (0, 1),
          s"the Filter must skip the bloom on checked rows:\n$plan")
        assert(rowsOutOfFilter(plan) == StorageFilterTestSource.rowsReturned.get(),
          s"the Filter must keep every checked row; got ${rowsOutOfFilter(plan)} of " +
            s"${StorageFilterTestSource.rowsReturned.get()}")
      }
    }

    test(s"a $kind test reader gives a filter up when it throws, and returns its rows unchecked") {
      // `1 / k` under ANSI throws at k = 0, the partition's first row, so the reader gives the
      // filter up before it keeps or drops anything. No runtime bloom throws, so the reader is
      // driven by hand.
      val k = AttributeReference("k", LongType)()
      val inverse = Divide(Literal(1.0), Cast(k, DoubleType), NumericEvalContext(EvalMode.ANSI))
      val filter = StorageFilterImpl(GreaterThan(inverse, Literal(0.0)), Seq(k))
      val partition = RangeInputPartition(0, StorageFilterTestSource.numRows)
      val schema = new StructType().add("k", LongType).add("v", LongType)
      val rows = mutable.ArrayBuffer.empty[(Long, Boolean)]
      if (columnar) {
        val reader = new StorageFilterTestColumnarReader(
          partition, schema, Some(filter), appendsChecked = true)
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
      } else {
        val reader =
          new StorageFilterTestReader(partition, schema, Some(filter), appendsChecked = true)
        while (reader.next()) {
          rows += ((reader.get().getLong(0), reader.get().getBoolean(2)))
        }
      }
      assert(rows == (0L until StorageFilterTestSource.numRows.toLong).map(_ -> false),
        s"expected every row, unchecked; got ${rows.take(5)} and ${rows.size} rows in all")
    }
  }

  test("a columnar reader that returns the checked column ahead of the others fails the task") {
    // The batch is as wide as asked for, but its last column is not a boolean. A row carries no
    // types, so a row-based reader's mistake goes unnoticed.
    withBloomJoin() {
      StorageFilterTestSource.reset()
      val df = bloomJoin(v2Probe("columnar" -> "true", "checkedLayout" -> "front"))
      checkError(
        exception = intercept[SparkException](df.collect()),
        condition = "DATA_SOURCE_STORAGE_FILTER_CHECKED_COLUMN_MISSING",
        sqlState = Some("KD000"),
        parameters = Map(
          "class" -> classOf[StorageFilterTestReaderFactory].getName,
          "expected" -> "3",
          "actual" -> "3"))
    }
  }

  // Under a limit of two fields, the checked column would make three.
  Seq(
    ("too wide for whole-stage codegen", SQLConf.WHOLESTAGE_MAX_NUM_FIELDS.key -> "2"),
    ("without whole-stage codegen", SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "false")
  ).foreach { case (when, conf) =>
    test(s"a scan $when gets the bloom without the checked column") {
      withBloomJoin(conf) {
        val plan = runBloomJoin("columnar" -> "true")
        val scan = scanOf(plan)
        assert(scan.storageFilters.size == 1,
          s"the bloom filter must be on the scan; got ${scan.storageFilters}\n$plan")
        assert(!scan.appendsCheckedColumn, s"the scan must not get the checked column:\n$plan")
        assert(StorageFilterTestSource.checkedColumnRequests.get() == 0,
          "the reader must not be asked for the checked column")
        assert(bloomsAbove(plan) == (1, 0), s"the Filter must keep the bloom as it is:\n$plan")
        assert(StorageFilterTestSource.rowsReturned.get() < StorageFilterTestSource.numRows,
          "the reader must have dropped rows, otherwise the filter did nothing")
      }
    }
  }

  Seq(
    ("with the conf off", Seq(SQLConf.V2_STORAGE_FILTER_PUSHDOWN_ENABLED.key -> "false"), Nil),
    ("to a scan that declares no column", Nil, Seq("declared" -> "")),
    ("to a scan that declares only another column", Nil, Seq("declared" -> "v")),
    ("to a scan that declares only a nested field", Nil, Seq("declared" -> "k.x"))
  ).foreach { case (when, conf, options) =>
    test(s"nothing is offered $when") {
      withBloomJoin(conf: _*) {
        val plan = runBloomJoin(options :+ ("columnar" -> "true"): _*)
        assertNothingOffered(plan)
        assert(StorageFilterTestSource.filteredReaders.get() == 0,
          "no reader may get a storage filter")
      }
    }
  }

  test("a scan that reads only the key and a metadata column is offered nothing") {
    // Only the data columns count as columns left to skip, as on the file-source side.
    withBloomJoin() {
      val source = v2Probe("columnar" -> "true", "metadataColumn" -> "true")
      val pos = StorageFilterTestSource.posName
      StorageFilterTestSource.reset()
      assertNothingOffered(
        checkJoin(bloomJoin(source.select(source("k"), source.metadataColumn(pos))),
          Set(Seq(5L, 5L))))
      StorageFilterTestSource.reset()
      val withValue = checkJoin(
        bloomJoin(source.select(source("k"), source("v"), source.metadataColumn(pos))),
        Set(Seq(5L, 50L, 5L)))
      assert(scanOf(withValue).storageFilters.size == 1,
        s"with `v` left to skip, the bloom must be offered:\n$withValue")
    }
  }

  test("a variant scan that reads only its key column and a metadata column is offered nothing") {
    // The key is an extraction of the declared variant column, so that column is the scan's only
    // data column, and the scan's output has fresh exprIds.
    withBloomJoin() {
      val pos = StorageFilterTestSource.posName
      val probe = v2Probe("columnar" -> "true", "variant" -> "true", "metadataColumn" -> "true",
        "declared" -> "j").selectExpr("variant_get(j, '$.a', 'long') AS a", pos)
      val build = spark.table("bf_build").where("bv = 5")
      val df = probe.join(build, probe("a") === build("bk") * 10).select(probe("a"), probe(pos))
      StorageFilterTestSource.reset()
      assertNothingOffered(checkJoin(df, Set(Seq(50L, 5L))))
    }
  }

  test("a scan that took a pushed variant extraction gets the bloom") {
    // The extraction keeps the scan's rows.
    withBloomJoin() {
      StorageFilterTestSource.reset()
      val probe = v2Probe("columnar" -> "true", "variant" -> "true")
        .selectExpr("k", "variant_get(j, '$.a', 'long') AS a")
      val plan = checkJoin(bloomJoin(probe), joinResult)
      val scan = scanOf(plan)
      assert(scan.output.exists(_.dataType.isInstanceOf[StructType]),
        s"test setup: the scan must read the extraction:\n$plan")
      assert(scan.storageFilters.size == 1,
        s"the bloom filter must be on the scan; got ${scan.storageFilters}\n$plan")
      assert(StorageFilterTestSource.rowsReturned.get() < StorageFilterTestSource.numRows,
        "the reader must have dropped rows, otherwise the filter did nothing")
    }
  }

  for (aqe <- Seq(false, true)) {
    test("a scan with a column named like the checked column gets the filter, and runtime " +
        s"filtering still resolves that column (AQE $aqe)") {
      // Runtime filtering resolves the declared filter attributes by name, which the checked column
      // the scan appends would make ambiguous. Dynamic partition pruning on `k` would take the
      // bloom's place.
      withBloomJoin(SQLConf.DYNAMIC_PARTITION_PRUNING_ENABLED.key -> "false",
          SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> aqe.toString) {
        val plan = runBloomJoin(
          "columnar" -> "true", "valueName" -> checkedName, "runtimeFiltering" -> "v2")
        val scan = scanOf(plan)
        assert(scan.storageFilters.size == 1 && scan.appendsCheckedColumn,
          s"the scan must get the bloom and the checked column:\n$plan")
        assert(StorageFilterTestSource.runtimeFilterCalls.get() > 0,
          "test setup: the bloom must reach runtime filtering")
        val probeSide = collectFirst(plan) { case j: SortMergeJoinExec => j.left.output }
        assert(probeSide.contains(scan.output.init),
          s"the Project above the scan must keep the table's column; got $probeSide")
      }
    }

    test(s"a bloom the scan fully evaluates through runtime filtering is not offered (AQE $aqe)") {
      // Dynamic partition pruning on `k` would take the bloom's place.
      withBloomJoin(SQLConf.DYNAMIC_PARTITION_PRUNING_ENABLED.key -> "false",
          SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> aqe.toString) {
        val plan = runBloomJoin("columnar" -> "true", "runtimeFiltering" -> "catalyst")
        val scan = scanOf(plan)
        assert(scan.runtimeFilters.exists(_.exists(_.isInstanceOf[BloomFilterMightContain])) &&
          StorageFilterTestSource.runtimeFilterCalls.get() > 0,
          s"test setup: the scan must take the bloom as a runtime filter:\n$plan")
        assert(scan.storageFilters.isEmpty, s"the scan must get no storage filter:\n$plan")
        assert(bloomsAbove(plan) == (0, 0), s"no Filter may keep the bloom:\n$plan")
      }
    }

    test(s"a two-key join offers both blooms in one filter, each bloom with its own bytes " +
        s"(AQE $aqe)") {
      // `MergeSubplans` merges the two blooms' aggregates into one subquery returning both blooms.
      withBloomJoin(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> aqe.toString) {
        withTable("bf_build2") {
          spark.range(0, 30).selectExpr("id AS bk", "id * 10 AS bv", "id AS flag")
            .write.format("parquet").saveAsTable("bf_build2")
          StorageFilterTestSource.reset()
          val probe = v2Probe("columnar" -> "true", "wide" -> "true", "declared" -> "k,v")
            .where("w % 3 != 1")
          val build = spark.table("bf_build2").where("flag = 5")
          val df = probe.join(build, probe("k") === build("bk") && probe("v") === build("bv"))
            .select(probe("k"), probe("v"), probe("w"))
          val plan = checkJoin(df, Set(Seq(5L, 50L, 500L)))
          val scan = scanOf(plan)
          assert(scan.storageFilters.size == 2, s"both blooms must be on the scan:\n$plan")
          assert(scan.storageFilters.forall(_.exists {
            case GetStructField(_: ExecSubqueryExpression, _, _) => true
            case _ => false
          }), s"test setup: the blooms must come from one merged subquery:\n$plan")
          assert(bloomsAbove(plan) == (0, 2), s"the Filter must skip both blooms:\n$plan")
          val filter = collect(plan) {
            case f: FilterExec if f.child.exists(_.isInstanceOf[BatchScanExec]) => f
          }.head
          assert(splitConjunctivePredicates(filter.condition).exists { c =>
            c.references.map(_.name).toSet == Set("w") && !c.isInstanceOf[If]
          }, s"the Filter must evaluate the conjunct over `w` on every row:\n$plan")
          assert(StorageFilterTestSource.filterReferences == Set(Seq("k", "v")),
            s"one filter over both keys; got ${StorageFilterTestSource.filterReferences}")
          val bloomBytes = scan.storageFilters.map(_.collectFirst {
            case m: BloomFilterMightContain =>
              m.bloomFilterExpression.eval().asInstanceOf[Array[Byte]].length
          }.get).sum
          assert(StorageFilterTestSource.filterBytes.get() < bloomBytes * 3 / 2,
            s"each bloom must ship once; the filter takes ${StorageFilterTestSource.filterBytes} " +
              s"bytes for $bloomBytes bytes of blooms")
          assert(StorageFilterTestSource.rowsEvaluated.get() == StorageFilterTestSource.numRows,
            "the reader must evaluate every row; got " +
              StorageFilterTestSource.rowsEvaluated.get())
        }
      }
    }

    test(s"the readers of one task share the filter the task deserialized (AQE $aqe)") {
      // `coalesce(1)` reads both partitions in one task.
      withBloomJoin(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> aqe.toString) {
        StorageFilterTestSource.reset()
        val probe = v2Probe("columnar" -> "true", "numPartitions" -> "2").coalesce(1)
        val plan = checkJoin(bloomJoin(probe), joinResult)
        assert(collect(plan) { case c: CoalesceExec => c }.nonEmpty, s"test setup:\n$plan")
        assert(scanOf(plan).storageFilters.size == 1, s"the bloom must be on the scan:\n$plan")
        assert(StorageFilterTestSource.filteredReaders.get() == 2,
          s"both readers must get the filter; got ${StorageFilterTestSource.filteredReaders.get()}")
        assert(StorageFilterTestSource.filterIdentities.size == 1,
          "both readers must get the same instance")
        assert(StorageFilterTestSource.rowsEvaluated.get() == StorageFilterTestSource.numRows,
          "the readers must evaluate every row; got " + StorageFilterTestSource.rowsEvaluated.get())
        assert(StorageFilterTestSource.rowsReturned.get() < StorageFilterTestSource.numRows,
          "the readers must have dropped rows, otherwise the filter did nothing")
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

      intercept[IllegalArgumentException](scan.copy(storageFilters = Nil))
      // As the scan would be without the checked column, too wide or without codegen.
      val unchecked = scan.copy(output = scan.output.init, appendsCheckedColumn = false)
      assert(scan != unchecked, "equals must see the checked column")
      val plain = unchecked.copy(storageFilters = Nil)
      assert(unchecked != plain && unchecked.hashCode != plain.hashCode,
        "equals and hashCode must see storageFilters")
      assert(!unchecked.sameResult(plain),
        s"a filtering scan must not be reusable as a plain one:\n${unchecked.canonicalized}\n" +
          s"${plain.canonicalized}")
      assert(!plain.simpleString(100).contains("StorageFilters:"),
        s"a plain scan's description must not name storage filters: ${plain.simpleString(100)}")
      assert(!plain.verboseStringWithOperatorId().contains("StorageFilters:"),
        s"nor its EXPLAIN FORMATTED: ${plain.verboseStringWithOperatorId()}")

      // As another relation instance of the same table gets, with fresh exprIds.
      val fresh = AttributeMap(scan.output.map(a => a -> a.newInstance()))
      val twin = scan.copy(
        output = scan.output.map(fresh(_)),
        storageFilters = scan.storageFilters.map(_.transform {
          case a: Attribute => fresh.getOrElse(a, a)
        }))
      assert(scan.sameResult(twin),
        s"the same filtering scan must be reusable:\n${scan.canonicalized}\n${twin.canonicalized}")
    }
  }

  // The reader would filter ahead of the operation, which then picks other rows.
  Seq[(String, DataFrame => DataFrame)](
    "limit" -> (_.limit(100)),
    "offset" -> (_.offset(10)),
    "topN" -> (_.orderBy("v").limit(100)),
    "sample" -> (_.sample(0.5))
  ).foreach { case (op, apply) =>
    test(s"nothing is offered to a scan that took a pushed $op") {
      withBloomJoin() {
        val probe = apply(v2Probe("columnar" -> "true", "push" -> op))
        val plan = bloomJoin(probe).queryExecution.executedPlan
        assert(collect(plan) {
          case p @ (_: BaseLimitExec | _: TakeOrderedAndProjectExec | _: SampleExec) => p
        }.isEmpty, s"the $op must be pushed into the scan whole:\n$plan")
        assertNothingOffered(plan)
      }
    }
  }

  test("the scan's pushdowns decide on storage filters, not whether it can be merged") {
    // A pushed variant extraction, for one, keeps the scan's rows but cannot be merged.
    withBloomJoin() {
      val optimized = bloomJoin(v2Probe("columnar" -> "true")).queryExecution.optimizedPlan
      def planned(mergeable: Boolean, rowWise: Boolean): BatchScanExec = scanOf(
        spark.sessionState.planner.plan(ReturnAnswer(optimized.transform {
          case r: DataSourceV2ScanRelation =>
            r.copy(mergeableScan = mergeable, onlyRowWisePushdowns = rowWise)
        })).next())
      assert(planned(mergeable = false, rowWise = true).storageFilters.size == 1,
        "a scan with only row-wise pushdowns must get the bloom")
      assert(planned(mergeable = true, rowWise = false).storageFilters.isEmpty,
        "a scan with a row-selecting pushdown must get none")
    }
  }

  test("a declared name that matches no column of the scan is ignored") {
    withBloomJoin() {
      val plan = runBloomJoin("columnar" -> "true", "declared" -> "k,w")
      assert(scanOf(plan).storageFilters.size == 1,
        s"the bloom filter must be on the scan:\n$plan")
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

  test("a field read out of a merged subquery's struct ships as a literal of its own") {
    val k = AttributeReference("k", LongType)()
    val blooms = Seq(Array[Byte](1, 2), Array[Byte](3))
    val struct = Literal.create(InternalRow.fromSeq(blooms :+ 7L),
      new StructType().add("b0", BinaryType).add("b1", BinaryType).add("max", LongType))
    val shipped = StorageFilterPlanning.withSubqueryResults(
      BloomFilterMightContain(GetStructField(struct, 1), new XxHash64(Seq(k))))
    val bloom = shipped.asInstanceOf[BloomFilterMightContain].bloomFilterExpression
    assert(bloom.isInstanceOf[Literal] && bloom.dataType == BinaryType &&
      bloom.eval().asInstanceOf[Array[Byte]].toSeq == blooms(1).toSeq, s"got $bloom")
    val bound =
      StorageFilterPlanning.withSubqueryResults(LessThanOrEqual(k, GetStructField(struct, 2)))
    assert(bound == LessThanOrEqual(k, Literal(7L)), s"got $bound")
  }
}
