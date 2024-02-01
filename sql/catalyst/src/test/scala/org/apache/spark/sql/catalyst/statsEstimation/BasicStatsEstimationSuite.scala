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

package org.apache.spark.sql.catalyst.statsEstimation

import org.mockito.Mockito.mock

import org.apache.spark.sql.catalyst.analysis.ResolvedNamespace
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions.{Ascending, Attribute, AttributeMap, AttributeReference, Literal, SortOrder}
import org.apache.spark.sql.catalyst.plans.{Inner, PlanTest}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.connector.catalog.SupportsNamespaces
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{BooleanType, ByteType, IntegerType, LongType}
import org.apache.spark.util.ArrayImplicits._

class BasicStatsEstimationSuite extends PlanTest with StatsEstimationTestBase {
  val attribute = attr("key")
  val colStat = ColumnStat(distinctCount = Some(10), min = Some(1), max = Some(10),
    nullCount = Some(0), avgLen = Some(4), maxLen = Some(4))

  val plan = StatsTestPlan(
    outputList = Seq(attribute),
    attributeStats = AttributeMap(Seq(attribute -> colStat)),
    rowCount = 10,
    // row count * (overhead + column size)
    size = Some(10 * (8 + 4)))

  test("range with positive step") {
    val range = Range(1, 5, 1, None)
    val histogramBins = Array(
      HistogramBin(1.0, 2.0, 2),
      HistogramBin(2.0, 3.0, 1),
      HistogramBin(3.0, 4.0, 1))
    val histogram = Some(Histogram(4.toDouble / 3, histogramBins))
    // Number of range elements should be same as number of distinct values
    assert(range.numElements === 4)

    val rangeStats = Statistics(
      sizeInBytes = 4 * 8,
      rowCount = Some(4),
      attributeStats = AttributeMap(
        range.output.map(
          attr =>
            (
              attr,
              ColumnStat(
                distinctCount = Some(4),
                min = Some(1),
                max = Some(4),
                nullCount = Some(0),
                maxLen = Some(LongType.defaultSize),
                avgLen = Some(LongType.defaultSize),
                histogram = histogram)))))
    val extraConfig = Map(SQLConf.HISTOGRAM_ENABLED.key -> "true",
      SQLConf.HISTOGRAM_NUM_BINS.key -> "3")
    checkStats(range, expectedStatsCboOn = rangeStats,
      expectedStatsCboOff = rangeStats, extraConfig)
  }

  test("range with positive step where end minus start not divisible by step") {
    val range = Range(-4, 5, 2, None)
    val histogramBins = Array(
      HistogramBin(-4.0, -2.0, 2),
      HistogramBin(-2.0, 2.0, 2),
      HistogramBin(2.0, 4.0, 1))
    val histogram = Some(Histogram(5.toDouble / 3, histogramBins))
    // Number of range elements should be same as number of distinct values
    assert(range.numElements === 5)
    val rangeStats = Statistics(
      sizeInBytes = 5 * 8,
      rowCount = Some(5),
      attributeStats = AttributeMap(
        range.output.map(
          attr =>
            (
              attr,
              ColumnStat(
                distinctCount = Some(5),
                min = Some(-4),
                max = Some(4),
                nullCount = Some(0),
                maxLen = Some(LongType.defaultSize),
                avgLen = Some(LongType.defaultSize),
                histogram = histogram)))))
    val extraConfig = Map(SQLConf.HISTOGRAM_ENABLED.key -> "true",
      SQLConf.HISTOGRAM_NUM_BINS.key -> "3")
    checkStats(range, expectedStatsCboOn = rangeStats,
      expectedStatsCboOff = rangeStats, extraConfig)
  }

  test("range with negative step") {
    val range = Range(-10, -20, -2, None)
    val histogramBins = Array(
      HistogramBin(-18.0, -16.0, 2),
      HistogramBin(-16.0, -12.0, 2),
      HistogramBin(-12.0, -10.0, 1))
    val histogram = Some(Histogram(5.toDouble / 3, histogramBins))
    // Number of range elements should be same as number of distinct values
    assert(range.numElements === 5)
    val rangeStats = Statistics(
      sizeInBytes = 5 * 8,
      rowCount = Some(5),
      attributeStats = AttributeMap(
        range.output.map(
          attr =>
            (
              attr,
              ColumnStat(
                distinctCount = Some(5),
                min = Some(-18),
                max = Some(-10),
                nullCount = Some(0),
                maxLen = Some(LongType.defaultSize),
                avgLen = Some(LongType.defaultSize),
                histogram = histogram)))))
    val extraConfig = Map(SQLConf.HISTOGRAM_ENABLED.key -> "true",
      SQLConf.HISTOGRAM_NUM_BINS.key -> "3")
    checkStats(range, expectedStatsCboOn = rangeStats,
      expectedStatsCboOff = rangeStats, extraConfig)
  }

  test("range with negative step where end minus start not divisible by step") {
    val range = Range(-10, -20, -3, None)
    val histogramBins = Array(
      HistogramBin(-19.0, -16.0, 2),
      HistogramBin(-16.0, -13.0, 1),
      HistogramBin(-13.0, -10.0, 1))
    val histogram = Some(Histogram(4.toDouble / 3, histogramBins))
    // Number of range elements should be same as number of distinct values
    assert(range.numElements === 4)

    val rangeStats = Statistics(
      sizeInBytes = 4 * 8,
      rowCount = Some(4),
      attributeStats = AttributeMap(
        range.output.map(
          attr =>
            (
              attr,
              ColumnStat(
                distinctCount = Some(4),
                min = Some(-19),
                max = Some(-10),
                nullCount = Some(0),
                maxLen = Some(LongType.defaultSize),
                avgLen = Some(LongType.defaultSize),
                histogram = histogram)))))
    val extraConfig = Map(SQLConf.HISTOGRAM_ENABLED.key -> "true",
      SQLConf.HISTOGRAM_NUM_BINS.key -> "3")
    checkStats(range, expectedStatsCboOn = rangeStats,
      expectedStatsCboOff = rangeStats, extraConfig)
  }

  test("range with empty output") {
      val range = Range(-10, -10, -1, None)
      val rangeStats = Statistics(sizeInBytes = 0, rowCount = Some(0))
    val extraConfig = Map(SQLConf.HISTOGRAM_ENABLED.key -> "true",
      SQLConf.HISTOGRAM_NUM_BINS.key -> "3")
      checkStats(range, expectedStatsCboOn = rangeStats,
        expectedStatsCboOff = rangeStats, extraConfig)
  }

test("range with invalid long value") {
  val numElements = BigInt(Long.MaxValue) - BigInt(Long.MinValue)
  val range = Range(Long.MinValue, Long.MaxValue, 1, None)
  val rangeAttrs = AttributeMap(range.output.map(attr =>
    (attr, ColumnStat(
      distinctCount = Some(numElements),
      nullCount = Some(0),
      maxLen = Some(LongType.defaultSize),
      avgLen = Some(LongType.defaultSize)))))
  val rangeStats = Statistics(
    sizeInBytes = numElements * 8,
    rowCount = Some(numElements),
    attributeStats = rangeAttrs)
  checkStats(range, rangeStats, rangeStats)
}

  test("windows") {
    val windows = plan.window(Seq(min(attribute).as("sum_attr")), Seq(attribute), Nil)
    val windowsStats = Statistics(sizeInBytes = plan.size.get * (4 + 4 + 8) / (4 + 8))
    checkStats(
      windows,
      expectedStatsCboOn = windowsStats,
      expectedStatsCboOff = windowsStats)
  }

  test("offset estimation: offset < child's rowCount") {
    val offset = Offset(Literal(2), plan)
    checkStats(offset, Statistics(sizeInBytes = 96, rowCount = Some(8)))
  }

  test("offset estimation: offset > child's rowCount") {
    val offset = Offset(Literal(20), plan)
    checkStats(offset, Statistics(sizeInBytes = 1, rowCount = Some(0)))
  }

  test("offset estimation: offset = 0") {
    val offset = Offset(Literal(0), plan)
    // Offset is equal to zero, so Offset's stats is equal to its child's stats.
    checkStats(offset, plan.stats.copy(attributeStats = AttributeMap(Nil)))
  }

  test("limit estimation: limit < child's rowCount") {
    val localLimit = LocalLimit(Literal(2), plan)
    val globalLimit = GlobalLimit(Literal(2), plan)
    // LocalLimit's stats is just its child's stats except column stats
    checkStats(localLimit, plan.stats.copy(attributeStats = AttributeMap(Nil)))
    checkStats(globalLimit, Statistics(sizeInBytes = 24, rowCount = Some(2)))
  }

  test("limit estimation: limit > child's rowCount") {
    val localLimit = LocalLimit(Literal(20), plan)
    val globalLimit = GlobalLimit(Literal(20), plan)
    checkStats(localLimit, plan.stats.copy(attributeStats = AttributeMap(Nil)))
    // Limit is larger than child's rowCount, so GlobalLimit's stats is equal to its child's stats.
    checkStats(globalLimit, plan.stats.copy(attributeStats = AttributeMap(Nil)))
  }

  test("limit estimation: limit = 0") {
    val localLimit = LocalLimit(Literal(0), plan)
    val globalLimit = GlobalLimit(Literal(0), plan)
    val stats = Statistics(sizeInBytes = 1, rowCount = Some(0))
    checkStats(localLimit, stats)
    checkStats(globalLimit, stats)
  }

  test("tail estimation") {
    checkStats(Tail(Literal(1), plan), Statistics(sizeInBytes = 12, rowCount = Some(1)))
    checkStats(Tail(Literal(20), plan), plan.stats.copy(attributeStats = AttributeMap(Nil)))
    checkStats(Tail(Literal(0), plan), Statistics(sizeInBytes = 1, rowCount = Some(0)))
  }

  test("sample estimation") {
    val sample = Sample(0.0, 0.5, withReplacement = false, (math.random() * 1000).toLong, plan)
    checkStats(sample, Statistics(sizeInBytes = 60, rowCount = Some(5)))

    // Child doesn't have rowCount in stats
    val childStats = Statistics(sizeInBytes = 120)
    val childPlan = DummyLogicalPlan(childStats, childStats)
    val sample2 =
      Sample(0.0, 0.11, withReplacement = false, (math.random() * 1000).toLong, childPlan)
    checkStats(sample2, Statistics(sizeInBytes = 14))
  }

  test("estimate statistics when the conf changes") {
    val expectedDefaultStats =
      Statistics(
        sizeInBytes = 40,
        rowCount = Some(10),
        attributeStats = AttributeMap(Seq(
          AttributeReference("c1", IntegerType)() -> ColumnStat(distinctCount = Some(10),
            min = Some(1), max = Some(10),
            nullCount = Some(0), avgLen = Some(4), maxLen = Some(4)))))
    val expectedCboStats =
      Statistics(
        sizeInBytes = 4,
        rowCount = Some(1),
        attributeStats = AttributeMap(Seq(
          AttributeReference("c1", IntegerType)() -> ColumnStat(distinctCount = Some(10),
            min = Some(5), max = Some(5),
            nullCount = Some(0), avgLen = Some(4), maxLen = Some(4)))))

    val plan = DummyLogicalPlan(defaultStats = expectedDefaultStats, cboStats = expectedCboStats)
    checkStats(
      plan, expectedStatsCboOn = expectedCboStats, expectedStatsCboOff = expectedDefaultStats)
  }

  test("command should report a dummy stats") {
    val plan = CommentOnNamespace(
      ResolvedNamespace(mock(classOf[SupportsNamespaces]),
        Array("ns").toImmutableArraySeq), "comment")
    checkStats(
      plan,
      expectedStatsCboOn = Statistics.DUMMY,
      expectedStatsCboOff = Statistics.DUMMY)
  }

  test("Improve Repartition statistics estimation") {
    // SPARK-35203 for repartition and repartitionByExpr
    // SPARK-37949 for rebalance
    Seq(
      RepartitionByExpression(plan.output, plan, 10),
      RepartitionByExpression(Nil, plan, None),
      plan.repartition(2),
      plan.coalesce(3),
      plan.rebalance(),
      plan.rebalance(plan.output: _*)).foreach { rep =>
      val expectedStats = Statistics(plan.size.get, Some(plan.rowCount), plan.attributeStats)
      checkStats(
        rep,
        expectedStatsCboOn = expectedStats,
        expectedStatsCboOff = expectedStats)
    }
  }

  test("SPARK-34031: Union operator missing rowCount when enable CBO") {
    val union = Union(plan :: plan :: plan :: Nil)
    val childrenSize = union.children.size
    val sizeInBytes = plan.size.get * childrenSize
    val rowCount = Some(plan.rowCount * childrenSize)
    val attributeStats = AttributeMap(
      Seq(
        attribute -> ColumnStat(min = Some(1), max = Some(10), nullCount = Some(0))))
    checkStats(
      union,
      expectedStatsCboOn = Statistics(sizeInBytes = sizeInBytes,
        rowCount = rowCount,
        attributeStats = attributeStats),
      expectedStatsCboOff = Statistics(sizeInBytes = sizeInBytes))
  }

  test("SPARK-34121: Intersect operator missing rowCount when enable CBO") {
    val intersect = Intersect(plan, plan, false)
    val sizeInBytes = plan.size.get
    val rowCount = Some(plan.rowCount)
    checkStats(
      intersect,
      expectedStatsCboOn = Statistics(sizeInBytes = sizeInBytes, rowCount = rowCount),
      expectedStatsCboOff = Statistics(sizeInBytes = sizeInBytes))
  }

  test("SPARK-35185: Improve Distinct statistics estimation") {
    val distinct = Distinct(plan)
    val sizeInBytes = plan.size.get
    checkStats(
      distinct,
      expectedStatsCboOn = Statistics(sizeInBytes, Some(plan.rowCount), plan.attributeStats),
      expectedStatsCboOff = Statistics(sizeInBytes = sizeInBytes))
  }

  test("SPARK-39851: Improve join stats estimation if one side can keep uniqueness") {
    val brandId = attr("brand_id")
    val classId = attr("class_id")
    val aliasedBrandId = brandId.as("new_brand_id")
    val aliasedClassId = classId.as("new_class_id")

    val tableSize = 4059900
    val tableRowCnt = 202995

    val tbl = StatsTestPlan(
      outputList = Seq(brandId, classId),
      size = Some(tableSize),
      rowCount = tableRowCnt,
      attributeStats =
        AttributeMap(Seq(
          brandId -> ColumnStat(Some(858), Some(101001), Some(1016017), Some(0), Some(4), Some(4)),
          classId -> ColumnStat(Some(16), Some(1), Some(16), Some(0), Some(4), Some(4)))))

    val join = Join(
      tbl,
      tbl.groupBy(brandId, classId)(aliasedBrandId, aliasedClassId),
      Inner,
      Some(brandId === aliasedBrandId.toAttribute && classId === aliasedClassId.toAttribute),
      JoinHint.NONE)

    checkStats(
      join,
      expectedStatsCboOn = Statistics(4871880, Some(tableRowCnt), join.stats.attributeStats),
      expectedStatsCboOff = Statistics(sizeInBytes = 4059900 * 2))
  }

  test("row size and column stats estimation for sort") {
    val columnInfo = AttributeMap(
      Seq(
        AttributeReference("cbool", BooleanType)() -> ColumnStat(
          distinctCount = Some(2),
          min = Some(false),
          max = Some(true),
          nullCount = Some(0),
          avgLen = Some(1),
          maxLen = Some(1)),
        AttributeReference("cbyte", ByteType)() -> ColumnStat(
          distinctCount = Some(2),
          min = Some(1),
          max = Some(2),
          nullCount = Some(0),
          avgLen = Some(1),
          maxLen = Some(1))))

    val expectedSize = 16
    val child = StatsTestPlan(
      outputList = columnInfo.keys.toSeq,
      rowCount = 2,
      attributeStats = columnInfo,
      size = Some(expectedSize))

    val sortOrder = SortOrder(columnInfo.keys.head, Ascending)
    val sort = Sort(order = Seq(sortOrder), global = true, child = child)
    val expectedSortStats =
      Statistics(sizeInBytes = expectedSize, rowCount = Some(2), attributeStats = columnInfo)
    checkStats(
      sort,
      expectedStatsCboOn = expectedSortStats,
      expectedStatsCboOff = expectedSortStats)
  }

  /** Check estimated stats when cbo is turned on/off. */
  private def checkStats(
      plan: LogicalPlan,
      expectedStatsCboOn: Statistics,
      expectedStatsCboOff: Statistics,
      extraConfigs: Map[String, String] = Map.empty): Unit = {
    val cboEnabledConfig = Seq(SQLConf.CBO_ENABLED.key -> "true") ++ extraConfigs.toSeq
    withSQLConf(cboEnabledConfig: _*) {
      // Invalidate statistics
      plan.invalidateStatsCache()
      assert(plan.stats == expectedStatsCboOn)
    }
    val cboDisabledConfig = Seq(SQLConf.CBO_ENABLED.key -> "false") ++ extraConfigs.toSeq
    withSQLConf(cboDisabledConfig: _*) {
      plan.invalidateStatsCache()
      assert(plan.stats == expectedStatsCboOff)
    }
  }

  /** Check estimated stats when it's the same whether cbo is turned on or off. */
  private def checkStats(plan: LogicalPlan, expectedStats: Statistics): Unit =
    checkStats(plan, expectedStats, expectedStats)
}

/**
 * This class is used for unit-testing the cbo switch, it mimics a logical plan which computes
 * a simple statistics or a cbo estimated statistics based on the conf.
 */
private case class DummyLogicalPlan(
    defaultStats: Statistics,
    cboStats: Statistics)
  extends LeafNode {

  override def output: Seq[Attribute] = Nil

  override def computeStats(): Statistics = if (conf.cboEnabled) cboStats else defaultStats
}
