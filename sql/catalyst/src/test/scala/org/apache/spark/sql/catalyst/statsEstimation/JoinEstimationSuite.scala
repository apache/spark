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

import java.sql.{Date, Timestamp}
import java.time.LocalTime

import scala.collection.mutable

import org.apache.spark.sql.catalyst.expressions.{Alias, And, Attribute, AttributeMap, AttributeReference, EqualTo, GreaterThan, Literal}
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.plans.logical.statsEstimation.{DefaultValueInterval, NullValueInterval, ValueInterval}
import org.apache.spark.sql.catalyst.plans.logical.statsEstimation.EstimationUtils._
import org.apache.spark.sql.catalyst.util.{CharVarcharUtils, DateTimeUtils}
import org.apache.spark.sql.types.{DateType, TimestampType, _}
import org.apache.spark.unsafe.types.TimestampNanosVal


class JoinEstimationSuite extends StatsEstimationTestBase {

  /** Set up tables and its columns for testing */
  private val columnInfo: AttributeMap[ColumnStat] = AttributeMap(Seq(
    attr("key-1-5") -> ColumnStat(distinctCount = Some(5), min = Some(1), max = Some(5),
      nullCount = Some(0), avgLen = Some(4), maxLen = Some(4)),
    attr("key-5-9") -> ColumnStat(distinctCount = Some(5), min = Some(5), max = Some(9),
      nullCount = Some(0), avgLen = Some(4), maxLen = Some(4)),
    attr("key-1-2") -> ColumnStat(distinctCount = Some(2), min = Some(1), max = Some(2),
      nullCount = Some(0), avgLen = Some(4), maxLen = Some(4)),
    attr("key-2-4") -> ColumnStat(distinctCount = Some(3), min = Some(2), max = Some(4),
      nullCount = Some(0), avgLen = Some(4), maxLen = Some(4)),
    attr("key-2-3") -> ColumnStat(distinctCount = Some(2), min = Some(2), max = Some(3),
      nullCount = Some(0), avgLen = Some(4), maxLen = Some(4))
  ))

  private val nameToAttr: Map[String, Attribute] = columnInfo.map(kv => kv._1.name -> kv._1)
  private val nameToColInfo: Map[String, (Attribute, ColumnStat)] =
    columnInfo.map(kv => kv._1.name -> kv)

  // Suppose table1 (key-1-5 int, key-5-9 int) has 5 records: (1, 9), (2, 8), (3, 7), (4, 6), (5, 5)
  private val table1 = StatsTestPlan(
    outputList = Seq("key-1-5", "key-5-9").map(nameToAttr),
    rowCount = 5,
    attributeStats = AttributeMap(Seq("key-1-5", "key-5-9").map(nameToColInfo)))

  // Suppose table2 (key-1-2 int, key-2-4 int) has 3 records: (1, 2), (2, 3), (2, 4)
  private val table2 = StatsTestPlan(
    outputList = Seq("key-1-2", "key-2-4").map(nameToAttr),
    rowCount = 3,
    attributeStats = AttributeMap(Seq("key-1-2", "key-2-4").map(nameToColInfo)))

  // Suppose table3 (key-1-2 int, key-2-3 int) has 2 records: (1, 2), (2, 3)
  private val table3 = StatsTestPlan(
    outputList = Seq("key-1-2", "key-2-3").map(nameToAttr),
    rowCount = 2,
    attributeStats = AttributeMap(Seq("key-1-2", "key-2-3").map(nameToColInfo)))

  private def estimateByHistogram(
      leftHistogram: Histogram,
      rightHistogram: Histogram,
      expectedMin: Any,
      expectedMax: Any,
      expectedNdv: Long,
      expectedRows: Long): Unit = {
    val col1 = attr("key1")
    val col2 = attr("key2")
    val c1 = generateJoinChild(col1, leftHistogram, expectedMin, expectedMax)
    val c2 = generateJoinChild(col2, rightHistogram, expectedMin, expectedMax)

    val c1JoinC2 = Join(c1, c2, Inner, Some(EqualTo(col1, col2)), JoinHint.NONE)
    val c2JoinC1 = Join(c2, c1, Inner, Some(EqualTo(col2, col1)), JoinHint.NONE)
    val expectedStatsAfterJoin = Statistics(
      sizeInBytes = expectedRows * (8 + 2 * 4),
      rowCount = Some(expectedRows),
      attributeStats = AttributeMap(Seq(
        col1 -> c1.stats.attributeStats(col1).copy(
          distinctCount = Some(expectedNdv),
          min = Some(expectedMin), max = Some(expectedMax)),
        col2 -> c2.stats.attributeStats(col2).copy(
          distinctCount = Some(expectedNdv),
          min = Some(expectedMin), max = Some(expectedMax))))
    )

    // Join order should not affect estimation result.
    Seq(c1JoinC2, c2JoinC1).foreach { join =>
      assert(join.stats == expectedStatsAfterJoin)
    }
  }

  private def generateJoinChild(
      col: Attribute,
      histogram: Histogram,
      expectedMin: Any,
      expectedMax: Any): LogicalPlan = {
    val colStat = inferColumnStat(histogram, expectedMin, expectedMax)
    StatsTestPlan(
      outputList = Seq(col),
      rowCount = (histogram.height * histogram.bins.length).toLong,
      attributeStats = AttributeMap(Seq(col -> colStat)))
  }

  /** Column statistics should be consistent with histograms in tests. */
  private def inferColumnStat(
      histogram: Histogram,
      expectedMin: Any,
      expectedMax: Any): ColumnStat = {

    var ndv = 0L
    for (i <- histogram.bins.indices) {
      val bin = histogram.bins(i)
      if (i == 0 || bin.hi != histogram.bins(i - 1).hi) {
        ndv += bin.ndv
      }
    }
    ColumnStat(distinctCount = Some(ndv),
      min = Some(expectedMin), max = Some(expectedMax),
      nullCount = Some(0), avgLen = Some(4), maxLen = Some(4),
      histogram = Some(histogram))
  }

  test("equi-height histograms: a bin is contained by another one") {
    val histogram1 = Histogram(height = 300, Array(
      HistogramBin(lo = 10, hi = 30, ndv = 10), HistogramBin(lo = 30, hi = 60, ndv = 30)))
    val histogram2 = Histogram(height = 100, Array(
      HistogramBin(lo = 0, hi = 50, ndv = 50), HistogramBin(lo = 50, hi = 100, ndv = 40)))
    // test bin trimming
    val (t0, h0) = trimBin(histogram2.bins(0), height = 100, lowerBound = 10, upperBound = 60)
    assert(t0 == HistogramBin(lo = 10, hi = 50, ndv = 40) && h0 == 80)
    val (t1, h1) = trimBin(histogram2.bins(1), height = 100, lowerBound = 10, upperBound = 60)
    assert(t1 == HistogramBin(lo = 50, hi = 60, ndv = 8) && h1 == 20)

    val expectedRanges = Seq(
      // histogram1.bins(0) overlaps t0
      OverlappedRange(10, 30, 10, 40 * 1 / 2, 300, 80 * 1 / 2),
      // histogram1.bins(1) overlaps t0
      OverlappedRange(30, 50, 30 * 2 / 3, 40 * 1 / 2, 300 * 2 / 3, 80 * 1 / 2),
      // histogram1.bins(1) overlaps t1
      OverlappedRange(50, 60, 30 * 1 / 3, 8, 300 * 1 / 3, 20)
    )
    assert(expectedRanges.equals(
      getOverlappedRanges(histogram1, histogram2, lowerBound = 10, upperBound = 60)))

    estimateByHistogram(
      leftHistogram = histogram1,
      rightHistogram = histogram2,
      expectedMin = 10,
      expectedMax = 60,
      expectedNdv = 10 + 20 + 8,
      expectedRows = 300 * 40 / 20 + 200 * 40 / 20 + 100 * 20 / 10)
  }

  test("equi-height histograms: a bin has only one value after trimming") {
    val histogram1 = Histogram(height = 300, Array(
      HistogramBin(lo = 50, hi = 60, ndv = 10), HistogramBin(lo = 60, hi = 75, ndv = 3)))
    val histogram2 = Histogram(height = 100, Array(
      HistogramBin(lo = 0, hi = 50, ndv = 50), HistogramBin(lo = 50, hi = 100, ndv = 40)))
    // test bin trimming
    val (t0, h0) = trimBin(histogram2.bins(0), height = 100, lowerBound = 50, upperBound = 75)
    assert(t0 == HistogramBin(lo = 50, hi = 50, ndv = 1) && h0 == 2)
    val (t1, h1) = trimBin(histogram2.bins(1), height = 100, lowerBound = 50, upperBound = 75)
    assert(t1 == HistogramBin(lo = 50, hi = 75, ndv = 20) && h1 == 50)

    val expectedRanges = Seq(
      // histogram1.bins(0) overlaps t0
      OverlappedRange(50, 50, 1, 1, 300 / 10, 2),
      // histogram1.bins(0) overlaps t1
      OverlappedRange(50, 60, 10, 20 * 10 / 25, 300, 50 * 10 / 25),
      // histogram1.bins(1) overlaps t1
      OverlappedRange(60, 75, 3, 20 * 15 / 25, 300, 50 * 15 / 25)
    )
    assert(expectedRanges.equals(
      getOverlappedRanges(histogram1, histogram2, lowerBound = 50, upperBound = 75)))

    estimateByHistogram(
      leftHistogram = histogram1,
      rightHistogram = histogram2,
      expectedMin = 50,
      expectedMax = 75,
      expectedNdv = 1 + 8 + 3,
      expectedRows = 30 * 2 / 1 + 300 * 20 / 10 + 300 * 30 / 12)
  }

  test("equi-height histograms: skew distribution (some bins have only one value)") {
    val histogram1 = Histogram(height = 300, Array(
      HistogramBin(lo = 30, hi = 30, ndv = 1),
      HistogramBin(lo = 30, hi = 30, ndv = 1),
      HistogramBin(lo = 30, hi = 60, ndv = 30)))
    val histogram2 = Histogram(height = 100, Array(
      HistogramBin(lo = 0, hi = 50, ndv = 50), HistogramBin(lo = 50, hi = 100, ndv = 40)))
    // test bin trimming
    val (t0, h0) = trimBin(histogram2.bins(0), height = 100, lowerBound = 30, upperBound = 60)
    assert(t0 == HistogramBin(lo = 30, hi = 50, ndv = 20) && h0 == 40)
    val (t1, h1) = trimBin(histogram2.bins(1), height = 100, lowerBound = 30, upperBound = 60)
    assert(t1 ==HistogramBin(lo = 50, hi = 60, ndv = 8) && h1 == 20)

    val expectedRanges = Seq(
      OverlappedRange(30, 30, 1, 1, 300, 40 / 20),
      OverlappedRange(30, 30, 1, 1, 300, 40 / 20),
      OverlappedRange(30, 50, 30 * 2 / 3, 20, 300 * 2 / 3, 40),
      OverlappedRange(50, 60, 30 * 1 / 3, 8, 300 * 1 / 3, 20)
    )
    assert(expectedRanges.equals(
      getOverlappedRanges(histogram1, histogram2, lowerBound = 30, upperBound = 60)))

    estimateByHistogram(
      leftHistogram = histogram1,
      rightHistogram = histogram2,
      expectedMin = 30,
      expectedMax = 60,
      expectedNdv = 1 + 20 + 8,
      expectedRows = 300 * 2 / 1 + 300 * 2 / 1 + 200 * 40 / 20 + 100 * 20 / 10)
  }

  test("equi-height histograms: skew distribution (histograms have different skewed values") {
    val histogram1 = Histogram(height = 300, Array(
      HistogramBin(lo = 30, hi = 30, ndv = 1), HistogramBin(lo = 30, hi = 60, ndv = 30)))
    val histogram2 = Histogram(height = 100, Array(
      HistogramBin(lo = 0, hi = 50, ndv = 50), HistogramBin(lo = 50, hi = 50, ndv = 1)))
    // test bin trimming
    val (t0, h0) = trimBin(histogram1.bins(1), height = 300, lowerBound = 30, upperBound = 50)
    assert(t0 == HistogramBin(lo = 30, hi = 50, ndv = 20) && h0 == 200)
    val (t1, h1) = trimBin(histogram2.bins(0), height = 100, lowerBound = 30, upperBound = 50)
    assert(t1 == HistogramBin(lo = 30, hi = 50, ndv = 20) && h1 == 40)

    val expectedRanges = Seq(
      OverlappedRange(30, 30, 1, 1, 300, 40 / 20),
      OverlappedRange(30, 50, 20, 20, 200, 40),
      OverlappedRange(50, 50, 1, 1, 200 / 20, 100)
    )
    assert(expectedRanges.equals(
      getOverlappedRanges(histogram1, histogram2, lowerBound = 30, upperBound = 50)))

    estimateByHistogram(
      leftHistogram = histogram1,
      rightHistogram = histogram2,
      expectedMin = 30,
      expectedMax = 50,
      expectedNdv = 1 + 20,
      expectedRows = 300 * 2 / 1 + 200 * 40 / 20 + 10 * 100 / 1)
  }

  test("equi-height histograms: skew distribution (both histograms have the same skewed value") {
    val histogram1 = Histogram(height = 300, Array(
      HistogramBin(lo = 30, hi = 30, ndv = 1), HistogramBin(lo = 30, hi = 60, ndv = 30)))
    val histogram2 = Histogram(height = 150, Array(
      HistogramBin(lo = 0, hi = 30, ndv = 30), HistogramBin(lo = 30, hi = 30, ndv = 1)))
    // test bin trimming
    val (t0, h0) = trimBin(histogram1.bins(1), height = 300, lowerBound = 30, upperBound = 30)
    assert(t0 == HistogramBin(lo = 30, hi = 30, ndv = 1) && h0 == 10)
    val (t1, h1) = trimBin(histogram2.bins(0), height = 150, lowerBound = 30, upperBound = 30)
    assert(t1 == HistogramBin(lo = 30, hi = 30, ndv = 1) && h1 == 5)

    val expectedRanges = Seq(
      OverlappedRange(30, 30, 1, 1, 300, 5),
      OverlappedRange(30, 30, 1, 1, 300, 150),
      OverlappedRange(30, 30, 1, 1, 10, 5),
      OverlappedRange(30, 30, 1, 1, 10, 150)
    )
    assert(expectedRanges.equals(
      getOverlappedRanges(histogram1, histogram2, lowerBound = 30, upperBound = 30)))

    estimateByHistogram(
      leftHistogram = histogram1,
      rightHistogram = histogram2,
      expectedMin = 30,
      expectedMax = 30,
      // only one value: 30
      expectedNdv = 1,
      expectedRows = 300 * 5 / 1 + 300 * 150 / 1 + 10 * 5 / 1 + 10 * 150 / 1)
  }

  test("cross join") {
    // table1 (key-1-5 int, key-5-9 int): (1, 9), (2, 8), (3, 7), (4, 6), (5, 5)
    // table2 (key-1-2 int, key-2-4 int): (1, 2), (2, 3), (2, 4)
    val join = Join(table1, table2, Cross, None, JoinHint.NONE)
    val expectedStats = Statistics(
      sizeInBytes = 5 * 3 * (8 + 4 * 4),
      rowCount = Some(5 * 3),
      // Keep the column stat from both sides unchanged.
      attributeStats = AttributeMap(
        Seq("key-1-5", "key-5-9", "key-1-2", "key-2-4").map(nameToColInfo)))
    assert(join.stats == expectedStats)
  }

  test("disjoint inner join") {
    // table1 (key-1-5 int, key-5-9 int): (1, 9), (2, 8), (3, 7), (4, 6), (5, 5)
    // table2 (key-1-2 int, key-2-4 int): (1, 2), (2, 3), (2, 4)
    // key-5-9 and key-2-4 are disjoint
    val join = Join(table1, table2, Inner,
      Some(EqualTo(nameToAttr("key-5-9"), nameToAttr("key-2-4"))), JoinHint.NONE)
    val expectedStats = Statistics(
      sizeInBytes = 1,
      rowCount = Some(0),
      attributeStats = AttributeMap(Nil))
    assert(join.stats == expectedStats)
  }

  test("disjoint left outer join") {
    // table1 (key-1-5 int, key-5-9 int): (1, 9), (2, 8), (3, 7), (4, 6), (5, 5)
    // table2 (key-1-2 int, key-2-4 int): (1, 2), (2, 3), (2, 4)
    // key-5-9 and key-2-4 are disjoint
    val join = Join(table1, table2, LeftOuter,
      Some(EqualTo(nameToAttr("key-5-9"), nameToAttr("key-2-4"))), JoinHint.NONE)
    val expectedStats = Statistics(
      sizeInBytes = 5 * (8 + 4 * 4),
      rowCount = Some(5),
      attributeStats = AttributeMap(Seq("key-1-5", "key-5-9").map(nameToColInfo) ++
        // Null count for right side columns = left row count
        Seq(nameToAttr("key-1-2") -> nullColumnStat(nameToAttr("key-1-2").dataType, 5),
          nameToAttr("key-2-4") -> nullColumnStat(nameToAttr("key-2-4").dataType, 5))))
    assert(join.stats == expectedStats)
  }

  test("disjoint right outer join") {
    // table1 (key-1-5 int, key-5-9 int): (1, 9), (2, 8), (3, 7), (4, 6), (5, 5)
    // table2 (key-1-2 int, key-2-4 int): (1, 2), (2, 3), (2, 4)
    // key-5-9 and key-2-4 are disjoint
    val join = Join(table1, table2, RightOuter,
      Some(EqualTo(nameToAttr("key-5-9"), nameToAttr("key-2-4"))), JoinHint.NONE)
    val expectedStats = Statistics(
      sizeInBytes = 3 * (8 + 4 * 4),
      rowCount = Some(3),
      attributeStats = AttributeMap(Seq("key-1-2", "key-2-4").map(nameToColInfo) ++
        // Null count for left side columns = right row count
        Seq(nameToAttr("key-1-5") -> nullColumnStat(nameToAttr("key-1-5").dataType, 3),
          nameToAttr("key-5-9") -> nullColumnStat(nameToAttr("key-5-9").dataType, 3))))
    assert(join.stats == expectedStats)
  }

  test("disjoint full outer join") {
    // table1 (key-1-5 int, key-5-9 int): (1, 9), (2, 8), (3, 7), (4, 6), (5, 5)
    // table2 (key-1-2 int, key-2-4 int): (1, 2), (2, 3), (2, 4)
    // key-5-9 and key-2-4 are disjoint
    val join = Join(table1, table2, FullOuter,
      Some(EqualTo(nameToAttr("key-5-9"), nameToAttr("key-2-4"))), JoinHint.NONE)
    val expectedStats = Statistics(
      sizeInBytes = (5 + 3) * (8 + 4 * 4),
      rowCount = Some(5 + 3),
      attributeStats = AttributeMap(
        // Update null count in column stats.
        Seq(nameToAttr("key-1-5") -> columnInfo(nameToAttr("key-1-5")).copy(nullCount = Some(3)),
          nameToAttr("key-5-9") -> columnInfo(nameToAttr("key-5-9")).copy(nullCount = Some(3)),
          nameToAttr("key-1-2") -> columnInfo(nameToAttr("key-1-2")).copy(nullCount = Some(5)),
          nameToAttr("key-2-4") -> columnInfo(nameToAttr("key-2-4")).copy(nullCount = Some(5)))))
    assert(join.stats == expectedStats)
  }

  test("inner join") {
    // table1 (key-1-5 int, key-5-9 int): (1, 9), (2, 8), (3, 7), (4, 6), (5, 5)
    // table2 (key-1-2 int, key-2-4 int): (1, 2), (2, 3), (2, 4)
    val join = Join(table1, table2, Inner,
      Some(EqualTo(nameToAttr("key-1-5"), nameToAttr("key-1-2"))), JoinHint.NONE)
    // Update column stats for equi-join keys (key-1-5 and key-1-2).
    val joinedColStat = ColumnStat(distinctCount = Some(2), min = Some(1), max = Some(2),
      nullCount = Some(0), avgLen = Some(4), maxLen = Some(4))
    // Update column stat for other column if #outputRow / #sideRow < 1 (key-5-9), or keep it
    // unchanged (key-2-4).
    val colStatForkey59 = nameToColInfo("key-5-9")._2.copy(distinctCount = Some(5 * 3 / 5))

    val expectedStats = Statistics(
      sizeInBytes = 3 * (8 + 4 * 4),
      rowCount = Some(3),
      attributeStats = AttributeMap(
        Seq(nameToAttr("key-1-5") -> joinedColStat, nameToAttr("key-1-2") -> joinedColStat,
          nameToAttr("key-5-9") -> colStatForkey59, nameToColInfo("key-2-4"))))
    assert(join.stats == expectedStats)
  }

  test("inner join with multiple equi-join keys") {
    // table2 (key-1-2 int, key-2-4 int): (1, 2), (2, 3), (2, 4)
    // table3 (key-1-2 int, key-2-3 int): (1, 2), (2, 3)
    val join = Join(table2, table3, Inner, Some(
      And(EqualTo(nameToAttr("key-1-2"), nameToAttr("key-1-2")),
        EqualTo(nameToAttr("key-2-4"), nameToAttr("key-2-3")))), JoinHint.NONE)

    // Update column stats for join keys.
    val joinedColStat1 = ColumnStat(distinctCount = Some(2), min = Some(1), max = Some(2),
      nullCount = Some(0), avgLen = Some(4), maxLen = Some(4))
    val joinedColStat2 = ColumnStat(distinctCount = Some(2), min = Some(2), max = Some(3),
      nullCount = Some(0), avgLen = Some(4), maxLen = Some(4))

    val expectedStats = Statistics(
      sizeInBytes = 2 * (8 + 4 * 4),
      rowCount = Some(2),
      attributeStats = AttributeMap(
        Seq(nameToAttr("key-1-2") -> joinedColStat1, nameToAttr("key-1-2") -> joinedColStat1,
          nameToAttr("key-2-4") -> joinedColStat2, nameToAttr("key-2-3") -> joinedColStat2)))
    assert(join.stats == expectedStats)
  }

  test("left outer join") {
    // table2 (key-1-2 int, key-2-4 int): (1, 2), (2, 3), (2, 4)
    // table3 (key-1-2 int, key-2-3 int): (1, 2), (2, 3)
    val join = Join(table3, table2, LeftOuter,
      Some(EqualTo(nameToAttr("key-2-3"), nameToAttr("key-2-4"))), JoinHint.NONE)
    val joinedColStat = ColumnStat(distinctCount = Some(2), min = Some(2), max = Some(3),
      nullCount = Some(0), avgLen = Some(4), maxLen = Some(4))

    val expectedStats = Statistics(
      sizeInBytes = 2 * (8 + 4 * 4),
      rowCount = Some(2),
      // Keep the column stat from left side unchanged.
      attributeStats = AttributeMap(
        Seq(nameToColInfo("key-1-2"), nameToColInfo("key-2-3"),
          nameToColInfo("key-1-2"), nameToAttr("key-2-4") -> joinedColStat)))
    assert(join.stats == expectedStats)
  }

  test("right outer join") {
    // table2 (key-1-2 int, key-2-4 int): (1, 2), (2, 3), (2, 4)
    // table3 (key-1-2 int, key-2-3 int): (1, 2), (2, 3)
    val join = Join(table2, table3, RightOuter,
      Some(EqualTo(nameToAttr("key-2-4"), nameToAttr("key-2-3"))), JoinHint.NONE)
    val joinedColStat = ColumnStat(distinctCount = Some(2), min = Some(2), max = Some(3),
      nullCount = Some(0), avgLen = Some(4), maxLen = Some(4))

    val expectedStats = Statistics(
      sizeInBytes = 2 * (8 + 4 * 4),
      rowCount = Some(2),
      // Keep the column stat from right side unchanged.
      attributeStats = AttributeMap(
        Seq(nameToColInfo("key-1-2"), nameToAttr("key-2-4") -> joinedColStat,
          nameToColInfo("key-1-2"), nameToColInfo("key-2-3"))))
    assert(join.stats == expectedStats)
  }

  test("full outer join") {
    // table2 (key-1-2 int, key-2-4 int): (1, 2), (2, 3), (2, 4)
    // table3 (key-1-2 int, key-2-3 int): (1, 2), (2, 3)
    val join = Join(table2, table3, FullOuter,
      Some(EqualTo(nameToAttr("key-2-4"), nameToAttr("key-2-3"))), JoinHint.NONE)

    val expectedStats = Statistics(
      sizeInBytes = 3 * (8 + 4 * 4),
      rowCount = Some(3),
      // Keep the column stat from both sides unchanged.
      attributeStats = AttributeMap(Seq(nameToColInfo("key-1-2"), nameToColInfo("key-2-4"),
        nameToColInfo("key-1-2"), nameToColInfo("key-2-3"))))
    assert(join.stats == expectedStats)
  }

  test("left semi/anti join") {
    // table2 (key-1-2 int, key-2-4 int): (1, 2), (2, 3), (2, 4)
    // table3 (key-1-2 int, key-2-3 int): (1, 2), (2, 3)
    Seq(LeftSemi, LeftAnti).foreach { jt =>
      val join = Join(table2, table3, jt,
        Some(EqualTo(nameToAttr("key-2-4"), nameToAttr("key-2-3"))), JoinHint.NONE)
      // For now we just propagate the statistics from left side for left semi/anti join.
      val expectedStats = Statistics(
        sizeInBytes = 3 * (8 + 4 * 2),
        rowCount = Some(3),
        attributeStats = AttributeMap(Seq(nameToColInfo("key-1-2"), nameToColInfo("key-2-4"))))
      assert(join.stats == expectedStats)
    }
  }

  test("test join keys of different types") {
    /** Columns in a table with only one row */
    def genColumnData: mutable.LinkedHashMap[Attribute, ColumnStat] = {
      val dec = Decimal("1.000000000000000000")
      val date = DateTimeUtils.fromJavaDate(Date.valueOf("2016-05-08"))
      val timestamp = DateTimeUtils.fromJavaTimestamp(Timestamp.valueOf("2016-05-08 00:00:01"))
      val time = DateTimeUtils.localTimeToNanos(LocalTime.parse("12:34:56.123456"))
      mutable.LinkedHashMap[Attribute, ColumnStat](
        AttributeReference("cbool", BooleanType)() -> ColumnStat(distinctCount = Some(1),
          min = Some(false), max = Some(false),
          nullCount = Some(0), avgLen = Some(1), maxLen = Some(1)),
        AttributeReference("cbyte", ByteType)() -> ColumnStat(distinctCount = Some(1),
          min = Some(1.toByte), max = Some(1.toByte),
          nullCount = Some(0), avgLen = Some(1), maxLen = Some(1)),
        AttributeReference("cshort", ShortType)() -> ColumnStat(distinctCount = Some(1),
          min = Some(1.toShort), max = Some(1.toShort),
          nullCount = Some(0), avgLen = Some(2), maxLen = Some(2)),
        AttributeReference("cint", IntegerType)() -> ColumnStat(distinctCount = Some(1),
          min = Some(1), max = Some(1),
          nullCount = Some(0), avgLen = Some(4), maxLen = Some(4)),
        AttributeReference("clong", LongType)() -> ColumnStat(distinctCount = Some(1),
          min = Some(1L), max = Some(1L),
          nullCount = Some(0), avgLen = Some(8), maxLen = Some(8)),
        AttributeReference("cdouble", DoubleType)() -> ColumnStat(distinctCount = Some(1),
          min = Some(1.0), max = Some(1.0),
          nullCount = Some(0), avgLen = Some(8), maxLen = Some(8)),
        AttributeReference("cfloat", FloatType)() -> ColumnStat(distinctCount = Some(1),
          min = Some(1.0f), max = Some(1.0f),
          nullCount = Some(0), avgLen = Some(4), maxLen = Some(4)),
        AttributeReference("cdec", DecimalType.SYSTEM_DEFAULT)() -> ColumnStat(
          distinctCount = Some(1), min = Some(dec), max = Some(dec),
          nullCount = Some(0), avgLen = Some(16), maxLen = Some(16)),
        AttributeReference("cstring", StringType)() -> ColumnStat(distinctCount = Some(1),
          min = None, max = None, nullCount = Some(0), avgLen = Some(3), maxLen = Some(3)),
        AttributeReference("cchar", CharType(3))() -> ColumnStat(distinctCount = Some(1),
          min = None, max = None, nullCount = Some(0), avgLen = Some(3), maxLen = Some(3)),
        AttributeReference("cvarchar", VarcharType(3))() -> ColumnStat(distinctCount = Some(1),
          min = None, max = None, nullCount = Some(0), avgLen = Some(3), maxLen = Some(3)),
        AttributeReference("cbinary", BinaryType)() -> ColumnStat(distinctCount = Some(1),
          min = None, max = None, nullCount = Some(0), avgLen = Some(3), maxLen = Some(3)),
        AttributeReference("cdate", DateType)() -> ColumnStat(distinctCount = Some(1),
          min = Some(date), max = Some(date),
          nullCount = Some(0), avgLen = Some(4), maxLen = Some(4)),
        AttributeReference("ctimestamp", TimestampType)() -> ColumnStat(distinctCount = Some(1),
          min = Some(timestamp), max = Some(timestamp),
          nullCount = Some(0), avgLen = Some(8), maxLen = Some(8)),
        AttributeReference("ctimestamp_ntz", TimestampNTZType)() -> ColumnStat(
          distinctCount = Some(1), min = Some(timestamp), max = Some(timestamp),
          nullCount = Some(0), avgLen = Some(8), maxLen = Some(8)),
        AttributeReference("ctime", TimeType())() -> ColumnStat(distinctCount = Some(1),
          min = Some(time), max = Some(time),
          nullCount = Some(0), avgLen = Some(8), maxLen = Some(8)),
        AttributeReference("cyminterval", YearMonthIntervalType())() -> ColumnStat(
          distinctCount = Some(1), min = Some(13), max = Some(13),
          nullCount = Some(0), avgLen = Some(4), maxLen = Some(4)),
        AttributeReference("cdtinterval", DayTimeIntervalType())() -> ColumnStat(
          distinctCount = Some(1), min = Some(timestamp), max = Some(timestamp),
          nullCount = Some(0), avgLen = Some(8), maxLen = Some(8))
      )
    }

    val columnInfo1 = genColumnData
    val columnInfo2 = genColumnData
    val table1 = StatsTestPlan(
      outputList = columnInfo1.keys.toSeq,
      rowCount = 1,
      attributeStats = AttributeMap(columnInfo1.toSeq))
    val table2 = StatsTestPlan(
      outputList = columnInfo2.keys.toSeq,
      rowCount = 1,
      attributeStats = AttributeMap(columnInfo2.toSeq))
    val joinKeys = table1.output.zip(table2.output)
    joinKeys.foreach { case (key1, key2) =>
      withClue(s"For data type ${key1.dataType}") {
        // All values in two tables are the same, so column stats after join are also the same.
        val join = Join(Project(Seq(key1), table1), Project(Seq(key2), table2), Inner,
          Some(EqualTo(key1, key2)), JoinHint.NONE)
        val expectedStats = Statistics(
          sizeInBytes = 1 * (8 + 2 * getColSize(key1, columnInfo1(key1))),
          rowCount = Some(1),
          attributeStats = AttributeMap(Seq(key1 -> columnInfo1(key1), key2 -> columnInfo1(key1))))
        assert(join.stats == expectedStats)
        if (key1.dataType.isInstanceOf[StringType]) {
          // UTF8 rows include the average payload length plus base and offset overhead.
          assert(join.stats.sizeInBytes == 38)
        }
      }
    }
  }

  test("join with null column") {
    val (nullColumn, nullColStat) = (attr("cnull"),
      ColumnStat(distinctCount = Some(0), min = None, max = None,
        nullCount = Some(1), avgLen = Some(4), maxLen = Some(4)))
    val nullTable = StatsTestPlan(
      outputList = Seq(nullColumn),
      rowCount = 1,
      attributeStats = AttributeMap(Seq(nullColumn -> nullColStat)))
    val join = Join(table1, nullTable, Inner,
      Some(EqualTo(nameToAttr("key-1-5"), nullColumn)), JoinHint.NONE)
    val expectedStats = Statistics(
      sizeInBytes = 1,
      rowCount = Some(0),
      attributeStats = AttributeMap(Nil))
    assert(join.stats == expectedStats)
  }

  test("SPARK-60810: string and binary join keys with histograms use the distinct count") {
    // DSv2 connectors can report a histogram for any type, but only numeric-like keys can be
    // estimated with one. 10 x 10 rows with 10 distinct values on each side give 10 rows.
    val histogram = Histogram(height = 1, Array(HistogramBin(lo = 0, hi = 10, ndv = 10)))
    val stat = ColumnStat(distinctCount = Some(10), nullCount = Some(0), avgLen = Some(1),
      maxLen = Some(1), histogram = Some(histogram))
    def side(key: Attribute): LogicalPlan =
      StatsTestPlan(Seq(key), 10, AttributeMap(Seq(key -> stat)))
    def joinRows(left: LogicalPlan, right: LogicalPlan): Option[BigInt] = Join(left, right, Inner,
      Some(EqualTo(left.output.head, right.output.head)), JoinHint.NONE).stats.rowCount

    Seq(StringType, BinaryType).foreach { dt =>
      val rows = joinRows(side(AttributeReference("l", dt)()), side(AttributeReference("r", dt)()))
      assert(rows.contains(BigInt(10)), dt)
    }

    // CHAR keys reach the join through the read-side padding Project, which passes the
    // column's statistics, histogram included, to its alias.
    def paddedSide(name: String): LogicalPlan = {
      val c = charAttr(name, 5)
      Project(Seq(Alias(CharVarcharUtils.addPaddingForScan(c), name)()), side(c))
    }
    assert(joinRows(paddedSide("l"), paddedSide("r")).contains(BigInt(10)))
  }

  test("SPARK-60810: boolean join keys with histograms use the histogram estimate") {
    // A DSv2 connector can report a histogram for a boolean column. The intersected bounds come
    // back as Booleans and must be converted like the filter's histogram estimate does.
    // 8 false and 4 true values on each side.
    val histogram = Histogram(height = 4, Array(HistogramBin(lo = 0, hi = 0, ndv = 1),
      HistogramBin(lo = 0, hi = 0, ndv = 1), HistogramBin(lo = 1, hi = 1, ndv = 1)))
    val stat = ColumnStat(distinctCount = Some(2), min = Some(false), max = Some(true),
      nullCount = Some(0), avgLen = Some(1), maxLen = Some(1), histogram = Some(histogram))
    def side(name: String): LogicalPlan = {
      val key = AttributeReference(name, BooleanType)()
      StatsTestPlan(Seq(key), 12, AttributeMap(Seq(key -> stat)))
    }
    val (left, right) = (side("l"), side("r"))
    val join = Join(left, right, Inner, Some(EqualTo(left.output.head, right.output.head)),
      JoinHint.NONE)
    // 8 x 8 + 4 x 4 from the histograms. The distinct count alone would give 12 x 12 / 2 = 72.
    assert(join.stats.rowCount.contains(BigInt(80)))
  }

  test("SPARK-61724: TIMESTAMP_NTZ join keys with histograms use the histogram estimate") {
    // ANALYZE builds the histogram of a TIMESTAMP_NTZ column from its microseconds, as it does
    // for a TIMESTAMP column. 8 rows of 0 and 4 rows of 1 on each side.
    val histogram = Histogram(height = 4, Array(HistogramBin(lo = 0, hi = 0, ndv = 1),
      HistogramBin(lo = 0, hi = 0, ndv = 1), HistogramBin(lo = 1, hi = 1, ndv = 1)))
    val stat = ColumnStat(distinctCount = Some(2), min = Some(0L), max = Some(1L),
      nullCount = Some(0), avgLen = Some(8), maxLen = Some(8), histogram = Some(histogram))
    def side(name: String): LogicalPlan = {
      val key = AttributeReference(name, TimestampNTZType)()
      StatsTestPlan(Seq(key), 12, AttributeMap(Seq(key -> stat)))
    }
    val (left, right) = (side("l"), side("r"))
    val join = Join(left, right, Inner, Some(EqualTo(left.output.head, right.output.head)),
      JoinHint.NONE)
    // 8 x 8 + 4 x 4 from the histograms. The distinct count alone would give 12 x 12 / 2 = 72.
    assert(join.stats.rowCount.contains(BigInt(80)))
  }

  test("SPARK-61724: nanosecond timestamp join keys with min/max use the distinct count") {
    // Their values cannot be converted to Double, so their min/max give no range.
    val stat = ColumnStat(distinctCount = Some(10),
      min = Some(new TimestampNanosVal(1L, 1.toShort)),
      max = Some(new TimestampNanosVal(10L, 1.toShort)),
      nullCount = Some(0), avgLen = Some(16), maxLen = Some(16))
    def side(name: String): LogicalPlan = {
      val key = AttributeReference(name, TimestampNTZNanosType(9))()
      StatsTestPlan(Seq(key), 10, AttributeMap(Seq(key -> stat)))
    }
    val (left, right) = (side("l"), side("r"))
    val join = Join(left, right, Inner, Some(EqualTo(left.output.head, right.output.head)),
      JoinHint.NONE)
    // 10 x 10 rows with 10 distinct values on each side.
    assert(join.stats.rowCount.contains(BigInt(10)))
  }

  test("SPARK-60810: join keys without non-null values match no rows") {
    // ANALYZE stores a distinct count of 0 for a column of nulls, and an IS NULL filter sets
    // it. Padded CHAR keys get the column's statistics through the padding Project.
    val nullStat = ColumnStat(distinctCount = Some(0), nullCount = Some(10), avgLen = Some(5),
      maxLen = Some(5))
    def nullSide(key: Attribute): LogicalPlan =
      StatsTestPlan(Seq(key), 10, AttributeMap(Seq(key -> nullStat)))
    def paddedSide(name: String): LogicalPlan = {
      val c = charAttr(name, 5)
      Project(Seq(Alias(CharVarcharUtils.addPaddingForScan(c), name)()), nullSide(c))
    }
    def stringSide(name: String): LogicalPlan = nullSide(AttributeReference(name, StringType)())
    Seq(
      paddedSide("l") -> paddedSide("r"),
      paddedSide("l") -> stringSide("r"),
      stringSide("l") -> stringSide("r")).foreach { case (left, right) =>
      val condition = Some(EqualTo(left.output.head, right.output.head))
      assert(Join(left, right, Inner, condition, JoinHint.NONE).stats.rowCount
        .contains(BigInt(0)))
      assert(Join(left, right, FullOuter, condition, JoinHint.NONE).stats.rowCount
        .contains(BigInt(20)))
    }
  }

  test("SPARK-61002: a join key without min/max is not taken as holding only nulls") {
    // table1 (key-1-5 int, key-5-9 int): (1, 9), (2, 8), (3, 7), (4, 6), (5, 5)
    // A data source can report the distinct and null counts of a column but not its min/max.
    val (noMinMax, noMinMaxStat) = (attr("key-no-min-max"),
      ColumnStat(distinctCount = Some(3), min = None, max = None,
        nullCount = Some(0), avgLen = Some(4), maxLen = Some(4)))
    val noMinMaxTable = StatsTestPlan(
      outputList = Seq(noMinMax),
      rowCount = 3,
      attributeStats = AttributeMap(Seq(noMinMax -> noMinMaxStat)))
    val join = Join(table1, noMinMaxTable, Inner,
      Some(EqualTo(nameToAttr("key-1-5"), noMinMax)), JoinHint.NONE)
    // Estimated by ndv: 5 * 3 / max(5, 3) = 3. The joined keys keep the range of key-1-5.
    val joinedColStat = ColumnStat(distinctCount = Some(3), min = Some(1), max = Some(5),
      nullCount = Some(0), avgLen = Some(4), maxLen = Some(4))
    val expectedStats = Statistics(
      sizeInBytes = 3 * (8 + 4 * 3),
      rowCount = Some(3),
      attributeStats = AttributeMap(Seq(
        nameToAttr("key-1-5") -> joinedColStat, noMinMax -> joinedColStat,
        nameToAttr("key-5-9") -> nameToColInfo("key-5-9")._2.copy(distinctCount = Some(3)))))
    assert(join.stats == expectedStats)
  }

  test("SPARK-61002: an outer join on a key without min/max") {
    // table1 (key-1-5 int, key-5-9 int): (1, 9), (2, 8), (3, 7), (4, 6), (5, 5)
    val (noMinMax, noMinMaxStat) = (attr("key-no-min-max"),
      ColumnStat(distinctCount = Some(3), min = None, max = None,
        nullCount = Some(0), avgLen = Some(4), maxLen = Some(4)))
    val noMinMaxTable = StatsTestPlan(
      outputList = Seq(noMinMax),
      rowCount = 3,
      attributeStats = AttributeMap(Seq(noMinMax -> noMinMaxStat)))
    // 3 rows match, and the joined keys take the range of key-1-5.
    val joinedColStat = ColumnStat(distinctCount = Some(3), min = Some(1), max = Some(5),
      nullCount = Some(0), avgLen = Some(4), maxLen = Some(4))
    val (key15, key59) = (nameToColInfo("key-1-5"), nameToColInfo("key-5-9"))
    Seq(
      // The outer side keeps its stats. On the inner side, the join key takes the joined stats.
      (LeftOuter, 5, Seq(key15, key59, noMinMax -> joinedColStat)),
      (RightOuter, 3, Seq(key15._1 -> joinedColStat,
        key59._1 -> key59._2.copy(distinctCount = Some(3)), noMinMax -> noMinMaxStat)),
      // 5 + 3 - 3 rows, and both sides keep their stats.
      (FullOuter, 5, Seq(key15, key59, noMinMax -> noMinMaxStat))
    ).foreach { case (joinType, rows, colStats) =>
      val join = Join(table1, noMinMaxTable, joinType,
        Some(EqualTo(nameToAttr("key-1-5"), noMinMax)), JoinHint.NONE)
      assert(join.stats.rowCount == Some(rows), joinType)
      assert(join.stats.attributeStats == AttributeMap(colStats), joinType)
    }
  }

  test("SPARK-61002: join keys with histograms but without min/max are estimated by ndv") {
    val histogram = Histogram(height = 10, Array(
      HistogramBin(lo = 1, hi = 5, ndv = 5), HistogramBin(lo = 5, hi = 9, ndv = 4)))
    val (col1, col2) = (attr("key1"), attr("key2"))
    val withMinMax = generateJoinChild(col1, histogram, 1, 9)
    val noMinMax = StatsTestPlan(
      outputList = Seq(col2),
      rowCount = 20,
      attributeStats = AttributeMap(Seq(
        col2 -> inferColumnStat(histogram, 1, 9).copy(min = None, max = None))))
    val join = Join(withMinMax, noMinMax, Inner, Some(EqualTo(col1, col2)), JoinHint.NONE)
    // 20 * 20 / max(9, 9), rounded up.
    assert(join.stats.rowCount == Some(45))
  }

  test("SPARK-61002: join keys that both lack min/max are estimated by ndv") {
    // Two data sources that report distinct and null counts but no min/max.
    val histogram = Histogram(height = 5, Array(
      HistogramBin(lo = 1, hi = 3, ndv = 3), HistogramBin(lo = 3, hi = 5, ndv = 3)))
    Seq(None, Some(histogram)).foreach { hist =>
      def table(name: String, ndv: Int): StatsTestPlan = {
        val key = attr(name)
        StatsTestPlan(Seq(key), rowCount = 10, attributeStats = AttributeMap(Seq(
          key -> ColumnStat(distinctCount = Some(ndv), min = None, max = None,
            nullCount = Some(0), avgLen = Some(4), maxLen = Some(4), histogram = hist))))
      }
      val (left, right) = (table("l", 5), table("r", 4))
      val join = Join(left, right, Inner, Some(EqualTo(left.output.head, right.output.head)),
        JoinHint.NONE)
      // 10 * 10 / max(5, 4). The range of the joined keys is unknown, so a histogram, which
      // later estimates use only with a range, is passed on as it is.
      val joinedColStat = ColumnStat(distinctCount = Some(4), min = None, max = None,
        nullCount = Some(0), avgLen = Some(4), maxLen = Some(4), histogram = hist)
      assert(join.stats.rowCount == Some(20), hist)
      assert(join.stats.attributeStats(left.output.head) == joinedColStat, hist)
      assert(join.stats.attributeStats(right.output.head) == joinedColStat, hist)
    }
  }

  test("SPARK-61002: a join key without min/max drops its histogram for the range of the other") {
    // The joined keys take the range [1, 5] of `known`. The histogram of `unknown` covers only
    // [2, 4], so it is dropped, while the histogram of `known` is kept.
    val knownHistogram = Histogram(height = 5, Array(
      HistogramBin(lo = 1, hi = 3, ndv = 3), HistogramBin(lo = 3, hi = 5, ndv = 3)))
    val unknownHistogram = Histogram(height = 5, Array(
      HistogramBin(lo = 2, hi = 3, ndv = 2), HistogramBin(lo = 3, hi = 4, ndv = 2)))
    val (known, unknown) = (attr("known"), attr("unknown"))
    def table(key: AttributeReference, stat: ColumnStat): StatsTestPlan =
      StatsTestPlan(Seq(key), rowCount = 10, attributeStats = AttributeMap(Seq(key -> stat)))
    val knownTable = table(known, ColumnStat(distinctCount = Some(5), min = Some(1),
      max = Some(5), nullCount = Some(0), avgLen = Some(4), maxLen = Some(4),
      histogram = Some(knownHistogram)))
    val unknownTable = table(unknown, ColumnStat(distinctCount = Some(3), min = None,
      max = None, nullCount = Some(0), avgLen = Some(4), maxLen = Some(4),
      histogram = Some(unknownHistogram)))
    // 10 * 10 / max(5, 3), rounded up.
    val joinedColStat = ColumnStat(distinctCount = Some(3), min = Some(1), max = Some(5),
      nullCount = Some(0), avgLen = Some(4), maxLen = Some(4))
    Seq(
      Join(knownTable, unknownTable, Inner, Some(EqualTo(known, unknown)), JoinHint.NONE),
      Join(unknownTable, knownTable, Inner, Some(EqualTo(unknown, known)), JoinHint.NONE)
    ).foreach { join =>
      assert(join.stats.rowCount == Some(20))
      assert(join.stats.attributeStats(known) ==
        joinedColStat.copy(histogram = Some(knownHistogram)))
      assert(join.stats.attributeStats(unknown) == joinedColStat)
      // Without the histogram of `unknown`, a filter on it uses the joined min/max and distinct
      // count: 20 / 3 and 20 * (5 - 3) / (5 - 1), rounded up.
      assert(Filter(EqualTo(unknown, Literal(3)), join).stats.rowCount == Some(7))
      assert(Filter(GreaterThan(unknown, Literal(3)), join).stats.rowCount == Some(10))
    }
  }

  test("SPARK-61002: a boolean join key from a union is not taken as holding only nulls") {
    def booleanTable(name: String, rowCount: Int): StatsTestPlan = {
      val col = AttributeReference(name, BooleanType)()
      StatsTestPlan(
        outputList = Seq(col),
        rowCount = rowCount,
        attributeStats = AttributeMap(Seq(col -> ColumnStat(distinctCount = Some(2),
          min = Some(false), max = Some(true),
          nullCount = Some(0), avgLen = Some(1), maxLen = Some(1)))))
    }
    val union = Union(booleanTable("b1", 10), booleanTable("b2", 10))
    val unionKey = union.output.head
    // The union keeps the distinct and null counts of a boolean column but not its min/max.
    assert(union.stats.attributeStats(unionKey).hasCountStats)
    assert(!union.stats.attributeStats(unionKey).hasMinMaxStats)
    val other = booleanTable("b3", 4)
    val join = Join(union, other, Inner, Some(EqualTo(unionKey, other.output.head)), JoinHint.NONE)
    // 20 * 4 / max(2, 2)
    assert(join.stats.rowCount == Some(40))
  }

  test("SPARK-61002: a join key holding only nulls matches no key without min/max") {
    // Neither key has min/max: the type has none, as for STRING, or the source reports none.
    Seq(IntegerType, StringType).foreach { dataType =>
      def noMinMaxTable(name: String, distinctCount: Int): StatsTestPlan = {
        val col = AttributeReference(name, dataType)()
        StatsTestPlan(
          outputList = Seq(col),
          rowCount = 3,
          attributeStats = AttributeMap(Seq(col -> ColumnStat(
            distinctCount = Some(distinctCount), min = None, max = None,
            nullCount = Some(if (distinctCount == 0) 3 else 0), avgLen = Some(4),
            maxLen = Some(4)))))
      }
      val nullKeys = noMinMaxTable("null-keys", 0)
      Seq(noMinMaxTable("some-keys", 3), noMinMaxTable("other-null-keys", 0)).foreach { other =>
        Seq(Join(nullKeys, other, Inner, Some(EqualTo(nullKeys.output.head, other.output.head)),
            JoinHint.NONE),
          Join(other, nullKeys, Inner, Some(EqualTo(other.output.head, nullKeys.output.head)),
            JoinHint.NONE)).foreach { join =>
          assert(join.stats.rowCount == Some(0), dataType)
        }
      }
    }
    // A distinct count of 0 makes a key null-only also when a data source reports its min/max.
    val reported = attr("reported")
    val reportedTable = StatsTestPlan(Seq(reported), rowCount = 3, attributeStats = AttributeMap(
      Seq(reported -> ColumnStat(distinctCount = Some(0), min = Some(1), max = Some(3),
        nullCount = Some(3), avgLen = Some(4), maxLen = Some(4)))))
    assert(Join(table1, reportedTable, Inner, Some(EqualTo(nameToAttr("key-1-5"), reported)),
      JoinHint.NONE).stats.rowCount == Some(0))
    // `intersect` follows `isIntersected`, which rejects such a pair, but does not fail on it.
    assert(ValueInterval.intersect(new NullValueInterval(), new DefaultValueInterval(),
      IntegerType) == ((None, None)))
  }

  test("SPARK-33018 Fix estimate statistics issue if child has 0 bytes") {
    case class MyStatsTestPlan(
        outputList: Seq[Attribute],
        sizeInBytes: BigInt) extends LeafNode {
      override def output: Seq[Attribute] = outputList
      override def computeStats(): Statistics = Statistics(sizeInBytes = sizeInBytes)
    }

    val left = MyStatsTestPlan(
      outputList = Seq("key-1-2", "key-2-4").map(nameToAttr),
      sizeInBytes = BigInt(100))

    val right = MyStatsTestPlan(
      outputList = Seq("key-1-2", "key-2-3").map(nameToAttr),
      sizeInBytes = BigInt(0))

    val join = Join(left, right, LeftOuter,
      Some(EqualTo(nameToAttr("key-2-4"), nameToAttr("key-2-3"))), JoinHint.NONE)

    assert(join.stats == Statistics(sizeInBytes = 100))
  }
}
