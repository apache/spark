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

import scala.collection.mutable

import org.apache.spark.sql.catalyst.expressions.{And, Attribute, AttributeMap, AttributeReference, EqualTo}
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical.{ColumnStat, Join, Project, Statistics}
import org.apache.spark.sql.types.{DateType, TimestampType, _}


class JoinEstimationSuite extends StatsEstimationTestBase {

  /** Set up tables and its columns for testing */
  private val columnInfo: AttributeMap[ColumnStat] = AttributeMap(Seq(
    attr("key11") -> ColumnStat(distinctCount = 5, min = Some(1), max = Some(5), nullCount = 0,
      avgLen = 4, maxLen = 4),
    attr("key12") -> ColumnStat(distinctCount = 5, min = Some(5), max = Some(9), nullCount = 0,
      avgLen = 4, maxLen = 4),
    attr("key21") -> ColumnStat(distinctCount = 2, min = Some(1), max = Some(2), nullCount = 0,
      avgLen = 4, maxLen = 4),
    attr("key22") -> ColumnStat(distinctCount = 3, min = Some(2), max = Some(4), nullCount = 0,
      avgLen = 4, maxLen = 4),
    attr("key31") -> ColumnStat(distinctCount = 2, min = Some(1), max = Some(2), nullCount = 0,
      avgLen = 4, maxLen = 4),
    attr("key32") -> ColumnStat(distinctCount = 2, min = Some(2), max = Some(3), nullCount = 0,
      avgLen = 4, maxLen = 4)
  ))

  private val nameToAttr: Map[String, Attribute] = columnInfo.map(kv => kv._1.name -> kv._1)
  private val nameToColInfo: Map[String, (Attribute, ColumnStat)] =
    columnInfo.map(kv => kv._1.name -> kv)

  // Suppose table1 (key11 int, key12 int) has 5 records: (1, 9), (2, 8), (3, 7), (4, 6), (5, 5)
  private val table1 = StatsTestPlan(
    outputList = Seq("key11", "key12").map(nameToAttr),
    rowCount = 5,
    attributeStats = AttributeMap(Seq("key11", "key12").map(nameToColInfo)))

  // Suppose table2 (key21 int, key22 int) has 3 records: (1, 2), (2, 3), (2, 4)
  private val table2 = StatsTestPlan(
    outputList = Seq("key21", "key22").map(nameToAttr),
    rowCount = 3,
    attributeStats = AttributeMap(Seq("key21", "key22").map(nameToColInfo)))

  // Suppose table3 (key31 int, key32 int) has 2 records: (1, 2), (2, 3)
  private val table3 = StatsTestPlan(
    outputList = Seq("key31", "key32").map(nameToAttr),
    rowCount = 2,
    attributeStats = AttributeMap(Seq("key31", "key32").map(nameToColInfo)))

  test("cross join") {
    // table1 (key11 int, key12 int): (1, 9), (2, 8), (3, 7), (4, 6), (5, 5)
    // table2 (key21 int, key22 int): (1, 2), (2, 3), (2, 4)
    val join = Join(table1, table2, Cross, None)
    val expectedStats = Statistics(
      sizeInBytes = 5 * 3 * (8 + 4 * 4),
      rowCount = Some(5 * 3),
      // Keep the column stat from both sides unchanged.
      attributeStats = AttributeMap(Seq("key11", "key12", "key21", "key22").map(nameToColInfo)))
    assert(join.stats(conf) == expectedStats)
  }

  test("disjoint inner join") {
    // table1 (key11 int, key12 int): (1, 9), (2, 8), (3, 7), (4, 6), (5, 5)
    // table2 (key21 int, key22 int): (1, 2), (2, 3), (2, 4)
    // key12 and key22 are disjoint
    val join = Join(table1, table2, Inner, Some(
      And(EqualTo(nameToAttr("key11"), nameToAttr("key21")),
        EqualTo(nameToAttr("key12"), nameToAttr("key22")))))
    // Empty column stats for all output columns.
    val emptyColStat = ColumnStat(distinctCount = 0, min = None, max = None, nullCount = 0,
      avgLen = 4, maxLen = 4)

    val expectedStats = Statistics(
      sizeInBytes = 1,
      rowCount = Some(0),
      attributeStats = AttributeMap(
        Seq("key11", "key12", "key21", "key22").map(c => (nameToAttr(c), emptyColStat))))
    assert(join.stats(conf) == expectedStats)
  }

  test("inner join") {
    // table1 (key11 int, key12 int): (1, 9), (2, 8), (3, 7), (4, 6), (5, 5)
    // table2 (key21 int, key22 int): (1, 2), (2, 3), (2, 4)
    val join = Join(table1, table2, Inner, Some(EqualTo(nameToAttr("key11"), nameToAttr("key21"))))
    // Update column stats for equi-join keys (key11 and key21).
    val joinedColStat = ColumnStat(distinctCount = 2, min = Some(1), max = Some(2), nullCount = 0,
      avgLen = 4, maxLen = 4)
    // Update column stat for other column if #outputRow / #sideRow < 1 (key12), or keep it
    // unchanged (key22).
    val colStatForKey12 = nameToColInfo("key12")._2.copy(distinctCount = 5 * 3 / 5)

    val expectedStats = Statistics(
      sizeInBytes = 3 * (8 + 4 * 4),
      rowCount = Some(3),
      attributeStats = AttributeMap(
        Seq(nameToAttr("key11") -> joinedColStat, nameToAttr("key21") -> joinedColStat,
          nameToAttr("key12") -> colStatForKey12, nameToColInfo("key22"))))
    assert(join.stats(conf) == expectedStats)
  }

  test("inner join with multiple equi-join keys") {
    // table2 (key21 int, key22 int): (1, 2), (2, 3), (2, 4)
    // table3 (key31 int, key32 int): (1, 2), (2, 3)
    val join = Join(table2, table3, Inner, Some(
      And(EqualTo(nameToAttr("key21"), nameToAttr("key31")),
        EqualTo(nameToAttr("key22"), nameToAttr("key32")))))

    // Update column stats for join keys.
    val joinedColStat1 = ColumnStat(distinctCount = 2, min = Some(1), max = Some(2), nullCount = 0,
        avgLen = 4, maxLen = 4)
    val joinedColStat2 = ColumnStat(distinctCount = 2, min = Some(2), max = Some(3), nullCount = 0,
      avgLen = 4, maxLen = 4)

    val expectedStats = Statistics(
      sizeInBytes = 2 * (8 + 4 * 4),
      rowCount = Some(2),
      attributeStats = AttributeMap(
        Seq(nameToAttr("key21") -> joinedColStat1, nameToAttr("key31") -> joinedColStat1,
          nameToAttr("key22") -> joinedColStat2, nameToAttr("key32") -> joinedColStat2)))
    assert(join.stats(conf) == expectedStats)
  }

  test("left outer join") {
    // table2 (key21 int, key22 int): (1, 2), (2, 3), (2, 4)
    // table3 (key31 int, key32 int): (1, 2), (2, 3)
    val join = Join(table3, table2, LeftOuter,
      Some(EqualTo(nameToAttr("key32"), nameToAttr("key22"))))
    val joinedColStat = ColumnStat(distinctCount = 2, min = Some(2), max = Some(3), nullCount = 0,
      avgLen = 4, maxLen = 4)

    val expectedStats = Statistics(
      sizeInBytes = 2 * (8 + 4 * 4),
      rowCount = Some(2),
      // Keep the column stat from left side unchanged.
      attributeStats = AttributeMap(
        Seq(nameToColInfo("key31"), nameToColInfo("key32"),
          nameToColInfo("key21"), nameToAttr("key22") -> joinedColStat)))
    assert(join.stats(conf) == expectedStats)
  }

  test("right outer join") {
    // table2 (key21 int, key22 int): (1, 2), (2, 3), (2, 4)
    // table3 (key31 int, key32 int): (1, 2), (2, 3)
    val join = Join(table2, table3, RightOuter,
      Some(EqualTo(nameToAttr("key22"), nameToAttr("key32"))))
    val joinedColStat = ColumnStat(distinctCount = 2, min = Some(2), max = Some(3), nullCount = 0,
      avgLen = 4, maxLen = 4)

    val expectedStats = Statistics(
      sizeInBytes = 2 * (8 + 4 * 4),
      rowCount = Some(2),
      // Keep the column stat from right side unchanged.
      attributeStats = AttributeMap(
        Seq(nameToColInfo("key21"), nameToAttr("key22") -> joinedColStat,
          nameToColInfo("key31"), nameToColInfo("key32"))))
    assert(join.stats(conf) == expectedStats)
  }

  test("full outer join") {
    // table2 (key21 int, key22 int): (1, 2), (2, 3), (2, 4)
    // table3 (key31 int, key32 int): (1, 2), (2, 3)
    val join = Join(table2, table3, FullOuter,
      Some(EqualTo(nameToAttr("key22"), nameToAttr("key32"))))

    val expectedStats = Statistics(
      sizeInBytes = 3 * (8 + 4 * 4),
      rowCount = Some(3),
      // Keep the column stat from both sides unchanged.
      attributeStats = AttributeMap(Seq(nameToColInfo("key21"), nameToColInfo("key22"),
        nameToColInfo("key31"), nameToColInfo("key32"))))
    assert(join.stats(conf) == expectedStats)
  }

  test("left semi/anti join") {
    // table2 (key21 int, key22 int): (1, 2), (2, 3), (2, 4)
    // table3 (key31 int, key32 int): (1, 2), (2, 3)
    Seq(LeftSemi, LeftAnti).foreach { jt =>
      val join = Join(table2, table3, jt, Some(EqualTo(nameToAttr("key22"), nameToAttr("key32"))))
      // For now we just propagate the statistics from left side for left semi/anti join.
      val expectedStats = Statistics(
        sizeInBytes = 3 * (8 + 4 * 2),
        rowCount = Some(3),
        attributeStats = AttributeMap(Seq(nameToColInfo("key21"), nameToColInfo("key22"))))
      assert(join.stats(conf) == expectedStats)
    }
  }

  test("test join keys of different types") {
    val dec1 = new java.math.BigDecimal("1.000000000000000000")
    val dec2 = new java.math.BigDecimal("8.000000000000000000")
    val d1 = Date.valueOf("2016-05-08")
    val d2 = Date.valueOf("2016-05-09")
    val t1 = Timestamp.valueOf("2016-05-08 00:00:01")
    val t2 = Timestamp.valueOf("2016-05-09 00:00:02")

    /** Columns in a table with only one row */
    val columnInfo1 = mutable.LinkedHashMap[Attribute, ColumnStat](
      AttributeReference("cbool", BooleanType)() -> ColumnStat(distinctCount = 1,
        min = Some(false), max = Some(false), nullCount = 0, avgLen = 1, maxLen = 1),
      AttributeReference("cbyte", ByteType)() -> ColumnStat(distinctCount = 1,
        min = Some(1L), max = Some(1L), nullCount = 0, avgLen = 1, maxLen = 1),
      AttributeReference("cshort", ShortType)() -> ColumnStat(distinctCount = 1,
        min = Some(1L), max = Some(1L), nullCount = 0, avgLen = 2, maxLen = 2),
      AttributeReference("cint", IntegerType)() -> ColumnStat(distinctCount = 1,
        min = Some(1L), max = Some(1L), nullCount = 0, avgLen = 4, maxLen = 4),
      AttributeReference("clong", LongType)() -> ColumnStat(distinctCount = 1,
        min = Some(1L), max = Some(1L), nullCount = 0, avgLen = 8, maxLen = 8),
      AttributeReference("cdouble", DoubleType)() -> ColumnStat(distinctCount = 1,
        min = Some(1.0), max = Some(1.0), nullCount = 0, avgLen = 8, maxLen = 8),
      AttributeReference("cfloat", FloatType)() -> ColumnStat(distinctCount = 1,
        min = Some(1.0), max = Some(1.0), nullCount = 0, avgLen = 4, maxLen = 4),
      AttributeReference("cdecimal", DecimalType.SYSTEM_DEFAULT)() -> ColumnStat(distinctCount = 1,
        min = Some(dec1), max = Some(dec1), nullCount = 0, avgLen = 16, maxLen = 16),
      AttributeReference("cstring", StringType)() -> ColumnStat(distinctCount = 1,
        min = None, max = None, nullCount = 0, avgLen = 3, maxLen = 3),
      AttributeReference("cbinary", BinaryType)() -> ColumnStat(distinctCount = 1,
        min = None, max = None, nullCount = 0, avgLen = 3, maxLen = 3),
      AttributeReference("cdate", DateType)() -> ColumnStat(distinctCount = 1,
        min = Some(d1), max = Some(d1), nullCount = 0, avgLen = 4, maxLen = 4),
      AttributeReference("ctimestamp", TimestampType)() -> ColumnStat(distinctCount = 1,
        min = Some(t1), max = Some(t1), nullCount = 0, avgLen = 8, maxLen = 8)
    )

    /** Columns in a table with two rows */
    val columnInfo2 = mutable.LinkedHashMap[Attribute, ColumnStat](
      AttributeReference("cbool", BooleanType)() -> ColumnStat(distinctCount = 2,
        min = Some(false), max = Some(true), nullCount = 0, avgLen = 1, maxLen = 1),
      AttributeReference("cbyte", ByteType)() -> ColumnStat(distinctCount = 2,
        min = Some(1L), max = Some(2L), nullCount = 0, avgLen = 1, maxLen = 1),
      AttributeReference("cshort", ShortType)() -> ColumnStat(distinctCount = 2,
        min = Some(1L), max = Some(3L), nullCount = 0, avgLen = 2, maxLen = 2),
      AttributeReference("cint", IntegerType)() -> ColumnStat(distinctCount = 2,
        min = Some(1L), max = Some(4L), nullCount = 0, avgLen = 4, maxLen = 4),
      AttributeReference("clong", LongType)() -> ColumnStat(distinctCount = 2,
        min = Some(1L), max = Some(5L), nullCount = 0, avgLen = 8, maxLen = 8),
      AttributeReference("cdouble", DoubleType)() -> ColumnStat(distinctCount = 2,
        min = Some(1.0), max = Some(6.0), nullCount = 0, avgLen = 8, maxLen = 8),
      AttributeReference("cfloat", FloatType)() -> ColumnStat(distinctCount = 2,
        min = Some(1.0), max = Some(7.0), nullCount = 0, avgLen = 4, maxLen = 4),
      AttributeReference("cdecimal", DecimalType.SYSTEM_DEFAULT)() -> ColumnStat(distinctCount = 2,
        min = Some(dec1), max = Some(dec2), nullCount = 0, avgLen = 16, maxLen = 16),
      AttributeReference("cstring", StringType)() -> ColumnStat(distinctCount = 2,
        min = None, max = None, nullCount = 0, avgLen = 3, maxLen = 3),
      AttributeReference("cbinary", BinaryType)() -> ColumnStat(distinctCount = 2,
        min = None, max = None, nullCount = 0, avgLen = 3, maxLen = 3),
      AttributeReference("cdate", DateType)() -> ColumnStat(distinctCount = 2,
        min = Some(d1), max = Some(d2), nullCount = 0, avgLen = 4, maxLen = 4),
      AttributeReference("ctimestamp", TimestampType)() -> ColumnStat(distinctCount = 2,
        min = Some(t1), max = Some(t2), nullCount = 0, avgLen = 8, maxLen = 8)
    )

    val oneRowTable = StatsTestPlan(
      outputList = columnInfo1.keys.toSeq,
      rowCount = 1,
      attributeStats = AttributeMap(columnInfo1.toSeq))
    val twoRowTable = StatsTestPlan(
      outputList = columnInfo2.keys.toSeq,
      rowCount = 2,
      attributeStats = AttributeMap(columnInfo2.toSeq))
    val joinKeys = oneRowTable.output.zip(twoRowTable.output)
    joinKeys.foreach { case (key1, key2) =>
      withClue(s"For data type ${key1.dataType}") {
        // All values in oneRowTable is contained in twoRowTable, so column stats after join is
        // equal to that of oneRowTable.
        val join = Join(Project(Seq(key1), oneRowTable), Project(Seq(key2), twoRowTable), Inner,
          Some(EqualTo(key1, key2)))
        val expectedStats = Statistics(
          sizeInBytes = 1 * (8 + 2 * getColSize(key1, columnInfo1(key1))),
          rowCount = Some(1),
          attributeStats = AttributeMap(Seq(key1 -> columnInfo1(key1), key2 -> columnInfo1(key1))))
        assert(join.stats(conf) == expectedStats)
      }
    }
  }

  test("join with null column") {
    val (nullColumn, nullColStat) = (attr("cnull"),
      ColumnStat(distinctCount = 0, min = None, max = None, nullCount = 1, avgLen = 4, maxLen = 4))
    val nullTable = StatsTestPlan(
      outputList = Seq(nullColumn),
      rowCount = 1,
      attributeStats = AttributeMap(Seq(nullColumn -> nullColStat)))
    val join = Join(table1, nullTable, Inner,
      Some(EqualTo(nameToAttr("key11"), nullColumn)))
    val emptyColStat = ColumnStat(distinctCount = 0, min = None, max = None, nullCount = 0,
      avgLen = 4, maxLen = 4)
    val expectedStats = Statistics(
      sizeInBytes = 1,
      rowCount = Some(0),
      attributeStats = AttributeMap(Seq(nameToAttr("key11") -> emptyColStat,
        nameToAttr("key12") -> emptyColStat, nullColumn -> emptyColStat)))
    assert(join.stats(conf) == expectedStats)
  }
}
