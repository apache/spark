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

import org.apache.spark.sql.catalyst.expressions.{And, Attribute, AttributeMap, EqualTo}
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical.{ColumnStat, Join, Statistics}


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
}
