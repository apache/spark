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

import org.apache.spark.sql.catalyst.expressions.{AttributeMap, AttributeReference}
import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.catalyst.plans.logical.{ColumnStat, Union}
import org.apache.spark.sql.types.{DoubleType, IntegerType}

class UnionEstimationSuite extends StatsEstimationTestBase {

  test("test row size estimation") {
    val attrInt = AttributeReference("cint", IntegerType)()

    val sz : Option[BigInt] = Some(1024)
    val child1 = StatsTestPlan(
      outputList = Seq(attrInt),
      rowCount = 2,
      attributeStats = AttributeMap(Nil), size = sz)

    val child2 = StatsTestPlan(
      outputList = Seq(attrInt),
      rowCount = 2,
      attributeStats = AttributeMap(Nil), size = sz)

    val union = Union(Seq(child1, child2))
    val expectedStats = logical.Statistics(sizeInBytes = 2 * 1024, rowCount = Some(4))
    assert(union.stats === expectedStats)
  }

  test("col stats estimation") {
    val sz : Option[BigInt] = Some(1024)

    val attrInt = AttributeReference("cint", IntegerType)()
    val attrDouble = AttributeReference("cdouble", DoubleType)()


    val columnInfo: AttributeMap[ColumnStat] = AttributeMap(Seq(
      attrInt -> ColumnStat(distinctCount = Some(2),
        min = Some(1), max = Some(4),
        nullCount = Some(0), avgLen = Some(4), maxLen = Some(4)),
      attrDouble -> ColumnStat(distinctCount = Some(2),
        min = Some(5.0), max = Some(4.0),
        nullCount = Some(0), avgLen = Some(4), maxLen = Some(4))
    ))

    val columnInfo1: AttributeMap[ColumnStat] = AttributeMap(Seq(
      AttributeReference("cint1", IntegerType)() -> ColumnStat(distinctCount = Some(2),
        min = Some(3), max = Some(6),
        nullCount = Some(0), avgLen = Some(8), maxLen = Some(8)),
      AttributeReference("cdouble1", DoubleType)() -> ColumnStat(distinctCount = Some(2),
        min = Some(2.0), max = Some(7.0),
        nullCount = Some(0), avgLen = Some(8), maxLen = Some(8))
    ))

    val child1 = StatsTestPlan(
      outputList = columnInfo.keys.toSeq,
      rowCount = 2,
      attributeStats = columnInfo, size = sz)

    val child2 = StatsTestPlan(
      outputList = columnInfo1.keys.toSeq,
      rowCount = 2,
      attributeStats = columnInfo1, size = sz)

    val union = Union(Seq(child1, child2))

    val expectedStats = logical.Statistics(sizeInBytes = 2 * 1024, rowCount = Some(4),
      attributeStats = AttributeMap(Seq(attrInt -> ColumnStat(min = Some(1), max = Some(6)),
        attrDouble -> ColumnStat(min = Some(2.0), max = Some(7.0)))))
    assert(union.stats === expectedStats)
  }

  test("col stats estimation when min max stats not present for one child") {
    val sz : Option[BigInt] = Some(1024)

    val attrInt = AttributeReference("cint", IntegerType)()

    val columnInfo: AttributeMap[ColumnStat] = AttributeMap(Seq(
      attrInt -> ColumnStat(distinctCount = Some(2), min = Some(2), max = Some(2),
        nullCount = Some(0), avgLen = Some(4), maxLen = Some(4))
    ))

    val columnInfo1: AttributeMap[ColumnStat] = AttributeMap(Seq(
      AttributeReference("cint1", IntegerType)() -> ColumnStat(distinctCount = Some(2),
        nullCount = Some(0), avgLen = Some(8), maxLen = Some(8))
    ))

    val child1 = StatsTestPlan(
      outputList = columnInfo.keys.toSeq,
      rowCount = 2,
      attributeStats = columnInfo, size = sz)

    val child2 = StatsTestPlan(
      outputList = columnInfo1.keys.toSeq,
      rowCount = 2,
      attributeStats = columnInfo1, size = sz)

    val union = Union(Seq(child1, child2))

    val expectedStats = logical.Statistics(sizeInBytes = 2 * 1024, rowCount = Some(4))
    assert(union.stats === expectedStats)
  }
}
