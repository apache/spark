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

import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeMap, AttributeReference}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.types._


class ProjectEstimationSuite extends StatsEstimationTestBase {

  test("project with alias") {
    val (ar1, colStat1) = (attr("key1"), ColumnStat(distinctCount = 2, min = Some(1),
      max = Some(2), nullCount = 0, avgLen = 4, maxLen = 4))
    val (ar2, colStat2) = (attr("key2"), ColumnStat(distinctCount = 1, min = Some(10),
      max = Some(10), nullCount = 0, avgLen = 4, maxLen = 4))

    val child = StatsTestPlan(
      outputList = Seq(ar1, ar2),
      rowCount = 2,
      attributeStats = AttributeMap(Seq(ar1 -> colStat1, ar2 -> colStat2)))

    val proj = Project(Seq(ar1, Alias(ar2, "abc")()), child)
    val expectedColStats = Seq("key1" -> colStat1, "abc" -> colStat2)
    val expectedAttrStats = toAttributeMap(expectedColStats, proj)
    val expectedStats = Statistics(
      sizeInBytes = 2 * (8 + 4 + 4),
      rowCount = Some(2),
      attributeStats = expectedAttrStats)
    assert(proj.stats(conf) == expectedStats)
  }

  test("project on empty table") {
    val (ar1, colStat1) = (attr("key1"), ColumnStat(distinctCount = 0, min = None, max = None,
      nullCount = 0, avgLen = 4, maxLen = 4))
    val child = StatsTestPlan(
      outputList = Seq(ar1),
      rowCount = 0,
      attributeStats = AttributeMap(Seq(ar1 -> colStat1)))
    checkProjectStats(
      child = child,
      projectAttrMap = child.attributeStats,
      expectedSize = 1,
      expectedRowCount = 0)
  }

  test("test row size estimation") {
    val dec1 = Decimal("1.000000000000000000")
    val dec2 = Decimal("8.000000000000000000")
    val d1 = DateTimeUtils.fromJavaDate(Date.valueOf("2016-05-08"))
    val d2 = DateTimeUtils.fromJavaDate(Date.valueOf("2016-05-09"))
    val t1 = DateTimeUtils.fromJavaTimestamp(Timestamp.valueOf("2016-05-08 00:00:01"))
    val t2 = DateTimeUtils.fromJavaTimestamp(Timestamp.valueOf("2016-05-09 00:00:02"))

    val columnInfo: AttributeMap[ColumnStat] = AttributeMap(Seq(
      AttributeReference("cbool", BooleanType)() -> ColumnStat(distinctCount = 2,
        min = Some(false), max = Some(true), nullCount = 0, avgLen = 1, maxLen = 1),
      AttributeReference("cbyte", ByteType)() -> ColumnStat(distinctCount = 2,
        min = Some(1.toByte), max = Some(2.toByte), nullCount = 0, avgLen = 1, maxLen = 1),
      AttributeReference("cshort", ShortType)() -> ColumnStat(distinctCount = 2,
        min = Some(1.toShort), max = Some(3.toShort), nullCount = 0, avgLen = 2, maxLen = 2),
      AttributeReference("cint", IntegerType)() -> ColumnStat(distinctCount = 2,
        min = Some(1), max = Some(4), nullCount = 0, avgLen = 4, maxLen = 4),
      AttributeReference("clong", LongType)() -> ColumnStat(distinctCount = 2,
        min = Some(1L), max = Some(5L), nullCount = 0, avgLen = 8, maxLen = 8),
      AttributeReference("cdouble", DoubleType)() -> ColumnStat(distinctCount = 2,
        min = Some(1.0), max = Some(6.0), nullCount = 0, avgLen = 8, maxLen = 8),
      AttributeReference("cfloat", FloatType)() -> ColumnStat(distinctCount = 2,
        min = Some(1.0f), max = Some(7.0f), nullCount = 0, avgLen = 4, maxLen = 4),
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
    ))
    val columnSizes: Map[Attribute, Long] = columnInfo.map(kv => (kv._1, getColSize(kv._1, kv._2)))
    val child = StatsTestPlan(
      outputList = columnInfo.keys.toSeq,
      rowCount = 2,
      attributeStats = columnInfo)

    // Row with single column
    columnInfo.keys.foreach { attr =>
      withClue(s"For data type ${attr.dataType}") {
        checkProjectStats(
          child = child,
          projectAttrMap = AttributeMap(attr -> columnInfo(attr) :: Nil),
          expectedSize = 2 * (8 + columnSizes(attr)),
          expectedRowCount = 2)
      }
    }

    // Row with multiple columns
    checkProjectStats(
      child = child,
      projectAttrMap = columnInfo,
      expectedSize = 2 * (8 + columnSizes.values.sum),
      expectedRowCount = 2)
  }

  private def checkProjectStats(
      child: LogicalPlan,
      projectAttrMap: AttributeMap[ColumnStat],
      expectedSize: BigInt,
      expectedRowCount: BigInt): Unit = {
    val proj = Project(projectAttrMap.keys.toSeq, child)
    val expectedStats = Statistics(
      sizeInBytes = expectedSize,
      rowCount = Some(expectedRowCount),
      attributeStats = projectAttrMap)
    assert(proj.stats(conf) == expectedStats)
  }
}
