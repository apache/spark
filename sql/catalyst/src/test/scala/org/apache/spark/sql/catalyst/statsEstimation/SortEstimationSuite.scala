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

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.catalyst.plans.logical.{ColumnStat, Sort}
import org.apache.spark.sql.types.{BooleanType, ByteType}

class SortEstimationSuite extends StatsEstimationTestBase {

  test("test row size and column stats estimation") {
    val columnInfo: AttributeMap[ColumnStat] = AttributeMap(
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
    val expectedStats = logical.Statistics(
      sizeInBytes = expectedSize,
      rowCount = Some(2),
      attributeStats = columnInfo)
    assert(sort.stats === expectedStats)
  }
}
