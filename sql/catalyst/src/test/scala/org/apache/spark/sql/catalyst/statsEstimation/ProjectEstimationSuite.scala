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

import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeMap, AttributeReference}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.plans.logical.statsEstimation.EstimationUtils._
import org.apache.spark.sql.types.IntegerType


class ProjectEstimationSuite extends StatsEstimationTestBase {

  test("estimate project with alias") {
    val ar1 = AttributeReference("key1", IntegerType)()
    val ar2 = AttributeReference("key2", IntegerType)()
    val colStat1 = ColumnStat(2, Some(1), Some(2), 0, 4, 4)
    val colStat2 = ColumnStat(1, Some(10), Some(10), 0, 4, 4)

    val child = StatsTestPlan(
      outputList = Seq(ar1, ar2),
      stats = Statistics(
        sizeInBytes = 2 * (4 + 4),
        rowCount = Some(2),
        attributeStats = AttributeMap(Seq(ar1 -> colStat1, ar2 -> colStat2))))

    val project = Project(Seq(ar1, Alias(ar2, "abc")()), child)
    val expectedColStats = Seq("key1" -> colStat1, "abc" -> colStat2)
    val expectedAttrStats = toAttributeMap(expectedColStats, project)
    // The number of rows won't change for project.
    val expectedStats = Statistics(
      sizeInBytes = 2 * getRowSize(project.output, expectedAttrStats),
      rowCount = Some(2),
      attributeStats = expectedAttrStats)
    assert(project.statistics == expectedStats)
  }
}
