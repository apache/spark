
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

/**
 * In this test suite, we test the proedicates containing the following operators:
 * =, <, <=, >, >=, AND, OR, IS NULL, IS NOT NULL, IN, NOT IN
 */

class FilterEstimationSuite extends StatsEstimationTestBase {

  test("filter estimation with equality comparison") {
    val ar = AttributeReference("key1", IntegerType)()
    val colStat = ColumnStat(2, Some(1), Some(2), 0, 4, 4)

    val child = StatsTestPlan(
      outputList = Seq(ar),
      stats = Statistics(
        sizeInBytes = 10 * 4,
        rowCount = Some(10),
        attributeStats = AttributeMap(Seq(ar -> colStat))
      )
    )

    val filterNode = Filter(condition: Expression, child)
    val expectedColStats = Seq("key1" -> colStat)
    val expectedAttrStats = toAttributeMap(expectedColStats, filterNode)
    // The number of rows won't change for project.
    val expectedStats = Statistics(
      sizeInBytes = 2 * getRowSize(filterNode.output, expectedAttrStats),
      rowCount = Some(2),
      attributeStats = expectedAttrStats)
    assert(filterNode.statistics == expectedStats)
  }
}

