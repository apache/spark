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

package org.apache.spark.sql.catalyst.util

import org.apache.spark.sql.catalyst.analysis.SimpleAnalyzer
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.planning.PhysicalAggregation
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation

class PhysicalAggregationSuite extends PlanTest {
  val testRelation = LocalRelation($"a".int, $"b".int)

  test("SPARK-35014: a foldable expression should not be replaced by an AttributeReference") {
    val query = testRelation
      .groupBy($"a", Literal.create(1) as Symbol("k"))(
        $"a", Round(Literal.create(1.2), Literal.create(1)) as Symbol("r"),
        count($"b") as Symbol("c"))
    val analyzedQuery = SimpleAnalyzer.execute(query)

    val PhysicalAggregation(
      groupingExpressions,
      aggregateExpressions,
      resultExpressions,
      _ /* child */
    ) = analyzedQuery

    assertResult(2)(groupingExpressions.length)
    assertResult(1)(aggregateExpressions.length)
    assertResult(3)(resultExpressions.length)

    // Verify that Round's scale parameter is a Literal.
    resultExpressions(1) match {
      case Alias(Round(_, _: Literal), _) =>
      case other => fail("unexpected result expression: " + other)
    }
  }
}
