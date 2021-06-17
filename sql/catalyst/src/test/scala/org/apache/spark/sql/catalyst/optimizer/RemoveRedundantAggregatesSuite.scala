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

package org.apache.spark.sql.catalyst.optimizer

import org.apache.spark.api.python.PythonEvalType
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions.{Expression, PythonUDF}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.types.IntegerType

class RemoveRedundantAggregatesSuite extends PlanTest {

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches = Batch("RemoveRedundantAggregates", FixedPoint(10),
      RemoveRedundantAggregates) :: Nil
  }

  private def aggregates(e: Expression): Seq[Expression] = {
    Seq(
      count(e),
      PythonUDF("pyUDF", null, IntegerType, Seq(e),
        PythonEvalType.SQL_GROUPED_AGG_PANDAS_UDF, udfDeterministic = true)
    )
  }

  test("Remove redundant aggregate") {
    val relation = LocalRelation('a.int, 'b.int)
    for (agg <- aggregates('b)) {
      val query = relation
        .groupBy('a)('a, agg)
        .groupBy('a)('a)
        .analyze
      val expected = relation
        .groupBy('a)('a)
        .analyze
      val optimized = Optimize.execute(query)
      comparePlans(optimized, expected)
    }
  }

  test("Remove 2 redundant aggregates") {
    val relation = LocalRelation('a.int, 'b.int)
    for (agg <- aggregates('b)) {
      val query = relation
        .groupBy('a)('a, agg)
        .groupBy('a)('a)
        .groupBy('a)('a)
        .analyze
      val expected = relation
        .groupBy('a)('a)
        .analyze
      val optimized = Optimize.execute(query)
      comparePlans(optimized, expected)
    }
  }

  test("Remove redundant aggregate with different grouping") {
    val relation = LocalRelation('a.int, 'b.int)
    val query = relation
      .groupBy('a, 'b)('a)
      .groupBy('a)('a)
      .analyze
    val expected = relation
      .groupBy('a)('a)
      .analyze
    val optimized = Optimize.execute(query)
    comparePlans(optimized, expected)
  }

  test("Remove redundant aggregate with aliases") {
    val relation = LocalRelation('a.int, 'b.int)
    for (agg <- aggregates('b)) {
      val query = relation
        .groupBy('a + 'b)(('a + 'b) as 'c, agg)
        .groupBy('c)('c)
        .analyze
      val expected = relation
        .groupBy('a + 'b)(('a + 'b) as 'c)
        .analyze
      val optimized = Optimize.execute(query)
      comparePlans(optimized, expected)
    }
  }

  test("Remove redundant aggregate with non-deterministic upper") {
    val relation = LocalRelation('a.int, 'b.int)
    val query = relation
      .groupBy('a)('a)
      .groupBy('a)('a, rand(0) as 'c)
      .analyze
    val expected = relation
      .groupBy('a)('a, rand(0) as 'c)
      .analyze
    val optimized = Optimize.execute(query)
    comparePlans(optimized, expected)
  }

  test("Remove redundant aggregate with non-deterministic lower") {
    val relation = LocalRelation('a.int, 'b.int)
    val query = relation
      .groupBy('a, 'c)('a, rand(0) as 'c)
      .groupBy('a, 'c)('a, 'c)
      .analyze
    val expected = relation
      .groupBy('a, 'c)('a, rand(0) as 'c)
      .analyze
    val optimized = Optimize.execute(query)
    comparePlans(optimized, expected)
  }

  test("Keep non-redundant aggregate - upper has duplicate sensitive agg expression") {
    val relation = LocalRelation('a.int, 'b.int)
    for (agg <- aggregates('b)) {
      val query = relation
        .groupBy('a, 'b)('a, 'b)
        // The count would change if we remove the first aggregate
        .groupBy('a)('a, agg)
        .analyze
      val optimized = Optimize.execute(query)
      comparePlans(optimized, query)
    }
  }

  test("Remove redundant aggregate - upper has duplicate agnostic agg expression") {
    val relation = LocalRelation('a.int, 'b.int)
    val query = relation
      .groupBy('a, 'b)('a, 'b)
      // The max and countDistinct does not change if there are duplicate values
      .groupBy('a)('a, max('b), countDistinct('b))
      .analyze
    val expected = relation
      .groupBy('a)('a, max('b), countDistinct('b))
      .analyze
    val optimized = Optimize.execute(query)
    comparePlans(optimized, expected)
  }

  test("Keep non-redundant aggregate - upper references agg expression") {
    val relation = LocalRelation('a.int, 'b.int)
    for (agg <- aggregates('b)) {
      val query = relation
        .groupBy('a)('a, agg as 'c)
        .groupBy('c)('c)
        .analyze
      val optimized = Optimize.execute(query)
      comparePlans(optimized, query)
    }
  }

  test("Keep non-redundant aggregate - upper references non-deterministic non-grouping") {
    val relation = LocalRelation('a.int, 'b.int)
    val query = relation
      .groupBy('a)('a, ('a + rand(0)) as 'c)
      .groupBy('a, 'c)('a, 'c)
      .analyze
    val optimized = Optimize.execute(query)
    comparePlans(optimized, query)
  }
}
