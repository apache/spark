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

import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.RuleExecutor

class RemoveRedundantAggregatesSuite extends PlanTest {

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches = Batch("RemoveRedundantAggregates", FixedPoint(10),
      RemoveRedundantAggregates) :: Nil
  }

  test("Remove redundant aggregate") {
    val relation = LocalRelation('a.int, 'b.int)
    val query = relation
      .groupBy('a)('a, count('b))
      .groupBy('a)('a)
      .analyze
    val expected = relation
      .groupBy('a)('a)
      .analyze
    val optimized = Optimize.execute(query)
    comparePlans(optimized, expected)
  }

  test("Remove 2 redundant aggregates") {
    val relation = LocalRelation('a.int, 'b.int)
    val query = relation
      .groupBy('a)('a, count('b))
      .groupBy('a)('a)
      .groupBy('a)('a)
      .analyze
    val expected = relation
      .groupBy('a)('a)
      .analyze
    val optimized = Optimize.execute(query)
    comparePlans(optimized, expected)
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

  test("Keep non-redundant aggregate") {
    val relation = LocalRelation('a.int, 'b.int)
    val query = relation
      .groupBy('a)('a, first('b) as 'b)
      // The count would change if we remove the first aggregate
      .groupBy('a)('a, count('b))
      .analyze
    val optimized = Optimize.execute(query)
    comparePlans(optimized, query)
  }
}
