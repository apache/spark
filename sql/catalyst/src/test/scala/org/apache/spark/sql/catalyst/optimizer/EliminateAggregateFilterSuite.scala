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
import org.apache.spark.sql.catalyst.expressions.{GreaterThan, Literal}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.types.LongType

class EliminateAggregateFilterSuite extends PlanTest {
  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("Operator Optimizations", Once, ConstantFolding, EliminateAggregateFilter) :: Nil
  }

  val testRelation = LocalRelation('a.int)

  test("Eliminate Filter always is true") {
    val query = testRelation
      .select(sumDistinct('a, Some(Literal.TrueLiteral)).as('result))
      .analyze
    val answer = testRelation
      .select(sumDistinct('a).as('result))
      .analyze
    comparePlans(Optimize.execute(query), answer)
  }

  test("Eliminate Filter is foldable and always is true") {
    val query = testRelation
      .select(countDistinctWithFilter(GreaterThan(Literal(2), Literal(1)), 'a).as('result))
      .analyze
    val answer = testRelation
      .select(countDistinct('a).as('result))
      .analyze
    comparePlans(Optimize.execute(query), answer)
  }

  test("Eliminate Filter always is false") {
    val query = testRelation
      .select(sumDistinct('a, Some(Literal.FalseLiteral)).as('result))
      .analyze
    val answer = testRelation
      .groupBy()(Literal.create(null, LongType).as('result))
      .analyze
    comparePlans(Optimize.execute(query), answer)
  }

  test("Eliminate Filter is foldable and always is false") {
    val query = testRelation
      .select(countDistinctWithFilter(GreaterThan(Literal(1), Literal(2)), 'a).as('result))
      .analyze
    val answer = testRelation
      .groupBy()(Literal.create(0L, LongType).as('result))
      .analyze
    comparePlans(Optimize.execute(query), answer)
  }

  test("SPARK-38177: Eliminate Filter in non-root node") {
    val query = testRelation
      .select(countDistinctWithFilter(GreaterThan(Literal(1), Literal(2)), 'a).as('result))
      .limit(1)
      .analyze
    val answer = testRelation
      .groupBy()(Literal.create(0L, LongType).as('result))
      .limit(1)
      .analyze
    comparePlans(Optimize.execute(query), answer)
  }
}
