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
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LocalRelation, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.RuleExecutor

class PullupCorrelatedPredicatesSuite extends PlanTest {

  object Optimize extends RuleExecutor[LogicalPlan] {
    override protected val blacklistedOnceBatches = Set("PullupCorrelatedPredicates")

    val batches =
      Batch("PullupCorrelatedPredicates", Once,
        PullupCorrelatedPredicates) :: Nil
  }

  val testRelation = LocalRelation('a.int, 'b.double)
  val testRelation2 = LocalRelation('c.int, 'd.double)

  test("PullupCorrelatedPredicates should not produce unresolved plan") {
    val subPlan =
      testRelation2
        .where('b < 'd)
        .select('c)
    val inSubquery =
      testRelation
        .where(InSubquery(Seq('a), ListQuery(subPlan)))
        .select('a).analyze
    assert(inSubquery.resolved)

    val optimized = Optimize.execute(inSubquery)
    assert(optimized.resolved)
  }

  test("PullupCorrelatedPredicates in correlated subquery idempotency check") {
    val subPlan =
      testRelation2
      .where('b < 'd)
      .select('c)
    val inSubquery =
      testRelation
      .where(InSubquery(Seq('a), ListQuery(subPlan)))
      .select('a).analyze
    assert(inSubquery.resolved)

    val optimized = Optimize.execute(inSubquery)
    val doubleOptimized = Optimize.execute(optimized)
    comparePlans(optimized, doubleOptimized)
  }

  test("PullupCorrelatedPredicates exists correlated subquery idempotency check") {
    val subPlan =
      testRelation2
        .where('b === 'd && 'd === 1)
        .select(Literal(1))
    val existsSubquery =
      testRelation
        .where(Exists(subPlan))
        .select('a).analyze
    assert(existsSubquery.resolved)

    val optimized = Optimize.execute(existsSubquery)
    val doubleOptimized = Optimize.execute(optimized)
    comparePlans(optimized, doubleOptimized)
  }

  test("PullupCorrelatedPredicates scalar correlated subquery idempotency check") {
    val subPlan =
      testRelation2
        .where('b === 'd && 'd === 1)
        .select(max('d))
    val scalarSubquery =
      testRelation
        .where(ScalarSubquery(subPlan) === 1)
        .select('a).analyze

    val optimized = Optimize.execute(scalarSubquery)
    val doubleOptimized = Optimize.execute(optimized)
    comparePlans(optimized, doubleOptimized, false)
  }
}
