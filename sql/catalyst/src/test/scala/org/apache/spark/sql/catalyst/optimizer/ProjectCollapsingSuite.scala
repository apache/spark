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

import org.apache.spark.sql.catalyst.analysis.EliminateSubQueries
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions.Rand
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.RuleExecutor


class ProjectCollapsingSuite extends PlanTest {
  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("Subqueries", FixedPoint(10), EliminateSubQueries) ::
        Batch("ProjectCollapsing", Once, ProjectCollapsing) :: Nil
  }

  val testRelation = LocalRelation('a.int, 'b.int)

  test("collapse two deterministic, independent projects into one") {
    val query = testRelation
      .select(('a + 1).as('a_plus_1), 'b)
      .select('a_plus_1, ('b + 1).as('b_plus_1))

    val optimized = Optimize.execute(query.analyze)
    val correctAnswer = testRelation.select(('a + 1).as('a_plus_1), ('b + 1).as('b_plus_1)).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("collapse two deterministic, dependent projects into one") {
    val query = testRelation
      .select(('a + 1).as('a_plus_1), 'b)
      .select(('a_plus_1 + 1).as('a_plus_2), 'b)

    val optimized = Optimize.execute(query.analyze)

    val correctAnswer = testRelation.select(
      (('a + 1).as('a_plus_1) + 1).as('a_plus_2),
      'b).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("do not collapse nondeterministic projects") {
    val query = testRelation
      .select(Rand(10).as('rand))
      .select(('rand + 1).as('rand1), ('rand + 2).as('rand2))

    val optimized = Optimize.execute(query.analyze)
    val correctAnswer = query.analyze

    comparePlans(optimized, correctAnswer)
  }

  test("collapse two nondeterministic, independent projects into one") {
    val query = testRelation
      .select(Rand(10).as('rand))
      .select(Rand(20).as('rand2))

    val optimized = Optimize.execute(query.analyze)

    val correctAnswer = testRelation
      .select(Rand(20).as('rand2)).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("collapse one nondeterministic, one deterministic, independent projects into one") {
    val query = testRelation
      .select(Rand(10).as('rand), 'a)
      .select(('a + 1).as('a_plus_1))

    val optimized = Optimize.execute(query.analyze)

    val correctAnswer = testRelation
      .select(('a + 1).as('a_plus_1)).analyze

    comparePlans(optimized, correctAnswer)
  }
}
