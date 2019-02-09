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

import org.apache.spark.sql.catalyst.analysis.EliminateSubqueryAliases
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules._
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.unsafe.types.CalendarInterval

class LeftSemiPushdownSuite extends PlanTest {

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("Subqueries", Once,
        EliminateSubqueryAliases) ::
      Batch("Filter Pushdown", FixedPoint(10),
        CombineFilters,
        PushDownPredicate,
        PushDownLeftSemiAntiJoin,
        BooleanSimplification,
        PushPredicateThroughJoin,
        CollapseProject) :: Nil
  }

  val testRelation = LocalRelation('a.int, 'b.int, 'c.int)

  val testRelation1 = LocalRelation('d.int)

  test("Project: LeftSemiAnti join pushdown") {
    val originalQuery = testRelation
      .select(star())
      .join(testRelation1, joinType = LeftSemi, condition = Some('b === 'd))

    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer = testRelation
      .join(testRelation1, joinType = LeftSemi, condition = Some('b === 'd))
      .select('a, 'b, 'c)
      .analyze
    comparePlans(optimized, correctAnswer)
  }

  test("Project: LeftSemiAnti join no pushdown because of non-deterministic proj exprs") {
    val x = testRelation.subquery('x)
    val y = testRelation1.subquery('y)

    val originalQuery = testRelation
      .select(Rand('a), 'b, 'c)
      .join(testRelation1, joinType = LeftSemi, condition = Some('b === 'd))

    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer = testRelation
      .select(Rand('a), 'b, 'c)
      .join(y, joinType = LeftSemi, condition = Some('b === 'd))
      .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("Aggregate: LeftSemiAnti join pushdown") {
    val originalQuery = testRelation
      .groupBy('b)('b, sum('c))
      .join(testRelation1, joinType = LeftSemi, condition = Some('b === 'd))

    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer = testRelation
      .join(testRelation1, joinType = LeftSemi, condition = Some('b === 'd))
      .groupBy('b)('b, sum('c))
      .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("Aggregate: LeftSemiAnti join no pushdown due to non-deterministic aggr expressions") {
    val originalQuery = testRelation
      .groupBy('b)('b, Rand(10).as('c))
      .join(testRelation1, joinType = LeftSemi, condition = Some('b === 'd))

    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer = testRelation
      .groupBy('b)('b, Rand(10).as('c))
      .join(testRelation1, joinType = LeftSemi, condition = Some('b === 'd))
      .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("Aggregate: LeftSemiAnti join partial pushdown") {
    val originalQuery = testRelation
      // .select('b.as('alias1))
      .groupBy('b)('b, sum('c).as('sum))
      .join(testRelation1, joinType = LeftSemi, condition = Some('b === 'd && 'sum === 10))

    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer = testRelation
      .join(testRelation1, joinType = LeftSemi, condition = Some('b === 'd))
      .groupBy('b)('b, sum('c).as('sum))
      .where('sum === 10)
      .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("LeftSemiAnti join over aggregate - no pushdown") {
    val originalQuery = testRelation
      // .select('b.as('alias1))
      .groupBy('b)('b, sum('c).as('sum))
      .join(testRelation1, joinType = LeftSemi, condition = Some('b === 'd && 'sum === 'd))

    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer = testRelation
      // .select('b.as('alias1))
      .groupBy('b)('b, sum('c).as('sum))
      .join(testRelation1, joinType = LeftSemi, condition = Some('b === 'd && 'sum === 'd))
      .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("LeftSemiAnti join over Window") {
    val winExpr = windowExpr(count('b), windowSpec('a :: Nil, 'b.asc :: Nil, UnspecifiedFrame))

    val originalQuery = testRelation
      .select('a, 'b, 'c, winExpr.as('window))
      .join(testRelation1, joinType = LeftSemi, condition = Some('a === 'd))

    val optimized = Optimize.execute(originalQuery.analyze)

    val correctAnswer = testRelation
      .join(testRelation1, joinType = LeftSemi, condition = Some('a === 'd))
      .select('a, 'b, 'c)
      .window(winExpr.as('window) :: Nil, 'a :: Nil, 'b.asc :: Nil)
      .select('a, 'b, 'c, 'window).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("Window: LeftSemiAnti partial pushdown") {
    // Attributes from join condition which does not refer to the window partition spec
    // are kept up in the plan as a Filter operator above Window.
    val winExpr = windowExpr(count('b), windowSpec('a :: Nil, 'b.asc :: Nil, UnspecifiedFrame))

    val originalQuery = testRelation
      .select('a, 'b, 'c, winExpr.as('window))
      .join(testRelation1, joinType = LeftSemi, condition = Some('a === 'd && 'b > 5))

    val optimized = Optimize.execute(originalQuery.analyze)

    val correctAnswer = testRelation
      .join(testRelation1, joinType = LeftSemi, condition = Some('a === 'd))
      .select('a, 'b, 'c)
      .window(winExpr.as('window) :: Nil, 'a :: Nil, 'b.asc :: Nil)
      .where('b > 5)
      .select('a, 'b, 'c, 'window).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("Union: LeftSemiAnti join pushdown") {
      val testRelation2 = LocalRelation('x.int, 'y.int, 'z.int)

      val originalQuery = Union(Seq(testRelation, testRelation2))
        .join(testRelation1, joinType = LeftSemi, condition = Some('a === 'd))

      val optimized = Optimize.execute(originalQuery.analyze)

      val correctAnswer = Union(Seq(
        testRelation.join(testRelation1, joinType = LeftSemi, condition = Some('a === 'd)),
        testRelation2.join(testRelation1, joinType = LeftSemi, condition = Some('x === 'd))))
        .analyze

      comparePlans(optimized, correctAnswer)
  }

  test("Union: LeftSemiAnti join no pushdown in self join scenario") {
    val testRelation2 = LocalRelation('x.int, 'y.int, 'z.int)

    val originalQuery = Union(Seq(testRelation, testRelation2))
      .join(testRelation2, joinType = LeftSemi, condition = Some('a === 'x))

    val optimized = Optimize.execute(originalQuery.analyze)

    comparePlans(optimized, originalQuery.analyze)
  }

  test("Unary: LeftSemiAnti join pushdown") {
    val originalQuery = testRelation
      .select(star())
      .repartition(1)
      .join(testRelation1, joinType = LeftSemi, condition = Some('b === 'd))

    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer = testRelation
      .join(testRelation1, joinType = LeftSemi, condition = Some('b === 'd))
      .select('a, 'b, 'c)
      .repartition(1)
      .analyze
    comparePlans(optimized, correctAnswer)
  }
}
