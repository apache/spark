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
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules._

/**
 * This test suite ensures that the [[PushDownPredicates]] actually does predicate pushdown in
 * an efficient manner. This is enforced by asserting that a single predicate pushdown can push
 * all predicate to bottom as much as possible.
 */
class FilterPushdownOnePassSuite extends PlanTest {

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("Subqueries", Once,
        EliminateSubqueryAliases) ::
      // this batch must reach expected state in one pass
      Batch("Filter Pushdown One Pass", Once,
        ReorderJoin,
        PushDownPredicates
      ) :: Nil
  }

  val testRelation1 = LocalRelation('a.int, 'b.int, 'c.int)
  val testRelation2 = LocalRelation('a.int, 'd.int, 'e.int)

  test("really simple predicate push down") {
    val x = testRelation1.subquery('x)
    val y = testRelation2.subquery('y)

    val originalQuery = x.join(y).where("x.a".attr === 1)

    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer = x.where("x.a".attr === 1).join(y).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("push down conjunctive predicates") {
    val x = testRelation1.subquery('x)
    val y = testRelation2.subquery('y)

    val originalQuery = x.join(y).where("x.a".attr === 1 && "y.d".attr < 1)

    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer = x.where("x.a".attr === 1).join(y.where("y.d".attr < 1)).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("push down predicates for simple joins") {
    val x = testRelation1.subquery('x)
    val y = testRelation2.subquery('y)

    val originalQuery =
      x.where("x.c".attr < 0)
        .join(y.where("y.d".attr > 1))
        .where("x.a".attr === 1 && "y.d".attr < 2)

    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer =
      x.where("x.c".attr < 0 && "x.a".attr === 1)
        .join(y.where("y.d".attr > 1 && "y.d".attr < 2)).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("push down top-level filters for cascading joins") {
    val x = testRelation1.subquery('x)
    val y = testRelation2.subquery('y)

    val originalQuery =
      y.join(x).join(x).join(x).join(x).join(x).where("y.d".attr === 0)

    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer = y.where("y.d".attr === 0).join(x).join(x).join(x).join(x).join(x).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("push down predicates for tree-like joins") {
    val x = testRelation1.subquery('x)
    val y1 = testRelation2.subquery('y1)
    val y2 = testRelation2.subquery('y2)

    val originalQuery =
      y1.join(x).join(x)
        .join(y2.join(x).join(x))
        .where("y1.d".attr === 0 && "y2.d".attr === 3)

    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer =
      y1.where("y1.d".attr === 0).join(x).join(x)
        .join(y2.where("y2.d".attr === 3).join(x).join(x)).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("push down through join and project") {
    val x = testRelation1.subquery('x)
    val y = testRelation2.subquery('y)

    val originalQuery =
      x.where('a > 0).select('a, 'b)
        .join(y.where('d < 100).select('e))
        .where("x.a".attr < 100)

    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer =
      x.where('a > 0 && 'a < 100).select('a, 'b)
        .join(y.where('d < 100).select('e)).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("push down through deep projects") {
    val x = testRelation1.subquery('x)

    val originalQuery =
      x.select(('a + 1) as 'a1, 'b)
        .select(('a1 + 1) as 'a2, 'b)
        .select(('a2 + 1) as 'a3, 'b)
        .select(('a3 + 1) as 'a4, 'b)
        .select('b)
        .where('b > 0)

    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer =
      x.where('b > 0)
        .select(('a + 1) as 'a1, 'b)
        .select(('a1 + 1) as 'a2, 'b)
        .select(('a2 + 1) as 'a3, 'b)
        .select(('a3 + 1) as 'a4, 'b)
        .select('b).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("push down through aggregate and join") {
    val x = testRelation1.subquery('x)
    val y = testRelation2.subquery('y)

    val left = x
      .where('c > 0)
      .groupBy('a)('a, count('b))
      .subquery('left)
    val right = y
      .where('d < 0)
      .groupBy('a)('a, count('d))
      .subquery('right)
    val originalQuery = left
      .join(right).where("left.a".attr < 100 && "right.a".attr < 100)

    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer =
      x.where('c > 0 && 'a < 100).groupBy('a)('a, count('b))
        .join(y.where('d < 0 && 'a < 100).groupBy('a)('a, count('d)))
        .analyze

    comparePlans(optimized, correctAnswer)
  }
}
