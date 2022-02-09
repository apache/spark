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
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules._

class FoldablePropagationSuite extends PlanTest {
  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("Foldable Propagation", FixedPoint(20),
        FoldablePropagation) :: Nil
  }

  val testRelation = LocalRelation('a.int, 'b.int)

  test("Propagate from subquery") {
    val query = OneRowRelation()
      .select(Literal(1).as('a), Literal(2).as('b))
      .subquery('T)
      .select('a, 'b)
    val optimized = Optimize.execute(query.analyze)
    val correctAnswer = OneRowRelation()
      .select(Literal(1).as('a), Literal(2).as('b))
      .subquery('T)
      .select(Literal(1).as('a), Literal(2).as('b)).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("Propagate to select clause") {
    val query = testRelation
      .select('a.as('x), "str".as('y), 'b.as('z))
      .select('x, 'y, 'z)
    val optimized = Optimize.execute(query.analyze)
    val correctAnswer = testRelation
      .select('a.as('x), "str".as('y), 'b.as('z))
      .select('x, "str".as('y), 'z).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("Propagate to where clause") {
    val query = testRelation
      .select("str".as('y))
      .where('y === "str" && "str" === 'y)
    val optimized = Optimize.execute(query.analyze)
    val correctAnswer = testRelation
      .select("str".as('y))
      .where("str".as('y) === "str" && "str" === "str".as('y)).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("Propagate to orderBy clause") {
    val query = testRelation
      .select('a.as('x), Year(CurrentDate()).as('y), 'b)
      .orderBy('x.asc, 'y.asc, 'b.desc)
    val optimized = Optimize.execute(query.analyze)
    val correctAnswer = testRelation
      .select('a.as('x), Year(CurrentDate()).as('y), 'b)
      .orderBy('x.asc, SortOrder(Year(CurrentDate()), Ascending), 'b.desc).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("Propagate to groupBy clause") {
    val query = testRelation
      .select('a.as('x), Year(CurrentDate()).as('y), 'b)
      .groupBy('x, 'y, 'b)(sum('x), avg('y).as('AVG), count('b))
    val optimized = Optimize.execute(query.analyze)
    val correctAnswer = testRelation
      .select('a.as('x), Year(CurrentDate()).as('y), 'b)
      .groupBy('x, Year(CurrentDate()).as('y), 'b)(sum('x), avg(Year(CurrentDate())).as('AVG),
        count('b)).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("Propagate in a complex query") {
    val query = testRelation
      .select('a.as('x), Year(CurrentDate()).as('y), 'b)
      .where('x > 1 && 'y === 2016 && 'b > 1)
      .groupBy('x, 'y, 'b)(sum('x), avg('y).as('AVG), count('b))
      .orderBy('x.asc, 'AVG.asc)
    val optimized = Optimize.execute(query.analyze)
    val correctAnswer = testRelation
      .select('a.as('x), Year(CurrentDate()).as('y), 'b)
      .where('x > 1 && Year(CurrentDate()).as('y) === 2016 && 'b > 1)
      .groupBy('x, Year(CurrentDate()).as("y"), 'b)(sum('x), avg(Year(CurrentDate())).as('AVG),
        count('b))
      .orderBy('x.asc, 'AVG.asc).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("Propagate in subqueries of Union queries") {
    val query = Union(
      Seq(
        testRelation.select(Literal(1).as('x), 'a).select('x, 'x + 'a),
        testRelation.select(Literal(2).as('x), 'a).select('x, 'x + 'a)))
      .select('x)
    val optimized = Optimize.execute(query.analyze)
    val correctAnswer = Union(
      Seq(
        testRelation.select(Literal(1).as('x), 'a)
          .select(Literal(1).as('x), (Literal(1).as('x) + 'a).as("(x + a)")),
        testRelation.select(Literal(2).as('x), 'a)
          .select(Literal(2).as('x), (Literal(2).as('x) + 'a).as("(x + a)"))))
      .select('x).analyze
    comparePlans(optimized, correctAnswer)
  }

  test("Propagate in inner join") {
    val ta = testRelation.select('a, Literal(1).as('tag))
      .union(testRelation.select('a.as('a), Literal(2).as('tag)))
      .subquery('ta)
    val tb = testRelation.select('a, Literal(1).as('tag))
      .union(testRelation.select('a.as('a), Literal(2).as('tag)))
      .subquery('tb)
    val query = ta.join(tb, Inner,
      Some("ta.a".attr === "tb.a".attr && "ta.tag".attr === "tb.tag".attr))
    val optimized = Optimize.execute(query.analyze)
    val correctAnswer = query.analyze
    comparePlans(optimized, correctAnswer)
  }

  test("Propagate in expand") {
    val c1 = Literal(1).as('a)
    val c2 = Literal(2).as('b)
    val a1 = c1.toAttribute.newInstance().withNullability(true)
    val a2 = c2.toAttribute.newInstance().withNullability(true)
    val expand = Expand(
      Seq(Seq(Literal(null), 'b), Seq('a, Literal(null))),
      Seq(a1, a2),
      OneRowRelation().select(c1, c2))
    val query = expand.where(a1.isNotNull).select(a1, a2).analyze
    val optimized = Optimize.execute(query)
    val correctExpand = expand.copy(projections = Seq(
      Seq(Literal(null), Literal(2)),
      Seq(Literal(1), Literal(null))))
    val correctAnswer = correctExpand.where(a1.isNotNull).select(a1, a2).analyze
    comparePlans(optimized, correctAnswer)
  }

  test("Propagate above outer join") {
    val left = LocalRelation('a.int).select('a, Literal(1).as('b))
    val right = LocalRelation('c.int).select('c, Literal(1).as('d))

    val join = left.join(
      right,
      joinType = LeftOuter,
      condition = Some('a === 'c && 'b === 'd))
    val query = join.select(('b + 3).as('res)).analyze
    val optimized = Optimize.execute(query)

    val correctAnswer = left.join(
      right,
      joinType = LeftOuter,
      condition = Some('a === 'c && Literal(1) === Literal(1)))
      .select((Literal(1) + 3).as('res)).analyze
    comparePlans(optimized, correctAnswer)
  }

  test("SPARK-32635: Replace references with foldables coming only from the node's children") {
    val leftExpression = 'a.int
    val left = LocalRelation(leftExpression).select('a)
    val rightExpression = Alias(Literal(2), "a")(leftExpression.exprId)
    val right = LocalRelation('b.int).select('b, rightExpression).select('b)
    val join = left.join(right, joinType = LeftOuter, condition = Some('b === 'a))

    val query = join.analyze
    val optimized = Optimize.execute(query)
    comparePlans(optimized, query)
  }

  test("SPARK-32951: Foldable propagation from Aggregate") {
    val query = testRelation
      .groupBy('a)('a, sum('b).as('b), Literal(1).as('c))
      .select('a, 'b, 'c)

    val optimized = Optimize.execute(query.analyze)
    val correctAnswer = testRelation
      .groupBy('a)('a, sum('b).as('b), Literal(1).as('c))
      .select('a, 'b, Literal(1).as('c)).analyze
    comparePlans(optimized, correctAnswer)
  }

  test("SPARK-37904: Improve rebalance in FoldablePropagation") {
    val foldableAttr = Literal(1).as("x")
    val plan = testRelation.select(foldableAttr, $"a").rebalance($"x", $"a").analyze
    val optimized = Optimize.execute(plan)
    val expected = testRelation.select(foldableAttr, $"a").rebalance(foldableAttr, $"a").analyze
    comparePlans(optimized, expected)
  }
}
