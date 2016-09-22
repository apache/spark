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
    val query = OneRowRelation
      .select(Literal(1).as('a), Literal(2).as('b))
      .subquery('T)
      .select('a, 'b)
    val optimized = Optimize.execute(query.analyze)
    val correctAnswer = OneRowRelation
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
        testRelation.select(Literal(1).as('x), 'a).select('x + 'a),
        testRelation.select(Literal(2).as('x), 'a).select('x + 'a)))
      .select('x)
    val optimized = Optimize.execute(query.analyze)
    val correctAnswer = Union(
      Seq(
        testRelation.select(Literal(1).as('x), 'a).select((Literal(1).as('x) + 'a).as("(x + a)")),
        testRelation.select(Literal(2).as('x), 'a).select((Literal(2).as('x) + 'a).as("(x + a)"))))
      .select('x).analyze

    comparePlans(optimized, correctAnswer)
  }
}
