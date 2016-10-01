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

class InferFiltersFromConstraintsSuite extends PlanTest {

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches = Batch("InferFilters", FixedPoint(100), InferFiltersFromConstraints) ::
      Batch("PredicatePushdown", FixedPoint(100),
        PushPredicateThroughJoin,
        PushDownPredicate) ::
      Batch("CombineFilters", FixedPoint(100), CombineFilters) :: Nil
  }

  val testRelation = LocalRelation('a.int, 'b.int, 'c.int)

  test("filter: filter out constraints in condition") {
    val originalQuery = testRelation.where('a === 1 && 'a === 'b).analyze
    val correctAnswer = testRelation
      .where(IsNotNull('a) && IsNotNull('b) && 'a === 'b && 'a === 1 && 'b === 1).analyze
    val optimized = Optimize.execute(originalQuery)
    comparePlans(optimized, correctAnswer)
  }

  test("single inner join: filter out values on either side on equi-join keys") {
    val x = testRelation.subquery('x)
    val y = testRelation.subquery('y)
    val originalQuery = x.join(y,
      condition = Some(("x.a".attr === "y.a".attr) && ("x.a".attr === 1) && ("y.c".attr > 5)))
      .analyze
    val left = x.where(IsNotNull('a) && "x.a".attr === 1)
    val right = y.where(IsNotNull('a) && IsNotNull('c) && "y.c".attr > 5 && "y.a".attr === 1)
    val correctAnswer = left.join(right, condition = Some("x.a".attr === "y.a".attr)).analyze
    val optimized = Optimize.execute(originalQuery)
    comparePlans(optimized, correctAnswer)
  }

  test("single inner join: filter out nulls on either side on non equal keys") {
    val x = testRelation.subquery('x)
    val y = testRelation.subquery('y)
    val originalQuery = x.join(y,
      condition = Some(("x.a".attr =!= "y.a".attr) && ("x.b".attr === 1) && ("y.c".attr > 5)))
      .analyze
    val left = x.where(IsNotNull('a) && IsNotNull('b) && "x.b".attr === 1)
    val right = y.where(IsNotNull('a) && IsNotNull('c) && "y.c".attr > 5)
    val correctAnswer = left.join(right, condition = Some("x.a".attr =!= "y.a".attr)).analyze
    val optimized = Optimize.execute(originalQuery)
    comparePlans(optimized, correctAnswer)
  }

  test("single inner join with pre-existing filters: filter out values on either side") {
    val x = testRelation.subquery('x)
    val y = testRelation.subquery('y)
    val originalQuery = x.where('b > 5).join(y.where('a === 10),
      condition = Some("x.a".attr === "y.a".attr && "x.b".attr === "y.b".attr)).analyze
    val left = x.where(IsNotNull('a) && 'a === 10 && IsNotNull('b) && 'b > 5)
    val right = y.where(IsNotNull('a) && IsNotNull('b) && 'a === 10 && 'b > 5)
    val correctAnswer = left.join(right,
      condition = Some("x.a".attr === "y.a".attr && "x.b".attr === "y.b".attr)).analyze
    val optimized = Optimize.execute(originalQuery)
    comparePlans(optimized, correctAnswer)
  }

  test("single outer join: no null filters are generated") {
    val x = testRelation.subquery('x)
    val y = testRelation.subquery('y)
    val originalQuery = x.join(y, FullOuter,
      condition = Some("x.a".attr === "y.a".attr)).analyze
    val optimized = Optimize.execute(originalQuery)
    comparePlans(optimized, originalQuery)
  }

  test("multiple inner joins: filter out values on all sides on equi-join keys") {
    val t1 = testRelation.subquery('t1)
    val t2 = testRelation.subquery('t2)
    val t3 = testRelation.subquery('t3)
    val t4 = testRelation.subquery('t4)

    val originalQuery = t1.where('b > 5)
      .join(t2, condition = Some("t1.b".attr === "t2.b".attr))
      .join(t3, condition = Some("t2.b".attr === "t3.b".attr))
      .join(t4, condition = Some("t3.b".attr === "t4.b".attr)).analyze
    val correctAnswer = t1.where(IsNotNull('b) && 'b > 5)
      .join(t2.where(IsNotNull('b) && 'b > 5), condition = Some("t1.b".attr === "t2.b".attr))
      .join(t3.where(IsNotNull('b) && 'b > 5), condition = Some("t2.b".attr === "t3.b".attr))
      .join(t4.where(IsNotNull('b) && 'b > 5), condition = Some("t3.b".attr === "t4.b".attr))
      .analyze
    val optimized = Optimize.execute(originalQuery)
    comparePlans(optimized, correctAnswer)
  }

  test("inner join with filter: filter out values on all sides on equi-join keys") {
    val x = testRelation.subquery('x)
    val y = testRelation.subquery('y)

    val originalQuery =
      x.join(y, Inner, Some("x.a".attr === "y.a".attr)).where("x.a".attr > 5).analyze
    val correctAnswer = x.where(IsNotNull('a) && 'a.attr > 5)
      .join(y.where(IsNotNull('a) && 'a.attr > 5), Inner, Some("x.a".attr === "y.a".attr)).analyze
    val optimized = Optimize.execute(originalQuery)
    comparePlans(optimized, correctAnswer)
  }

  test("don't generate constraints for recursive functions") {
    val t1 = testRelation.subquery('t1)
    val t2 = testRelation.subquery('t2)

    val originalQuery = t1.select('a, 'b.as('d), Coalesce(Seq('a, 'b)).as('int_col)).as("t")
      .join(t2, Inner,
        Some("t.a".attr === "t2.a".attr
          && "t.d".attr === "t2.a".attr
          && "t.int_col".attr === "t2.a".attr))
      .analyze
    val correctAnswer = t1.where(IsNotNull('a) && 'a === Coalesce(Seq('a, 'b))
      && IsNotNull('b) && 'b === Coalesce(Seq('a, 'b))
      && IsNotNull(Coalesce(Seq('a, 'b))) && 'a === 'b)
      .select('a, 'b.as('d), Coalesce(Seq('a, 'b)).as('int_col)).as("t")
      .join(t2.where(IsNotNull('a)), Inner,
        Some("t.a".attr === "t2.a".attr
          && "t.d".attr === "t2.a".attr
          && "t.int_col".attr === "t2.a".attr))
      .analyze
    val optimized = Optimize.execute(originalQuery)
    comparePlans(optimized, correctAnswer)
  }
}
