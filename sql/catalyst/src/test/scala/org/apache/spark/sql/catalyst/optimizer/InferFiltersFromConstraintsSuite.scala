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
import org.apache.spark.sql.internal.SQLConf.CONSTRAINT_PROPAGATION_ENABLED

class InferFiltersFromConstraintsSuite extends PlanTest {

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("InferAndPushDownFilters", FixedPoint(100),
        PushPredicateThroughJoin,
        PushDownPredicate,
        InferFiltersFromConstraints(conf),
        CombineFilters,
        BooleanSimplification) :: Nil
  }

  object OptimizeWithConstraintPropagationDisabled extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("InferAndPushDownFilters", FixedPoint(100),
        PushPredicateThroughJoin,
        PushDownPredicate,
        InferFiltersFromConstraints(conf.copy(CONSTRAINT_PROPAGATION_ENABLED -> false)),
        CombineFilters) :: Nil
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

  test("inner join with alias: alias contains multiple attributes") {
    val t1 = testRelation.subquery('t1)
    val t2 = testRelation.subquery('t2)

    val originalQuery = t1.select('a, Coalesce(Seq('a, 'b)).as('int_col)).as("t")
      .join(t2, Inner, Some("t.a".attr === "t2.a".attr && "t.int_col".attr === "t2.a".attr))
      .analyze
    val correctAnswer = t1
      .where(IsNotNull('a) && IsNotNull(Coalesce(Seq('a, 'b))) && 'a === Coalesce(Seq('a, 'b)))
      .select('a, Coalesce(Seq('a, 'b)).as('int_col)).as("t")
      .join(t2.where(IsNotNull('a)), Inner,
        Some("t.a".attr === "t2.a".attr && "t.int_col".attr === "t2.a".attr))
      .analyze
    val optimized = Optimize.execute(originalQuery)
    comparePlans(optimized, correctAnswer)
  }

  test("inner join with alias: alias contains single attributes") {
    val t1 = testRelation.subquery('t1)
    val t2 = testRelation.subquery('t2)

    val originalQuery = t1.select('a, 'b.as('d)).as("t")
      .join(t2, Inner, Some("t.a".attr === "t2.a".attr && "t.d".attr === "t2.a".attr))
      .analyze
    val correctAnswer = t1
      .where(IsNotNull('a) && IsNotNull('b) && 'a <=> 'a && 'b <=> 'b &&'a === 'b)
      .select('a, 'b.as('d)).as("t")
      .join(t2.where(IsNotNull('a) && 'a <=> 'a), Inner,
        Some("t.a".attr === "t2.a".attr && "t.d".attr === "t2.a".attr))
      .analyze
    val optimized = Optimize.execute(originalQuery)
    comparePlans(optimized, correctAnswer)
  }

  test("inner join with alias: don't generate constraints for recursive functions") {
    val t1 = testRelation.subquery('t1)
    val t2 = testRelation.subquery('t2)

    // We should prevent `Coalese(a, b)` from recursively creating complicated constraints through
    // the constraint inference procedure.
    val originalQuery = t1.select('a, 'b.as('d), Coalesce(Seq('a, 'b)).as('int_col))
      // We hide an `Alias` inside the child's child's expressions, to cover the situation reported
      // in [SPARK-20700].
      .select('int_col, 'd, 'a).as("t")
      .join(t2, Inner,
        Some("t.a".attr === "t2.a".attr
          && "t.d".attr === "t2.a".attr
          && "t.int_col".attr === "t2.a".attr))
      .analyze
    val correctAnswer = t1
      .where(IsNotNull('a) && IsNotNull(Coalesce(Seq('a, 'a)))
        && 'a === Coalesce(Seq('a, 'a)) && 'a <=> Coalesce(Seq('a, 'a))
        && Coalesce(Seq('b, 'b)) <=> 'a && 'a === 'b && IsNotNull(Coalesce(Seq('a, 'b)))
        && 'a === Coalesce(Seq('a, 'b)) && Coalesce(Seq('a, 'b)) === 'b
        && IsNotNull('b) && IsNotNull(Coalesce(Seq('b, 'b)))
        && 'b === Coalesce(Seq('b, 'b)) && 'b <=> Coalesce(Seq('b, 'b)))
      .select('a, 'b.as('d), Coalesce(Seq('a, 'b)).as('int_col))
      .select('int_col, 'd, 'a).as("t")
      .join(t2
        .where(IsNotNull('a) && IsNotNull(Coalesce(Seq('a, 'a)))
          && 'a <=> Coalesce(Seq('a, 'a)) && 'a === Coalesce(Seq('a, 'a)) && 'a <=> 'a), Inner,
        Some("t.a".attr === "t2.a".attr && "t.d".attr === "t2.a".attr
          && "t.int_col".attr === "t2.a".attr))
      .analyze
    val optimized = Optimize.execute(originalQuery)
    comparePlans(optimized, correctAnswer)
  }

  test("generate correct filters for alias that don't produce recursive constraints") {
    val t1 = testRelation.subquery('t1)

    val originalQuery = t1.select('a.as('x), 'b.as('y)).where('x === 1 && 'x === 'y).analyze
    val correctAnswer =
      t1.where('a === 1 && 'b === 1 && 'a === 'b && IsNotNull('a) && IsNotNull('b))
        .select('a.as('x), 'b.as('y)).analyze
    val optimized = Optimize.execute(originalQuery)
    comparePlans(optimized, correctAnswer)
  }

  test("No inferred filter when constraint propagation is disabled") {
    val originalQuery = testRelation.where('a === 1 && 'a === 'b).analyze
    val optimized = OptimizeWithConstraintPropagationDisabled.execute(originalQuery)
    comparePlans(optimized, originalQuery)
  }
}
