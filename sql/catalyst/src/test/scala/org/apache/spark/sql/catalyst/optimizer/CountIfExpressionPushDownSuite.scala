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
import org.apache.spark.sql.catalyst.plans.{Inner, PlanTest}
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan, Union}
import org.apache.spark.sql.catalyst.rules.RuleExecutor

class CountIfExpressionPushDownSuite extends PlanTest {

  object Optimizer extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("Push down Count_if filter", FixedPoint(1),
        CountIfExpressionPushDown) :: Nil
  }

  private val testRelation = LocalRelation($"a".int, $"b".int)
  private val testRelation2 = LocalRelation($"x".int, $"y".int)
  private val x = testRelation.subquery("x")
  private val y = testRelation2.subquery("y")

  test("SPARK-53742: Push down Count_if filter") {
    val a = testRelation.output.head
    val expr = With(And(GreaterThan(a, Literal(1)),
      LessThanOrEqual(a, Literal(3)))) { case Seq(ref) =>
      If(ref, a, Literal.create(null, a.dataType))
    }
    val plan = testRelation
      .groupBy()(
        count(expr).as("col1")
      )
    val optimized = Optimizer.execute(plan.analyze)
    val oldPlan = testRelation.where(And($"a"> 1, $"a"<= 3))
      .groupBy()(count(expr).as("col1")).analyze
    comparePlans(oldPlan, optimized)
  }


  test("SPARK-53742: Push down Count_if filter with group by") {
    val a = testRelation.output.head
    val b = testRelation.output.last
    val expr = With(And(GreaterThan(a, Literal(1)),
      LessThanOrEqual(a, Literal(3)))) { case Seq(ref) =>
      If(ref, a, Literal.create(null, a.dataType))
    }
    val plan = testRelation
      .groupBy(b)(
        count(expr).as("col1")
      )
    val optimized = Optimizer.execute(plan.analyze)
    val oldPlan = testRelation.where(And($"a"> 1, $"a"<= 3))
      .groupBy(b)(count(expr).as("col1")).analyze
    comparePlans(oldPlan, optimized)
  }

  test("SPARK-53742: Push down multiple Count_if with same filter") {
    val a = testRelation.output.head
    val b = testRelation.output.last
    val expr = With(And(GreaterThan(a, Literal(1)),
      LessThanOrEqual(a, Literal(3)))) { case Seq(ref) =>
      If(ref, a, Literal.create(null, a.dataType))
    }
    val plan = testRelation
      .groupBy(b)(
        count(expr).as("col1"),
        count(expr).as("col2"),
      )
    val optimized = Optimizer.execute(plan.analyze)
    val oldPlan = testRelation.where(And($"a"> 1, $"a"<= 3))
      .groupBy(b)(count(expr).as("col1")
        , count(expr).as("col2")).analyze
    comparePlans(oldPlan, optimized)
  }


  test("SPARK-53742: Push down multiple Count_if with different filter ") {
    val a = testRelation.output.head
    val b = testRelation.output.last
    val expr = With(And(GreaterThan(a, Literal(1)),
      LessThanOrEqual(a, Literal(3)))) { case Seq(ref) =>
      If(ref, a, Literal.create(null, a.dataType))
    }
    val expr1 = With(Or(GreaterThan(b, Literal(1)),
      LessThanOrEqual(b, Literal(3)))) { case Seq(ref) =>
      If(ref, a, Literal.create(null, a.dataType))
    }
    val plan = testRelation
      .groupBy()(
        count(expr).as("col1"),
        count(expr1).as("col2"),
      )
    val optimized = Optimizer.execute(plan.analyze)
    val oldPlan = testRelation
      .where(Or(And($"a" > 1, $"a" <= 3), Or($"b" > 1, $"b" <= 3)))
      .groupBy()(count(expr).as("col1")
        , count(expr1).as("col2")).analyze
    comparePlans(oldPlan, optimized)
  }


  test("SPARK-53742: Push down multiple Count_if with other agg function") {
    val a = testRelation.output.head
    val b = testRelation.output.last
    val expr = With(And(GreaterThan(a, Literal(1)),
      LessThanOrEqual(a, Literal(3)))) { case Seq(ref) =>
      If(ref, a, Literal.create(null, a.dataType))
    }
    val expr1 = With(Or(GreaterThan(b, Literal(1)),
      LessThanOrEqual(b, Literal(3)))) { case Seq(ref) =>
      If(ref, a, Literal.create(null, a.dataType))
    }
    val plan = testRelation
      .groupBy()(
        count(expr).as("col1"),
        sum(expr1).as("col2"),
      )
    val optimized = Optimizer.execute(plan.analyze)
    val oldPlan = testRelation
      .groupBy()(count(expr).as("col1")
        , sum(expr1).as("col2")).analyze
    comparePlans(oldPlan, optimized)
  }


  test("SPARK-53742: Push down multiple Count_if with where condition") {
    val a = testRelation.output.head
    val b = testRelation.output.last
    val expr = With(GreaterThan(a, Literal(1))) { case Seq(ref) =>
      If(ref, a, Literal.create(null, a.dataType))
    }
    val expr1 = With(LessThanOrEqual(b, Literal(3))) { case Seq(ref) =>
      If(ref, a, Literal.create(null, a.dataType))
    }
    val plan = testRelation
      .where($"a" >  0)
      .groupBy()(
        count(expr).as("col1"),
        count(expr1).as("col2"),
      )
    val optimized = Optimizer.execute(plan.analyze)
    val oldPlan = testRelation
      .where(And($"a" > 0, Or($"a" > 1, $"b" <= 3)))
      .groupBy()(count(expr).as("col1")
        , count(expr1).as("col2")).analyze
    comparePlans(oldPlan, optimized)
  }

  test("SPARK-53742: Push down multiple Count_if subquery") {
    val a = x.output.head
    val b = x.output.last
    val expr = With(GreaterThan(a, Literal(1))) { case Seq(ref) =>
      If(ref, a, Literal.create(null, a.dataType))
    }
    val expr1 = With(LessThanOrEqual(b, Literal(3))) { case Seq(ref) =>
      If(ref, a, Literal.create(null, a.dataType))
    }
    val plan = x.select($"a", $"b")
      .where($"a" >  0)
      .groupBy()(
        count(expr).as("col1"),
        count(expr1).as("col2"),
      )
    val optimized = Optimizer.execute(plan.analyze)
    val oldPlan = x.select($"a", $"b")
      .where(And($"a" > 0, Or($"a" > 1, $"b" <= 3)))
      .groupBy()(count(expr).as("col1")
        , count(expr1).as("col2")).analyze
    comparePlans(oldPlan, optimized)
  }


  test("SPARK-53742: Push down multiple Count_if with union all") {

    val unionQuery = Union(testRelation, testRelation)
    val a = unionQuery.output.head
    val b = unionQuery.output.last
    val expr = With(GreaterThan(a, Literal(1))) { case Seq(ref) =>
      If(ref, a, Literal.create(null, a.dataType))
    }
    val expr1 = With(LessThanOrEqual(b, Literal(3))) { case Seq(ref) =>
      If(ref, a, Literal.create(null, a.dataType))
    }
    val plan = unionQuery.select($"a", $"b")
      .where($"a" >  0)
      .groupBy()(
        count(expr).as("col1"),
        count(expr1).as("col2"),
      )
    val optimized = Optimizer.execute(plan.analyze)
    val oldPlan = unionQuery.select($"a", $"b")
      .where(And($"a" > 0, Or($"a" > 1, $"b" <= 3)))
      .groupBy()(count(expr).as("col1")
        , count(expr1).as("col2")).analyze
    comparePlans(oldPlan, optimized)
  }

  test("SPARK-53742: Push down multiple Count_if with join") {

    val originalQuery = x.join(y, Inner, Some("x.a".attr === "y.y".attr))
    val a = originalQuery.output.head
    val yy = originalQuery.output.last
    val expr = With(GreaterThan(a, Literal(1))) { case Seq(ref) =>
      If(ref, a, Literal.create(null, a.dataType))
    }
    val expr1 = With(LessThanOrEqual(yy, Literal(3))) { case Seq(ref) =>
      If(ref, a, Literal.create(null, a.dataType))
    }
    val plan = originalQuery.select($"x.a", $"y.y")
      .where($"x.a" >  0)
      .groupBy()(
        count(expr).as("col1"),
        count(expr1).as("col2"),
      )
    val optimized = Optimizer.execute(plan.analyze)
    val oldPlan = originalQuery.select($"x.a", $"y.y")
      .where(And($"x.a" > 0, Or($"x.a" > 1, $"y.y" <= 3)))
      .groupBy()(count(expr).as("col1")
        , count(expr1).as("col2")).analyze
    comparePlans(oldPlan, optimized)
  }



}
