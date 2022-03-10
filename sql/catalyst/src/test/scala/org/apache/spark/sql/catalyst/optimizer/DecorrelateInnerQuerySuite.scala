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
import org.apache.spark.sql.types.IntegerType

class DecorrelateInnerQuerySuite extends PlanTest {

  val a = AttributeReference("a", IntegerType)()
  val b = AttributeReference("b", IntegerType)()
  val c = AttributeReference("c", IntegerType)()
  val x = AttributeReference("x", IntegerType)()
  val y = AttributeReference("y", IntegerType)()
  val z = AttributeReference("z", IntegerType)()
  val t0 = OneRowRelation()
  val testRelation = LocalRelation(a, b, c)
  val testRelation2 = LocalRelation(x, y, z)

  private def hasOuterReferences(plan: LogicalPlan): Boolean = {
    plan.find(_.expressions.exists(SubExprUtils.containsOuter)).isDefined
  }

  private def check(
      innerPlan: LogicalPlan,
      outerPlan: LogicalPlan,
      correctAnswer: LogicalPlan,
      conditions: Seq[Expression]): Unit = {
    val (outputPlan, joinCond) = DecorrelateInnerQuery(innerPlan, outerPlan.select())
    assert(!hasOuterReferences(outputPlan))
    comparePlans(outputPlan, correctAnswer)
    assert(joinCond.length == conditions.length)
    joinCond.zip(conditions).foreach(e => compareExpressions(e._1, e._2))
  }

  test("filter with correlated equality predicates only") {
    val outerPlan = testRelation2
    val innerPlan =
      Project(Seq(a, b),
        Filter(OuterReference(x) === a,
          testRelation))
    val correctAnswer = Project(Seq(a, b), testRelation)
    check(innerPlan, outerPlan, correctAnswer, Seq(x === a))
  }

  test("filter with local and correlated equality predicates") {
    val outerPlan = testRelation2
    val innerPlan =
      Project(Seq(a, b),
        Filter(And(OuterReference(x) === a, b === 3),
          testRelation))
    val correctAnswer =
      Project(Seq(a, b),
        Filter(b === 3,
          testRelation))
    check(innerPlan, outerPlan, correctAnswer, Seq(x === a))
  }

  test("filter with correlated non-equality predicates") {
    val outerPlan = testRelation2
    val innerPlan =
      Project(Seq(a, b),
        Filter(OuterReference(x) > a,
          testRelation))
    val correctAnswer = Project(Seq(a, b), testRelation)
    check(innerPlan, outerPlan, correctAnswer, Seq(x > a))
  }

  test("duplicated output attributes") {
    val outerPlan = testRelation
    val innerPlan =
      Project(Seq(a),
        Filter(OuterReference(a) === a,
          testRelation))
    val (outputPlan, joinCond) = DecorrelateInnerQuery(innerPlan, outerPlan.select())
    val a1 = outputPlan.output.head
    val correctAnswer =
      Project(Seq(Alias(a, a1.name)(a1.exprId)),
        Project(Seq(a),
          testRelation))
    comparePlans(outputPlan, correctAnswer)
    assert(joinCond == Seq(a === a1))
  }

  test("filter with equality predicates with correlated values on both sides") {
    val outerPlan = testRelation2
    val innerPlan =
      Project(Seq(a),
        Filter(OuterReference(x) === OuterReference(y) + b,
          testRelation))
    val correctAnswer = Project(Seq(a, b), testRelation)
    check(innerPlan, outerPlan, correctAnswer, Seq(x === y + b))
  }

  test("aggregate with correlated equality predicates that can be pulled up") {
    val outerPlan = testRelation2
    val minB = Alias(min(b), "min_b")()
    val innerPlan =
      Aggregate(Nil, Seq(minB),
        Filter(And(OuterReference(x) === a, b === 3),
          testRelation))
    val correctAnswer =
      Aggregate(Seq(a), Seq(minB, a),
        Filter(b === 3,
          testRelation))
    check(innerPlan, outerPlan, correctAnswer, Seq(x === a))
  }

  test("aggregate with correlated equality predicates that cannot be pulled up") {
    val outerPlan = testRelation2
    val minB = Alias(min(b), "min_b")()
    val innerPlan =
      Aggregate(Nil, Seq(minB),
        Filter(OuterReference(x) === OuterReference(y) + a,
          testRelation))
    val correctAnswer =
      Aggregate(Seq(x, y), Seq(minB, x, y),
        Filter(x === y + a,
          DomainJoin(Seq(x, y), testRelation)))
    check(innerPlan, outerPlan, correctAnswer, Seq(x <=> x, y <=> y))
  }

  test("aggregate with correlated equality predicates that has no attribute") {
    val outerPlan = testRelation2
    val minB = Alias(min(b), "min_b")()
    val innerPlan =
      Aggregate(Nil, Seq(minB),
        Filter(OuterReference(x) === OuterReference(y),
          testRelation))
    val correctAnswer =
      Aggregate(Nil, Seq(minB),
        testRelation)
    check(innerPlan, outerPlan, correctAnswer, Seq(x === y))
  }

  test("aggregate with correlated non-equality predicates") {
    val outerPlan = testRelation2
    val minB = Alias(min(b), "min_b")()
    val innerPlan =
      Aggregate(Nil, Seq(minB),
        Filter(OuterReference(x) > a,
          testRelation))
    val correctAnswer =
      Aggregate(Seq(x), Seq(minB, x),
        Filter(x > a,
          DomainJoin(Seq(x), testRelation)))
    check(innerPlan, outerPlan, correctAnswer, Seq(x <=> x))
  }

  test("join with correlated equality predicates") {
    val outerPlan = testRelation2
    val joinCondition = Some($"t1.b" === $"t2.b")
    val left =
      Project(Seq(b),
        Filter(OuterReference(x) === b,
          testRelation)).as("t1")
    val right =
      Project(Seq(b),
        Filter(OuterReference(x) === a,
          testRelation)).as("t2")
    Seq(Inner, LeftOuter, LeftSemi, LeftAnti, RightOuter, FullOuter, Cross).foreach { joinType =>
      val innerPlan = Join(left, right, joinType, joinCondition, JoinHint.NONE).analyze
      val newLeft = Project(Seq(b), testRelation).as("t1")
      val newRight = Project(Seq(b, a), testRelation).as("t2")
      // Since the left-hand side has outer(x) = b, and the right-hand side has outer(x) = a, the
      // join condition will be augmented with b <=> a.
      val newCond = Some(And($"t1.b" <=> $"t2.a", $"t1.b" === $"t2.b"))
      val correctAnswer = Join(newLeft, newRight, joinType, newCond, JoinHint.NONE).analyze
      check(innerPlan, outerPlan, correctAnswer, Seq(x === b, x === a))
    }
  }

  test("correlated values inside join condition") {
    val outerPlan = testRelation2
    val innerPlan =
      Join(
        testRelation.as("t1"),
        Filter(OuterReference(y) === 3, testRelation),
        Inner,
        Some(OuterReference(x) === a),
        JoinHint.NONE)
    val error = intercept[AssertionError] { DecorrelateInnerQuery(innerPlan, outerPlan.select()) }
    assert(error.getMessage.contains("Correlated column is not allowed in join"))
  }

  test("correlated values in project") {
    val outerPlan = testRelation2
    val innerPlan = Project(Seq(OuterReference(x).as("x1"), OuterReference(y).as("y1")), t0)
    val correctAnswer = Project(
      Seq(x.as("x1"), y.as("y1"), x, y), DomainJoin(Seq(x, y), t0))
    check(innerPlan, outerPlan, correctAnswer, Seq(x <=> x, y <=> y))
  }

  test("correlated values in project with alias") {
    val outerPlan = testRelation2
    val innerPlan =
      Project(Seq(OuterReference(x).as("x1"), 'y1, 'sum),
        Project(Seq(
          OuterReference(x),
          OuterReference(y).as("y1"),
          Add(OuterReference(x), OuterReference(y)).as("sum")),
            testRelation)).analyze
    val correctAnswer =
      Project(Seq(x.as("x1"), 'y1, 'sum, x, y),
        Project(Seq(x.as(x.name), y.as("y1"), (x + y).as("sum"), x, y),
          DomainJoin(Seq(x, y), testRelation))).analyze
    check(innerPlan, outerPlan, correctAnswer, Seq(x <=> x, y <=> y))
  }

  test("correlated values in project with correlated equality conditions in filter") {
    val outerPlan = testRelation2
    val innerPlan =
      Project(
        Seq(OuterReference(x).as("x1")),
        Filter(
          And(OuterReference(x) === a, And(OuterReference(x) + OuterReference(y) === c, b === 1)),
          testRelation
        )
      )
    val correctAnswer = Project(Seq(a.as("x1"), a, c), Filter(b === 1, testRelation))
    check(innerPlan, outerPlan, correctAnswer, Seq(x === a, x + y === c))
  }

  test("correlated values in project without correlated equality conditions in filter") {
    val outerPlan = testRelation2
    val innerPlan =
      Project(
        Seq(OuterReference(y).as("y1")),
        Filter(
          And(OuterReference(x) === a, And(OuterReference(x) + OuterReference(y) === c, b === 1)),
          testRelation
        )
      )
    val correctAnswer =
      Project(Seq(y.as("y1"), y, a, c),
        Filter(b === 1,
          DomainJoin(Seq(y), testRelation)
        )
      )
    check(innerPlan, outerPlan, correctAnswer, Seq(y <=> y, x === a, x + y === c))
  }

  test("correlated values in project with aggregate") {
    val outerPlan = testRelation2
    val innerPlan =
      Aggregate(
        Seq('x1), Seq(min('y1).as("min_y1")),
        Project(
          Seq(a, OuterReference(x).as("x1"), OuterReference(y).as("y1")),
          Filter(
            And(OuterReference(x) === a, OuterReference(y) === OuterReference(z)),
            testRelation
          )
        )
      ).analyze
    val correctAnswer =
      Aggregate(
        Seq('x1, y, a), Seq(min('y1).as("min_y1"), y, a),
        Project(
          Seq(a, a.as("x1"), y.as("y1"), y),
          DomainJoin(Seq(y), testRelation)
        )
      ).analyze
    check(innerPlan, outerPlan, correctAnswer, Seq(y <=> y, x === a, y === z))
  }

  test("SPARK-38155: distinct with non-equality correlated predicates") {
    val outerPlan = testRelation2
    val innerPlan =
      Distinct(
        Project(Seq(b),
          Filter(OuterReference(x) > a, testRelation)))
    val correctAnswer =
      Distinct(
        Project(Seq(b, x),
          Filter(x > a,
            DomainJoin(Seq(x), testRelation))))
    check(innerPlan, outerPlan, correctAnswer, Seq(x <=> x))
  }
}
