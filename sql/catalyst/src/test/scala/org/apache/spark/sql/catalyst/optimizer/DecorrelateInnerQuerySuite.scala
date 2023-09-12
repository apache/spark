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
  val a3 = AttributeReference("a3", IntegerType)()
  val b3 = AttributeReference("b3", IntegerType)()
  val c3 = AttributeReference("c3", IntegerType)()
  val a4 = AttributeReference("a4", IntegerType)()
  val b4 = AttributeReference("b4", IntegerType)()
  val t0 = OneRowRelation()
  val testRelation = LocalRelation(a, b, c)
  val testRelation2 = LocalRelation(x, y, z)
  val testRelation3 = LocalRelation(a3, b3, c3)
  val testRelation4 = LocalRelation(a4, b4)

  private def hasOuterReferences(plan: LogicalPlan): Boolean = {
    plan.exists(_.expressions.exists(SubExprUtils.containsOuter))
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

  private def check(
      outputPlan: LogicalPlan,
      joinCond: Seq[Expression],
      correctAnswer: LogicalPlan,
      conditions: Seq[Expression]): Unit = {
    assert(!hasOuterReferences(outputPlan))
    comparePlans(outputPlan, correctAnswer)
    assert(joinCond.length == conditions.length)
    joinCond.zip(conditions).foreach(e => compareExpressions(e._1, e._2))
  }

  // For tests involving window functions: extract and return the ROW_NUMBER function
  // from the 'input' plan.
  private def getRowNumberFunc(input: LogicalPlan): Alias = {
    val windowFunction = input.collect({ case w: Window => w }).head
    windowFunction.expressions.collect(
      { case w: Alias if w.child.isInstanceOf[WindowExpression] => w }).head
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
        Filter(OuterReference(y) === b3, testRelation3),
        Inner,
        Some(OuterReference(x) === a),
        JoinHint.NONE)
    val correctAnswer =
      Join(
        testRelation.as("t1"), testRelation3,
        Inner, Some(a === a), JoinHint.NONE)
    check(innerPlan, outerPlan, correctAnswer, Seq(b3 === y, x === a))
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
      Project(Seq(OuterReference(x).as("x1"), $"y1", $"sum"),
        Project(Seq(
          OuterReference(x),
          OuterReference(y).as("y1"),
          Add(OuterReference(x), OuterReference(y)).as("sum")),
            testRelation)).analyze
    val correctAnswer =
      Project(Seq(x.as("x1"), $"y1", $"sum", x, y),
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
        Seq($"x1"), Seq(min($"y1").as("min_y1")),
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
        Seq($"x1", y, a), Seq(min($"y1").as("min_y1"), y, a),
        Project(
          Seq(a, a.as("x1"), y.as("y1"), y),
          DomainJoin(Seq(y), testRelation)
        )
      ).analyze
    check(innerPlan, outerPlan, correctAnswer, Seq(y <=> y, x === a, y === z))
  }

  test("SPARK-36124: union in correlation path") {
    val outerPlan = testRelation2
    val innerPlan =
      Union(
        Filter(And(OuterReference(x) === a, c === 3),
          testRelation),
        Filter(And(OuterReference(y) === b, c === 6),
          testRelation))
    val correctAnswer =
      Union(
        Project(Seq(a, b, c, x, y),
          Filter(And(x === a, c === 3),
            DomainJoin(Seq(x, y),
              testRelation))),
        Project(Seq(a, b, c, x, y),
          Filter(And(y === b, c === 6),
            DomainJoin(Seq(x, y),
              testRelation)))
      )
    check(innerPlan, outerPlan, correctAnswer, Seq(x <=> x, y <=> y))
  }

  test("SPARK-36124: another union in correlation path") {
    val outerPlan = testRelation2
    val innerPlan =
      Union(Seq(
        Filter(And(OuterReference(x) === a, a > 2),
          testRelation),
        Filter(And(OuterReference(y) === b, b > 3),
          testRelation),
        Filter(And(OuterReference(z) === c, c > 4),
          testRelation)))
    val correctAnswer =
      Union(Seq(
        Project(Seq(a, b, c, x, y, z),
          Filter(And(x === a, a > 2),
            DomainJoin(Seq(x, y, z),
              testRelation))),
        Project(Seq(a, b, c, x, y, z),
          Filter(And(y === b, b > 3),
            DomainJoin(Seq(x, y, z),
              testRelation))),
        Project(Seq(a, b, c, x, y, z),
          Filter(And(z === c, c > 4),
            DomainJoin(Seq(x, y, z),
              testRelation)))
      ))
    check(innerPlan, outerPlan, correctAnswer, Seq(x <=> x, y <=> y, z <=> z))
  }

  test("SPARK-36124: INTERSECT ALL in correlation path") {
    val outerPlan = testRelation2
    val innerPlan =
      Intersect(
        Filter(And(OuterReference(x) === a, c === 3),
          testRelation),
        Filter(And(OuterReference(y) === b3, c3 === 6),
          testRelation3),
        isAll = true)
    val x2 = x.newInstance()
    val y2 = y.newInstance()
    val correctAnswer =
      Intersect(
        Project(Seq(a, b, c, x, y),
          Filter(And(x === a, c === 3),
            DomainJoin(Seq(x, y),
              testRelation))),
        Project(Seq(a3, b3, c3, x2, y2),
          Filter(And(y2 === b3, c3 === 6),
            DomainJoin(Seq(x2, y2),
              testRelation3))),
        isAll = true
      )
    check(innerPlan, outerPlan, correctAnswer, Seq(x <=> x, y <=> y))
  }

    test("SPARK-36124: INTERSECT DISTINCT in correlation path") {
    val outerPlan = testRelation2
    val innerPlan =
      Intersect(
        Filter(And(OuterReference(x) === a, c === 3),
          testRelation),
        Filter(And(OuterReference(y) === b3, c3 === 6),
          testRelation3),
        isAll = false)
    val x2 = x.newInstance()
    val y2 = y.newInstance()
    val correctAnswer =
      Intersect(
        Project(Seq(a, b, c, x, y),
          Filter(And(x === a, c === 3),
            DomainJoin(Seq(x, y),
              testRelation))),
        Project(Seq(a3, b3, c3, x2, y2),
          Filter(And(y2 === b3, c3 === 6),
            DomainJoin(Seq(x2, y2),
              testRelation3))),
        isAll = false
      )
    check(innerPlan, outerPlan, correctAnswer, Seq(x <=> x, y <=> y))
  }

  test("SPARK-36124: EXCEPT ALL in correlation path") {
    val outerPlan = testRelation2
    val innerPlan =
      Except(
        Filter(And(OuterReference(x) === a, c === 3),
          testRelation),
        Filter(And(OuterReference(y) === b3, c3 === 6),
          testRelation3),
        isAll = true)
    val x2 = x.newInstance()
    val y2 = y.newInstance()
    val correctAnswer =
      Except(
        Project(Seq(a, b, c, x, y),
          Filter(And(x === a, c === 3),
            DomainJoin(Seq(x, y),
              testRelation))),
        Project(Seq(a3, b3, c3, x2, y2),
          Filter(And(y2 === b3, c3 === 6),
            DomainJoin(Seq(x2, y2),
              testRelation3))),
        isAll = true
      )
    check(innerPlan, outerPlan, correctAnswer, Seq(x <=> x, y <=> y))
  }

  test("SPARK-36124: EXCEPT DISTINCT in correlation path") {
    val outerPlan = testRelation2
    val innerPlan =
      Except(
        Filter(And(OuterReference(x) === a, c === 3),
          testRelation),
        Filter(And(OuterReference(y) === b3, c3 === 6),
          testRelation3),
        isAll = false)
    val x2 = x.newInstance()
    val y2 = y.newInstance()
    val correctAnswer =
      Except(
        Project(Seq(a, b, c, x, y),
          Filter(And(x === a, c === 3),
            DomainJoin(Seq(x, y),
              testRelation))),
        Project(Seq(a3, b3, c3, x2, y2),
          Filter(And(y2 === b3, c3 === 6),
            DomainJoin(Seq(x2, y2),
              testRelation3))),
        isAll = false
      )
    check(innerPlan, outerPlan, correctAnswer, Seq(x <=> x, y <=> y))
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

  test("SPARK-43780: aggregation in subquery with correlated equi-join") {
    // Join in the subquery is on equi-predicates, so all the correlated references can be
    // substituted by equivalent ones from the outer query, and domain join is not needed.
    val outerPlan = testRelation
    val innerPlan =
      Aggregate(
        Seq.empty[Expression], Seq(Alias(count(Literal(1)), "a")()),
        Project(Seq(x, y, a3, b3),
          Join(testRelation2, testRelation3, Inner,
            Some(And(x === a3, y === OuterReference(a))), JoinHint.NONE)))

    val correctAnswer =
      Aggregate(
        Seq(y), Seq(Alias(count(Literal(1)), "a")(), y),
        Project(Seq(x, y, a3, b3),
          Join(testRelation2, testRelation3, Inner, Some(And(y === y, x === a3)), JoinHint.NONE)))
    check(innerPlan, outerPlan, correctAnswer, Seq(y === a))
  }

  test("SPARK-43780: aggregation in subquery with correlated non-equi-join") {
    // Join in the subquery is on non-equi-predicate, so we introduce a DomainJoin.
    val outerPlan = testRelation
    val innerPlan =
      Aggregate(
        Seq.empty[Expression], Seq(Alias(count(Literal(1)), "a")()),
        Project(Seq(x, y, a3, b3),
          Join(testRelation2, testRelation3, Inner,
            Some(And(x === a3, y > OuterReference(a))), JoinHint.NONE)))
    val correctAnswer =
      Aggregate(
        Seq(a), Seq(Alias(count(Literal(1)), "a")(), a),
        Project(Seq(x, y, a3, b3, a),
          Join(
            DomainJoin(Seq(a), testRelation2),
            testRelation3, Inner, Some(And(x === a3, y > a)), JoinHint.NONE)))
    check(innerPlan, outerPlan, correctAnswer, Seq(a <=> a))
  }

  test("SPARK-43780: aggregation in subquery with correlated left join") {
    // Join in the subquery is on equi-predicates, so all the correlated references can be
    // substituted by equivalent ones from the outer query, and domain join is not needed.
    val outerPlan = testRelation
    val innerPlan =
      Aggregate(
        Seq.empty[Expression], Seq(Alias(count(Literal(1)), "a")()),
        Project(Seq(x, y, a3, b3),
          Join(testRelation2, testRelation3, LeftOuter,
            Some(And(x === a3, y === OuterReference(a))), JoinHint.NONE)))

    val correctAnswer =
      Aggregate(
        Seq(a), Seq(Alias(count(Literal(1)), "a")(), a),
        Project(Seq(x, y, a3, b3, a),
          Join(DomainJoin(Seq(a), testRelation2), testRelation3, LeftOuter,
            Some(And(y === a, x === a3)), JoinHint.NONE)))
    check(innerPlan, outerPlan, correctAnswer, Seq(a <=> a))
  }

  test("SPARK-43780: aggregation in subquery with correlated left join, " +
    "correlation over right side") {
    // Same as above, but the join predicate connects the outer reference and the column from the
    // right (optional) side of the left join. Domain join is still not needed.
    val outerPlan = testRelation
    val innerPlan =
      Aggregate(
        Seq.empty[Expression], Seq(Alias(count(Literal(1)), "a")()),
        Project(Seq(x, y, a3, b3),
          Join(testRelation2, testRelation3, LeftOuter,
            Some(And(x === a3, b3 === OuterReference(b))), JoinHint.NONE)))

    val correctAnswer =
      Aggregate(
        Seq(b), Seq(Alias(count(Literal(1)), "a")(), b),
        Project(Seq(x, y, a3, b3, b),
          Join(DomainJoin(Seq(b), testRelation2), testRelation3, LeftOuter,
            Some(And(b === b3, x === a3)), JoinHint.NONE)))
    check(innerPlan, outerPlan, correctAnswer, Seq(b <=> b))
  }

  test("SPARK-43780: correlated left join preserves the join predicates") {
    // Left outer join preserves both predicates after being decorrelated.
    val outerPlan = testRelation
    val innerPlan =
      Filter(
        IsNotNull(c3),
        Project(Seq(x, y, a3, b3, c3),
          Join(testRelation2, testRelation3, LeftOuter,
            Some(And(x === a3, b3 === OuterReference(b))), JoinHint.NONE)))

    val correctAnswer =
      Filter(
        IsNotNull(c3),
        Project(Seq(x, y, a3, b3, c3, b),
          Join(DomainJoin(Seq(b), testRelation2), testRelation3, LeftOuter,
            Some(And(x === a3, b === b3)), JoinHint.NONE)))
    check(innerPlan, outerPlan, correctAnswer, Seq(b <=> b))
  }

  test("SPARK-43780: union all in subquery with correlated join") {
    val outerPlan = testRelation
    val innerPlan =
      Union(
        Seq(Project(Seq(x, b3),
          Join(testRelation2, testRelation3, Inner,
            Some(And(x === a3, y === OuterReference(a))), JoinHint.NONE)),
          Project(Seq(a4, b4),
            testRelation4)))
    val correctAnswer =
      Union(
        Seq(Project(Seq(x, b3, a),
          Project(Seq(x, b3, a),
            Join(
              DomainJoin(Seq(a), testRelation2),
              testRelation3, Inner,
              Some(And(x === a3, y === a)), JoinHint.NONE))),
          Project(Seq(a4, b4, a),
            DomainJoin(Seq(a),
              Project(Seq(a4, b4), testRelation4)))))
    check(innerPlan, outerPlan, correctAnswer, Seq(a <=> a))
  }

  test("window function with correlated equality predicate") {
    val outerPlan = testRelation2
    val innerPlan =
      Window(Seq(b, c),
        partitionSpec = Seq(c), orderSpec = b.asc :: Nil,
        Filter(And(OuterReference(x) === a, b === 3),
          testRelation))
    // Both the project list and the partition spec have added the correlated variable.
    val correctAnswer =
      Window(Seq(b, c, a), partitionSpec = Seq(c, a), orderSpec = b.asc :: Nil,
        Filter(b === 3,
          testRelation))
    check(innerPlan, outerPlan, correctAnswer, Seq(x === a))
  }

  test("window function with correlated non-equality predicate") {
    val outerPlan = testRelation2
    val innerPlan =
      Window(Seq(b, c),
        partitionSpec = Seq(c), orderSpec = b.asc :: Nil,
        Filter(And(OuterReference(x) > a, b === 3),
          testRelation))
    // Both the project list and the partition spec have added the correlated variable.
    // The input to the filter is a domain join that produces 'x' values.
    val correctAnswer =
    Window(Seq(b, c, x), partitionSpec = Seq(c, x), orderSpec = b.asc :: Nil,
      Filter(And(b === 3, x > a),
        DomainJoin(Seq(x), testRelation)))
    check(innerPlan, outerPlan, correctAnswer, Seq(x <=> x))
  }

  test("window function with correlated columns inside") {
    val outerPlan = testRelation2
    val innerPlan =
      Window(Seq(b, c),
        partitionSpec = Seq(c, OuterReference(x)), orderSpec = b.asc :: Nil,
        Filter(b === 3,
          testRelation))
    val e = intercept[java.lang.AssertionError] {
      DecorrelateInnerQuery(innerPlan, outerPlan.select())
    }
    assert(e.getMessage.contains("Correlated column is not allowed in"))
  }

  test("SPARK-36191: limit in the correlated subquery") {
    val outerPlan = testRelation
    val innerPlan =
      Project(Seq(x),
        Limit(1, Filter(OuterReference(a) === x,
          testRelation2)))
    val (outputPlan, joinCond) = DecorrelateInnerQuery(innerPlan, outerPlan.select())

    val alias = getRowNumberFunc(outputPlan)

    val correctAnswer = Project(Seq(x), Project(Seq(x, y, z),
      Filter(GreaterThanOrEqual(1, alias.toAttribute),
        Window(Seq(alias), Seq(x), Nil, testRelation2))))
    check(outputPlan, joinCond, correctAnswer, Seq(x === a))
  }

  test("SPARK-36191: limit and order by in the correlated subquery") {
    val outerPlan = testRelation
    val innerPlan =
      Project(Seq(x),
        Limit(5, Sort(Seq(SortOrder(x, Ascending)), true,
          Filter(OuterReference(a) > x,
            testRelation2))))

    val (outputPlan, joinCond) = DecorrelateInnerQuery(innerPlan, outerPlan.select())

    val alias = getRowNumberFunc(outputPlan)
    val rowNumber = WindowExpression(RowNumber(),
      WindowSpecDefinition(Seq(a), Seq(SortOrder(x, Ascending)),
        SpecifiedWindowFrame(RowFrame, UnboundedPreceding, CurrentRow)))
    val rowNumberAlias = Alias(rowNumber, alias.name)()

    val correctAnswer = Project(Seq(x, a), Project(Seq(a, x, y, z),
      Filter(LessThanOrEqual(rowNumberAlias.toAttribute, 5),
        Window(Seq(rowNumberAlias), Seq(a), Seq(SortOrder(x, Ascending)),
          Filter(GreaterThan(a, x),
            DomainJoin(Seq(a), testRelation2))))))
    check(outputPlan, joinCond, correctAnswer, Seq(a <=> a))
  }

  test("SPARK-36191: limit and order by in the correlated subquery with aggregation") {
    val outerPlan = testRelation
    val minY = Alias(min(y), "min_y")()

    val innerPlan =
      Project(Seq(x),
        Limit(5, Sort(Seq(SortOrder(minY.toAttribute, Ascending)), true,
          Aggregate(Seq(x), Seq(minY, x),
            Filter(OuterReference(a) > x,
              testRelation2)))))

    val (outputPlan, joinCond) = DecorrelateInnerQuery(innerPlan, outerPlan.select())

    val alias = getRowNumberFunc(outputPlan)
    val rowNumber = WindowExpression(RowNumber(),
      WindowSpecDefinition(Seq(a), Seq(SortOrder(minY.toAttribute, Ascending)),
        SpecifiedWindowFrame(RowFrame, UnboundedPreceding, CurrentRow)))
    val rowNumberAlias = Alias(rowNumber, alias.name)()
    val correctAnswer = Project(Seq(x, a), Project(Seq(minY.toAttribute, x, a),
      Filter(LessThanOrEqual(rowNumberAlias.toAttribute, 5),
        Window(Seq(rowNumberAlias), Seq(a),
          Seq(SortOrder(minY.toAttribute, Ascending)),
          Aggregate(Seq(x, a), Seq(minY, x, a),
            Filter(GreaterThan(a, x),
              DomainJoin(Seq(a), testRelation2)))))))
    check(outputPlan, joinCond, correctAnswer, Seq(a <=> a))

  }

  test("SPARK-36191: order by with correlated attribute") {
    val outerPlan = testRelation
    val innerPlan =
      Project(Seq(x),
        Limit(5, Sort(Seq(SortOrder(OuterReference(a), Ascending)), true,
          Filter(OuterReference(a) > x,
            testRelation2))))
    val (outputPlan, joinCond) = DecorrelateInnerQuery(innerPlan, outerPlan.select())

    val alias = getRowNumberFunc(outputPlan)
    val rowNumber = WindowExpression(RowNumber(),
      WindowSpecDefinition(Seq(a), Seq(SortOrder(a, Ascending)),
        SpecifiedWindowFrame(RowFrame, UnboundedPreceding, CurrentRow)))
    val rowNumberAlias = Alias(rowNumber, alias.name)()

    val correctAnswer = Project(Seq(x, a), Project(Seq(a, x, y, z),
      Filter(LessThanOrEqual(rowNumberAlias.toAttribute, 5),
        Window(Seq(rowNumberAlias), Seq(a), Seq(SortOrder(a, Ascending)),
          Filter(GreaterThan(a, x),
            DomainJoin(Seq(a), testRelation2))))))
    check(outputPlan, joinCond, correctAnswer, Seq(a <=> a))
  }
}
