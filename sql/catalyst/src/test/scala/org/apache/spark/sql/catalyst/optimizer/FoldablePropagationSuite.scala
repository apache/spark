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

  val testRelation = LocalRelation("a".attr.int, "b".attr.int)

  test("Propagate from subquery") {
    val query = OneRowRelation()
      .select(Literal(1).as("a"), Literal(2).as("b"))
      .subquery("T")
      .select("a".attr, "b".attr)
    val optimized = Optimize.execute(query.analyze)
    val correctAnswer = OneRowRelation()
      .select(Literal(1).as("a"), Literal(2).as("b"))
      .subquery("T")
      .select(Literal(1).as("a"), Literal(2).as("b")).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("Propagate to select clause") {
    val query = testRelation
      .select("a".attr.as("x"), "str".as("y"), "b".attr.as("z"))
      .select("x".attr, "y".attr, "z".attr)
    val optimized = Optimize.execute(query.analyze)
    val correctAnswer = testRelation
      .select("a".attr.as("x"), "str".as("y"), "b".attr.as("z"))
      .select("x".attr, "str".as("y"), "z".attr).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("Propagate to where clause") {
    val query = testRelation
      .select("str".as("y"))
      .where("y".attr === "str" && "str" === "y".attr)
    val optimized = Optimize.execute(query.analyze)
    val correctAnswer = testRelation
      .select("str".as("y"))
      .where("str".as("y") === "str" && "str" === "str".as("y")).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("Propagate to orderBy clause") {
    val query = testRelation
      .select("a".attr.as("x"), Year(CurrentDate()).as("y"), "b".attr)
      .orderBy("x".attr.asc, "y".attr.asc, "b".attr.desc)
    val optimized = Optimize.execute(query.analyze)
    val correctAnswer = testRelation
      .select("a".attr.as("x"), Year(CurrentDate()).as("y"), "b".attr)
      .orderBy("x".attr.asc, SortOrder(Year(CurrentDate()), Ascending), "b".attr.desc).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("Propagate to groupBy clause") {
    val query = testRelation
      .select("a".attr.as("x"), Year(CurrentDate()).as("y"), "b".attr)
      .groupBy("x".attr, "y".attr, "b".attr)(sum("x".attr),
        avg("y".attr).as("AVG"), count("b".attr))
    val optimized = Optimize.execute(query.analyze)
    val correctAnswer = testRelation
      .select("a".attr.as("x"), Year(CurrentDate()).as("y"), "b".attr)
      .groupBy("x".attr, Year(CurrentDate()).as("y"), "b".attr)(sum("x".attr),
        avg(Year(CurrentDate())).as("AVG"), count("b".attr)).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("Propagate in a complex query") {
    val query = testRelation
      .select("a".attr.as("x"), Year(CurrentDate()).as("y"), "b".attr)
      .where("x".attr > 1 && "y".attr === 2016 && "b".attr > 1)
      .groupBy("x".attr, "y".attr, "b".attr)(sum("x".attr),
        avg("y".attr).as("AVG"), count("b".attr))
      .orderBy("x".attr.asc, "AVG".attr.asc)
    val optimized = Optimize.execute(query.analyze)
    val correctAnswer = testRelation
      .select("a".attr.as("x"), Year(CurrentDate()).as("y"), "b".attr)
      .where("x".attr > 1 && Year(CurrentDate()).as("y") === 2016 && "b".attr > 1)
      .groupBy("x".attr, Year(CurrentDate()).as("y"), "b".attr)(sum("x".attr),
        avg(Year(CurrentDate())).as("AVG"), count("b".attr))
      .orderBy("x".attr.asc, "AVG".attr.asc).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("Propagate in subqueries of Union queries") {
    val query = Union(
      Seq(
        testRelation.select(Literal(1).as("x"), "a".attr).select("x".attr, "x".attr + "a".attr),
        testRelation.select(Literal(2).as("x"), "a".attr).select("x".attr, "x".attr + "a".attr)))
      .select("x".attr)
    val optimized = Optimize.execute(query.analyze)
    val correctAnswer = Union(
      Seq(
        testRelation.select(Literal(1).as("x"), "a".attr)
          .select(Literal(1).as("x"), (Literal(1).as("x") + "a".attr).as("(x + a)")),
        testRelation.select(Literal(2).as("x"), "a".attr)
          .select(Literal(2).as("x"), (Literal(2).as("x") + "a".attr).as("(x + a)"))))
      .select("x".attr).analyze
    comparePlans(optimized, correctAnswer)
  }

  test("Propagate in inner join") {
    val ta = testRelation.select("a".attr, Literal(1).as("tag"))
      .union(testRelation.select("a".attr.as("a"), Literal(2).as("tag")))
      .subquery("ta")
    val tb = testRelation.select("a".attr, Literal(1).as("tag"))
      .union(testRelation.select("a".attr.as("a"), Literal(2).as("tag")))
      .subquery("tb")
    val query = ta.join(tb, Inner,
      Some("ta.a".attr === "tb.a".attr && "ta.tag".attr === "tb.tag".attr))
    val optimized = Optimize.execute(query.analyze)
    val correctAnswer = query.analyze
    comparePlans(optimized, correctAnswer)
  }

  test("Propagate in expand") {
    val c1 = Literal(1).as("a")
    val c2 = Literal(2).as("b")
    val a1 = c1.toAttribute.newInstance().withNullability(true)
    val a2 = c2.toAttribute.newInstance().withNullability(true)
    val expand = Expand(
      Seq(Seq(Literal(null), "b".attr), Seq("a".attr, Literal(null))),
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
    val left = LocalRelation("a".attr.int).select("a".attr, Literal(1).as("b"))
    val right = LocalRelation("c".attr.int).select("c".attr, Literal(1).as("d"))

    val join = left.join(
      right,
      joinType = LeftOuter,
      condition = Some("a".attr === "c".attr && "b".attr === "d".attr))
    val query = join.select(("b".attr + 3).as("res")).analyze
    val optimized = Optimize.execute(query)

    val correctAnswer = left.join(
      right,
      joinType = LeftOuter,
      condition = Some("a".attr === "c".attr && Literal(1) === Literal(1)))
      .select((Literal(1) + 3).as("res")).analyze
    comparePlans(optimized, correctAnswer)
  }

  test("SPARK-32635: Replace references with foldables coming only from the node's children") {
    val leftExpression = "a".attr.int
    val left = LocalRelation(leftExpression).select("a".attr)
    val rightExpression = Alias(Literal(2), "a")(leftExpression.exprId)
    val right = LocalRelation("b".attr.int).select("b".attr, rightExpression).select("b".attr)
    val join = left.join(right, joinType = LeftOuter, condition = Some("b".attr === "a".attr))

    val query = join.analyze
    val optimized = Optimize.execute(query)
    comparePlans(optimized, query)
  }

  test("SPARK-32951: Foldable propagation from Aggregate") {
    val query = testRelation
      .groupBy("a".attr)("a".attr, sum("b".attr).as("b"), Literal(1).as("c"))
      .select("a".attr, "b".attr, "c".attr)

    val optimized = Optimize.execute(query.analyze)
    val correctAnswer = testRelation
      .groupBy("a".attr)("a".attr, sum("b".attr).as("b"), Literal(1).as("c"))
      .select("a".attr, "b".attr, Literal(1).as("c")).analyze
    comparePlans(optimized, correctAnswer)
  }
}
