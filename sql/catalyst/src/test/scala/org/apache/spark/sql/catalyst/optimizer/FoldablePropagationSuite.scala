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

  val testRelation = LocalRelation(Symbol("a").int, Symbol("b").int)

  test("Propagate from subquery") {
    val query = OneRowRelation()
      .select(Literal(1).as(Symbol("a")), Literal(2).as(Symbol("b")))
      .subquery(Symbol("T"))
      .select(Symbol("a"), Symbol("b"))
    val optimized = Optimize.execute(query.analyze)
    val correctAnswer = OneRowRelation()
      .select(Literal(1).as(Symbol("a")), Literal(2).as(Symbol("b")))
      .subquery(Symbol("T"))
      .select(Literal(1).as(Symbol("a")), Literal(2).as(Symbol("b"))).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("Propagate to select clause") {
    val query = testRelation
      .select(Symbol("a").as(Symbol("x")), "str".as(Symbol("y")), Symbol("b").as(Symbol("z")))
      .select(Symbol("x"), Symbol("y"), Symbol("z"))
    val optimized = Optimize.execute(query.analyze)
    val correctAnswer = testRelation
      .select(Symbol("a").as(Symbol("x")), "str".as(Symbol("y")), Symbol("b").as(Symbol("z")))
      .select(Symbol("x"), "str".as(Symbol("y")), Symbol("z")).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("Propagate to where clause") {
    val query = testRelation
      .select("str".as(Symbol("y")))
      .where(Symbol("y") === "str" && "str" === Symbol("y"))
    val optimized = Optimize.execute(query.analyze)
    val correctAnswer = testRelation
      .select("str".as(Symbol("y")))
      .where("str".as(Symbol("y")) === "str" && "str" === "str".as(Symbol("y"))).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("Propagate to orderBy clause") {
    val query = testRelation
      .select(Symbol("a").as(Symbol("x")), Year(CurrentDate()).as(Symbol("y")), Symbol("b"))
      .orderBy(Symbol("x").asc, Symbol("y").asc, Symbol("b").desc)
    val optimized = Optimize.execute(query.analyze)
    val correctAnswer = testRelation
      .select(Symbol("a").as(Symbol("x")), Year(CurrentDate()).as(Symbol("y")), Symbol("b"))
      .orderBy(Symbol("x").asc, SortOrder(Year(CurrentDate()), Ascending), Symbol("b").desc).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("Propagate to groupBy clause") {
    val query = testRelation
      .select(Symbol("a").as(Symbol("x")), Year(CurrentDate()).as(Symbol("y")), Symbol("b"))
      .groupBy(Symbol("x"), Symbol("y"), Symbol("b"))(sum(Symbol("x")),
        avg(Symbol("y")).as(Symbol("AVG")), count(Symbol("b")))
    val optimized = Optimize.execute(query.analyze)
    val correctAnswer = testRelation
      .select(Symbol("a").as(Symbol("x")), Year(CurrentDate()).as(Symbol("y")), Symbol("b"))
      .groupBy(Symbol("x"), Year(CurrentDate()).as(Symbol("y")), Symbol("b"))(sum(Symbol("x")),
        avg(Year(CurrentDate())).as(Symbol("AVG")),
        count(Symbol("b"))).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("Propagate in a complex query") {
    val query = testRelation
      .select(Symbol("a").as(Symbol("x")), Year(CurrentDate()).as(Symbol("y")), Symbol("b"))
      .where(Symbol("x") > 1 && Symbol("y") === 2016 && Symbol("b") > 1)
      .groupBy(Symbol("x"), Symbol("y"), Symbol("b"))(sum(Symbol("x")),
        avg(Symbol("y")).as(Symbol("AVG")), count(Symbol("b")))
      .orderBy(Symbol("x").asc, Symbol("AVG").asc)
    val optimized = Optimize.execute(query.analyze)
    val correctAnswer = testRelation
      .select(Symbol("a").as(Symbol("x")), Year(CurrentDate()).as(Symbol("y")), Symbol("b"))
      .where(Symbol("x") > 1 && Year(CurrentDate()).as(Symbol("y")) === 2016 && Symbol("b") > 1)
      .groupBy(Symbol("x"), Year(CurrentDate()).as("y"), Symbol("b"))(sum(Symbol("x")),
        avg(Year(CurrentDate())).as(Symbol("AVG")),
        count(Symbol("b")))
      .orderBy(Symbol("x").asc, Symbol("AVG").asc).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("Propagate in subqueries of Union queries") {
    val query = Union(
      Seq(
        testRelation.select(Literal(1).as(Symbol("x")),
          Symbol("a")).select(Symbol("x"), Symbol("x") + Symbol("a")),
        testRelation.select(Literal(2).as(Symbol("x")),
          Symbol("a")).select(Symbol("x"), Symbol("x") + Symbol("a"))))
      .select(Symbol("x"))
    val optimized = Optimize.execute(query.analyze)
    val correctAnswer = Union(
      Seq(
        testRelation.select(Literal(1).as(Symbol("x")), Symbol("a"))
          .select(Literal(1).as(Symbol("x")),
            (Literal(1).as(Symbol("x")) + Symbol("a")).as("(x + a)")),
        testRelation.select(Literal(2).as(Symbol("x")), Symbol("a"))
          .select(Literal(2).as(Symbol("x")),
            (Literal(2).as(Symbol("x")) + Symbol("a")).as("(x + a)"))))
      .select(Symbol("x")).analyze
    comparePlans(optimized, correctAnswer)
  }

  test("Propagate in inner join") {
    val ta = testRelation.select(Symbol("a"), Literal(1).as(Symbol("tag")))
      .union(testRelation.select(Symbol("a").as(Symbol("a")), Literal(2).as(Symbol("tag"))))
      .subquery(Symbol("ta"))
    val tb = testRelation.select(Symbol("a"), Literal(1).as(Symbol("tag")))
      .union(testRelation.select(Symbol("a").as(Symbol("a")), Literal(2).as(Symbol("tag"))))
      .subquery(Symbol("tb"))
    val query = ta.join(tb, Inner,
      Some("ta.a".attr === "tb.a".attr && "ta.tag".attr === "tb.tag".attr))
    val optimized = Optimize.execute(query.analyze)
    val correctAnswer = query.analyze
    comparePlans(optimized, correctAnswer)
  }

  test("Propagate in expand") {
    val c1 = Literal(1).as(Symbol("a"))
    val c2 = Literal(2).as(Symbol("b"))
    val a1 = c1.toAttribute.newInstance().withNullability(true)
    val a2 = c2.toAttribute.newInstance().withNullability(true)
    val expand = Expand(
      Seq(Seq(Literal(null), Symbol("b")), Seq(Symbol("a"), Literal(null))),
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
    val left = LocalRelation(Symbol("a").int).select(Symbol("a"), Literal(1).as(Symbol("b")))
    val right = LocalRelation(Symbol("c").int).select(Symbol("c"), Literal(1).as(Symbol("d")))

    val join = left.join(
      right,
      joinType = LeftOuter,
      condition = Some(Symbol("a") === Symbol("c") && Symbol("b") === Symbol("d")))
    val query = join.select((Symbol("b") + 3).as(Symbol("res"))).analyze
    val optimized = Optimize.execute(query)

    val correctAnswer = left.join(
      right,
      joinType = LeftOuter,
      condition = Some(Symbol("a") === Symbol("c") && Literal(1) === Literal(1)))
      .select((Literal(1) + 3).as(Symbol("res"))).analyze
    comparePlans(optimized, correctAnswer)
  }

  test("SPARK-32635: Replace references with foldables coming only from the node's children") {
    val leftExpression = Symbol("a").int
    val left = LocalRelation(leftExpression).select(Symbol("a"))
    val rightExpression = Alias(Literal(2), "a")(leftExpression.exprId)
    val right = LocalRelation(Symbol("b").int).select(Symbol("b"),
      rightExpression).select(Symbol("b"))
    val join =
      left.join(right, joinType = LeftOuter, condition = Some(Symbol("b") === Symbol("a")))

    val query = join.analyze
    val optimized = Optimize.execute(query)
    comparePlans(optimized, query)
  }

  test("SPARK-32951: Foldable propagation from Aggregate") {
    val query = testRelation
      .groupBy(Symbol("a"))(Symbol("a"), sum(Symbol("b")).as(Symbol("b")),
        Literal(1).as(Symbol("c")))
      .select(Symbol("a"), Symbol("b"), Symbol("c"))

    val optimized = Optimize.execute(query.analyze)
    val correctAnswer = testRelation
      .groupBy(Symbol("a"))(Symbol("a"), sum(Symbol("b")).as(Symbol("b")),
        Literal(1).as(Symbol("c")))
      .select(Symbol("a"), Symbol("b"), Literal(1).as(Symbol("c"))).analyze
    comparePlans(optimized, correctAnswer)
  }
}
