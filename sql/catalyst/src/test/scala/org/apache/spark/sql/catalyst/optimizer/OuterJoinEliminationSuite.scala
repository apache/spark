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
import org.apache.spark.sql.catalyst.expressions.{Coalesce, If, IsNotNull, Literal, RaiseError}
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StringType
import org.apache.spark.unsafe.types.UTF8String

class OuterJoinEliminationSuite extends PlanTest {
  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("Subqueries", Once,
        EliminateSubqueryAliases) ::
      Batch("Outer Join Elimination", Once,
        EliminateOuterJoin,
        PushPredicateThroughJoin) :: Nil
  }

  val testRelation = LocalRelation($"a".int, $"b".int, $"c".int)
  val testRelation1 = LocalRelation($"d".int, $"e".int, $"f".int)

  test("joins: full outer to inner") {
    val x = testRelation.subquery("x")
    val y = testRelation1.subquery("y")

    val originalQuery =
      x.join(y, FullOuter, Option("x.a".attr === "y.d".attr))
        .where("x.b".attr >= 1 && "y.d".attr >= 2)

    val optimized = Optimize.execute(originalQuery.analyze)
    val left = testRelation.where($"b" >= 1)
    val right = testRelation1.where($"d" >= 2)
    val correctAnswer =
      left.join(right, Inner, Option("a".attr === "d".attr)).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("joins: full outer to right") {
    val x = testRelation.subquery("x")
    val y = testRelation1.subquery("y")

    val originalQuery =
      x.join(y, FullOuter, Option("x.a".attr === "y.d".attr)).where("y.d".attr > 2)

    val optimized = Optimize.execute(originalQuery.analyze)
    val left = testRelation
    val right = testRelation1.where($"d" > 2)
    val correctAnswer =
      left.join(right, RightOuter, Option("a".attr === "d".attr)).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("joins: full outer to left") {
    val x = testRelation.subquery("x")
    val y = testRelation1.subquery("y")

    val originalQuery =
      x.join(y, FullOuter, Option("x.a".attr === "y.d".attr)).where("x.a".attr <=> 2)

    val optimized = Optimize.execute(originalQuery.analyze)
    val left = testRelation.where($"a" <=> 2)
    val right = testRelation1
    val correctAnswer =
      left.join(right, LeftOuter, Option("a".attr === "d".attr)).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("joins: right to inner") {
    val x = testRelation.subquery("x")
    val y = testRelation1.subquery("y")

    val originalQuery =
      x.join(y, RightOuter, Option("x.a".attr === "y.d".attr)).where("x.b".attr > 2)

    val optimized = Optimize.execute(originalQuery.analyze)
    val left = testRelation.where($"b" > 2)
    val right = testRelation1
    val correctAnswer =
      left.join(right, Inner, Option("a".attr === "d".attr)).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("joins: left to inner") {
    val x = testRelation.subquery("x")
    val y = testRelation1.subquery("y")

    val originalQuery =
      x.join(y, LeftOuter, Option("x.a".attr === "y.d".attr))
        .where("y.e".attr.isNotNull)

    val optimized = Optimize.execute(originalQuery.analyze)
    val left = testRelation
    val right = testRelation1.where($"e".isNotNull)
    val correctAnswer =
      left.join(right, Inner, Option("a".attr === "d".attr)).analyze

    comparePlans(optimized, correctAnswer)
  }

  // evaluating if mixed OR and NOT expressions can eliminate all null-supplying rows
  test("joins: left to inner with complicated filter predicates #1") {
    val x = testRelation.subquery("x")
    val y = testRelation1.subquery("y")

    val originalQuery =
      x.join(y, LeftOuter, Option("x.a".attr === "y.d".attr))
        .where(!$"e".isNull || ($"d".isNotNull && $"f".isNull))

    val optimized = Optimize.execute(originalQuery.analyze)
    val left = testRelation
    val right = testRelation1.where(!$"e".isNull || ($"d".isNotNull && $"f".isNull))
    val correctAnswer =
      left.join(right, Inner, Option("a".attr === "d".attr)).analyze

    comparePlans(optimized, correctAnswer)
  }

  // eval(emptyRow) of 'e.in(1, 2) will return null instead of false
  test("joins: left to inner with complicated filter predicates #2") {
    val x = testRelation.subquery("x")
    val y = testRelation1.subquery("y")

    val originalQuery =
      x.join(y, LeftOuter, Option("x.a".attr === "y.d".attr))
        .where($"e".in(1, 2))

    val optimized = Optimize.execute(originalQuery.analyze)
    val left = testRelation
    val right = testRelation1.where($"e".in(1, 2))
    val correctAnswer =
      left.join(right, Inner, Option("a".attr === "d".attr)).analyze

    comparePlans(optimized, correctAnswer)
  }

  // evaluating if mixed OR and AND expressions can eliminate all null-supplying rows
  test("joins: left to inner with complicated filter predicates #3") {
    val x = testRelation.subquery("x")
    val y = testRelation1.subquery("y")

    val originalQuery =
      x.join(y, LeftOuter, Option("x.a".attr === "y.d".attr))
        .where((!$"e".isNull || ($"d".isNotNull && $"f".isNull)) && $"e".isNull)

    val optimized = Optimize.execute(originalQuery.analyze)
    val left = testRelation
    val right = testRelation1.where((!$"e".isNull || ($"d".isNotNull && $"f".isNull))
      && $"e".isNull)
    val correctAnswer =
      left.join(right, Inner, Option("a".attr === "d".attr)).analyze

    comparePlans(optimized, correctAnswer)
  }

  // evaluating if the expressions that have both left and right attributes
  // can eliminate all null-supplying rows
  // FULL OUTER => INNER
  test("joins: left to inner with complicated filter predicates #4") {
    val x = testRelation.subquery("x")
    val y = testRelation1.subquery("y")

    val originalQuery =
      x.join(y, FullOuter, Option("x.a".attr === "y.d".attr))
        .where("x.b".attr + 3 === "y.e".attr)

    val optimized = Optimize.execute(originalQuery.analyze)
    val left = testRelation
    val right = testRelation1
    val correctAnswer =
      left.join(right, Inner, Option("b".attr + 3 === "e".attr && "a".attr === "d".attr)).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("joins: no outer join elimination if the filter is not NULL eliminated") {
    val x = testRelation.subquery("x")
    val y = testRelation1.subquery("y")

    val originalQuery =
      x.join(y, FullOuter, Option("x.a".attr === "y.d".attr))
        .where(Coalesce("y.e".attr :: "x.a".attr :: Nil) === 0)

    val optimized = Optimize.execute(originalQuery.analyze)

    val left = testRelation
    val right = testRelation1
    val correctAnswer =
      left.join(right, FullOuter, Option("a".attr === "d".attr))
        .where(Coalesce("e".attr :: "a".attr :: Nil) === 0).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("joins: no outer join elimination if the filter's constraints are not NULL eliminated") {
    val x = testRelation.subquery("x")
    val y = testRelation1.subquery("y")

    val originalQuery =
      x.join(y, FullOuter, Option("x.a".attr === "y.d".attr))
        .where(IsNotNull(Coalesce("y.e".attr :: "x.a".attr :: Nil)))

    val optimized = Optimize.execute(originalQuery.analyze)

    val left = testRelation
    val right = testRelation1
    val correctAnswer =
      left.join(right, FullOuter, Option("a".attr === "d".attr))
        .where(IsNotNull(Coalesce("e".attr :: "a".attr :: Nil))).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("no outer join elimination if constraint propagation is disabled") {
    withSQLConf(SQLConf.CONSTRAINT_PROPAGATION_ENABLED.key -> "false") {
      val x = testRelation.subquery("x")
      val y = testRelation1.subquery("y")

      // The predicate "x.b + y.d >= 3" will be inferred constraints like:
      // "x.b != null" and "y.d != null", if constraint propagation is enabled.
      // When we disable it, the predicate can't be evaluated on left or right plan and used to
      // filter out nulls. So the Outer Join will not be eliminated.
      val originalQuery =
      x.join(y, FullOuter, Option("x.a".attr === "y.d".attr))
        .where("x.b".attr + "y.d".attr >= 3)

      val optimized = Optimize.execute(originalQuery.analyze)

      comparePlans(optimized, originalQuery.analyze)
    }
  }

  test("SPARK-38868: exception thrown from filter predicate does not propagate") {
    val x = testRelation.subquery("x")
    val y = testRelation1.subquery("y")

    val message = Literal(UTF8String.fromString("Bad value"), StringType)
    val originalQuery =
      x.join(y, LeftOuter, Option("x.a".attr === "y.d".attr))
        .where(If("y.d".attr > 0, true, RaiseError(message)).isNull)

    val optimized = Optimize.execute(originalQuery.analyze)

    comparePlans(optimized, originalQuery.analyze)
  }

  test("SPARK-39172: Remove left/right outer join if only left/right side columns are selected " +
    "and the join keys on the other side are unique") {
    val x = testRelation.subquery("x")
    val y = testRelation1.subquery("y")
    comparePlans(Optimize.execute(
      x.join(y.groupBy($"d")($"d"), LeftOuter, Some($"a" === $"d"))
        .select($"a", $"b", $"c").analyze),
      x.select($"a", $"b", $"c").analyze
    )

    comparePlans(Optimize.execute(
      x.join(y.groupBy($"d")($"d", count($"d").as("x")), LeftOuter,
        Some($"a" === $"d" && $"b" === $"x"))
        .select($"a", $"b", $"c").analyze),
      x.select($"a", $"b", $"c").analyze
    )

    comparePlans(Optimize.execute(
      x.groupBy($"a")($"a").join(y, RightOuter, Some($"a" === $"d"))
        .select($"d", $"e", $"f").analyze),
      y.select($"d", $"e", $"f").analyze
    )

    comparePlans(Optimize.execute(
      x.groupBy($"a")($"a", count($"a").as("x")).join(y, RightOuter,
        Some($"a" === $"d" && $"x" === $"e"))
        .select($"d", $"e", $"f").analyze),
      y.select($"d", $"e", $"f").analyze
    )

    // negative cases
    // not a equi-join
    val p1 = x.join(y.groupBy($"d")($"d"), LeftOuter, Some($"a" > $"d"))
      .select($"a").analyze
    comparePlans(Optimize.execute(p1), p1)

    // do not exist unique key
    val p2 = x.join(y.groupBy($"d", $"e")($"d", $"e"), LeftOuter, Some($"a" === $"d"))
      .select($"a").analyze
    comparePlans(Optimize.execute(p2), p2)

    // output comes from the right side of a left outer join
    val p3 = x.join(y.groupBy($"d")($"d"), LeftOuter, Some($"a" === $"d"))
      .select($"a", $"d").analyze
    comparePlans(Optimize.execute(p3), p3)
  }
}
