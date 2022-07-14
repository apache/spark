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

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.analysis.EliminateSubqueryAliases
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions.Add
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules._

class LimitPushdownSuite extends PlanTest {

  private object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("Subqueries", Once,
        EliminateSubqueryAliases) ::
      Batch("Limit pushdown", FixedPoint(100),
        LimitPushDown,
        EliminateLimits,
        ConstantFolding,
        BooleanSimplification) :: Nil
  }

  private val testRelation = LocalRelation.fromExternalRows(
    Seq("a".attr.int, "b".attr.int, "c".attr.int),
    1.to(6).map(_ => Row(1, 2, 3)))
  private val testRelation2 = LocalRelation.fromExternalRows(
    Seq("d".attr.int, "e".attr.int, "f".attr.int),
    1.to(6).map(_ => Row(1, 2, 3)))
  private val x = testRelation.subquery("x")
  private val y = testRelation.subquery("y")

  // Union ---------------------------------------------------------------------------------------

  test("Union: limit to each side") {
    val unionQuery = Union(testRelation, testRelation2).limit(1)
    val unionOptimized = Optimize.execute(unionQuery.analyze)
    val unionCorrectAnswer =
      Limit(1, Union(LocalLimit(1, testRelation), LocalLimit(1, testRelation2))).analyze
    comparePlans(unionOptimized, unionCorrectAnswer)
  }

  test("Union: limit to each side with constant-foldable limit expressions") {
    val unionQuery = Union(testRelation, testRelation2).limit(Add(1, 1))
    val unionOptimized = Optimize.execute(unionQuery.analyze)
    val unionCorrectAnswer =
      Limit(2, Union(LocalLimit(2, testRelation), LocalLimit(2, testRelation2))).analyze
    comparePlans(unionOptimized, unionCorrectAnswer)
  }

  test("Union: limit to each side with the new limit number") {
    val unionQuery = Union(testRelation, testRelation2.limit(3)).limit(1)
    val unionOptimized = Optimize.execute(unionQuery.analyze)
    val unionCorrectAnswer =
      Limit(1, Union(LocalLimit(1, testRelation), LocalLimit(1, testRelation2))).analyze
    comparePlans(unionOptimized, unionCorrectAnswer)
  }

  test("Union: no limit to both sides if children having smaller limit values") {
    val unionQuery =
      Union(testRelation.limit(1), testRelation2.select($"d", $"e", $"f").limit(1)).limit(2)
    val unionOptimized = Optimize.execute(unionQuery.analyze)
    val unionCorrectAnswer =
      Union(testRelation.limit(1), testRelation2.select($"d", $"e", $"f").limit(1)).analyze
    comparePlans(unionOptimized, unionCorrectAnswer)
  }

  test("Union: limit to each sides if children having larger limit values") {
    val unionQuery =
      Union(testRelation.limit(3), testRelation2.select($"d", $"e", $"f").limit(4)).limit(2)
    val unionOptimized = Optimize.execute(unionQuery.analyze)
    val unionCorrectAnswer =
      Limit(2, Union(
        LocalLimit(2, testRelation), LocalLimit(2, testRelation2.select($"d", $"e", $"f")))).analyze
    comparePlans(unionOptimized, unionCorrectAnswer)
  }

  // Outer join ----------------------------------------------------------------------------------

  test("left outer join") {
    val originalQuery = x.join(y, LeftOuter).limit(1)
    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer = Limit(1, LocalLimit(1, x).join(y, LeftOuter)).analyze
    comparePlans(optimized, correctAnswer)
  }

  test("left outer join and left sides are limited") {
    val originalQuery = x.limit(2).join(y, LeftOuter).limit(1)
    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer = Limit(1, LocalLimit(1, x).join(y, LeftOuter)).analyze
    comparePlans(optimized, correctAnswer)
  }

  test("left outer join and right sides are limited") {
    val originalQuery = x.join(y.limit(2), LeftOuter).limit(1)
    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer = Limit(1, LocalLimit(1, x).join(Limit(2, y), LeftOuter)).analyze
    comparePlans(optimized, correctAnswer)
  }

  test("right outer join") {
    val originalQuery = x.join(y, RightOuter).limit(1)
    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer = Limit(1, x.join(LocalLimit(1, y), RightOuter)).analyze
    comparePlans(optimized, correctAnswer)
  }

  test("right outer join and right sides are limited") {
    val originalQuery = x.join(y.limit(2), RightOuter).limit(1)
    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer = Limit(1, x.join(LocalLimit(1, y), RightOuter)).analyze
    comparePlans(optimized, correctAnswer)
  }

  test("right outer join and left sides are limited") {
    val originalQuery = x.limit(2).join(y, RightOuter).limit(1)
    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer = Limit(1, Limit(2, x).join(LocalLimit(1, y), RightOuter)).analyze
    comparePlans(optimized, correctAnswer)
  }

  test("larger limits are not pushed on top of smaller ones in right outer join") {
    val originalQuery = x.join(y.limit(5), RightOuter).limit(10)
    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer = Limit(10, x.join(Limit(5, y), RightOuter)).analyze
    comparePlans(optimized, correctAnswer)
  }

  test("full outer join where neither side is limited and both sides have same statistics") {
    assert(x.stats.sizeInBytes === y.stats.sizeInBytes)
    val originalQuery = x.join(y, FullOuter).limit(1).analyze
    val optimized = Optimize.execute(originalQuery)
    // No pushdown for FULL OUTER JOINS.
    comparePlans(optimized, originalQuery)
  }

  test("full outer join where neither side is limited and left side has larger statistics") {
    val xBig = testRelation.copy(data = Seq.fill(10)(null)).subquery("x")
    assert(xBig.stats.sizeInBytes > y.stats.sizeInBytes)
    val originalQuery = xBig.join(y, FullOuter).limit(1).analyze
    val optimized = Optimize.execute(originalQuery)
    // No pushdown for FULL OUTER JOINS.
    comparePlans(optimized, originalQuery)
  }

  test("full outer join where neither side is limited and right side has larger statistics") {
    val yBig = testRelation.copy(data = Seq.fill(10)(null)).subquery("y")
    assert(x.stats.sizeInBytes < yBig.stats.sizeInBytes)
    val originalQuery = x.join(yBig, FullOuter).limit(1).analyze
    val optimized = Optimize.execute(originalQuery)
    // No pushdown for FULL OUTER JOINS.
    comparePlans(optimized, originalQuery)
  }

  test("full outer join where both sides are limited") {
    val originalQuery = x.limit(2).join(y.limit(2), FullOuter).limit(1).analyze
    val optimized = Optimize.execute(originalQuery)
    // No pushdown for FULL OUTER JOINS.
    comparePlans(optimized, originalQuery)
  }

  test("SPARK-33433: Change Aggregate max rows to 1 if grouping is empty") {
    val analyzed1 = Limit(1, Union(
      x.groupBy()(count(1)),
      y.groupBy()(count(1)))).analyze
    val optimized1 = Optimize.execute(analyzed1)
    comparePlans(analyzed1, optimized1)

    // test push down
    val analyzed2 = Limit(1, Union(
      x.groupBy($"a")(count(1)),
      y.groupBy($"b")(count(1)))).analyze
    val optimized2 = Optimize.execute(analyzed2)
    val expected2 = Limit(1, Union(
      LocalLimit(1, x.groupBy($"a")(count(1))),
      LocalLimit(1, y.groupBy($"b")(count(1))))).analyze
    comparePlans(expected2, optimized2)
  }

  test("SPARK-26138: pushdown limit through InnerLike when condition is empty") {
    Seq(Cross, Inner).foreach { joinType =>
      val originalQuery = x.join(y, joinType).limit(1)
      val optimized = Optimize.execute(originalQuery.analyze)
      val correctAnswer = Limit(1, LocalLimit(1, x).join(LocalLimit(1, y), joinType)).analyze
      comparePlans(optimized, correctAnswer)
    }
  }

  test("SPARK-26138: Should not pushdown limit through InnerLike when condition is not empty") {
    Seq(Cross, Inner).foreach { joinType =>
      val originalQuery = x.join(y, joinType, Some("x.a".attr === "y.b".attr)).limit(1)
      val optimized = Optimize.execute(originalQuery.analyze)
      val correctAnswer = Limit(1, x.join(y, joinType, Some("x.a".attr === "y.b".attr))).analyze
      comparePlans(optimized, correctAnswer)
    }
  }

  test("SPARK-34514: Push down limit through LEFT SEMI and LEFT ANTI join") {
    // Push down when condition is empty
    Seq(LeftSemi, LeftAnti).foreach { joinType =>
      val originalQuery = x.join(y, joinType).limit(5)
      val optimized = Optimize.execute(originalQuery.analyze)
      val correctAnswer = Limit(5, LocalLimit(5, x).join(LocalLimit(1, y), joinType)).analyze
      comparePlans(optimized, correctAnswer)
    }

    // No push down when condition is not empty
    Seq(LeftSemi, LeftAnti).foreach { joinType =>
      val originalQuery = x.join(y, joinType, Some("x.a".attr === "y.b".attr)).limit(1)
      val optimized = Optimize.execute(originalQuery.analyze)
      val correctAnswer = Limit(1, x.join(y, joinType, Some("x.a".attr === "y.b".attr))).analyze
      comparePlans(optimized, correctAnswer)
    }
  }

  test("SPARK-34622: Fix Push down limit through join if its output is not match the LocalLimit") {
    val joinCondition = Some("x.a".attr === "y.a".attr && "x.b".attr === "y.b".attr)
    val originalQuery = x.join(y, LeftOuter, joinCondition).select("x.a".attr).limit(5)
    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer =
      Limit(5, LocalLimit(5, x).join(y, LeftOuter, joinCondition).select("x.a".attr)).analyze
    comparePlans(optimized, correctAnswer)
  }

  test("SPARK-36183: Push down limit 1 through Aggregate if it is group only") {
    // Push down when it is group only and limit 1.
    comparePlans(
      Optimize.execute(x.groupBy("x.a".attr)("x.a".attr).limit(1).analyze),
      LocalLimit(1, x).select("x.a".attr).limit(1).analyze)

    comparePlans(
      Optimize.execute(x.groupBy("x.a".attr)("x.a".attr).select("x.a".attr).limit(1).analyze),
      LocalLimit(1, x).select("x.a".attr).select("x.a".attr).limit(1).analyze)

    comparePlans(
      Optimize.execute(x.union(y).groupBy("x.a".attr)("x.a".attr).limit(1).analyze),
      LocalLimit(1, LocalLimit(1, x).union(LocalLimit(1, y))).select("x.a".attr).limit(1).analyze)

    comparePlans(
      Optimize.execute(
        x.groupBy("x.a".attr)("x.a".attr)
          .select("x.a".attr.as("a1"), "x.a".attr.as("a2")).limit(1).analyze),
      LocalLimit(1, x).select("x.a".attr)
        .select("x.a".attr.as("a1"), "x.a".attr.as("a2")).limit(1).analyze)

    // No push down
    comparePlans(
      Optimize.execute(x.groupBy("x.a".attr)("x.a".attr).limit(2).analyze),
      x.groupBy("x.a".attr)("x.a".attr).limit(2).analyze)

    comparePlans(
      Optimize.execute(x.groupBy("x.a".attr)("x.a".attr, count("x.a".attr)).limit(1).analyze),
      x.groupBy("x.a".attr)("x.a".attr, count("x.a".attr)).limit(1).analyze)
  }

  test("Push down limit 1 through Offset") {
    comparePlans(
      Optimize.execute(testRelation.offset(2).limit(1).analyze),
      GlobalLimit(1, Offset(2, LocalLimit(3, testRelation))).analyze)
  }

  test("SPARK-39511: Push limit 1 to right side if join type is LeftSemiOrAnti") {
    Seq(LeftSemi, LeftAnti).foreach { joinType =>
      comparePlans(
        Optimize.execute(x.join(y, joinType).analyze),
        x.join(LocalLimit(1, y), joinType).analyze)
    }

    Seq(LeftSemi, LeftAnti).foreach { joinType =>
      comparePlans(
        Optimize.execute(x.join(y.limit(2), joinType).analyze),
        x.join(LocalLimit(1, y), joinType).analyze)
    }

    Seq(LeftSemi, LeftAnti).foreach { joinType =>
      val originalQuery1 = x.join(LocalLimit(1, y), joinType).analyze
      val originalQuery2 = x.join(y.limit(1), joinType).analyze

      comparePlans(Optimize.execute(originalQuery1), originalQuery1)
      comparePlans(Optimize.execute(originalQuery2), originalQuery2)
    }
  }
}
