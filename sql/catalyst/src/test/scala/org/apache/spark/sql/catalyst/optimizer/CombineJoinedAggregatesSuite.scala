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

import org.scalactic.source.Position
import org.scalatest.Tag

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.plans.{Cross, FullOuter, Inner, LeftAnti, LeftOuter, LeftSemi, PlanTest, RightOuter}
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.internal.SQLConf

class CombineJoinedAggregatesSuite extends PlanTest {
  private object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("Eliminate Join By Combine Aggregate", FixedPoint(10),
        CollapseProject,
        RemoveNoopOperators,
        PushDownPredicates,
        CombineJoinedAggregates,
        BooleanSimplification) :: Nil
  }

  private object WithoutOptimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("Eliminate Join By Combine Aggregate", FixedPoint(10),
        CollapseProject,
        RemoveNoopOperators,
        PushDownPredicates,
        BooleanSimplification) :: Nil
  }

  private val testRelation = LocalRelation.fromExternalRows(
    Seq("a".attr.int, "b".attr.int, "c".attr.int),
    1.to(6).map(i => Row(i, 2 * i, 3 * i)))
  private val a = testRelation.output(0)
  private val b = testRelation.output(1)
  private val c = testRelation.output(2)

  override def test(testName: String, testTags: Tag*)(testFun: => Any)
    (implicit pos: Position): Unit = {
    super.test(testName, testTags: _*) {
      withSQLConf(SQLConf.COMBINE_JOINED_AGGREGATES_ENABLED.key -> "true") {
        testFun
      }
    }
  }

  test("join type is unsupported") {
    Seq(LeftSemi, LeftAnti).foreach { joinType =>
      val originalQuery1 =
        testRelation.where(a === 1).groupBy()(sum(b).as("sum_b")).join(
          testRelation.where(a === 2).groupBy()(sum(b).as("sum_b")), joinType)

      comparePlans(
        Optimize.execute(originalQuery1.analyze),
        WithoutOptimize.execute(originalQuery1.analyze))

      val originalQuery2 =
        testRelation.where(a === 1).groupBy()(sum(b).as("sum_b")).join(
          testRelation.where(a === 2).groupBy()(avg(b).as("avg_b")), joinType).join(
          testRelation.where(a === 3).groupBy()(count(b).as("count_b")))

      comparePlans(
        Optimize.execute(originalQuery2.analyze),
        WithoutOptimize.execute(originalQuery2.analyze))
    }
  }

  test("join with condition is unsupported") {
    Seq(Inner, Cross, LeftOuter, RightOuter, FullOuter).foreach { joinType =>
      val originalQuery1 =
        testRelation.where(a === 1).groupBy()(sum(b).as("sum_b")).as("left").join(
          testRelation.where(a === 2).groupBy()(sum(b).as("sum_b")).as("right"),
          joinType, Some($"left.sum_b" === $"right.sum_b"))

      comparePlans(
        Optimize.execute(originalQuery1.analyze),
        WithoutOptimize.execute(originalQuery1.analyze))

      val originalQuery2 =
        testRelation.where(a === 1).groupBy()(sum(b).as("sum_b")).as("left").join(
          testRelation.where(a === 2).groupBy()(avg(b).as("avg_b")).as("right"),
          joinType, Some($"left.sum_b" === $"right.avg_b")).join(
          testRelation.where(a === 3).groupBy()(count(b).as("count_b")))

      comparePlans(
        Optimize.execute(originalQuery2.analyze),
        WithoutOptimize.execute(originalQuery2.analyze))
    }
  }

  test("join side doesn't contains Aggregate") {
    val originalQuery1 =
      testRelation.where(a === 1).join(
        testRelation.where(a === 2))

    comparePlans(
      Optimize.execute(originalQuery1.analyze),
      WithoutOptimize.execute(originalQuery1.analyze))

    val originalQuery2 =
      testRelation.where(a === 1).select(b, c).join(
        testRelation.where(a === 2).select(b, c))

    comparePlans(
      Optimize.execute(originalQuery2.analyze),
      WithoutOptimize.execute(originalQuery2.analyze))
  }

  test("join side is not Aggregate") {
    val originalQuery1 =
      testRelation.where(a === 1).groupBy()(sum(b).as("sum_b")).join(
        testRelation.where(a === 2))

    comparePlans(
      Optimize.execute(originalQuery1.analyze),
      WithoutOptimize.execute(originalQuery1.analyze))

    val originalQuery2 =
      testRelation.where(a === 1).groupBy()(sum(b).as("sum_b"), avg(b).as("avg_b")).join(
        testRelation.where(a === 2).select(b, c))

    comparePlans(
      Optimize.execute(originalQuery2.analyze),
      WithoutOptimize.execute(originalQuery2.analyze))

    // Nested Join
    val originalQuery3 =
      testRelation.where(a === 1).groupBy()(sum(b).as("sum_b")).join(
        testRelation.where(a === 2)).join(
        testRelation.where(a === 3).groupBy()(count(b).as("count_b")))

    comparePlans(
      Optimize.execute(originalQuery3.analyze),
      WithoutOptimize.execute(originalQuery3.analyze))

    val originalQuery4 =
      testRelation.where(a === 1).groupBy()(sum(b).as("sum_b")).join(
        testRelation.where(a === 2).join(
          testRelation.where(a === 3).groupBy()(count(b).as("count_b"))))

    comparePlans(
      Optimize.execute(originalQuery4.analyze),
      WithoutOptimize.execute(originalQuery4.analyze))
  }

  test("join side contains Aggregate with group by clause") {
    val originalQuery1 =
      testRelation.where(a === 1).groupBy(c)(sum(b).as("sum_b")).join(
        testRelation.where(a === 2).groupBy()(sum(b).as("sum_b")))

    comparePlans(
      Optimize.execute(originalQuery1.analyze),
      WithoutOptimize.execute(originalQuery1.analyze))

    val originalQuery2 =
      testRelation.where(a === 1).groupBy()(sum(b).as("sum_b")).join(
        testRelation.where(a === 2).groupBy(c)(sum(b).as("sum_b")))

    comparePlans(
      Optimize.execute(originalQuery2.analyze),
      WithoutOptimize.execute(originalQuery2.analyze))

    val originalQuery3 =
      testRelation.where(a === 1).groupBy(c)(sum(b).as("sum_b")).join(
        testRelation.where(a === 2).groupBy(c)(sum(b).as("sum_b")))

    comparePlans(
      Optimize.execute(originalQuery3.analyze),
      WithoutOptimize.execute(originalQuery3.analyze))

    val originalQuery4 =
      testRelation.where(a === 1).groupBy(c)(sum(b).as("sum_b")).join(
        testRelation.where(a === 2).groupBy()(avg(b).as("avg_b"))).join(
        testRelation.where(a === 3).groupBy()(count(b).as("count_b")))

    comparePlans(
      Optimize.execute(originalQuery4.analyze),
      WithoutOptimize.execute(originalQuery4.analyze))
  }

  test("join two side are Aggregates with subquery") {
    val subQuery1 = testRelation.where(a === 1).as("tab1")
    val subQuery2 = testRelation.where(a === 2).as("tab2")
    val b1 = subQuery1.output(1)
    val c1 = subQuery1.output(2)
    val b2 = subQuery2.output(1)
    val c2 = subQuery2.output(2)
    val originalQuery =
      subQuery1.where(c1 === 1).groupBy()(sum(b1).as("sum_b")).join(
        subQuery2.where(c2 === 2).groupBy()(avg(b2).as("avg_b")))

    val correctAnswer =
      testRelation.where((a === 1 && c === 1) || (a === 2 && c === 2)).groupBy()(
        sum(b, Some(a === 1 && c === 1)).as("sum_b"),
        avg(b, Some(a === 2 && c === 2)).as("avg_b"))

    comparePlans(
      Optimize.execute(originalQuery.analyze),
      WithoutOptimize.execute(correctAnswer.analyze))
  }

  test("join two side are Aggregates and only one side with Filter") {
    val originalQuery =
      testRelation.where(a === 1).groupBy()(sum(b).as("sum_b")).join(
        testRelation.groupBy()(sum(b).as("sum_b")))

    comparePlans(
      Optimize.execute(originalQuery.analyze),
      WithoutOptimize.execute(originalQuery.analyze))
  }

  test("join two side are Aggregates without Filter") {
    val originalQuery =
      testRelation.groupBy()(sum(b).as("sum_b")).join(
        testRelation.groupBy()(sum(b).as("sum_b")))

    val correctAnswer = testRelation.groupBy()(sum(b).as("sum_b"), sum(b).as("sum_b"))

    comparePlans(
      Optimize.execute(originalQuery.analyze),
      WithoutOptimize.execute(correctAnswer.analyze))
  }

  test("join two side are Aggregates with Filter") {
    val originalQuery1 =
      testRelation.where(a === 1).groupBy()(sum(b).as("sum_b")).join(
        testRelation.where(a === 2).groupBy()(sum(b).as("sum_b")))

    val correctAnswer1 =
      testRelation.where(a === 1 || a === 2).groupBy()(
        sum(b, Some(a === 1)).as("sum_b"), sum(b, Some(a === 2)).as("sum_b"))

    comparePlans(
      Optimize.execute(originalQuery1.analyze),
      WithoutOptimize.execute(correctAnswer1.analyze))

    val originalQuery2 =
      testRelation.where(a === 1).groupBy()(sum(b).as("sum_b")).join(
        testRelation.where(a === 2).groupBy()(avg(b).as("avg_b")))

    val correctAnswer2 =
      testRelation.where(a === 1 || a === 2).groupBy()(
        sum(b, Some(a === 1)).as("sum_b"), avg(b, Some(a === 2)).as("avg_b"))

    comparePlans(
      Optimize.execute(originalQuery2.analyze),
      WithoutOptimize.execute(correctAnswer2.analyze))

    val originalQuery3 =
      testRelation.where(a === 1).groupBy()(sum(b).as("sum_b")).join(
        testRelation.where(a === 2).groupBy()(avg(c).as("avg_c")))

    val correctAnswer3 =
      testRelation.where(a === 1 || a === 2).groupBy()(
        sum(b, Some(a === 1)).as("sum_b"), avg(c, Some(a === 2)).as("avg_c"))

    comparePlans(
      Optimize.execute(originalQuery3.analyze),
      WithoutOptimize.execute(correctAnswer3.analyze))

    val originalQuery4 =
      testRelation.where(a === 1).groupBy()(sum(b).as("sum_b"), count(c).as("count_c")).join(
        testRelation.where(a === 2).groupBy()(avg(c).as("avg_c")))

    val correctAnswer4 =
      testRelation.where(a === 1 || a === 2).groupBy()(
        sum(b, Some(a === 1)).as("sum_b"),
        count(c, Some(a === 1)).as("count_c"),
        avg(c, Some(a === 2)).as("avg_c"))

    comparePlans(
      Optimize.execute(originalQuery4.analyze),
      WithoutOptimize.execute(correctAnswer4.analyze))

    val originalQuery5 =
      testRelation.where(a === 1).groupBy()(sum(b).as("sum_b")).join(
        testRelation.where(a === 2).groupBy()(avg(c).as("avg_c"), count(c).as("count_c")))

    val correctAnswer5 =
      testRelation.where(a === 1 || a === 2).groupBy()(
        sum(b, Some(a === 1)).as("sum_b"),
        avg(c, Some(a === 2)).as("avg_c"),
        count(c, Some(a === 2)).as("count_c"))

    comparePlans(
      Optimize.execute(originalQuery5.analyze),
      WithoutOptimize.execute(correctAnswer5.analyze))
  }

  test("all side of nested join are Aggregates") {
    val originalQuery1 =
      testRelation.where(a === 1).groupBy()(sum(b).as("sum_b")).join(
        testRelation.where(a === 2).groupBy()(avg(b).as("avg_b"))).join(
        testRelation.where(a === 3).groupBy()(count(b).as("count_b")))

    val correctAnswer1 =
      testRelation.where(a === 1 || a === 2 || a === 3).groupBy()(
        sum(b, Some(a === 1)).as("sum_b"),
        avg(b, Some(a === 2)).as("avg_b"),
        count(b, Some(a === 3)).as("count_b"))

    comparePlans(
      Optimize.execute(originalQuery1.analyze),
      WithoutOptimize.execute(correctAnswer1.analyze))

    val originalQuery2 =
      testRelation.where(a === 1).groupBy()(sum(b).as("sum_b")).join(
        testRelation.where(a === 2).groupBy()(avg(b).as("avg_b")).join(
          testRelation.where(a === 3).groupBy()(count(b).as("count_b"))))

    val correctAnswer2 =
      testRelation.where(a === 1 || (a === 2 || a === 3)).groupBy()(
        sum(b, Some(a === 1)).as("sum_b"),
        avg(b, Some(a === 2)).as("avg_b"),
        count(b, Some(a === 3)).as("count_b"))

    comparePlans(
      Optimize.execute(originalQuery2.analyze),
      WithoutOptimize.execute(correctAnswer2.analyze))

    val originalQuery3 =
      testRelation.where(a === 1).groupBy()(avg(a).as("avg_a"), sum(b).as("sum_b")).join(
        testRelation.where(a === 2).groupBy()(avg(b).as("avg_b"), sum(a).as("sum_a")).join(
          testRelation.where(a === 3).groupBy()(
            count(a).as("count_a"),
            count(b).as("count_b"),
            count(c).as("count_c"))))

    val correctAnswer3 =
      testRelation.where(a === 1 || (a === 2 || a === 3)).groupBy()(
        avg(a, Some(a === 1)).as("avg_a"),
        sum(b, Some(a === 1)).as("sum_b"),
        avg(b, Some(a === 2)).as("avg_b"),
        sum(a, Some(a === 2)).as("sum_a"),
        count(a, Some(a === 3)).as("count_a"),
        count(b, Some(a === 3)).as("count_b"),
        count(c, Some(a === 3)).as("count_c"))

    comparePlans(
      Optimize.execute(originalQuery3.analyze),
      WithoutOptimize.execute(correctAnswer3.analyze))

    val originalQuery4 =
      testRelation.where(a === 1).groupBy()(sum(b).as("sum_b")).join(
        testRelation.where(a === 2).groupBy()(avg(b).as("avg_b"))).join(
        testRelation.where(a === 3).groupBy()(countDistinct(b).as("count_distinct_b")))

    val correctAnswer4 =
      testRelation.where(a === 1 || a === 2 || a === 3).groupBy()(
        sum(b, Some(a === 1)).as("sum_b"),
        avg(b, Some(a === 2)).as("avg_b"),
        countDistinctWithFilter(a === 3, b).as("count_distinct_b"))

    comparePlans(
      Optimize.execute(originalQuery4.analyze),
      WithoutOptimize.execute(correctAnswer4.analyze))
  }

  test("join two side are Aggregates and aggregate expressions exist Filter clause") {
    val originalQuery1 =
      testRelation.where(a === 1).groupBy()(sum(b, Some(c === 1)).as("sum_b")).join(
        testRelation.where(a === 2).groupBy()(avg(b).as("avg_b")))

    val correctAnswer1 =
      testRelation.where(a === 1 || a === 2).groupBy()(
        sum(b, Some((a === 1) && (c === 1))).as("sum_b"),
        avg(b, Some(a === 2)).as("avg_b"))

    comparePlans(
      Optimize.execute(originalQuery1.analyze),
      WithoutOptimize.execute(correctAnswer1.analyze))

    val originalQuery2 =
      testRelation.where(a === 1).groupBy()(sum(b).as("sum_b")).join(
        testRelation.where(a === 2).groupBy()(avg(b, Some(c === 1)).as("avg_b")))

    val correctAnswer2 =
      testRelation.where(a === 1 || a === 2).groupBy()(
        sum(b, Some(a === 1)).as("sum_b"),
        avg(b, Some((a === 2) && (c === 1))).as("avg_b"))

    comparePlans(
      Optimize.execute(originalQuery2.analyze),
      WithoutOptimize.execute(correctAnswer2.analyze))

    val originalQuery3 =
      testRelation.where(a === 1).groupBy()(sum(b, Some(c === 1)).as("sum_b")).join(
        testRelation.where(a === 2).groupBy()(avg(b, Some(c === 1)).as("avg_b")))

    val correctAnswer3 =
      testRelation.where(a === 1 || a === 2).groupBy()(
        sum(b, Some((a === 1) && (c === 1))).as("sum_b"),
        avg(b, Some((a === 2) && (c === 1))).as("avg_b"))

    comparePlans(
      Optimize.execute(originalQuery3.analyze),
      WithoutOptimize.execute(correctAnswer3.analyze))

    val originalQuery4 =
      testRelation.where(a === 1).groupBy()(sum(b, Some(c === 1)).as("sum_b")).join(
        testRelation.where(a === 2).groupBy()(avg(b, Some(c === 1)).as("avg_b"))).join(
        testRelation.where(a === 3).groupBy()(count(b, Some(c > 1)).as("count_b")))

    val correctAnswer4 =
      testRelation.where(a === 1 || a === 2 || a === 3).groupBy()(
        sum(b, Some(((a === 1) || (a === 2)) && ((a === 1) && (c === 1)))).as("sum_b"),
        avg(b, Some(((a === 1) || (a === 2)) && ((a === 2) && (c === 1)))).as("avg_b"),
        count(b, Some((a === 3) && (c > 1))).as("count_b"))

    comparePlans(
      Optimize.execute(originalQuery4.analyze),
      WithoutOptimize.execute(correctAnswer4.analyze))

    val originalQuery5 =
      testRelation.where(a === 1).groupBy()(sum(b).as("sum_b")).join(
        testRelation.where(a === 2).groupBy()(avg(b).as("avg_b"))).join(
        testRelation.where(a === 3).groupBy()(count(b, Some(c === 1)).as("count_b")))

    val correctAnswer5 =
      testRelation.where(a === 1 || a === 2 || a === 3).groupBy()(
        sum(b, Some(a === 1)).as("sum_b"),
        avg(b, Some(a === 2)).as("avg_b"),
        count(b, Some((a === 3) && (c === 1))).as("count_b"))

    comparePlans(
      Optimize.execute(originalQuery5.analyze),
      WithoutOptimize.execute(correctAnswer5.analyze))
  }

  test("upstream join could be optimized") {
    Seq(LeftSemi, LeftAnti).foreach { joinType =>
      val originalQuery1 =
        testRelation.where(a === 1).groupBy()(sum(b).as("sum_b")).join(
          testRelation.where(a === 2).groupBy()(avg(b).as("avg_b"))).join(
          testRelation.where(a === 3).groupBy()(count(b).as("count_b")), joinType)

      val correctAnswer1 =
        testRelation.where(a === 1 || a === 2).groupBy()(
          sum(b, Some(a === 1)).as("sum_b"),
          avg(b, Some(a === 2)).as("avg_b")).join(
          testRelation.where(a === 3).groupBy()(count(b).as("count_b")), joinType)

      comparePlans(
        Optimize.execute(originalQuery1.analyze),
        WithoutOptimize.execute(correctAnswer1.analyze))
    }

    Seq(Inner, Cross, LeftOuter, RightOuter, FullOuter).foreach { joinType =>
      val originalQuery2 =
        testRelation.where(a === 1).groupBy()(sum(b).as("sum_b")).join(
          testRelation.where(a === 2).groupBy()(avg(b).as("avg_b"))).as("left").join(
          testRelation.where(a === 3).groupBy()(count(b).as("count_b")).as("right"),
          joinType, Some($"left.sum_b" === $"right.count_b"))

      val correctAnswer2 =
        testRelation.where(a === 1 || a === 2).groupBy()(
          sum(b, Some(a === 1)).as("sum_b"),
          avg(b, Some(a === 2)).as("avg_b")).as("left").join(
          testRelation.where(a === 3).groupBy()(count(b).as("count_b")).as("right"),
          joinType, Some($"left.sum_b" === $"right.count_b"))

      comparePlans(
        Optimize.execute(originalQuery2.analyze),
        WithoutOptimize.execute(correctAnswer2.analyze))
    }

    val originalQuery3 =
      testRelation.where(a === 1).groupBy()(sum(b).as("sum_b")).join(
        testRelation.where(a === 2).groupBy()(avg(b).as("avg_b"))).join(
        testRelation.where(a === 3))

    val correctAnswer3 =
      testRelation.where(a === 1 || a === 2).groupBy()(
        sum(b, Some(a === 1)).as("sum_b"),
        avg(b, Some(a === 2)).as("avg_b")).join(
        testRelation.where(a === 3))

    comparePlans(
      Optimize.execute(originalQuery3.analyze),
      WithoutOptimize.execute(correctAnswer3.analyze))

    val originalQuery4 =
      testRelation.where(a === 1).groupBy()(sum(b).as("sum_b")).join(
        testRelation.where(a === 2).groupBy()(avg(b).as("avg_b"))).join(
        testRelation.where(a === 3).groupBy(c)(count(b).as("count_b")))

    val correctAnswer4 =
      testRelation.where(a === 1 || a === 2).groupBy()(
        sum(b, Some(a === 1)).as("sum_b"),
        avg(b, Some(a === 2)).as("avg_b")).join(
        testRelation.where(a === 3).groupBy(c)(count(b).as("count_b")))

    comparePlans(
      Optimize.execute(originalQuery4.analyze),
      WithoutOptimize.execute(correctAnswer4.analyze))
  }
}
