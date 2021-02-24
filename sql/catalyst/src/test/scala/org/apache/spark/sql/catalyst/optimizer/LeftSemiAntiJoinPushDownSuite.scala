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
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.IntegerType

class LeftSemiPushdownSuite extends PlanTest {

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("Subqueries", Once,
        EliminateSubqueryAliases) ::
      Batch("Filter Pushdown", FixedPoint(10),
        CombineFilters,
        PushPredicateThroughNonJoin,
        PushDownLeftSemiAntiJoin,
        PushLeftSemiLeftAntiThroughJoin,
        BooleanSimplification,
        CollapseProject) :: Nil
  }

  val testRelation = LocalRelation("a".attr.int, "b".attr.int, "c".attr.int)
  val testRelation1 = LocalRelation("d".attr.int)
  val testRelation2 = LocalRelation("e".attr.int)

  test("Project: LeftSemiAnti join pushdown") {
    val originalQuery = testRelation
      .select(star())
      .join(testRelation1, joinType = LeftSemi, condition = Some("b".attr === "d".attr))

    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer = testRelation
      .join(testRelation1, joinType = LeftSemi, condition = Some("b".attr === "d".attr))
      .select("a".attr, "b".attr, "c".attr)
      .analyze
    comparePlans(optimized, correctAnswer)
  }

  test("Project: LeftSemiAnti join no pushdown because of non-deterministic proj exprs") {
    val originalQuery = testRelation
      .select(Rand(1), "b".attr, "c".attr)
      .join(testRelation1, joinType = LeftSemi, condition = Some("b".attr === "d".attr))

    val optimized = Optimize.execute(originalQuery.analyze)
    comparePlans(optimized, originalQuery.analyze)
  }

  test("Project: LeftSemiAnti join non correlated scalar subq") {
    val subq = ScalarSubquery(testRelation.groupBy("b".attr)(sum("c".attr).as("sum")).analyze)
    val originalQuery = testRelation
      .select(subq.as("sum"))
      .join(testRelation1, joinType = LeftSemi, condition = Some("sum".attr === "d".attr))

    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer = testRelation
      .join(testRelation1, joinType = LeftSemi, condition = Some(subq === "d".attr))
      .select(subq.as("sum"))
      .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("Project: LeftSemiAnti join no pushdown - correlated scalar subq in projection list") {
    val testRelation2 = LocalRelation("e".attr.int, "f".attr.int)
    val subqPlan =
      testRelation2.groupBy("e".attr)(sum("f".attr).as("sum")).where("e".attr === "a".attr)
    val subqExpr = ScalarSubquery(subqPlan)
    val originalQuery = testRelation
      .select(subqExpr.as("sum"))
      .join(testRelation1, joinType = LeftSemi, condition = Some("sum".attr === "d".attr))

    val optimized = Optimize.execute(originalQuery.analyze)
    comparePlans(optimized, originalQuery.analyze)
  }

  test("Aggregate: LeftSemiAnti join pushdown") {
    val originalQuery = testRelation
      .groupBy("b".attr)("b".attr, sum("c".attr))
      .join(testRelation1, joinType = LeftSemi, condition = Some("b".attr === "d".attr))

    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer = testRelation
      .join(testRelation1, joinType = LeftSemi, condition = Some("b".attr === "d".attr))
      .groupBy("b".attr)("b".attr, sum("c".attr))
      .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("Aggregate: LeftSemiAnti join no pushdown due to non-deterministic aggr expressions") {
    val originalQuery = testRelation
      .groupBy("b".attr)("b".attr, Rand(10).as("c"))
      .join(testRelation1, joinType = LeftSemi, condition = Some("b".attr === "d".attr))

    val optimized = Optimize.execute(originalQuery.analyze)
    comparePlans(optimized, originalQuery.analyze)
  }

  test("Aggregate: LeftSemi join partial pushdown") {
    val originalQuery = testRelation
      .groupBy("b".attr)("b".attr, sum("c".attr).as("sum"))
      .join(testRelation1, joinType = LeftSemi,
        condition = Some("b".attr === "d".attr && "sum".attr === 10))

    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer = testRelation
      .join(testRelation1, joinType = LeftSemi, condition = Some("b".attr === "d".attr))
      .groupBy("b".attr)("b".attr, sum("c".attr).as("sum"))
      .where("sum".attr === 10)
      .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("Aggregate: LeftAnti join no pushdown") {
    val originalQuery = testRelation
      .groupBy("b".attr)("b".attr, sum("c".attr).as("sum"))
      .join(testRelation1, joinType = LeftAnti,
        condition = Some("b".attr === "d".attr && "sum".attr === 10))

    val optimized = Optimize.execute(originalQuery.analyze)
    comparePlans(optimized, originalQuery.analyze)
  }

  test("LeftSemiAnti join over aggregate - no pushdown") {
    val originalQuery = testRelation
      .groupBy("b".attr)("b".attr, sum("c".attr).as("sum"))
      .join(testRelation1, joinType = LeftSemi,
        condition = Some("b".attr === "d".attr && "sum".attr === "d".attr))

    val optimized = Optimize.execute(originalQuery.analyze)
    comparePlans(optimized, originalQuery.analyze)
  }

  test("Aggregate: LeftSemiAnti join non-correlated scalar subq aggr exprs") {
    val subq = ScalarSubquery(testRelation.groupBy("b".attr)(sum("c".attr).as("sum")).analyze)
    val originalQuery = testRelation
      .groupBy("a".attr) ("a".attr, subq.as("sum"))
      .join(testRelation1, joinType = LeftSemi,
        condition = Some("sum".attr === "d".attr && "a".attr === "d".attr))

    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer = testRelation
      .join(testRelation1, joinType = LeftSemi,
        condition = Some(subq === "d".attr && "a".attr === "d".attr))
      .groupBy("a".attr) ("a".attr, subq.as("sum"))
      .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("LeftSemiAnti join over Window") {
    val winExpr = windowExpr(count("b".attr),
      windowSpec("a".attr :: Nil, "b".attr.asc :: Nil, UnspecifiedFrame))

    val originalQuery = testRelation
      .select("a".attr, "b".attr, "c".attr, winExpr.as("window"))
      .join(testRelation1, joinType = LeftSemi, condition = Some("a".attr === "d".attr))

    val optimized = Optimize.execute(originalQuery.analyze)

    val correctAnswer = testRelation
      .join(testRelation1, joinType = LeftSemi, condition = Some("a".attr === "d".attr))
      .select("a".attr, "b".attr, "c".attr)
      .window(winExpr.as("window") :: Nil, "a".attr :: Nil, "b".attr.asc :: Nil)
      .select("a".attr, "b".attr, "c".attr, "window".attr).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("Window: LeftSemi partial pushdown") {
    // Attributes from join condition which does not refer to the window partition spec
    // are kept up in the plan as a Filter operator above Window.
    val winExpr =
    windowExpr(count("b".attr), windowSpec("a".attr :: Nil, "b".attr.asc :: Nil, UnspecifiedFrame))

    val originalQuery = testRelation
      .select("a".attr, "b".attr, "c".attr, winExpr.as("window"))
      .join(testRelation1, joinType = LeftSemi,
        condition = Some("a".attr === "d".attr && "b".attr > 5))

    val optimized = Optimize.execute(originalQuery.analyze)

    val correctAnswer = testRelation
      .join(testRelation1, joinType = LeftSemi, condition = Some("a".attr === "d".attr))
      .select("a".attr, "b".attr, "c".attr)
      .window(winExpr.as("window") :: Nil, "a".attr :: Nil, "b".attr.asc :: Nil)
      .where("b".attr > 5)
      .select("a".attr, "b".attr, "c".attr, "window".attr).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("Window: LeftAnti no pushdown") {
    // Attributes from join condition which does not refer to the window partition spec
    // are kept up in the plan as a Filter operator above Window.
    val winExpr =
    windowExpr(count("b".attr), windowSpec("a".attr :: Nil, "b".attr.asc :: Nil, UnspecifiedFrame))

    val originalQuery = testRelation
      .select("a".attr, "b".attr, "c".attr, winExpr.as("window"))
      .join(testRelation1, joinType = LeftAnti,
        condition = Some("a".attr === "d".attr && "b".attr > 5))

    val optimized = Optimize.execute(originalQuery.analyze)

    val correctAnswer = testRelation
      .select("a".attr, "b".attr, "c".attr)
      .window(winExpr.as("window") :: Nil, "a".attr :: Nil, "b".attr.asc :: Nil)
      .join(testRelation1, joinType = LeftAnti,
        condition = Some("a".attr === "d".attr && "b".attr > 5))
      .select("a".attr, "b".attr, "c".attr, "window".attr).analyze
    comparePlans(optimized, correctAnswer)
  }

  test("Union: LeftSemiAnti join pushdown") {
    val testRelation2 = LocalRelation("x".attr.int, "y".attr.int, "z".attr.int)

    val originalQuery = Union(Seq(testRelation, testRelation2))
      .join(testRelation1, joinType = LeftSemi, condition = Some("a".attr === "d".attr))

    val optimized = Optimize.execute(originalQuery.analyze)

    val correctAnswer = Union(Seq(
      testRelation.join(testRelation1, joinType = LeftSemi,
        condition = Some("a".attr === "d".attr)),
      testRelation2.join(testRelation1, joinType = LeftSemi,
        condition = Some("x".attr === "d".attr))))
      .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("Union: LeftSemiAnti join no pushdown in self join scenario") {
    val testRelation2 = LocalRelation("x".attr.int, "y".attr.int, "z".attr.int)

    val originalQuery = Union(Seq(testRelation, testRelation2))
      .join(testRelation2, joinType = LeftSemi, condition = Some("a".attr === "x".attr))

    val optimized = Optimize.execute(originalQuery.analyze)
    comparePlans(optimized, originalQuery.analyze)
  }

  test("Unary: LeftSemiAnti join pushdown") {
    val originalQuery = testRelation
      .select(star())
      .repartition(1)
      .join(testRelation1, joinType = LeftSemi, condition = Some("b".attr === "d".attr))

    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer = testRelation
      .join(testRelation1, joinType = LeftSemi, condition = Some("b".attr === "d".attr))
      .select("a".attr, "b".attr, "c".attr)
      .repartition(1)
      .analyze
    comparePlans(optimized, correctAnswer)
  }

  test("Unary: LeftSemiAnti join pushdown - empty join condition") {
    val originalQuery = testRelation
      .select(star())
      .repartition(1)
      .join(testRelation1, joinType = LeftSemi, condition = None)

    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer = testRelation
      .join(testRelation1, joinType = LeftSemi, condition = None)
      .select("a".attr, "b".attr, "c".attr)
      .repartition(1)
      .analyze
    comparePlans(optimized, correctAnswer)
  }

  test("Unary: LeftSemi join pushdown - partial pushdown") {
    val testRelationWithArrayType =
      LocalRelation("a".attr.int, "b".attr.int, "c_arr".attr.array(IntegerType))
    val originalQuery = testRelationWithArrayType
      .generate(Explode("c_arr".attr), alias = Some("arr"), outputNames = Seq("out_col"))
      .join(testRelation1, joinType = LeftSemi,
        condition = Some("b".attr === "d".attr && "b".attr === "out_col".attr))

    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer = testRelationWithArrayType
      .join(testRelation1, joinType = LeftSemi, condition = Some("b".attr === "d".attr))
      .generate(Explode("c_arr".attr), alias = Some("arr"), outputNames = Seq("out_col"))
      .where("b".attr === "out_col".attr)
      .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("Unary: LeftAnti join pushdown - no pushdown") {
    val testRelationWithArrayType =
      LocalRelation("a".attr.int, "b".attr.int, "c_arr".attr.array(IntegerType))
    val originalQuery = testRelationWithArrayType
      .generate(Explode("c_arr".attr), alias = Some("arr"), outputNames = Seq("out_col"))
      .join(testRelation1, joinType = LeftAnti,
        condition = Some("b".attr === "d".attr && "b".attr === "out_col".attr))

    val optimized = Optimize.execute(originalQuery.analyze)
    comparePlans(optimized, originalQuery.analyze)
  }

  test("Unary: LeftSemiAnti join pushdown - no pushdown") {
    val testRelationWithArrayType =
      LocalRelation("a".attr.int, "b".attr.int, "c_arr".attr.array(IntegerType))
    val originalQuery = testRelationWithArrayType
      .generate(Explode("c_arr".attr), alias = Some("arr"), outputNames = Seq("out_col"))
      .join(testRelation1, joinType = LeftSemi,
        condition = Some("b".attr === "d".attr && "d".attr === "out_col".attr))

    val optimized = Optimize.execute(originalQuery.analyze)
    comparePlans(optimized, originalQuery.analyze)
  }

  test("Unary: LeftSemi join push down through Expand") {
    val expand = Expand(Seq(Seq("a".attr, "b".attr, "null"), Seq("a".attr, "null", "c".attr)),
      Seq("a".attr, "b".attr, "c".attr), testRelation)
    val originalQuery = expand
      .join(testRelation1, joinType = LeftSemi,
        condition = Some("b".attr === "d".attr && "b".attr === 1))

    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer =
      Expand(Seq(Seq("a".attr, "b".attr, "null"), Seq("a".attr, "null", "c".attr)),
      Seq("a".attr, "b".attr, "c".attr), testRelation
        .join(testRelation1, joinType = LeftSemi,
          condition = Some("b".attr === "d".attr && "b".attr === 1)))
      .analyze

    comparePlans(optimized, correctAnswer)
  }

  Seq(Some("d".attr === "e".attr), None).foreach { case innerJoinCond =>
    Seq(LeftSemi, LeftAnti).foreach { case outerJT =>
      Seq(Inner, LeftOuter, Cross, RightOuter).foreach { case innerJT =>
        test(s"$outerJT pushdown empty join cond join type $innerJT join cond $innerJoinCond") {
          val joinedRelation = testRelation1.join(testRelation2, joinType = innerJT, innerJoinCond)
          val originalQuery = joinedRelation.join(testRelation, joinType = outerJT, None)
          val optimized = Optimize.execute(originalQuery.analyze)

          val correctAnswer = if (innerJT == RightOuter) {
            val pushedDownJoin = testRelation2.join(testRelation, joinType = outerJT, None)
            testRelation1.join(pushedDownJoin, joinType = innerJT, innerJoinCond).analyze
          } else {
            val pushedDownJoin = testRelation1.join(testRelation, joinType = outerJT, None)
            pushedDownJoin.join(testRelation2, joinType = innerJT, innerJoinCond).analyze
          }
          comparePlans(optimized, correctAnswer)
        }
      }
    }
  }

  Seq(Some("d".attr === "e".attr), None).foreach { case innerJoinCond =>
    Seq(LeftSemi, LeftAnti).foreach { case outerJT =>
      Seq(Inner, LeftOuter, Cross).foreach { case innerJT =>
        test(s"$outerJT pushdown to left of join type: $innerJT join condition $innerJoinCond") {
          val joinedRelation = testRelation1.join(testRelation2, joinType = innerJT, innerJoinCond)
          val originalQuery =
            joinedRelation.join(testRelation, joinType = outerJT,
              condition = Some("a".attr === "d".attr))
          val optimized = Optimize.execute(originalQuery.analyze)

          val pushedDownJoin =
            testRelation1.join(testRelation, joinType = outerJT,
              condition = Some("a".attr === "d".attr))
          val correctAnswer =
            pushedDownJoin.join(testRelation2, joinType = innerJT, innerJoinCond).analyze
          comparePlans(optimized, correctAnswer)
        }
      }
    }
  }

  Seq(Some("e".attr === "d".attr), None).foreach { case innerJoinCond =>
    Seq(LeftSemi, LeftAnti).foreach { case outerJT =>
      Seq(Inner, RightOuter, Cross).foreach { case innerJT =>
        test(s"$outerJT pushdown to right of join type: $innerJT join condition $innerJoinCond") {
          val joinedRelation = testRelation1.join(testRelation2, joinType = innerJT, innerJoinCond)
          val originalQuery =
            joinedRelation.join(testRelation, joinType = outerJT,
              condition = Some("a".attr === "e".attr))
          val optimized = Optimize.execute(originalQuery.analyze)

          val pushedDownJoin =
            testRelation2.join(testRelation, joinType = outerJT,
              condition = Some("a".attr === "e".attr))
          val correctAnswer =
            testRelation1.join(pushedDownJoin, joinType = innerJT, innerJoinCond).analyze
          comparePlans(optimized, correctAnswer)
        }
      }
    }
  }

  Seq(LeftSemi, LeftAnti).foreach { case jt =>
    test(s"$jt no pushdown - join condition refers left leg - join type for RightOuter") {
      val joinedRelation = testRelation1.join(testRelation2, joinType = RightOuter, None)
      val originalQuery =
        joinedRelation.join(testRelation, joinType = jt, condition = Some("a".attr === "d".attr))
      val optimized = Optimize.execute(originalQuery.analyze)
      comparePlans(optimized, originalQuery.analyze)
    }
  }

  Seq(LeftSemi, LeftAnti).foreach { case jt =>
    test(s"$jt no pushdown - join condition refers right leg - join type for LeftOuter") {
      val joinedRelation = testRelation1.join(testRelation2, joinType = LeftOuter, None)
      val originalQuery =
        joinedRelation.join(testRelation, joinType = jt, condition = Some("a".attr === "e".attr))
      val optimized = Optimize.execute(originalQuery.analyze)
      comparePlans(optimized, originalQuery.analyze)
    }
  }

  Seq(LeftSemi, LeftAnti).foreach { case outerJT =>
    Seq(Inner, LeftOuter, RightOuter, Cross).foreach { case innerJT =>
      test(s"$outerJT no pushdown - join condition refers both leg - join type $innerJT") {
        val joinedRelation = testRelation1.join(testRelation2, joinType = innerJT, None)
        val originalQuery = joinedRelation
          .join(testRelation, joinType = outerJT,
            condition = Some("a".attr === "d".attr && "a".attr === "e".attr))
        val optimized = Optimize.execute(originalQuery.analyze)
        comparePlans(optimized, originalQuery.analyze)
      }
    }
  }

  Seq(LeftSemi, LeftAnti).foreach { case outerJT =>
    Seq(Inner, LeftOuter, RightOuter, Cross).foreach { case innerJT =>
      test(s"$outerJT no pushdown - join condition refers none of the leg - join type $innerJT") {
        val joinedRelation = testRelation1.join(testRelation2, joinType = innerJT, None)
        val originalQuery = joinedRelation
          .join(testRelation, joinType = outerJT,
            condition = Some("d".attr + "e".attr === "a".attr))
        val optimized = Optimize.execute(originalQuery.analyze)
        comparePlans(optimized, originalQuery.analyze)
      }
    }
  }

  Seq(LeftSemi, LeftAnti).foreach { case jt =>
    test(s"$jt no pushdown when child join type is FullOuter") {
      val joinedRelation = testRelation1.join(testRelation2, joinType = FullOuter, None)
      val originalQuery =
        joinedRelation.join(testRelation, joinType = jt, condition = Some("a".attr === "e".attr))
      val optimized = Optimize.execute(originalQuery.analyze)
      comparePlans(optimized, originalQuery.analyze)
    }
  }

  Seq(LeftSemi, LeftAnti).foreach { jt =>
    test(s"SPARK-34081: $jt only push down if join can be planned as broadcast join") {
      Seq(-1, 100000).foreach { threshold =>
        withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> threshold.toString) {
          val originalQuery = testRelation
            .groupBy("b".attr)("b".attr)
            .join(testRelation1, joinType = jt, condition = Some("b".attr <=> "d".attr))

          val optimized = Optimize.execute(originalQuery.analyze)
          val correctAnswer = if (threshold > 0) {
            testRelation
              .join(testRelation1, joinType = jt, condition = Some("b".attr <=> "d".attr))
              .groupBy("b".attr)("b".attr)
              .analyze
          } else {
            originalQuery.analyze
          }

          comparePlans(optimized, correctAnswer)
        }
      }
    }
  }

}
