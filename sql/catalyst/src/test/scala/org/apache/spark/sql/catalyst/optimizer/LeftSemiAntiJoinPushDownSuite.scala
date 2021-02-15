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

  val testRelation = LocalRelation(Symbol("a").int, Symbol("b").int, Symbol("c").int)
  val testRelation1 = LocalRelation(Symbol("d").int)
  val testRelation2 = LocalRelation(Symbol("e").int)

  test("Project: LeftSemiAnti join pushdown") {
    val originalQuery = testRelation
      .select(star())
      .join(testRelation1, joinType = LeftSemi, condition = Some(Symbol("b") === Symbol("d")))

    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer = testRelation
      .join(testRelation1, joinType = LeftSemi, condition = Some(Symbol("b") === Symbol("d")))
      .select(Symbol("a"), Symbol("b"), Symbol("c"))
      .analyze
    comparePlans(optimized, correctAnswer)
  }

  test("Project: LeftSemiAnti join no pushdown because of non-deterministic proj exprs") {
    val originalQuery = testRelation
      .select(Rand(1), Symbol("b"), Symbol("c"))
      .join(testRelation1, joinType = LeftSemi, condition = Some(Symbol("b") === Symbol("d")))

    val optimized = Optimize.execute(originalQuery.analyze)
    comparePlans(optimized, originalQuery.analyze)
  }

  test("Project: LeftSemiAnti join non correlated scalar subq") {
    val subq = ScalarSubquery(testRelation.groupBy(Symbol("b"))(sum(Symbol("c")).as("sum")).analyze)
    val originalQuery = testRelation
      .select(subq.as("sum"))
      .join(testRelation1, joinType = LeftSemi, condition = Some(Symbol("sum") === Symbol("d")))

    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer = testRelation
      .join(testRelation1, joinType = LeftSemi, condition = Some(subq === Symbol("d")))
      .select(subq.as("sum"))
      .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("Project: LeftSemiAnti join no pushdown - correlated scalar subq in projection list") {
    val testRelation2 = LocalRelation(Symbol("e").int, Symbol("f").int)
    val subqPlan = testRelation2.groupBy(Symbol("e"))(sum(Symbol("f")).as("sum"))
      .where(Symbol("e") === Symbol("a"))
    val subqExpr = ScalarSubquery(subqPlan)
    val originalQuery = testRelation
      .select(subqExpr.as("sum"))
      .join(testRelation1, joinType = LeftSemi, condition = Some(Symbol("sum") === Symbol("d")))

    val optimized = Optimize.execute(originalQuery.analyze)
    comparePlans(optimized, originalQuery.analyze)
  }

  test("Aggregate: LeftSemiAnti join pushdown") {
    val originalQuery = testRelation
      .groupBy(Symbol("b"))(Symbol("b"), sum(Symbol("c")))
      .join(testRelation1, joinType = LeftSemi, condition = Some(Symbol("b") === Symbol("d")))

    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer = testRelation
      .join(testRelation1, joinType = LeftSemi, condition = Some(Symbol("b") === Symbol("d")))
      .groupBy(Symbol("b"))(Symbol("b"), sum(Symbol("c")))
      .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("Aggregate: LeftSemiAnti join no pushdown due to non-deterministic aggr expressions") {
    val originalQuery = testRelation
      .groupBy(Symbol("b"))(Symbol("b"), Rand(10).as(Symbol("c")))
      .join(testRelation1, joinType = LeftSemi, condition = Some(Symbol("b") === Symbol("d")))

    val optimized = Optimize.execute(originalQuery.analyze)
    comparePlans(optimized, originalQuery.analyze)
  }

  test("Aggregate: LeftSemi join partial pushdown") {
    val originalQuery = testRelation
      .groupBy(Symbol("b"))(Symbol("b"), sum(Symbol("c")).as(Symbol("sum")))
      .join(testRelation1, joinType = LeftSemi,
        condition = Some(Symbol("b") === Symbol("d") && Symbol("sum") === 10))

    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer = testRelation
      .join(testRelation1, joinType = LeftSemi, condition = Some(Symbol("b") === Symbol("d")))
      .groupBy(Symbol("b"))(Symbol("b"), sum(Symbol("c")).as(Symbol("sum")))
      .where(Symbol("sum") === 10)
      .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("Aggregate: LeftAnti join no pushdown") {
    val originalQuery = testRelation
      .groupBy(Symbol("b"))(Symbol("b"), sum(Symbol("c")).as(Symbol("sum")))
      .join(testRelation1, joinType = LeftAnti,
        condition = Some(Symbol("b") === Symbol("d") && Symbol("sum") === 10))

    val optimized = Optimize.execute(originalQuery.analyze)
    comparePlans(optimized, originalQuery.analyze)
  }

  test("LeftSemiAnti join over aggregate - no pushdown") {
    val originalQuery = testRelation
      .groupBy(Symbol("b"))(Symbol("b"), sum(Symbol("c")).as(Symbol("sum")))
      .join(testRelation1, joinType = LeftSemi,
        condition = Some(Symbol("b") === Symbol("d") && Symbol("sum") === Symbol("d")))

    val optimized = Optimize.execute(originalQuery.analyze)
    comparePlans(optimized, originalQuery.analyze)
  }

  test("Aggregate: LeftSemiAnti join non-correlated scalar subq aggr exprs") {
    val subq = ScalarSubquery(testRelation.groupBy(Symbol("b"))(sum(Symbol("c")).as("sum")).analyze)
    val originalQuery = testRelation
      .groupBy(Symbol("a")) (Symbol("a"), subq.as("sum"))
      .join(testRelation1, joinType = LeftSemi,
        condition = Some(Symbol("sum") === Symbol("d") && Symbol("a") === Symbol("d")))

    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer = testRelation
      .join(testRelation1, joinType = LeftSemi,
        condition = Some(subq === Symbol("d") && Symbol("a") === Symbol("d")))
      .groupBy(Symbol("a")) (Symbol("a"), subq.as("sum"))
      .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("LeftSemiAnti join over Window") {
    val winExpr = windowExpr(count(Symbol("b")),
      windowSpec(Symbol("a") :: Nil, Symbol("b").asc :: Nil, UnspecifiedFrame))

    val originalQuery = testRelation
      .select(Symbol("a"), Symbol("b"), Symbol("c"), winExpr.as(Symbol("window")))
      .join(testRelation1, joinType = LeftSemi, condition = Some(Symbol("a") === Symbol("d")))

    val optimized = Optimize.execute(originalQuery.analyze)

    val correctAnswer = testRelation
      .join(testRelation1, joinType = LeftSemi, condition = Some(Symbol("a") === Symbol("d")))
      .select(Symbol("a"), Symbol("b"), Symbol("c"))
      .window(winExpr.as(Symbol("window")) :: Nil, Symbol("a") :: Nil, Symbol("b").asc :: Nil)
      .select(Symbol("a"), Symbol("b"), Symbol("c"), Symbol("window")).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("Window: LeftSemi partial pushdown") {
    // Attributes from join condition which does not refer to the window partition spec
    // are kept up in the plan as a Filter operator above Window.
    val winExpr = windowExpr(count(Symbol("b")),
      windowSpec(Symbol("a") :: Nil, Symbol("b").asc :: Nil, UnspecifiedFrame))

    val originalQuery = testRelation
      .select(Symbol("a"), Symbol("b"), Symbol("c"), winExpr.as(Symbol("window")))
      .join(testRelation1, joinType = LeftSemi,
        condition = Some(Symbol("a") === Symbol("d") && Symbol("b") > 5))

    val optimized = Optimize.execute(originalQuery.analyze)

    val correctAnswer = testRelation
      .join(testRelation1, joinType = LeftSemi, condition = Some(Symbol("a") === Symbol("d")))
      .select(Symbol("a"), Symbol("b"), Symbol("c"))
      .window(winExpr.as(Symbol("window")) :: Nil, Symbol("a") :: Nil, Symbol("b").asc :: Nil)
      .where(Symbol("b") > 5)
      .select(Symbol("a"), Symbol("b"), Symbol("c"), Symbol("window")).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("Window: LeftAnti no pushdown") {
    // Attributes from join condition which does not refer to the window partition spec
    // are kept up in the plan as a Filter operator above Window.
    val winExpr = windowExpr(count(Symbol("b")),
      windowSpec(Symbol("a") :: Nil, Symbol("b").asc :: Nil, UnspecifiedFrame))

    val originalQuery = testRelation
      .select(Symbol("a"), Symbol("b"), Symbol("c"), winExpr.as(Symbol("window")))
      .join(testRelation1, joinType = LeftAnti,
        condition = Some(Symbol("a") === Symbol("d") && Symbol("b") > 5))

    val optimized = Optimize.execute(originalQuery.analyze)

    val correctAnswer = testRelation
      .select(Symbol("a"), Symbol("b"), Symbol("c"))
      .window(winExpr.as(Symbol("window")) :: Nil, Symbol("a") :: Nil, Symbol("b").asc :: Nil)
      .join(testRelation1, joinType = LeftAnti,
        condition = Some(Symbol("a") === Symbol("d") && Symbol("b") > 5))
      .select(Symbol("a"), Symbol("b"), Symbol("c"), Symbol("window")).analyze
    comparePlans(optimized, correctAnswer)
  }

  test("Union: LeftSemiAnti join pushdown") {
    val testRelation2 = LocalRelation(Symbol("x").int, Symbol("y").int, Symbol("z").int)

    val originalQuery = Union(Seq(testRelation, testRelation2))
      .join(testRelation1, joinType = LeftSemi, condition = Some(Symbol("a") === Symbol("d")))

    val optimized = Optimize.execute(originalQuery.analyze)

    val correctAnswer = Union(Seq(
      testRelation.join(testRelation1, joinType = LeftSemi,
        condition = Some(Symbol("a") === Symbol("d"))),
      testRelation2.join(testRelation1, joinType = LeftSemi,
        condition = Some(Symbol("x") === Symbol("d")))))
      .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("Union: LeftSemiAnti join no pushdown in self join scenario") {
    val testRelation2 = LocalRelation(Symbol("x").int, Symbol("y").int, Symbol("z").int)

    val originalQuery = Union(Seq(testRelation, testRelation2))
      .join(testRelation2, joinType = LeftSemi, condition = Some(Symbol("a") === Symbol("x")))

    val optimized = Optimize.execute(originalQuery.analyze)
    comparePlans(optimized, originalQuery.analyze)
  }

  test("Unary: LeftSemiAnti join pushdown") {
    val originalQuery = testRelation
      .select(star())
      .repartition(1)
      .join(testRelation1, joinType = LeftSemi, condition = Some(Symbol("b") === Symbol("d")))

    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer = testRelation
      .join(testRelation1, joinType = LeftSemi, condition = Some(Symbol("b") === Symbol("d")))
      .select(Symbol("a"), Symbol("b"), Symbol("c"))
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
      .select(Symbol("a"), Symbol("b"), Symbol("c"))
      .repartition(1)
      .analyze
    comparePlans(optimized, correctAnswer)
  }

  test("Unary: LeftSemi join pushdown - partial pushdown") {
    val testRelationWithArrayType = LocalRelation(Symbol("a").int,
      Symbol("b").int, Symbol("c_arr").array(IntegerType))
    val originalQuery = testRelationWithArrayType
      .generate(Explode(Symbol("c_arr")), alias = Some("arr"), outputNames = Seq("out_col"))
      .join(testRelation1, joinType = LeftSemi,
        condition = Some(Symbol("b") === Symbol("d") && Symbol("b") === Symbol("out_col")))

    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer = testRelationWithArrayType
      .join(testRelation1, joinType = LeftSemi, condition = Some(Symbol("b") === Symbol("d")))
      .generate(Explode(Symbol("c_arr")), alias = Some("arr"), outputNames = Seq("out_col"))
      .where(Symbol("b") === Symbol("out_col"))
      .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("Unary: LeftAnti join pushdown - no pushdown") {
    val testRelationWithArrayType = LocalRelation(Symbol("a").int,
      Symbol("b").int, Symbol("c_arr").array(IntegerType))
    val originalQuery = testRelationWithArrayType
      .generate(Explode(Symbol("c_arr")), alias = Some("arr"), outputNames = Seq("out_col"))
      .join(testRelation1, joinType = LeftAnti,
        condition = Some(Symbol("b") === Symbol("d") && Symbol("b") === Symbol("out_col")))

    val optimized = Optimize.execute(originalQuery.analyze)
    comparePlans(optimized, originalQuery.analyze)
  }

  test("Unary: LeftSemiAnti join pushdown - no pushdown") {
    val testRelationWithArrayType = LocalRelation(Symbol("a").int,
      Symbol("b").int, Symbol("c_arr").array(IntegerType))
    val originalQuery = testRelationWithArrayType
      .generate(Explode(Symbol("c_arr")), alias = Some("arr"), outputNames = Seq("out_col"))
      .join(testRelation1, joinType = LeftSemi,
        condition = Some(Symbol("b") === Symbol("d") && Symbol("d") === Symbol("out_col")))

    val optimized = Optimize.execute(originalQuery.analyze)
    comparePlans(optimized, originalQuery.analyze)
  }

  test("Unary: LeftSemi join push down through Expand") {
    val expand = Expand(Seq(Seq(Symbol("a"), Symbol("b"), "null"), Seq(Symbol("a"), "null",
      Symbol("c"))), Seq(Symbol("a"), Symbol("b"), Symbol("c")), testRelation)
    val originalQuery = expand.join(testRelation1, joinType = LeftSemi,
        condition = Some(Symbol("b") === Symbol("d") && Symbol("b") === 1))

    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer = Expand(Seq(Seq(Symbol("a"),
      Symbol("b"), "null"), Seq(Symbol("a"), "null", Symbol("c"))),
      Seq(Symbol("a"), Symbol("b"), Symbol("c")), testRelation
        .join(testRelation1, joinType = LeftSemi,
          condition = Some(Symbol("b") === Symbol("d") && Symbol("b") === 1)))
      .analyze

    comparePlans(optimized, correctAnswer)
  }

  Seq(Some(Symbol("d") === Symbol("e")), None).foreach { case innerJoinCond =>
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

  Seq(Some(Symbol("d") === Symbol("e")), None).foreach { case innerJoinCond =>
    Seq(LeftSemi, LeftAnti).foreach { case outerJT =>
      Seq(Inner, LeftOuter, Cross).foreach { case innerJT =>
        test(s"$outerJT pushdown to left of join type: $innerJT join condition $innerJoinCond") {
          val joinedRelation = testRelation1.join(testRelation2, joinType = innerJT, innerJoinCond)
          val originalQuery =
            joinedRelation.join(testRelation, joinType = outerJT,
              condition = Some(Symbol("a") === Symbol("d")))
          val optimized = Optimize.execute(originalQuery.analyze)

          val pushedDownJoin =
            testRelation1.join(testRelation, joinType = outerJT,
              condition = Some(Symbol("a") === Symbol("d")))
          val correctAnswer =
            pushedDownJoin.join(testRelation2, joinType = innerJT, innerJoinCond).analyze
          comparePlans(optimized, correctAnswer)
        }
      }
    }
  }

  Seq(Some(Symbol("e") === Symbol("d")), None).foreach { case innerJoinCond =>
    Seq(LeftSemi, LeftAnti).foreach { case outerJT =>
      Seq(Inner, RightOuter, Cross).foreach { case innerJT =>
        test(s"$outerJT pushdown to right of join type: $innerJT join condition $innerJoinCond") {
          val joinedRelation = testRelation1.join(testRelation2, joinType = innerJT, innerJoinCond)
          val originalQuery =
            joinedRelation.join(testRelation, joinType = outerJT,
              condition = Some(Symbol("a") === Symbol("e")))
          val optimized = Optimize.execute(originalQuery.analyze)

          val pushedDownJoin =
            testRelation2.join(testRelation, joinType = outerJT,
              condition = Some(Symbol("a") === Symbol("e")))
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
        joinedRelation.join(testRelation, joinType = jt,
          condition = Some(Symbol("a") === Symbol("d")))
      val optimized = Optimize.execute(originalQuery.analyze)
      comparePlans(optimized, originalQuery.analyze)
    }
  }

  Seq(LeftSemi, LeftAnti).foreach { case jt =>
    test(s"$jt no pushdown - join condition refers right leg - join type for LeftOuter") {
      val joinedRelation = testRelation1.join(testRelation2, joinType = LeftOuter, None)
      val originalQuery =
        joinedRelation.join(testRelation, joinType = jt,
          condition = Some(Symbol("a") === Symbol("e")))
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
            condition = Some(Symbol("a") === Symbol("d") && Symbol("a") === Symbol("e")))
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
            condition = Some(Symbol("d") + Symbol("e") === Symbol("a")))
        val optimized = Optimize.execute(originalQuery.analyze)
        comparePlans(optimized, originalQuery.analyze)
      }
    }
  }

  Seq(LeftSemi, LeftAnti).foreach { case jt =>
    test(s"$jt no pushdown when child join type is FullOuter") {
      val joinedRelation = testRelation1.join(testRelation2, joinType = FullOuter, None)
      val originalQuery =
        joinedRelation.join(testRelation, joinType = jt,
          condition = Some(Symbol("a") === Symbol("e")))
      val optimized = Optimize.execute(originalQuery.analyze)
      comparePlans(optimized, originalQuery.analyze)
    }
  }

  Seq(LeftSemi, LeftAnti).foreach { jt =>
    test(s"SPARK-34081: $jt only push down if join can be planned as broadcast join") {
      Seq(-1, 100000).foreach { threshold =>
        withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> threshold.toString) {
          val originalQuery = testRelation
            .groupBy(Symbol("b"))(Symbol("b"))
            .join(testRelation1, joinType = jt, condition = Some(Symbol("b") <=> Symbol("d")))

          val optimized = Optimize.execute(originalQuery.analyze)
          val correctAnswer = if (threshold > 0) {
            testRelation
              .join(testRelation1, joinType = jt, condition = Some(Symbol("b") <=> Symbol("d")))
              .groupBy(Symbol("b"))(Symbol("b"))
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
