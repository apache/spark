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
import org.apache.spark.sql.catalyst.expressions.{Alias, ScalarSubquery}
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules._
import org.apache.spark.sql.types.StringType

class JoinEliminationSuite extends PlanTest {
  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("Subqueries", Once,
        EliminateSubqueryAliases) ::
      Batch("Outer Join Elimination", Once,
        EliminateJoin,
        PushPredicateThroughJoin) :: Nil
  }

  private val testRelation = LocalRelation($"a".int, $"b".int, $"c".int)
  private val testRelation1 = LocalRelation($"d".int, $"e".int, $"f".int)

  test("right side max row number is 1 and join condition is exist") {
    Seq(Inner, Cross, LeftSemi, LeftOuter, LeftAnti, RightOuter, FullOuter).foreach { joinType =>
      val y = testRelation1.groupBy()(max($"d").as("d"))
      val originalQuery =
        testRelation.as("x").join(y.subquery("y"), joinType, Option("x.a".attr === "y.d".attr))
          .select($"b")

      val correctAnswer = joinType match {
        case Inner | Cross | LeftSemi =>
          testRelation.as("x")
            .where($"a" === ScalarSubquery(Project(Seq(Alias($"d", "_joinkey")()), y)))
            .select($"b")
        case LeftOuter =>
          testRelation.as("x").select($"b")
        case _ =>
          originalQuery
      }

      comparePlans(Optimize.execute(originalQuery.analyze), correctAnswer.analyze)
    }
  }

  test("right side max row number is 1 and join condition is not exist") {
    Seq(Inner, Cross, LeftSemi, LeftOuter, LeftAnti, RightOuter, FullOuter).foreach { joinType =>
      val y = testRelation1.groupBy()(max($"d").as("d"))
      val originalQuery =
        testRelation.as("x").join(y.subquery("y"), joinType, None)
          .select($"b")

      val correctAnswer = joinType match {
        case LeftOuter | FullOuter =>
          testRelation.as("x").select($"b")
        case _ =>
          originalQuery
      }

      comparePlans(Optimize.execute(originalQuery.analyze), correctAnswer.analyze)
    }
  }

  test("right side max row number is 1 and join condition is complex expressions") {
    Seq(Inner, Cross, LeftSemi, LeftOuter, LeftAnti, RightOuter, FullOuter).foreach { joinType =>
      val y = testRelation1.groupBy()(max($"d").as("d"))
      val originalQuery =
        testRelation.as("x").join(y.subquery("y"), joinType,
          Option("x.a".attr.cast(StringType) === "y.d".attr.cast(StringType)))
          .select($"b")

      val correctAnswer = joinType match {
        case Inner | Cross | LeftSemi =>
          testRelation.as("x")
            .where($"a".cast(StringType) ===
              ScalarSubquery(Project(Seq(Alias($"d".cast(StringType), "_joinkey")()), y)))
            .select($"b")
        case LeftOuter =>
          testRelation.as("x").select($"b")
        case _ =>
          originalQuery
      }

      comparePlans(Optimize.execute(originalQuery.analyze), correctAnswer.analyze)
    }
  }

  test("left side max row number is 1 and join condition is exist") {
    Seq(Inner, Cross, LeftOuter, RightOuter, FullOuter).foreach { joinType =>
      val x = testRelation.groupBy()(max($"a").as("a"))
      val originalQuery =
        x.as("x").join(testRelation1.as("y"), joinType, Option("x.a".attr === "y.d".attr))
          .select($"e")

      val correctAnswer = joinType match {
        case Inner | Cross =>
          testRelation1.as("y")
            .where($"d" === ScalarSubquery(Project(Seq(Alias($"a", "_joinkey")()), x)))
            .select($"e").analyze
        case RightOuter =>
          testRelation1.as("y").select($"e")
        case _ =>
          originalQuery
      }

      comparePlans(Optimize.execute(originalQuery.analyze), correctAnswer.analyze)
    }
  }
}
