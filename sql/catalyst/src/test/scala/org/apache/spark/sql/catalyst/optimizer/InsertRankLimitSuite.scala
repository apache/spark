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
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.internal.SQLConf

class InsertRankLimitSuite extends PlanTest {

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("Insert RankLimit", FixedPoint(10),
        CollapseProject,
        RemoveNoopOperators,
        PushDownPredicates,
        InsertRankLimit) :: Nil
  }

  object WithoutOptimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("Insert RankLimit", FixedPoint(10),
        CollapseProject,
        RemoveNoopOperators,
        PushDownPredicates) :: Nil
  }

  private val testRelation = LocalRelation.fromExternalRows(
    Seq("a".attr.int, "b".attr.int, "c".attr.int),
    1.to(10).map(i => Row(i % 3, 2, i)))
  private val a = testRelation.output(0)
  private val b = testRelation.output(1)
  private val c = testRelation.output(2)
  private val rankFunctions = Seq(RowNumber(), Rank(c :: Nil), DenseRank(c :: Nil))
  private val windowFrame = SpecifiedWindowFrame(RowFrame, UnboundedPreceding, CurrentRow)

  test("insert rank limit node for top-k computation: rn < 3") {
    withSQLConf(SQLConf.RANK_LIMIT_ENABLE.key -> "true") {
      rankFunctions.foreach { rankFunction =>
        val originalQuery =
          testRelation
            .select(a, b, c,
              windowExpr(rankFunction,
                windowSpec(a :: Nil, c.desc :: Nil, windowFrame)).as("rn"))
            .where('rn < 3)

        val correctAnswer =
          testRelation
            .rankLimit(a :: Nil, c.desc :: Nil, rankFunction, 2)
            .select(a, b, c,
              windowExpr(rankFunction,
                windowSpec(a :: Nil, c.desc :: Nil, windowFrame)).as("rn"))
            .where('rn < 3)

        comparePlans(
          Optimize.execute(originalQuery.analyze),
          WithoutOptimize.execute(correctAnswer.analyze))
      }
    }
  }

  test("insert rank limit node for top-k computation: rn === 2 && b > 0") {
    withSQLConf(SQLConf.RANK_LIMIT_ENABLE.key -> "true") {
      rankFunctions.foreach { rankFunction =>
        val originalQuery =
          testRelation
            .select(a, b, c,
              windowExpr(rankFunction,
                windowSpec(a :: Nil, c.desc :: Nil, windowFrame)).as("rn"))
            .where('rn === 2 && b > 0)

        val correctAnswer =
          testRelation
            .rankLimit(a :: Nil, c.desc :: Nil, rankFunction, 2)
            .select(a, b, c,
              windowExpr(rankFunction,
                windowSpec(a :: Nil, c.desc :: Nil, windowFrame)).as("rn"))
            .where('rn === 2 && b > 0)

        comparePlans(
          Optimize.execute(originalQuery.analyze),
          WithoutOptimize.execute(correctAnswer.analyze))
      }
    }
  }

  test("insert rank limit node for top-k computation: empty partitionSpec") {
    withSQLConf(SQLConf.RANK_LIMIT_ENABLE.key -> "true") {
      rankFunctions.foreach { rankFunction =>
        val originalQuery =
          testRelation
            .select(a, b, c,
              windowExpr(rankFunction,
                windowSpec(Nil, c.desc :: Nil, windowFrame)).as("rn"))
            .where('rn <= 4)

        val correctAnswer =
          testRelation
            .rankLimit(Nil, c.desc :: Nil, rankFunction, 4)
            .select(a, b, c,
              windowExpr(rankFunction,
                windowSpec(Nil, c.desc :: Nil, windowFrame)).as("rn"))
            .where('rn <= 4)

        comparePlans(
          Optimize.execute(originalQuery.analyze),
          WithoutOptimize.execute(correctAnswer.analyze))
      }
    }
  }

  test("unsupported condition: rn > 2") {
    withSQLConf(SQLConf.RANK_LIMIT_ENABLE.key -> "true") {
      rankFunctions.foreach { rankFunction =>
        val originalQuery =
          testRelation
            .select(a, b, c,
              windowExpr(rankFunction,
                windowSpec(a :: Nil, c.desc :: Nil, windowFrame)).as("rn"))
            .where('rn > 2)

        comparePlans(
          Optimize.execute(originalQuery.analyze),
          WithoutOptimize.execute(originalQuery.analyze))
      }
    }
  }

  test("unsupported condition: rn === 1 || b > 0") {
    withSQLConf(SQLConf.RANK_LIMIT_ENABLE.key -> "true") {
      rankFunctions.foreach { rankFunction =>
        val originalQuery =
          testRelation
            .select(a, b, c,
              windowExpr(rankFunction,
                windowSpec(a :: Nil, c.desc :: Nil, windowFrame)).as("rn"))
            .where('rn === 1 || b > 0)

        comparePlans(
          Optimize.execute(originalQuery.analyze),
          WithoutOptimize.execute(originalQuery.analyze))
      }
    }
  }
}
