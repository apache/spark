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

import org.scalatest.matchers.must.Matchers._

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.internal.SQLConf.CROSS_JOINS_ENABLED

class CheckCartesianProductsSuite extends PlanTest {

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches = Batch("Check Cartesian Products", Once, CheckCartesianProducts) :: Nil
  }

  val testRelation1 = LocalRelation('a.int, 'b.int)
  val testRelation2 = LocalRelation('c.int, 'd.int)

  val joinTypesWithRequiredCondition = Seq(Inner, LeftOuter, RightOuter, FullOuter)
  val joinTypesWithoutRequiredCondition = Seq(LeftSemi, LeftAnti, ExistenceJoin('exists))

  test("CheckCartesianProducts doesn't throw an exception if cross joins are enabled)") {
    withSQLConf(CROSS_JOINS_ENABLED.key -> "true") {
      noException should be thrownBy {
        for (joinType <- joinTypesWithRequiredCondition ++ joinTypesWithoutRequiredCondition) {
          performCartesianProductCheck(joinType)
        }
      }
    }
  }

  test("CheckCartesianProducts throws an exception for join types that require a join condition") {
    withSQLConf(CROSS_JOINS_ENABLED.key -> "false") {
      for (joinType <- joinTypesWithRequiredCondition) {
        val thrownException = the [AnalysisException] thrownBy {
          performCartesianProductCheck(joinType)
        }
        assert(thrownException.message.contains("Detected implicit cartesian product"))
      }
    }
  }

  test("CheckCartesianProducts doesn't throw an exception if a join condition is present") {
    withSQLConf(CROSS_JOINS_ENABLED.key -> "false") {
      for (joinType <- joinTypesWithRequiredCondition) {
        noException should be thrownBy {
          performCartesianProductCheck(joinType, Some('a === 'd))
        }
      }
    }
  }

  test("CheckCartesianProducts doesn't throw an exception if join types don't require conditions") {
    withSQLConf(CROSS_JOINS_ENABLED.key -> "false") {
      for (joinType <- joinTypesWithoutRequiredCondition) {
        noException should be thrownBy {
          performCartesianProductCheck(joinType)
        }
      }
    }
  }

  private def performCartesianProductCheck(
      joinType: JoinType,
      condition: Option[Expression] = None): Unit = {
    val analyzedPlan = testRelation1.join(testRelation2, joinType, condition).analyze
    val optimizedPlan = Optimize.execute(analyzedPlan)
    comparePlans(analyzedPlan, optimizedPlan)
  }
}
