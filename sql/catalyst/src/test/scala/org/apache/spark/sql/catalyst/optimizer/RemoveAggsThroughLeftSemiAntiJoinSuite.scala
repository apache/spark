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
import org.apache.spark.sql.catalyst.expressions.Literal.TrueLiteral
import org.apache.spark.sql.catalyst.plans.{LeftAnti, LeftSemi, PlanTest}
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.RuleExecutor

class RemoveAggsThroughLeftSemiAntiJoinSuite extends PlanTest {

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches = Batch("RemoveAggsThroughLeftSemiAntiJoin", FixedPoint(10),
      RemoveAggsThroughLeftSemiAntiJoin) :: Nil
  }

  private val testRelation = LocalRelation('a.int, 'b.int)
  private val x = testRelation.subquery('x)
  private val y = testRelation.subquery('y)

  test("Remove the aggregation from left semi/anti join if aggregation the same") {
    Seq(LeftSemi, LeftAnti).foreach { joinType =>
      val originalQuery = x.groupBy('a, 'b)('a, 'b)
        .join(y, joinType, Some("x.a".attr === "y.a".attr && "x.b".attr === "y.b".attr))
        .groupBy("x.a".attr, "x.b".attr)("x.a".attr, "x.b".attr)
      val correctAnswer = x.groupBy('a, 'b)('a, 'b)
        .join(y, joinType, Some("x.a".attr === "y.a".attr && "x.b".attr === "y.b".attr))

      val optimized = Optimize.execute(originalQuery.analyze)
      comparePlans(optimized, correctAnswer.analyze)
    }
  }

  test("Remove the aggregation from left semi/anti join if it is the sub aggregateExprs") {
    Seq(LeftSemi, LeftAnti).foreach { joinType =>
      val originalQuery = x.groupBy('a, 'b)('a, 'b)
        .join(y, joinType, Some("x.a".attr === "y.a".attr && "x.b".attr === "y.b".attr))
        .groupBy("x.a".attr, "x.b".attr)("x.a".attr)
      val correctAnswer = x.groupBy('a, 'b)('a, 'b)
        .join(y, joinType, Some("x.a".attr === "y.a".attr && "x.b".attr === "y.b".attr))

      val optimized = Optimize.execute(originalQuery.analyze)
      comparePlans(optimized, correctAnswer.analyze)
    }
  }

  test("Negative case: The grouping expressions not same") {
    Seq(LeftSemi, LeftAnti).foreach { joinType =>
      val originalQuery = x.groupBy('a, 'b)('a, 'b)
        .join(y, joinType, Some("x.a".attr === "y.a".attr && "x.b".attr === "y.b".attr))
        .groupBy("x.a".attr)("x.a".attr)

      val optimized = Optimize.execute(originalQuery.analyze)
      comparePlans(optimized, originalQuery.analyze)
    }
  }

  test("Negative case: The aggregate expressions not same") {
    Seq(LeftSemi, LeftAnti).foreach { joinType =>
      val originalQuery = x.groupBy('a, 'b)('a, 'b)
        .join(y, joinType, Some("x.a".attr === "y.a".attr && "x.b".attr === "y.b".attr))
        .groupBy("x.a".attr)(count("x.b".attr))

      val optimized = Optimize.execute(originalQuery.analyze)
      comparePlans(optimized, originalQuery.analyze)
    }
  }

  test("Negative case: The aggregate expressions with Literal") {
    Seq(LeftSemi, LeftAnti).foreach { joinType =>
      val originalQuery = x.groupBy('a, 'b)('a, TrueLiteral)
        .join(y, joinType, Some("x.a".attr === "y.a".attr))
        .groupBy("x.a".attr)("x.a".attr, TrueLiteral)

      val optimized = Optimize.execute(originalQuery.analyze)
      comparePlans(optimized, originalQuery.analyze)
    }
  }
}
