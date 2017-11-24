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
import org.apache.spark.sql.catalyst.expressions.{Cast, Expression, Literal, Not}
import org.apache.spark.sql.catalyst.plans.{Cross, Inner, JoinType, PlanTest}
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.internal.SQLConf.CONSTRAINT_PROPAGATION_ENABLED
import org.apache.spark.sql.types.IntegerType

class EliminateCrossJoinSuite extends PlanTest {

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("Eliminate cross joins", FixedPoint(10),
        EliminateCrossJoin,
        PushPredicateThroughJoin) :: Nil
  }

  val testRelation1 = LocalRelation('a.int, 'b.int)
  val testRelation2 = LocalRelation('c.int, 'd.int)

  test("successful elimination of cross joins (1)") {
    checkJoinOptimization(
      originalFilter = 'a === 1 && 'c === 1 && 'd === 1,
      originalJoinType = Cross,
      originalJoinCondition = None,
      expectedLeftRelationFilter = 'a === 1,
      expectedRightRelationFilter = 'c === 1 && 'd === 1,
      expectedJoinType = Inner,
      expectedJoinCondition = Some('a === 'c && 'a === 'd))
  }

  test("successful elimination of cross joins (2)") {
    checkJoinOptimization(
      originalFilter = 'a === 1 && 'b === 2 && 'd === 1,
      originalJoinType = Cross,
      originalJoinCondition = None,
      expectedLeftRelationFilter = 'a === 1 && 'b === 2,
      expectedRightRelationFilter = 'd === 1,
      expectedJoinType = Inner,
      expectedJoinCondition = Some('a === 'd))
  }

  test("successful elimination of cross joins (3)") {
    // PushPredicateThroughJoin will push 'd === 'a into the join condition
    // EliminateCrossJoin will NOT apply because the condition will be already present
    // therefore, the join type will stay the same (i.e., CROSS)
    checkJoinOptimization(
      originalFilter = 'a === 1 && Literal(1) === 'd && 'd === 'a,
      originalJoinType = Cross,
      originalJoinCondition = None,
      expectedLeftRelationFilter = 'a === 1,
      expectedRightRelationFilter = Literal(1) === 'd,
      expectedJoinType = Cross,
      expectedJoinCondition = Some('a === 'd))
  }

  test("successful elimination of cross joins (4)") {
    // Literal(1) * Literal(2) and Literal(2) * Literal(1) are semantically equal
    checkJoinOptimization(
      originalFilter = 'a === Literal(1) * Literal(2) && Literal(2) * Literal(1) === 'c,
      originalJoinType = Cross,
      originalJoinCondition = None,
      expectedLeftRelationFilter = 'a === Literal(1) * Literal(2),
      expectedRightRelationFilter = Literal(2) * Literal(1) === 'c,
      expectedJoinType = Inner,
      expectedJoinCondition = Some('a === 'c))
  }

  test("successful elimination of cross joins (5)") {
    checkJoinOptimization(
      originalFilter = 'a === 1 && Literal(1) === 'a && 'c === 1,
      originalJoinType = Cross,
      originalJoinCondition = None,
      expectedLeftRelationFilter = 'a === 1 && Literal(1) === 'a,
      expectedRightRelationFilter = 'c === 1,
      expectedJoinType = Inner,
      expectedJoinCondition = Some('a === 'c))
  }

  test("successful elimination of cross joins (6)") {
    checkJoinOptimization(
      originalFilter = 'a === Cast("1", IntegerType) && 'c === Cast("1", IntegerType) && 'd === 1,
      originalJoinType = Cross,
      originalJoinCondition = None,
      expectedLeftRelationFilter = 'a === Cast("1", IntegerType),
      expectedRightRelationFilter = 'c === Cast("1", IntegerType) && 'd === 1,
      expectedJoinType = Inner,
      expectedJoinCondition = Some('a === 'c))
  }

  test("successful elimination of cross joins (7)") {
    // The join condition appears due to PushPredicateThroughJoin
    checkJoinOptimization(
      originalFilter = (('a >= 1 && 'c === 1) || 'd === 10) && 'b === 10 && 'c === 1,
      originalJoinType = Cross,
      originalJoinCondition = None,
      expectedLeftRelationFilter = 'b === 10,
      expectedRightRelationFilter = 'c === 1,
      expectedJoinType = Cross,
      expectedJoinCondition = Some(('a >= 1 && 'c === 1) || 'd === 10))
  }

  test("successful elimination of cross joins (8)") {
    checkJoinOptimization(
      originalFilter = 'a === 1 && 'c === 1 && Literal(1) === 'a && Literal(1) === 'c,
      originalJoinType = Cross,
      originalJoinCondition = None,
      expectedLeftRelationFilter = 'a === 1 && Literal(1) === 'a,
      expectedRightRelationFilter = 'c === 1 && Literal(1) === 'c,
      expectedJoinType = Inner,
      expectedJoinCondition = Some('a === 'c))
  }

  test("inability to detect join conditions when constant propagation is disabled") {
    withSQLConf(CONSTRAINT_PROPAGATION_ENABLED.key -> "false") {
      checkJoinOptimization(
        originalFilter = 'a === 1 && 'c === 1 && 'd === 1,
        originalJoinType = Cross,
        originalJoinCondition = None,
        expectedLeftRelationFilter = 'a === 1,
        expectedRightRelationFilter = 'c === 1 && 'd === 1,
        expectedJoinType = Cross,
        expectedJoinCondition = None)
    }
  }

  test("inability to detect join conditions (1)") {
    checkJoinOptimization(
      originalFilter = 'a >= 1 && 'c === 1 && 'd >= 1,
      originalJoinType = Cross,
      originalJoinCondition = None,
      expectedLeftRelationFilter = 'a >= 1,
      expectedRightRelationFilter = 'c === 1 && 'd >= 1,
      expectedJoinType = Cross,
      expectedJoinCondition = None)
  }

  test("inability to detect join conditions (2)") {
    checkJoinOptimization(
      originalFilter = Literal(1) === 'b && ('c === 1 || 'd === 1),
      originalJoinType = Cross,
      originalJoinCondition = None,
      expectedLeftRelationFilter = Literal(1) === 'b,
      expectedRightRelationFilter = 'c === 1 || 'd === 1,
      expectedJoinType = Cross,
      expectedJoinCondition = None)
  }

  test("inability to detect join conditions (3)") {
    checkJoinOptimization(
      originalFilter = Literal(1) === 'b && 'c === 1,
      originalJoinType = Cross,
      originalJoinCondition = Some('c === 'b),
      expectedLeftRelationFilter = Literal(1) === 'b,
      expectedRightRelationFilter = 'c === 1,
      expectedJoinType = Cross,
      expectedJoinCondition = Some('c === 'b))
  }

  test("inability to detect join conditions (4)") {
    checkJoinOptimization(
      originalFilter = Not('a === 1) && 'd === 1,
      originalJoinType = Cross,
      originalJoinCondition = None,
      expectedLeftRelationFilter = Not('a === 1),
      expectedRightRelationFilter = 'd === 1,
      expectedJoinType = Cross,
      expectedJoinCondition = None)
  }

  private def checkJoinOptimization(
      originalFilter: Expression,
      originalJoinType: JoinType,
      originalJoinCondition: Option[Expression],
      expectedLeftRelationFilter: Expression,
      expectedRightRelationFilter: Expression,
      expectedJoinType: JoinType,
      expectedJoinCondition: Option[Expression]): Unit = {

    val originalQuery = testRelation1
      .join(testRelation2, originalJoinType, originalJoinCondition)
      .where(originalFilter)
    val optimizedQuery = Optimize.execute(originalQuery.analyze)

    val left = testRelation1.where(expectedLeftRelationFilter)
    val right = testRelation2.where(expectedRightRelationFilter)
    val expectedQuery = left.join(right, expectedJoinType, expectedJoinCondition).analyze
    comparePlans(optimizedQuery, expectedQuery)
  }
}
