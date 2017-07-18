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

class InferJoinConditionsSuite extends PlanTest {

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("Infer Join Conditions", FixedPoint(10),
        InferJoinConditionsFromConstraints,
        PushPredicateThroughJoin) :: Nil
  }

  val testRelation1 = LocalRelation('a.int, 'b.int)
  val testRelation2 = LocalRelation('c.int, 'd.int)

  test("successful detection of join conditions (1)") {
    checkJoinOptimization(
      originalFilter = 'a === 1 && 'c === 1 && 'd === 1,
      originalJoinType = Cross,
      originalJoinCondition = None,
      expectedLeftRelationFilter = 'a === 1,
      expectedRightRelationFilter = 'c === 1 && 'd === 1,
      expectedJoinType = Inner,
      expectedJoinCondition = Some('a === 'c && 'a === 'd))
  }

  test("successful detection of join conditions (2)") {
    checkJoinOptimization(
      originalFilter = 'a === 1 && 'b === 2 && 'd === 1,
      originalJoinType = Cross,
      originalJoinCondition = None,
      expectedLeftRelationFilter = 'a === 1 && 'b === 2,
      expectedRightRelationFilter = 'd === 1,
      expectedJoinType = Inner,
      expectedJoinCondition = Some('a === 'd))
  }

  test("successful detection of join conditions (3)") {
    checkJoinOptimization(
      originalFilter = 'a === 1 && Literal(1) === 'c && 'd === 'a,
      originalJoinType = Cross,
      originalJoinCondition = None,
      expectedLeftRelationFilter = 'a === 1,
      expectedRightRelationFilter = Literal(1) === 'c,
      expectedJoinType = Inner,
      expectedJoinCondition = Some('a === 'c && 'd === 'a))
  }

  test("successful detection of join conditions (4)") {
    // PushPredicateThroughJoin will push down 'd === 'a as a join condition
    // InferJoinConditionsFromConstraints will NOT infer any semantically new predicates,
    // so the join type will stay the same (i.e., CROSS)
    checkJoinOptimization(
      originalFilter = 'a === 1 && Literal(1) === 'd && 'd === 'a,
      originalJoinType = Cross,
      originalJoinCondition = None,
      expectedLeftRelationFilter = 'a === 1,
      expectedRightRelationFilter = Literal(1) === 'd,
      expectedJoinType = Cross,
      expectedJoinCondition = Some('a === 'd))
  }

  test("successful detection of join conditions (5)") {
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

  test("successful detection of join conditions (6)") {
    checkJoinOptimization(
      originalFilter = 'a === 1 && 'd === 1,
      originalJoinType = Cross,
      originalJoinCondition = Some('a === 'c),
      expectedLeftRelationFilter = 'a === 1,
      expectedRightRelationFilter = 'd === 1,
      expectedJoinType = Inner,
      expectedJoinCondition = Some('a === 'c && 'a === 'd))
  }

  test("successful detection of join conditions (7)") {
    checkJoinOptimization(
      originalFilter = 'b === 1 && 'd === 1,
      originalJoinType = Cross,
      originalJoinCondition = Some('a > 'c),
      expectedLeftRelationFilter = 'b === 1,
      expectedRightRelationFilter = 'd === 1,
      expectedJoinType = Inner,
      expectedJoinCondition = Some('a > 'c && 'b === 'd))
  }

  test("successful detection of join conditions (8)") {
    checkJoinOptimization(
      originalFilter = 'a === 'b + 'd && 'a === 1 && 'c === 'b + 'd && 'd === 4,
      originalJoinType = Cross,
      originalJoinCondition = Some('a >= 'c),
      expectedLeftRelationFilter = 'a === 1,
      expectedRightRelationFilter = 'd === 4,
      expectedJoinType = Inner,
      expectedJoinCondition = Some('a === 'c && 'a >= 'c && 'a === 'b + 'd && 'c === 'b + 'd))
  }

  test("successful detection of complex join conditions via InferJoinConditionsFromConstraints") {
    checkJoinOptimization(
      originalFilter = 'a === Cast("1", IntegerType) && 'c === Cast("1", IntegerType) && 'd === 1,
      originalJoinType = Cross,
      originalJoinCondition = None,
      expectedLeftRelationFilter = 'a === Cast("1", IntegerType),
      expectedRightRelationFilter = 'c === Cast("1", IntegerType) && 'd === 1,
      expectedJoinType = Inner,
      expectedJoinCondition = Some('a === 'c))
  }

  test("successful detection of complex join conditions via PushPredicateThroughJoin") {
    checkJoinOptimization(
      originalFilter = 'a === ('b % 2) && 'c === ('b % 2) && 'd === 1,
      originalJoinType = Cross,
      originalJoinCondition = None,
      expectedLeftRelationFilter = 'a === ('b % 2),
      expectedRightRelationFilter = 'd === 1,
      expectedJoinType = Inner,
      expectedJoinCondition = Some('c === ('b % 2) && 'a === 'c))
  }

  test("successful detection of complex join conditions") {
    // InferJoinConditionsFromConstraints will infer 'a === d'
    // PushPredicateThroughJoin will add 'a === (c % 2)' to the join condition
    checkJoinOptimization(
      originalFilter = 'a === 1 && 'a === ('c % 2) && 'd === 1,
      originalJoinType = Cross,
      originalJoinCondition = None,
      expectedLeftRelationFilter = 'a === 1,
      expectedRightRelationFilter = 'd === 1,
      expectedJoinType = Inner,
      expectedJoinCondition = Some('a === 'd && 'a === ('c % 2)))
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

  test("inability to detect join conditions (3)") {
    checkJoinOptimization(
      originalFilter = Literal(1) === 'b && ('c === 1 || 'd === 1),
      originalJoinType = Cross,
      originalJoinCondition = None,
      expectedLeftRelationFilter = Literal(1) === 'b,
      expectedRightRelationFilter = 'c === 1 || 'd === 1,
      expectedJoinType = Cross,
      expectedJoinCondition = None)
  }

  test("inability to detect join conditions (4)") {
    checkJoinOptimization(
      originalFilter = Literal(1) === 'b && 'c === 1,
      originalJoinType = Cross,
      originalJoinCondition = Some('c === 'b),
      expectedLeftRelationFilter = Literal(1) === 'b,
      expectedRightRelationFilter = 'c === 1,
      expectedJoinType = Cross,
      expectedJoinCondition = Some('c === 'b))
  }

  test("inability to detect join conditions (5)") {
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
