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
import org.apache.spark.sql.catalyst.dsl.plans.DslLogicalPlan
import org.apache.spark.sql.catalyst.expressions.{IsNotNull, IsNull}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.RuleExecutor

class SimplifyJoinConditionSuite extends PlanTest {
  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("Simplify Join Condition", Once,
        SimplifyJoinCondition)  :: Nil
  }

  val testRelation = LocalRelation('a.int, 'b.int, 'c.int)
  val testRelation1 = LocalRelation('d.int, 'e.int)

  test("Simple condition with null check on right side of or") {
    val originalQuery = testRelation
      .join(testRelation1, condition = Some(('b === 'd)||(IsNull('b) && IsNull('d))))

    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer = testRelation
      .join(testRelation1, condition = Some('b <=> 'd))
      .analyze
    comparePlans(optimized, correctAnswer)
  }

  test("Simple condition with null check on left side of or") {
    val originalQuery = testRelation
      .join(testRelation1, condition = Some((IsNull('b) && IsNull('d)) || ('b === 'd)))

    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer = testRelation
      .join(testRelation1, condition = Some('b <=> 'd))
      .analyze
    comparePlans(optimized, correctAnswer)
  }

  test("Simple condition with is not null check on one column") {
    val originalQuery = testRelation
      .join(testRelation1, condition = Some((IsNull('b) && IsNotNull('d)) || ('b === 'd)))

    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer = testRelation
      .join(testRelation1, condition = Some((IsNull('b) && IsNotNull('d)) || ('b === 'd)))
      .analyze
    comparePlans(optimized, correctAnswer)
  }

  test("multiple equal null safe conditions separated by and") {
    val originalQuery = testRelation.join(testRelation1,
      condition = Some(((IsNull('b) && IsNull('d)) || ('b === 'd)) &&
        ((IsNull('a) && IsNull('e)) || ('a === 'e))))

    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer = testRelation
      .join(testRelation1, condition = Some('b <=> 'd && 'a <=> 'e))
      .analyze
    comparePlans(optimized, correctAnswer)
  }

  test("multiple equal null safe conditions separated by or") {
    val originalQuery = testRelation.join(testRelation1,
      condition = Some(((IsNull('b) && IsNull('d)) || ('b === 'd)) ||
        ((IsNull('a) && IsNull('e)) || ('a === 'e))))

    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer = testRelation
      .join(testRelation1, condition = Some('b <=> 'd || 'a <=> 'e))
      .analyze
    comparePlans(optimized, correctAnswer)
  }

  test("Condition with another or in expression") {
    val originalQuery = testRelation.join(testRelation1,
      condition = Some((IsNull('b) && IsNull('d)) || ('b === 'd) || ('a === 'e)))

    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer = testRelation.join(testRelation1,
      condition = Some('b <=> 'd || ('a === 'e)))
      .analyze
    comparePlans(optimized, correctAnswer)
  }

  test("Condition with another and in expression so that and gets calculated first") {
    val originalQuery = testRelation.join(testRelation1,
      condition = Some((IsNull('b) && IsNull('d)) || ('b === 'd) && ('a === 'e)))

    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer = testRelation.join(testRelation1,
      condition = Some((IsNull('b) && IsNull('d)) || ('b === 'd) && ('a === 'e)))
      .analyze
    comparePlans(optimized, correctAnswer)
  }

  test("Condition with another and in expression") {
    val originalQuery = testRelation.join(testRelation1,
      condition = Some(((IsNull('b) && IsNull('d)) || ('b === 'd)) && ('a === 'e)))

    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer = testRelation.join(testRelation1,
      condition = Some('b <=> 'd && ('a === 'e)))
      .analyze
    comparePlans(optimized, correctAnswer)
  }
}
