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
import org.apache.spark.sql.catalyst.expressions.{Alias, Literal, UpdateFields, WithField}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules._


class CombineUpdateFieldsSuite extends PlanTest {

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches = Batch("CombineUpdateFields", FixedPoint(10), CombineUpdateFields) :: Nil
  }

  private val testRelation = LocalRelation('a.struct('a1.int))

  test("combines two adjacent UpdateFields Expressions") {
    val originalQuery = testRelation
      .select(Alias(
        UpdateFields(
          UpdateFields(
            'a,
            WithField("b1", Literal(4)) :: Nil),
          WithField("c1", Literal(5)) :: Nil), "out")())

    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer = testRelation
      .select(Alias(UpdateFields('a, WithField("b1", Literal(4)) :: WithField("c1", Literal(5)) ::
        Nil), "out")())
      .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("combines three adjacent UpdateFields Expressions") {
    val originalQuery = testRelation
      .select(Alias(
        UpdateFields(
          UpdateFields(
            UpdateFields(
              'a,
              WithField("b1", Literal(4)) :: Nil),
            WithField("c1", Literal(5)) :: Nil),
          WithField("d1", Literal(6)) :: Nil), "out")())

    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer = testRelation
      .select(Alias(UpdateFields('a, WithField("b1", Literal(4)) :: WithField("c1", Literal(5)) ::
        WithField("d1", Literal(6)) :: Nil), "out")())
      .analyze

    comparePlans(optimized, correctAnswer)
  }
}
