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
import org.apache.spark.sql.catalyst.expressions.{Alias, Literal, WithFields}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules._


class CombineWithFieldsSuite extends PlanTest {

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches = Batch("CombineWithFields", FixedPoint(10), CombineWithFields) :: Nil
  }

  private val testRelation = LocalRelation('a.struct('a1.int))

  test("combines two WithFields") {
    val originalQuery = testRelation
      .select(Alias(
        WithFields(
          WithFields(
            'a,
            Seq("b1"),
            Seq(Literal(4))),
          Seq("c1"),
          Seq(Literal(5))), "out")())

    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer = testRelation
      .select(Alias(WithFields('a, Seq("b1", "c1"), Seq(Literal(4), Literal(5))), "out")())
      .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("combines three WithFields") {
    val originalQuery = testRelation
      .select(Alias(
        WithFields(
          WithFields(
            WithFields(
              'a,
              Seq("b1"),
              Seq(Literal(4))),
            Seq("c1"),
            Seq(Literal(5))),
          Seq("d1"),
          Seq(Literal(6))), "out")())

    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer = testRelation
      .select(Alias(WithFields('a, Seq("b1", "c1", "d1"), Seq(4, 5, 6).map(Literal(_))), "out")())
      .analyze

    comparePlans(optimized, correctAnswer)
  }
}
