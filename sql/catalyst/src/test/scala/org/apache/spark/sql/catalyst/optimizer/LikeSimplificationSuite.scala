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

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.rules._

/* Implicit conversions */
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._

class LikeSimplificationSuite extends PlanTest {

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("Like Simplification", Once,
        LikeSimplification) :: Nil
  }

  val testRelation = LocalRelation('a.string)

  test("simplify Like into StartsWith") {
    val originalQuery =
      testRelation
        .where(('a like "abc%") || ('a like "abc\\%"))

    val optimized = Optimize(originalQuery.analyze)
    val correctAnswer = testRelation
      .where(StartsWith('a, "abc") || ('a like "abc\\%"))
      .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("simplify Like into EndsWith") {
    val originalQuery =
      testRelation
        .where('a like "%xyz")

    val optimized = Optimize(originalQuery.analyze)
    val correctAnswer = testRelation
      .where(EndsWith('a, "xyz"))
      .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("simplify Like into Contains") {
    val originalQuery =
      testRelation
        .where(('a like "%mn%") || ('a like "%mn\\%"))

    val optimized = Optimize(originalQuery.analyze)
    val correctAnswer = testRelation
      .where(Contains('a, "mn") || ('a like "%mn\\%"))
      .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("simplify Like into EqualTo") {
    val originalQuery =
      testRelation
        .where(('a like "") || ('a like "abc"))

    val optimized = Optimize(originalQuery.analyze)
    val correctAnswer = testRelation
      .where(('a === "") || ('a === "abc"))
      .analyze

    comparePlans(optimized, correctAnswer)
  }
}
