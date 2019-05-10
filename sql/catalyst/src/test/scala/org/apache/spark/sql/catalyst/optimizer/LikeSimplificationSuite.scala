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
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules._
import org.apache.spark.sql.types.{BooleanType, StringType}

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

    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer = testRelation
      .where(StartsWith('a, "abc") || ('a like "abc\\%"))
      .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("simplify Like into EndsWith") {
    val originalQuery =
      testRelation
        .where('a like "%xyz")

    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer = testRelation
      .where(EndsWith('a, "xyz"))
      .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("simplify Like into startsWith and EndsWith") {
    val originalQuery =
      testRelation
        .where(('a like "abc\\%def") || ('a like "abc%def"))

    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer = testRelation
      .where(('a like "abc\\%def") ||
        (Length('a) >= 6 && (StartsWith('a, "abc") && EndsWith('a, "def"))))
      .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("simplify Like into Contains") {
    val originalQuery =
      testRelation
        .where(('a like "%mn%") || ('a like "%mn\\%"))

    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer = testRelation
      .where(Contains('a, "mn") || ('a like "%mn\\%"))
      .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("simplify Like into EqualTo") {
    val originalQuery =
      testRelation
        .where(('a like "") || ('a like "abc"))

    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer = testRelation
      .where(('a === "") || ('a === "abc"))
      .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("null pattern") {
    val originalQuery = testRelation.where('a like Literal(null, StringType)).analyze
    val optimized = Optimize.execute(originalQuery)
    comparePlans(optimized, testRelation.where(Literal(false, BooleanType)).analyze)
  }

  test("optimize limit queries that start with single underscore.") {
      val singleUnderscoreStarsWithQuery =
        testRelation
          .where('a like "_abc")
      val optimized = Optimize.execute(singleUnderscoreStarsWithQuery.analyze)
      val correctAnswer = testRelation
        .where(
            And(EqualTo(Length('a), Literal(4)),
              EndsWith('a, Literal("abc"))))
        .analyze
      comparePlans(optimized, correctAnswer)
  }

  test("expressions that start and end with single underscore, " +
    "underscore optimization should be ignored.") {
      val singleUnderscoreEndsWithQuery =
        testRelation
          .where('a like "_abc_")
      val optimized = Optimize.execute(singleUnderscoreEndsWithQuery.analyze)
      val correctAnswer = testRelation
        .where('a like "_abc_")
        .analyze
      comparePlans(optimized, correctAnswer)
  }

  test("optimize limit queries that have single underscore in between.") {
      val singleUnderscoreInQuery =
        testRelation
          .where('a like "abc_def")
      val optimized = Optimize.execute(singleUnderscoreInQuery.analyze)
      val correctAnswer = testRelation
        .where(
          And(EqualTo(Length('a), Literal(7)),
              And(StartsWith('a, Literal("abc")),
                EndsWith('a, Literal("def")))))
        .analyze
      comparePlans(optimized, correctAnswer)
  }

  test("Ignore multiple underscore characters") {
      val singleUnderscoreInQuery =
        testRelation
          .where('a like "abc__def")
      val optimized = Optimize.execute(singleUnderscoreInQuery.analyze)
      val correctAnswer = testRelation
        .where('a like "abc__def")
        .analyze
      comparePlans(optimized, correctAnswer)
  }

  test("multiple underscore characters, underscore optimization should be ignored.") {
      val singleUnderscoreInQuery =
        testRelation
          .where('a like "abc_def_ghi")
      val optimized = Optimize.execute(singleUnderscoreInQuery.analyze)
      val correctAnswer = testRelation
        .where('a like "abc_def_ghi")
        .analyze
      comparePlans(optimized, correctAnswer)
  }
}
