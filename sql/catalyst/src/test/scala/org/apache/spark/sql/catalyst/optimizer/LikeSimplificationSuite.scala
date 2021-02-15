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

  val testRelation = LocalRelation(Symbol("a").string)

  test("simplify Like into StartsWith") {
    val originalQuery =
      testRelation
        .where((Symbol("a") like "abc%") || (Symbol("a") like "abc\\%"))

    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer = testRelation
      .where(StartsWith(Symbol("a"), "abc") || (Symbol("a") like "abc\\%"))
      .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("simplify Like into EndsWith") {
    val originalQuery =
      testRelation
        .where(Symbol("a") like "%xyz")

    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer = testRelation
      .where(EndsWith(Symbol("a"), "xyz"))
      .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("simplify Like into startsWith and EndsWith") {
    val originalQuery =
      testRelation
        .where((Symbol("a") like "abc\\%def") || (Symbol("a") like "abc%def"))

    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer = testRelation
      .where((Symbol("a") like "abc\\%def") || (Length(Symbol("a")) >= 6 &&
        (StartsWith(Symbol("a"), "abc") && EndsWith(Symbol("a"), "def"))))
      .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("simplify Like into Contains") {
    val originalQuery =
      testRelation
        .where((Symbol("a") like "%mn%") || (Symbol("a") like "%mn\\%"))

    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer = testRelation
      .where(Contains(Symbol("a"), "mn") || (Symbol("a") like "%mn\\%"))
      .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("simplify Like into EqualTo") {
    val originalQuery =
      testRelation
        .where((Symbol("a") like "") || (Symbol("a") like "abc"))

    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer = testRelation
      .where((Symbol("a") === "") || (Symbol("a") === "abc"))
      .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("null pattern") {
    val originalQuery = testRelation.where(Symbol("a") like Literal(null, StringType)).analyze
    val optimized = Optimize.execute(originalQuery)
    comparePlans(optimized, testRelation.where(Literal(null, BooleanType)).analyze)
  }

  test("test like escape syntax") {
    val originalQuery1 = testRelation.where(Symbol("a").like("abc#%", '#'))
    val optimized1 = Optimize.execute(originalQuery1.analyze)
    comparePlans(optimized1, originalQuery1.analyze)

    val originalQuery2 = testRelation.where(Symbol("a").like("abc#%abc", '#'))
    val optimized2 = Optimize.execute(originalQuery2.analyze)
    comparePlans(optimized2, originalQuery2.analyze)
  }

  test("SPARK-33677: LikeSimplification should be skipped if pattern contains any escapeChar") {
    val originalQuery1 =
      testRelation
        .where((Symbol("a") like "abc%") || (Symbol("a") like "\\abc%"))
    val optimized1 = Optimize.execute(originalQuery1.analyze)
    val correctAnswer1 = testRelation
      .where(StartsWith(Symbol("a"), "abc") || (Symbol("a") like "\\abc%"))
      .analyze
    comparePlans(optimized1, correctAnswer1)

    val originalQuery2 =
      testRelation
        .where((Symbol("a") like "%xyz") || (Symbol("a") like "%xyz\\"))
    val optimized2 = Optimize.execute(originalQuery2.analyze)
    val correctAnswer2 = testRelation
      .where(EndsWith(Symbol("a"), "xyz") || (Symbol("a") like "%xyz\\"))
      .analyze
    comparePlans(optimized2, correctAnswer2)

    val originalQuery3 =
      testRelation
        .where((Symbol("a") like ("@bc%def", '@')) || (Symbol("a") like "abc%def"))
    val optimized3 = Optimize.execute(originalQuery3.analyze)
    val correctAnswer3 = testRelation
      .where((Symbol("a") like ("@bc%def", '@')) || (Length(Symbol("a")) >= 6 &&
        (StartsWith(Symbol("a"), "abc") && EndsWith(Symbol("a"), "def"))))
      .analyze
    comparePlans(optimized3, correctAnswer3)

    val originalQuery4 =
      testRelation
        .where((Symbol("a") like "%mn%") || (Symbol("a") like ("%mn%", '%')))
    val optimized4 = Optimize.execute(originalQuery4.analyze)
    val correctAnswer4 = testRelation
      .where(Contains(Symbol("a"), "mn") || (Symbol("a") like ("%mn%", '%')))
      .analyze
    comparePlans(optimized4, correctAnswer4)

    val originalQuery5 =
      testRelation
        .where((Symbol("a") like "abc") || (Symbol("a") like ("abbc", 'b')))
    val optimized5 = Optimize.execute(originalQuery5.analyze)
    val correctAnswer5 = testRelation
      .where((Symbol("a") === "abc") || (Symbol("a") like ("abbc", 'b')))
      .analyze
    comparePlans(optimized5, correctAnswer5)
  }

  test("simplify LikeAll") {
    val originalQuery =
      testRelation
        .where((Symbol("a") likeAll(
    "abc%", "abc\\%", "%xyz", "abc\\%def", "abc%def", "%mn%", "%mn\\%", "", "abc")))

    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer = testRelation
      .where((((((StartsWith(Symbol("a"), "abc") && EndsWith(Symbol("a"), "xyz")) &&
        (Length(Symbol("a")) >= 6 && (StartsWith(Symbol("a"), "abc") &&
          EndsWith(Symbol("a"), "def")))) && Contains(Symbol("a"), "mn")) &&
        (Symbol("a") === "")) && (Symbol("a") === "abc")) &&
        (Symbol("a") likeAll("abc\\%", "abc\\%def", "%mn\\%")))
      .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("simplify NotLikeAll") {
    val originalQuery =
      testRelation
        .where((Symbol("a") notLikeAll(
          "abc%", "abc\\%", "%xyz", "abc\\%def", "abc%def", "%mn%", "%mn\\%", "", "abc")))

    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer = testRelation
      .where((((((Not(StartsWith(Symbol("a"), "abc")) && Not(EndsWith(Symbol("a"), "xyz"))) &&
        Not(Length(Symbol("a")) >= 6 && (StartsWith(Symbol("a"), "abc") &&
          EndsWith(Symbol("a"), "def")))) && Not(Contains(Symbol("a"), "mn"))) &&
        Not(Symbol("a") === "")) && Not(Symbol("a") === "abc")) &&
        (Symbol("a") notLikeAll("abc\\%", "abc\\%def", "%mn\\%")))
      .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("simplify LikeAny") {
    val originalQuery =
      testRelation
        .where((Symbol("a") likeAny(
          "abc%", "abc\\%", "%xyz", "abc\\%def", "abc%def", "%mn%", "%mn\\%", "", "abc")))

    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer = testRelation
      .where((((((StartsWith(Symbol("a"), "abc") || EndsWith(Symbol("a"), "xyz")) ||
        (Length(Symbol("a")) >= 6 && (StartsWith(Symbol("a"), "abc") &&
          EndsWith(Symbol("a"), "def")))) ||
        Contains(Symbol("a"), "mn")) || (Symbol("a") === "")) || (Symbol("a") === "abc")) ||
        (Symbol("a") likeAny("abc\\%", "abc\\%def", "%mn\\%")))
      .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("simplify NotLikeAny") {
    val originalQuery =
      testRelation
        .where((Symbol("a") notLikeAny(
          "abc%", "abc\\%", "%xyz", "abc\\%def", "abc%def", "%mn%", "%mn\\%", "", "abc")))

    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer = testRelation
      .where((((((Not(StartsWith(Symbol("a"), "abc")) || Not(EndsWith(Symbol("a"), "xyz"))) ||
        Not(Length(Symbol("a")) >= 6 && (StartsWith(Symbol("a"), "abc") &&
          EndsWith(Symbol("a"), "def")))) ||
        Not(Contains(Symbol("a"), "mn"))) || Not(Symbol("a") === "")) ||
        Not(Symbol("a") === "abc")) ||
        (Symbol("a") notLikeAny("abc\\%", "abc\\%def", "%mn\\%")))
      .analyze

    comparePlans(optimized, correctAnswer)
  }
}
