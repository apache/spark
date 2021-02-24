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

  val testRelation = LocalRelation("a".attr.string)

  test("simplify Like into StartsWith") {
    val originalQuery =
      testRelation
        .where(("a".attr like "abc%") || ("a".attr like "abc\\%"))

    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer = testRelation
      .where(StartsWith("a".attr, "abc") || ("a".attr like "abc\\%"))
      .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("simplify Like into EndsWith") {
    val originalQuery =
      testRelation
        .where("a".attr like "%xyz")

    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer = testRelation
      .where(EndsWith("a".attr, "xyz"))
      .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("simplify Like into startsWith and EndsWith") {
    val originalQuery =
      testRelation
        .where(("a".attr like "abc\\%def") || ("a".attr like "abc%def"))

    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer = testRelation
      .where(("a".attr like "abc\\%def") ||
        (Length("a".attr) >= 6 && (StartsWith("a".attr, "abc") && EndsWith("a".attr, "def"))))
      .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("simplify Like into Contains") {
    val originalQuery =
      testRelation
        .where(("a".attr like "%mn%") || ("a".attr like "%mn\\%"))

    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer = testRelation
      .where(Contains("a".attr, "mn") || ("a".attr like "%mn\\%"))
      .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("simplify Like into EqualTo") {
    val originalQuery =
      testRelation
        .where(("a".attr like "") || ("a".attr like "abc"))

    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer = testRelation
      .where(('a === "") || ('a === "abc"))
      .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("null pattern") {
    val originalQuery = testRelation.where("a".attr like Literal(null, StringType)).analyze
    val optimized = Optimize.execute(originalQuery)
    comparePlans(optimized, testRelation.where(Literal(null, BooleanType)).analyze)
  }

  test("test like escape syntax") {
    val originalQuery1 = testRelation.where("a".attr.like("abc#%", '#'))
    val optimized1 = Optimize.execute(originalQuery1.analyze)
    comparePlans(optimized1, originalQuery1.analyze)

    val originalQuery2 = testRelation.where("a".attr.like("abc#%abc", '#'))
    val optimized2 = Optimize.execute(originalQuery2.analyze)
    comparePlans(optimized2, originalQuery2.analyze)
  }

  test("SPARK-33677: LikeSimplification should be skipped if pattern contains any escapeChar") {
    val originalQuery1 =
      testRelation
        .where(("a".attr like "abc%") || ("a".attr like "\\abc%"))
    val optimized1 = Optimize.execute(originalQuery1.analyze)
    val correctAnswer1 = testRelation
      .where(StartsWith("a".attr, "abc") || ("a".attr like "\\abc%"))
      .analyze
    comparePlans(optimized1, correctAnswer1)

    val originalQuery2 =
      testRelation
        .where(("a".attr like "%xyz") || ("a".attr like "%xyz\\"))
    val optimized2 = Optimize.execute(originalQuery2.analyze)
    val correctAnswer2 = testRelation
      .where(EndsWith("a".attr, "xyz") || ("a".attr like "%xyz\\"))
      .analyze
    comparePlans(optimized2, correctAnswer2)

    val originalQuery3 =
      testRelation
        .where(("a".attr like ("@bc%def", '@')) || ("a".attr like "abc%def"))
    val optimized3 = Optimize.execute(originalQuery3.analyze)
    val correctAnswer3 = testRelation
      .where(("a".attr like ("@bc%def", '@')) ||
        (Length("a".attr) >= 6 && (StartsWith("a".attr, "abc") && EndsWith("a".attr, "def"))))
      .analyze
    comparePlans(optimized3, correctAnswer3)

    val originalQuery4 =
      testRelation
        .where(("a".attr like "%mn%") || ("a".attr like ("%mn%", '%')))
    val optimized4 = Optimize.execute(originalQuery4.analyze)
    val correctAnswer4 = testRelation
      .where(Contains("a".attr, "mn") || ("a".attr like ("%mn%", '%')))
      .analyze
    comparePlans(optimized4, correctAnswer4)

    val originalQuery5 =
      testRelation
        .where(("a".attr like "abc") || ("a".attr like ("abbc", 'b')))
    val optimized5 = Optimize.execute(originalQuery5.analyze)
    val correctAnswer5 = testRelation
      .where(("a".attr === "abc") || ("a".attr like ("abbc", 'b')))
      .analyze
    comparePlans(optimized5, correctAnswer5)
  }

  test("simplify LikeAll") {
    val originalQuery =
      testRelation
        .where(("a".attr likeAll(
    "abc%", "abc\\%", "%xyz", "abc\\%def", "abc%def", "%mn%", "%mn\\%", "", "abc")))

    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer = testRelation
      .where((((((StartsWith("a".attr, "abc") && EndsWith("a".attr, "xyz")) &&
        (Length("a".attr) >= 6 && (StartsWith("a".attr, "abc") && EndsWith("a".attr, "def")))) &&
        Contains("a".attr, "mn")) && ("a".attr === "")) && ("a".attr === "abc")) &&
        ("a".attr likeAll("abc\\%", "abc\\%def", "%mn\\%")))
      .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("simplify NotLikeAll") {
    val originalQuery =
      testRelation
        .where(("a".attr notLikeAll(
          "abc%", "abc\\%", "%xyz", "abc\\%def", "abc%def", "%mn%", "%mn\\%", "", "abc")))

    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer = testRelation
      .where((((((Not(StartsWith("a".attr, "abc")) && Not(EndsWith("a".attr, "xyz"))) &&
        Not(Length("a".attr) >= 6 && (StartsWith("a".attr, "abc") && EndsWith("a".attr, "def")))) &&
        Not(Contains("a".attr, "mn"))) && Not("a".attr === "")) && Not("a".attr === "abc")) &&
        ("a".attr notLikeAll("abc\\%", "abc\\%def", "%mn\\%")))
      .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("simplify LikeAny") {
    val originalQuery =
      testRelation
        .where(("a".attr likeAny(
          "abc%", "abc\\%", "%xyz", "abc\\%def", "abc%def", "%mn%", "%mn\\%", "", "abc")))

    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer = testRelation
      .where((((((StartsWith("a".attr, "abc") || EndsWith("a".attr, "xyz")) ||
        (Length("a".attr) >= 6 && (StartsWith("a".attr, "abc") && EndsWith("a".attr, "def")))) ||
        Contains("a".attr, "mn")) || ("a".attr === "")) || ("a".attr === "abc")) ||
        ("a".attr likeAny("abc\\%", "abc\\%def", "%mn\\%")))
      .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("simplify NotLikeAny") {
    val originalQuery =
      testRelation
        .where(("a".attr notLikeAny(
          "abc%", "abc\\%", "%xyz", "abc\\%def", "abc%def", "%mn%", "%mn\\%", "", "abc")))

    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer = testRelation
      .where((((((Not(StartsWith("a".attr, "abc")) || Not(EndsWith("a".attr, "xyz"))) ||
        Not(Length("a".attr) >= 6 && (StartsWith("a".attr, "abc") && EndsWith("a".attr, "def")))) ||
        Not(Contains("a".attr, "mn"))) || Not("a".attr === "")) || Not("a".attr === "abc")) ||
        ("a".attr notLikeAny("abc\\%", "abc\\%def", "%mn\\%")))
      .analyze

    comparePlans(optimized, correctAnswer)
  }
}
