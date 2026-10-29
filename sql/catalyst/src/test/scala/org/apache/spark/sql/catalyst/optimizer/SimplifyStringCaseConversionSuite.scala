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

class SimplifyStringCaseConversionSuite extends PlanTest {

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("Simplify CaseConversionExpressions", Once,
        SimplifyCaseConversionExpressions) :: Nil
  }

  val testRelation = LocalRelation($"a".string)

  test("simplify UPPER(UPPER(str))") {
    val originalQuery =
      testRelation
        .select(Upper(Upper($"a")) as "u")

    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer =
      testRelation
        .select(Upper($"a") as "u")
        .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("SPARK-59043: do not simplify UPPER(LOWER(str)) to preserve Unicode semantics") {
    val originalQuery =
      testRelation
        .select(Upper(Lower($"a")) as "u")

    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer =
      testRelation
        .select(Upper(Lower($"a")) as "u")
        .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("SPARK-59043: do not simplify LOWER(UPPER(str)) to preserve Unicode semantics") {
    val originalQuery =
      testRelation
        .select(Lower(Upper($"a")) as "l")

    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer = testRelation
      .select(Lower(Upper($"a")) as "l")
      .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("simplify LOWER(LOWER(str))") {
    val originalQuery =
      testRelation
        .select(Lower(Lower($"a")) as "l")

    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer = testRelation
      .select(Lower($"a") as "l")
      .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("SPARK-59043: simplify deeply nested same-case expressions") {
    val nestedUpper = testRelation.select(Upper(Upper(Upper($"a"))) as "u")
    val optimizedUpper = Optimize.execute(nestedUpper.analyze)
    val expectedUpper = testRelation.select(Upper($"a") as "u").analyze
    comparePlans(optimizedUpper, expectedUpper)

    val nestedLower = testRelation.select(Lower(Lower(Lower($"a"))) as "l")
    val optimizedLower = Optimize.execute(nestedLower.analyze)
    val expectedLower = testRelation.select(Lower($"a") as "l").analyze
    comparePlans(optimizedLower, expectedLower)
  }

  test("SPARK-59043: mixed case expressions with multiple layers") {
    // Upper(Lower(Upper(str))) is preserved as there are no adjacent same-case operations
    val query1 = testRelation.select(Upper(Lower(Upper($"a"))) as "res")
    val optimized1 = Optimize.execute(query1.analyze)
    val expected1 = testRelation.select(Upper(Lower(Upper($"a"))) as "res").analyze
    comparePlans(optimized1, expected1)

    // Lower(Upper(Upper(str))) simplifies inner Upper(Upper) to Upper -> Lower(Upper(str))
    val query2 = testRelation.select(Lower(Upper(Upper($"a"))) as "res")
    val optimized2 = Optimize.execute(query2.analyze)
    val expected2 = testRelation.select(Lower(Upper($"a")) as "res").analyze
    comparePlans(optimized2, expected2)

    // Upper(Lower(Lower(str))) simplifies inner Lower(Lower) to Lower -> Upper(Lower(str))
    val query3 = testRelation.select(Upper(Lower(Lower($"a"))) as "res")
    val optimized3 = Optimize.execute(query3.analyze)
    val expected3 = testRelation.select(Upper(Lower($"a")) as "res").analyze
    comparePlans(optimized3, expected3)
  }
}
