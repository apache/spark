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

class SimplifyCaseConversionExpressionsSuite extends PlanTest {

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("Simplify CaseConversionExpressions", Once,
        SimplifyCaseConversionExpressions) :: Nil
  }

  val testRelation = LocalRelation('a.string)

  test("simplify UPPER(UPPER(str))") {
    val originalQuery =
      testRelation
        .select(Upper(Upper('a)) as 'u)

    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer =
      testRelation
        .select(Upper('a) as 'u)
        .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("simplify UPPER(LOWER(str))") {
    val originalQuery =
      testRelation
        .select(Upper(Lower('a)) as 'u)

    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer =
      testRelation
        .select(Upper('a) as 'u)
        .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("simplify LOWER(UPPER(str))") {
    val originalQuery =
      testRelation
        .select(Lower(Upper('a)) as 'l)

    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer = testRelation
      .select(Lower('a) as 'l)
      .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("simplify LOWER(LOWER(str))") {
    val originalQuery =
      testRelation
        .select(Lower(Lower('a)) as 'l)

    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer = testRelation
      .select(Lower('a) as 'l)
      .analyze

    comparePlans(optimized, correctAnswer)
  }
}
