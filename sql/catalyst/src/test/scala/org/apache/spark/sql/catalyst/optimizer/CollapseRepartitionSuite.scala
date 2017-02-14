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
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.RuleExecutor

class CollapseRepartitionSuite extends PlanTest {
  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("CollapseRepartition", FixedPoint(10),
        CollapseRepartition) :: Nil
  }

  val testRelation = LocalRelation('a.int, 'b.int)


  test("collapse two adjacent coalesces into one") {
    val query = testRelation
      .coalesce(10)
      .coalesce(20)

    val optimized = Optimize.execute(query.analyze)
    val correctAnswer = testRelation.coalesce(20).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("collapse two adjacent repartitions into one") {
    val query = testRelation
      .repartition(10)
      .repartition(20)

    val optimized = Optimize.execute(query.analyze)
    val correctAnswer = testRelation.repartition(20).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("collapse one coalesce and one repartition into one") {
    val query1 = testRelation
      .coalesce(20)
      .repartition(5)

    val optimized1 = Optimize.execute(query1.analyze)
    val correctAnswer1 = testRelation.repartition(5).analyze

    comparePlans(optimized1, correctAnswer1)

    val query2 = testRelation
      .repartition(5)
      .coalesce(20)

    val optimized2 = Optimize.execute(query2.analyze)
    val correctAnswer2 = testRelation.repartition(5).coalesce(20).analyze

    comparePlans(optimized2, correctAnswer2)
  }

  test("collapse repartition and repartitionBy into one") {
    val query1 = testRelation
      .repartition(10)
      .distribute('a)(20)

    val optimized1 = Optimize.execute(query1.analyze)
    val correctAnswer1 = testRelation.distribute('a)(20).analyze

    comparePlans(optimized1, correctAnswer1)

    val query2 = testRelation
      .coalesce(10)
      .distribute('a)(20)

    val optimized2 = Optimize.execute(query2.analyze)
    val correctAnswer2 = testRelation.distribute('a)(20).analyze

    comparePlans(optimized2, correctAnswer2)
  }

  test("collapse repartitionBy and repartition into one") {
    val query = testRelation
      .distribute('a)(20)
      .repartition(10)

    val optimized = Optimize.execute(query.analyze)
    val correctAnswer = testRelation.distribute('a)(10).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("do not collapse coalesce above repartitionBy into one") {
    val query = testRelation
      .distribute('a)(20)
      .coalesce(10)

    val optimized = Optimize.execute(query.analyze)
    val correctAnswer = testRelation.distribute('a)(20).coalesce(10).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("collapse two adjacent repartitionBys into one") {
    val query = testRelation
      .distribute('b)(10)
      .distribute('a)(20)

    val optimized = Optimize.execute(query.analyze)
    val correctAnswer = testRelation.distribute('a)(20).analyze

    comparePlans(optimized, correctAnswer)
  }
}
