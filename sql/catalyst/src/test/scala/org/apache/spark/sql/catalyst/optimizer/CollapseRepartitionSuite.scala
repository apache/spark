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
    // Always respects the top coalesces amd removes useless coalesce below coalesce
    val query1 = testRelation
      .coalesce(10)
      .coalesce(20)
    val query2 = testRelation
      .coalesce(30)
      .coalesce(20)

    val optimized1 = Optimize.execute(query1.analyze)
    val optimized2 = Optimize.execute(query2.analyze)
    val correctAnswer = testRelation.coalesce(20).analyze

    comparePlans(optimized1, correctAnswer)
    comparePlans(optimized2, correctAnswer)
  }

  test("collapse two adjacent repartitions into one") {
    // Always respects the top repartition amd removes useless repartition below repartition
    val query1 = testRelation
      .repartition(10)
      .repartition(20)
    val query2 = testRelation
      .repartition(30)
      .repartition(20)

    val optimized1 = Optimize.execute(query1.analyze)
    val optimized2 = Optimize.execute(query2.analyze)
    val correctAnswer = testRelation.repartition(20).analyze

    comparePlans(optimized1, correctAnswer)
    comparePlans(optimized2, correctAnswer)
  }

  test("coalesce above repartition") {
    // Remove useless coalesce above repartition
    val query1 = testRelation
      .repartition(10)
      .coalesce(20)

    val optimized1 = Optimize.execute(query1.analyze)
    val correctAnswer1 = testRelation.repartition(10).analyze

    comparePlans(optimized1, correctAnswer1)

    // No change in this case
    val query2 = testRelation
      .repartition(30)
      .coalesce(20)

    val optimized2 = Optimize.execute(query2.analyze)
    val correctAnswer2 = query2.analyze

    comparePlans(optimized2, correctAnswer2)
  }

  test("repartition above coalesce") {
    // Always respects the top repartition amd removes useless coalesce below repartition
    val query1 = testRelation
      .coalesce(10)
      .repartition(20)
    val query2 = testRelation
      .coalesce(30)
      .repartition(20)

    val optimized1 = Optimize.execute(query1.analyze)
    val optimized2 = Optimize.execute(query2.analyze)
    val correctAnswer = testRelation.repartition(20).analyze

    comparePlans(optimized1, correctAnswer)
    comparePlans(optimized2, correctAnswer)
  }

  test("distribute above repartition") {
    // Always respects the top distribute and removes useless repartition
    val query1 = testRelation
      .repartition(10)
      .distribute('a)(20)
    val query2 = testRelation
      .repartition(30)
      .distribute('a)(20)

    val optimized1 = Optimize.execute(query1.analyze)
    val optimized2 = Optimize.execute(query2.analyze)
    val correctAnswer = testRelation.distribute('a)(20).analyze

    comparePlans(optimized1, correctAnswer)
    comparePlans(optimized2, correctAnswer)
  }

  test("distribute above coalesce") {
    // Always respects the top distribute and removes useless coalesce below repartition
    val query1 = testRelation
      .coalesce(10)
      .distribute('a)(20)
    val query2 = testRelation
      .coalesce(30)
      .distribute('a)(20)

    val optimized1 = Optimize.execute(query1.analyze)
    val optimized2 = Optimize.execute(query2.analyze)
    val correctAnswer = testRelation.distribute('a)(20).analyze

    comparePlans(optimized1, correctAnswer)
    comparePlans(optimized2, correctAnswer)
  }

  test("repartition above distribute") {
    // Always respects the top repartition and removes useless distribute below repartition
    val query1 = testRelation
      .distribute('a)(10)
      .repartition(20)
    val query2 = testRelation
      .distribute('a)(30)
      .repartition(20)

    val optimized1 = Optimize.execute(query1.analyze)
    val optimized2 = Optimize.execute(query2.analyze)
    val correctAnswer = testRelation.repartition(20).analyze

    comparePlans(optimized1, correctAnswer)
    comparePlans(optimized2, correctAnswer)
  }

  test("coalesce above distribute") {
    // Remove useless coalesce above distribute
    val query1 = testRelation
      .distribute('a)(10)
      .coalesce(20)

    val optimized1 = Optimize.execute(query1.analyze)
    val correctAnswer1 = testRelation.distribute('a)(10).analyze

    comparePlans(optimized1, correctAnswer1)

    // No change in this case
    val query2 = testRelation
      .distribute('a)(30)
      .coalesce(20)

    val optimized2 = Optimize.execute(query2.analyze)
    val correctAnswer2 = query2.analyze

    comparePlans(optimized2, correctAnswer2)
  }

  test("collapse two adjacent distributes into one") {
    // Always respects the top distribute
    val query1 = testRelation
      .distribute('b)(10)
      .distribute('a)(20)
    val query2 = testRelation
      .distribute('b)(30)
      .distribute('a)(20)

    val optimized1 = Optimize.execute(query1.analyze)
    val optimized2 = Optimize.execute(query2.analyze)
    val correctAnswer = testRelation.distribute('a)(20).analyze

    comparePlans(optimized1, correctAnswer)
    comparePlans(optimized2, correctAnswer)
  }

  test("SPARK-36703: Remove the global Sort if it is the child of RepartitionByExpression") {
    val originalQuery1 = testRelation
      .orderBy('a.asc, 'b.asc)
      .distribute('a)(20)
    comparePlans(Optimize.execute(originalQuery1.analyze), testRelation.distribute('a)(20).analyze)

    val originalQuery2 = testRelation.distribute('a)(10)
      .sortBy('a.asc, 'b.asc)
      .distribute('a)(20)
    comparePlans(Optimize.execute(originalQuery2.analyze), originalQuery2.analyze)
  }
}
