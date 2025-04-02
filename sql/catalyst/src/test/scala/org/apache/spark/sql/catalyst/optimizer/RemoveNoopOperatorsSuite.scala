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
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.RuleExecutor

class RemoveNoopOperatorsSuite extends PlanTest {

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("RemoveNoopOperators", Once,
        RemoveNoopOperators) :: Nil
  }

  val testRelation = LocalRelation($"a".int, $"b".int, $"c".int)

  test("Remove all redundant projections in one iteration") {
    val originalQuery = testRelation
      .select($"a", $"b", $"c")
      .select($"a", $"b", $"c")
      .analyze

    val optimized = Optimize.execute(originalQuery.analyze)

    comparePlans(optimized, testRelation)
  }

  test("Remove all redundant windows in one iteration") {
    val originalQuery = testRelation
      .window(Nil, Nil, Nil)
      .window(Nil, Nil, Nil)
      .analyze

    val optimized = Optimize.execute(originalQuery.analyze)

    comparePlans(optimized, testRelation)
  }

  test("SPARK-36353: RemoveNoopOperators should keep output schema") {
    val query = testRelation
      .select(($"a" + $"b").as("c"))
      .analyze
    val originalQuery = Project(Seq(query.output.head.withName("C")), query)
    val optimized = Optimize.execute(originalQuery.analyze)
    val result = testRelation
      .select(($"a" + $"b").as("C"))
      .analyze
    comparePlans(optimized, result)
  }
}
