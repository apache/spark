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
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Expand, LocalRelation, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.RuleExecutor

class EliminateDistinctSuite extends PlanTest {

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("Operator Optimizations", Once,
        EliminateDistinct) :: Nil
  }

  val testRelation = LocalRelation('a.int)

  test("Eliminate Distinct in Max") {
    val query = testRelation
      .select(maxDistinct('a).as('result))
      .analyze
    val answer = testRelation
      .select(max('a).as('result))
      .analyze
    assert(query != answer)
    comparePlans(Optimize.execute(query), answer)
  }

  test("Eliminate Distinct in Min") {
    val query = testRelation
      .select(minDistinct('a).as('result))
      .analyze
    val answer = testRelation
      .select(min('a).as('result))
      .analyze
    assert(query != answer)
    comparePlans(Optimize.execute(query), answer)
  }
}
