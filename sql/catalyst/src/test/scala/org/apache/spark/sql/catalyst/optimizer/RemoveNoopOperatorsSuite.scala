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
import org.apache.spark.sql.catalyst.expressions.{Alias, Literal}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.RuleExecutor

class RemoveNoopOperatorsSuite extends PlanTest {

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("RemoveNoopOperators", Once,
        RemoveNoopOperators) :: Nil
  }

  val testRelation = LocalRelation('a.int, 'b.int, 'c.int)

  test("Remove all redundant projections in one iteration") {
    val originalQuery = testRelation
      .select('a, 'b, 'c)
      .select('a, 'b, 'c)
      .analyze

    val optimized = Optimize.execute(originalQuery.analyze)

    comparePlans(optimized, testRelation)
  }

  test("Do not remove projections with non-attribute expressions that reuse expr ids") {
    val base = testRelation
      .select('a, 'b, 'c)
      .analyze

    // Replace a with a literal but reuse its expression id
    val Seq(a, b, c) = base.output
    val input = base.select(Alias(Literal("x"), "a")(exprId = a.exprId), b, c).analyze

    val optimized = Optimize.execute(input)

    val expected = testRelation.select(Alias(Literal("x"), "a")(), 'b, 'c).analyze

    comparePlans(optimized, expected)
  }

  test("Remove all redundant windows in one iteration") {
    val originalQuery = testRelation
      .window(Nil, Nil, Nil)
      .window(Nil, Nil, Nil)
      .analyze

    val optimized = Optimize.execute(originalQuery.analyze)

    comparePlans(optimized, testRelation)
  }
}
