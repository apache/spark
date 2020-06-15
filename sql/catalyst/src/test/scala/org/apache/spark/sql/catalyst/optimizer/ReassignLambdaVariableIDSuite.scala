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
import org.apache.spark.sql.catalyst.expressions.objects.LambdaVariable
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.types.BooleanType

class ReassignLambdaVariableIDSuite extends PlanTest {

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches = Batch("Optimizer Batch", FixedPoint(100), ReassignLambdaVariableID) :: Nil
  }

  test("basic: replace positive IDs with unique negative IDs") {
    val testRelation = LocalRelation('col.int)
    val var1 = LambdaVariable("a", BooleanType, true, id = 2)
    val var2 = LambdaVariable("b", BooleanType, true, id = 4)
    val query = testRelation.where(var1 && var2)
    val optimized = Optimize.execute(query)
    val expected = testRelation.where(var1.copy(id = -1) && var2.copy(id = -2))
    comparePlans(optimized, expected)
  }

  test("ignore LambdaVariable with negative IDs") {
    val testRelation = LocalRelation('col.int)
    val var1 = LambdaVariable("a", BooleanType, true, id = -2)
    val var2 = LambdaVariable("b", BooleanType, true, id = -4)
    val query = testRelation.where(var1 && var2)
    val optimized = Optimize.execute(query)
    comparePlans(optimized, query)
  }

  test("fail if positive ID LambdaVariable and negative LambdaVariable both exist") {
    val testRelation = LocalRelation('col.int)
    val var1 = LambdaVariable("a", BooleanType, true, id = -2)
    val var2 = LambdaVariable("b", BooleanType, true, id = 4)
    val query = testRelation.where(var1 && var2)
    val e = intercept[IllegalStateException](Optimize.execute(query))
    assert(e.getMessage.contains("should be all positive or negative"))
  }
}
