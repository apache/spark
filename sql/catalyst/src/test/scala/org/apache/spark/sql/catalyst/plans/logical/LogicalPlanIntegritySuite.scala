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

package org.apache.spark.sql.catalyst.plans.logical

import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.types.LongType

class LogicalPlanIntegritySuite extends PlanTest {
  import LogicalPlanIntegrity._

  case class OutputTestPlan(child: LogicalPlan, output: Seq[Attribute]) extends UnaryNode {
    override val analyzed = true
    override protected def withNewChildInternal(newChild: LogicalPlan): LogicalPlan =
      copy(child = newChild)
  }

  test("Checks if the same `ExprId` refers to a semantically-equal attribute in a plan output") {
    val t = LocalRelation($"a".int, $"b".int)
    assert(hasUniqueExprIdsForOutput(OutputTestPlan(t, t.output)))
    assert(!hasUniqueExprIdsForOutput(OutputTestPlan(t, t.output.zipWithIndex.map {
      case (a, i) => AttributeReference(s"c$i", LongType)(a.exprId)
    })))
  }

  test("Checks if reference ExprIds are not reused when assigning a new ExprId") {
    val t = LocalRelation($"a".int, $"b".int)
    val Seq(a, b) = t.output
    assert(checkIfSameExprIdNotReused(t.select(Alias(a + 1, "a")())))
    assert(!checkIfSameExprIdNotReused(t.select(Alias(a + 1, "a")(exprId = a.exprId))))
    assert(checkIfSameExprIdNotReused(t.select(Alias(a + 1, "a")(exprId = b.exprId))))
    assert(checkIfSameExprIdNotReused(t.select(Alias(a + b, "ab")())))
    assert(!checkIfSameExprIdNotReused(t.select(Alias(a + b, "ab")(exprId = a.exprId))))
    assert(!checkIfSameExprIdNotReused(t.select(Alias(a + b, "ab")(exprId = b.exprId))))
  }
}
